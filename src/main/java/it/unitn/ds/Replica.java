package it.unitn.ds;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

/**
 * A replica of the shared list.
 *
 * - Reads are served locally; writes are forwarded to the coordinator.
 * - Writes go through a two-phase quorum broadcast: UPDATE / ACK / WRITEOK.
 * - Crashes are detected by timeouts (heartbeat, write-forward, update).
 * - On coordinator failure, a ring election picks the most up-to-date replica,
 *   which finishes any interrupted update and re-syncs everyone.
 */
public class Replica extends AbstractReplica {

    // =================================================================================
    // Update identity ⟨epoch, sequence⟩
    // =================================================================================

    /** Globally-orderable identifier of an update: ⟨epoch, sequence⟩. */
    public static final class UpdateId implements Serializable, Comparable<UpdateId> {
        public final int epoch;
        public final int seq;

        public UpdateId(int epoch, int seq) {
            this.epoch = epoch;
            this.seq = seq;
        }

        @Override
        public int compareTo(UpdateId o) {
            if (this.epoch != o.epoch) return Integer.compare(this.epoch, o.epoch);
            return Integer.compare(this.seq, o.seq);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof UpdateId)) return false;
            UpdateId o = (UpdateId) obj;
            return o.epoch == epoch && o.seq == seq;
        }

        @Override
        public int hashCode() {
            return 31 * epoch + seq;
        }

        @Override
        public String toString() {
            return epoch + ":" + seq;
        }
    }

    // Key for an outstanding client write. rid is per-client, so two clients
    // can send the same rid to one replica; (client, rid) makes it unique.
    private static final class WriteKey implements Serializable {
        final ActorRef client;
        final long rid;

        WriteKey(ActorRef client, long rid) {
            this.client = client;
            this.rid = rid;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof WriteKey)) return false;
            WriteKey k = (WriteKey) o;
            return rid == k.rid && Objects.equals(client, k.client);
        }

        @Override
        public int hashCode() {
            return Objects.hash(client, rid);
        }
    }

    /** A single update with its payload and originating-request bookkeeping. */
    public static final class UpdateRecord implements Serializable {
        public final UpdateId id;
        public final int index;
        public final int value;
        public final int originId;   // replica that received the client write
        public final ActorRef client; // client to ack once applied (may be null)
        public final long rid;        // client request id

        public UpdateRecord(UpdateId id, int index, int value, int originId, ActorRef client, long rid) {
            this.id = id;
            this.index = index;
            this.value = value;
            this.originId = originId;
            this.client = client;
            this.rid = rid;
        }
    }

    // =================================================================================
    // Messages
    // =================================================================================

    /** A contacted replica forwards a client write to the coordinator. */
    public static class UpdateRequest implements Serializable {
        public final int index, value, originId;
        public final ActorRef client;
        public final long rid;
        public UpdateRequest(int index, int value, int originId, ActorRef client, long rid) {
            this.index = index; this.value = value; this.originId = originId;
            this.client = client; this.rid = rid;
        }
    }

    /** Phase 1: coordinator -> all replicas. */
    public static class Update implements Serializable {
        public final UpdateRecord record;
        public Update(UpdateRecord record) { this.record = record; }
    }

    /** Phase 1 reply: replica -> coordinator. */
    public static class UpdateAck implements Serializable {
        public final UpdateId id;
        public final int fromId;
        public UpdateAck(UpdateId id, int fromId) { this.id = id; this.fromId = fromId; }
    }

    /** Phase 2: coordinator -> all replicas. Apply the update. */
    public static class WriteOk implements Serializable {
        public final UpdateId id;
        public WriteOk(UpdateId id) { this.id = id; }
    }

    /** Coordinator liveness beacon. */
    public static class Heartbeat implements Serializable {
        public final int coordinatorId;
        public final int epoch;
        public Heartbeat(int coordinatorId, int epoch) { this.coordinatorId = coordinatorId; this.epoch = epoch; }
    }

    /** Reply to a client read. {@code value} is null when {@code success} is false. */
    public static class ReadReply implements Serializable {
        public final long rid;
        public final int index;
        public final Integer value;
        public final int replicaId;
        public final boolean success;
        public ReadReply(long rid, int index, Integer value, int replicaId, boolean success) {
            this.rid = rid; this.index = index; this.value = value; this.replicaId = replicaId;
            this.success = success;
        }
    }

    /** Reply to a client write (on success, sent once the write is durably applied). */
    public static class WriteReply implements Serializable {
        public final long rid;
        public final int index;
        public final int value;
        public final int replicaId;
        public final boolean success;
        public WriteReply(long rid, int index, int value, int replicaId, boolean success) {
            this.rid = rid; this.index = index; this.value = value; this.replicaId = replicaId;
            this.success = success;
        }
    }

    /** Ring election token. Accumulates each participant's most-recent update. */
    public static class Election implements Serializable {
        public final int crashedCoordinatorId;
        public final Map<Integer, UpdateId> candidates; // id -> most recent observed update
        public final boolean decided;
        public final int winnerId; // valid iff decided
        public Election(int crashedCoordinatorId, Map<Integer, UpdateId> candidates, boolean decided, int winnerId) {
            this.crashedCoordinatorId = crashedCoordinatorId;
            // copy + wrap: messages are shared between actors, keep them immutable
            this.candidates = Collections.unmodifiableMap(new HashMap<>(candidates));
            this.decided = decided;
            this.winnerId = winnerId;
        }
    }

    /** Acknowledges receipt of an Election token (so the forwarder need not skip). */
    public static class ElectionAck implements Serializable {
        public final int fromId;
        public ElectionAck(int fromId) { this.fromId = fromId; }
    }

    /** New coordinator announces leadership and ships missing updates. */
    public static class Synchronization implements Serializable {
        public final int newCoordinatorId;
        public final int newEpoch;
        public final List<UpdateRecord> log; // ordered updates the new coordinator knows
        public Synchronization(int newCoordinatorId, int newEpoch, List<UpdateRecord> log) {
            this.newCoordinatorId = newCoordinatorId;
            this.newEpoch = newEpoch;
            // copy + wrap: one instance is broadcast to every peer, keep it immutable
            this.log = Collections.unmodifiableList(new ArrayList<>(log));
        }
    }

    // ===== Timeout self-messages =====
    private static class HeartbeatTick implements Serializable {}
    private static class HeartbeatTimeout implements Serializable {}
    private static class WriteForwardTimeout implements Serializable { final WriteKey key; WriteForwardTimeout(WriteKey key){this.key=key;} }
    private static class UpdateTimeout implements Serializable { final UpdateId id; UpdateTimeout(UpdateId id){this.id=id;} }
    private static class ElectionAckTimeout implements Serializable { final long token; ElectionAckTimeout(long token){this.token=token;} }
    private static class ElectionOverallTimeout implements Serializable { final int forCrashed; ElectionOverallTimeout(int forCrashed){this.forCrashed=forCrashed;} }

    // =================================================================================
    // State
    // =================================================================================

    private Map<Integer, ActorRef> group = new HashMap<>();
    private List<Integer> ring = new ArrayList<>(); // sorted replica ids
    private int coordinatorId = -1;
    private int epoch = 0;
    private int nextSeq = 0;            // coordinator: next sequence number to assign
    private boolean crashed = false;

    private final int[] positions = new int[POSITIONS_LIST_LENGTH];

    /** Updates observed (UPDATE received) but not yet applied, keyed by id. */
    private final Map<UpdateId, UpdateRecord> pendingUpdates = new HashMap<>();
    /** Updates applied locally, in apply order (the durable history). */
    private final List<UpdateRecord> history = new ArrayList<>();
    private final Set<UpdateId> appliedIds = new HashSet<>();
    private UpdateId mostRecentObserved = null; // max over applied ∪ pending

    // Coordinator side. Updates are pipelined; requestQueue only holds writes
    // that arrived while an election was blocking new broadcasts.
    private final Map<UpdateId, Set<Integer>> ackCounts = new HashMap<>();
    private final Set<UpdateId> committed = new HashSet<>();
    private final List<UpdateRequest> requestQueue = new ArrayList<>();

    // Client writes we forwarded and are still waiting to commit.
    // LinkedHashMap on purpose: after a failover we resubmit these in insertion
    // order = client send order, so a client's writes keep their order.
    private final Map<WriteKey, UpdateRequest> pendingClientWrites = new LinkedHashMap<>();
    private final Map<WriteKey, Cancellable> writeForwardTimeouts = new HashMap<>();

    // Crash configuration (typed crashes)
    private final Map<Crash.Type, Integer> crashCountdown = new HashMap<>();

    // Timers
    private Cancellable heartbeatTimer;   // coordinator: periodic beacon
    private Cancellable heartbeatTimeout; // replica: watchdog on coordinator
    private final Map<UpdateId, Cancellable> updateTimeouts = new HashMap<>();

    // Election state
    private boolean inElection = false;
    private int electionForCrashed = -1;
    private long electionTokenCounter = 0;
    private long pendingElectionToken = -1;     // token id awaiting an ElectionAck
    private Election pendingElectionMsg = null;  // message awaiting an ElectionAck
    private int pendingElectionTargetOffset = 0; // ring offset currently being tried
    private int expectedAckFrom = -1;            // replica id we expect an ElectionAck from
    private Cancellable electionAckTimer;
    private Cancellable electionOverallTimer;
    private boolean declaredThisElection = false;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    // =================================================================================
    // Helpers
    // =================================================================================

    @Override
    public int getSystemNumberOfActors() {
        return group.size();
    }

    private int quorum() {
        return group.size() / 2 + 1;
    }

    private boolean isCoordinator() {
        return coordinatorId == id;
    }

    private Cancellable scheduleOnce(Serializable msg, long delayMs) {
        return getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(Math.max(1, delayMs), TimeUnit.MILLISECONDS),
                getSelf(), msg, getContext().getDispatcher(), getSelf());
    }

    /** Network send to every other replica. */
    private void broadcast(Serializable m) {
        for (Map.Entry<Integer, ActorRef> e : group.entrySet()) {
            if (e.getKey() != id) tell(m, e.getValue());
        }
    }

    // Broadcast that checks the crash countdown before each send, so the
    // replica can crash mid-loop after reaching only some peers (used to test
    // a coordinator dying during an UPDATE or WRITEOK broadcast).
    // Callers must check `crashed` afterwards.
    private void broadcastCrashing(Serializable m, Crash.Type t) {
        for (Map.Entry<Integer, ActorRef> e : group.entrySet()) {
            if (e.getKey() == id) continue;
            if (maybeCrashBefore(t)) return; // crashed mid-broadcast
            tell(m, e.getValue());
        }
    }

    /** Timeout used to suspect an unresponsive coordinator / dropped message. */
    private long coordinatorTimeoutDelay() {
        return getCoordinatorBeatInterval() * 2L + getMaxLatencyPlusTolerance();
    }

    /** Timeout used while waiting for an ElectionAck before skipping a node. */
    private long electionAckDelay() {
        return getMaxLatencyPlusTolerance() + 50L;
    }

    private void updateMostRecentObserved(UpdateId candidate) {
        if (candidate == null) return;
        if (mostRecentObserved == null || candidate.compareTo(mostRecentObserved) > 0) {
            mostRecentObserved = candidate;
        }
    }

    // True if (epoch, coordId) is older than the current (epoch, coordinator),
    // compared as a pair. Used to drop messages from a superseded coordinator.
    // On equal epochs the higher id wins, so two winners can't flip-flop.
    // Static + public so it can be unit-tested directly.
    public static boolean staleRegime(int curEpoch, int curCoordId, int epoch, int coordId) {
        return epoch < curEpoch || (epoch == curEpoch && coordId < curCoordId);
    }

    // =================================================================================
    // Initialization
    // =================================================================================

    @Override
    public void initSystem(InitSystem sysInit) {
        if (!group.isEmpty()) return; // ignore duplicate init
        this.group = new HashMap<>(sysInit.group);
        this.coordinatorId = sysInit.coordinator_id;
        this.epoch = 0;
        this.nextSeq = 0;
        this.ring = new ArrayList<>(group.keySet());
        Collections.sort(this.ring);

        if (isCoordinator()) {
            startHeartbeating();
        } else {
            armHeartbeatTimeout();
        }
    }

    // =================================================================================
    // Crash handling
    // =================================================================================

    @Override
    public void crash(Crash how_to_crash) {
        if (how_to_crash.type == Crash.Type.Now) {
            crashNow();
        } else {
            crashCountdown.put(how_to_crash.type, how_to_crash.after_n_messages_of_type);
        }
    }

    /** Returns true and crashes if a pending typed-crash for {@code t} has elapsed. */
    private boolean maybeCrashBefore(Crash.Type t) {
        Integer remaining = crashCountdown.get(t);
        if (remaining == null) return false;
        if (remaining <= 0) {
            crashNow();
            return true;
        }
        crashCountdown.put(t, remaining - 1);
        return false;
    }

    private void crashNow() {
        crashed = true;
        cancel(heartbeatTimer);
        cancel(heartbeatTimeout);
        cancel(electionAckTimer);
        cancel(electionOverallTimer);
        for (Cancellable c : updateTimeouts.values()) cancel(c);
        for (Cancellable c : writeForwardTimeouts.values()) cancel(c);
        getContext().become(crashedReceive());
    }

    private static void cancel(Cancellable c) {
        if (c != null) c.cancel();
    }

    // =================================================================================
    // Heartbeats / coordinator liveness
    // =================================================================================

    private void startHeartbeating() {
        cancel(heartbeatTimeout);
        cancel(heartbeatTimer);
        heartbeatTimer = getContext().getSystem().scheduler().scheduleWithFixedDelay(
                Duration.create(0, TimeUnit.MILLISECONDS),
                Duration.create(getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                getSelf(), new HeartbeatTick(), getContext().getDispatcher(), getSelf());
    }

    private void onHeartbeatTick(HeartbeatTick t) {
        if (crashed || !isCoordinator()) return;
        // May crash mid-loop: Crash.Type.Heartbeat on the coordinator = crash
        // while sending out heartbeats, after n messages were sent.
        broadcastCrashing(new Heartbeat(id, epoch), Crash.Type.Heartbeat);
    }

    private void armHeartbeatTimeout() {
        cancel(heartbeatTimeout);
        heartbeatTimeout = scheduleOnce(new HeartbeatTimeout(), coordinatorTimeoutDelay());
    }

    private void onHeartbeat(Heartbeat hb) {
        if (crashed) return;
        // Crash.Type.Heartbeat on a plain replica = crash on receiving a heartbeat.
        if (maybeCrashBefore(Crash.Type.Heartbeat)) return;
        // Ignore beacons of a strictly older ⟨epoch, coordinator⟩ regime.
        if (staleRegime(epoch, coordinatorId, hb.epoch, hb.coordinatorId)) {
            debug("ignoring HEARTBEAT from stale regime (epoch " + hb.epoch
                    + ", coordinator " + hb.coordinatorId + ")");
            return;
        }
        this.epoch = hb.epoch;
        this.coordinatorId = hb.coordinatorId;
        if (!isCoordinator()) {
            cancel(heartbeatTimer); // step down if we were a superseded same-epoch coordinator
            armHeartbeatTimeout();
        }
    }

    private void onHeartbeatTimeout(HeartbeatTimeout t) {
        if (crashed || isCoordinator()) return;
        if (!inElection) {
            debug("no heartbeat from coordinator " + coordinatorId + " within "
                    + coordinatorTimeoutDelay() + "ms -> suspecting crash");
        }
        startElection(coordinatorId);
    }

    // =================================================================================
    // Read / write request handling
    // =================================================================================

    /** Guards every client-supplied index before it can reach positions[]. */
    private static boolean isValidIndex(int index) {
        return index >= 0 && index < POSITIONS_LIST_LENGTH;
    }

    private void onReadReq(Client.ReadReq msg) {
        if (crashed) return;
        ActorRef client = getSender();
        if (!isValidIndex(msg.index)) {
            // Reply with failure. A raw array access would throw, and Akka would
            // then restart the actor with fresh (empty) state.
            debug("rejecting READ with out-of-range index " + msg.index);
            tell(new ReadReply(msg.rid, msg.index, null, id, false), client);
            return;
        }
        tell(new ReadReply(msg.rid, msg.index, positions[msg.index], id, true), client);
    }

    private void onWriteReq(Client.WriteReq msg) {
        if (crashed) return;
        ActorRef client = getSender();
        if (!isValidIndex(msg.index)) {
            // Reject up front: a bad index must not reach the broadcast, or
            // every replica would throw when applying it.
            debug("rejecting WRITE with out-of-range index " + msg.index);
            tell(new WriteReply(msg.rid, msg.index, msg.value, id, false), client);
            return;
        }
        UpdateRequest req = new UpdateRequest(msg.index, msg.value, id, client, msg.rid);
        pendingClientWrites.put(new WriteKey(client, msg.rid), req);
        debug("client WRITE (" + msg.index + ", " + msg.value + ") -> "
                + (isCoordinator() ? "handling locally (I am coordinator)"
                                   : "forwarding to coordinator " + coordinatorId));
        forwardWriteToCoordinator(req);
    }

    private void forwardWriteToCoordinator(UpdateRequest req) {
        if (isCoordinator()) {
            // Handle locally; no forward timeout needed.
            onUpdateRequest(req, getSelf());
            return;
        }
        ActorRef coord = group.get(coordinatorId);
        if (coord == null) {
            // Shouldn't happen after init. Leave the write pending; it gets
            // re-forwarded on the next Synchronization.
            return;
        }
        WriteKey key = new WriteKey(req.client, req.rid);
        tell(req, coord);
        cancel(writeForwardTimeouts.remove(key));
        writeForwardTimeouts.put(key,
                scheduleOnce(new WriteForwardTimeout(key), coordinatorTimeoutDelay()));
    }

    private void onWriteForwardTimeout(WriteForwardTimeout t) {
        if (crashed) return;
        writeForwardTimeouts.remove(t.key);
        if (!pendingClientWrites.containsKey(t.key)) return; // already committed
        // Coordinator never started the broadcast -> suspect it crashed.
        debug("coordinator " + coordinatorId + " did not initiate the broadcast in time -> suspecting crash");
        startElection(coordinatorId);
    }

    // =================================================================================
    // Two-phase broadcast — coordinator side
    // =================================================================================

    private void onUpdateRequest(UpdateRequest req, ActorRef sender) {
        if (crashed || !isCoordinator()) return;
        if (inElection) {
            // Don't start new broadcasts until the election/sync is done.
            requestQueue.add(req);
            return;
        }
        initiateUpdate(req);
    }

    // Coordinator: stamp the next (epoch, seq) and start the broadcast.
    // Several updates can be in flight at once. FIFO channels keep UPDATEs (and
    // later WRITEOKs) in order, and replicas ACK in order, so quorums for
    // consecutive updates also complete in order.
    private void initiateUpdate(UpdateRequest req) {
        UpdateId uid = new UpdateId(epoch, nextSeq++);
        UpdateRecord rec = new UpdateRecord(uid, req.index, req.value, req.originId, req.client, req.rid);

        pendingUpdates.put(uid, rec);
        updateMostRecentObserved(uid);
        Set<Integer> acks = new HashSet<>();
        acks.add(id); // coordinator counts itself
        ackCounts.put(uid, acks);

        // May crash mid-loop (Crash.Type.Update on the coordinator = crash
        // during the broadcast of an UPDATE, after n messages were sent out).
        broadcastCrashing(new Update(rec), Crash.Type.Update);
        if (crashed) return;
        // If we are the whole quorum already (e.g. only survivor), commit now.
        maybeCommit(uid);
    }

    /** Coordinator: broadcast the writes that queued up during the election. */
    private void drainRequestQueue() {
        while (!crashed && !inElection && !requestQueue.isEmpty()) {
            initiateUpdate(requestQueue.remove(0));
        }
    }

    private void onUpdateAck(UpdateAck ack) {
        if (crashed || !isCoordinator()) return;
        Set<Integer> acks = ackCounts.get(ack.id);
        if (acks == null) return;
        acks.add(ack.fromId);
        maybeCommit(ack.id);
    }

    private void maybeCommit(UpdateId uid) {
        if (committed.contains(uid)) return;
        Set<Integer> acks = ackCounts.get(uid);
        if (acks == null || acks.size() < quorum()) return;
        committed.add(uid);
        debug("quorum reached for update " + uid + " (" + acks.size() + "/" + group.size()
                + " ACKs, incl. self) -> applying + WRITEOK dissemination");
        // Apply here and tell everyone to apply. May crash mid-WRITEOK: we've
        // applied but only some replicas hear about it -> recovery finishes it.
        applyUpdate(pendingUpdates.get(uid));
        broadcastCrashing(new WriteOk(uid), Crash.Type.WriteOK);
    }

    // =================================================================================
    // Two-phase broadcast — replica side
    // =================================================================================

    private void onUpdate(Update msg) {
        if (crashed) return;
        if (maybeCrashBefore(Crash.Type.Update)) return; // crash on receiving an UPDATE
        UpdateRecord rec = msg.record;
        // Drop UPDATEs from an old epoch (delayed message from a dead coordinator).
        if (rec.id.epoch < epoch) {
            debug("ignoring UPDATE " + rec.id + " from a dead epoch (current epoch " + epoch + ")");
            return;
        }
        // An UPDATE means the coordinator is alive.
        if (!isCoordinator()) armHeartbeatTimeout();

        // Coordinator started the broadcast for our own write: the forward
        // timeout is done, the WRITEOK timeout below takes over.
        if (rec.originId == id && rec.client != null) {
            cancel(writeForwardTimeouts.remove(new WriteKey(rec.client, rec.rid)));
        }

        if (!appliedIds.contains(rec.id) && !pendingUpdates.containsKey(rec.id)) {
            pendingUpdates.put(rec.id, rec);
            updateMostRecentObserved(rec.id);
        }
        // ACK the sender (the coordinator); getSender() is preserved by NetworkChannel.
        tell(new UpdateAck(rec.id, id), getSender());

        // If no WRITEOK arrives, suspect the coordinator.
        cancel(updateTimeouts.remove(rec.id));
        updateTimeouts.put(rec.id, scheduleOnce(new UpdateTimeout(rec.id), coordinatorTimeoutDelay()));
    }

    private void onWriteOk(WriteOk msg) {
        if (crashed) return;
        // Optional crash point: die on receiving a WRITEOK, before applying.
        if (maybeCrashBefore(Crash.Type.WriteOK)) return;
        cancel(updateTimeouts.remove(msg.id));
        if (!isCoordinator()) armHeartbeatTimeout();
        UpdateRecord rec = pendingUpdates.get(msg.id);
        if (rec == null) {
            // already applied or never observed
            return;
        }
        applyUpdate(rec);
    }

    private void onUpdateTimeout(UpdateTimeout t) {
        if (crashed) return;
        if (appliedIds.contains(t.id)) return;       // applied in the meantime
        if (!pendingUpdates.containsKey(t.id)) return;
        // No WRITEOK for an observed UPDATE -> suspect the coordinator.
        debug("no WRITEOK for observed update " + t.id + " -> suspecting coordinator " + coordinatorId);
        startElection(coordinatorId);
    }

    /** Apply an update once, updating local state, and notify the test probe. */
    private void applyUpdate(UpdateRecord rec) {
        if (rec == null || appliedIds.contains(rec.id)) return; // apply once
        cancel(updateTimeouts.remove(rec.id));
        positions[rec.index] = rec.value;
        appliedIds.add(rec.id);
        pendingUpdates.remove(rec.id);
        history.add(rec);
        updateMostRecentObserved(rec.id);
        log("applied update " + rec.id + " (" + rec.index + ", " + rec.value + ")");
        callbackOnUpdateApplied(rec.index, rec.value);

        // If we took this client write, reply now. remove() != null guards
        // against replying twice if the update is re-learned during a sync.
        if (rec.originId == id && rec.client != null) {
            WriteKey key = new WriteKey(rec.client, rec.rid);
            if (pendingClientWrites.remove(key) != null) {
                cancel(writeForwardTimeouts.remove(key));
                tell(new WriteReply(rec.rid, rec.index, rec.value, id, true), rec.client);
            }
        }
    }

    // =================================================================================
    // Ring-based coordinator election
    // =================================================================================

    private int ringIndexOf(int replicaId) {
        return ring.indexOf(replicaId);
    }

    /** Begin (or join) an election for a suspected-crashed coordinator. */
    private void startElection(int crashedCoordId) {
        if (crashed) return;
        if (inElection) return;
        beginElectionParticipation(crashedCoordId);

        Map<Integer, UpdateId> candidates = new HashMap<>();
        candidates.put(id, mostRecentObserved);
        Election token = new Election(crashedCoordId, candidates, false, -1);
        forwardElection(token);
    }

    private void beginElectionParticipation(int crashedCoordId) {
        inElection = true;
        electionForCrashed = crashedCoordId;
        declaredThisElection = false;
        cancel(heartbeatTimer);
        callbackOnElectionStarted(crashedCoordId);
        cancel(electionOverallTimer);
        electionOverallTimer = scheduleOnce(new ElectionOverallTimeout(crashedCoordId),
                coordinatorTimeoutDelay() * 4L);
    }

    /** Forward the token to the next ring member, retrying past crashed peers. */
    private void forwardElection(Election token) {
        pendingElectionMsg = token;
        pendingElectionToken = ++electionTokenCounter;
        pendingElectionTargetOffset = 1;
        sendElectionToOffset();
    }

    private void sendElectionToOffset() {
        int myIdx = ringIndexOf(id);
        int n = ring.size();
        if (pendingElectionTargetOffset >= n) {
            // Tried the whole ring, no ACK: everyone else is down. If it's our
            // own undecided token, we're the only survivor and win.
            if (!pendingElectionMsg.decided) {
                becomeCoordinatorIfWinner(decideWinner(pendingElectionMsg.candidates), pendingElectionMsg);
            }
            expectedAckFrom = -1;
            return;
        }
        int targetId = ring.get((myIdx + pendingElectionTargetOffset) % n);
        expectedAckFrom = targetId;
        debug("ELECTION token" + (pendingElectionMsg.decided ? " (decided, winner " + pendingElectionMsg.winnerId + ")" : "")
                + " -> Replica " + targetId
                + (pendingElectionTargetOffset > 1 ? " (after skipping " + (pendingElectionTargetOffset - 1) + " dead)" : ""));
        tell(pendingElectionMsg, group.get(targetId));
        cancel(electionAckTimer);
        final long token = pendingElectionToken;
        electionAckTimer = scheduleOnce(new ElectionAckTimeout(token), electionAckDelay());
    }

    private void onElectionAckTimeout(ElectionAckTimeout t) {
        if (crashed) return;
        if (t.token != pendingElectionToken) return; // stale timer
        if (expectedAckFrom == -1) return;            // already acked
        // Presumed crash of the current target: skip it and try the next.
        debug("no ElectionAck from Replica " + expectedAckFrom + " -> presumed crashed, skipping");
        pendingElectionTargetOffset++;
        sendElectionToOffset();
    }

    private void onElectionAck(ElectionAck ack) {
        if (crashed) return;
        if (ack.fromId != expectedAckFrom) return; // ack for a node we already skipped
        cancel(electionAckTimer);
        expectedAckFrom = -1;
    }

    private void onElection(Election token) {
        if (crashed) return;
        // Crash BEFORE acking, so the forwarder sees no ACK and skips us.
        if (maybeCrashBefore(Crash.Type.Election)) return;

        // ACK the forwarder so it doesn't skip us. getSender() is preserved.
        ActorRef forwarder = getSender();
        if (forwarder != null) tell(new ElectionAck(id), forwarder);

        // Ignore tokens for an election we already finished.
        if (!inElection) {
            if (token.crashedCoordinatorId != coordinatorId) return;
            beginElectionParticipation(token.crashedCoordinatorId);
        }

        if (token.decided) {
            handleDecidedToken(token);
            return;
        }

        if (token.candidates.containsKey(id)) {
            // Token came back to us: it visited everyone reachable, so decide.
            int winner = decideWinner(token.candidates);
            Election decided = new Election(token.crashedCoordinatorId, token.candidates, true, winner);
            if (winner == id) {
                becomeCoordinatorIfWinner(winner, decided);
            } else {
                forwardElection(decided);
            }
        } else {
            Map<Integer, UpdateId> candidates = new HashMap<>(token.candidates);
            candidates.put(id, mostRecentObserved);
            Election forwarded = new Election(token.crashedCoordinatorId, candidates, false, -1);
            forwardElection(forwarded);
        }
    }

    private void handleDecidedToken(Election token) {
        if (token.winnerId == id) {
            becomeCoordinatorIfWinner(token.winnerId, token);
        } else if (!declaredThisElection) {
            // Forward the decision onward until it reaches the winner.
            forwardElection(token);
        }
    }

    private int decideWinner(Map<Integer, UpdateId> candidates) {
        int bestId = -1;
        UpdateId bestUpd = null;
        for (Map.Entry<Integer, UpdateId> e : candidates.entrySet()) {
            int cid = e.getKey();
            UpdateId u = e.getValue();
            if (bestId == -1 || isBetterCandidate(u, cid, bestUpd, bestId)) {
                bestId = cid;
                bestUpd = u;
            }
        }
        return bestId;
    }

    /** More recent update wins; ties broken by higher replica id. */
    private boolean isBetterCandidate(UpdateId u, int cid, UpdateId bestUpd, int bestId) {
        int cmp = compareUpdate(u, bestUpd);
        if (cmp != 0) return cmp > 0;
        return cid > bestId;
    }

    private int compareUpdate(UpdateId a, UpdateId b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        return a.compareTo(b);
    }

    private void becomeCoordinatorIfWinner(int winner, Election token) {
        if (winner != id || declaredThisElection) return;
        declaredThisElection = true;

        // Start a fresh epoch above anything we have seen.
        int maxEpoch = epoch;
        if (mostRecentObserved != null) maxEpoch = Math.max(maxEpoch, mostRecentObserved.epoch);
        for (UpdateId uid : pendingUpdates.keySet()) maxEpoch = Math.max(maxEpoch, uid.epoch);
        this.epoch = maxEpoch + 1;
        this.coordinatorId = id;

        // Finish any update we observed but never applied, then sync everyone
        // with our full log.
        debug("election won -> new epoch " + epoch + ", completing " + pendingUpdates.size()
                + " interrupted update(s), then broadcasting SYNCHRONIZATION");
        completePendingUpdates();
        List<UpdateRecord> fullLog = new ArrayList<>(history);

        this.nextSeq = 0;
        finishElection();
        callbackOnCoordinatorElected(id);

        broadcast(new Synchronization(id, epoch, fullLog));
        startHeartbeating();

        // Re-issue any client writes we still owe, and resume queued work.
        resubmitOwnPendingWrites();
        drainRequestQueue();
    }

    /** Apply, in id order, any observed-but-unapplied updates (interrupted broadcasts). */
    private void completePendingUpdates() {
        List<UpdateRecord> pend = new ArrayList<>(pendingUpdates.values());
        pend.sort((a, b) -> a.id.compareTo(b.id));
        for (UpdateRecord rec : pend) {
            applyUpdate(rec);
        }
    }

    private void finishElection() {
        inElection = false;
        electionForCrashed = -1;
        cancel(electionAckTimer);
        cancel(electionOverallTimer);
        pendingElectionToken = -1;
        pendingElectionMsg = null;
    }

    // Pending ids that are NOT in the sync log. These are leftovers of the dead
    // epoch that never committed (a committed update would be in the log), so we
    // drop them on sync; otherwise a later election could re-apply them.
    // Static + public so it can be unit-tested directly.
    public static Set<UpdateId> staleAfterSync(Collection<UpdateId> pendingIds, List<UpdateRecord> syncLog) {
        Set<UpdateId> vouched = new HashSet<>();
        for (UpdateRecord rec : syncLog) vouched.add(rec.id);
        Set<UpdateId> stale = new HashSet<>();
        for (UpdateId uid : pendingIds) {
            if (!vouched.contains(uid)) stale.add(uid);
        }
        return stale;
    }

    /** Election candidacy must not keep counting purged pending updates. */
    private void recomputeMostRecentObserved() {
        mostRecentObserved = null;
        for (UpdateRecord rec : history) updateMostRecentObserved(rec.id);
        for (UpdateId uid : pendingUpdates.keySet()) updateMostRecentObserved(uid);
    }

    private void onSynchronization(Synchronization sync) {
        if (crashed) return;
        // Ignore a stale or already-seen regime: a delayed Synchronization from
        // an old coordinator must not roll the epoch back. Equal epoch, higher id wins.
        if (sync.newEpoch < epoch
                || (sync.newEpoch == epoch && sync.newCoordinatorId <= coordinatorId)) {
            debug("ignoring SYNCHRONIZATION from stale regime (epoch " + sync.newEpoch
                    + ", coordinator " + sync.newCoordinatorId + ")");
            return;
        }

        // Apply any updates we are missing, in order.
        int missing = 0;
        for (UpdateRecord rec : sync.log) {
            if (!appliedIds.contains(rec.id)) {
                applyUpdate(rec);
                missing++;
            }
        }
        // The log is the new epoch's starting state: drop pending updates not in
        // it (dead-epoch leftovers). Their origins resubmit them below.
        Set<UpdateId> stale = staleAfterSync(pendingUpdates.keySet(), sync.log);
        for (UpdateId uid : stale) {
            pendingUpdates.remove(uid);
            cancel(updateTimeouts.remove(uid));
        }
        recomputeMostRecentObserved();
        debug("SYNCHRONIZATION from new coordinator " + sync.newCoordinatorId + " (epoch " + sync.newEpoch
                + "): applied " + missing + " missing update(s), purged " + stale.size() + " stale pending");
        // Drop any queued requests: their origins resubmit them, so replaying
        // ours would duplicate.
        requestQueue.clear();

        this.coordinatorId = sync.newCoordinatorId;
        this.epoch = sync.newEpoch;
        finishElection();
        callbackOnCoordinatorElected(sync.newCoordinatorId);
        cancel(heartbeatTimer); // stop beaconing if we briefly thought we were coordinator
        armHeartbeatTimeout();

        // Re-send our own writes that never committed to the new coordinator.
        resubmitOwnPendingWrites();
    }

    private void resubmitOwnPendingWrites() {
        if (pendingClientWrites.isEmpty()) return;
        List<UpdateRequest> pend = new ArrayList<>(pendingClientWrites.values());
        for (UpdateRequest req : pend) {
            forwardWriteToCoordinator(req);
        }
    }

    private void onElectionOverallTimeout(ElectionOverallTimeout t) {
        if (crashed || !inElection) return;
        if (t.forCrashed != electionForCrashed) return;
        // Election stalled (e.g. the best candidate crashed). Restart it.
        inElection = false;
        startElection(electionForCrashed >= 0 ? electionForCrashed : coordinatorId);
    }

    // =================================================================================
    // Receive behaviors
    // =================================================================================

    @Override
    public Receive createReceive() {
        return createBaseReceiveBuilder()
                // client requests
                .match(Client.ReadReq.class, this::onReadReq)
                .match(Client.WriteReq.class, this::onWriteReq)
                // 2PC
                .match(UpdateRequest.class, msg -> onUpdateRequest(msg, getSender()))
                .match(Update.class, this::onUpdate)
                .match(UpdateAck.class, this::onUpdateAck)
                .match(WriteOk.class, this::onWriteOk)
                // liveness
                .match(Heartbeat.class, this::onHeartbeat)
                .match(HeartbeatTick.class, this::onHeartbeatTick)
                .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
                .match(WriteForwardTimeout.class, this::onWriteForwardTimeout)
                .match(UpdateTimeout.class, this::onUpdateTimeout)
                // election
                .match(Election.class, this::onElection)
                .match(ElectionAck.class, this::onElectionAck)
                .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
                .match(ElectionOverallTimeout.class, this::onElectionOverallTimeout)
                .match(Synchronization.class, this::onSynchronization)
                .build();
    }

    /** Behavior of a crashed replica: ignore everything, send nothing. */
    private Receive crashedReceive() {
        return receiveBuilder()
                .matchAny(msg -> { /* ignored: replica has crashed */ })
                .build();
    }
}
