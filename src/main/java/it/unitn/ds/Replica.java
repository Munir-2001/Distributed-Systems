package it.unitn.ds;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

/**
 * A replica of the distributed intelligence database.
 *
 * <p>Implements:
 * <ul>
 *   <li>Read/write request handling (reads served locally, writes forwarded to
 *       the coordinator).</li>
 *   <li>A two-phase, quorum-based total-order broadcast (UPDATE / ACK / WRITEOK)
 *       driven by the coordinator.</li>
 *   <li>Timeout-based crash detection (heartbeats, write-forward timeouts,
 *       update timeouts).</li>
 *   <li>A ring-based coordinator election that selects the replica holding the
 *       most recent update, completes any interrupted update, and re-synchronizes
 *       the group.</li>
 * </ul>
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

    /** Reply to a client read. */
    public static class ReadReply implements Serializable {
        public final long rid;
        public final int index;
        public final Integer value;
        public final int replicaId;
        public ReadReply(long rid, int index, Integer value, int replicaId) {
            this.rid = rid; this.index = index; this.value = value; this.replicaId = replicaId;
        }
    }

    /** Reply to a client write (sent once the write is durably applied). */
    public static class WriteReply implements Serializable {
        public final long rid;
        public final int index;
        public final int value;
        public final int replicaId;
        public WriteReply(long rid, int index, int value, int replicaId) {
            this.rid = rid; this.index = index; this.value = value; this.replicaId = replicaId;
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
            this.candidates = candidates;
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
            this.log = log;
        }
    }

    // ===== Timeout self-messages =====
    private static class HeartbeatTick implements Serializable {}
    private static class HeartbeatTimeout implements Serializable {}
    private static class WriteForwardTimeout implements Serializable { final long rid; WriteForwardTimeout(long rid){this.rid=rid;} }
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

    // Coordinator-side 2PC bookkeeping
    private final Map<UpdateId, Set<Integer>> ackCounts = new HashMap<>();
    private final Set<UpdateId> committed = new HashSet<>();
    private boolean broadcasting = false;
    private final List<UpdateRequest> requestQueue = new ArrayList<>();

    // Origin-side: client writes this replica forwarded and is still waiting on
    private final Map<Long, UpdateRequest> pendingClientWrites = new HashMap<>();
    private final Map<Long, Cancellable> writeForwardTimeouts = new HashMap<>();

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
        if (maybeCrashBefore(Crash.Type.Heartbeat)) return;
        broadcast(new Heartbeat(id, epoch));
    }

    private void armHeartbeatTimeout() {
        cancel(heartbeatTimeout);
        heartbeatTimeout = scheduleOnce(new HeartbeatTimeout(), coordinatorTimeoutDelay());
    }

    private void onHeartbeat(Heartbeat hb) {
        if (crashed) return;
        if (hb.epoch < epoch) return; // stale
        if (hb.epoch > epoch) {
            // a coordinator from a newer epoch we have not synced with yet
            this.epoch = hb.epoch;
        }
        this.coordinatorId = hb.coordinatorId;
        if (!isCoordinator()) armHeartbeatTimeout();
    }

    private void onHeartbeatTimeout(HeartbeatTimeout t) {
        if (crashed || isCoordinator()) return;
        startElection(coordinatorId);
    }

    // =================================================================================
    // Read / write request handling
    // =================================================================================

    private void onReadReq(Client.ReadReq msg) {
        if (crashed) return;
        ActorRef client = getSender();
        tell(new ReadReply(msg.rid, msg.index, positions[msg.index], id), client);
    }

    private void onWriteReq(Client.WriteReq msg) {
        if (crashed) return;
        ActorRef client = getSender();
        UpdateRequest req = new UpdateRequest(msg.index, msg.value, id, client, msg.rid);
        pendingClientWrites.put(msg.rid, req);
        forwardWriteToCoordinator(req);
    }

    private void forwardWriteToCoordinator(UpdateRequest req) {
        if (isCoordinator()) {
            // Handle locally; no forward timeout needed.
            onUpdateRequest(req, getSelf());
        } else {
            tell(req, group.get(coordinatorId));
            cancel(writeForwardTimeouts.remove(req.rid));
            writeForwardTimeouts.put(req.rid,
                    scheduleOnce(new WriteForwardTimeout(req.rid), coordinatorTimeoutDelay()));
        }
    }

    private void onWriteForwardTimeout(WriteForwardTimeout t) {
        if (crashed) return;
        writeForwardTimeouts.remove(t.rid);
        if (!pendingClientWrites.containsKey(t.rid)) return; // already committed
        // The coordinator did not initiate the broadcast in time -> suspect crash.
        startElection(coordinatorId);
    }

    // =================================================================================
    // Two-phase broadcast — coordinator side
    // =================================================================================

    private void onUpdateRequest(UpdateRequest req, ActorRef sender) {
        if (crashed || !isCoordinator()) return;
        requestQueue.add(req);
        processNextRequest();
    }

    private void processNextRequest() {
        if (broadcasting || requestQueue.isEmpty() || inElection) return;
        UpdateRequest req = requestQueue.remove(0);
        broadcasting = true;

        UpdateId uid = new UpdateId(epoch, nextSeq++);
        UpdateRecord rec = new UpdateRecord(uid, req.index, req.value, req.originId, req.client, req.rid);

        pendingUpdates.put(uid, rec);
        updateMostRecentObserved(uid);
        Set<Integer> acks = new HashSet<>();
        acks.add(id); // coordinator counts itself
        ackCounts.put(uid, acks);

        broadcast(new Update(rec));
        // If we are the whole quorum already (e.g. only survivor), commit now.
        maybeCommit(uid);
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
        // Apply locally and tell everyone to apply.
        applyUpdate(pendingUpdates.get(uid));
        broadcast(new WriteOk(uid));
        if (maybeCrashBefore(Crash.Type.WriteOK)) return;
        broadcasting = false;
        processNextRequest();
    }

    // =================================================================================
    // Two-phase broadcast — replica side
    // =================================================================================

    private void onUpdate(Update msg) {
        if (crashed) return;
        if (maybeCrashBefore(Crash.Type.Update)) return; // crash on receiving an UPDATE
        UpdateRecord rec = msg.record;
        // Treat the UPDATE as a sign of coordinator liveness.
        if (rec.id.epoch >= epoch) {
            this.epoch = rec.id.epoch;
        }
        if (!isCoordinator()) armHeartbeatTimeout();

        if (!appliedIds.contains(rec.id) && !pendingUpdates.containsKey(rec.id)) {
            pendingUpdates.put(rec.id, rec);
            updateMostRecentObserved(rec.id);
        }
        // ACK back to the coordinator.
        ActorRef coord = group.get(coordinatorId);
        if (coord != null) tell(new UpdateAck(rec.id, id), coord);

        // Arm a timeout: if no WRITEOK arrives, suspect the coordinator.
        cancel(updateTimeouts.remove(rec.id));
        updateTimeouts.put(rec.id, scheduleOnce(new UpdateTimeout(rec.id), coordinatorTimeoutDelay()));
    }

    private void onWriteOk(WriteOk msg) {
        if (crashed) return;
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
        startElection(coordinatorId);
    }

    /** Apply an update to local state exactly once, in id order, and notify. */
    private void applyUpdate(UpdateRecord rec) {
        if (rec == null || appliedIds.contains(rec.id)) return;
        positions[rec.index] = rec.value;
        appliedIds.add(rec.id);
        pendingUpdates.remove(rec.id);
        history.add(rec);
        updateMostRecentObserved(rec.id);
        callbackOnUpdateApplied(rec.index, rec.value);

        // If this replica originally received the client write, ack the client now.
        if (rec.originId == id && rec.client != null && pendingClientWrites.containsKey(rec.rid)) {
            pendingClientWrites.remove(rec.rid);
            cancel(writeForwardTimeouts.remove(rec.rid));
            tell(new WriteReply(rec.rid, rec.index, rec.value, id), rec.client);
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
        broadcasting = false;
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
            // Wrapped around the whole ring without an ack: every other replica is
            // unreachable. If this is our own (undecided) token, we win by default.
            if (!pendingElectionMsg.decided) {
                becomeCoordinatorIfWinner(decideWinner(pendingElectionMsg.candidates), pendingElectionMsg);
            }
            expectedAckFrom = -1;
            return;
        }
        int targetId = ring.get((myIdx + pendingElectionTargetOffset) % n);
        expectedAckFrom = targetId;
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
        // Acknowledge the forwarder immediately so it does not skip us.
        // NetworkChannel preserves the original sender, so getSender() is the replica.
        ActorRef forwarder = getSender();
        if (forwarder != null) tell(new ElectionAck(id), forwarder);

        // Drop stale tokens from an election we have already resolved.
        if (!inElection) {
            if (token.crashedCoordinatorId != coordinatorId) return; // already moved on
            beginElectionParticipation(token.crashedCoordinatorId);
        }

        if (maybeCrashBefore(Crash.Type.Election)) return;

        if (token.decided) {
            handleDecidedToken(token);
            return;
        }

        if (token.candidates.containsKey(id)) {
            // The token has visited every reachable replica: decide.
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

        // Complete any interrupted update we observed but never applied, so that the
        // safety property holds, then publish our full log to everyone.
        completePendingUpdates();
        List<UpdateRecord> fullLog = new ArrayList<>(history);

        this.nextSeq = 0;
        finishElection();
        callbackOnCoordinatorElected(id);

        broadcast(new Synchronization(id, epoch, fullLog));
        startHeartbeating();

        // Re-issue any client writes we still owe, and resume queued work.
        resubmitOwnPendingWrites();
        processNextRequest();
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

    private void onSynchronization(Synchronization sync) {
        if (crashed) return;
        // Apply any updates we are missing, in order.
        for (UpdateRecord rec : sync.log) {
            if (!appliedIds.contains(rec.id)) {
                applyUpdate(rec);
            }
        }
        this.coordinatorId = sync.newCoordinatorId;
        this.epoch = sync.newEpoch;
        finishElection();
        callbackOnCoordinatorElected(sync.newCoordinatorId);
        armHeartbeatTimeout();

        // Re-send any client writes that never committed to the new coordinator.
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
