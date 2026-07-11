package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

/**
 * External client actor.
 *
 * A client may contact any replica to issue read or write requests. For each
 * outstanding request the client arms a timeout; if no reply is received in
 * time the matching {@code callbackOn*Timeout} is fired. Replies are matched to
 * the original request through a unique request id ({@code rid}).
 */
public class Client extends AbstractClient {

    // ===== Outgoing request messages (client -> replica) =====

    /** Read the value stored at {@code index}. Replied to with {@link Replica.ReadReply}. */
    public static class ReadReq implements Serializable {
        public final long rid;
        public final int index;
        public ReadReq(long rid, int index) {
            this.rid = rid;
            this.index = index;
        }
    }

    /** Set position[{@code index}] = {@code value}. Replied to with {@link Replica.WriteReply}. */
    public static class WriteReq implements Serializable {
        public final long rid;
        public final int index;
        public final int value;
        public WriteReq(long rid, int index, int value) {
            this.rid = rid;
            this.index = index;
            this.value = value;
        }
    }

    // ===== Internal timeout self-messages =====

    private static class ReadTO implements Serializable {
        final long rid;
        ReadTO(long rid) { this.rid = rid; }
    }

    private static class WriteTO implements Serializable {
        final long rid;
        WriteTO(long rid) { this.rid = rid; }
    }

    // ===== Bookkeeping for outstanding requests =====

    private static class Pending {
        final ActorRef replica;
        final int index;
        final int value; // only meaningful for writes
        final Cancellable timeout;
        Pending(ActorRef replica, int index, int value, Cancellable timeout) {
            this.replica = replica;
            this.index = index;
            this.value = value;
            this.timeout = timeout;
        }
    }

    private long nextRid = 0;
    private final Map<Long, Pending> pendingReads = new HashMap<>();
    private final Map<Long, Pending> pendingWrites = new HashMap<>();

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    private Cancellable scheduleOnce(Serializable msg, long delayMs) {
        return getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(delayMs, TimeUnit.MILLISECONDS),
                getSelf(), msg, getContext().getDispatcher(), getSelf());
    }

    // =================================================================================
    // Abstract methods
    // =================================================================================

    @Override
    public void sendRead(ActorRef replica, int index) {
        final long rid = nextRid++;
        log("requesting READ (" + index + ") to " + replica.path().name());
        Cancellable to = scheduleOnce(new ReadTO(rid), getReadTimeoutDelay());
        pendingReads.put(rid, new Pending(replica, index, 0, to));
        replica.tell(new ReadReq(rid, index), getSelf());
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        final long rid = nextRid++;
        log("requesting WRITE (" + index + ", " + value + ") to " + replica.path().name());
        Cancellable to = scheduleOnce(new WriteTO(rid), getWriteTimeoutDelay());
        pendingWrites.put(rid, new Pending(replica, index, value, to));
        replica.tell(new WriteReq(rid, index, value), getSelf());
    }

    // =================================================================================
    // Reply / timeout handlers
    // =================================================================================

    private void onReadReply(Replica.ReadReply msg) {
        Pending p = pendingReads.remove(msg.rid);
        if (p == null) return; // already timed out / duplicate
        p.timeout.cancel();
        callbackOnReadResult(new ReadResult(msg.success, msg.index, msg.value, msg.replicaId));
    }

    private void onWriteReply(Replica.WriteReply msg) {
        Pending p = pendingWrites.remove(msg.rid);
        if (p == null) return;
        p.timeout.cancel();
        callbackOnWriteResult(new WriteResult(msg.success, msg.index, msg.value, msg.replicaId));
    }

    private void onReadTO(ReadTO msg) {
        Pending p = pendingReads.remove(msg.rid);
        if (p == null) return;
        callbackOnReadTimeout(new ReadTimeout(getSelf(), p.replica, p.index));
    }

    private void onWriteTO(WriteTO msg) {
        Pending p = pendingWrites.remove(msg.rid);
        if (p == null) return;
        callbackOnWriteTimeout(new WriteTimeout(getSelf(), p.replica, p.index, p.value));
    }

    @Override
    public Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(Replica.ReadReply.class, this::onReadReply)
                .match(Replica.WriteReply.class, this::onWriteReply)
                .match(ReadTO.class, this::onReadTO)
                .match(WriteTO.class, this::onWriteTO)
                .build();
    }
}
