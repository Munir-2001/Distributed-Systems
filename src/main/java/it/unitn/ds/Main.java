package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.InitSystem;

/**
 * Demo entry point. Each scenario runs in its own actor system:
 *
 * <ol>
 *   <li>Normal operation: writes and reads, including two clients writing
 *       concurrently through different replicas (total order).</li>
 *   <li>Crash of a non-coordinator replica: the system keeps serving
 *       requests; a client contacting the dead replica observes timeouts.</li>
 *   <li>Coordinator crash in the middle of the WRITEOK dissemination: only
 *       one replica learns the outcome; the survivors detect the failure,
 *       elect the most up-to-date replica, and the new coordinator completes
 *       the interrupted update (safety property).</li>
 *   <li>Crash during the election: a replica dies upon receiving its first
 *       ELECTION message without acknowledging it; the ring routes around it
 *       and the election still completes.</li>
 * </ol>
 */
public class Main {

    private static final int N_REPLICAS = 5;
    private static final int COORDINATOR_ID = 0;

    // Generous client timeouts: a write issued right before a coordinator
    // crash must survive failure detection + election + synchronization.
    private static final long CLIENT_READ_TIMEOUT_MS = 3_000;
    private static final long CLIENT_WRITE_TIMEOUT_MS = 20_000;

    // Rough upper bound on detection + election + synchronization time.
    private static final long ELECTION_SETTLE_MS =
            AbstractReplica.COORDINATOR_BEAT_INTERVAL * 4L + 2_000;

    /** Per-scenario harness: one actor system, N replicas, two clients. */
    private static final class Scenario implements AutoCloseable {
        final ActorSystem system;
        final Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        final ActorRef clientA; // default target: replica 1
        final ActorRef clientB; // default target: replica 2

        Scenario(String name) throws InterruptedException {
            system = ActorSystem.create(name);
            for (int i = 0; i < N_REPLICAS; i++) {
                replicas.put(i, system.actorOf(
                        Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                                AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                        "Replica_" + i));
            }
            InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
            for (ActorRef replica : replicas.values()) {
                replica.tell(initMsg, ActorRef.noSender());
            }
            clientA = system.actorOf(
                    Client.props(CLIENT_READ_TIMEOUT_MS, CLIENT_WRITE_TIMEOUT_MS, Optional.of(replicas.get(1))),
                    "clientA");
            clientB = system.actorOf(
                    Client.props(CLIENT_READ_TIMEOUT_MS, CLIENT_WRITE_TIMEOUT_MS, Optional.of(replicas.get(2))),
                    "clientB");
            Thread.sleep(300); // let the system settle after init
        }

        void write(ActorRef client, int index, int value) {
            client.tell(new AbstractClient.WriteRequest(index, value), ActorRef.noSender());
        }

        void read(ActorRef client, int index) {
            client.tell(new AbstractClient.ReadRequest(index), ActorRef.noSender());
        }

        void readFrom(ActorRef client, int index, int replicaId) {
            client.tell(new AbstractClient.ReadRequest(index, replicas.get(replicaId)), ActorRef.noSender());
        }

        void crash(int replicaId, Crash how) {
            replicas.get(replicaId).tell(how, ActorRef.noSender());
        }

        @Override
        public void close() {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().join();
        }
    }

    private static void banner(String title) {
        Logger.log("");
        Logger.log("================================================================");
        Logger.log("  " + title);
        Logger.log("================================================================");
    }

    public static void main(String[] args) throws InterruptedException {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(false);

        scenarioNormalOperation();
        scenarioNonCoordinatorCrash();
        scenarioCoordinatorCrashDuringWriteOk();
        scenarioCrashDuringElection();

        banner("DEMO COMPLETE");
    }

    /** Basic writes and reads; concurrent writes from two clients (total order). */
    private static void scenarioNormalOperation() throws InterruptedException {
        banner("SCENARIO 1: normal operation (writes, reads, total order)");
        try (Scenario s = new Scenario("scenario1")) {
            s.write(s.clientA, 0, 42);
            Thread.sleep(600);
            s.read(s.clientA, 0);
            s.read(s.clientB, 0);
            Thread.sleep(600);

            Logger.log("--- concurrent writes to index 1 from two clients ---");
            s.write(s.clientA, 1, 100);
            s.write(s.clientB, 1, 200);
            Thread.sleep(1_000);
            s.read(s.clientA, 1);
            s.read(s.clientB, 1); // both must read the same (last in total order) value
            Thread.sleep(600);
        }
    }

    /** A non-coordinator replica crashes: service continues, dead replica times out. */
    private static void scenarioNonCoordinatorCrash() throws InterruptedException {
        banner("SCENARIO 2: non-coordinator replica crashes; system keeps working");
        try (Scenario s = new Scenario("scenario2")) {
            s.crash(4, new Crash(Crash.Type.Now, 0));
            Thread.sleep(300);
            s.write(s.clientA, 2, 7);
            Thread.sleep(1_000);
            s.read(s.clientB, 2);

            Logger.log("--- a read sent to the crashed replica must time out ---");
            s.readFrom(s.clientB, 2, 4);
            Thread.sleep(CLIENT_READ_TIMEOUT_MS + 500);
        }
    }

    /**
     * The coordinator applies an update, sends WRITEOK to a single replica and
     * crashes: survivors detect the missing WRITEOK, elect the most up-to-date
     * replica, and the new coordinator completes the interrupted update.
     */
    private static void scenarioCoordinatorCrashDuringWriteOk() throws InterruptedException {
        banner("SCENARIO 3: coordinator crashes during WRITEOK dissemination -> election");
        try (Scenario s = new Scenario("scenario3")) {
            s.crash(COORDINATOR_ID, new Crash(Crash.Type.WriteOK, 1));
            s.write(s.clientA, 3, 33);
            Thread.sleep(ELECTION_SETTLE_MS);

            Logger.log("--- after the election: value must be visible on every surviving replica ---");
            for (int i = 1; i < N_REPLICAS; i++) {
                s.readFrom(s.clientB, 3, i);
            }
            Thread.sleep(1_000);
        }
    }

    /**
     * A replica crashes upon receiving its first ELECTION message, without
     * acknowledging it: the forwarder times out waiting for the ACK, skips the
     * dead replica, and the election completes among the survivors.
     */
    private static void scenarioCrashDuringElection() throws InterruptedException {
        banner("SCENARIO 4: a replica crashes during the election; ring skips it");
        try (Scenario s = new Scenario("scenario4")) {
            // Give the system a non-trivial history first.
            s.write(s.clientB, 4, 55);
            Thread.sleep(800);

            // Replica 1 dies as soon as the first ELECTION message reaches it;
            // the coordinator crashes now -> election among {1..4} minus 1.
            s.crash(1, new Crash(Crash.Type.Election, 0));
            s.crash(COORDINATOR_ID, new Crash(Crash.Type.Now, 0));
            Thread.sleep(ELECTION_SETTLE_MS);

            Logger.log("--- new coordinator elected among survivors; system must still serve requests ---");
            s.write(s.clientB, 5, 99);
            Thread.sleep(1_000);
            for (int i = 2; i < N_REPLICAS; i++) {
                s.readFrom(s.clientB, 5, i);
            }
            Thread.sleep(1_000);
        }
    }
}
