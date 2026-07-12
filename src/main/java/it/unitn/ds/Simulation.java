package it.unitn.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.InitSystem;

/**
 * Annotated simulation of every protocol concept, with debug logging enabled
 * so the logs narrate the algorithm step by step. One isolated actor system
 * per scenario. Concept numbers in the banners refer to the viva study guide.
 *
 * Run with:
 *   ./gradlew run -PmainClass=it.unitn.ds.Simulation             (all scenarios)
 *   ./gradlew run -PmainClass=it.unitn.ds.Simulation --args="F"  (a single one, A–I)
 * (plain `./gradlew run` still runs the presentation demo in Main)
 */
public class Simulation {

    private static final int N = 5;
    private static final int COORD = 0;
    private static final long READ_TO = 3_000;
    private static final long WRITE_TO = 25_000;
    private static final long ELECTION_SETTLE_MS =
            AbstractReplica.COORDINATOR_BEAT_INTERVAL * 4L + 2_000;

    /** One isolated system per scenario: N replicas + on-demand clients. */
    private static final class Sim implements AutoCloseable {
        final ActorSystem system;
        final Map<Integer, ActorRef> replicas = new HashMap<>();

        Sim(String name, int n, int coordinatorId, int minLat, int maxLat) throws InterruptedException {
            system = ActorSystem.create(name);
            for (int i = 0; i < n; i++) {
                replicas.put(i, system.actorOf(
                        Replica.props(i, minLat, maxLat, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                        "Replica_" + i));
            }
            InitSystem init = new InitSystem(replicas, coordinatorId);
            for (ActorRef r : replicas.values()) r.tell(init, ActorRef.noSender());
            Thread.sleep(300);
        }

        Sim(String name) throws InterruptedException {
            this(name, N, COORD, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY);
        }

        ActorRef client(String name, int defaultReplica) {
            return system.actorOf(
                    Client.props(READ_TO, WRITE_TO, Optional.of(replicas.get(defaultReplica))), name);
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

    private static void note(String s) {
        Logger.log("--- " + s);
    }

    @FunctionalInterface
    private interface Scenario {
        void run() throws InterruptedException;
    }

    public static void main(String[] args) throws InterruptedException {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true); // debug lines narrate the protocol internals

        Map<String, Scenario> scenarios = new LinkedHashMap<>();
        scenarios.put("A", Simulation::writePath);              // concepts 1,2,3,4,5
        scenarios.put("B", Simulation::pipelinedTotalOrder);    // concepts 3,5
        scenarios.put("C", Simulation::sequentialConsistency);  // concepts 6,13
        scenarios.put("D", Simulation::clientTimeouts);         // concepts 7,8
        scenarios.put("E", Simulation::forwardTimeoutFailover); // concepts 8,10,12 (+2 validity)
        scenarios.put("F", Simulation::interruptedWriteOk);     // concepts 4,9,10,11
        scenarios.put("G", Simulation::crashDuringElection);    // concepts 9,5
        scenarios.put("H", Simulation::exactlyOncePerClient);   // concept 12
        scenarios.put("I", Simulation::malformedInput);         // concept 7 (crash-stop vs restart)

        if (args.length == 0) {
            for (Scenario s : scenarios.values()) s.run();
        } else {
            for (String arg : args) {
                Scenario s = scenarios.get(arg.trim().toUpperCase());
                if (s == null) {
                    Logger.log("Unknown scenario '" + arg + "' — valid: " + scenarios.keySet());
                } else {
                    s.run();
                }
            }
        }

        banner("SIMULATION COMPLETE");
    }

    /** [1,2,3,4,5] The undisturbed write path: forward -> UPDATE -> quorum ACK -> WRITEOK -> apply. */
    private static void writePath() throws InterruptedException {
        banner("SIM A [concepts 1-5]: write path — two-phase quorum broadcast");
        try (Sim s = new Sim("simWritePath")) {
            ActorRef a = s.client("clientA", 1);
            note("clientA writes (0, 42) through Replica 1; watch: forward, quorum 3/5, WRITEOK, applies");
            s.write(a, 0, 42);
            Thread.sleep(800);
            note("reads served locally by ANY replica (no coordination)");
            s.read(a, 0);
            s.readFrom(a, 0, 3);
            Thread.sleep(600);
        }
    }

    /** [3,5] Several updates in flight at once; commits stay in ⟨epoch:seq⟩ order everywhere. */
    private static void pipelinedTotalOrder() throws InterruptedException {
        banner("SIM B [concepts 3,5]: pipelined updates, one total order 0:0 .. 0:3");
        try (Sim s = new Sim("simPipeline")) {
            ActorRef a = s.client("clientA", 1);
            ActorRef b = s.client("clientB", 2);
            note("4 concurrent writes to index 1 from two clients; all replicas apply in the SAME order");
            s.write(a, 1, 101);
            s.write(b, 1, 202);
            s.write(a, 1, 103);
            s.write(b, 1, 204);
            Thread.sleep(1_200);
            note("both clients read the same final value (last update in the total order)");
            s.read(a, 1);
            s.read(b, 1);
            Thread.sleep(600);
        }
    }

    /** [6,13] High-latency network: reads can be STALE but never regress per replica. */
    private static void sequentialConsistency() throws InterruptedException {
        banner("SIM C [concepts 6,13]: sequential consistency — stale-but-monotone local reads");
        try (Sim s = new Sim("simSeqConsistency", N, COORD, 80, 150)) { // slow channels
            ActorRef a = s.client("clientA", 1);
            ActorRef b = s.client("clientB", 3);
            note("commit (2, 7) fully first");
            s.write(a, 2, 7);
            Thread.sleep(2_000);
            note("write (2, 8), then read from Replica 3 BEFORE its WRITEOK can arrive (~500ms away)");
            s.write(a, 2, 8);
            Thread.sleep(120);
            s.read(b, 2); // expected: still 7 — stale, but a legal, older prefix of the order
            Thread.sleep(2_000);
            note("read again after propagation: the value moved FORWARD in the order (7 -> 8), never back");
            s.read(b, 2); // expected: 8
            Thread.sleep(800);
        }
    }

    /** [7,8] Crash-stop replicas answer nothing; clients detect via their own timeouts. */
    private static void clientTimeouts() throws InterruptedException {
        banner("SIM D [concepts 7,8]: crashed replica — client-side timeout callbacks");
        try (Sim s = new Sim("simClientTimeout")) {
            ActorRef b = s.client("clientB", 2);
            s.crash(4, new Crash(Crash.Type.Now, 0));
            Thread.sleep(300);
            note("system keeps working through live replicas");
            s.write(b, 3, 30);
            Thread.sleep(900);
            note("a read sent to crashed Replica 4 gets no reply -> TIMEOUT callback after " + READ_TO + "ms");
            s.readFrom(b, 3, 4);
            Thread.sleep(READ_TO + 600);
        }
    }

    /** [8,10,12] Coordinator swallows a write: forward-timeout detection, election, resubmission. */
    private static void forwardTimeoutFailover() throws InterruptedException {
        banner("SIM E [concepts 8,10,12]: coordinator crashes BEFORE broadcasting — write survives");
        try (Sim s = new Sim("simForwardTimeout")) {
            ActorRef a = s.client("clientA", 1);
            note("coordinator will crash before sending any UPDATE for the next write");
            s.crash(COORD, new Crash(Crash.Type.Update, 0));
            s.write(a, 4, 44);
            note("origin's forward-timeout / heartbeat watchdog fires -> election -> origin RESUBMITS");
            Thread.sleep(ELECTION_SETTLE_MS);
            note("the write commits in the NEW epoch (applied update 1:0) — Validity preserved");
            for (int i = 1; i < N; i++) s.readFrom(a, 4, i);
            Thread.sleep(1_000);
        }
    }

    /** [4,9,10,11] Crash mid-WRITEOK: quorum intersection elects a knower; interrupted update completed. */
    private static void interruptedWriteOk() throws InterruptedException {
        banner("SIM F [concepts 4,9,10,11]: coordinator crashes during WRITEOK dissemination");
        try (Sim s = new Sim("simInterruptedWriteOk")) {
            ActorRef a = s.client("clientA", 1);
            note("coordinator applies (5, 55), sends WRITEOK to ONE replica, crashes");
            s.crash(COORD, new Crash(Crash.Type.WriteOK, 1));
            s.write(a, 5, 55);
            note("survivors detect the missing WRITEOK -> election -> most-recent-update replica wins");
            Thread.sleep(ELECTION_SETTLE_MS);
            note("every survivor must hold 55 (uniform agreement: applied-anywhere => applied-everywhere)");
            for (int i = 1; i < N; i++) s.readFrom(a, 5, i);
            Thread.sleep(1_000);

            note("[11] injecting DELAYED messages from the dead epoch-0 regime -> fenced (see debug)");
            s.replicas.get(2).tell(new Replica.Heartbeat(COORD, 0), ActorRef.noSender());
            s.replicas.get(2).tell(new Replica.Synchronization(COORD, 0, new ArrayList<>()), ActorRef.noSender());
            Thread.sleep(600);
            note("Replica 2 still serves the new regime's value");
            s.readFrom(a, 5, 2);
            Thread.sleep(600);
        }
    }

    /** [9,5] A replica dies on its first ELECTION message (no ACK) — the ring routes around it. */
    private static void crashDuringElection() throws InterruptedException {
        banner("SIM G [concepts 9,5]: crash during the election — ACK timeout skips the dead node");
        try (Sim s = new Sim("simElectionCrash")) {
            ActorRef b = s.client("clientB", 2);
            s.write(b, 6, 66);
            Thread.sleep(900);
            note("Replica 1 will crash on its first ELECTION message WITHOUT acking; coordinator dies now");
            s.crash(1, new Crash(Crash.Type.Election, 0));
            s.crash(COORD, new Crash(Crash.Type.Now, 0));
            Thread.sleep(ELECTION_SETTLE_MS);
            note("new coordinator elected among {2,3,4}; a write in the new epoch resets seq: 1:0");
            s.write(b, 7, 77);
            Thread.sleep(900);
            for (int i = 2; i < N; i++) s.readFrom(b, 7, i);
            Thread.sleep(800);
        }
    }

    /** [12] Two clients, same origin replica, SAME rid=0: kept apart by the ⟨client, rid⟩ key. */
    private static void exactlyOncePerClient() throws InterruptedException {
        banner("SIM H [concept 12]: two clients through ONE replica — colliding rids, both answered");
        try (Sim s = new Sim("simExactlyOnce")) {
            ActorRef x = s.client("clientX", 1);
            ActorRef y = s.client("clientY", 1);
            note("both first requests carry rid=0; origin keys them by (client, rid)");
            s.write(x, 8, 111);
            s.write(y, 9, 222);
            Thread.sleep(900);
            s.read(x, 8);
            s.read(y, 9);
            Thread.sleep(600);
        }
    }

    /** [7] Malformed indices: explicit failure replies; the replica is NOT restarted/wiped. */
    private static void malformedInput() throws InterruptedException {
        banner("SIM I [concept 7]: out-of-range index — explicit failure, state intact");
        try (Sim s = new Sim("simMalformed", 3, 0, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY)) {
            ActorRef a = s.client("clientA", 1);
            s.write(a, 0, 9);
            Thread.sleep(700);
            note("write to index -1 and read of index " + AbstractReplica.POSITIONS_LIST_LENGTH
                    + " are rejected with success=false");
            s.write(a, -1, 42);
            a.tell(new AbstractClient.ReadRequest(AbstractReplica.POSITIONS_LIST_LENGTH), ActorRef.noSender());
            Thread.sleep(600);
            note("the replica survived with its state intact (crash-stop model, no silent restart)");
            s.read(a, 0);
            Thread.sleep(600);
        }
    }
}
