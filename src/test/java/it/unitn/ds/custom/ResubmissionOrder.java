package it.unitn.ds.custom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

/**
 * Spec §1: "All operations issued by one client to a specific replica are
 * observed in the same order in which they were sent."
 *
 * If the coordinator dies before broadcasting any of them, every write a
 * replica forwarded stays pending and is resubmitted to the new coordinator
 * after the election. That resubmission must preserve the client's send
 * order, otherwise the new coordinator commits the writes inverted.
 *
 * Repeated because the defect (iteration order of an unordered map) depends
 * on the client ActorRef's hash, which changes per run.
 */
public class ResubmissionOrder {

    private static final int N_NODES = 5;
    private static final int NUM_WRITES = 8;
    private static final int INDEX = 5;

    @RepeatedTest(4)
    void pendingWritesResubmittedInClientSendOrder(RepetitionInfo rep) {
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "resubmissionOrder" + rep.getCurrentRepetition(), N_NODES, 0);
        TestKit probe = new TestKit(sys.system);
        ActorRef origin = sys.actors.get(1);
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(origin), probe.getRef()),
                "client");

        // The coordinator dies before broadcasting the FIRST update: all the
        // writes below stay pending on the origin until after the election.
        sys.actors.get(0).tell(new Crash(Crash.Type.Update, 0), Actor.noSender());

        for (int v = 1; v <= NUM_WRITES; v++) {
            client.tell(new AbstractClient.WriteRequest(INDEX, v), Actor.noSender());
        }

        // All writes must eventually commit (first one only after failover),
        // and their results must come back in client send order.
        long window = TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys);
        List<Integer> committed = new ArrayList<>();
        for (int i = 0; i < NUM_WRITES; i++) {
            WriteResult wr = (WriteResult) probe.fishForMessage(
                    Duration.ofMillis(window), "WriteResult", m -> m instanceof WriteResult);
            assertTrue(wr.success, "every pending write must eventually commit");
            committed.add(wr.value);
            window = TestsCommons.getMaxUpdateDelay(sys) * 2L; // rest commit quickly
        }
        for (int v = 1; v <= NUM_WRITES; v++) {
            assertEquals(v, committed.get(v - 1).intValue(),
                    "writes from one client must commit in send order; got " + committed);
        }

        // The final value everywhere must be the LAST write.
        client.tell(new AbstractClient.ReadRequest(INDEX, sys.actors.get(2)), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys) + 500),
                "ReadResult", m -> m instanceof ReadResult);
        assertEquals(NUM_WRITES, rr.value.intValue(),
                "the last write in client order must be the final value");

        sys.system.terminate();
    }
}
