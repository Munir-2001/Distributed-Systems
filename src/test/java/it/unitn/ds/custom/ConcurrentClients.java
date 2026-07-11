package it.unitn.ds.custom;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

/**
 * Additional tests beyond the provided base suite.
 */
public class ConcurrentClients {

    /**
     * Two different clients write through the SAME replica at the same time.
     * Client request ids are per-client counters, so both writes carry rid=0:
     * the replica must keep the two outstanding writes apart (keyed by
     * ⟨client, rid⟩) and answer BOTH clients.
     */
    @Test
    void twoClientsSameReplicaCollidingRequestIds() {
        final int N_NODES = 5;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "twoClientsSameReplica", N_NODES, 0);

        ActorRef origin = sys.actors.get(1); // non-coordinator origin for both

        TestKit probeA = new TestKit(sys.system);
        TestKit probeB = new TestKit(sys.system);
        ActorRef clientA = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(origin), probeA.getRef()),
                "clientA");
        ActorRef clientB = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(origin), probeB.getRef()),
                "clientB");

        // Both first writes carry rid = 0.
        clientA.tell(new AbstractClient.WriteRequest(1, 111), Actor.noSender());
        clientB.tell(new AbstractClient.WriteRequest(2, 222), Actor.noSender());

        long window = TestsCommons.getMaxUpdateDelay(sys) * 2L;
        WriteResult wrA = (WriteResult) probeA.fishForMessage(
                Duration.ofMillis(window), "WriteResult A", m -> m instanceof WriteResult);
        WriteResult wrB = (WriteResult) probeB.fishForMessage(
                Duration.ofMillis(window), "WriteResult B", m -> m instanceof WriteResult);

        // NOTE: compare fields, not objects: AbstractClient.Result.equals uses
        // == on the boxed Integer value, which fails outside the [-128, 127]
        // Integer cache (base-class issue, reported to the instructors).
        assertEquals(true, wrA.success, "client A write must succeed");
        assertEquals(1, wrA.index, "client A must get the result of ITS write (index)");
        assertEquals(111, (int) wrA.value, "client A must get ITS written value");
        assertEquals(true, wrB.success, "client B write must succeed");
        assertEquals(2, wrB.index, "client B must get the result of ITS write (index)");
        assertEquals(222, (int) wrB.value, "client B must get ITS written value");

        sys.system.terminate();
    }
}
