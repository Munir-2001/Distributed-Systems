package it.unitn.ds.custom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

/**
 * An out-of-range index must be answered with an explicit failure, not an
 * exception: Akka's default supervision RESTARTS a throwing actor with fresh
 * state, which would silently wipe positions[] on the replica (and, for a
 * malformed write reaching WRITEOK, on every replica). Replicas fail by
 * crashing and do not recover — they must never be resurrected blank.
 */
public class IndexValidation {

    @Test
    void malformedIndicesAreRejectedWithoutStateLoss() {
        final TestsSystemWrapper sys = TestsCommons.createTestSystem("indexValidation", 3, 0);
        TestKit probe = new TestKit(sys.system);
        ActorRef replica = sys.actors.get(1);
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(2_000, 4_000, Optional.of(replica), probe.getRef()),
                "client");

        long writeWindow = TestsCommons.getMaxUpdateDelay(sys);
        long readWindow = TestsCommons.getLatencyPlusEpsilon(sys) + 3_000;

        // Establish prior state the malformed requests must not destroy.
        client.tell(new AbstractClient.WriteRequest(0, 9), Actor.noSender());
        WriteResult w0 = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(writeWindow), "WriteResult", m -> m instanceof WriteResult);
        assertTrue(w0.success);

        // Out-of-range READ: explicit failure result, not a timeout.
        client.tell(new AbstractClient.ReadRequest(AbstractReplica.POSITIONS_LIST_LENGTH), Actor.noSender());
        ReadResult badRead = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(readWindow), "ReadResult", m -> m instanceof ReadResult);
        assertFalse(badRead.success, "an out-of-range read must fail explicitly");

        // Out-of-range WRITE: explicit failure result, never broadcast.
        client.tell(new AbstractClient.WriteRequest(-1, 42), Actor.noSender());
        WriteResult badWrite = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(writeWindow + 5_000), "WriteResult", m -> m instanceof WriteResult);
        assertFalse(badWrite.success, "an out-of-range write must fail explicitly");

        // The replica must still be alive with its state INTACT (no restart wipe).
        client.tell(new AbstractClient.ReadRequest(0), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(readWindow), "ReadResult", m -> m instanceof ReadResult);
        assertTrue(rr.success);
        assertEquals(9, rr.value.intValue(),
                "state written before the malformed requests must survive them");

        // And it must still serve new writes.
        client.tell(new AbstractClient.WriteRequest(1, 5), Actor.noSender());
        WriteResult w1 = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(writeWindow), "WriteResult", m -> m instanceof WriteResult);
        assertTrue(w1.success, "the replica must keep serving valid requests");

        sys.system.terminate();
    }
}
