package it.unitn.ds.custom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.ElectionStarted;
import it.unitn.ds.AbstractReplica.UpdateApplied;
import it.unitn.ds.Client;
import it.unitn.ds.Replica;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

/**
 * Failover recovery invariants:
 * <ul>
 *   <li>{@code staleAfterSync}: only pending updates the new coordinator's log
 *       does not vouch for are purged at synchronization (the purge is what
 *       prevents a later election winner from re-applying superseded updates
 *       — Integrity would be violated).</li>
 *   <li>An update interrupted mid-broadcast is completed by the new
 *       coordinator and applied EXACTLY ONCE on every surviving replica.</li>
 * </ul>
 */
public class FailoverRecovery {

    // =========================================================================
    // staleAfterSync (pure function)
    // =========================================================================

    private static Replica.UpdateRecord rec(int epoch, int seq) {
        return new Replica.UpdateRecord(new Replica.UpdateId(epoch, seq), 0, 1, 0, null, 0);
    }

    @Test
    void staleAfterSyncPurgesExactlyTheUnvouchedIds() {
        List<Replica.UpdateRecord> syncLog = List.of(rec(0, 0), rec(0, 1));

        Set<Replica.UpdateId> pending = new HashSet<>();
        pending.add(new Replica.UpdateId(0, 1)); // vouched by the log — keep
        pending.add(new Replica.UpdateId(0, 5)); // dead-epoch leftover — purge
        pending.add(new Replica.UpdateId(0, 9)); // dead-epoch leftover — purge

        Set<Replica.UpdateId> stale = Replica.staleAfterSync(pending, syncLog);

        assertEquals(Set.of(new Replica.UpdateId(0, 5), new Replica.UpdateId(0, 9)), stale,
                "exactly the ids absent from the sync log must be purged");
    }

    @Test
    void staleAfterSyncWithEmptyLogPurgesEverything() {
        Set<Replica.UpdateId> pending = Set.of(new Replica.UpdateId(0, 0), new Replica.UpdateId(0, 1));
        assertEquals(pending, Replica.staleAfterSync(pending, List.of()),
                "with an empty authoritative log every pending update is stale");
    }

    // =========================================================================
    // Interrupted update completed exactly once (integration)
    // =========================================================================

    @Test
    void interruptedUpdateIsCompletedExactlyOnceOnEverySurvivor() {
        final int N = 5;
        final int INDEX = 7, VALUE = 77;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem("interruptedUpdate", N, 0);

        TestKit clientProbe = new TestKit(sys.system);
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(1)), clientProbe.getRef()),
                "client");

        // The coordinator sends the UPDATE to exactly ONE replica, then crashes:
        // the interrupted broadcast must be detected and completed by the new
        // coordinator (safety: an observed update acked into an election win
        // must reach everyone).
        sys.actors.get(0).tell(new Crash(Crash.Type.Update, 1), Actor.noSender());
        client.tell(new AbstractClient.WriteRequest(INDEX, VALUE), Actor.noSender());

        long window = TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys);
        WriteResult wr = (WriteResult) clientProbe.fishForMessage(
                Duration.ofMillis(window), "WriteResult", m -> m instanceof WriteResult);
        assertTrue(wr.success, "the interrupted write must eventually commit");

        // Let synchronization finish everywhere, then drain each survivor's
        // probe: the update must have been applied EXACTLY once per replica.
        try {
            Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (int i = 1; i < N; i++) {
            int applied = 0;
            while (true) {
                try {
                    Object m = sys.probes.get(i).expectMsgAnyClassOf(
                            Duration.ofMillis(400),
                            UpdateApplied.class, ElectionStarted.class,
                            CoordinatorElected.class, AbstractReplica.Crash.class);
                    if (m instanceof UpdateApplied) {
                        UpdateApplied ua = (UpdateApplied) m;
                        assertEquals(INDEX, ua.index);
                        assertEquals(VALUE, ua.value);
                        applied++;
                    }
                } catch (AssertionError drained) {
                    break;
                }
            }
            assertEquals(1, applied,
                    "replica " + i + " must apply the interrupted update exactly once");
        }

        // The value must be visible on every survivor.
        for (int i = 1; i < N; i++) {
            client.tell(new AbstractClient.ReadRequest(INDEX, sys.actors.get(i)), Actor.noSender());
            ReadResult rr = (ReadResult) clientProbe.fishForMessage(
                    Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys) + 500),
                    "ReadResult", m -> m instanceof ReadResult);
            assertTrue(rr.success);
            assertEquals(VALUE, rr.value.intValue(), "replica " + i + " must hold the completed value");
        }

        sys.system.terminate();
    }
}
