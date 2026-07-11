package it.unitn.ds.custom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.Replica;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

/**
 * Regime fencing: coordinatorship is identified by the pair
 * ⟨epoch, coordinatorId⟩, compared lexicographically. Announcements and
 * broadcasts of a stale regime must be ignored, and on equal epochs the
 * higher coordinator id must deterministically win — otherwise two
 * concurrent election winners minting the same epoch make replicas
 * flip-flop between coordinators (split brain).
 */
public class RegimeFencing {

    @Test
    void staleRegimeComparesEpochThenCoordinatorIdLexicographically() {
        // current regime: epoch 1, coordinator 3
        assertTrue(Replica.staleRegime(1, 3, 0, 9), "older epoch is stale regardless of id");
        assertTrue(Replica.staleRegime(1, 3, 1, 2), "equal epoch, lower id is stale");
        assertFalse(Replica.staleRegime(1, 3, 1, 3), "the installed regime itself is not stale");
        assertFalse(Replica.staleRegime(1, 3, 1, 4), "equal epoch, higher id wins deterministically");
        assertFalse(Replica.staleRegime(1, 3, 2, 0), "newer epoch wins regardless of id");
    }

    @Test
    void staleRegimeAnnouncementsAndBroadcastsAreIgnored() {
        final TestsSystemWrapper sys = TestsCommons.createTestSystem("regimeFencing", 3, 0);
        TestKit probe = sys.probes.get(2); // replica 2's listener
        ActorRef r2 = sys.actors.get(2);

        // 1) A Synchronization for a newer regime (epoch 1, coordinator 1) is installed.
        r2.tell(new Replica.Synchronization(1, 1, new ArrayList<>()), Actor.noSender());
        CoordinatorElected ce = (CoordinatorElected) probe.fishForMessage(
                Duration.ofMillis(2_000), "", m -> m instanceof CoordinatorElected);
        assertEquals(1, ce.newCoordinatorId);

        // 2) An equal-epoch announcement from a LOWER-id coordinator must be
        //    rejected; from a higher-id one it must win. If the lower-id one
        //    leaked through, the next observed event would be CoordinatorElected(0).
        r2.tell(new Replica.Synchronization(0, 1, new ArrayList<>()), Actor.noSender());
        r2.tell(new Replica.Synchronization(2, 1, new ArrayList<>()), Actor.noSender());
        ce = (CoordinatorElected) probe.fishForMessage(
                Duration.ofMillis(2_000), "", m -> m instanceof CoordinatorElected);
        assertEquals(2, ce.newCoordinatorId,
                "an equal-epoch Synchronization from a lower-id coordinator must be rejected");

        // 3) An UPDATE stamped with an older epoch must be neither observed nor
        //    acknowledged: the sender must receive no UpdateAck.
        TestKit staleSender = new TestKit(sys.system);
        Replica.UpdateRecord staleRec = new Replica.UpdateRecord(
                new Replica.UpdateId(0, 9), 3, 30, 0, null, 0);
        r2.tell(new Replica.Update(staleRec), staleSender.getRef());
        staleSender.expectNoMessage(Duration.ofMillis(1_000));

        sys.system.terminate();
    }
}
