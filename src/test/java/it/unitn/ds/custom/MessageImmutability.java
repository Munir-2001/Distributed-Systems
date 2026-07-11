package it.unitn.ds.custom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import it.unitn.ds.Replica;

/**
 * Codebase rule (spec §2): "Shared mutable state is forbidden; any shared
 * objects must be immutable." Election tokens and Synchronization messages
 * carry collections and the very same instance is sent to several actors
 * (broadcast / retained for ring-skip retransmission), so their contents must
 * be unmodifiable and defensively copied.
 */
public class MessageImmutability {

    @Test
    void electionCandidatesAreImmutableAndDefensivelyCopied() {
        Map<Integer, Replica.UpdateId> candidates = new HashMap<>();
        candidates.put(1, new Replica.UpdateId(0, 0));
        Replica.Election election = new Replica.Election(0, candidates, false, -1);

        assertThrows(UnsupportedOperationException.class,
                () -> election.candidates.put(2, new Replica.UpdateId(0, 1)),
                "Election.candidates must be unmodifiable");

        // Mutating the source map after construction must not leak into the message.
        candidates.put(9, new Replica.UpdateId(0, 9));
        assertFalse(election.candidates.containsKey(9),
                "Election must defensively copy the candidates map");
    }

    @Test
    void synchronizationLogIsImmutableAndDefensivelyCopied() {
        Replica.UpdateRecord rec = new Replica.UpdateRecord(
                new Replica.UpdateId(0, 0), 0, 1, 0, null, 0);
        List<Replica.UpdateRecord> log = new ArrayList<>();
        log.add(rec);
        Replica.Synchronization sync = new Replica.Synchronization(1, 1, log);

        assertThrows(UnsupportedOperationException.class,
                () -> sync.log.add(rec),
                "Synchronization.log must be unmodifiable");

        log.add(new Replica.UpdateRecord(new Replica.UpdateId(0, 1), 0, 2, 0, null, 1));
        assertEquals(1, sync.log.size(),
                "Synchronization must defensively copy the log");
    }
}
