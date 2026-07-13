# Report notes — design decisions, assumptions, and known limitations

Working notes to fold into the LaTeX report (`ds1_project_2026_report_template`).
Section references: code-review items of 2026-07-11.

## Design decisions to describe in the report

- **Pipelined two-phase broadcast.** The coordinator does not serialize one
  update at a time: several ⟨UPDATE → quorum ACK → WRITEOK⟩ rounds may be in
  flight. Correctness argument (worth a paragraph in the report): channels are
  FIFO, so every replica receives UPDATEs — and later WRITEOKs — in sequence
  order and ACKs in receive order; therefore the coordinator necessarily
  collects a quorum for update *s* before it can complete one for *s+1*, and
  commits happen in sequence order. Pipelining also keeps the "coordinator did
  not initiate the broadcast in time" client-side timeout meaningful under
  bursts of writes (no queueing delay that could cause false suspicion).
- **Forward-timeout semantics.** The origin replica's write-forward timeout is
  satisfied exactly when it observes the coordinator *initiate the broadcast*
  (receives the UPDATE for its own request), per spec; from that point the
  per-update WRITEOK watchdog takes over.
- **Regime fencing (review item 5).** Coordinatorship is identified by the
  pair ⟨epoch, coordinatorId⟩, compared lexicographically (`staleRegime`).
  Heartbeats, Synchronizations and (by epoch) UPDATEs of a strictly smaller
  pair are ignored; on equal epochs the higher coordinator id wins
  deterministically, so two concurrent election winners that mint the same
  epoch cannot make replicas flip-flop (split brain). Residual limitation: a
  deposed same-epoch winner may have applied its own completed-pending updates
  during its brief reign; these are superseded by subsequent committed writes
  to the same indices (bounded inconsistency window in an execution that
  already violates the accurate-detection assumption).
- **Purge at synchronization (review item 2).** The new coordinator's log is
  the authoritative initial state of the new epoch: pending updates it does
  not vouch for are purged (`staleAfterSync`), their client writes are
  resubmitted by their origins. Quorum-intersection guarantees every
  *committed* update appears in the log, so nothing durable is ever purged.
- **Per-client request identity.** Outstanding writes are keyed by
  ⟨client, rid⟩ (rids are per-client counters) in a `LinkedHashMap`, so
  failover resubmission preserves each client's send order (sequential
  consistency across coordinator changes — review item 1).
- **Malformed client input (review item 4).** Indices outside
  `[0, POSITIONS_LIST_LENGTH)` are answered with an explicit failure reply and
  never enter the update protocol. `application.conf` installs Akka's
  `StoppingSupervisorStrategy`: an unexpected exception stops the actor
  (crash-stop) instead of restarting it with wiped state, matching the model
  "replicas fail by crashing and do not recover".

## Assumptions / accepted limitations (state these in the report)

- **Item 6 — single in-flight election token per replica.** A replica retries
  (skip-on-timeout) only its most recent forward; a token overwritten while
  awaiting its ACK is no longer retried and can die if the next hop crashed.
  Any surviving token still circulates the full ring and reaches the same
  decision; total token loss is recovered by the election restart timer
  (4 × coordinator timeout). Liveness-only concern; keyed per-token retry
  state would remove it.
- **Item 9 — decided token addressed to a dead winner.** The decision keeps
  circulating until the same restart timer fires and a new election excludes
  the dead winner (its candidacy entry is gone). Accepted as a backstop;
  restart-with-exclusion after one unacknowledged lap would be the eager fix.
- **Item 10 — unbounded growth.** `history`, `appliedIds` and coordinator-side
  `ackCounts` grow with the number of writes, and Synchronization ships the
  full history. Acceptable at project scale; production would checkpoint state
  and send only missing suffixes (the ⟨epoch, seq⟩ order makes delta
  computation straightforward).
- **Ring probing.** Each election lap re-probes the crashed coordinator's ring
  slot and pays one ACK timeout per dead node; harmless at project scale.
- **Accurate crash detection** is assumed (spec): timeouts are sized from
  `getMaxLatencyPlusTolerance()` and the heartbeat interval so false suspicion
  does not occur under emulated latencies.

## Base-class issue to report to the instructors (email TAs)

- `AbstractClient.Result.equals` compares the boxed `Integer value` (and
  `Boolean success`) with `==`. Reference comparison only works inside the
  Integer cache [-128, 127]; base tests pass because `TEST_VALUE = 10`.
  Any `expectMsgEquals` on a result with value > 127 fails spuriously.
  Our own tests compare fields instead (see `custom/ConcurrentClients.java`).

## Custom test suite map (src/test/java/it/unitn/ds/custom/)

| Test | Guards against |
|---|---|
| `ConcurrentClients` | ⟨client, rid⟩ collision between two clients writing through the same replica |
| `ResubmissionOrder` (×4 reps) | per-client write order inverted by failover resubmission (item 1) |
| `FailoverRecovery.staleAfterSync*` | stale pending updates surviving synchronization (item 2) |
| `FailoverRecovery.interruptedUpdate*` | interrupted broadcast not completed, or applied more than once per replica |
| `MessageImmutability` | mutable shared message contents (item 3) |
| `IndexValidation` | out-of-range index wiping replica state via Akka restart (item 4) |
| `RegimeFencing` | stale-regime Synchronization/UPDATE accepted; equal-epoch tie not deterministic (items 5, 7) |
