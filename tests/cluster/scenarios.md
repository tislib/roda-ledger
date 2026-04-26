# Cluster Test Scenarios

Catalogue of cluster-level scenarios that need test coverage. Every scenario
operates against `ClusterTestingControl` (multi-node, standalone, or bare
mode) — there are no entries here for ledger-only behaviour. Scenarios marked
**[covered]** map to an existing test file under `tests/cluster/`; the rest
are gaps to fill.

References:
- ADR-0015 (cluster mode, lagged single-phase replication, `Quorum`)
- ADR-0016 (leader election, term/vote durability, divergence reseed,
  term-fenced `AppendEntries`)

---

## 1. Configuration & bring-up

| # | Scenario | Notes |
|---|----------|-------|
| 1.1 | Standalone (`cluster.is_none()`) boots only the writable client gRPC; no Node service, no `Quorum`. | **[covered]** `standalone_test::standalone_serves_writes_with_no_node_grpc` |
| 1.2 | Standalone term log advances on every restart (ADR-0016 §11). | **[covered]** `standalone_test::standalone_term_log_advances_on_restart` |
| 1.3 | Single-node cluster (one self-peer) → boots directly into `Leader` (no election). | Gap — covered implicitly in many tests but no explicit assertion of "no election round occurred" + "term bumped exactly once". |
| 1.4 | Multi-node cluster boots into `Initializing`, runs election, exactly one `Leader` reachable via `Ping`. | **[covered]** `election_test::three_nodes_elect_a_unique_leader_from_cold_boot` |
| 1.5 | `Config::validate` rejects: zero `node_id`, empty peer list, self missing from peers, duplicate `peer_id`. | **[covered]** unit tests in `config.rs`. Add cluster-level test: harness must surface `Build` error if a node's peer list is malformed. |
| 1.6 | Symmetric peer list: all nodes have identical `cluster.peers`, differ only by `cluster.node`. | Gap — assert two nodes' configs serialised after build are equal in everything except `cluster.node`. |
| 1.7 | Cluster of N where N ∈ {2, 3, 5} reaches a unique leader from cold boot within `5×max_election_timeout`. | Gap — extend `election_test` to parameterised N ∈ {2,3,5}. |
| 1.8 | `phantom_peer_count` lets the harness boot N real + M phantom peers; quorum sizing reflects total membership. | **[covered]** `divergence_reseed_test` uses 1 real + 1 phantom; gap — explicit quorum-size assertion. |
| 1.9 | Two nodes that disagree about `cluster.peers` (asymmetric membership) — assert undefined-but-bounded behaviour and document. | Gap — asymmetry isn't blocked at validation; need explicit test or guard. |

## 2. Leader election (ADR-0016 §5)

| # | Scenario | Notes |
|---|----------|-------|
| 2.1 | Cold-boot election in a 3-node cluster: exactly one Leader, others Follower. | **[covered]** `election_test`. |
| 2.2 | "No two leaders in the same term" invariant under healthy heartbeats. | **[covered]** `election_test` (400 ms snapshot). Strengthen by extending to 5 s with continuous writes. |
| 2.3 | Failover: kill leader → surviving node wins re-election; new leader id ≠ killed id. | **[covered]** `election_test`. |
| 2.4 | Re-elected leader bumps to a strictly higher term than the deposed one. | Gap — assert `new_term > old_term` after failover. |
| 2.5 | `RequestVote` granted only when `(req.last_term, req.last_tx_id) >= (our_last_term, our_last_tx_id)` (Raft §5.4.1 up-to-date check). | Gap — drive a candidate with stale log against an up-to-date follower; vote refused. |
| 2.6 | `RequestVote` already-voted: same term, different candidate → refused; same candidate → idempotent grant. | Gap (unit covers Vote, not gRPC handler) — exercise via `node_handler` in bare mode. |
| 2.7 | `RequestVote` higher-term observation persists `voted_for=0` durably (`vote.log` fdatasync) before reply. | Gap — write a `RequestVote` with `term` above ours, kill the process, reopen `Vote` and assert state. |
| 2.8 | Stale-term `RequestVote` (req.term < ours) → refused, response carries our term. | Gap. |
| 2.9 | Election round ends in `Lost` if no majority within `max_ms` deadline; supervisor re-arms timer. | Gap — partition the candidate from a majority and assert it cycles `Initializing → Candidate → Initializing`. |
| 2.10 | Election round ends in `HigherTermSeen` when any peer reply carries higher term; node steps down to `Initializing`, durably observes term. | Gap. |
| 2.11 | Single-node cluster: candidate wins by self-vote alone (fast-path in `run_election_round`). | Gap — assert no `RequestVote` RPC was issued. |
| 2.12 | Election timer resets on any valid `AppendEntries` (heartbeat or non-empty) and on granted `RequestVote`. | Partially covered (`election_timer.rs` unit). Cluster-level: assert leader's heartbeats prevent followers from becoming Candidate. |
| 2.13 | Randomised election timeout actually splits ties: 3 nodes simultaneously timing out should converge in O(timeouts), not livelock. | Gap — repeated cold-boot harness over 100 iterations, no test should exceed the deadline. |
| 2.14 | Pre-vote (ADR-0016 §13) prevents a partitioned node from inflating term on reconnection. | Gap — pre-vote is documented as default but no test asserts disconnected→reconnected node does NOT cause leader to step down. |
| 2.15 | Candidate's term bump uses `last_commit_id + 1` as `start_tx_id` even before any tx is appended (ADR-0016 §11). | Gap — read `term.log` after a lost candidate and assert the recorded `start_tx_id` matches expectation. |
| 2.16 | Concurrent simultaneous candidates in the same term: Vote layer permits only one; loser must transition out of Candidate. | Gap. |
| 2.17 | Election with one node failing mid-vote (transport error): counts as no-vote, election still completes. | Gap. |
| 2.18 | Granted vote durably `fdatasync`s `vote.log` BEFORE responding (crash-after-grant must be observable). | Gap — kill the node mid-`RequestVote` and assert reopen recovers the granted vote. |

## 3. Term / Vote durability

| # | Scenario | Notes |
|---|----------|-------|
| 3.1 | `Term::new_term` is idempotent in atomic value, monotonic in records. | **[covered]** unit. |
| 3.2 | `Term::observe` rejects strict regression. | **[covered]** unit. |
| 3.3 | Term ring rolls over (>10 000 entries) → cold lookup via `TermStorage::cold_lookup`. | **[covered]** unit. Cluster-level: send an `AppendEntries` with `prev_tx_id` that requires the cold path. **[covered]** `append_entries_prev_check_test::cold_lookup_path_via_term_storage_also_detects_divergence`. |
| 3.4 | `Vote` reopens with `(current_term, voted_for)` rehydrated from last record. | **[covered]** unit. |
| 3.5 | `Vote::observe_term` clears `voted_for` on higher term (allows next vote in new term). | **[covered]** unit. |
| 3.6 | Two clustered nodes restarted with persistent dirs see consistent term progression (no node ever sees term going backwards). | Gap — restart-loop test asserting term monotonicity. |
| 3.7 | After leader step-down, `Term::observe` and `Vote::observe_term` are both written before role flips back to `Initializing`. | Gap — race between abort and step-down. |

## 4. Lagged single-phase replication (ADR-0015)

| # | Scenario | Notes |
|---|----------|-------|
| 4.1 | Leader → follower replication catches follower up to leader's commit. | **[covered]** `basic_test::cluster_leader_replicates_to_follower`. |
| 4.2 | Idle heartbeat (`wal_bytes` empty) refreshes follower's `last_commit_id` in `Quorum`. | Gap — write once, stop writes, assert `Quorum::get()` continues to advance. |
| 4.3 | Heartbeat after writes stop closes the "one batch stale" gap. | Gap. |
| 4.4 | `prev_tx_id == 0` sentinel accepted unconditionally (first RPC of fresh follower). | **[covered]** `append_entries_prev_check_test::prev_tx_id_zero_is_accepted_unconditionally`. |
| 4.5 | `prev_tx_id` beyond follower's `last_commit_id` → `RejectPrevMismatch` (gap, not divergence). | **[covered]** `append_entries_prev_check_test::prev_tx_id_beyond_our_last_commit_is_rejected_as_gap`. |
| 4.6 | Matching `(prev_tx_id, prev_term)` accepted. | **[covered]** `append_entries_prev_check_test::prev_log_match_with_correct_term_is_accepted`. |
| 4.7 | Mismatched `prev_term` at existing `prev_tx_id` → divergence (RejectPrevMismatch + watermark stash). | **[covered]** `append_entries_prev_check_test::prev_term_mismatch_at_existing_tx_id_triggers_divergence`. |
| 4.8 | Cold-path term lookup (term ring rotated past `prev_tx_id`) also detects divergence. | **[covered]** `append_entries_prev_check_test::cold_lookup_path_via_term_storage_also_detects_divergence`. |
| 4.9 | `AppendEntries` to a Leader → `RejectNotFollower`, ledger untouched. | **[covered]** `append_entries_prev_check_test::leader_role_rejects_append_entries`. |
| 4.10 | Stale-term request (`req.term < current_term`) → `RejectTermStale` with our term in response. | Gap. |
| 4.11 | Higher-term request observes term durably (`Term::observe` + `Vote::observe_term`) before applying entries. | Gap — leader bumps term, ships first AE; follower's `term.log`/`vote.log` reflect bump after the call. |
| 4.12 | After observing a higher term, follower role transitions `Initializing|Candidate → Follower`. | Gap. |
| 4.13 | `WalTailer` survives WAL segment rotation (inode change detected). | Gap — drive a long enough write run that the leader's WAL rotates mid-replication. |
| 4.14 | Follower tailer's `from_tx_id` is monotonically non-decreasing across calls. | Implicit in 4.1; gap — explicit assertion. |
| 4.15 | Replication chunk size capped at `append_entries_max_bytes`; oversized batches spill over multiple RPCs. | Gap — leader producing > max_bytes/RPC of WAL; assert >1 RPC observed by follower. |
| 4.16 | gRPC `max_decoding_message_size = max_bytes * 2 + 4 KiB` accepts chunks at the configured boundary, rejects beyond. | Gap. |
| 4.17 | Per-peer replication retries transport failures in place; reconnects on next iteration. | Gap — kill + restart follower mid-write, leader resumes shipping without restart. |
| 4.18 | Per-peer task self-terminates promptly when leader steps down (cooperative `running` flag). | Gap — observe `peer_handles` join after step-down. |
| 4.19 | Process-wide `supervisor_running=false` drains every peer task even if the per-leader flag is still set (no zombie heartbeats). | Gap — observed in `peer_replication.rs` doc-comment but not asserted. |
| 4.20 | `to_tx_id` field on response correctly reflects the last record's tx_id (off-by-one boundary). | Gap. |
| 4.21 | Follower applies `decode_records` correctly for partial / aligned / unaligned byte buffers. | Gap. |

## 5. Quorum

| # | Scenario | Notes |
|---|----------|-------|
| 5.1 | `Quorum::new(n).get() == 0` on construction. | **[covered]** unit. |
| 5.2 | Majority arithmetic for n ∈ {1, 2, 3, 5} (sorted-desc[majority-1]). | **[covered]** unit. |
| 5.3 | `majority_index` never regresses (`fetch_max`). | **[covered]** unit. |
| 5.4 | Leader counts itself toward quorum (slot 0 fed by `on_commit` hook). | Gap — assert in a 1+1 cluster that pulling the follower offline still lets `Quorum::get()` advance from leader's local commits in a 1-node setup; but in true majority cases, that the leader's slot contributes. |
| 5.5 | `reset_peers(leader_slot)` clears every other slot but preserves the leader's. | Gap. |
| 5.6 | `cluster_commit_index` mirrors `Quorum::get()` after every `advance`. | **[covered]** unit. |
| 5.7 | Concurrent `advance` calls from many peer tasks never publish a regressed value. | Gap — stress test with 8+ peers writing concurrently. |
| 5.8 | After a Leader transition, the new leader sees its own `match_index[leader_slot]` populated from the on-commit hook within one tick. | Gap. |

## 6. Divergence detection & reseed (ADR-0016 §9)

| # | Scenario | Notes |
|---|----------|-------|
| 6.1 | Crafted divergent `AppendEntries` triggers reseed; live `last_commit_id` truncates to `leader_commit_tx_id` watermark. | **[covered]** `divergence_reseed_test`. |
| 6.2 | After reseed, balances reflect only the surviving prefix. | **[covered]** `divergence_reseed_test`. |
| 6.3 | `take_divergence_watermark` is destructive single-shot (subsequent calls return `None`). | **[covered]** `append_entries_prev_check_test::prev_term_mismatch_at_existing_tx_id_triggers_divergence`. |
| 6.4 | Multiple divergence rejects in a row → latest `leader_commit_tx_id` wins. | **[covered]** `append_entries_prev_check_test::divergence_watermark_updates_to_latest_leader_commit`. |
| 6.5 | gRPC servers stay up across the reseed (`LedgerSlot` swap, no rebind). Same client continues working. | **[covered]** `divergence_reseed_test` (client still queries after reseed). Strengthen by holding an in-flight RPC across the swap. |
| 6.6 | Reseed with a snapshot whose `covered_up_to_tx > watermark` skips that snapshot, falls back to earlier snapshot or genesis + WAL replay. | Gap — write 2 snapshots, then reseed below the latest. |
| 6.7 | Reseed when no snapshot covers a sufficiently early point (aggressive retention) → recovery cannot complete locally; node should stay in `Initializing` and surface error / require `InstallSnapshot`. | Gap (deferred to recovery follow-up but worth a documented failure-mode test). |
| 6.8 | Deposed leader becomes follower and reseeds via the same path when its uncommitted tail diverges. | Gap. |
| 6.9 | `start_with_recovery_until` is the ONLY runtime entry point that drops `Arc<Ledger>`; cold boot never truncates. | Gap — assert `start()` cannot accept a watermark. (Compile-time invariant; runtime smoke test that no boot path calls `start_with_recovery_until`.) |
| 6.10 | Reseed re-registers `on_commit` hook on the new ledger so `Quorum` keeps tracking. | Gap. |
| 6.11 | If divergence is observed on the leader (shouldn't happen but defensive), node steps down rather than reseeding. | Gap — supervisor logs "leader observed unexpected divergence; stepping down". |
| 6.12 | Reseed during in-flight `AppendEntries` finishes the current RPC against old ledger; next RPC observes new ledger. | Gap. |
| 6.13 | Concurrent client reads during reseed are not torn (they observe either pre- or post- reseed state). | Gap. |

## 7. Role transitions (ADR-0016 §2, §3)

| # | Scenario | Notes |
|---|----------|-------|
| 7.1 | `Initializing → Candidate` on election timer expiry. | Gap — expose internal counter / observe via Ping. |
| 7.2 | `Candidate → Leader` on `ElectionOutcome::Won`; leader-specific tasks (peer replication, on-commit hook) come up. | Gap. |
| 7.3 | `Candidate → Initializing` on `Lost` (timer re-armed). | Gap. |
| 7.4 | `Candidate → Initializing` on `HigherTermSeen` (term + vote durably observed). | Gap. |
| 7.5 | `Leader → Initializing` step-down via `Transition::StepDownHigherTerm` from a peer's response. | Gap. |
| 7.6 | `Leader → Initializing` step-down via supervisor handler observing higher term on incoming RPC. | Gap. |
| 7.7 | `Initializing/Candidate → Follower` via `settle_as_follower` on valid `AppendEntries`. | Gap. |
| 7.8 | Drop-and-rebuild invariant: each role transition tears down `LeaderHandles`/`FollowerHandles`, no stale connections leak. | Gap — observe peer connection count via OS lsof or tonic metrics before/after step-down. |
| 7.9 | Per-leader cancellation (`leader_alive`) flips on step-down without affecting `supervisor_running`. | Gap. |
| 7.10 | After step-down, the next Leader entry sees fresh `Quorum` peer slots (via `reset_peers`) but its own slot intact. | Gap. |

## 8. Fault scenarios

### 8.1 Process / node failures

| # | Scenario | Notes |
|---|----------|-------|
| 8.1.1 | Single-leader cluster (3 nodes), kill leader → re-election, no committed tx lost. | Partial **[covered]** `election_test` (no commit assertion). Strengthen with active writes pre-kill, balance check post-kill. |
| 8.1.2 | Kill follower → leader continues, `Quorum::get()` only stalls if cluster falls below majority. | Gap. |
| 8.1.3 | Kill **two** of three nodes → surviving node cannot reach quorum, blocks `WaitLevel::ClusterCommit`. | Gap. |
| 8.1.4 | Kill **two** of five nodes → cluster still tolerates, leader continues. | Gap. |
| 8.1.5 | Kill **three** of five → no quorum, no progress on cluster-commit waits. | Gap. |
| 8.1.6 | All nodes killed, all restarted with persistent dirs → cluster reforms, no committed data lost. | Gap. |
| 8.1.7 | Killed leader restarted (rejoins as follower under higher-term new leader); divergence reseed truncates uncommitted tail. | Gap. |
| 8.1.8 | Repeated leader churn (kill → re-elect → kill → re-elect …×N) without losing writes that were `WaitLevel::ClusterCommit`-acked. | Gap. |
| 8.1.9 | Process aborted mid-`AppendEntries` write on follower; on restart, partial WAL bytes either fully recovered or recoverable via Recover. | Gap (covered partially by ledger crash tests; needs cluster framing). |
| 8.1.10 | Process aborted mid-vote on `Vote::vote` → on restart, vote state reflects whichever side of the fdatasync it was on. | Gap. |
| 8.1.11 | Process aborted mid-`Term::new_term` → restart sees either old or new term, never torn. | Gap. |

### 8.2 Network failures

| # | Scenario | Notes |
|---|----------|-------|
| 8.2.1 | Symmetric partition: 3 nodes split into {1, 2}; majority side keeps quorum, minority side cannot elect. | Gap — needs an in-process partition harness (e.g. firewalled tonic transport). |
| 8.2.2 | Partition heals: minority reconnects, observes higher term via heartbeat, settles as Follower without disrupting leader. | Gap. |
| 8.2.3 | Asymmetric partition: leader can send to A but not B; B times out and starts election. Pre-vote should suppress (ADR-0016 §13). | Gap. |
| 8.2.4 | Repeated transient errors: every Nth `AppendEntries` fails. Replication eventually catches up. | Gap. |
| 8.2.5 | Tonic transport-level error (`UNAVAILABLE`) on `RequestVote` counts as `RoundReply::TransportError`; election proceeds without that vote. | Gap. |
| 8.2.6 | Slow link (added latency) on one peer → that peer lags, but `Quorum::get()` still advances from the other peer + leader. | Gap. |

### 8.3 Storage failures

| # | Scenario | Notes |
|---|----------|-------|
| 8.3.1 | `term.log` write fails (read-only fs / disk full) on `Term::new_term` → election round returns error, candidate spins back to `Initializing`. | Gap. |
| 8.3.2 | `vote.log` fdatasync fails on grant → `RequestVote` responds with `vote_granted=false` (handler swallows error). | Gap. |
| 8.3.3 | WAL append fails on follower → `AppendEntriesResponse` returns `RejectWalAppendFailed`. Leader treats as transient, retries. | Gap. |
| 8.3.4 | `term.log` corruption (truncated tail) on reopen → `Term::open_in_dir` rebuilds ring up to last valid record. | Gap (storage-layer test exists; gap at cluster level). |

## 9. Edge cases

| # | Scenario | Notes |
|---|----------|-------|
| 9.1 | Leader with **zero** other peers (single-node cluster) — `Quorum` is sized 1; `last_commit` from on-commit hook IS the cluster commit. | Gap — assert `cluster_commit_index` advances even with no `AppendEntries` traffic. |
| 9.2 | Empty `AppendEntries` (heartbeat) succeeds, advances follower's `cluster_commit_index` to clamped `leader_commit_tx_id`. | Gap. |
| 9.3 | `leader_commit_tx_id` greater than follower's `last_commit_id` is clamped (no false positive on cluster-commit waits). | Gap — assert via `LedgerHandler::wait_for_transaction_level`. |
| 9.4 | `from_tx_id < prev_tx_id` (malformed request) — handler should reject with `RejectSequenceInvalid` (proto allows; code currently silent). | Gap — current implementation does not enforce; document or fix. |
| 9.5 | `to_tx_id` does not match last record in `wal_bytes` (decode mismatch) — should reject. | Gap. |
| 9.6 | `LedgerSlot` swap during in-flight RPC: outstanding handler finishes against old `Arc<Ledger>`; next handler sees new one. | Gap. |
| 9.7 | Node serving `Ping` while in `Candidate` reports `NodeRole::Recovering` (no proto value for Candidate yet). | Gap (documented in `role_flag.rs`). |
| 9.8 | Two simultaneous candidates arrive at the same follower: `node_mutex` serialises so one grant happens, the other refuses. | Gap. |
| 9.9 | `RequestVote` and `AppendEntries` race on the same follower under `node_mutex`. Whichever wins the lock observes a consistent view. | Gap. |
| 9.10 | Election timer reset race: timer is mid-await when `reset()` lands → re-arm and continue without firing. | **[covered]** `election_timer.rs` unit. Cluster-level: leader's heartbeats prevent followers from ever becoming Candidate over a 5 s window. |
| 9.11 | gRPC reflection service available on client port (operational tooling). | Gap. |
| 9.12 | SIGINT / SIGTERM → both servers shut down cleanly (no panic, no orphan tasks). | Gap. |
| 9.13 | Restart same harness on same data dir → Term and Vote both rehydrate; leader bumps term once on bring-up. | Gap. |
| 9.14 | `cluster_size = 2` → split-brain edge case. Either node can elect (majority = 2 means BOTH must vote). Lose one peer → no progress. | Gap. |
| 9.15 | Phantom peers (configured but never started): leader's replication tasks retry forever, do not crash, do not pollute Quorum. | Implicit in `divergence_reseed_test`; gap at explicit assertion. |
| 9.16 | Replication state when `cluster.peers.len() == 1` (only self): no peer replication tasks spawned (`other_count == 0` log line). | Gap. |

## 10. Wait levels & guarantees

| # | Scenario | Notes |
|---|----------|-------|
| 10.1 | `WaitLevel::Computed` returns when local pipeline reaches the tx (no replication required). | Standalone-covered (sync_submit_test). Gap on a cluster: cluster mode with `Computed` should still return promptly, even if peers are down. |
| 10.2 | `WaitLevel::Committed` returns when local WAL fsync reaches the tx (still local). | Gap on cluster. |
| 10.3 | `WaitLevel::Snapshot` returns when local snapshot covers the tx. | Gap on cluster. |
| 10.4 | `WaitLevel::ClusterCommit` requires ALL of: snapshot ≥ tx, commit ≥ tx, `cluster_commit_index ≥ tx`. | Gap — write a tx, observe `wait_for_transaction(ClusterCommit)` blocks until quorum tracker advances. |
| 10.5 | `WaitLevel::ClusterCommit` blocks indefinitely (until 20s timeout) when no quorum is reachable. | Gap. |
| 10.6 | Wait for unknown tx returns `WaitOutcome::NotFound` immediately, never blocks. | **[covered]** `term_behavior_test::wait_rpc_returns_not_found_for_unknown_tx_immediately`. |
| 10.7 | Wait with `term` fence + wrong term → `WaitOutcome::TermMismatch` with the actual covering term/start. | **[covered]** `term_behavior_test::wait_rpc_returns_term_mismatch_with_actual_term`. |
| 10.8 | Wait reached carries actual term back to caller. | **[covered]** `term_behavior_test::wait_rpc_reached_carries_actual_term`. |
| 10.9 | Submit on a Follower → `FAILED_PRECONDITION`. | **[covered]** `basic_test`. |
| 10.10 | Submit on `Initializing` (post-reseed, pre-leader-recovery) → `FAILED_PRECONDITION`. | Gap. See §18.4 for the cluster-wide "no leader → writes blocked" assertion. |
| 10.11 | Submit on `Candidate` → `FAILED_PRECONDITION`. | Gap. See §18.4. |
| 10.12 | Reads on Follower (`get_balance`, `get_pipeline_index`) succeed and reflect locally-committed state (with replication lag). | Implicit; gap — explicit lag assertion. |
| 10.13 | `get_pipeline_index().cluster_commit_index` advances on the leader as quorum acks roll in. | Gap. |
| 10.14 | `get_pipeline_index().cluster_commit_index` on a follower advances based on `leader_commit_tx_id` clamped to local commit. | Gap. |
| 10.15 | After leader failover, prior `WaitLevel::ClusterCommit` ack remains durable on the new leader (no rollback of acked writes). | Gap — strongest end-to-end correctness check. |

## 11. Lag / progress observability

| # | Scenario | Notes |
|---|----------|-------|
| 11.1 | `Quorum::peer(i)` reflects each peer's last acked tx; observable for `min_lag/max_lag/maj_lag` reporting. | Gap on cluster (unit tests at `quorum.rs` only). |
| 11.2 | Stop writes on a healthy cluster → after one heartbeat cycle, `Quorum::get() == leader.last_commit_id()` (stale-by-one closes). | Gap. |
| 11.3 | One slow peer in 5-node cluster → `Quorum::get()` keeps advancing because majority excludes the laggard. | Gap. |
| 11.4 | Large catch-up window (follower offline for K seconds, then online): catch-up rate respects `append_entries_max_bytes` and `replication_poll_ms`. | Gap. |
| 11.5 | Backpressure: when follower disk is slow, leader's `from_tx_id` advances but `peer_last_tx` lags; `Quorum::get()` reflects the lag. | Gap. |
| 11.6 | Heartbeat-only stretch: `peer_last_tx` doesn't advance but `last_tx_id` (returned by follower) does, so `match_index[peer]` stays current. | Gap. |

## 12. Concurrency & atomicity

| # | Scenario | Notes |
|---|----------|-------|
| 12.1 | `node_mutex` (per-node async mutex) serialises every mutating Node-service handler so `(currentTerm, votedFor, log)` are atomically observed/written. | Gap — fire interleaved RPCs; assert no torn state. |
| 12.2 | `Ping` does NOT take `node_mutex` (read-only). | Gap. |
| 12.3 | `LedgerSlot` is lock-free (ArcSwap); concurrent `ledger()` and `replace()` never see a torn `Arc<Ledger>`. | **[covered]** unit. Cluster-level: stress-test reseed under heavy RPC load. |
| 12.4 | `RoleFlag` AcqRel reads/writes: handler observes role change immediately after supervisor flips it. | Gap. |
| 12.5 | `Quorum::advance` from many peer tasks concurrently never publishes a regressed `majority_index`. | Gap. |
| 12.6 | `Transition` mpsc channel `try_send` drops gracefully when full (next signal retries). | Gap. |
| 12.7 | Election timer `reset()` while a hold-and-await is in progress: deadline pushed forward, no spurious fire. | **[covered]** unit. |

## 13. Membership & topology

| # | Scenario | Notes |
|---|----------|-------|
| 13.1 | 1-node cluster: single self-peer, leader from boot, all wait levels work. | Gap — explicit. |
| 13.2 | 2-node cluster: majority = 2, both must be alive for cluster commit. | Gap. |
| 13.3 | 3-node cluster: tolerates 1 failure. | Gap (tested implicitly in `election_test`). |
| 13.4 | 5-node cluster: tolerates 2 failures, majority = 3. | Gap. |
| 13.5 | 7-node cluster: tolerates 3 failures (boundary scaling). | Gap. |
| 13.6 | Membership defined symmetrically across all nodes' configs (sanity assertion at startup). | Gap. |

## 14. Resource lifecycle

| # | Scenario | Notes |
|---|----------|-------|
| 14.1 | `SupervisorHandles::abort` aborts driver, watcher, both servers, and all transient peer tasks via `running` flag. | Gap — observe via JoinHandle::is_finished. |
| 14.2 | Standalone harness drop releases the gRPC port (next harness on same port works). | Gap (implicit in `client_test::test_deposit_restart_withdraw`). |
| 14.3 | `Drop` of `ClusterTestingControl` removes harness-owned root data dir (auto cleanup). | Gap. |
| 14.4 | Caller-owned `data_dir_root` survives Drop (no rm). | Implicitly used by `standalone_term_log_advances_on_restart`; gap on explicit assertion. |
| 14.5 | After `stop_node`, `start_node` on the same slot reopens existing data dir; term/vote/ledger all rehydrate. | **[covered]** `client_test::test_deposit_restart_withdraw` (standalone). Gap: clustered restart. |
| 14.6 | Aborted leader's peer tasks observe `supervisor_running=false` and exit cleanly within one poll interval; no zombie heartbeats hit surviving followers. | Gap — critical for failover correctness, called out in `peer_replication.rs` doc. |

## 15. End-to-end correctness invariants

| # | Scenario | Notes |
|---|----------|-------|
| 15.1 | After any number of leader changes, all surviving nodes converge on the same balances for every account. | Gap — chaos-style: random kills + writes + healing; final balance comparison. |
| 15.2 | Across a 3-node cluster running 100k tx with mid-run failover, total system+account balance pairs sum to 0 on every node. | Gap (extension of #15.1). |
| 15.3 | A tx that returned `WaitLevel::ClusterCommit` success is still committed on the cluster after any single node failure. | Gap — strongest durability guarantee. Concrete shapes: §18.2 (full-restart durability) and §18.5 (mid-election durability). |
| 15.4 | A tx that returned only `WaitLevel::Committed` (local) on a leader that subsequently failed before quorum may be lost — assert this is the documented behaviour (no false durability). | Gap — negative test asserting the contract. |
| 15.5 | Replication is byte-exact: leader's WAL byte-for-byte matches followers' WAL after catch-up. | Gap. |
| 15.6 | `term.log` content on every node converges (same record set up to local truncation point) after a full failover cycle. | Gap. |

## 16. Performance / soak (cluster-flavoured)

| # | Scenario | Notes |
|---|----------|-------|
| 16.1 | 1M tx through a 3-node cluster: throughput, p50/p99 latency, no leader churn under steady state. | Gap (single-node soak exists — `soak_test`). |
| 16.2 | Replication lag (`maj_lag`) bounded under sustained load. | Gap. |
| 16.3 | Election storms under repeated kill-restart loops complete within bounded time (no livelock). | Gap. |
| 16.4 | Reseed time scales with WAL volume up to `watermark` (rather than total WAL size). | Gap — write a lot then reseed to a small prefix; expect fast reseed. |

## 17. Security / invariants under adversarial input

| # | Scenario | Notes |
|---|----------|-------|
| 17.1 | An RPC with `term=0` is treated specially (skipped term observation per current code). Document the contract; ensure no node accepts garbage as authoritative. | Gap — read code path: `if req.term != 0`. |
| 17.2 | Malicious `RequestVote` with `term=u64::MAX-1` does not overflow `Term::new_term`'s `checked_add(1)`. | Gap (code returns `Other` error; assert this propagates). |
| 17.3 | `candidate_id == 0` is rejected at `Vote::vote`. | **[covered]** unit; gap via gRPC handler. |
| 17.4 | Wrong cluster's RPCs (mismatched proto schema) are rejected by tonic decoding. | Out of test scope (transport-level). |
| 17.5 | Replay of an old `AppendEntries` (replayed from network) — old `term`/`prev_term` triggers stale rejection without affecting state. | Gap. |

---

## Coverage matrix

| Source area | Covered tests | Gaps (#) |
|------------|---------------|----------|
| Configuration | `config.rs::tests`, `standalone_test` | 4 |
| Election | `election_test` | 14 |
| Term/Vote durability | `term.rs::tests`, `vote.rs::tests`, `term_behavior_test` | 5 |
| Replication | `basic_test`, `append_entries_prev_check_test` | 16 |
| Quorum | `quorum.rs::tests` | 5 |
| Divergence reseed | `append_entries_prev_check_test`, `divergence_reseed_test` | 8 |
| Role transitions | (none cluster-level) | 10 |
| Faults (process / network / storage) | partial via `election_test` | 21 |
| Edge cases | `election_timer.rs::tests` | 14 |
| Wait levels | `term_behavior_test`, `sync_submit_test`, `client_test` | 11 |
| Lag / observability | (none) | 6 |
| Concurrency | `quorum.rs::tests`, `ledger_slot.rs::tests` | 7 |
| Topology | (implicit only) | 6 |
| Lifecycle | `client_test::test_deposit_restart_withdraw` | 5 |
| Correctness invariants | (none) | 6 |
| Performance / soak | (none) | 4 |
| Adversarial input | partial | 4 |

Total identified scenarios: ~150. Currently covered (fully or partially): ~25.
Highest priority gaps are §6.5–6.13 (reseed under load), §7 (role transitions
without dedicated tests), §8.1.7–8.1.11 (fault recovery correctness), §10.4
(`ClusterCommit` wait level), and §15 (end-to-end correctness invariants).
