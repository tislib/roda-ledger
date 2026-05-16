# Raft crate — test scenarios

An index of every test under `crates/raft/tests/`, with a one-sentence description, a short scenario, and a remark noting gaps / oddities / known limitations. Use this to audit coverage at a glance.

Files are listed in the order they sit in the directory. A **Coverage gaps** section closes the document.

## Test infrastructure (`tests/common/`)

- **`common/mod.rs`** — Deterministic in-process simulator (`Sim`) used by most files. Provides a virtual clock, FIFO message queue, crash/restart model, configurable drop/duplicate/delay fault injection, and safety helpers (`assert_election_safety`, `assert_log_matching`, `assert_state_machine_safety`). Also exposes lower-level RaftNode-driving helpers (`drive_tick`, `deliver_vote_reply`, `outbound_vote_count`) used by unit-style tests.
- **`common/mem_persistence.rs`** — In-memory `Persistence` implementation. Embeds an inner `mod tests` with **18 unit tests** covering term-log/vote-log semantics directly on `MemPersistence`. These run as part of the integration target. Topics covered: fresh state, `commit_term` accept/refuse paths (forward skips, regression, zero), `truncate_term_after` (including `u64::MAX` no-op), `term_at_tx` boundaries (zero, below first record's start, `u64::MAX`), `vote` idempotency / cross-term flow, panics on zero candidate id and vote-term regression.

## `simulator.rs`

End-to-end happy-path and partition/crash scenarios on the deterministic `Sim`.

### `three_node_cluster_elects_a_leader_and_replicates`
- **Description**: 3-node cluster elects a leader and replicates 5 entries to every node.
- **Scenario**: Boots a 3-node `Sim`, awaits leader election, has the leader accept 5 client writes, advances virtual time until cluster_commit ≥ 5 on every node.
- **Remark**: Golden-path smoke test for consensus + replication. Asserts all three safety properties at the end.

### `single_node_cluster_self_elects_and_commits`
- **Description**: 1-node cluster self-elects on its first timeout and commits writes immediately.
- **Scenario**: Single-node `Sim` (seed=7) ticks past the election window, then accepts 3 writes.
- **Remark**: Degenerate quorum=1 case; commit fires the moment the entry lands locally.

### `leader_failover_after_crash`
- **Description**: After the leader crashes, survivors elect a new leader at a higher term.
- **Scenario**: 3-node cluster commits 3 entries, leader is crashed, survivors run until they elect a new leader.
- **Remark**: Core failover; uses `await_leader_in` to scope the new-leader search to surviving nodes.

### `partitioned_minority_does_not_disrupt_majority`
- **Description**: A 2-node minority partition cannot prevent the 3-node majority from committing.
- **Scenario**: 5-node cluster commits 4 entries, then partitions 2 nodes away. Majority continues writing; final cluster_commit ≥ 8 on the majority side.
- **Remark**: Validates partition tolerance / quorum invariance.

### `read_api_consistency_with_action_stream`
- **Description**: Role / term / commit getters are consistent and observable without an action stream.
- **Scenario**: 3-node cluster elects a leader, writes 2 entries, then polls `role()`, `current_term()`, `local_commit_of(...)`, `cluster_commit_of(...)`.
- **Remark**: Test name mentions "action stream" but this codebase intentionally has none — name is misleading. Validates the polling-only API.

### `rejoin_with_divergent_log_truncates_to_leader`
- **Description**: A previously isolated node, on rejoin, catches up via §5.3 walk-back + backfill.
- **Scenario**: 3-node cluster, isolate one node, leader replicates 4 entries on the remaining quorum, heal partition, restart the isolated node, wait for convergence.
- **Remark**: Without pre-vote the rejoiner may bump terms repeatedly; the test allows multiple election rounds. The "divergent log" framing is aspirational — in practice the isolated node has an empty/short log, not a conflicting one.

## `single_node.rs`

Direct `RaftNode` unit tests — no simulator, no message bus.

### `construction_with_empty_peer_list_panics`
- **Description**: `RaftNode::new` panics if `self_id` is not in the peer list.
- **Scenario**: Pass an empty peer vec to the constructor.
- **Remark**: Input validation; uses `#[should_panic(expected = "...")]` with the literal message.

### `single_node_self_elects_on_first_timeout_tick`
- **Description**: A 1-node cluster transitions to Leader after the first election-timeout tick.
- **Scenario**: Tick at `t0` (lazy-arm), tick again at `t0 + 60s` (well past the timeout window).
- **Remark**: Hardcoded 60s gap dwarfs any realistic election window — robust to retuning of timeouts but masks tighter-bound bugs.

### `single_node_leader_commits_own_writes_immediately`
- **Description**: Single-node leader commits via `advance_local_index` with quorum=1.
- **Scenario**: Become leader, call `advance_local_index(5)`, read `cluster_commit_index()`.
- **Remark**: Validates the quorum=1 shortcut.

### `no_events_means_no_transitions`
- **Description**: With no ticks and no messages, the node stays in `Initializing` at term 0.
- **Scenario**: Construct a fresh node and read role/term/commit indices without doing anything.
- **Remark**: Documents the "library doesn't measure time internally" invariant.

### `two_node_cluster_without_peer_replies_keeps_bumping_term`
- **Description**: A candidate that receives no votes stays Candidate and bumps its term on every election timeout.
- **Scenario**: 2-node cluster, tick past two election windows, observe term increasing while role stays Candidate.
- **Remark**: Documents the no-pre-vote split-vote behavior.

### `advance_is_monotonic`
- **Description**: `advance_local_index` is monotonic; lower values are silently ignored.
- **Scenario**: Single-node leader at commit 3, attempt `advance_local_index(1)`.
- **Remark**: Validates a safety invariant — but silent regression-ignore is also load-bearing in driver code.

### `advance_updates_cluster_commit_on_leader_only`
- **Description**: `advance_local_index` lifts cluster_commit only when the caller is leader.
- **Scenario**: Call `advance_local_index(5)` on a follower (cluster_commit stays 0); promote to single-node leader and repeat (cluster_commit reaches 5).
- **Remark**: Role-gated mutator; relevant for safety under role transitions.

### `advance_returns_unit`
- **Description**: `advance_local_index` is a void mutator; effect is observed via a getter.
- **Scenario**: Advance to 5, read `cluster_commit_index()`.
- **Remark**: Encodes the "no action stream" model.

## `multi_node.rs`

Multi-node behavior, replication edges, election state-machine tests, and read-API contracts. The largest single file in the suite.

### `three_node_cluster_elects_one_leader_and_replicates`
- **Description**: 3-node cluster elects exactly one leader and replicates 5 entries to all followers.
- **Scenario**: Boot, await leader, client_write 5, advance time until commit converges.
- **Remark**: Duplicates `simulator.rs::three_node_cluster_elects_a_leader_and_replicates`. Two files, same scenario — one could be removed.

### `heartbeats_suppress_follower_elections`
- **Description**: Heartbeats keep followers in `Follower` for several election windows.
- **Scenario**: Elect leader, run ~2s virtual time, assert followers stay Follower at the leader's term.
- **Remark**: Validates heartbeat-suppression but only at one cluster size.

### `cluster_commit_index_advances_monotonically`
- **Description**: `cluster_commit_index` never regresses across multiple write rounds.
- **Scenario**: 3-node cluster, 5 batches of 3 writes, poll commit after each round.
- **Remark**: Monotonicity under sustained load; not a stress-level test.

### `lost_election_does_not_pollute_term_log`
- **Description**: A candidate that never wins doesn't write to the term log.
- **Scenario**: 2-node cluster where node 1 can never win; trigger candidacy three times, inspect persistence.
- **Remark**: Regression for the "term-bump-before-win" bug. Asserts vote_term ≥ 1 but no term-log record.

### `higher_term_request_vote_during_candidacy_forces_step_down`
- **Description**: A higher-term `RequestVote` while Candidate forces immediate step-down.
- **Scenario**: Drive node to Candidate, deliver RV at term 99.
- **Remark**: Term-bump precedence test.

### `higher_term_seen_in_vote_reply_aborts_candidacy`
- **Description**: A vote reply at a higher term aborts the candidacy (grant flag ignored).
- **Scenario**: Candidate at term T receives a vote reply at term T+2.
- **Remark**: Validates that term-bump beats grant.

### `stale_vote_reply_does_not_count_toward_majority`
- **Description**: A vote reply at a lower term is ignored even if `granted=true`.
- **Scenario**: Candidate at term T, deliver granted reply at T-1.
- **Remark**: Stale-message filter.

### `higher_term_in_append_reply_demotes_leader`
- **Description**: A higher term in an AE reject demotes the leader.
- **Scenario**: 2-node leader at term 1 receives `Reject { term: 99, reason: TermBehind }`.
- **Remark**: Term-bump via the replication path.

### `boot_does_not_bump_term`
- **Description**: Constructing a `RaftNode` from prior persistence preserves term and `voted_for` exactly.
- **Scenario**: Become leader at term 1, voted_for=Some(1), build a new node from the same persistence.
- **Remark**: Regression for boot-time term bump.

### `empty_follower_rejects_then_accepts_after_walk_back`
- **Description**: A follower with an empty log rejects AE, the leader walks back, and the follower eventually accepts the bulk load.
- **Scenario**: 3-node cluster, write 5 entries, wait for full convergence.
- **Remark**: Validates §5.3 implicitly via cluster convergence — doesn't directly assert the walk-back step count.

### `prev_log_tx_id_zero_skips_term_match_check`
- **Description**: `prev_log_tx_id = 0` accepts unconditionally (skips §5.3 term match).
- **Scenario**: Follower receives a handshake with prev_log_tx_id=0 and prev_log_term=99 (impossible). Validate handshake.
- **Remark**: Special-case for empty-log boundary.

### `leader_commit_clamp_to_local_log`
- **Description**: A follower clamps `leader_commit` to its local last-known entry.
- **Scenario**: Follower with 3 entries gets a handshake claiming `leader_commit=10`; call `advance_cluster_index(10)`.
- **Remark**: Critical safety invariant — followers can't commit what they haven't received.

### `append_entries_from_old_leader_is_rejected`
- **Description**: A handshake from a lower-term leader is rejected with `TermBehind`.
- **Scenario**: Follower observed term 5, handshake arrives at term 3.
- **Remark**: Term-based leader filtering.

### `read_methods_are_pure`
- **Description**: Read methods are pure / idempotent — N calls return identical values.
- **Scenario**: Set up a cluster with committed entries; call `role()`, `current_term()`, `cluster_commit_of()`, `local_commit_of()` 1000× each.
- **Remark**: Validates side-effect-free reads.

### `became_leader_matches_immediate_query`
- **Description**: Once elected, leader status is visible immediately via getters.
- **Scenario**: Single-node tick past election window, read role/term.
- **Remark**: Polling-only API contract.

### `cluster_commit_query_reflects_advance`
- **Description**: `advance_local_index` is immediately visible via the cluster-commit getter.
- **Scenario**: Single-node leader, `advance_local_index(7)`, read `cluster_commit_index()`.
- **Remark**: Polling-only contract.

### `vote_denied_when_candidate_last_term_below_ours`
- **Description**: §5.4.1 — vote denied if candidate's `last_term` is below voter's, even with higher `last_tx_id`.
- **Scenario**: Voter has entries at term 5; candidate advertises last_term=2, last_tx_id=10.
- **Remark**: Up-to-date check (term priority).

### `vote_denied_when_same_last_term_but_lower_last_tx_id`
- **Description**: §5.4.1 — at equal `last_term`, vote denied if candidate has lower `last_tx_id`.
- **Scenario**: Voter has 10 entries at term 5; candidate claims last_term=5, last_tx_id=7.
- **Remark**: Up-to-date check (tx_id tiebreaker).

### `vote_granted_when_candidate_last_term_above_ours`
- **Description**: §5.4.1 — higher `last_term` grants the vote regardless of `last_tx_id`.
- **Scenario**: Voter has 10 entries at term 5; candidate claims last_term=7, last_tx_id=2.
- **Remark**: Term beats tx_id.

### `duplicate_request_vote_from_same_candidate_grants_again`
- **Description**: A duplicate RV from the same candidate in the same term is granted (idempotent); a different candidate is denied.
- **Scenario**: Grant candidate 2 at term 3 twice; then deny candidate 3 at term 3.
- **Remark**: One-vote-per-term with idempotent re-grants.

### `heartbeat_propagates_leader_commit_advance`
- **Description**: A heartbeat (empty AE) carrying `leader_commit` advances follower's cluster_commit.
- **Scenario**: Follower with 3 entries, heartbeat at term 1, driver calls `advance_cluster_index(2)`.
- **Remark**: Heartbeat as commit-watermark carrier.

### `heartbeat_leader_commit_clamps_to_local_commit`
- **Description**: Heartbeat-carried `leader_commit` is clamped to local last-known entry.
- **Scenario**: Follower with 3 entries, heartbeat claims commit=10.
- **Remark**: Same safety invariant as `leader_commit_clamp_to_local_log` but via heartbeat.

### `local_commit_index_survives_election_loss`
- **Description**: `local_commit_index` survives a Candidate → Follower transition on higher-term AE.
- **Scenario**: Node with 3 entries becomes Candidate, then receives a handshake at a higher term.
- **Remark**: Node-level state preservation across role transitions.

### `concurrent_vote_handling_reaches_quorum`
- **Description**: A batch of concurrent vote replies that reach quorum elects the candidate immediately.
- **Scenario**: 5-node candidate at term T, deliver 4 `Granted` outcomes in one `handle_votes` call.
- **Remark**: Validates batch processing.

### `mixed_grant_deny_batch_steps_down_on_higher_term`
- **Description**: A batch with a higher-term reply forces step-down even if majority granted.
- **Scenario**: 5-node candidate, batch of [grants + higher-term deny + timeout].
- **Remark**: Term-bump beats majority.

### `partial_batch_below_quorum_keeps_candidate`
- **Description**: A batch with 1 grant + 3 timeouts (no quorum) leaves the node as Candidate.
- **Scenario**: 5-node candidate, deliver 1 Granted + 3 Failed.
- **Remark**: Confirms the quorum gate.

### `get_requests_idempotent_within_candidacy`
- **Description**: `get_requests()` is a pure read — two calls return identical output.
- **Scenario**: 3-node candidate, call `get_requests()` twice, compare.
- **Remark**: Idempotent read.

### `get_requests_empty_when_not_candidate`
- **Description**: `get_requests()` returns empty when not in Candidate role.
- **Scenario**: Initializing node, call `get_requests()`, check length.
- **Remark**: Role-gated read.

### `tick_returns_wakeup_with_future_deadline_after_lazy_arm`
- **Description**: The first tick (lazy-arm) returns a wakeup whose deadline is in the future.
- **Scenario**: Fresh node, tick at `t0`, read `wakeup.deadline`.
- **Remark**: Validates election-timer arming on first tick.

### `cluster_index_advance_on_replication`
- **Description**: Cluster commit advances when a 2-of-3 quorum acks at the new index.
- **Scenario**: 3-node leader, `advance_local_index(200)`, deliver `Success { last_commit_id: 200 }` from peer 2.
- **Remark**: Standard quorum-ack path.

### `cluster_commit_stuck_at_prior_term_boundary_after_promotion`
- **Description**: **Known bug** — a new leader inheriting prior-term entries cannot commit them via §5.4.2 even with full acks, and stays stuck without external traffic.
- **Scenario**: Leader crashes with 200 prior-term entries (cluster_commit=0); a new leader is elected and receives 3-of-3 acks at the prior-term boundary; cluster_commit stays at 0.
- **Remark**: **This is asserted as a failure mode, not a passing invariant.** Standard Raft fixes this with a no-op append on election win; this codebase doesn't. The test exists to document the limitation.

## `state_machine.rs`

Regression tests for the **Bug 3** family (vote-log races ahead of term-log).

### `term_log_has_no_phantom_record_after_vote_log_race`
- **Description**: After a 4-term vote-log race and self-election in single-node, the term log contains only the won term.
- **Scenario**: Candidate at term 1 observes an RV at term 5, steps down. Then promote to single-node and tick past the election timeout.
- **Remark**: Smallest-skew Bug 3.1 regression.

### `no_synthetic_term_records_for_unobserved_terms`
- **Description**: A 5-term vote-log race produces no synthetic term-log records for unobserved terms.
- **Scenario**: Five inbound RVs bump vote-log term; promote to single-node; self-elect.
- **Remark**: Multi-step skew (Bug 3.2).

### `term_log_records_have_unique_start_tx_id`
- **Description**: Term-log records have unique `start_tx_id` even after a vote-log race.
- **Scenario**: Two-step RV skew + single-node self-election; collect all `start_tx_id` values into a HashSet.
- **Remark**: Bug 3.3 regression.

### `term_at_tx_for_existing_entries_unaffected_by_catch_up`
- **Description**: `term_at_tx` for older entries returns the original term, not the new one, after a vote-log race.
- **Scenario**: Leader at term 1 writes 3 entries, crashes; vote log races ahead; promote to single-node and self-elect at higher term; query `term_at_tx` for entries 1..=3.
- **Remark**: Bug 3.4 regression guard.

## `error_handling.rs`

Edge-case regressions for audit findings.

### `truncation_below_cluster_commit_index_panics_in_debug`
- **Description**: A truncation below `cluster_commit_index` triggers a debug-assert (Leader Completeness violation).
- **Scenario**: Follower at cluster_commit=5 receives a handshake whose `prev_log_term` mismatches, forcing a truncation to tx_id=1.
- **Remark**: `#[cfg(debug_assertions)]` + `#[should_panic]` — only enforced in debug builds. A release-mode regression would slip through.

### `raftnode_new_panics_on_misconfigured_heartbeat_interval`
- **Description**: Constructor panics when `heartbeat_interval > 2 × election_timer.min_ms`.
- **Scenario**: Build a `RaftConfig` with heartbeat=200ms and election.min=150ms.
- **Remark**: Config validation at construction time. Only one misconfig is tested — no coverage for inverted election bounds, zero values, etc.

### `current_term_returns_max_of_term_log_and_vote_log`
- **Description**: `current_term()` returns `max(term_log.term, vote_log.term)`.
- **Scenario**: Persistence with term_log.term=3 and vote_log.term=7; query `current_term()`.
- **Remark**: Term composition (vote log races ahead).

### `current_term_returns_term_log_when_vote_log_is_lower`
- **Description**: `current_term()` returns the term log when the vote log is lower.
- **Scenario**: term_log.term=9, vote_log.term=4.
- **Remark**: Reverse case of the max.

## `safety_properties.rs`

`proptest`-driven §5.4 safety property tests.

### `safety_under_random_schedule`
- **Description**: Election Safety + Log Matching + State Machine Safety hold under arbitrary `Write` / `CrashLeader` / `Partition` schedules.
- **Scenario**: `proptest! { cases: 32, max_shrink: 64 }`. Each case generates a random op sequence and asserts the three safety properties after each op.
- **Remark**: Only 32 cases — CI time budget. The op vocabulary is small (3 ops); no message-level fuzzing.

### `many_seeds_eventually_elect_a_unique_leader`
- **Description**: Across many seeds, a 3-node cluster elects exactly one leader within 5s of virtual time.
- **Scenario**: `proptest! { cases: 64 }`. Per seed, await leader election and assert `election_safety`.
- **Remark**: Liveness check across seeds. The 5s budget is conservative for 3 nodes but could shrink with pre-vote.

## `stress.rs`

Soak / mixed-fault scenarios.

### `long_schedule_with_mixed_faults_preserves_safety`
- **Description**: A 5-node cluster under 5% drop + 5% duplication + periodic crashes maintains safety throughout.
- **Scenario**: Run for 5s virtual time, elect leader, write 5 entries; then 3 rounds of (crash a non-leader, write 2, restart). At the end, clear faults and run a settle window before asserting safety.
- **Remark**: The "settle window" is required because asserting under active chaos races with in-flight messages. Documented in code but obscures whether safety holds during chaos.

### `deterministic_replay_with_same_seed`
- **Description**: Same seed produces identical final state across two independent runs.
- **Scenario**: Run the simulator twice with seed=7777; compare final cluster_commit on every node.
- **Remark**: Determinism guard. Only checks final state — doesn't compare intermediate schedules.

## `crash_partition.rs`

Crash-recovery and partition scenarios.

### `leader_crash_triggers_failover`
- **Description**: Leader crash mid-replication triggers re-election at a higher term.
- **Scenario**: 3-node cluster commits 3 entries, leader crashes, survivors elect a new leader.
- **Remark**: Duplicate of `simulator.rs::leader_failover_after_crash`.

### `follower_crash_then_restart_catches_up`
- **Description**: A crashed follower restarts and catches up to the leader's commit.
- **Scenario**: 3-node cluster commits 3, follower crashes, leader writes 4 more (still has 2-of-3 quorum), follower restarts, wait for it to reach commit ≥ 7.
- **Remark**: Tests catch-up after a gap. Hardcoded 2s "stay crashed" window.

### `boot_recovers_term_and_vote`
- **Description**: Restarting from persistence preserves term and `voted_for`.
- **Scenario**: Single-node leader at term 1, voted_for=Some(1), crash/restart.
- **Remark**: Duplicate of `multi_node.rs::boot_does_not_bump_term`.

### `restart_preserves_one_vote_per_term_rule`
- **Description**: After restart, a duplicate vote for a different candidate in the same term is denied.
- **Scenario**: Self-elect single-node (vote=Some(1), term=1), crash/restart in 3-node config, deliver an RV from candidate 2 at term 1.
- **Remark**: Vote-log durability across restart.

### `minority_partition_cannot_elect_majority_continues`
- **Description**: A 2-of-5 minority partition cannot elect; majority commits 4 more entries.
- **Scenario**: 5-node cluster commits 4, partitions 2-vs-3, majority writes 4 more.
- **Remark**: Partition tolerance.

### `rejoin_after_partition_catches_up`
- **Description**: A partitioned node, on heal, catches up despite having bumped its term while isolated.
- **Scenario**: 3-node cluster, isolate 1 node, leader writes 4 entries, heal, wait for isolated node to reach commit ≥ 4 within 8s.
- **Remark**: 8s timeout is generous because no pre-vote means the rejoiner may bump terms several times.

### `repeated_partition_flap_eventually_stabilises`
- **Description**: 10 rounds of partition open/close eventually stabilize into a single leader.
- **Scenario**: 3-node cluster, 10× (partition for 100ms, heal for 100ms), then wait for a leader within 5s.
- **Remark**: Hardcoded flap timing. No assertion on commit progress during the flap — only post-flap stability.

### `symmetric_partition_no_quorum_either_side`
- **Description**: When neither side of a partition has quorum, no leader emerges until heal.
- **Scenario**: 5-node cluster, kill leader, split remaining 4 into 2-vs-2, run 2s, heal, expect re-election.
- **Remark**: Quorum requirement under symmetric split.

### `restart_recovers_local_indexes_via_advance`
- **Description**: Crash/restart resets `commit_index` to 0; the driver must rehydrate via `advance_local_index`.
- **Scenario**: Single-node leader, `advance_local_index(7)`, crash, restart, observe commit=0, driver calls `advance_local_index(7)`, observe commit=7.
- **Remark**: Documents that commit indices are intentionally non-durable. Worth flagging at the architecture layer.

### `leader_step_down_preserves_commit_index`
- **Description**: A leader stepping down to Follower on higher-term AE preserves `local_commit_index`.
- **Scenario**: Multi-node leader, `advance_local_index(5)`, deliver a handshake at a higher term.
- **Remark**: Duplicate-ish with `local_commit_index_survives_election_loss` from multi_node.rs.

## `delivery_faults.rs`

Message-delivery fault scenarios (drop, duplicate, delay, late replies).

### `cluster_makes_progress_under_10pct_drop`
- **Description**: At 10% drop, a 3-node cluster commits at least 5 of 10 attempted writes within 10s.
- **Scenario**: Set `drop=0.10`, elect, write 10, run until commit ≥ 5.
- **Remark**: Threshold (5 of 10) is loose — passes even with significant degradation. No upper bound on time/term increases.

### `cluster_is_idempotent_under_duplication`
- **Description**: With 100% duplication (every message delivered twice), the cluster commits 5 writes within 2s.
- **Scenario**: Set `duplicate=1.0`, elect, write 5, wait for convergence.
- **Remark**: RPCs are idempotent in this Raft implementation.

### `cluster_thrashes_under_delay_above_election_timeout`
- **Description**: A network delay (200ms) above the election window (80–160ms) causes terms to keep climbing.
- **Scenario**: Set `delay=200ms`, run 2s, assert `max_term ≥ 3`.
- **Remark**: Documents the no-pre-vote limitation.

### `cluster_makes_progress_under_moderate_delay`
- **Description**: With a widened election window (200–400ms) and 30ms delay, the cluster commits.
- **Scenario**: Custom config; elect, write 3, run until commit ≥ 3 within 5s.
- **Remark**: Shows the no-pre-vote issue is tunable via timers.

### `unsolicited_append_entries_reply_is_unreachable_via_replication`
- **Description**: `replication.peer(99)` returns `None` for an unknown peer — there's no way to inject a spurious reply.
- **Scenario**: Single-node leader, attempt `peer(99)`.
- **Remark**: API-shape test. Doesn't exercise the network adversary case directly — only proves the surface is closed.

### `late_reply_after_rpc_timeout_does_not_crash`
- **Description**: A late reply after the in-flight slot was cleared by timeout is handled without panic.
- **Scenario**: 2-node leader, deliver `Timeout` then a stale `Reject`.
- **Remark**: Robustness check. Only asserts no panic + still leader — doesn't check that the late reply was discarded vs. processed.

## `regressions.rs`

Targeted regressions for previously-fixed bugs.

### `lost_elections_do_not_pollute_term_log`
- **Description**: Term log stays empty across failed candidacies.
- **Scenario**: Same as `multi_node.rs::lost_election_does_not_pollute_term_log`.
- **Remark**: **Duplicate of the multi_node test** — kept for the "regression" label.

### `restart_does_not_bump_term`
- **Description**: Constructor preserves term exactly across restart.
- **Scenario**: Same as `multi_node.rs::boot_does_not_bump_term`.
- **Remark**: **Duplicate** kept for the regression label.

### `log_mismatch_decrements_next_index`
- **Description**: On `LogMismatch`, the leader's per-peer `next_index` walks back one step at a time.
- **Scenario**: 2-node leader, 3 entries, deliver 3 `LogMismatch` rejects to peer 2; poll `next_index` after each.
- **Remark**: Regression for missing §5.3 walk-back. Walks one step at a time — no fast-backoff fast-path tested here.

### `term_log_truncates_with_entry_log`
- **Description**: When a §5.3 mismatch forces a follower truncation, term-log records past the truncation point are dropped.
- **Scenario**: Follower with 5 entries at term 1 gets a handshake at term 2 with `prev_log_tx_id=5, prev_log_term=2` (mismatch).
- **Remark**: Regression for term-log + entry-log truncation coherence.

### `commit_term_refuses_when_already_advanced`
- **Description**: Defensive — `commit_term(T)` returns `false` when current ≥ T.
- **Scenario**: Persistence at term 5, call `commit_term(5)`.
- **Remark**: Prevents term-N split-brain re-commit.

### `figure_8_new_leader_does_not_commit_prior_term_entries_by_replica_count`
- **Description**: §5.4.2 gate blocks prior-term entry commits even with full replica counts (Figure 8).
- **Scenario**: 5-node cluster, 5 prior-term entries with commit=0, crash leader, new leader at term 2 wins 3-of-5, all peers ack at last_commit_id=5; assert commit stays < 5.
- **Remark**: Critical safety property. Paired with the "stuck cluster" bug in `multi_node.rs` — the gate is *correct* but the codebase has no no-op-on-election unstick.

### `three_node_failover_preserves_all_safety_properties`
- **Description**: 3-node leader crash followed by failover preserves Election / Log Matching / State Machine safety.
- **Scenario**: Write 4, crash leader, await re-election, assert all three.
- **Remark**: End-to-end smoke combining failover + safety.

## Coverage gaps

- **No log snapshotting / compaction tests** — if the library implements (or plans to implement) snapshots, nothing here exercises them.
- **No dynamic membership / cluster reconfiguration** — `peers` is static.
- **No leadership-lease / read-index optimization** — no linearizable-read path tested.
- **No out-of-order delivery** — the simulator delivers in FIFO order, so reordering is structurally untestable in this harness.
- **No Byzantine / leader-equivocation tests** — only crash-stop and message-loss faults.
- **Clusters cap at 5 nodes** — no 7-, 9-, or 11-node stress.
- **No release-build coverage of the truncation debug-assert** — `truncation_below_cluster_commit_index_panics_in_debug` is `#[cfg(debug_assertions)]`-only.
- **Known limitation: prior-term commit deadlock** — `cluster_commit_stuck_at_prior_term_boundary_after_promotion` documents a real bug. Standard Raft fixes this with a no-op append on election win; this codebase doesn't.
- **Two test duplicates worth pruning** — `simulator.rs::leader_failover_after_crash` ↔ `crash_partition.rs::leader_crash_triggers_failover`; `multi_node.rs::boot_does_not_bump_term` ↔ `crash_partition.rs::boot_recovers_term_and_vote` ↔ `regressions.rs::restart_does_not_bump_term`.
- **Disk persistence is never exercised** — all tests use the in-memory `MemPersistence` shim. fsync / partial-write / corruption paths are untested.
