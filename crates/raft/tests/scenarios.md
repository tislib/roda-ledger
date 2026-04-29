# roda-raft Test Scenarios

Comprehensive test plan for `crates/raft/`. Every scenario maps to an explicit test name and includes the setup, the
trigger, and the assertions. Designed to catch the bug classes from the previous implementation plus the standard Raft
edge cases.

The file is organized by **what gets tested**, not by what kind of test it is. Unit, integration, property, and
simulator tests all coexist; the harness section at the end describes which level each scenario belongs in.

---

## Section 1 — Term Module (`crates/raft/src/term.rs`)

Already mostly tested in the existing module. Add these:

### 1.1 `commit_term` rejects expected_new == 0

Cannot commit term 0; that's the default. Should return `InvalidInput`.

### 1.2 `commit_term` after `observe` of same term returns false

Fresh term log → `observe(5, 0)` → `commit_term(5, 0)` returns `Ok(false)` (not error, not true). The "I already saw
this term" path.

### 1.3 `commit_term` after `observe` of higher term returns false

`observe(7, 0)` → `commit_term(5, 0)` returns `Ok(false)`. Stale election.

### 1.4 `truncate_after` is crash-safe (atomic rewrite)

Test: write 3 records, simulate a crash mid-truncate (e.g. by truncating to length 0 and crashing before rewrite).
Reopen — must NOT see empty term log. **This requires the atomic-rewrite fix using rename.** If the implementation uses
non-atomic truncate-then-rewrite, this test exposes the bug.

### 1.5 Concurrent `observe` and `commit_term` is impossible by construction

The library is single-threaded; no test needed. Document the invariant.

### 1.6 `term_at_tx` boundary cases

- `term_at_tx(0)` when only record is `{term: 5, start_tx_id: 0}` → returns `Some({5, 0})`.
- `term_at_tx(0)` when only record is `{term: 5, start_tx_id: 100}` → returns `None`.
- `term_at_tx(u64::MAX)` returns the latest record.

### 1.7 `truncate_after(u64::MAX)` is a no-op

Drops nothing, doesn't rewrite the file.

---

## Section 2 — Vote Module (`crates/raft/src/vote.rs`)

Already mostly tested. Add:

### 2.1 Cross-term vote sequence

`vote(1, 7)` granted → `observe_term(2)` → `vote(2, 7)` granted (same node id allowed in different term). Verify both
records on disk after reopen.

### 2.2 Vote durability across crash

`vote(5, 3)` succeeds → simulate crash before any other call → reopen → `voted_for() == Some(3)`, `current_term() == 5`.
The fdatasync guarantee.

### 2.3 `observe_term` followed by immediate `vote` in same term

`observe_term(7)` clears voted_for → `vote(7, 11)` granted → `voted_for() == Some(11)`. The standard "I observed a
higher term, now I'm voting" flow.

### 2.4 Reject vote when behind on log (§5.4.1)

This isn't `Vote`'s responsibility — it's `RaftNode`'s. But document the layering: `Vote::vote` always grants if
term/candidate rules pass; the up-to-date check happens in `RaftNode` before calling `Vote::vote`. Test `RaftNode`
separately.

---

## Section 3 — Quorum Module (`crates/raft/src/quorum.rs`)

Existing tests are good. Add:

### 3.1 `advance` with `node_index` out of bounds

In debug builds, `debug_assert!` fires. In release, undefined behavior (which is acceptable). Test: confirm debug-mode
panic.

### 3.2 Two-node cluster (degenerate even-N case)

`Quorum::new(2)` → majority = 2 (both must agree). Advance only slot 0 → no commit. Advance both → commit.

### 3.3 Watermark monotonicity under reordered advances

```rust
q.advance(0, 100);
q.advance(1, 100);  // commit = 100
q.advance(0, 50);   // late slot 0 update
assert_eq!(q.cluster_commit_index(), 100);  // unchanged
```

The `if index > self.match_index[node_index]` guard.

### 3.4 `reset_peers` does not advance the watermark

After reset, `cluster_commit_index` is unchanged. Only `advance` writes the watermark.

---

## Section 4 — Election Timer (`crates/raft/src/timer.rs`)

Existing tests cover seeding, arm/disarm, expiry. Add:

### 4.1 Multiple `arm` calls produce different deadlines (with same seed)

Sequence of `arm` calls with the same `now` should still produce different deadlines because the RNG advances. Verify
divergence after 10+ arms.

### 4.2 `arm` with `now` decreasing (clock skew)

If the driver passes a `now` earlier than the previous deadline, what happens? Document the contract: the new deadline
is `now + random(min, max)` regardless of prior state. Test that this is the behavior.

### 4.3 `is_expired` exactly at the deadline

`now == deadline` → returns `true`. (Confirmed in existing test, just call it out.)

---

## Section 5 — RaftNode Single-Node Behavior

These exercise the state machine in isolation, no peers. Run as unit tests in `node.rs` or as a single-node integration
test.

### 5.1 Construction with empty peer list panics or errors

`RaftNode::new(1, vec![], seed)` — what's the contract? Either panic with a clear message, or error. Document and test.

### 5.2 Single-node cluster self-elects on first tick

`RaftNode::new(1, vec![1], seed)` → role is `Initializing` initially → first `step(Event::Tick)` after election
timeout → role becomes `Leader`, current_term = 1.

### 5.3 Single-node leader commits its own writes immediately

Leader at term 1 → `step(Event::LocalCommitAdvanced { tx_id: 5 })` → returns
`Action::AdvanceClusterCommit { tx_id: 5 }` (because majority is 1).

### 5.4 Single-node with no events never transitions

Construct, never tick, never feed events. Role stays `Initializing` forever. Verifies that nothing wakes itself.

### 5.5 Two-node cluster requires both for majority

`peers = [1, 2]`, only node 1 ticks → starts election → no peer to vote → never wins. Stays Candidate. Election timer
re-arms; eventually times out again, bumps term again. Verify term increases monotonically through Candidate cycles.

---

## Section 6 — RaftNode Multi-Node Behavior (no faults)

Three-node cluster, idealized network (every message delivered, no delay).

### 6.1 Three-node cluster elects exactly one leader within 2 election cycles

Run until `ANY` node is leader. Assert exactly one is. Assert `current_term` >= 1 on the leader.

### 6.2 Election Safety: at most one leader per term

Across the entire run, no two nodes are simultaneously leaders of the same term. This is the §5.4 invariant — assert it
after every `step` call in the simulator.

### 6.3 Heartbeat suppresses follower elections

After a leader is elected, run for `5 * election_timeout`. Followers must NOT transition to Candidate during this
window. The leader's heartbeats keep their election timers reset.

### 6.4 Client write replicates to all followers

Leader receives `LocalCommitAdvanced(5)` → eventually all followers' `commit_index` reaches 5. Verify under no-fault
conditions in `< 5 * heartbeat_interval`.

### 6.5 Cluster commit index advances monotonically

Across the run, `cluster_commit_index()` on the leader is non-decreasing. Across followers, also non-decreasing.

### 6.6 Log Matching: identical prefixes on all nodes

For any two nodes that have committed `tx_id N`, the `(tx_id, term)` of every entry up to N is identical. This is the
§5.3 invariant — check by inspecting each node's log metadata after every committed entry.

---

## Section 7 — RaftNode Election Edge Cases

These are the bugs from the previous implementation. Each needs an explicit test.

### 7.1 Term-bump-before-win: lost election does not pollute term log

Setup: 3 nodes. Force a split-vote scenario: node 1 and node 2 both timeout simultaneously, both bump term, neither
wins (each only votes for itself). After the round, neither `term.log` should contain the new term — `commit_term` is
only called on Won.

This directly tests the bug from your earlier logs where every lost election burned a term.

**Test**: count `Term::len()` before and after a forced split-vote round. Should be equal.

### 7.2 Concurrent RequestVote during candidacy

Setup: node 1 starts election at term 5. Before its RequestVotes complete, node 2's RequestVote(term=6) arrives at node
1.

**Expected**: node 1 observes term 6, transitions to Follower of term 6 (or Initializing), does NOT become Leader of
term 5 even if it had collected enough votes.

**Test**: drive the exact event sequence in the simulator with controlled message delivery. Assert post-condition.

### 7.3 Won-with-higher-term-seen step-down

Setup: candidate at term 5. Receives `RequestVoteReply { from: 2, term: 7, granted: true }` (peer somehow granted under
a higher term).

**Expected**: candidate transitions to Initializing/Follower at term 7, does not become Leader.

### 7.4 Self-vote required for win

In a single-node cluster, the self-vote is the majority. In a multi-node cluster, a candidate that fails to durably
self-vote (vote already cast for another in this term, or fdatasync error) cannot win.

**Test**: simulate a `Vote::vote` returning `Ok(false)` for self-vote. Candidate must abort and return to Initializing.

### 7.5 Election timer randomization prevents indefinite split votes

Run 1000 random seeds with 3 nodes that all timeout simultaneously. Within a bounded number of rounds (say 10), one node
wins. If this fails, randomization is broken.

### 7.6 Stale RequestVoteReply ignored

Candidate at term 5 receives `RequestVoteReply { term: 3, granted: true }` (a delayed reply from a previous election
round at term 3). Must NOT count this toward the majority.

### 7.7 Higher-term observation while Leader

Leader at term 5 receives `AppendEntriesReply { from: 2, term: 7, ... }`. Must step down to Follower at term 7.

### 7.8 Boot does not bump term

`RaftNode::new` followed immediately by query: `current_term()` must equal whatever was on disk (from a previous run).
No `Action::PersistTerm` emitted at construction.

This catches the boot-time-bump bug from your logs.

### 7.9 Election timer resets on AppendEntries grant

Follower receives valid AppendEntries → election timer's deadline pushes forward. Verify by inspecting `next_wakeup()` (
or the equivalent internal state) before and after.

### 7.10 Election timer does not reset on rejected AppendEntries

Follower at term 7 receives AppendEntries(term=5) → reject (term too old). Election timer must NOT reset; otherwise a
malicious old leader can suppress legitimate elections.

---

## Section 8 — Replication Edge Cases

### 8.1 Log Mismatch (§5.3) walk-back

Setup: leader at term 5 with log `[1,2,3,4,5]`. Follower has `[1,2]` only.

Leader sends AppendEntries with prev_log_tx_id=4. Follower rejects (LogMismatch).

**Expected**: leader decrements `next_index` for that peer, retries with prev_log_tx_id=3, again rejects, retries with
prev_log_tx_id=2, succeeds, then ships 3,4,5.

**Test**: verify the sequence of `Action::SendAppendEntries` emitted, including the decreasing `prev_log_tx_id` values.
This is the bug from your logs where the leader stayed at the same range with backoff.

### 8.2 Log truncation on divergent suffix

Setup: leader at term 5 with log `[1@1, 2@1, 3@5]`. Follower has `[1@1, 2@1, 3@4]` (term-4 entry where leader has
term-5).

Leader sends AppendEntries with prev_log_tx_id=2, prev_log_term=1, entries=[3@5]. Follower's local `3@4` conflicts.

**Expected**: follower emits `Action::TruncateLog { after_tx_id: 2 }`, then `Action::AppendLog { tx_id: 3, term: 5 }`.
Term log truncates record `{term: 4, start_tx_id: 3}`.

**Test**: drive the events, assert the action sequence.

### 8.3 Empty AppendEntries (heartbeat)

Leader sends AppendEntries with `entries: []`. Follower accepts, resets election timer, replies success with its current
`last_tx_id`. No `AppendLog` actions emitted.

### 8.4 Out-of-order AppendEntries replies

Leader sends AppendEntries to peer 2 for entries 1..5, then 6..10. Reply for 6..10 arrives BEFORE reply for 1..5.

**Expected**: leader's `match_index[2]` advances correctly. The test verifies that `Quorum::advance` handles
out-of-order updates.

(In practice the library serializes one in-flight AppendEntries per peer, so this scenario doesn't occur. Document the
constraint and test that the constraint is enforced — i.e. an `Action::SendAppendEntries` is not emitted for a peer with
an in-flight RPC.)

### 8.5 AppendEntries from old leader

Setup: node 1 is leader at term 5, partitioned. Node 2 becomes leader at term 6.

Node 1's old AppendEntries(term=5) eventually reaches node 3 (which has observed term 6). Node 3 must reject with
`term=6, success=false, reason=TermBehind`.

**Test**: drive the events, assert the rejection.

### 8.6 Leader_commit clamping on follower

Leader sends AppendEntries with `leader_commit=10`, but follower's local log only goes up to tx_id=7. Follower's
`cluster_commit_index` should advance to 7, not 10.

### 8.7 Leader_commit advances follower's apply pipeline

Leader's `cluster_commit_index` was 5, advances to 8. Next AppendEntries carries `leader_commit=8`. Follower emits
`Action::AdvanceClusterCommit { tx_id: 8 }`.

### 8.8 Apply on commit, not on durable

Follower receives AppendEntries with entries 6..10 (durable) but `leader_commit=5`. Follower's local `commit_index`
advances to 10 (durable), but `cluster_commit_index` stays at 5.

`Action::AdvanceClusterCommit` is NOT emitted for tx 6..10 yet.

### 8.9 Heartbeat-only advance of cluster_commit_index

Leader durably commits entries 1..5 locally, replicates, gets quorum. Now sends a heartbeat (empty AppendEntries) with
`leader_commit=5`. Followers learn about the commit via the heartbeat, even though no new entries flow.

### 8.10 Replication after rejoin

Node 3 was partitioned, missed entries 5..10. Heals. Leader's next AppendEntries to node 3 has `prev_log_tx_id=4` (its
`next_index[3]`). Walk-back logic kicks in or it succeeds directly. Follower catches up.

### 8.11 Follower with empty log

Brand-new follower (`last_tx_id=0`) receives AppendEntries with `prev_log_tx_id=10`. Reject (LogMismatch). Leader walks
back to `prev_log_tx_id=0`, succeeds, ships everything.

### 8.12 Snapshot install (deferred per non-goals)

Stub only. When `Action::InstallSnapshot` is added, scenarios will follow. Document deferral.

---

## Section 9 — Crash and Recovery

### 9.1 Leader crashes mid-replication

Leader has shipped entries 1..5 to one of two followers. Leader crashes. New leader elected.

**Expected**: new leader's term > old leader's term. New leader determines its log state, starts shipping to followers.
Eventually all nodes converge.

**Subtlety**: if the new leader was the lagging follower (only saw up to entry 3), entries 4..5 are lost. This is
correct Raft behavior — uncommitted entries can be lost on leader change. Test asserts: only entries that were
`cluster_commit_index >= N` before the crash survive.

### 9.2 Follower crashes and restarts with stale log

Follower at `last_tx_id=10`. Crashes. While down, leader commits 11..20. Follower restarts with `last_tx_id=10`.

**Expected**: leader's heartbeat reaches follower, follower sees high `leader_commit`, accepts AppendEntries for 11..20,
catches up.

### 9.3 Boot recovers term and vote

Node 2 voted for node 1 in term 5, then crashed. Restarts. `current_term()=5`, `voted_for()=Some(1)`.

If node 3 sends RequestVote(term=5, candidate=3), node 2 must reject (already voted). If node 3 sends RequestVote(
term=6, candidate=3), node 2 grants (after observing term 6).

### 9.4 Boot recovers `commit_index` from term log + WAL

(Driver responsibility, but document) — the driver must reconstruct `commit_index` from the WAL's last durable tx_id.
The library trusts the constructor argument.

### 9.5 Crash during `commit_term`

Node wins election. `commit_term(5, 50)` is called. Crash mid-fdatasync.

On restart: term log might or might not have the term-5 record. Both states must be safe. If the record is present, the
node has "claimed" term 5 and must not allow another node to win term 5. If absent, the node hasn't claimed; another
node can win.

(Because fdatasync is atomic, the record either exists or doesn't; no partial state. Test: open the file with the record
half-written and verify either the open succeeds and recovers the record, or fails cleanly.)

### 9.6 Crash during `truncate_after`

Critical test: node has term log records `[1@0, 2@50, 3@100]`. `truncate_after(75)` is called. Crash between
`set_len(0)` and the rewrite.

On restart, `Term::open` must NOT see an empty term log if records 1 and 2 should survive. **This catches the
atomic-rewrite bug.**

### 9.7 Multi-node cluster with rolling restarts

5-node cluster. Restart each node one at a time, waiting for re-stabilization between restarts. Cluster never loses
leader for >5s.

---

## Section 10 — Network Partitions

### 10.1 Minority partition cannot elect

5-node cluster. Partition {1, 2} from {3, 4, 5}. Leader was in {3, 4, 5}.

**Expected**: {1, 2} side bumps term repeatedly but never wins (only 2 of 5). {3, 4, 5} side continues normally with 3
of 5 majority.

Verify {1, 2} nodes are stuck in Candidate state with increasing term but no leader.

### 10.2 Majority partition continues to commit

Same setup. Client writes to leader in {3, 4, 5} succeed. `cluster_commit_index` advances.

### 10.3 Heal merges minority into majority

Heal partition. {1, 2} side observes higher term from {3, 4, 5}'s leader heartbeats, steps down to Follower. Catches up
via AppendEntries.

**Subtlety**: {1, 2}'s term might be higher than the active leader's (because they bumped repeatedly while partitioned).
When they receive heartbeat, the active leader is "behind." This is exactly the pre-vote scenario — without pre-vote,
the active leader must step down.

**Expected** (without pre-vote, as v1): cluster experiences a brief leader-flap, then re-elects. Eventually stabilizes.
Test: run for sufficient virtual time (10s), assert eventual single-leader convergence.

### 10.4 Symmetric partition (no leader either side)

Partition {1, 2} from {3, 4, 5}. Then crash the leader. Now neither side has a quorum.

**Expected**: both sides bump term indefinitely without electing.

Heal partition. The combined cluster has 4 alive nodes (one is crashed). Eventually elects.

### 10.5 Asymmetric partition (one-way drop)

Node 1 can send to node 2, but node 2 cannot send to node 1. AppendEntries from leader 1 reach 2; replies don't return.

**Expected**: leader 1's `match_index[2]` never advances. From node 1's perspective, node 2 is dead. From node 2's
perspective, the leader is alive (heartbeats arrive). Node 2's election timer keeps resetting.

If the leader needs node 2 for majority (3-node cluster), commits stall.

### 10.6 Repeated partition flap

Partition, heal, partition, heal, repeated 50 times in 10s. Cluster must eventually stabilize and continue committing.

### 10.7 Partition during candidacy

Node 1 starts election at term 5. Partitioned mid-RequestVote — its outbound RPCs don't reach 2 or 3. Node 1's election
times out, retries at term 6, also partitioned.

Heal. Node 1 sends RequestVote(term=N, where N is high). The other side (which has its own leader at term M < N)
observes the higher term and steps down.

**Expected**: cluster destabilizes briefly, re-elects. Eventually one leader.

---

## Section 11 — Message Delivery Faults

### 11.1 Message drop

Random message drop with probability `p`. For `p=0.1`, cluster should still make progress (slowly). For `p=0.9`, cluster
may stall briefly but recovers.

**Test**: parameterized by `p`, run 5s of virtual time, assert at least N entries committed.

### 11.2 Message duplication

Each message delivered twice. Cluster behavior unaffected — Raft is idempotent on retransmission.

### 11.3 Message reordering

Messages between two nodes arrive out of order. Specific to AppendEntries: as noted in 8.4, the library serializes one
in-flight per peer, so reorder is between peers. Test reordering across peer pairs.

### 11.4 Message delay (long)

A message takes 10x the heartbeat interval to deliver. By the time it arrives, it's stale (term is old). Receiver
rejects.

### 11.5 Reply to a non-existent request

Peer sends `AppendEntriesReply` for an RPC the leader never sent (synthetic test of robustness). Leader must ignore
gracefully.

### 11.6 Late reply after RPC timeout

Leader's RPC to peer 2 timed out (synthesized as `RejectReason::RpcTimeout`). The actual reply arrives later. Leader
must handle the late reply correctly (probably ignore — the in-flight slot is already cleared).

---

## Section 12 — Read API Consistency

These ensure the read methods agree with the action stream. Run as unit tests.

### 12.1 `role()` matches `BecomeRole` action

After every `step` call, if `Action::BecomeRole { role: R, term: T }` was emitted, `node.role() == R` and
`node.current_term() == T`.

### 12.2 `cluster_commit_index()` matches `AdvanceClusterCommit` action

If `Action::AdvanceClusterCommit { tx_id: N }` was emitted, `node.cluster_commit_index() >= N` after the step (it might
be higher if multiple advances happened in one step).

### 12.3 Queries are pure (no side effects)

Calling `role()`, `current_term()`, etc. 1000 times in a row does not change any internal state. Verify by snapshotting
state before and after.

### 12.4 Queries are consistent across calls within one step

Within a single `step` call's effect, multiple queries return the same value. `role() == role()` always.

---

## Section 13 — §5.4 Safety Properties (Property Tests)

These are the four invariants from the Raft paper. Run as proptest with **1000+ cases** per property.

### 13.1 Election Safety

> At most one leader per term.

Across any reachable state of the cluster: for any term T, the set of nodes whose
`role() == Leader && current_term() == T` has cardinality ≤ 1.

**Test**: after every `step` in the simulator, scan all nodes, assert ≤ 1 leader per term.

### 13.2 Leader Append-Only

> A leader never overwrites or deletes entries in its log; it only appends.

For each leader, sequence of log states should be append-only. No `Action::TruncateLog` is emitted by a leader for its
own log.

**Test**: assert `Action::TruncateLog` is never emitted while `role() == Leader`.

### 13.3 Log Matching

> If two logs contain an entry with the same tx_id and term, then the logs are identical in all entries up through
> tx_id.

For any two nodes A and B and any tx_id N where both have an entry: the `(tx_id, term)` of every entry up to N must
match. This is the §5.3 property.

**Test**: across all pairs of nodes, for every committed tx_id, verify metadata equality.

### 13.4 Leader Completeness

> If a log entry is committed in a given term, that entry will be present in the logs of all leaders for higher-numbered
> terms.

For every committed entry E in term T, every node that becomes leader in term T+1 or later must have E in its log.

**Test**: track committed entries. When a new leader is elected, verify its log contains all previously-committed
entries.

### 13.5 State Machine Safety

> If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different
> log entry for the same index.

For any tx_id N and any two nodes A and B that have applied N: the payload at N must be byte-equal.

**Test**: track applied entries on each node. Assert payload equality across nodes.

This is the property that catches the silent-divergence bug from your earlier logs.

---

## Section 14 — Stress and Soak

### 14.1 Long-running random schedule

Run 60 seconds of virtual time with random ops (mix of writes, crashes, partitions, message drops). Assert all four §5.4
properties hold throughout. Final state: all alive nodes converged to identical commit index.

### 14.2 Maximum chaos

All fault types enabled simultaneously: message drops, delays, partitions, crashes. Run 30 seconds. Cluster eventually
stabilizes after chaos ends.

### 14.3 Throughput sanity

Under no faults, with 5 nodes, leader processes 1000 client writes. Time to commit all 1000 should be reasonable (< 5
seconds virtual time). This isn't a perf test; it's a "no pathological behavior under load" test.

### 14.4 Deterministic replay

Same seed + same op sequence = identical state across runs. Test by running twice and comparing every node's state.

---

## Section 15 — Specific Bugs From Previous Implementation

These are regression tests — one per bug found in the original code.

### 15.1 Term-5 split-brain

Reproduce the exact sequence from your earlier logs:

1. Leader at term 4, partitioned.
2. New leader elected at term 5.
3. Old leader receives reply with term 5, steps down.
4. Now: another candidate runs for term 5 simultaneously.

Without the fix: both nodes briefly believe they're leader of term 5.
With the fix: at most one can succeed.

### 15.2 Boot-time term bump produces ghost terms

Restart a node that was at term 6. Without the fix: node bumps to term 7 on boot. With the fix: node stays at term 6.

### 15.3 §5.3 LogMismatch results in exponential backoff (the bug)

Force a log mismatch. Without the fix: leader retries the same range with growing backoff. With the fix: leader
decrements `next_index` and retries with earlier `prev_log_tx_id`.

### 15.4 Term log not truncating with WAL

Force a log truncation scenario (8.2). After truncation, verify the term log was also truncated. Without the fix: term
log contains stale records.

### 15.5 Apply on durable causes silent divergence

Reproduce the test that failed: `balances_converge_across_nodes_after_churn`. Multiple nodes that applied diverging
entries. Without the apply-on-commit fix: balances differ. With the fix: balances converge.

---

## Test Harness Requirements

For these scenarios to actually work, the harness in `tests/common/mod.rs` must support:

1. **Virtual time.** `Sim::clock()` returns a synthetic `Instant`. `run_until(deadline)` advances the clock by stepping
   nodes through events until `deadline` is reached, NOT by sleeping in real time.

2. **Deterministic message delivery.** Messages between nodes go through a queue the harness controls. Default: FIFO,
   immediate delivery. Configurable: drop, delay, reorder, partition.

3. **Crash and restart.** `crash(n)` removes node `n` from the active set, drops its in-flight messages. `restart(n)`
   re-creates the node from its persisted state (term log, vote log).

4. **Partition control.** `partition(a, b)` adds a deny-list entry for the (a,b) link. Bidirectional unless asymmetric
   is requested. `heal_partition(a, b)` removes it.

5. **Message manipulation.** `drop_next_message_from(n)`, `delay_messages(p, ms)`, `reorder_window(n, ms)` for
   fine-grained control.

6. **Inspection.** `role_of(n)`, `current_term_of(n)`, `commit_index_of(n)`, `cluster_commit_index_of(n)`,
   `last_log_index_of(n)`, `log_metadata_of(n, range)`. Read-only.

7. **Property assertion helpers.** `assert_election_safety()`, `assert_log_matching()`, `assert_leader_completeness()`,
   `assert_state_machine_safety()` — called after every operation in test bodies and after every step in property tests.

8. **Deterministic seeding.** Every random choice (election timeout jitter, message order under reorder, drop
   probability) uses a seeded RNG.

---

## Test Organization

```
crates/raft/
  src/
    *.rs                       # unit tests inline (mod tests, #[cfg(test)])
  tests/
    common/
      mod.rs                   # Sim harness
      scenarios.rs             # builders for common setups (3-node-elected, etc.)
    section_01_term.rs         # Section 1 scenarios
    section_02_vote.rs         # Section 2
    ...
    section_13_safety.rs       # Section 13 — property tests with proptest
    section_14_stress.rs       # Section 14 — long-running soak
    section_15_regressions.rs  # Section 15 — bug reproductions
```

Naming `section_NN_*.rs` keeps the file order aligned with this document. `section_15_regressions.rs` is the file you
grow over time as new bugs are found and fixed.

---

## Coverage Goals

Per the design's success criteria:

- **10K+ proptest cases per CI run.** Section 13 properties run with `cases: 10_000` (cheap because virtual time).
- **Section 6 (no-fault) tests run < 100ms each.** Smoke tests, no patience required.
- **Section 7 (election edge cases) all reproduce within 1s of virtual time each.** No "wait and hope" patterns.
- **Section 9 (crash) and Section 10 (partition) tests within 5s of virtual time each.**
- **Section 14 (soak) runs nightly, 60s virtual time, > 1M events processed.**

If the harness can't deliver these, the harness is the bug, not the tests.

---

## What This Plan Catches

- Every bug class from the previous implementation (Section 15)
- All four §5.4 safety properties (Section 13)
- §5.3 log matching corner cases (Section 8)
- Election edge cases including the term-bump-before-win race (Section 7)
- Crash and partition recovery (Sections 9, 10)
- Message-delivery faults (Section 11)
- Boundary/degenerate cases (Sections 5.1, 3.2, 4.2)
- Read-API consistency with action stream (Section 12)

## What This Plan Does NOT Catch (Documented Non-Goals)

- Pre-vote scenarios (deferred per design)
- Joint consensus / membership changes (deferred)
- Snapshot install correctness beyond stub (deferred)
- Real-network performance (different concern, separate suite)
- Byzantine faults (out of scope for Raft)

---

This plan is the test contract. If a bug is found that isn't covered by a scenario here, the scenario list grows; tests
are written; the bug becomes a permanent regression check.