# roda-raft Design

## Goals

Extract a pure, deterministic Raft state machine as a separate crate inside the roda-ledger workspace. The library is specialised to roda-ledger (not generalised), independent of cluster/ledger code, and testable in isolation with a deterministic simulator. The previous `cluster::raft` module is replaced; specific known bugs (term-bump-before-win race, ghost-term bumping on boot, missing §5.3 truncation logic, term log drifting from the entry log) cannot recur under the new design.

`RaftNode` is a **single-node decision system**. It models one node's view of Raft: takes events in, returns actions out, exposes the minimum read surface the driver needs to gate writes and stamp wire responses. Anything that's not a decision input or a decision output does not belong in the library.

The exact Rust types — function signatures, enum variants, field names — live in [crates/raft/src](../../crates/raft/src/). This ADR describes *how the system should work*, not the current shape of the source.

## Architectural Boundary

**Pure state machine.** No internal threads, no tokio, no tonic, no I/O. The library transitions in-memory state and returns actions describing what the driver must do. The driver (cluster crate) executes actions and feeds results back as new events.

The library exposes one entry point — a synchronous `step(now, event) -> Vec<Action>` — plus a small read-only query surface (role, current term, the two commit indexes, voted-for). Time enters the library only through the `now` parameter; the library never reads a clock.

**Crate constraints.** `raft` depends only on minimal utility crates (RNG, logging, error). It does NOT depend on `storage`, `tokio`, `tonic`, `proto`, `ledger`, or `cluster`. If any of these creep in, the boundary is broken.

## Log Ownership Model: Option 3

**Tx_id is assigned by the leader's ledger, not by raft.** AppendEntries propagates leader-assigned tx_ids to followers. Followers accept these as authoritative.

**Asymmetric write paths:**

- **Leader.** Ledger assigns tx_id and writes to its store, then notifies raft via a `LocalCommitAdvanced` event. Raft replicates by emitting `SendAppendEntries` actions.
- **Follower.** Raft receives AppendEntries, validates §5.3 prev_log_term match, and directs the storage layer via `AppendLog` and `TruncateLog` actions. The follower's ledger is passive — apply only.

**Apply-on-commit, not apply-on-durable.** The ledger's apply pipeline is gated on raft's commit signal (the `AdvanceClusterCommit` action), not on raw storage durability. This eliminates the rollback-after-truncation problem.

## One Range Per AppendEntries

Raft's §5.3 prev_log_term check is a single (tx_id, term) pair. The library carries one *same-term contiguous range* per AppendEntries — a `(start_tx_id, count, term)` triple — never a heterogeneous batch. A range with `count == 0` is a heartbeat: no entries to append, just §5.3 / `leader_commit` propagation.

When a leader's log spans multiple terms, it ships one AE per same-term chunk, in order, walking the peer's `next_index` forward across term boundaries.

This shape lets the follower update its durable term-log with one `observe_term` call per AE, lets one `(prev_log_tx_id, prev_log_term)` pair carry the full §5.3 contract, and keeps payload bytes out of the library entirely (the driver moves bytes between storage and the wire on its own, indexed by tx_id).

## Two Commit Indexes

The library distinguishes them explicitly:

- **`commit_index`** (local). The largest tx_id this node has durably committed in its own log. On the leader, advanced when the ledger's on-commit hook fires. On followers, advanced when an AppendEntries write durably lands.
- **`cluster_commit_index`** (cluster-wide). The largest tx_id known to be quorum-committed across the cluster. On the leader, recomputed from per-peer `match_index` whenever an AppendEntries reply arrives. On a follower, the leader's `leader_commit` clamped to local `commit_index`.

These are different facts. The leader's `commit_index` can be ahead of `cluster_commit_index` (persisted locally but not yet quorum-acked). A follower's `commit_index` can be ahead of its `cluster_commit_index` (persisted entries the leader hasn't yet told it are committed cluster-wide).

Cluster-level reads (e.g. `wait_for_transaction_level`) and the apply pipeline use `cluster_commit_index`. The local `commit_index` is for raft's own bookkeeping and per-node introspection.

## Required Invariants

The design must enforce these under all schedules:

1. **Election Safety (§5.2).** At most one leader per term. The candidate's self-vote is durably persisted before any RequestVote goes out; the term log is committed atomically on election win, not on candidacy entry. A `commit_term(expected_new, start_tx_id)` returning `Ok(false)` means a concurrent observer already advanced past `expected_new` — the candidate must step down.

2. **Log Matching (§5.3).** AppendEntries rejection due to prev_log mismatch triggers `next_index[peer] -= 1` (clamped at 1), not exponential backoff. Followers truncate divergent suffixes when prev_log_term mismatches, and the term log is truncated synchronously alongside the entry log so `term_at_tx` cannot return phantom records past the new high-water mark.

3. **Leader Completeness / Figure 8 (§5.4.2).** A leader does not commit entries from prior terms by counting replicas. Once an entry from the leader's *current* term has been committed by replica count, prior entries follow via the Log Matching property. The library tracks the leader's current-term first-entry watermark and gates `cluster_commit_index` advancement on it. Single-node clusters bypass the gate (no peer can disagree, so no future leader can overwrite).

4. **No boot-time term bumping.** A node coming back up sits at its persisted term; only candidates running an actual election bump term. The constructor never advances state.

5. **Durability before externalisation.** Two cases: (a) Term-log and vote-log writes go through a synchronous `Persistence` trait — when the trait returns `Ok`, the state is durable, and only then does the library externalise its consequences (replies, role transitions). (b) The follower's `AppendEntries success` reply and any `cluster_commit` advance derived from `leader_commit` wait for the driver's `LogAppendComplete` ack before being emitted; the leader cannot count an entry as durably replicated on a peer until the peer has actually written it.

6. **Fatal-on-unrecoverable-persistence-failure.** If the persistence trait returns `Err` from a write that, if the library proceeded, would diverge in-memory state from on-disk state (term-log truncate, term observation), the library emits a fatal-error action, freezes the node, and refuses further forward progress. The driver must shut the node down on receipt — partial state recovery from this point is not safe. (Errors that leave the on-disk state untouched, like a refused vote or a refused `commit_term`, are recoverable and the library reacts in-band.)

7. **Term log allows non-contiguous forward jumps.** The term log can validly skip terms — a node that observed a higher term via RPC (vote-log advance) but did not receive entries from that term will not have a term-log record for it, and a later election win at an even higher term commits a non-adjacent record. `commit_term` is gated on `current < expected`, not `current + 1 == expected`. Ranged term-log records mean each record covers the inclusive interval starting at `start_tx_id` until the next record (or open-ended if no next record exists).

8. **Quorum monotonicity.** Per-peer `match_index` is monotonically non-decreasing on success. `cluster_commit_index` itself is monotonic — out-of-order, late, or duplicated peer acks cannot lower it. A defensive `regress` lowers a peer's slot when a partially-recovered peer resurfaces with a shorter durable log, but the watermark holds.

## What raft Owns

- Role state and transitions (Initializing, Candidate, Leader, Follower, plus a frozen-on-fatal terminal).
- Election-timer state — deadlines as data, not as `tokio::sleep` instances.
- Per-peer replication state (next_index, match_index, in-flight tracking) — internal, not exposed.
- Quorum / cluster_commit_index calculation.
- The Figure 8 watermark for the leader's current term.
- The deferred-reply slot on the follower (one outstanding success reply at a time, parked until durability ack).
- All policy decisions: when to truncate, when to apply, when to send AppendEntries, when to grant a vote, when to step down, when to declare the node fatally failed.

## What raft Does NOT Own

- **Durable persistence of the term and vote logs.** A `Persistence` trait describes the durable contract; the driver instantiates whatever storage layer it likes (production: disk-backed term/vote files; tests: an in-memory fake) and hands the implementation to `RaftNode` at construction. Trait writes are synchronous and atomic per call.
- The entry-log payload bytes and entry-log durability mechanics. The library asks the driver to append or truncate via actions; the driver acks via events.
- Tx_id assignment (the ledger does this on the leader).
- Application of committed entries (the ledger's pipeline does this; raft just signals "tx_id N is committed cluster-wide").
- Network transport (the driver translates `SendAppendEntries` to a wire RPC).
- Tokio / async (the driver wraps the library in async).
- Wall-clock time (the library observes time only through the `now` parameter to `step`).
- Filesystem paths and `data_dir` configuration.

## Persistence trait contract

A single trait covers durable term-log and vote-log access. Reads are cheap, side-effect-free queries. Writes (record election win, record observed higher term, truncate term log, grant vote, observe higher vote-log term) are synchronous — when the call returns `Ok(_)`, the state is durable on the underlying medium. The library does not maintain a "pending" mirror.

The trait's `truncate_term_after` must use rename-based atomic replacement so a crash mid-truncate leaves the original file intact. Other writes are atomic at single-call granularity.

The vote log carries its own per-term `voted_for` slot, kept in sync with the term log via an `observe_vote_term` call. The library calls both `observe_term` and `observe_vote_term` whenever it observes a higher term via inbound RPC. The vote log can validly race ahead of the term log when a candidate has self-voted at term N but not yet won; the read-side `current_term()` returns `max(term-log term, vote-log term)`.

## Read-only query API

The driver consumes a small, fixed set of queries: current role, current term, local commit index, cluster commit index, voted-for. The role is enough for gRPC handlers to gate writes (Leader accepts, others reject) and to stamp wire responses; the cluster commit index is what `wait_for_transaction_level` and similar consumers query.

Queries are cheap and side-effect-free. No peer state, election deadlines, log extents, or per-peer introspection are exposed in v1 — they are observability concerns, not decision inputs. Add when there is a concrete consumer.

## Timeout Handling

`RaftNode` cannot call `tokio::time::sleep`, cannot spawn tasks, cannot wake itself. Yet Raft fundamentally depends on time. The library cannot *measure* time — it can only *reason about* time.

### Pattern: deadlines as data

Every timeout is represented as an `Instant` deadline owned by the library:

1. The library computes the deadline (e.g. "election fires at `now + random(150..300ms)`").
2. Stores it in internal state.
3. Emits a `SetWakeup { at: Instant }` action whenever the next deadline changes.

The driver receives the deadline, schedules a sleep that fires at it, and on fire calls `step(now, Tick)`. The library checks "has any deadline elapsed?" on each `Tick` and reacts.

The library never sleeps. The driver sleeps. The library only checks "is `now >= deadline`?" on each `Tick`.

### Three timeout categories

**Election timeout** (Follower / Candidate / Initializing). If no leader heartbeat arrives within the window, transition to Candidate and start an election. The window is randomised within a `[min_ms, max_ms]` range to prevent split votes. Reset on a valid AppendEntries from the current leader, on granting a vote, or on entering Candidate.

**Heartbeat interval** (Leader). The leader sends AppendEntries to every peer at a regular interval to suppress their election timers. Per-peer next-heartbeat tracking; on `Tick`, scan peers and emit a fresh `SendAppendEntries` for each due peer that has no in-flight RPC.

**RPC deadline** (Candidate / Leader). An RPC fired at a peer might never come back. The library tracks per-RPC `expires_at`. On `Tick`, expired RPCs are treated as failed replies.

### Why this works

**Determinism.** The library's behaviour is a function of `(state, event_stream)`. No hidden time dependency. Tests feed `Tick` events with chosen instants and assert on outputs. The simulator can compress 10 minutes of cluster activity into 50 ms of test time.

**No async leakage.** No `tokio::time` in the library. No `Future`. No `async fn`. The library compiles without an async runtime.

**Crisp testability of timeout-sensitive bugs.** Timer race conditions become trivial to reproduce: feed events in a chosen order, assert on actions. No real clock involved.

### Election timeout randomisation

`RaftNode::new` takes a `seed: u64`. The internal RNG is seeded from this. Tests use fixed seeds for reproducibility; production uses time-based seeding done by the driver. "Raft owns its own RNG, seeded externally" is a clean abstraction with less round-tripping than the alternative of routing randomness through events.

`RaftConfig::validate()` checks the configured timer windows at construction: the heartbeat interval must be safely below the minimum election timeout (so a single missed heartbeat doesn't trigger an election storm), and the `[min, max]` election-timer range must be non-empty.

### What this rules out

- Internal channels or notify primitives — would mean raft is wired into a runtime.
- An `async fn step` — raft must be synchronous; the driver handles async.
- Background tasks inside the library.
- Direct calls to `Instant::now()` inside the library.
- Disk I/O inside the library — durability is the driver's job, mediated by the `Persistence` trait and the entry-log action/event ack pairs.

## Test Strategy

**Unit tests** inside the source modules: in-isolation tests of role transitions, the quorum / commit-index calculator, the election timer, the deferred-reply slot, persistence semantics.

**Integration tests** drive a single `RaftNode` through chosen event sequences and assert on the action stream. Each known-bug class has a regression test that fails on the buggy behaviour and passes after the fix; this includes the recently-added durability-before-reply, fatal-on-unrecoverable-failure, and Figure 8 gate.

**Property tests** verify the four §5.4 safety properties (Election Safety, Log Matching, Leader Completeness, State Machine Safety) hold across randomised event schedules.

**Deterministic simulator** owns N `RaftNode` instances behind a fake clock and a fake message bus. It drives the cluster through scheduled events with fault injection (drop, reorder, duplicate, partition, crash + restart) and asserts safety invariants throughout. Designed for high schedule counts per CI run.

**Read-API consistency**: query results agree with the action stream emitted by `step` at every transition. If a step returns a role-transition action, the role query must reflect the new role immediately after.

**Timer tests**: time enters only through `Tick`, so every timer scenario is reproducible from a chosen sequence of `Tick` events.

## Non-Goals (v1, deferred)

- **Pre-vote.** A partitioned node that bumps term repeatedly can disrupt the cluster on rejoin. Acceptable for v1.
- **Leader leases / read-index.** All reads go through the leader and use the same commit-index check as writes.
- **Joint consensus / online membership changes.** Cluster membership is fixed at config-file time.
- **InstallSnapshot.** Scaffold only; full implementation deferred.
- **Observability surface.** No peer state, election deadlines, status, or per-peer introspection in v1. Add when there is a concrete consumer.

## Success Criteria

The library is correct when:

1. The four §5.4 safety properties hold under the property test suite at the configured schedule count per CI run.
2. The deterministic simulator runs the configured number of random schedules without invariant violations.
3. The known bug classes from the prior `cluster::raft` implementation cannot recur: term-bump-before-win, ghost-term-on-boot, missing §5.3 truncation, term log drifting from the entry log.
4. The added invariants from this design hold in their dedicated regression tests: durability-before-reply, fatal-on-unrecoverable-persistence-failure, Figure 8 prior-term commit guard.
5. Read-API consistency: query results agree with the action stream emitted by `step` at every transition.
6. All timer-sensitive scenarios are reproducibly testable through synthetic `Tick` sequences with no real-clock dependency.
