# roda-raft Design

## Goals

Extract a pure, deterministic Raft state machine as a separate crate inside the roda-ledger workspace. The library is specialized to roda-ledger (not generalized), independent of cluster/ledger code, and testable in isolation with a deterministic simulator. The current `cluster::raft` module is replaced; existing bugs (term-bump-before-win race, missing §5.3 truncation logic, ghost-term bumping on boot) are addressed by the new design.

`RaftNode` is a **single-node decision system**. It models one node's view of Raft: takes events in, returns actions out, exposes the minimum read surface the driver needs to gate writes and stamp wire responses. Anything that's not a decision input or a decision output does not belong in the library.

## Architectural Boundary

**Pure state machine.** No internal threads, no tokio, no tonic, no I/O — including no disk writes, no `data_dir`, no `Storage` handle. The library transitions in-memory state and returns actions describing what the driver must do. The driver (cluster crate) executes actions, performs the durable writes, and feeds results back as new events.

```rust
impl RaftNode {
    // Construction. The driver hydrates `(current_term, voted_for,
    // term_records, local_log_index)` off disk and hands them in.
    // Seed is for randomized election timeouts; tests use fixed seeds
    // for reproducibility, production seeds from time.
    pub fn new(
        node_id: NodeId,
        peers: Vec<NodeId>,
        seed: u64,
        cfg: RaftConfig,
        initial: RaftInitialState,
    ) -> Self;

    // Primary state transition entry point. `now` is the only way
    // wall-clock time enters the library — `Event::Tick` is the
    // explicit "wake and re-check" signal but every event rides this
    // `now`.
    pub fn step(&mut self, now: Instant, event: Event) -> Vec<Action>;

    // Read-only queries — &self, no side effects, cheap.
    pub fn role(&self) -> Role;
    pub fn current_term(&self) -> u64;
    pub fn commit_index(&self) -> u64;
    pub fn cluster_commit_index(&self) -> u64;
    pub fn voted_for(&self) -> Option<NodeId>;
    // Trails `current_term` between an `Action::Persist*` and its
    // matching `Event::*Persisted` ack — observability only.
    pub fn durable_term(&self) -> u64;
    pub fn durable_voted_for(&self) -> Option<NodeId>;
}
```

That is the entire public surface.

**Crate constraints (enforced via `Cargo.toml`):**

- `raft` depends only on minimal utility crates (`rand`, `spdlog-rs`, `thiserror`).
- `raft` does NOT depend on `storage`, `tokio`, `tonic`, `proto`, `ledger`, or `cluster`. The library performs zero I/O; durable persistence is the driver's responsibility.
- If any of these creep in, the boundary is broken.

## Log Ownership Model: Option 3

**Tx_id is assigned by the leader's ledger, not by raft.** AppendEntries propagates leader-assigned tx_ids to followers. Followers accept these as authoritative.

**Asymmetric write paths:**

- **Leader:** Ledger assigns tx_id, writes to its store, then notifies raft via `Event::LocalCommitAdvanced`. Raft replicates via `Action::SendAppendEntries`.
- **Follower:** Raft receives AppendEntries, validates §5.3 prev_log_term match, directs the storage layer via `Action::AppendLog` and `Action::TruncateLog`. Follower's ledger is passive — apply only.

**Apply-on-commit, not apply-on-durable.** The ledger's apply pipeline is gated on raft's commit signal (`Action::AdvanceClusterCommit { tx_id }`), not on storage durability. This eliminates the rollback-after-truncation problem.

## Two Commit Indexes

The library distinguishes them explicitly:

- **`commit_index`** — local. The largest tx_id this node has durably committed in its own log. On the leader, advanced when the ledger's on-commit hook fires (delivered as `Event::LocalCommitAdvanced`). On followers, advanced when an AppendEntries write durably lands.
- **`cluster_commit_index`** — cluster-wide. The largest tx_id known to be quorum-committed across the cluster. On the leader, recomputed from per-peer `match_index` whenever an AppendEntries reply arrives. On a follower, the leader's most-recent `leader_commit` clamped to local `commit_index`.

These are different facts. The leader's `commit_index` can be ahead of `cluster_commit_index` (it's persisted locally but not yet quorum-acked). A follower's `commit_index` can be ahead of its `cluster_commit_index` (it's persisted entries the leader hasn't yet told it are committed cluster-wide).

The cluster's writers use `cluster_commit_index` to gate things like `wait_for_transaction_level`. The local apply pipeline uses `cluster_commit_index` (apply-on-commit) — never `commit_index`.

## Required Invariants

The design must enforce these under all schedules:

1. **Election Safety.** At most one leader per term. The candidate path holds the node's serializing lock across `[read current_term → run RPCs → commit term on Won]`, and the term commit atomically checks `current_term + 1 == new_term`.

2. **Log Matching (§5.3).** AppendEntries rejection due to prev_log mismatch triggers `next_index[peer] -= 1` (or jump-back-by-conflicting-term optimization), not exponential backoff. Followers truncate divergent suffixes when prev_log_term mismatches.

3. **Term log truncation.** When log entries past tx_id N are truncated, the term log truncates records with `start_tx_id > N`. `Term::truncate_after(tx_id)` is part of the term-log API.

4. **No boot-time term bumping.** Nodes coming back up sit at their persisted term; only candidates running an actual election bump term.

5. **Atomic term commit on election win.** `Term::commit_term(expected_new: u64, start_tx_id: u64)` asserts `current + 1 == expected_new` and rejects if violated. Term log is NOT written until election victory.

## What raft Owns

- **In-memory model** of the term log (`Term`) and vote state (`Vote`). Pure data structures — no file handles, no storage crate, no I/O. Mutators update memory; the driver mirrors the change to disk via the action stream.
- Role state and transitions (`Initializing`, `Candidate`, `Leader`, `Follower`)
- Election timer state (deadlines as data, not as `tokio::sleep`)
- Per-peer replication state (`next_index`, `match_index`, in-flight tracking) — internal, not exposed
- Quorum / `cluster_commit_index` calculation
- All policy decisions: when to truncate, when to apply, when to send AppendEntries, when to grant a vote

## What raft Does NOT Own

- **Durable persistence of the term/vote logs.** The driver instantiates whatever storage layer it likes (`storage::TermStorage`, `storage::VoteStorage`, or otherwise), routes `Action::PersistTerm` / `Action::PersistVote` into it, and acks back via `Event::TermPersisted` / `Event::VotePersisted`.
- Log bytes and log durability mechanics for entry data (the storage layer is external; raft requests operations via `Action`).
- Tx_id assignment (ledger does this on the leader).
- Application of committed entries (ledger's pipeline does this; raft just signals "tx_id N is committed").
- Network transport (driver translates `Action::SendAppendEntries` to a tonic RPC).
- Tokio/async (driver wraps the library in async).
- Wall-clock time (raft observes time only through `step(now, _)`).
- Filesystem paths and `data_dir` configuration. The library does not know — and refuses to know — where the disk lives.

## Public API

### Event/Action — state transitions

Payload bytes do **not** flow through the library. `LogEntryMeta` carries only `(tx_id, term)` — enough for §5.3 prev-log-term matching and `match_index`/`next_index` arithmetic. The driver moves payload bytes between storage and the wire on its own, indexed by `tx_id`.

```rust
pub struct LogEntryMeta { pub tx_id: u64, pub term: u64 }

pub enum Event {
    // The driver fires Tick whenever an Action::SetWakeup deadline arrives.
    // Wall-clock time enters via the `now` parameter to step(), not via
    // a field on the variant itself.
    Tick,

    // Inbound RPCs
    AppendEntriesRequest {
        from: NodeId,
        term: u64,
        prev_log_tx_id: u64,
        prev_log_term: u64,
        entries: Vec<LogEntryMeta>,
        leader_commit: u64,
    },
    AppendEntriesReply {
        from: NodeId,
        term: u64,
        success: bool,
        last_tx_id: u64,
        reject_reason: Option<RejectReason>,
    },
    RequestVoteRequest {
        from: NodeId,
        term: u64,
        last_tx_id: u64,
        last_term: u64,
    },
    RequestVoteReply {
        from: NodeId,
        term: u64,
        granted: bool,
    },

    // Leader-only: ledger reports it durably committed an entry locally.
    // Bridge from the ledger's on-commit hook into raft. Advances the
    // leader's own slot in match_index, feeding into cluster_commit_index.
    LocalCommitAdvanced { tx_id: u64 },

    // Driver acks for log directives (follower path).
    LogAppendComplete { tx_id: u64 },
    LogTruncateComplete { up_to: u64 },

    // Driver acks for durable term/vote writes. The library has
    // already updated its in-memory model when emitting the matching
    // Action::Persist*; these events confirm the on-disk state caught
    // up and feed `durable_term()` / `durable_voted_for()`.
    TermPersisted { term: u64, start_tx_id: u64 },
    VotePersisted { term: u64, voted_for: NodeId },
}

pub enum Action {
    // Durable writes. The driver writes to its term log / vote log
    // (e.g. storage::TermStorage / VoteStorage) and feeds back the
    // matching Event::*Persisted. The library has already updated
    // its in-memory model when emitting these — the driver is
    // catching up.
    PersistTerm { term: u64, start_tx_id: u64 },
    PersistVote { term: u64, voted_for: NodeId },

    SendAppendEntries {
        to: NodeId,
        term: u64,
        prev_log_tx_id: u64,
        prev_log_term: u64,
        entries: Vec<LogEntryMeta>,
        leader_commit: u64,
    },
    SendAppendEntriesReply {
        to: NodeId,
        term: u64,
        success: bool,
        last_tx_id: u64,
    },
    SendRequestVote {
        to: NodeId,
        term: u64,
        last_tx_id: u64,
        last_term: u64,
    },
    SendRequestVoteReply {
        to: NodeId,
        term: u64,
        granted: bool,
    },

    // Follower: raft directs the log layer. TruncateLog implies
    // both the entry-log truncation and the paired term-log
    // truncation (§5.3). AppendLog metadata only — payload routing is
    // entirely on the driver.
    TruncateLog { after_tx_id: u64 },
    AppendLog { tx_id: u64, term: u64 },

    // Both: signal cluster commit advanced — ledger should apply through here
    AdvanceClusterCommit { tx_id: u64 },

    // Role transition
    BecomeRole { role: Role, term: u64 },

    // Time management — see "Timeout Handling" below
    SetWakeup { at: Instant },
}
```

### Initial state

The driver hydrates the durable state off disk and hands it in:

```rust
pub struct RaftInitialState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub term_records: Vec<TermRecord>,  // raft::TermRecord, oldest first
    pub local_log_index: u64,
}
```

Empty-everything (`RaftInitialState::default()`) is the fresh-install case. The library does no validation beyond what its mutators normally enforce — a corrupt or out-of-order record list is the driver's bug to catch.

### Two-phase persistence

ADR-0017's "etcd-raft shape": for any state change that needs to survive a crash, the library updates its **in-memory** model first, emits the matching `Action::Persist*`, and only later observes `Event::*Persisted` to update its `durable_*` view. The library does not gate further state-machine progress on the ack — it relies on the driver applying actions in the order returned (Persist before Send/Append) so durability stays ahead of externalisation.

`durable_term()` and `durable_voted_for()` exist purely for observability — tests, the driver's own invariant checks, and a future Pre-Vote layer might want to inspect the gap between intent and durable state.

### Read-only query API

```rust
// Current role of this node — used by gRPC handlers to gate writes
// (Leader accepts, others reject) and to stamp wire responses.
pub fn role(&self) -> Role;

// Current durable term known to this node.
pub fn current_term(&self) -> u64;

// Local: largest tx_id durably committed in this node's own log.
pub fn commit_index(&self) -> u64;

// Cluster-wide: largest tx_id known quorum-committed.
// On the leader, recomputed from peer match_indexes.
// On a follower, the leader's leader_commit clamped to local commit_index.
// This is what wait_for_transaction_level and similar consumers query.
pub fn cluster_commit_index(&self) -> u64;
```

That's it. Anything beyond these — peer states, election deadlines, leader id, log extents, voted_for — is internal to raft.

**Why no `leader_id`:** the role tells the driver everything it needs. A non-leader rejects writes regardless of who the leader is.

**Why no `voted_for`:** internal to raft's election logic. The driver has no decision to make based on it.

**Why no `last_log_index` / `last_log_term`:** raft doesn't own the log. The storage layer knows.

**Why no `peer_state` or `election_deadline` or `status`:** observability concerns, not decision inputs. v1 is the decision system, nothing more.

## Timeout Handling

`RaftNode` cannot call `tokio::time::sleep`, cannot spawn tasks, cannot wake itself. Yet Raft fundamentally depends on time. The library cannot *measure* time — it can only *reason about* time.

### Pattern: deadlines as data

Every timeout is represented as an `Instant` deadline owned by the library:

1. Library computes the deadline (e.g. "election fires at `now + random(150..300ms)`").
2. Stores it in internal state.
3. Emits `Action::SetWakeup { at: Instant }` whenever the next deadline changes.

The driver:

1. Receives the deadline.
2. Schedules `tokio::time::sleep_until(deadline)`.
3. When the sleep fires, calls `raft.step(Event::Tick { now: Instant::now() })`.
4. Raft checks "has any deadline elapsed?" and reacts.

The library never sleeps. The driver sleeps. The library only checks "is `now >= deadline`?" on each `Tick`.

### Three timeout categories

**Election timeout (Follower / Candidate / Initializing).** If no leader heartbeat arrives within the window, transition to Candidate and start an election. Internal `election_deadline: Instant`. Reset whenever a valid AppendEntries arrives, a vote is granted, or a new term begins. On `Tick` with `now >= election_deadline`, transition to Candidate and emit candidacy actions (`PersistTerm`, `PersistVote`, `SendRequestVote × N`).

**Heartbeat interval (Leader).** The leader sends AppendEntries to every peer at a regular interval to suppress their election timers. Per-peer `next_heartbeat: Instant`. On `Tick`, scan peers; for each with `now >= next_heartbeat` and no in-flight AppendEntries, emit a fresh `SendAppendEntries`.

**RPC deadline (Candidate / Leader).** An RPC fired at a peer might never come back. Library tracks per-RPC `expires_at: Instant`. On `Tick`, expired RPCs are treated as failed replies. (Alternatively, the driver enforces via `tokio::time::timeout` and synthesizes an `Event::AppendEntriesReply { reject_reason: RpcTimeout }` — either works.)

### Driver loop

```rust
loop {
    // After every step, scan actions for SetWakeup and re-arm sleep.
    let next_wakeup = current_wakeup;

    tokio::select! {
        _ = tokio::time::sleep_until(next_wakeup) => {
            let actions = raft.step(Instant::now(), Event::Tick);
            process_actions(actions).await;
        }
        msg = inbound_rpc.recv() => {
            let actions = raft.step(Instant::now(), Event::from(msg));
            process_actions(actions).await;
        }
        commit = ledger_on_commit.recv() => {
            let actions = raft.step(Instant::now(),
                Event::LocalCommitAdvanced { tx_id: commit });
            process_actions(actions).await;
        }
        // ... other event sources
    }
}

// Sketch — `process_actions` walks the action list in order. Persist
// actions write durably *before* any Send is issued, then the driver
// feeds the ack back through step(now, Event::*Persisted).
async fn process_actions(actions: Vec<Action>) {
    for a in actions {
        match a {
            Action::PersistTerm { term, start_tx_id } => {
                term_log.append(TermRecord { term, start_tx_id })?;
                term_log.sync()?;
                inject(Event::TermPersisted { term, start_tx_id });
            }
            Action::PersistVote { term, voted_for } => {
                vote_log.append(VoteRecord { term, voted_for })?;
                vote_log.sync()?;
                inject(Event::VotePersisted { term, voted_for });
            }
            Action::TruncateLog { after_tx_id } => {
                wal.truncate_after(after_tx_id)?;
                term_log.truncate_after(after_tx_id)?;
                inject(Event::LogTruncateComplete { up_to: after_tx_id });
            }
            Action::AppendLog { tx_id, .. } => {
                // payload was extracted from the inbound AE on receipt;
                // the driver writes (metadata + payload) here.
                wal.append(...)?;
                inject(Event::LogAppendComplete { tx_id });
            }
            Action::SendAppendEntries { .. } => grpc.send_append_entries(...).await,
            // ...
            Action::SetWakeup { at } => current_wakeup = at,
            Action::BecomeRole { .. } | Action::AdvanceClusterCommit { .. } => {
                // observability + apply-pipeline gating
            }
        }
    }
}
```

After every `step`, scan the action list for `SetWakeup` and re-arm `current_wakeup`. That's the entire interaction model.

### Why this works

**Determinism.** The library's behaviour is a function of `(state, event_stream)`. No hidden time dependency. Tests feed `Event::Tick { now: <chosen instant> }` and assert on outputs. The simulator can compress 10 minutes of cluster activity into 50ms of test time.

**No async leakage.** No `tokio::time` in the library. No `Future`. No `async fn`. The library compiles without an async runtime.

**Crisp testability of timeout-sensitive bugs.** Timer race conditions become trivial to reproduce: feed events in a chosen order, assert on actions. No real clock involved.

### Election timeout randomization

Raft requires randomized election timeouts to prevent split votes. The library is pure, so it can't call `rand::thread_rng()`.

`RaftNode::new` takes a `seed: u64`. Internal RNG is seeded from this. Tests use fixed seeds for reproducibility; production uses time-based seeding done by the driver. Less round-tripping than the alternative (driver provides randomness via events), and "raft owns its own RNG seeded externally" is a clean abstraction.

### What this rules out

- **Internal `tokio::sync::Notify` or channels** — would mean raft is wired into a runtime.
- **`async fn step`** — raft must be synchronous; the driver handles async.
- **Background tasks inside the library** — anything that needs to run "in the background" is the driver's job.
- **Direct calls to `Instant::now()` inside the library** — time enters only through the `now` parameter to `step`.
- **Disk I/O inside the library** — no `File`, no `fdatasync`, no `data_dir`. Durability is the driver's job, mediated by `Action::Persist*` and `Event::*Persisted`.

## Workspace Layout

```
crates/
  raft/
    src/
      lib.rs
      node.rs                     # RaftNode + step() + read-only queries
      event.rs                    # Event enum (incl. *Persisted ack events)
      action.rs                   # Action enum (incl. Persist* directives)
      role.rs                     # Role state machine
      leader.rs                   # leader-specific state
      candidate.rs                # candidate-specific state
      follower.rs                 # follower-specific state
      term.rs                     # in-memory term-log model (no I/O)
      vote.rs                     # in-memory vote model (no I/O)
      timer.rs                    # election timer (deadlines as data)
      quorum.rs                   # match_index tracking, cluster_commit_index calculation
      log_entry.rs                # LogEntryMeta { tx_id, term }
      types.rs                    # NodeId, RejectReason
    tests/
      common/mod.rs               # shared deterministic harness
      simulator.rs                # end-to-end scenarios
      safety_properties.rs        # proptest §5.4 properties
```

The driver lives outside the raft crate (today: `cluster::*`, future: any consumer). It owns the disk-backed term and vote logs (e.g. `storage::TermStorage`, `storage::VoteStorage`), instantiates them, and routes the action stream into them.

## Test Strategy

**Unit tests** inside `crates/raft/src/` modules: `Term`, `Vote`, `Quorum`, `ElectionTimer`, `Role` transitions in isolation.

**Property tests** in `crates/raft/tests/`: §5.4 safety properties (Election Safety, Log Matching, Leader Completeness, State Machine Safety) verified across randomized event schedules using `proptest`.

**Deterministic simulator** in `crates/raft/tests/simulator.rs`: fake clock, fake message bus. Drives N RaftNode instances through millions of randomly-ordered events. Asserts invariants hold throughout. Designed to run 10K schedules per CI invocation.

**Fault injection in the simulator:** message drop, reorder, duplication, network partition, node crash and recovery mid-operation.

**Read-API tests:** assert that `role()`, `current_term()`, `commit_index()`, `cluster_commit_index()` return values consistent with the action stream emitted by the same `step` call. (E.g. if `step` returned `Action::BecomeRole { role: Leader, term: 5 }`, then `role()` must return `Leader` and `current_term()` must return 5 immediately after.)

**Timer tests:** since time enters only through `Event::Tick { now }`, every timer-related scenario is reproducible. Tests construct specific `Tick` sequences and assert the resulting `Action` stream — no flakiness from real clocks.

## Non-Goals (v1, deferred to later)

- **Pre-vote.** Document the assumption: partitioned nodes that bump term repeatedly can disrupt the cluster on rejoin. Acceptable for v1.
- **Leader leases / read-index.** All reads go through the leader and use the same commit-index check as writes.
- **Joint consensus / online membership changes.** Cluster membership is fixed at config-file time.
- **InstallSnapshot.** Scaffold only; full implementation deferred.
- **Observability surface.** No `peer_state`, `election_deadline`, `status`, or per-peer introspection in v1. Add when there's a concrete consumer.

These are documented in `crates/raft/README.md` so consumers know what the library does and doesn't guarantee.

## Success Criteria

The new library is correct when:

1. The four §5.4 safety properties hold under the property test suite (10K schedules per run).
2. The deterministic simulator runs 1M+ random events without invariant violations.
3. Bug class regressions from the previous implementation cannot recur: term-bump-before-win, ghost-term-on-boot, missing §5.3 truncation, term log not truncating alongside log truncation.
4. Read-API consistency: query results agree with the action stream emitted by `step` at every transition.
5. All timer-sensitive scenarios are reproducibly testable through synthetic `Event::Tick` sequences with no real-clock dependency.