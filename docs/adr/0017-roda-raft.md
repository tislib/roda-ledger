# roda-raft Design Summary

## Goals

Extract a pure, deterministic Raft state machine as a separate crate inside the roda-ledger workspace. The library is specialized to roda-ledger (not generalized), independent of cluster/ledger code, and testable in isolation with a deterministic simulator. The current `cluster::raft` module is replaced; existing bugs (term-bump-before-win race, missing §5.3 truncation logic, ghost-term bumping on boot) are addressed by the new design.

## Architectural Boundary

**Pure state machine.** The library exposes a primary entry point for state transitions plus a set of read-only query methods for the driver to inspect current state without mutating it.

```rust
impl RaftNode {
// Primary state transition entry point.
pub fn step(&mut self, event: Event) -> Vec<Action>;

    // Read-only queries — &self, no side effects, cheap.
    pub fn role(&self) -> Role;
    pub fn current_term(&self) -> Term;
    pub fn voted_for(&self) -> Option<NodeId>;
    pub fn leader_id(&self) -> Option<NodeId>;
    pub fn commit_index(&self) -> u64;
    pub fn last_log_index(&self) -> u64;
    pub fn last_log_term(&self) -> Term;

    // Per-peer replication state (leader only; returns empty otherwise).
    pub fn peer_state(&self, peer: NodeId) -> Option<PeerState>;
    pub fn all_peer_states(&self) -> Vec<(NodeId, PeerState)>;

    // Election timing — for observability and the driver's tick scheduler.
    pub fn election_deadline(&self) -> Option<Instant>;

    // Aggregate snapshot for status RPCs / debugging — one call instead of many.
    pub fn status(&self) -> RaftStatus;
}
```

No internal threads, no tokio, no tonic, no I/O, no timers. The library transitions state and returns actions describing what the driver must do. The driver (cluster crate) is responsible for executing actions and feeding results back as new events.

**Crate constraints (enforced via `Cargo.toml`):**

- `raft` depends only on minimal utility crates (serde, thiserror, spdlog-rs, etc.)
- `raft` does NOT depend on `tokio`, `tonic`, `proto`, `ledger`, or `cluster`
- If any of these creep in, the boundary is broken

## Log Ownership Model: Option 3

**Tx_id is assigned by the leader's ledger, not by raft.** AppendEntries propagates leader-assigned tx_ids to followers. Followers accept these as authoritative.

**Asymmetric write paths:**

- **Leader:** Ledger assigns tx_id, writes to its store, then notifies raft via `Event::LocalEntryAppended`. Raft replicates via `Action::SendAppendEntries`.
- **Follower:** Raft receives AppendEntries, validates §5.3 prev_log_term match, directs the storage layer via `Action::AppendLog` and `Action::TruncateLog`. Follower's ledger is passive — apply only.

**Apply-on-commit, not apply-on-durable.** The ledger's apply pipeline is gated on raft's commit signal (`Action::AdvanceCommit { tx_id }`), not on storage durability. This eliminates the rollback-after-truncation problem.

## Required Invariants

The design must enforce these under all schedules:

1. **Election Safety.** At most one leader per term. The candidate path holds the node's serializing lock across `[read current_term → run RPCs → commit term on Won]`, and the term commit atomically checks `current_term + 1 == new_term`.

2. **Log Matching (§5.3).** AppendEntries rejection due to prev_log mismatch triggers `next_index[peer] -= 1` (or jump-back-by-conflicting-term optimization), not exponential backoff. Followers truncate divergent suffixes when prev_log_term mismatches.

3. **Term log truncation.** When log entries past tx_id N are truncated, the term log truncates records with `start_tx_id > N`. `Term::truncate_after(tx_id)` is part of the term-log API.

4. **No boot-time term bumping.** Nodes coming back up sit at their persisted term; only candidates running an actual election bump term.

5. **Atomic term commit on election win.** `Term::commit_term(expected_new: u64, start_tx_id: u64)` asserts `current + 1 == expected_new` and rejects if violated. Term log is NOT written until election victory.

## What raft Owns

- Term log (`Term`, durable, raft's own file)
- Vote log (`Vote`, durable, raft's own file)
- Role state and transitions (`Initializing`, `Candidate`, `Leader`, `Follower`)
- Election timer state (deadlines as data, not as `tokio::sleep`)
- Replication state per peer (`next_index`, `match_index`, in-flight tracking)
- Quorum / commit_index calculation
- All policy decisions: when to truncate, when to apply, when to send AppendEntries, when to grant a vote

## What raft Does NOT Own

- Log bytes or log durability mechanics (the storage layer is external; raft requests operations via `Action`)
- Tx_id assignment (ledger does this on the leader)
- Application of committed entries (ledger's pipeline does this; raft just signals "tx_id N is committed")
- Network transport (driver translates `Action::SendAppendEntries` to a tonic RPC)
- Tokio/async (driver wraps the library in async)

## Public API

### Event/Action — state transitions (sketch, refined during implementation)

```rust
pub enum Event {
Tick { now: Instant },

    // Inbound RPCs
    AppendEntriesRequest {
        from: NodeId,
        term: Term,
        prev_log_tx_id: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesReply {
        from: NodeId,
        term: Term,
        success: bool,
        last_tx_id: u64,
        reject_reason: Option<RejectReason>,
    },
    RequestVoteRequest {
        from: NodeId,
        term: Term,
        last_tx_id: u64,
        last_term: Term,
    },
    RequestVoteReply {
        from: NodeId,
        term: Term,
        granted: bool,
    },

    // Leader-only: ledger reports it durably wrote an entry
    LocalEntryAppended { tx_id: u64, term: Term },

    // Driver acks for actions raft requested
    LogAppendComplete { tx_id: u64 },
    LogTruncateComplete { up_to: u64 },
}

pub enum Action {
PersistTerm { term: Term, start_tx_id: u64 },
PersistVote { term: Term, voted_for: NodeId },

    SendAppendEntries {
        to: NodeId,
        term: Term,
        prev_log_tx_id: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    SendAppendEntriesReply {
        to: NodeId,
        term: Term,
        success: bool,
        last_tx_id: u64,
    },
    SendRequestVote {
        to: NodeId,
        term: Term,
        last_tx_id: u64,
        last_term: Term,
    },
    SendRequestVoteReply {
        to: NodeId,
        term: Term,
        granted: bool,
    },

    // Follower: raft directs the log layer
    TruncateLog { after_tx_id: u64 },
    AppendLog { tx_id: u64, term: Term, payload: Bytes },

    // Both: signal commit advanced (ledger should apply through here)
    AdvanceCommit { tx_id: u64 },

    // Role transition
    BecomeRole { role: Role, term: Term },
}
```

### Read-only query API

The driver and gRPC handlers need to inspect raft state on every RPC (e.g. "am I leader?" before accepting a write, "what's the cluster commit watermark?" for client reads). These queries are `&self` — they don't mutate state, they don't return actions, they're cheap.

```rust
// Role and term — used by every gRPC handler to gate writes and to stamp wire responses.
pub fn role(&self) -> Role;
pub fn current_term(&self) -> Term;
pub fn voted_for(&self) -> Option<NodeId>;

// Who's the leader? `None` if no current leader is known (e.g. Initializing or election in progress).
pub fn leader_id(&self) -> Option<NodeId>;

// Cluster-wide commit watermark — the largest tx_id known to be quorum-committed.
// On the leader, this is the recomputed majority. On followers, this is the leader's
// most-recent leader_commit clamped to local last_tx_id.
// This replaces today's ClusterCommitIndex; readers (e.g. wait_for_transaction_level)
// query raft for it.
pub fn commit_index(&self) -> u64;

// Local log extent.
pub fn last_log_index(&self) -> u64;
pub fn last_log_term(&self) -> Term;

// Per-peer replication state — leader-only, returns None on followers/candidates.
pub fn peer_state(&self, peer: NodeId) -> Option<PeerState>;
pub fn all_peer_states(&self) -> Vec<(NodeId, PeerState)>;

pub struct PeerState {
pub next_index: u64,
pub match_index: u64,
pub in_flight: bool,
pub last_contact: Option<Instant>,    // when did we last hear back from this peer
}

// Election timing — useful for the driver to schedule its tokio sleep correctly,
// and for observability dashboards. Returns None when election timer is disabled
// (e.g. while Leader).
pub fn election_deadline(&self) -> Option<Instant>;

// Aggregate snapshot — single call for status RPCs / debug endpoints / metrics.
pub fn status(&self) -> RaftStatus;

pub struct RaftStatus {
pub node_id: NodeId,
pub role: Role,
pub current_term: Term,
pub voted_for: Option<NodeId>,
pub leader_id: Option<NodeId>,
pub commit_index: u64,
pub last_log_index: u64,
pub last_log_term: Term,
pub election_deadline: Option<Instant>,
pub peers: Vec<(NodeId, PeerState)>,
}
```

**Why these exist as queries, not actions:** queries describe *current state* the driver needs to react to (e.g. "should I accept this write?"), not *future state changes* the driver needs to execute. Putting them in `Action` would force the driver to remember state across `step` calls or cache fields that raft already owns. Read methods give the driver a fresh, authoritative view on demand without coupling.

**Why these are not in `Event`:** queries don't change raft state, so they don't fit the event/action pattern. They're side-effect-free reads.

## What replaces `ClusterCommitIndex`

The current `ClusterCommitIndex` (atomic u64 read by `LedgerHandler::wait_for_transaction_level` and friends) is replaced by `RaftNode::commit_index()`. The driver provides whatever wrapper it needs — typically an `Arc<RwLock<RaftNode>>` or an `Arc<AtomicU64>` mirror that the driver updates whenever it processes an `Action::AdvanceCommit`. The library itself just reports the value.

This means the cluster crate's `LedgerHandler` no longer holds a separate `Arc<ClusterCommitIndex>` — it holds a handle to the driver, and asks the driver "what's the current commit index?" which delegates to `raft.commit_index()`.

## Workspace Layout

```
crates/
  raft/                           # this design
    src/
      lib.rs
      node.rs                     # RaftNode + step() + read-only queries
      event.rs                    # Event enum
      action.rs                   # Action enum
      role.rs                     # Role state machine
      leader.rs                   # leader-specific state
      candidate.rs                # candidate-specific state
      follower.rs                 # follower-specific state
      term.rs                     # term log (durable, raft-owned)
      vote.rs                     # vote log (durable, raft-owned)
      timer.rs                    # election timer (deadlines as data)
      quorum.rs                   # match_index tracking, commit_index calculation
      status.rs                   # PeerState, RaftStatus types
    tests/
      simulator.rs                # deterministic harness
      election_safety.rs          # property tests for §5.4
      log_matching.rs
      leader_completeness.rs
      state_machine_safety.rs
      fault_injection.rs
```

## Test Strategy

**Unit tests** inside `crates/raft/src/` modules: `Term`, `Vote`, `Quorum`, `ElectionTimer`, `Role` transitions in isolation.

**Property tests** in `crates/raft/tests/`: §5.4 safety properties (Election Safety, Log Matching, Leader Completeness, State Machine Safety) verified across randomized event schedules using `proptest`.

**Deterministic simulator** in `crates/raft/tests/simulator.rs`: fake clock, fake message bus. Drives N RaftNode instances through millions of randomly-ordered events. Asserts invariants hold throughout. Designed to run 10K schedules per CI invocation.

**Fault injection in the simulator:** message drop, reorder, duplication, network partition, node crash and recovery mid-operation.

**Read-API tests:** assert that `role()`, `current_term()`, `commit_index()`, etc. return values consistent with the actions the same `step` call returned. (E.g. if `step` returned `Action::BecomeRole { role: Leader, term: 5 }`, then `role()` must return `Leader` and `current_term()` must return 5 immediately after.) These are cheap consistency checks that catch a whole class of bugs where the read API drifts from the action stream.

## Non-Goals (v1, deferred to later)

- **Pre-vote.** Document the assumption: partitioned nodes that bump term repeatedly can disrupt the cluster on rejoin. Acceptable for v1.
- **Leader leases / read-index.** All reads go through the leader and use the same commit-index check as writes.
- **Joint consensus / online membership changes.** Cluster membership is fixed at config-file time.
- **InstallSnapshot.** Scaffold only; full implementation deferred.

These are documented in `crates/raft/README.md` so consumers know what the library does and doesn't guarantee.

## Success Criteria

The new library is correct when:

1. The four §5.4 safety properties hold under the property test suite (10K schedules per run).
2. The deterministic simulator runs 1M+ random events without invariant violations.
3. Bug class regressions from the previous implementation cannot recur: term-bump-before-win, ghost-term-on-boot, missing §5.3 truncation, term log not truncating alongside log truncation.
4. Read-API consistency: query results agree with the action stream emitted by `step` at every transition.