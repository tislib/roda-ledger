# ADR-018: Fault Injection Framework

**Status:** Proposed
**Date:** 2026-05-27
**Author:** Taleh Ibrahimli

---

## Context

roda-ledger's correctness story rests on a stack of testing layers,
each effective at a different scope:

- **Pure state-machine tests** in `crates/raft`
  ([crates/raft/tests/common/mod.rs](../../crates/raft/tests/common/mod.rs))
  — the deterministic simulator covers election safety, log
  matching, and leader completeness with no IO or network at all.
- **Single-component integration tests** in `crates/ledger` and
  `crates/storage`
  ([crates/ledger/tests/corruption_tests.rs](../../crates/ledger/tests/corruption_tests.rs),
  [crates/ledger/tests/tail_partial_recovery_test.rs](../../crates/ledger/tests/tail_partial_recovery_test.rs))
  — drive a real `Ledger` against a real disk, but only the
  *deliberate* faults the test author constructs by hand
  (renaming files, byte-editing a WAL).
- **In-process cluster tests**
  ([crates/cluster/tests/idle_cpu_test.rs](../../crates/cluster/tests/idle_cpu_test.rs)
  uses `ClusterTestingControl`) — bring up a real 1-to-N node
  cluster on threads of the test process. Real consensus, real
  replication, real-enough IO. No way to *force* a specific fault
  pattern.
- **E2E scenarios** ([ADR-012](0012-e2e-testing.md)) — real
  processes, real disks, real network. Slowest signal; faults
  surface only when timing happens to align.

The structural gap: nothing between layers 2 and 3 lets a test
**deterministically** subject the live consensus + replication
stack to adversarial IO or network conditions. As the system
matures, every new class of failure that gets discovered tends to
manifest first at the e2e layer — slow to detect, slow to
reproduce, slow to fix. We need a deterministic harness that
makes failure modes a *first-class input* to a test, not an
accident of timing.

The decision: introduce a **fault-injection framework** at two
layers (Ledger and Cluster), plus a `MockNode` family that lets
tests drive the real consensus / replication code against
programmable IO and network. Goal: every failure mode the system
should survive has a corresponding fast, deterministic
integration test that *forces* it.

---

## Decision

Three independent additions, each behind the workspace
`fault-injection` Cargo feature so production builds compile to the
same machine code as today:

1. **`LedgerFaultInjector`** — a seam in `crates/storage` that
   wraps disk IO with fault hooks. Lets tests drive each disk
   call on the axis `{ Normal, Error, Slow, Stuck }`, plus
   out-of-band file-level events (lose a file, corrupt a sealed
   segment, overwrite a byte range) at the exact call sites that
   handle WAL, snapshot, and raft-durable data.

2. **`ClusterFaultInjector`** — an injection layer in
   `crates/cluster` that intercepts (a) outbound replication-driver
   messages between leader and followers and (b) the role-state
   transitions inside the consensus driver. Lets tests drop /
   truncate / reorder / duplicate / delay messages and force
   role-flip moments.

3. **`MockNode` family** — `MockClusterNode`, `MockLeaderNode`,
   `MockFollowerNode` types in
   `crates/cluster/src/testing/mock/`, alongside the existing
   `ClusterTestingControl`. Each is a stripped-down node that runs
   the real consensus / replication-driver logic against a
   programmable event source instead of real disk + network. Builds
   a deterministic harness for scenarios that today require
   spinning a full process cluster.

All three are gated by a single workspace `fault-injection` feature.
CI runs `cargo test --features fault-injection` once to exercise
the union.

---

## Architecture

### Ledger-level (`LedgerFaultInjector`)

A pluggable injector installed on `Storage` (or equivalently on
`Segment`) that the IO call sites consult before / after touching
the disk. The hooks are conditionally compiled — when the
`fault-injection` feature is off they collapse to nothing.

Injection points — exactly the call sites that touch disk for a
distinct data class, no more:

- **`Segment::write_pending_entries`** — the WAL writer's append.
- **`Segment::sync`** — `fdatasync` on the active segment.
- **`WalTailer::tail`** — the replicator's read of its own WAL.
- **`TermStorage::append`** / **`VoteStorage::append`** — raft
  durability.

Picking these higher-level entry points (rather than every
`File::write_at` / `read_at`) keeps production code unchanged and
the test surface explicit. Adding a new injection point is a
deliberate ADR amendment, not an accidental side effect of
refactoring storage internals.

Fault model: each IO call has an outcome on the axis
`{ Normal, Error, Slow, Stuck }`. Stuck is "block until the test
explicitly unstucks it" — distinct from Slow, which completes on
its own. Data-at-rest can additionally be **corrupted** (in-place
mutation between calls) or **lost** (file-level removal). The two
axes are orthogonal: a write can be `Stuck` *and* the file it
would have written to can be `Lost` from underneath it.

#### Per-operation faults

Every injection point supports these outcomes. The "operation"
column names the natural method on the injector trait the first
implementation PR will land:

| Fault | Operation | Effect | Invariants it stresses |
|---|---|---|---|
| `WriteError` | write | Return `io::Error` | EIO / ENOSPC handling on WAL append |
| `WriteCorruption` | write | Persist different bytes than the caller asked for (flip / truncate / re-pad) | CRC verification; silent-corruption detection |
| `SlowWrite(d)` | write | Sleep `d` before completing | Backpressure under degraded disk |
| `StuckWrite` | write | Park the write until the test calls `unstuck()` | Readers seeing partially-flushed state; crash-mid-write recovery |
| `WalSyncerPrepareError` | sync prepare | Return error from the syncer's prepare phase | Pre-fdatasync setup failures |
| `SyncError` | sync | Return `io::Error` from `fdatasync` | Quorum-stall and leader-exit semantics on fsync failure |
| `SlowSync(d)` | sync | Sleep `d` before fdatasync returns | Committer latency under disk pressure |
| `StuckSync` | sync | Park the sync until `unstuck()` | Kill-leader-mid-fsync semantics |

`StuckWrite` / `StuckSync` use a control channel exposed by the
injector: the test holds a handle and calls `unstuck(call_id)` to
release a parked operation. This is what lets a test set up
"leader's WAL writer is parked on fdatasync; now kill the leader;
restart it; assert the unfinished write was discardable" without
racing wall-clock timing.

#### File-level events (out of band)

These are not per-IO faults — they're test-driven mutations of the
on-disk state between calls. The injector exposes them as direct
methods the test invokes from the outside:

| Event | Effect | Invariants it stresses |
|---|---|---|
| `FileLost::ActiveWal` | Remove `wal.bin` while the ledger is live | Restart with no active segment |
| `FileLost::PastWal(id)` | Remove `wal_NNNNNN.bin` (sealed segment) | Recovery when a sealed segment is missing |
| `FileLost::Snapshot(id)` | Remove `snapshot_NNNNNN.bin` (and / or `.crc`) | Snapshot-load fallback |
| `FileLost::FunctionSnapshot(id)` | Remove `function_snapshot_NNNNNN.bin` | Function-registry recovery path |
| `WalFileCorrupted(id, mutator)` | Apply a byte mutator to a sealed WAL segment | Per-segment CRC sidecar invariants |
| `DataLoss::SegmentDataOverride(id, range, bytes)` | Overwrite a byte range inside a segment file | Targeted byte-precise corruption at any offset |

`WalFileCorrupted` and `SegmentDataOverride` take a precise byte-
level intent so a regression test can pinpoint the exact site of
any past corruption shape we want to keep guarding against.

#### Reproducibility

Every injector takes a seeded `StdRng`. A scenario spec is a
`Vec<(call_count_predicate, FaultDecision)>` plus an ordered list
of file-level events keyed by call-count milestones, so e.g.
"return `WriteCorruption` on the 7th `WalTailer::tail` call and
`StuckSync` on the 3rd `Segment::sync` until call 12" is
expressible and deterministic across runs.

Trait shape and exact signatures are deferred to the first
implementation PR; this ADR commits only to *where* the hooks live
and *what classes of fault* they must support.

### Cluster-level (`ClusterFaultInjector`)

`ClusterFaultInjector` **composes** `LedgerFaultInjector` — it owns
one and exposes it as `cluster_injector.ledger()` so any disk-level
fault is reachable from a cluster-scope test without juggling two
handles. A cluster scenario like "lose `wal.bin` while leader is
mid-election" is one configuration on a single injector.

Five fault buckets, all configurable per-node and behind the same
workspace `fault-injection` feature. The first three reuse the
`{ Normal, Error, Slow, Stuck }` axis from the Ledger layer; the
last two are message- and consensus-shaped.

#### 1. `DiskAccessFault` — raft-durable IO

Term-log and vote-log writes share their own fault bucket because
their failure modes have consensus-level meaning (a vote that
isn't durable before the RPC reply violates §5.1). Backed by the
Ledger injector's `TermStorage::append` / `VoteStorage::append`
hooks; surfaced at cluster scope as a single named bucket so
tests don't need to know which file backs which side.

| Fault | Effect |
|---|---|
| `DiskAccessFault::Error` | Underlying append returns `io::Error` |
| `DiskAccessFault::Slow(d)` | Sleep `d` before completing |
| `DiskAccessFault::Stuck` | Park the append until `unstuck()` |
| `DiskAccessFault::Normal` | Pass-through (default) |

#### 2. Leader push faults

Wraps `run_peer_sender` in
[crates/cluster/src/consensus/replication_driver.rs](../../crates/cluster/src/consensus/replication_driver.rs).

| Fault | Effect |
|---|---|
| `LeaderPushError` | Treat the push as if the stream errored |
| `LeaderSlowPush(d)` | Sleep `d` before each `WalUpdate` is queued for send |
| `LeaderPushStuck` | Park the push loop until `unstuck()` |

#### 3. Follower replication faults

Wraps `apply_wal_update` (and the surrounding receive loop) on the
follower side.

| Fault | Effect |
|---|---|
| `FollowerReplicationError` | Return error from `apply_wal_update`; leader sees a rejected batch |
| `FollowerReplicationSlow(d)` | Delay each apply by `d` |
| `FollowerReplicationStuck` | Park the apply until `unstuck()`; leader's quorum stalls on this follower |

#### 4. Message-layer faults (`MessageInjector`)

Wraps the outbound side of `run_peer_sender` and the inbound side
of `apply_wal_update` at the message envelope, *before* the apply
hook in bucket 3.

| Operation | Effect |
|---|---|
| `Drop` | Message vanishes (network loss) |
| `Truncate(n)` | `wal_binary` shrunk to `n` bytes (partial delivery) |
| `Reorder` | Message queued and emitted out of order |
| `Duplicate` | Message delivered twice |
| `Delay(d)` | Message held for a configurable duration |

#### 5. Consensus-state faults (`ConsensusInjector`)

Wraps `Consensus::self_advance`, `RaftNode::election_start`, and
the role-transition path. Can force a role change, suppress a
heartbeat, or inject a stale term — the "make the leader think
it's still leader for one tick longer than reality" class of
fault.

Includes a `QuorumStaleMatchIndex` knob: force a peer's slot in
`Quorum` to report a stale `match_index` for one tick, so the
cached-majority's monotonicity (`fetch_max`) is exercised under
bad input.

#### 6. Election / handshake faults

Election and handshake have different correctness invariants than
steady-state replication (§5.1, §5.3, §5.4) and earn their own
bucket so tests don't have to express them through generic
`MessageInjector` rules.

| Fault | Effect |
|---|---|
| `RequestVoteDrop` | Vote request or response vanishes |
| `RequestVoteDelay(d)` | Vote response held for `d` |
| `RequestVoteLie::HigherTerm(t)` | Forge a vote reply at a term strictly higher than the candidate's |
| `RequestVoteLie::WrongGrant` | Reply `granted=true` when the §5.4.1 check should have refused |
| `HandshakeReject(reason)` | Follower rejects the leader's first `Handshake` with a chosen reject reason (term-mismatch, log-mismatch, etc.) |

#### 7. Liveness / health (`PingInjector`)

`Ping` ([proto/node.proto](../../crates/proto/proto/node.proto)) is
the operational health-check RPC and lives outside the consensus
hot path. Dropping or delaying pings creates "node looks dead to
monitors but is alive internally" scenarios that the rest of the
framework can't express.

| Fault | Effect |
|---|---|
| `PingDrop` | Ping request or response vanishes |
| `PingDelay(d)` | Ping response held for `d` |

#### 8. Network partitions (asymmetric)

A named primitive on top of `MessageInjector::Drop` filtered by
(peer, direction). Real partitions are often one-way at the
application layer, and composing them by hand from `Drop` rules
gets verbose fast.

| Fault | Effect |
|---|---|
| `Partition::TwoWay(a, b)` | All messages between `a` and `b` are dropped |
| `Partition::OneWay { from, to }` | Messages from `from` to `to` are dropped; the reverse direction still flows |
| `Partition::Heal(a, b)` | Restore normal delivery between `a` and `b` |

---

Tests pair an injector that *causes* a fault with a recorder that
*observes* what the system did, so a single test can assert both
"the fault was injected" and "the system responded correctly."
Production calls compile to nothing.

### Time and process-control primitives

Cross-cutting helpers that don't belong inside a single injector
bucket but are needed to make scenarios deterministic:

- **`ClockSkew(node, d)`** — applies a fixed per-node offset to
  every `Instant::now()` read through a wrapped clock. Election
  timers, heartbeat intervals, and the WAL committer's pacing are
  all wall-clock-driven; without per-node skew the framework can't
  deterministically stress timer-ordering edge cases.
- **`ClockJump(node, d)`** — one-shot jump forward by `d`, used to
  trip a timer once without leaving the clock skewed.
- **`KillMidFsync`** / **`KillMidElectionRound`** /
  **`KillMidHandshake`** — named helpers that install the
  corresponding `Stuck*` fault, wait until the system enters it,
  then `process::abort` the target. Doable by hand today via
  `LedgerFaultInjector::StuckSync` + external abort, but a named
  primitive makes the scenario readable.

Implementation uses `tokio::time::pause` plus a per-node
`MockClock` injected into the `Consensus` / `Ledger` time-read
paths. Both helpers are no-ops without the `fault-injection`
feature.

### Future buckets (placeholders, not v1)

- **`InstallSnapshotChunkLoss / Delay`** — once the
  `InstallSnapshot` handler in
  [crates/proto/proto/node.proto](../../crates/proto/proto/node.proto)
  moves past `UNIMPLEMENTED`, snapshot-transfer faults get the
  same `{ Drop, Truncate, Delay }` treatment as the steady-state
  message layer.

### MockNode family

Three layers, each a thin shim around the real component:

- **`MockClusterNode`** — owns a real `Consensus` + a real `Ledger`
  but replaces the network layer with a controllable shim and
  optionally swaps the disk layer for an in-memory or
  fault-injected variant. Drives scenarios like "leader sends one
  `WalUpdate` then dies; bring up a fresh node; assert the WAL is
  recoverable."

- **`MockFollowerNode`** — drives `apply_wal_update` directly,
  bypassing the gRPC layer. The test owns a `mpsc::Sender` that
  feeds synthetic `ReplicationLeaderMessage`s. Lets a test
  construct exact wire-byte sequences (well-formed, malformed,
  reordered, duplicated) and observe how the receive path
  responds.

- **`MockLeaderNode`** — drives `run_peer_sender` against a
  crafted WAL the test sets up directly. Lets a test answer
  "given leader's WAL with this specific layout, what bytes does
  the tailer ship to the follower?" without spinning up the full
  pipeline.

Reuse what already exists:

- The raft `Sim`
  ([crates/raft/tests/common/mod.rs](../../crates/raft/tests/common/mod.rs))
  is the state-machine layer beneath the MockNodes — they sit *on
  top* of `RaftNode`, the same way `Consensus` sits on top of
  `RaftNode` in production.
- `mem_persistence.rs`
  ([crates/raft/tests/common/mem_persistence.rs](../../crates/raft/tests/common/mem_persistence.rs))
  is the template for in-memory raft persistence.
- `ClusterTestingControl::cluster(N)` stays as the higher-tier
  harness for cases that genuinely need real processes. MockNodes
  are an *additional* tier, not a replacement.

---

## Test patterns the framework unlocks

The framework enables a library of deterministic scenarios that
today either don't exist or live only as flaky e2e probes. A
non-exhaustive catalogue, sorted by which layer drives them:

**Ledger-only (`LedgerFaultInjector`)**

- WAL append fails part-way; restart; assert recovery + balance.
- `fdatasync` returns `EIO`; assert leader exits cleanly rather
  than acking a non-durable commit.
- Active segment is `FileLost` while the ledger is running; assert
  recovery on next start either rebuilds from snapshot or fails
  loudly.
- A sealed segment is corrupted at a precise byte offset
  (`DataLoss::SegmentDataOverride`); assert the per-segment CRC
  detects it on next load.
- `SlowSync` raises sync latency to 200 ms under sustained writes;
  assert the WAL committer back-pressures the writer instead of
  silently dropping work.
- `StuckWrite` parks the WAL writer; concurrent reads must observe
  the pre-stuck snapshot; unstuck; assert the writer resumes
  without corruption.

**Cluster-only (`ClusterFaultInjector`)**

- `MessageInjector::Drop` on every other `AppendEntries`; assert
  the leader's retry path eventually converges quorum.
- `MessageInjector::Reorder` swaps two consecutive `WalUpdate`s;
  assert the follower either rejects out-of-order delivery or
  applies them in the correct sequence.
- `MessageInjector::Truncate` cuts a `WalUpdate` mid-batch;
  assert the follower handles partial delivery (reject, retry, or
  carry-over) without corrupting its on-disk state.
- `ConsensusInjector` forces a stale term on the next outbound
  message; assert the receiving leader steps down cleanly.
- `LeaderPushStuck` parks a peer's push; assert the cached
  quorum-majority continues to advance from other peers, and
  unstuck restores normal replication without state divergence.
- `FollowerReplicationError` rejects every other `WalUpdate`;
  assert the leader's retry/backoff converges and `next_tx`
  doesn't skip records.
- `DiskAccessFault::Stuck` on `vote.log` during an election round;
  unstuck after the candidate timed out; assert no two leaders
  ever appear in the same term.
- `RequestVoteLie::HigherTerm` against a candidate; assert
  candidate steps down to `Initializing` and observes the higher
  term durably.
- `HandshakeReject(LogMismatch)` returned to a leader's first
  contact; assert the leader follows the §5.3 truncate-after
  watermark and re-handshakes.
- `PingDrop` continuously; assert the cluster's gRPC handler still
  serves submits (Ping isn't on the commit path) and the
  monitor's "node down" judgement is reachable from another
  signal.
- `Partition::OneWay { from: leader, to: peer }`; assert the
  leader's view stalls quorum on that peer while the peer can
  still pull from the other live peer; heal; assert convergence.

**Time / crash-point primitives (cross-cutting)**

- `ClockSkew` two followers' clocks 100 ms apart; assert their
  randomised election timers still defeat split-vote storms
  (i.e. the randomisation range exceeds the skew).
- `ClockJump` past the next heartbeat deadline; assert the leader
  re-arms the wakeup correctly rather than spinning on a stale
  past-deadline (the class of bug we caught in `next_pending_wakeup`).
- `KillMidFsync` while the leader has acked a commit; restart;
  assert the on-disk WAL is recoverable and the surviving cluster
  agrees on the highest committed `tx_id`.

**MockNode-driven cross-layer**

- Craft a `MockFollowerNode` that receives wire bytes from a
  scripted source; kill+restart between deliveries; assert
  recovery + balance hold across every interleaving.
- `MockClusterNode` plus `LedgerFaultInjector::StuckSync` on the
  leader: trigger a leader election while the previous leader's
  fsync is parked; unstuck after the election; assert no two
  leaders ever commit conflicting tx_ids.

Each scenario is a deterministic unit / integration test runnable
on every CI push. The library accumulates over time so every
class of failure the system has been shown to survive is locked
in by an explicit test.

This is the inverse of the e2e strategy in [ADR-012](0012-e2e-testing.md):
e2e proves the *system* holds together under realistic conditions;
fault injection proves the *protocol* holds together under
adversarial ones. The two are complementary.

---

## Open questions

To be resolved in the first implementation PR:

- **Production overhead.** The ADR commits to "production builds
  compile to the same machine code as today." The implementation
  chooses the mechanism (trait monomorphisation, `cfg`-guarded
  matches, etc.) that demonstrably achieves this — verified by
  reading the generated `cargo asm` output for at least the
  `WalTailer::tail` and `Segment::write_pending_entries` paths.

- **Determinism boundary inside Mock tests.** Recommend
  `tokio::time::pause` so timing-dependent state transitions (e.g.
  the consensus loop's `tokio::time::sleep_until`) are reproducible
  without depending on wall-clock. To be confirmed when the first
  MockNode test that depends on timer behaviour lands.

- **gRPC interception layer.** `ClusterFaultInjector` currently
  sits *above* the gRPC layer (deciding what
  `ReplicationLeaderMessage`s to send / accept). Sitting *below*
  (corrupting bytes on the wire, frame-level faults) is needed for
  tonic-level bugs. Recommendation: start above, defer below until
  motivated by a concrete need.

---

## Out of scope

- **Production chaos engineering** (Pumba, ChaosMesh equivalents).
  That sits at the e2e layer and is covered — or not — by
  [ADR-012](0012-e2e-testing.md).
- **Generative fuzzing** (`proptest` / `bolero`) of full scenarios.
  This framework is the *substrate* that would make such fuzzing
  practical; the fuzz harness itself is its own ADR.
- **Compile-time invariant checking** (Loom, Shuttle). Same answer
  — the substrate is here; the harness is separate.
- **Library-grade `fail` / `failpoints`** integration. We adopt the
  same design pattern but a project-local injector keeps the
  surface narrow and tied to roda-ledger's specific call sites.
- **Memory / resource exhaustion** (allocator failure injection,
  RSS caps). The test harness can't safely intercept `alloc`
  without unsafe global state, and forcing OOM mid-run reduces to
  external `ulimit` + process kill — better expressed at the e2e
  layer.
