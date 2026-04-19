# ADR-015: WAL byte-range replication

**Status:** Proposed
**Date:** 2026-04-19
**Author:** Taleh Ibrahimli

**Amends:**
- ADR-010 — `WaitLevel::Committed` semantics change in multi-node mode
- ADR-011 — WAL committer extended with peer shipping

---

## Context

Roda-ledger is single-node today. This ADR adds multi-node
replication via a minimal Raft-style transport: the leader ships WAL
byte ranges to followers; followers append verbatim. Leader election,
term changes, and demote-and-recover are deferred to ADR-016.

Constraints:

1. No second fsync on the commit path.
2. The WAL is the source of truth. AppendEntries ships bytes from the
   existing WAL — not a separate Raft log.
3. Existing pipeline invariants stay intact.
4. No structure imposed on the WAL for Raft's benefit alone.

An earlier draft introduced a `BlockHeader` record and a `block_index`
to serve as the Raft log index. It was rejected: the block was a
transient abstraction with no end-user value that started driving the
writer's internal structure. The decision here is **one WAL record =
one Raft entry**; AppendEntries ships a contiguous byte range; batching
is whatever accumulated since the committer's last pass.

---

## Decision

### Raft log = WAL record stream

`RaftEntry = WalEntry`. Transactional records are indexed by `tx_id`.
Non-transactional records (`FunctionRegistered`, segment markers)
travel inside ranges without their own Raft index — they piggyback
between transactional records.

The consistency check runs on transactional boundaries:
`prev_tx_id` + `prev_term`.

### Term storage

- `raft_state` sidecar file — `{current_term, voted_for}`. Written
  once at init under ADR-015 (static leader, never updated). Exercised
  by ADR-016.
- `segment-N.term` per-segment sidecar — tx_id ranges and their term.
  Under ADR-015 each file holds one line (`term=1 tx_range=X-Y`).
- In-memory term index — reconstructed from `.term` sidecars on
  startup.

Term is not embedded in WAL records. Followers take term from the
`AppendEntries.term` envelope and write to their own `.term` files.

### Cluster configuration

A new `ClusterConfig` alongside `ServerConfig`:

```toml
[cluster]
mode    = "leader"           # or "follower"
node_id = 1

[[cluster.peers]]
node_id = 2
address = "10.0.0.2:50052"

[[cluster.peers]]
node_id = 3
address = "10.0.0.3:50052"
```

Validation: exactly one node in the cluster has `mode = "leader"`.
Static — no elections in this ADR.

### Ledger API gating

In follower mode, the Ledger rejects `submit` and `register_function`
with an error. Read APIs work on both roles.

### Pipeline composition

**Leader:** Sequencer → Transactor → WAL → Snapshot → Seal
(unchanged). The WAL committer gains peer shipping.

**Follower:** Replication → WAL → Snapshot → Seal. Transactor and
Sequencer are not started. The committer only fsyncs — no shipping.

### NodeHandler (gRPC server)

Implements `node.proto` on port 50052 (separate from the client API
on 50051).

RPCs:
- `AppendEntries`:
    - Leader mode: return `REJECT_NOT_FOLLOWER`.
    - Follower mode: hand off directly (synchronous call) to the
      Replication stage.
- `Ping`: works on both roles.
- `RequestVote`, `InstallSnapshot`: return `UNIMPLEMENTED` until
  ADR-016.

### Replication stage (follower only)

Lives in `pipeline/replication.rs`. Called directly from the gRPC
handler (no queue between them).

On each `AppendEntries`:

1. Validate: `request.term >= follower.current_term`,
   `prev_tx_id`/`prev_term` match the follower's log, per-record CRC,
   tx_id sequence within `wal_bytes`.
2. Push the incoming records into the WAL stage's inbound queue (the
   same queue the Transactor uses on a leader — Replication is the
   follower-side producer).
3. Wait for the WAL committer to advance `last_committed_tx_id` past
   `to_tx_id`. On a follower, committed = fsynced locally.
4. Return success with `last_tx_id`.

Segment markers (`SegmentHeader`, `SegmentSealed`) flow through in
replicated ranges but have no Raft-level meaning on the follower —
they're handed to the WAL stage like any other record.

### WAL writer (both roles): untouched

The writer cycle is unchanged. On a leader it's fed by the Transactor;
on a follower by the Replication stage. Same code, same queue.

One new shared variable: `last_write_position: Arc<AtomicU64>`.
Advanced by the writer after each flush, alongside `last_written_tx_id`.
Used by the committer to find the byte range to ship.

### Committer flow (leader)

Every pass:

1. Observe `last_write_position` (atomic). If unchanged since last
   pass, back off.
2. Compute the byte range:
   `[last_committed_position, last_write_position)` in the active
   segment. Read from the segment file into a buffer.
3. Spawn tokio tasks:
    - One per peer, calling `AppendEntries` with the byte range.
    - One for local `fdatasync`.
4. Wait for **all** tasks to complete. For this ADR, failure of any
   peer blocks commit progress (single-peer minimal implementation —
   quorum policy is a later refinement).
5. Advance `last_committed_tx_id` to `to_tx_id` and
   `last_committed_position` to the end of the shipped range.

Rotation is unaffected: rotation forces a commit first, so the
committer never sees a range that spans two segments. When the
committer observes a new active segment via `sync_id` change (existing
mechanism), it resets `last_committed_position` to just past the
`SegmentHeader`.

Peer connections are opened lazily on first `AppendEntries`.

### Committer flow (follower)

Same as today: fsync the active segment, advance
`last_committed_tx_id`. No peer shipping.

The follower's Replication stage waits on this watermark before
responding to the leader, so local fsync gates the ack.

### tx_id → byte position

Shared atomics (`last_write_position`, plus committer-private
`last_committed_position`) make this trivial: the committer doesn't
need a reverse lookup from tx_id to offset. It ships whatever bytes
lie between the two positions. The writer is the only source of truth
for position, and it publishes it atomically.

### WaitLevel::Committed semantics

Multi-node: "durable on leader's disk AND acked by all peers" (for
ADR-015's all-peers policy). Single-node: unchanged (durable locally).

No new WaitLevel introduced.

### Transport

Separate gRPC service on port 50052. Isolates client load from
replication traffic. Follows etcd / CockroachDB / TigerBeetle pattern.

---

## Consequences

### Positive

- No new abstractions on the WAL (no BlockHeader, no block_index).
  The `.term` sidecar is the only new on-disk artifact.
- Writer cycle is role-agnostic; leader and follower share writer
  code.
- Natural batching from the committer's pass cadence.
- Followers are simple: validate + append + fsync. No replay, no
  business logic.
- Follower balance cache stays in sync automatically via `TxMetadata`
  records in the replicated stream.
- Single fsync on the commit path; replication runs in parallel with
  fsync.

### Negative

- All-peers ack policy: any peer down blocks writes. Acceptable for
  ADR-015's minimal single-peer scope; quorum policy is a later
  refinement.
- `WaitLevel::Committed` latency increases in multi-node mode
  (network RTT + peer fsync).
- `AppendEntries.term` is the sole source of term for followers. A
  bug in the RPC envelope or `.term` writing could cause divergence
  between nodes' `.term` files. Mitigated by keeping both pieces
  small and unit-testable.
- Static cluster config — no failover in this ADR.

### Neutral

- Single-node deployment unchanged (empty peer list → committer skips
  shipping).
- WAL record format is unchanged.
- `raft_state` file created at init but never updated under ADR-015.

---

## Alternatives considered

**Block-based replication with `BlockHeader` records.** Rejected —
transient abstraction that started driving pipeline internals.

**Separate Raft log file.** Rejected — second fsync halves throughput.

**Per-record `term` field.** Rejected — term is sparse (changes only
on elections); sidecar matches the sparsity.

**Term only in `raft_state` (no per-range info).** Rejected — can't
reconstruct historical term assignments needed for Raft's Figure 8
commit safety rule.

**Queued handoff between NodeHandler and Replication.** Rejected —
direct call is simpler and the gRPC handler is already on a tokio
task.

**Quorum-ack policy (majority, not all peers).** Deferred — standard
Raft behavior and clearly correct, but ADR-015's scope is minimal
single-peer validation. Will revisit when adding the second follower.

---

## References

- ADR-006 — WAL, snapshot, seal durability
- ADR-010 — Wait levels
- ADR-011 — WAL write/commit separation
- ADR-014 — Function registry persistence
- Ongaro & Ousterhout 2014 — Raft paper