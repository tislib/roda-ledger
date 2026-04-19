# ADR-015: WAL byte-range replication

**Status:** Proposed
**Date:** 2026-04-19
**Author:** Taleh Ibrahimli

**Amends:**

- ADR-010 — `WaitLevel::Committed` semantics change in multi-node mode
- ADR-011 — WAL committer extended with peer shipping

---

## Context

Roda-ledger today is single-node. Multi-node replication is required
for production deployment — both for availability (surviving node
failure) and for data durability (surviving disk failure on a single
machine).

Raft is the consensus protocol of choice. This ADR addresses the first
step: **replication of the WAL record stream from leader to followers**
under a static cluster configuration with a manually designated
leader. Leader election, term changes, and demote-and-recover are
deferred to ADR-016.

The core constraints:

1. **No second fsync on the commit path.** Roda's existing WAL
   already performs one fdatasync per commit cycle. Adding a second
   for a separate Raft log would roughly halve throughput.
2. **The WAL is the source of truth.** Raft must not maintain its own
   separate log. AppendEntries ships bytes from the existing WAL.
3. **The pipeline's design invariants (single-writer Transactor,
   derived balances, WAL record as the atomic unit) must be
   preserved.** Raft-driven truncation, when introduced in ADR-016,
   will rely on the existing `Recover` mechanism rather than in-place
   rewinds.
4. **Replication must not impose structure on the WAL that exists only
   for Raft's benefit.** The WAL is a record stream the pipeline
   already produces for its own reasons; replication rides on top of
   it without changing its shape.

An earlier draft of this ADR introduced a block abstraction — a
`BlockHeader` WAL record type and a `block_index` that would serve as
the Raft log index. That approach was rejected during design review:
blocks were a transient abstraction with no end-user meaning that
would have become a driver of the writer's internal structure
(cycle == block, per-block transaction-completeness assertions,
role-specific follower block-tracking state). The decision here is to
treat **each WAL record as one Raft log entry** and ship WAL byte
ranges over the wire. Batching is implicit: whatever records the
committer finds durable on a given pass are shipped together in one
`AppendEntries`. No new record kind, no new on-disk structure, no new
concept in the pipeline.

---

## Decision

### Raft log = WAL record stream

`RaftEntry = WalEntry`. The Raft log is the WAL record stream, exactly
as the writer produces it today. Raft indexes WAL records by their
`tx_id`; non-transactional records (`FunctionRegistered`, segment
markers) travel inside `AppendEntries` byte ranges but do not have
their own Raft index — they are piggybacked between transactional
records in the stream.

The Raft consistency check runs on transactional boundaries:
`prev_tx_id` + `prev_term`. A follower accepting an `AppendEntries`
verifies that its own record at `prev_tx_id` was written under
`prev_term` before appending the incoming bytes.

### Term lives outside the WAL

Term is not embedded in WAL records. Each node maintains:

- A `raft_state` sidecar file holding `{current_term, voted_for}` for
  the node's own Raft state. Durable, fsync'd on change. Under
  ADR-015's static-leader model it is written once at initialization
  and never updated. Exercised by ADR-016's elections.
- A per-segment `segment-N.term` sidecar describing which tx_id
  ranges within that segment belong to which term. Append-only,
  written on every term transition (rare). Under ADR-015 this file
  contains a single line per segment (`term=1 tx_range=X-Y`) because
  the term never changes.
- An in-memory term index, reconstructed on startup by reading all
  `.term` sidecars. O(log n) lookup for "what term is tx_id X?" where
  n = number of elections over the cluster lifetime (typically tens,
  not thousands).

The `.term` sidecar parallels the existing `.crc` and `.seal`
sidecars: metadata *about* the WAL, not *in* it. Sealed segments
freeze their `.term` file alongside their `.crc`.

On a follower, the incoming `AppendEntries.term` (from the RPC
envelope) is the authoritative term for the bytes in that RPC. The
follower appends an entry to its active segment's `.term` whenever it
observes a term that differs from its last recorded term.

### AppendEntries carries WAL bytes

Each `AppendEntries` RPC carries a contiguous byte range from the
leader's WAL:

```
AppendEntries {
  leader_id,
  term,
  prev_tx_id, prev_term,     // Raft consistency check
  from_tx_id, to_tx_id,      // inclusive tx_id range
  wal_bytes,                 // raw WAL bytes, byte-identical to leader's WAL
  leader_commit_tx_id,
}
```

`wal_bytes` is what the leader wrote to its own WAL for the tx_id
range. The follower appends it verbatim after validation. No parse +
reserialize, no per-record wire representation.

The range size on each RPC is bounded by the gRPC default message
ceiling (4 MiB). Catching up a far-behind follower spills into many
RPCs processed back-to-back.

### Follower receives and validates

Follower's Replication stage (see "Pipeline stages by role") receives
`AppendEntries`, runs these checks in order:

1. `request.term >= follower.current_term` (reject `TERM_STALE` on
   mismatch).
2. Follower's record at `prev_tx_id` has term `prev_term` — looked up
   from the follower's `.term` sidecars (reject `PREV_MISMATCH` on
   mismatch).
3. Parse `wal_bytes` as WAL records; verify per-record CRC32C (reject
   `CRC_FAILED` on mismatch).
4. First parsed transactional record's tx_id == `from_tx_id`; last
   transactional record's tx_id == `to_tx_id`; tx_ids monotonic
   (reject `SEQUENCE_INVALID` on mismatch).
5. Append `wal_bytes` to local active segment, fdatasync. If the
   RPC's term differs from the last term recorded in the current
   `.term` sidecar, append a new entry to `.term`.
6. Respond `success` with updated `last_tx_id`.

No business logic runs on the follower. No Transactor. Balances are
carried inside replicated bytes via `TxMetadata` records (computed by
the leader's Transactor) and consumed by the follower's Snapshot
stage unchanged.

### Pipeline stages by role

The pipeline composition is role-dependent:

- **Leader:** Sequencer → Transactor → WAL → Snapshot → Seal
  (unchanged from single-node). The WAL committer additionally ships
  byte ranges to followers.
- **Follower:** Replication → WAL → Snapshot → Seal. The Transactor
  and Sequencer stages are absent. The WAL writer runs, but the
  committer does not ship anywhere — it only fsyncs locally.

The Replication stage replaces Transactor on followers. It does not
compute balances, does not invoke WASM functions, does not run any
business logic. It receives `AppendEntries`, validates, and feeds the
incoming bytes into the WAL stage via the same inbound queue the
Transactor would have used.

### WAL writer (leader and follower): unchanged

The WAL writer's cycle is the same on both roles, and the same as
today:

1. Drain available entries from inbound queue.
2. Append to segment's pending buffer.
3. Write pending buffer to file.
4. Advance `last_written_tx_id`.
5. Rotate segment if tx-count threshold exceeded.
6. Push drained entries to outbound queue for the Snapshot stage.

There is no block boundary logic, no BlockHeader emission, no
role-specific state in the writer. The leader's writer is fed by the
Transactor; the follower's writer is fed by the Replication stage.
Everything downstream of "inbound queue" is identical.

### Committer flow (leader)

The committer's existing fsync responsibility is extended with peer
shipping. On each pass:

1. Read `last_written_tx_id`. If unchanged since the last pass, back
   off and retry.
2. Spawn a tokio task per follower to send
   `AppendEntries{from_tx_id = last_shipped_tx_id + 1, to_tx_id =
   last_written_tx_id, wal_bytes = <WAL bytes for that range>}`.
3. In parallel, call `fdatasync` on the active WAL segment.
4. Await both: local fsync AND quorum (including self) acking up to
   `to_tx_id`.
5. Advance `last_committed_tx_id` to `to_tx_id`. Advance
   `last_shipped_tx_id` per follower based on their ack.

The committer is a pure reader of WAL state. It never writes. It
opens read-only `File` handles on WAL segments and reads byte ranges
via `read_at(offset, len)` bounded by `last_written_tx_id`. Each
follower-sender task has its own handle and its own
`last_shipped_tx_id`.

Batching falls out naturally: whatever accumulated between committer
passes is one `AppendEntries`. Busy periods ship large ranges; idle
periods ship small ranges or skip entirely. No tuning knobs.

### Committer flow (follower)

The committer on a follower only fsyncs. It does not ship. The
shipping code path is dead on followers — no committer-side walk of
ranges, no per-follower tracking. `last_committed_tx_id` advances
when the follower's AppendEntries handler acks durability, which is
itself the commit signal for the local copy.

### WaitLevel::Committed semantics

`WaitLevel::Committed` now means "durable on leader's disk AND acked
by quorum of followers (including leader itself)." This is the same
guarantee clients want in single-node mode (durable), extended to
multi-node (durable on a quorum).

No new WaitLevel is introduced. Single-node deployments (quorum =
self) behave identically to today.

`WaitLevel::Computed` and `WaitLevel::Snapshotted` retain their
existing leader-local semantics. A caller wanting "executed on leader
but not yet quorum-confirmed" uses `Computed`.

### Cluster configuration (static)

For ADR-015 scope: cluster membership is defined in a config file at
startup. No dynamic membership changes. No elections — one node is
designated leader via config, all others are followers. If the leader
crashes, the cluster halts new writes until manual restart. Failover
is addressed in ADR-016.

```toml
[cluster]
node_id = 1
role = "leader"   # or "follower"

[[cluster.peers]]
node_id = 2
address = "10.0.0.2:50052"

[[cluster.peers]]
node_id = 3
address = "10.0.0.3:50052"
```

The `raft_state` file holds `{current_term: 1, voted_for: None}`
forever. Each segment's `.term` sidecar contains a single
`term=1 tx_range=...` line.

### Transport: separate gRPC service, separate port

Internal node-to-node communication runs on a separate gRPC service
on a separate port from the client-facing Ledger API. This isolates
client load from replication traffic, simplifies firewall rules, and
allows different TLS configurations per channel.

```toml
[server]
host = "0.0.0.0"
port = 50051              # client API

[cluster]
listen_port = 50052       # peer API (this node)
```

The `node.proto` service for ADR-015 defines `AppendEntries`, `Ping`,
and stubs for `RequestVote` and `InstallSnapshot`. The stubbed RPCs
return `UNIMPLEMENTED` until ADR-016.

### Reading from the WAL

The committer and its per-follower senders read WAL bytes via their
own read-only `File` handles (opened separately from the writer's
append handle). Concurrency is handled by the OS: the writer appends
to its handle; readers use `read_at(offset, len)` up to the
`last_written_tx_id` watermark (translated to a byte offset via the
existing `wal_position` tracking).

No shared in-memory buffer. No `unsafe`. No mmap (durability
semantics are cleaner without it). Per-reader syscall overhead is
negligible — reads hit the OS page cache for recently-written WAL
data.

Each follower gets its own tokio task, its own `File` handle, and
its own `last_shipped_tx_id`. They operate independently.

### Non-transactional records and replication

`FunctionRegistered` and segment-lifecycle records (`SegmentHeader`,
`SegmentSealed`) carry no tx_id. They travel inside `AppendEntries`
byte ranges between transactional records but do not participate in
the Raft consistency check.

On the follower, `FunctionRegistered` records are replayed into the
function registry by the downstream Snapshot stage, exactly as they
are on the leader. Segment-lifecycle records are currently part of
the WAL stream — whether they should remain so is a separate cleanup
tracked outside this ADR; for now, followers observe them in the
stream and let them flow through to Snapshot/Seal without special
handling.

---

## Consequences

### Positive

- **Single fsync on the commit path.** No separate Raft log, no
  double-sync. Replication runs in parallel with fsync, not in
  series.
- **WAL remains source of truth.** Raft is a transport over the WAL,
  not a parallel structure.
- **No new abstractions.** No block concept, no `BlockHeader` record
  kind, no `block_index`. The only new on-disk artifact is the
  `.term` sidecar, which parallels existing sidecars.
- **Writer cycle is role-agnostic.** Leader and follower run the
  same writer code. The only role-specific code is in the Replication
  stage (follower-only) and in the committer's shipping logic
  (leader-only).
- **Natural batching.** The committer ships whatever records
  accumulated since its last pass. Busy periods ship large ranges;
  idle periods ship small ones. No tuning.
- **Followers are simple.** Receive bytes, validate per-record CRC
    + sequence, append, fdatasync. No replay, no business logic, no
      boundary tracking.
- **Follower balance cache stays in sync automatically.** Balances
  are carried inside replicated bytes via `TxMetadata` records
  (computed by the leader's Transactor). Followers run Snapshot and
  Seal stages as usual against their local WAL. On promotion
  (ADR-016), the new leader's cache is already current.
- **Historical queries work on any node** because each node's WAL
  contains the same record stream (post-replication).
- **Two-port gRPC cleanly separates client load from replication
  traffic.**
- **Term history is introspectable.** Segment `.term` sidecars are
  trivial to read and reason about. No inline metadata records mixed
  into the transactional stream.

### Negative

- **`AppendEntries.term` is the only source of term for incoming
  bytes.** A bug in the RPC envelope or in the follower's `.term`
  writing could cause `.term` sidecars on different followers to
  disagree, even with byte-identical WAL record streams. Mitigation:
  the RPC envelope and sidecar write are both small, simple pieces
  of code; unit-testable in isolation. If stronger cross-checking is
  needed later, a WAL-embedded term marker record can be added
  without changing the rest of this design.
- **`WaitLevel::Committed` latency in multi-node mode** includes
  network round-trip + follower fsync. Client-observed commit
  latency increases from ~700µs (single node) to ~1–3ms (3-node LAN).
- **Static cluster config** — no failover if leader dies. Operator
  intervention required. Addressed in ADR-016.
- **Non-transactional records piggyback.** `FunctionRegistered` and
  segment markers have no Raft index. They ride inside
  `AppendEntries` ranges without their own consistency check. This
  is correct (they are always adjacent to transactional records in
  the stream), but it means there is no way to reference them
  individually in a Raft-level truncation or rewind. Demote-and-
  recover (ADR-016) handles this by discarding and replaying
  everything past the divergence point, not by rewinding record-by-
  record.

### Neutral

- Single-node deployment continues to work unchanged — quorum =
  self, no network, behavior identical to today.
- The WAL record format is unchanged. No new record kind is
  introduced.
- `raft_state` file (current_term, voted_for) is created at init but
  under ADR-015's static-leader model is never updated.

---

## Alternatives considered

**Block-based replication with a `BlockHeader` WAL record.** Earlier
draft of this ADR. Rejected during design review: introduced a
transient abstraction with no end-user value that became a driver of
the writer's internal structure. The writer cycle became a block
cycle, role-specific follower block-tracking state appeared, and the
"transaction must not straddle a block boundary" invariant started
imposing constraints on pipeline scheduling. The byte-range model
here achieves the same goals (per-RPC batching, quorum commit) with
no new abstraction.

**Separate Raft log file.** Rejected — would require a second fsync
per commit, roughly halving throughput.

**Range references without bytes (leader sends `{from, to}` only;
follower fetches bytes separately).** Rejected — followers' WALs
don't exist yet; they must receive the bytes to produce identical
local WALs.

**Put `term` in every WAL record.** Rejected — term changes on
elections only (rare). Per-record term is expensive storage for
information that's sparse by nature. Sidecar-per-segment matches
the sparsity.

**Put `term` only in the `raft_state` file (no per-range record).**
Rejected — `raft_state` holds the node's current term but does not
describe which historical log ranges belong to which term. Raft's
commit safety (the "commit only current-term entries" rule from the
Figure 8 scenario) requires knowing historical terms.

**Per-record `wal_seq` field for a stream-wide Raft index.**
Rejected — adds 8 bytes per record for no benefit under this design.
`tx_id` serves as the Raft index for transactional records;
non-transactional records piggyback without their own index.

---

## References

- ADR-006 — WAL, snapshot, and seal durability
- ADR-010 — Synchronous submit with wait levels (Committed semantics)
- ADR-011 — WAL write/commit separation
- ADR-014 — Function registry persistence (`FunctionRegistered`
  record)
- Ongaro & Ousterhout 2014 — "In Search of an Understandable
  Consensus Algorithm" (Raft paper)
- PostgreSQL walsender — separate reader handles on WAL files, ship
  byte ranges
- TigerBeetle VR — pre-quorum execution, log-range ship