# ADR-015: Block-based WAL Replication

**Status:** Proposed  
**Date:** 2026-04-19  
**Author:** Taleh Ibrahimli

**Amends:**
- ADR-006 — adds `WalEntryKind::BlockHeader` (kind=6) to WAL record set
- ADR-010 — `WaitLevel::Committed` semantics change in multi-node mode
- ADR-011 — WAL writer cycle now emits BlockHeader at the start of each cycle

---

## Context

Roda-ledger today is single-node. Multi-node replication is required for
production deployment — both for availability (surviving node failure) and
for data durability (surviving disk failure on a single machine).

Raft is the consensus protocol of choice (ADR plan for subsequent steps
covers election and recovery). This ADR addresses the first step:
**block-based replication of the WAL from leader to followers** under a
static cluster configuration with a manually designated leader. Leader
election, term changes, and demote-and-recover are deferred to ADR-016.

The core constraints:

1. **No second fsync on the commit path.** Roda's existing WAL already
   performs one fdatasync per commit cycle. Adding a second for a separate
   Raft log would roughly halve throughput.
2. **The WAL is the source of truth.** Raft must not maintain its own
   separate log. Instead, Raft indexes byte ranges within the existing
   WAL.
3. **The pipeline's design invariants (single-writer Transactor, derived
   balances, WAL entries as the atomic record unit) must be preserved.**
   Raft-driven truncation, when introduced in ADR-016, will rely on the
   existing `Recover` mechanism rather than in-place rewinds.
4. **Writer is the source of truth for blocks.** The committer (which
   ships blocks to followers and advances the commit watermark) must be
   a pure reader of WAL state. It never writes block metadata, never
   coordinates with the writer.

---

## Decision

### Block as the unit of replication

Introduce the concept of a **block**: a contiguous run of WAL records
that replicate as a single Raft log entry. Each block has:

- A monotonic `block_index` (u64)
- A `term` (u64) — the Raft term under which the block was produced
- A `len` (u64) — the number of WAL records following the BlockHeader
  (not counting the BlockHeader itself)

Block boundaries are marked by a new WAL record type: `BlockHeader`,
written at the **start** of each block. A block consists of the
BlockHeader record plus the `len` records that follow it. The next
block begins at the next BlockHeader.

### WalEntryKind::BlockHeader (40 bytes)

```rust
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct BlockHeader {
    pub entry_type: u8,     // 1  @ 0  — WalEntryKind::BlockHeader (6)
    pub _pad0: [u8; 7],     // 7  @ 1
    pub block_index: u64,   // 8  @ 8  — monotonic across lifetime of cluster
    pub len: u64,           // 8  @ 16 — number of records in this block
    pub term: u64,          // 8  @ 24 — Raft term
    pub _pad1: [u8; 8],     // 8  @ 32 — total: 40 bytes
}
```

Compile-time assertion keeps the 40-byte invariant consistent with every
other WAL record type.

### Block formation rule

Block boundaries are driven by the WAL writer's natural cycle. Every
writer cycle that has entries to drain produces exactly one block:

- At the start of each cycle, `available` is computed (number of
  entries in the inbound queue, capped by buffer capacity) — this
  already exists in `WalRunner::run`.
- If `available > 0`:
    1. Write a `BlockHeader { block_index, term, len: available }`
       record to the WAL buffer.
    2. Drain `available` entries from the queue into the buffer (the
       existing loop).
    3. Flush the buffer to file.
    4. Advance `last_written_tx_id`.
    5. Increment `block_index`.
- If `available == 0`: no block is produced. Writer continues idle
  backoff.

Consequences:
- Blocks auto-size based on pipeline pressure. Busy periods produce
  large blocks; idle periods produce small blocks or none at all.
- Each writer cycle = at most one block. No per-record overhead
  beyond the single 40-byte BlockHeader.
- Writer owns block boundaries entirely. The committer (below) never
  writes to the WAL and never coordinates with the writer.
- `len` is known before writing — no chicken-and-egg problem, no
  speculative fields, no patching records after the fact.

### Block indexing across segments

`block_index` is monotonic and never resets. Blocks never span segments
— a new segment rotation is a natural cycle boundary, and a block is
produced end-to-end within a single cycle. If a rotation happens, the
next cycle's block begins in the new segment.

Recovery reconstructs the current `block_index` by scanning WAL
segments from the latest snapshot forward, counting BlockHeader records.

### Raft log = WAL blocks

Raft treats each block as one log entry. `raft_index = block_index`.
Entry content is the raw WAL bytes of the block: the BlockHeader plus
its `len` follower records. When the leader ships an AppendEntries, it
reads the WAL byte range for the target block(s) via a read-only file
handle and sends them unchanged.

Followers receive WAL bytes, append them directly to their own active
WAL segment, and verify:
- Per-transaction CRC32C (existing check, ADR-006)
- Monotonic tx_id sequence
- BlockHeader's `block_index` matches expected next value
- Record count matches `len`

No business logic runs on the follower. No Transactor on a follower —
see "Replication stage" below. Balances travel inside the block bytes
(computed by the leader's Transactor) and are consumed by the
follower's Snapshot stage unchanged.

### Committer flow (leader)

The committer is a pure reader of WAL state. It never writes. It runs
in its own thread/task and does:

1. Open a read-only `File` handle on the active WAL segment (+ handles
   to sealed segments containing blocks not yet quorum-committed).
2. Starting from the last shipped offset, walk forward reading
   `BlockHeader` records, collecting as many **fully written** blocks
   as are available:
    - Read a `BlockHeader`. Expected block size = `(1 + header.len) * 40`
      bytes.
    - Check against `last_written_tx_id` (already maintained by the
      writer): if the block's final tx_id ≤ `last_written_tx_id`, the
      block is fully durable. Include it in this batch, advance the
      read cursor past it, and repeat.
    - If the next block isn't fully written yet: stop walking. The
      batch is everything collected so far.
3. If the batch is empty: skip this cycle, retry.
4. If the batch is non-empty (one or more blocks):
    - Spawn a tokio task per follower that sends the entire batch as a
      single AppendEntries (all blocks in one RPC, parallel across
      followers).
    - In parallel, call `fdatasync` on the WAL segment file (local
      durability — covers all blocks in the batch).
    - Await both: local fsync completion AND quorum ack.
    - Advance `last_committed_tx_id` to the last tx_id of the final
      block in the batch.

Batching matters because the committer can run slower than the writer
(e.g. under network pressure, or after startup when catching up).
When it wakes and finds N blocks ready, shipping them in one RPC
amortises network overhead and keeps the committer from falling
further behind. `AppendEntries` already carries a list of entries by
design — this is the natural way to use it.

Both paths (fsync and replication) run in parallel; the slower sets
the commit latency.

### Pipeline stages by role

The pipeline composition is role-dependent:

- **Leader:** Sequencer → Transactor → WAL → Snapshot → Seal (unchanged
  from single-node). The WAL writer additionally emits BlockHeaders
  and the committer ships blocks to followers.
- **Follower:** Replication → WAL → Snapshot → Seal. The Transactor
  stage is absent; Sequencer is absent (clients do not submit writes
  to followers — this ADR does not address read-only followers or
  forwarding). The WAL writer on a follower does **not** run the
  committer — it does not ship blocks anywhere. Local fsync still
  runs.

The Replication stage replaces Transactor on followers. It does not
compute balances, does not invoke WASM functions, does not run any
business logic. Its sole responsibility is to receive blocks from the
leader and push their raw WAL bytes into the WAL stage.

### Replication stage (follower)

Receives AppendEntries carrying one or more blocks:

1. Validate prev_block_index/prev_block_term (Raft consistency check
   against the first block in the batch).
2. If mismatch: reject with `last_block_index` hint; leader walks back.
3. If match: for every block in the batch, validate the BlockHeader
   record and run per-tx CRC + sequence checks on the embedded
   records.
4. Push all block bytes into the WAL stage (in order) for local
   append + fsync.
5. Once the WAL stage reports the batch durable on local disk,
   respond with success and the new `last_block_index`.

Because the Replication stage feeds the same WAL stage that the
Transactor would have fed on a leader, all downstream stages —
Snapshot and Seal — run on followers exactly as they do on the leader.
Followers snapshot their balance cache and seal their WAL segments on
their own schedule.

### Balance cache on followers

Followers maintain their balance cache the same way the leader does:
it is a derived state computed from WAL entries. In single-node mode
today, the Transactor writes balances into the WAL's `TxMetadata`
records and the Snapshot stage reads from there. On a follower, those
same `TxMetadata` records arrive inside the replicated block bytes
(computed by the leader's Transactor), and the follower's Snapshot
stage reads them the same way.

The follower's balance cache is therefore always in sync with the
leader's up to the follower's `last_block_index`, without running any
business logic locally. Promotion (ADR-016) is fast: the new leader's
cache is already current; no recovery cycle is required purely on
account of "stale cache."

Leader advances its per-follower `matchIndex`. When `matchIndex` for a
majority of followers (including leader itself) reaches or exceeds a
block, that block is quorum-committed.

### WaitLevel semantics

`WaitLevel::Committed` now means "durable on leader's disk AND acked by
quorum." This is the same guarantee clients want in single-node mode
(durable), extended to multi-node (durable on a quorum).

No new WaitLevel introduced. Single-node deployments (quorum = self)
behave identically to today.

`WaitLevel::Computed` and `WaitLevel::Snapshotted` retain their existing
leader-local semantics. A caller wanting "executed on leader but not yet
quorum-confirmed" uses `Computed`.

### Cluster configuration (static)

For ADR-015 scope: cluster membership is defined in a config file at
startup. No dynamic membership changes. No elections — one node is
designated leader via config, all others are followers. If the leader
crashes, the cluster halts new writes until manual restart. Failover is
addressed in ADR-016.

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

### Transport: separate gRPC service, separate port

Internal node-to-node communication runs on a separate gRPC service on
a separate port from the client-facing Ledger API. This isolates
client load from replication traffic, simplifies firewall rules, and
allows different TLS configurations per channel.

```toml
[server]
host = "0.0.0.0"
port = 50051              # client API

[cluster]
listen_port = 50052       # peer API (this node)
```

The `node.proto` service for ADR-015 defines only `AppendEntries` (and
a `Ping` for health). `RequestVote` and `InstallSnapshot` are included
in the proto but unimplemented in this ADR — they become active in
ADR-016.

### Reading from the WAL (committer and per-follower senders)

The committer and its per-follower senders read WAL bytes via their own
read-only `File` handles (opened separately from the writer's append
handle). Concurrency is handled by the OS: the writer appends to its
handle; readers use `read_at(offset, len)` up to the
`last_written_tx_id` watermark.

No shared in-memory buffer. No `unsafe`. No mmap (durability semantics
are cleaner without it). Per-reader syscall overhead is negligible —
reads hit the OS page cache for recently-written WAL data.

Each follower gets its own tokio task (one per peer), its own `File`
handle, and its own position tracker. They operate independently.

### Block metadata (derivable, not stored)

Block metadata (byte offsets per block_index, term, last tx_id) is
**derived** from the WAL by scanning BlockHeader records. The committer
maintains an in-memory cursor (current block_index + current read
offset) but does not persist a separate index. On restart, the cursor
is rebuilt by scanning from the latest snapshot forward.

For ADR-015's static leader/static-term model, this is sufficient.
ADR-016 will decide whether a lightweight in-memory block index is
worthwhile for efficient block-by-index lookup.

---

## Consequences

### Positive

- Single fsync on commit path — no separate Raft log, no double-sync.
- WAL remains source of truth — Raft is an index over it.
- Block boundaries are decided by the writer alone — no coordination
  with the committer, no chicken-and-egg ordering, no post-hoc record
  patching.
- `len` is known at BlockHeader write time — followers and readers
  know block size before reading contents.
- BlockHeader at the start of a block simplifies sequential reads:
  read header, skip/read the next `len` records as a unit, move to
  next block.
- Block auto-sizes based on pipeline pressure — busy periods ship
  large blocks, idle periods ship small ones. No tuning knobs.
- Committer is a pure reader — simpler code, no write path conflicts,
  trivially parallelizable across followers.
- Partial-block detection via `last_written_tx_id` — no syscall per
  check.
- `WaitLevel::Committed` semantics extend naturally from single-node.
- Followers are simple: receive bytes, CRC-check, append — no replay,
  no business logic.
- Follower balance cache stays in sync with leader automatically:
  balances are carried inside replicated block bytes via `TxMetadata`
  records. Followers run Snapshot and Seal stages as usual against
  their local WAL. On promotion (ADR-016), the new leader's cache is
  already current.
- Historical queries work on any node because WAL bytes are identical
  cluster-wide (post-replication).
- Two-port gRPC cleanly separates client load from replication traffic.

### Negative

- Breaking WAL format change — new `BlockHeader` record type; existing
  WAL files must be migrated or replayed into a new cluster.
- BlockHeader is metadata overhead: 40 bytes per block. At 1 block/µs
  worst case that's 40 MB/s — in practice blocks are larger and this
  is negligible.
- Tiny blocks are possible (e.g. 1 tx per block under low load) —
  still correct, just more per-byte overhead. Mitigated in practice
  by pipeline batching.
- `WaitLevel::Committed` latency in multi-node mode includes network
  round-trip + follower fsync. Client-observed commit latency
  increases from ~700µs (single node) to ~1-3ms (3-node LAN).
- Static cluster config — no failover if leader dies. Operator
  intervention required. Addressed in ADR-016.

### Neutral

- Single-node deployment continues to work unchanged — quorum = self,
  no network, behavior identical to today.
- `block_index` persists in the WAL (via BlockHeader records), so
  recovery rebuilds it without external state.
- Raft `term` lives in `BlockHeader` record (self-describing blocks)
  and in the `raft_state` file (current term, voted_for — addressed
  in ADR-016).

---

## Alternatives considered

**Separate Raft log file.** Rejected — would require a second fsync
per commit, roughly halving throughput.

**Raft log entries reference WAL byte ranges, no bytes replicated.**
Rejected — followers' WALs don't exist yet; they must receive the
bytes to produce identical local WALs. Range references work only
post-replication, not as a transport.

**Put `term` + `raft_index` directly in every TxMetadata.** Rejected —
Raft state changes at block granularity, not per-transaction. Storing
it per-tx wastes space and couples Raft's lifecycle to the transaction
record.

**Time-based or size-based block formation.** Rejected — adds tuning
knobs. Writer-cycle formation is self-adapting and ties block
granularity directly to pipeline pressure.

**BlockEnd at the end of a block (instead of BlockHeader at the
start).** Rejected — creates a chicken-and-egg problem: the writer
would need to know the block's closing conditions (commit progress
or byte length) before writing the marker. BlockHeader-at-start
writes known information (`len = available`) at a known moment
(cycle start).

**Committer owns block boundaries.** Rejected — would force
coordination between writer and committer, create a second writer
to the WAL, and break the "writer is source of truth" invariant.
Writer-owned blocks are strictly simpler.

**Ship blocks before leader's fsync completes.** Deferred — reasonable
optimization (leader and follower fsync in parallel, quorum can
complete before leader's local sync). Already implicit in the
parallel design described above; explicit tuning left for
measurement-driven work.

**BlockStart + BlockEnd records (two markers per block).** Rejected —
BlockHeader alone is sufficient since `len` is known at write time.
40 bytes per block instead of 80.

---

## References

- ADR-006 — WAL, snapshot, and seal durability
- ADR-010 — Synchronous submit with wait levels (Committed semantics)
- ADR-011 — WAL write/commit separation
- Ongaro & Ousterhout 2014 — "In Search of an Understandable Consensus
  Algorithm" (Raft paper)
- PostgreSQL walsender — separate reader handles on WAL files
- TigerBeetle VR — pre-quorum execution, separate log ship