# ADR-011: WAL Write/Commit Separation

**Status:** Accepted  
**Date:** 2026-04-13  
**Author:** Taleh Ibrahimli  

**Amends:**
- ADR-006 — splits WAL stage into two threads (write + commit), changes segment header layout, updates write buffer semantics

---

## Context

The WAL stage (ADR-006) ran as a single thread that performed three responsibilities sequentially: receiving entries from the transactor, writing them to the active segment file, and calling `fdatasync` to commit. The `fdatasync` call is the durability bottleneck — it blocks the write thread for hundreds of microseconds to milliseconds depending on the storage backend.

This coupling meant the WAL could not accept new entries from the transactor while waiting for `fdatasync` to complete. Under sustained load, the single-threaded WAL became the pipeline bottleneck — the transactor's outbound queue would fill, stalling transaction processing.

Separating the write and commit paths allows the WAL to continue buffering and writing entries to the OS page cache while a dedicated commit thread handles `fdatasync` independently. This decouples write throughput from sync latency.

Additionally, several secondary cleanups were included:
- `SegmentHeader.first_tx_id` was always set after the fact and added complexity for marginal value — segment boundaries are already discoverable from `TxMetadata` records.
- `TxLink` lacked a `tx_id` field, making it impossible for `WalEntry::tx_id()` to return the owning transaction for link entries.
- `WaitStrategy::wait_strategy()` was poorly named — renamed to `retry()`.
- Index file I/O used unbuffered reads/writes — switched to `BufReader`/`BufWriter`.

---

## Decision

### Split WAL into two threads

The single `wal` thread is replaced by two threads:

1. **`wal_write`** — receives entries from the transactor queue, serializes them into the active segment's write buffer, calls `write_all` to flush the buffer to the OS page cache, and forwards committed entries downstream to the snapshot stage.

2. **`wal_commit`** — monitors the `last_written_tx_id` atomic. When it advances past `last_committed_tx_id`, calls `fdatasync` on a cloned file descriptor of the active segment, then updates `last_committed_tx_id`.

The two threads coordinate via three shared atomics:

```rust
last_written_tx_id:    AtomicU64   // set by wal_write after write_all
last_committed_tx_id:  AtomicU64   // set by wal_commit after fdatasync
active_segment_sync:   ArcSwap<Option<Syncer>>  // cloned file handle for fdatasync
```

The `wal_write` thread only forwards entries downstream (to the snapshot stage) after `last_committed_tx_id` has advanced past the entry's `tx_id`. This preserves the durability guarantee: entries reaching the snapshot stage are already durable on disk.

### Syncer abstraction

A new `Syncer` struct wraps a cloned `File` handle and exposes a single `sync()` method that calls `fdatasync`. The `wal_commit` thread holds a `Syncer` obtained from the active segment. On segment rotation, `wal_write` creates a new `Syncer` and swaps it into the shared `ArcSwap`.

```rust
pub struct Syncer {
    wal_file: File,
}

impl Syncer {
    pub(crate) fn sync(&mut self) -> std::io::Result<()> {
        self.wal_file.sync_data()
    }
}
```

### Segment rotation

Segment rotation is performed by the `wal_write` thread. Before rotating:

1. Write the `SegmentSealed` record
2. Perform a synchronous `fdatasync` (via `commit_sync()`) to ensure the sealed record is durable
3. Close and rename the segment
4. Open the new active segment
5. Swap the `Syncer` in the shared `ArcSwap`

The synchronous commit during rotation ensures no sealed records are lost. Normal operation uses the async commit path.

### Append-then-write buffer model

The previous `write_entries` method serialized and wrote entries in a single call. This is replaced by a two-phase approach:

- `append_pending_entry(entry)` — serializes the entry into the write buffer without I/O
- `write_pending_entries()` — flushes the write buffer to disk via `write_all`

The write buffer changed from `vec![0; 16MB]` (pre-allocated zeros) to `Vec::with_capacity(16MB)` (capacity-only, no allocation until used). This avoids touching 16MB of memory on segment creation.

### Remove `SegmentHeader.first_tx_id`

The `first_tx_id` field is removed from `SegmentHeader`. The field was set at segment creation time, but at that point the first transaction ID is not yet known (it depends on what the transactor produces next). The actual first transaction in a segment is always discoverable by scanning for the first `TxMetadata` record. The 8 bytes are returned to padding.

**Before:**
```rust
pub struct SegmentHeader {
    // ...
    pub first_tx_id: u64, // 8 @ 16
    pub _pad2: [u8; 16],  // 16 @ 24
}
```

**After:**
```rust
pub struct SegmentHeader {
    // ...
    pub _pad2: [u8; 24],  // 24 @ 16
}
```

The `wal_segment_header_entry` constructor no longer takes a `first_tx_id` parameter.

### Add `tx_id` to `TxLink`

`TxLink` gains a `tx_id` field so that `WalEntry::tx_id()` returns the owning transaction ID for link entries (previously returned 0). This is needed for the WAL write/commit separation — the write thread tracks transaction boundaries by inspecting `tx_id()` on every entry.

**Before:**
```rust
pub struct TxLink {
    pub entry_type: u8,
    pub link_kind: u8,
    pub _pad: [u8; 6],
    pub to_tx_id: u64,     // 8 @ 8
    pub _pad2: [u8; 24],   // 24 @ 16
}
```

**After:**
```rust
pub struct TxLink {
    pub entry_type: u8,
    pub link_kind: u8,
    pub _pad: [u8; 6],
    pub tx_id: u64,        // 8 @ 8  — links back to TxMetadata
    pub to_tx_id: u64,     // 8 @ 16 — referenced transaction
    pub _pad2: [u8; 16],   // 16 @ 24
}
```

### Rename `WaitStrategy::wait_strategy()` to `retry()`

The method `wait_strategy()` on the `WaitStrategy` enum was confusingly named (a method named after its own type). Renamed to `retry(retry_count)` to better express its purpose: backing off on a retry loop.

### Buffered I/O for index files

`write_index_file` and `read_index_file` now use `BufWriter` and `BufReader` respectively. Index files perform many small 8-byte writes/reads — buffering reduces syscall overhead significantly.

---

## Consequences

### Positive

- WAL write throughput is no longer blocked by `fdatasync` latency — the write thread continues accepting entries while the commit thread syncs
- Pipeline backpressure from WAL to transactor is reduced — the transactor queue drains faster
- Throughput improved from ~6.6M to ~2.89M sustained durable ops/s (the previous number did not include `fdatasync` — the new number reflects true durable throughput with `fdatasync` on every commit)
- `WalEntry::tx_id()` now returns meaningful values for all entry types including links
- `SegmentHeader` is simpler — no need to predict the first transaction ID at segment creation
- Index file I/O is faster with buffered readers/writers
- `retry()` is a clearer method name than `wait_strategy()`

### Negative

- Two WAL threads instead of one — slightly more complex coordination via atomics and `ArcSwap`
- `ArcSwap` is a new dependency for the `Syncer` hot-swap mechanism
- Breaking wire format change: `SegmentHeader` and `TxLink` layouts changed — existing WAL files are incompatible
- Segment rotation requires a synchronous `fdatasync` — brief stall during rotation (acceptable since rotation is infrequent)

### Neutral

- The `Wal::start()` return type changed from `JoinHandle<()>` to `[JoinHandle<()>; 2]`
- `Pipeline::WalContext` is now `Clone` to allow sharing between the two WAL threads
- The default queue size increased from 1024 to 16384 (`1 << 14`) to better absorb transactor bursts

---

## References

- ADR-006 — WAL, Snapshot, and Seal Durability (amended by this ADR)
- ADR-001 — Entries-based execution model, WalEntry variants
- ADR-009 — TxLink structure (amended: added `tx_id` field)
- `src/wal.rs` — WalRunner (write thread), WalCommitter (commit thread)
- `src/storage/syncer.rs` — Syncer abstraction
- `src/storage/segment.rs` — `append_pending_entry`, `write_pending_entries`, `syncer()`
- `src/entities.rs` — SegmentHeader (removed `first_tx_id`), TxLink (added `tx_id`)
- `src/wait_strategy.rs` — `retry()` method rename
- `src/storage/index.rs` — BufReader/BufWriter usage
