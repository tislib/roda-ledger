# ADR-024: Temporal Index

**Status:** Proposed  
**Date:** 2026-06-15  
**Author:** Taleh Ibrahimli  

**Amends:**
- ADR-006 — adds a per-segment temporal-index file (`temporal_index_NNNNNN.bin`) to the seal artifacts.
- ADR-008 — adds a third per-segment index (`time → tx_id`) and a new query kind, built and served by the same machinery as the transaction and account indexes.

---

## Context

A transaction is addressable today only by `tx_id` — the query surface is
`QueryKind::GetTransaction { tx_id }` and its batch form. Every commit record already
carries a wall-clock timestamp:

```rust
// crates/storage/src/entities.rs — TxMetadata (40-byte trailer commit record, ADR-020)
pub struct TxMetadata {
    // …
    pub tx_id: u64,       // 8 @ 8
    pub timestamp: u64,   // 8 @ 16
    // …
}
```

…but nothing indexes it. There is no `time → tx_id` mapping, so any time-addressed
question — "which transaction was live around time T", time-bounded audit, time-based
segment routing — requires a full WAL scan.

This ADR adds a **temporal index**: a standalone, always-available `time → tx_id`
capability with a hot (in-memory) and a cold (on-disk) tier, derived entirely from the
WAL and served through the existing query stage plus one new blocking `Ledger` API.

Scope is the temporal index **only**. Point-in-time replay ("view the ledger as of time
T") is explicitly out of scope; the temporal index is the addressing primitive such a
feature would build on, not the feature itself.

---

## Decision

### 1. Data model — second-bucketed `time → tx_id`

The index is an ordered map keyed by **second**: `unix_timestamp (u64) → first_tx_id
(u64)`, where `unix_timestamp` is `TxMetadata.timestamp` truncated to whole-second
resolution — the "second bucket". One entry per *populated* second; empty seconds are
never stored (sparse).

**Bucketing.** The metadata timestamp is sub-second; truncating to seconds groups all
transactions of one second under a single key, and the **first** transaction in that
second wins:

```text
tx @ 5.67s ─┐
            ├─► bucket 5  →  tx_id of the 5.67s transaction   (entry(5).or_insert(..))
tx @ 5.68s ─┘
```

**Query semantics — half-open, greatest bucket ≤ T.** A `time → tx_id` lookup returns
the entry for the greatest `unix_timestamp` ≤ T (`range(..=T).next_back()`). A timestamp
that lands in a gap (no transactions that second) resolves to the last populated second —
correct, because no state changed during the gap. The boundary convention is fixed here
once: an off-by-one is a wrong answer, not a tuning knob.

**Granularity is seconds.** Coarser views (minute, hour) derive by further truncation;
finer resolution is intentionally not stored — at the active window's transaction rate a
single second already pins a tight `tx_id` range.

**Ordered, not hashed.** Because the lookup is a range query, both the in-memory and the
on-disk form are ordered (a `BTreeMap` and a sorted file), not a hash map.

### 2. Hot tier — in-memory `BTreeMap`, scoped to the active segment

```rust
BTreeMap<u64 /* unix second */, u64 /* first tx_id */>
```

owned by the snapshot stage (`SnapshotRunner`), exactly as the `TransactionIndexer` is
owned and read lock-free on the snapshot thread. `Snapshot.indexer: Option<TransactionIndexer>`
is moved into the runner at start; the temporal map rides alongside it.

**Built inline in the committed-transaction handler.** In `SnapshotRunner::run`, the
`WalEntry::Metadata(m)` arm already indexes the transaction once it clears the commit
gate; the temporal insert sits right beside it:

```rust
WalEntry::Metadata(m) => {
    self.indexer.insert_transaction(&m, &group);
    self.temporal
        .entry(trunc_secs(m.timestamp))
        .or_insert(m.tx_id);          // first tx of each second wins
    // … apply balances/flags from followers …
}
```

**Cleared at segment close.** When `m.tx_id % transaction_count_per_segment == 0` — the
ADR-013 §1 rotation point — the map is `clear()`-ed. So the hot map only ever holds the
**current active segment's** seconds; older segments are served from cold. (`BTreeMap` is
node-allocated, so there is no `Vec`-style capacity to retain, but `clear` is cheap.)

**No synchronization.** Inserts (the Metadata handler) and reads (query dispatch) both run
on the single snapshot-stage thread — the same property that lets `GetTransaction` call
`self.indexer.get_transaction(...)` without a lock. The temporal map needs no atomics, no
`ArcSwap`, no `Mutex`.

**Recovery rebuilds it.** The hot map is not persisted. On startup the snapshot stage
already tails the active segment's WAL from `from_tx_id` (ADR-021); the same insert path
reconstructs the active segment's seconds. Restore is responsible for the **current**
segment's temporal points; the sealer is responsible for older segments (§4).

### 3. Querier — a new query kind, served like `GetTransaction`

Add one variant each to `QueryKind` and `QueryResponse` (`crates/ledger/src/snapshot.rs`):

```rust
pub enum QueryKind {
    GetTransaction { tx_id: u64 },
    GetTransactionBatch { tx_ids: Vec<u64> },
    ResolveTimeToTxId { unix_timestamp: u64 },   // new
}

pub enum QueryResponse {
    Transaction(Option<CommittedTransaction>),
    TransactionBatch(Vec<CommittedTransaction>),
    AccountHistory(Vec<IndexedTxEntry>),
    TimeToTxId(Option<u64>),                      // new
}
```

Dispatch in the `SnapshotRunner::run` query match, beside the `GetTransaction` arm:

```rust
QueryKind::ResolveTimeToTxId { unix_timestamp } => {
    let tx_id = self
        .temporal
        .range(..=unix_timestamp)
        .next_back()
        .map(|(_, &tx_id)| tx_id);
    QueryResponse::TimeToTxId(tx_id)
}
```

This arm answers from the hot map only (the active segment). Older timestamps are handled
by the `Ledger` API (§6) reading cold files, so the snapshot stage never blocks on disk.

The index is **`time → tx_id` only** — there is no reverse (`tx_id → time`) query. A
transaction's own timestamp is already reachable from its record; the temporal index does
not duplicate it.

### 4. Cold tier — a per-segment file written by the sealer

When a segment seals, the **sealer** writes its temporal index to disk. The hook is in
`SealRunner::process_seal` right after `segment.build_indexes()` (`crates/ledger/src/seal.rs`),
building from the already-in-memory `segment.wal_data` with the same single pass
`build_indexes` uses (`crates/storage/src/index.rs`): walk the 40-byte records, and for
each `WalEntry::Metadata`, `entry(trunc_secs(m.timestamp)).or_insert(m.tx_id)`.

**File + layout.** A new path helper in `crates/storage/src/layout.rs`, beside
`segment_tx_index_path` (`wal_index_{:06}.bin`) and `segment_account_index_path`
(`account_index_{:06}.bin`):

```rust
pub fn segment_temporal_index_path(data_dir: &Path, id: u32) -> PathBuf {
    data_dir.join(format!("temporal_index_{id:06}.bin"))
}
```

**Format.** Reuse the `write_index_file` shape: an 8-byte LE `count`, then sorted
`(unix_timestamp: u64 LE, first_tx_id: u64 LE)` pairs — sorted by second so the reader
binary-searches "greatest ≤ T", the same range semantics as the hot `BTreeMap`. A 16-byte
CRC sidecar `[crc32: 4 LE][size: 8 LE][magic: 4 LE]` mirrors the WAL/snapshot sidecars and
is verified on read (`verify_wal_data`-style).

**Derived, never authoritative.** The file is rebuildable by rescanning the sealed
segment's WAL; it is an accelerator, not a source of truth (ADR-021).

### 5. Cold-segment locator — resident `BTreeMap` directory

To answer a cold query the `Ledger` must first find *which* sealed segment covers T. A
resident directory does it:

```rust
BTreeMap<u64 /* segment start second */, SegmentDirEntry>

struct SegmentDirEntry {
    start_tx_id: u64,
    start_second: u64,
    end_second:  u64,
    segment_id:  u32,
}
```

one entry per **sealed** segment. The segment count stays small (default 10M
transactions/segment), so the directory is always RAM-resident.

`BTreeMap` again, for the same reason as the hot tier — locating a segment by time is a
range query (`range(..=T).next_back()` → O(log n)); a hash cannot do it. The directory
lives in the snapshot/storage layer — **nothing in the transactor** participates. It is
populated at seal (one insert per sealed segment) and is rebuildable from the sealed
temporal-index files (or the WAL) on startup.

### 6. Blocking `Ledger` API — merges hot + cold

One always-blocking method on `Ledger`, mirroring `get_transaction_block` over
`query_block`'s `sync_channel(1)` (`crates/ledger/src/ledger.rs`):

```rust
pub fn resolve_time_to_tx_id_block(&self, unix_timestamp: u64) -> Option<u64>;
```

Flow:

1. If T falls in the **active** segment's range → issue `QueryKind::ResolveTimeToTxId`
   to the snapshot stage (hot `BTreeMap`), blocking on the response channel exactly like
   `get_transaction_block`.
2. Otherwise → use the locator (§5) to find the **sealed** segment, read its
   `temporal_index_{:06}.bin` on the **caller** thread (CRC-verified, immutable), and
   binary-search "greatest ≤ T". Disk I/O stays off the snapshot stage.

"Always blocking" matches the existing `*_block` query surface; callers receive an
`Option<u64>` directly.

### 7. Source-of-truth discipline

The hot `BTreeMap`, the cold files, and the resident locator are all **acceleration
structures** — held or persisted for speed, fully rebuildable from the WAL, never
authoritative. This keeps the temporal index inside the ADR-021 invariant: the WAL is the
sole source of truth (`wal_only_source_of_truth_test.rs`).

---

## Consequences

### Positive
- A time-addressable ledger (`time → tx_id`) with O(log n) lookups in both tiers.
- The hot tier reuses the snapshot stage's single-thread, lock-free model — no atomics or
  locks added.
- Both in-memory structures are ordered `BTreeMap`s, so `range(..=T).next_back()` answers
  gap-second queries correctly (never a false "nothing here").
- The cold tier reuses the ADR-008 seal → build → serialize → CRC machinery and the
  ADR-006 sidecar format; little new surface.
- Clean ownership split: the **sealer** writes cold files for sealed segments; **restore**
  rebuilds the hot map for the active segment.
- Everything is derived and rebuildable from the WAL.

### Negative
- One new per-segment file at seal, plus a small resident locator `BTreeMap`; seal does
  marginally more work (a second single-pass scan, already paid for the tx/account indexes).
- A very sparse but very long-lived segment yields one row per active second — bounded by
  the segment's wall-clock span; flagged as a size watch, not yet measured.
- Cold lookups add disk I/O, taken on the caller thread by design (kept off the snapshot
  stage).

### Known limitation — by design, *not* a bug or TODO
The hot map is cleared at segment close (`tx_id % transaction_count_per_segment == 0`)
while the cold file is written later, at seal. Between those two moments the just-closed
segment's seconds are in **neither** tier — that window is "in the dark" and not
time-queryable by hot or cold. This is an accepted property of the two-tier hand-off,
documented here rather than worked around.

### Neutral
- WAL format unchanged — `TxMetadata.timestamp` already exists (ADR-020); no new record
  kinds.
- The existing `tx_id`-addressed query path is untouched.
- Nothing in the transactor participates — the feature lives in snapshot, seal, storage,
  and the `Ledger` query API.
- The index is optional for correctness: a missing file can be rebuilt, and absent the
  index a time query degrades to a WAL scan — never to a wrong answer.

---

## Open Empirical Items
- Per-segment temporal-index file size on genuinely sparse but long-lived segments —
  measure before considering delta/varint encoding of the second column.
- Whether to add a **time-ceiling** seal trigger (seal once a segment spans more than N
  seconds) to bound both the active window's wall-clock span and the hot map's size. Today
  segments seal on transaction count only (ADR-013); a time ceiling would interact with
  that single knob and is deferred until the size data above exists.

---

## References
- ADR-006 — WAL, Snapshot, and Seal Durability (seal artifacts; CRC sidecar format).
- ADR-008 — Transaction Index and Query Serving (the per-segment index build and query
  stage this extends).
- ADR-013 — Transaction-Count-Based Segments (`tx_id % transaction_count_per_segment == 0`
  rotation point, reused as the hot-tier reset boundary).
- ADR-020 — Trailer Metadata (the `TxMetadata.timestamp` location).
- ADR-021 — WAL is the Sole Ring Releaser; Snapshot Tails the WAL (recovery/rebuild model;
  source-of-truth invariant).
- Code: `crates/storage/src/entities.rs` (`TxMetadata.timestamp`),
  `crates/ledger/src/snapshot.rs` (`QueryKind`/`QueryResponse`, `SnapshotRunner::run`, the
  `WalEntry::Metadata` handler), `crates/ledger/src/seal.rs` (`process_seal`),
  `crates/storage/src/index.rs` (`build_indexes` / `write_index_file`),
  `crates/storage/src/layout.rs` (per-segment path helpers),
  `crates/ledger/src/ledger.rs` (`query_block` / `get_transaction_block`).
