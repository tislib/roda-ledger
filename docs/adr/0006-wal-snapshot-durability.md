# ADR-006: WAL, Snapshot, and Seal Durability

**Status:** Accepted  
**Date:** 2026-03-31  
**Updated:** 2026-04-01  
**Author:** Taleh Ibrahimli  

---

## Context

The current WAL implementation is a single append-only file that grows indefinitely.
The snapshot mechanism writes a full balance dump on a timer. Both have correctness
and operational problems that must be addressed before a public Docker release.

**Current WAL problems:**

- Single file grows indefinitely — recovery must scan the entire file on restart
- No file-level integrity check — silent corruption goes undetected
- No per-transaction CRC — a flipped bit in a `TxAmount` produces wrong balances silently
- No format versioning — format changes have no detection mechanism
- No segment boundary markers — replay has no structural anchors

**Current snapshot problems:**

- Timer-based trigger — arbitrary relationship between snapshot and WAL position
- Full balance dump every time — at 50M accounts this is 400MB uncompressed
- No integrity check — corrupt snapshot loads silently, produces wrong balances
- Single file — no history, no ability to recover from a bad snapshot write
- Atomic write (tmp → rename) already implemented — good, kept as-is

**Financial ledger requirement:**

A financial ledger must refuse to operate on corrupt data. Silent corruption is worse
than a crash. Every read from disk must be verified before being applied to in-memory
state.

---

## Decision

### Pipeline: four stages

The processing pipeline has four stages in order:

```
Transactor → WAL → Snapshot → Seal
```

Each stage receives records from the previous stage via a queue and forwards
processed records downstream.

### WAL: segmented files with two-level integrity

Replace the single WAL file with fixed-size segments. Each segment has a header record,
per-transaction CRC, and a sealed record. A sidecar `.crc` file provides file-level
integrity for sealed segments.

### Snapshots: retained, stored as part of Seal stage

Snapshots are kept. The `Snapshot` stage maintains the in-memory `Vec<AtomicI64>`
balance cache as before. However, **snapshot storing** (writing snapshots to disk)
is no longer a separate `SnapshotStorer` — it is moved into the **Seal stage**.

The Seal stage is responsible for both sealing WAL segments and storing snapshots.
It triggers both when it discovers unsealed WAL segments by listing the data directory.

### Balances: computed once in Transactor, stored in TxEntry

Balances are **not** computed dynamically during sealing or snapshotting. Instead,
the Transactor computes the running balance for each account after every entry and
stores it directly in the `TxEntry` record:

```rust
pub struct TxEntry {
    // ...
    pub computed_balance: i64,  // replaces _pad1 — set by Transactor, read by Seal/Snapshot
}
```

The `computed_balance` field holds the account balance **after** this entry is applied.
Seal and Snapshot stages read `TxEntry.computed_balance` directly — no balance
recomputation is needed anywhere downstream.

### Seal stage: async, discovers work by listing

The Seal stage starts as a dedicated async process. It discovers unsealed WAL segments
by **listing the data directory** — looking for `wal_NNNNNN.bin` files that have no
corresponding `.crc` sidecar. It does not rely on in-band signals from the WAL writer
for its file listing; the `WalEntry::SegmentSealed` record may serve as a hint but
the ground truth is the filesystem.

### Communication: in-band via WAL queue

The seal trigger travels through the existing pipeline queue as a
`WalEntry::SegmentSealed` record. No new channels, no shared atomics between stages.

---

## WAL Design

### Segmented files

The WAL is divided into fixed-size segment files. All files live flat in the data
directory — no subdirectories:

```
data/
  wal_000001.bin          ← sealed segment
  wal_000001.crc          ← file-level CRC sidecar
  wal_000002.bin
  wal_000002.crc
  wal_000003.bin
  wal_000003.crc
  wal.bin                 ← current active segment, max wal_segment_size_mb
  snapshot_000001.bin     ← snapshot (if taken at this seal)
  snapshot_000001.crc
  snapshot_000003.bin     ← latest stored snapshot (matches last sealed segment)
  snapshot_000003.crc     ← snapshot integrity sidecar
```

The active segment is `wal.bin` — no numeric suffix until sealed. Sealed segments
are renamed to `wal_NNNNNN.bin`. The distinction between active and sealed is the
filename — `wal.bin` is always the current active segment, `wal_NNNNNN.bin` files
are always sealed and immutable.

### Segment size

Default: **64MB**. Configurable via `LedgerConfig.wal_segment_size_mb`.

At 38 bytes per record and 5M records/s, a new segment is created approximately
every 340ms at peak load. At 100K records/s (sustained), approximately every 4 seconds.

### Two new WalEntry variants

```rust
pub enum WalEntry {
    Metadata(TxMetadata),           // existing
    Entry(TxEntry),                 // existing
    SegmentHeader(SegmentHeader),   // new — first record in every segment
    SegmentSealed(SegmentSealed),   // new — last record before rotation
}
```

**SegmentHeader** — written as the first record of every new segment:

```rust
pub struct SegmentHeader {
    pub magic:       u32,       // 0x524F4441 — "RODA"
    pub version:     u8,        // WAL format version, currently 1
    pub segment_id:  u32,       // monotonic, must match filename
    pub first_tx_id: u64,       // first transaction in this segment
    pub _pad:        [u8; 15],
}                               // 40 bytes
```

Validates that the right file is being read. Catches wrong-file errors before any
records are replayed. `segment_id` must match the numeric suffix in the filename —
mismatch means the file was renamed or misplaced.

**SegmentSealed** — written as the last record before rotation:

```rust
pub struct SegmentSealed {
    pub segment_id:   u32,    // must match SegmentHeader.segment_id
    pub last_tx_id:   u64,    // last committed transaction in this segment
    pub record_count: u64,    // total records including header and sealed records
    pub file_crc32c:  u32,    // CRC32C of entire segment file up to this point
    pub _pad:         [u8; 12],
}                             // 40 bytes
```

The Seal stage receives `SegmentSealed` via the normal pipeline queue and immediately
starts the async sealing sequence — fsync, sidecar write, rename — and triggers
snapshot storing.

### TxEntry.computed_balance

`_pad1` in `TxEntry` is repurposed as `computed_balance: i64`:

```rust
#[repr(C)]
pub struct TxEntry {
    pub entry_type:        u8,        // 1 @ 0
    pub kind:              EntryKind, // 1 @ 1
    pub _pad0:             [u8; 6],   // 6 @ 2
    pub tx_id:             u64,       // 8 @ 8
    pub account_id:        u64,       // 8 @ 16
    pub amount:            u64,       // 8 @ 24
    pub computed_balance:  i64,       // 8 @ 32  ← was _pad1
}                                     // total: 40 bytes — unchanged
```

The Transactor sets `computed_balance` to the account's balance **after** applying
this entry. All downstream stages (Snapshot, Seal) read this field directly — no
balance computation happens outside the Transactor.

### Per-transaction CRC32C

CRC is computed in the **Transactor** — the only stage that has both `TxMetadata` and
all `TxEntry` records simultaneously. CRC covers the complete logical transaction:
`TxMetadata` fields (excluding the `crc32c` field itself) + all `TxEntry` records.

`TxMetadata` gains a `crc32c` field using 4 bytes of existing padding:

```rust
#[repr(C)]
pub struct TxMetadata {
    pub tx_id:        u64,        // 8
    pub timestamp:    u64,        // 8
    pub user_ref:     u64,        // 8
    pub entry_count:  u8,         // 1
    pub fail_reason:  FailReason, // 1
    pub _pad:         [u8; 2],    // 2  (reduced from 6)
    pub crc32c:       u32,        // 4  ← NEW
}                                 // 40 bytes total — unchanged
```

**Why CRC32C over CRC32:**
CRC32C (Castagnoli) is hardware-accelerated on all modern CPUs (SSE4.2 on x86,
ARM CRC extension). Same cost as CRC32, better error detection. Standard for storage
systems — PostgreSQL, TigerBeetle, RocksDB, iSCSI all use CRC32C.

```toml
crc32c = "0.6"   # uses hardware instruction when available, pure Rust fallback
```

**Transactor must buffer before writing CRC:**
The Transactor already buffers `Vec<TxEntry>` before pushing to the WAL queue
(existing behaviour). CRC computation happens after all entries are produced,
before any records are pushed. No new buffering required.

### File-level CRC sidecar

Each sealed segment has a companion `.crc` file:

```
wal_000004.crc:
  [crc32c: 4 bytes]   ← CRC32C of entire wal_000004.bin
  [size:   8 bytes]   ← expected file size in bytes
  [magic:  4 bytes]   ← 0x524F4441
                      = 16 bytes
```

Written and fsynced before the segment is renamed from `wal.bin` to
`wal_000004.bin`. If the `.crc` file is missing for a sealed segment, the
segment is treated as suspect — verify via full scan or reject.

The `size` field catches truncation independently of CRC — if file size does not
match, the file is corrupt even if CRC somehow passes.

---

## Snapshot Design

### Snapshot stage (in-memory)

The `Snapshot` stage sits between `WAL` and `Seal` in the pipeline. It maintains
`Vec<AtomicI64>` — the live in-memory balance cache. For every `TxEntry` it receives,
it reads `entry.computed_balance` and writes it to `balances[account_id]`.

No arithmetic is performed in the Snapshot stage. The balance value comes directly
from `TxEntry.computed_balance`.

### Snapshot storing (part of Seal stage)

Writing snapshots to disk is the responsibility of the **Seal stage**, not the Snapshot
stage. When the Seal stage seals a WAL segment, it also stores a snapshot for that
segment boundary.

The snapshot file format is unchanged (`snapshot_NNNNNN.bin` + `snapshot_NNNNNN.crc`).
The Seal stage collects non-zero balances from the Snapshot stage's `Vec<AtomicI64>`
at the moment of sealing and writes them out.

### Balance source: TxEntry.computed_balance

Balances in the snapshot reflect `TxEntry.computed_balance` values forwarded through
the Snapshot stage. There is exactly one place where balance arithmetic happens:
the Transactor.

---

## Seal Stage Design

### Responsibilities

The Seal stage is a dedicated async process with two responsibilities:

1. **WAL segment sealing** — fsync, write `.crc` sidecar, rename `wal.bin` →
   `wal_NNNNNN.bin`
2. **Snapshot storing** — collect balances from Snapshot stage, write
   `snapshot_NNNNNN.bin` + `.crc`

### Discovering work by listing

The Seal stage discovers which segments need sealing by **listing the data directory**.
It looks for `wal_NNNNNN.bin` files with no corresponding `wal_NNNNNN.crc` — these
are segments that were written but not yet sealed (e.g. after a crash during sealing).

In normal operation the `WalEntry::SegmentSealed` record arriving via the pipeline
queue prompts the Seal stage to act. The directory listing serves as the authoritative
source of truth for recovery scenarios.

### Sealing sequence

```
WAL writer (segment full):
  1. Write WalEntry::SegmentSealed to outbound queue
     (includes file_crc32c computed over all bytes written so far)
  2. Create fresh wal.bin
  3. Write WalEntry::SegmentHeader to new segment
  → continues writing; no wait for sealing

Seal stage (on receiving SegmentSealed):
  1. fsync wal.bin (the just-closed segment)
  2. Write wal.crc (crc32c + size)
  3. fsync wal.crc
  4. Rename wal.bin  → wal_NNNNNN.bin
  5. Rename wal.crc  → wal_NNNNNN.crc
  6. Collect balances from Snapshot stage (Vec<AtomicI64>)
  7. Write snapshot_NNNNNN.bin + snapshot_NNNNNN.crc
```

Steps 4 and 5 are atomic POSIX renames. The WAL writer is never blocked by sealing I/O.

---

## Storage Layout

```
data/
  wal_000001.bin          ← sealed WAL segment
  wal_000001.crc          ← WAL segment integrity sidecar
  wal_000002.bin
  wal_000002.crc
  ...
  wal.bin                 ← current active segment, 0–64MB
  snapshot_000002.bin     ← latest stored snapshot
  snapshot_000002.crc     ← snapshot integrity sidecar
```

Snapshot files use the segment_id of the last sealed segment at the time of storage.
On recovery, the latest valid `snapshot_NNNNNN.bin` is loaded first to seed balances,
then WAL segments from that point forward are replayed.

---

## WAL Replay on Recovery

```
1. Find latest valid snapshot_NNNNNN.bin (verify .crc)
   → seed Vec<AtomicI64> from snapshot records
2. Find all wal_NNNN.bin segments with segment_id > snapshot segment_id
3. For each sealed segment:
     a. Verify wal_NNNN.crc (file-level integrity)
     b. Read SegmentHeader → validate magic, version, segment_id
     c. Replay TxMetadata + TxEntry records:
          verify per-transaction CRC32C
          apply TxEntry.computed_balance to in-memory balances
          on mismatch → stop replay, do not apply further records
     d. Read SegmentSealed → validate record_count
4. Replay wal.bin (active segment — no .crc file)
     Read SegmentHeader → validate
     Replay records with per-transaction CRC verification
     Stop at first CRC mismatch or unexpected EOF
5. Resume normal operation
```

**Corruption handling — never silent, never skip:**

```
File CRC mismatch    → reject entire segment, stop replay, report offset
Per-tx CRC mismatch  → stop replay at this transaction, report tx_id
Missing .crc file    → warn, attempt record-level verification only
Wrong segment_id     → reject immediately, wrong file in directory
Unexpected EOF       → stop replay, treat as end of valid data
```

---

## Consequences

### Positive

- Per-transaction CRC32C catches bit rot before it corrupts balances
- File-level CRC catches full-file corruption and truncation
- SegmentHeader catches wrong-file errors immediately on replay
- Seal stage is fully async — WAL writer never blocks on fsync or rename
- `computed_balance` in `TxEntry` eliminates balance recomputation downstream;
  Snapshot and Seal stages are read-only consumers of pre-computed balances
- Single authoritative balance computation point (Transactor) simplifies correctness
  reasoning
- Seal stage discovers unsealed work by directory listing — robust across crash/restart
- Snapshots retained — recovery time bounded by snapshot age, not full WAL history
- In-band seal trigger via SegmentSealed keeps pipeline communication model clean

### Negative

- `TxEntry._pad1` repurposed as `computed_balance: i64` — breaking wire format change
  for any existing WAL files
- `TxMetadata._pad` reduced from 6 to 2 bytes for CRC field — breaking change
- SegmentHeader and SegmentSealed add two new WalEntry variants — small code change
  across WAL reader, WAL writer, replay path
- One new crate dependency: `crc32c`
- Seal stage has two responsibilities (sealing + snapshot storing) — more complex
  than a single-purpose process, but tightly coupled by design

### Neutral

- Existing `save_snapshot()` zero-balance filtering reused as-is
- Existing tmp → rename atomic write pattern kept for sidecar files
- `snapshot_interval` config field removed; snapshot storing is now seal-triggered

---

## Alternatives Considered

**Compute balances in Seal/Snapshot stages (previous approach)**
Rejected — balance computation in multiple stages creates multiple sources of truth
and risks divergence. Computing once in Transactor and propagating `computed_balance`
through `TxEntry` is simpler, testable at one point, and avoids redundant arithmetic.

**Remove snapshots entirely (previous ADR revision)**
Superseded — snapshots bound recovery time. Without snapshots, recovery time grows
linearly with retained WAL history. Keeping snapshots in the Seal stage gives the
best of both worlds: deterministic write path and bounded recovery time.

**Separate SnapshotStorer process**
Superseded — moving snapshot storing into the Seal stage reduces process count and
makes the snapshot-per-seal relationship explicit. No separate coordination needed.

**Per-record CRC instead of per-transaction CRC**
Rejected — CRC per `TxEntry` individually does not verify that entries belong to
the correct transaction. Per-transaction CRC covers the complete logical unit.

**Synchronous sealing on WAL writer thread**
Rejected — fsync latency (1-10ms) on the WAL writer thread would directly reduce
transaction throughput. Async Seal stage keeps the write path clean.

---

## References

- ADR-001 — entries-based execution model, TxMetadata structure, WAL record format
- ADR-002 — Vec-based balance storage, Vec<AtomicI64> balance cache
- ADR-003 — Ledger API, LedgerConfig
- ADR-005 — PressureManager, park/unpark, Seal stage idle behaviour
- `src/wal.rs` — existing WAL implementation
- `src/snapshot.rs` — existing Snapshot and SnapshotStorer
- `src/entities.rs` — TxMetadata, TxEntry, WalEntry
- PostgreSQL WAL — CRC32C per record, segmented files
- TigerBeetle — CRC32C, explicit corruption rejection
- RocksDB — segmented SST files, async compaction
- `crc32c` crate — hardware-accelerated CRC32C
