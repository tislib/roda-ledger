# ADR-006: WAL, Snapshot, and Seal Durability

**Status:** Accepted
**Date:** 2026-03-31
**Updated:** 2026-04-03
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

### Pipeline: four stages plus background sealer

The processing pipeline has four queue-connected stages in order:

```
Sequencer → Transactor → WAL → Snapshot
```

Each stage runs in its own thread, connected by `ArrayQueue<WalEntry>` queues. The
`Seal` process runs as a **separate background thread** that polls the filesystem for
unsealed segments — it is not connected to any queue and does not receive entries
from the pipeline.

### WAL: segmented files with two-level integrity

Replace the single WAL file with fixed-size segments. Each segment has a header record,
per-transaction CRC, and a sealed record. A sidecar `.crc` file provides file-level
integrity for sealed segments.

### Snapshots: retained, stored as part of Seal stage

Snapshots use a new format with a 36-byte header, LZ4 compression, and two-level CRC
(data CRC in header, file CRC in sidecar). Snapshot storing is the responsibility of
the Seal thread. Snapshots are written at a configurable frequency — every Nth sealed
segment (default: every 4th segment).

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
Snapshot and Seal stages read `TxEntry.computed_balance` directly — no balance
recomputation is needed anywhere downstream.

### Seal thread: background poller, discovers work by listing

The Seal thread runs independently of the pipeline. It periodically scans the data
directory for segments that are CLOSED but not yet SEALED (i.e., `wal_NNNNNN.bin`
exists but no `wal_NNNNNN.seal` marker). It loads the segment, computes CRC, writes
the `.crc` sidecar and `.seal` marker, then conditionally writes a snapshot.

### Storage module: separated concerns

All storage I/O is consolidated into a `src/storage/` module with separated concerns:

- `layout.rs` — file path conventions and segment ID parsing
- `segment.rs` — segment lifecycle (ACTIVE → CLOSED → SEALED)
- `snapshot.rs` — snapshot serialization/deserialization (impl on `Segment`)
- `storage.rs` — `Storage` facade, segment factory, directory enumeration
- `wal_reader.rs` — WAL binary loading and CRC verification
- `wal_serializer.rs` — zero-copy record codec via `bytemuck`

### Communication: WAL forwards entries downstream; Seal polls independently

The WAL stage writes entries to the active segment and forwards them downstream to
the Snapshot stage via `ArrayQueue`. When a segment exceeds the size threshold, the
WAL appends a `SegmentSealed` record, closes the segment (rename), and opens a new
active segment. The Seal thread discovers closed segments by polling the filesystem.

---

## WAL Design

### Segmented files

The WAL is divided into fixed-size segment files. All files live flat in the data
directory — no subdirectories:

```
data/
  wal_000001.bin          ← sealed segment
  wal_000001.crc          ← file-level CRC sidecar
  wal_000001.seal         ← seal marker (empty file)
  wal_000002.bin
  wal_000002.crc
  wal_000002.seal
  wal_000003.bin          ← closed but not yet sealed
  wal_000003.crc
  wal.bin                 ← current active segment
  snapshot_000004.bin     ← snapshot (if taken at segment 4)
  snapshot_000004.crc     ← snapshot integrity sidecar
```

The active segment is `wal.bin` — no numeric suffix until closed. When the WAL
closes a segment, it is renamed to `wal_NNNNNN.bin` (CLOSED state). The Seal thread
then writes `.crc` and `.seal` sidecars to transition it to SEALED state.

### Segment lifecycle: three states

```
ACTIVE → CLOSED → SEALED
```

| State  | File              | CRC sidecar | Seal marker | Description                    |
|--------|-------------------|-------------|-------------|--------------------------------|
| ACTIVE | `wal.bin`         | No          | No          | Open for appends               |
| CLOSED | `wal_NNNNNN.bin`  | No          | No          | Renamed, awaiting seal         |
| SEALED | `wal_NNNNNN.bin`  | `.crc`      | `.seal`     | Checksummed, immutable         |

State is determined by filesystem inspection:
- `wal_NNNNNN.seal` exists → SEALED
- `wal_NNNNNN.bin` exists without `.seal` → CLOSED
- `wal.bin` → ACTIVE

### Segment size

Default: **64MB**. Configurable via `StorageConfig.wal_segment_size_mb`.

At 40 bytes per record and 5M records/s, a new segment is created approximately
every 340ms at peak load. At 100K records/s (sustained), approximately every 17 seconds.

### Segment ID assignment

The active segment ID is `last_segment_id + 1`, where `last_segment_id` is the
highest numeric suffix found among `wal_NNNNNN.bin` files in the data directory.
If no segments exist, the first active segment gets ID 1.

### Four WalEntry variants

All WAL records are exactly **40 bytes** with `entry_type: u8` as the first byte,
enabling self-describing record scanning. Zero-copy serialization via `bytemuck`
(`Pod + Zeroable` traits).

```rust
pub enum WalEntry {
    Metadata(TxMetadata),           // entry_type = 0
    Entry(TxEntry),                 // entry_type = 1
    SegmentHeader(SegmentHeader),   // entry_type = 2
    SegmentSealed(SegmentSealed),   // entry_type = 3
}
```

Compile-time assertions enforce the 40-byte invariant:

```rust
const _: () = assert!(std::mem::size_of::<TxMetadata>() == 40);
const _: () = assert!(std::mem::size_of::<TxEntry>() == 40);
const _: () = assert!(std::mem::size_of::<SegmentHeader>() == 40);
const _: () = assert!(std::mem::size_of::<SegmentSealed>() == 40);
```

### TxMetadata (40 bytes)

```rust
#[repr(C)]
pub struct TxMetadata {
    pub entry_type:   u8,           // 1 @ 0  — WalEntryKind::TxMetadata (0)
    pub entry_count:  u8,           // 1 @ 1  — 0 if transaction failed
    pub fail_reason:  FailReason,   // 1 @ 2  — FailReason::NONE if succeeded
    pub flags:        u8,           // 1 @ 3  — reserved flags
    pub crc32c:       u32,          // 4 @ 4  — CRC32C; zero this field when computing
    pub tx_id:        u64,          // 8 @ 8
    pub timestamp:    u64,          // 8 @ 16
    pub user_ref:     u64,          // 8 @ 24
    pub tag:          [u8; 8],      // 8 @ 32 — 8-byte metadata tag
}                                   // total: 40 bytes
```

### SegmentHeader (40 bytes)

Written as the first record of every new segment:

```rust
#[repr(C)]
pub struct SegmentHeader {
    pub entry_type:   u8,           // 1 @ 0  — WalEntryKind::SegmentHeader (2)
    pub version:      u8,           // 1 @ 1  — WAL_VERSION (1)
    pub _pad0:        [u8; 2],      // 2 @ 2
    pub magic:        u32,          // 4 @ 4  — WAL_MAGIC (0x524F4441 = "RODA")
    pub segment_id:   u32,          // 4 @ 8  — monotonic, must match filename
    pub _pad1:        [u8; 4],      // 4 @ 12
    pub first_tx_id:  u64,          // 8 @ 16 — first transaction in this segment
    pub _pad2:        [u8; 16],     // 16 @ 24
}                                   // total: 40 bytes
```

Validates that the right file is being read. `segment_id` must match the numeric
suffix in the filename.

### SegmentSealed (40 bytes)

Written as the last record before segment closure:

```rust
#[repr(C)]
pub struct SegmentSealed {
    pub entry_type:    u8,          // 1 @ 0  — WalEntryKind::SegmentSealed (3)
    pub _pad0:         [u8; 3],     // 3 @ 1
    pub segment_id:    u32,         // 4 @ 4  — matches SegmentHeader.segment_id
    pub last_tx_id:    u64,         // 8 @ 8  — last committed transaction
    pub record_count:  u64,         // 8 @ 16 — total records (including header + sealed)
    pub _pad1:         [u8; 16],    // 16 @ 24
}                                   // total: 40 bytes
```

The `record_count` includes the sealed record itself (incremented by 1 in the
constructor).

### TxEntry (40 bytes)

```rust
#[repr(C)]
pub struct TxEntry {
    pub entry_type:        u8,          // 1 @ 0  — WalEntryKind::TxEntry (1)
    pub kind:              EntryKind,   // 1 @ 1  — Credit (0) or Debit (1)
    pub _pad0:             [u8; 6],     // 6 @ 2
    pub tx_id:             u64,         // 8 @ 8
    pub account_id:        u64,         // 8 @ 16
    pub amount:            u64,         // 8 @ 24 — integer minor units
    pub computed_balance:  i64,         // 8 @ 32 — set by Transactor
}                                       // total: 40 bytes
```

The Transactor sets `computed_balance` to the account's balance **after** applying
this entry. All downstream stages read this field directly.

### Per-transaction CRC32C

CRC is computed in the **Transactor** — the only stage that has both `TxMetadata` and
all `TxEntry` records simultaneously. CRC covers the complete logical transaction:
`TxMetadata` fields (with `crc32c` field zeroed) + all `TxEntry` records.

The `crc32c` field in `TxMetadata` is set to zero during computation, then filled
with the computed value.

**Why CRC32C over CRC32:**
CRC32C (Castagnoli) is hardware-accelerated on all modern CPUs (SSE4.2 on x86,
ARM CRC extension). Same cost as CRC32, better error detection. Standard for storage
systems — PostgreSQL, TigerBeetle, RocksDB, iSCSI all use CRC32C.

```toml
crc32c = "0.6"   # uses hardware instruction when available, pure Rust fallback
```

### File-level CRC sidecar

Each sealed segment has a companion `.crc` file:

```
wal_000004.crc:
  [crc32c: 4 bytes LE]   ← CRC32C of entire wal_000004.bin
  [size:   8 bytes LE]   ← expected file size in bytes
  [magic:  4 bytes LE]   ← WAL_MAGIC (0x524F4441)
                          = 16 bytes
```

Written by the Seal thread after loading the segment data and computing CRC.
The `.seal` marker file (empty) is created after the `.crc` sidecar to signal
that sealing is complete.

The `size` field catches truncation independently of CRC — if file size does not
match, the file is corrupt even if CRC somehow passes.

### Write buffer

The active segment uses a **16MB write buffer** (`Vec<u8>`) to batch serialized
entries before writing. Each `append_entries` call serializes all entries into the
buffer, writes the buffer to the file, and calls `fsync`. The buffer is cleared
between calls.

```rust
wal_buffer: vec![0; 16 * 1024 * 1024], // 16MB
```

If `write_all` or `sync_data` fails, the segment retries after a 1-second sleep
until the I/O succeeds.

### Zero-copy serialization

WAL records are serialized using `bytemuck::bytes_of()` (zero-copy, returns borrowed
slice) and deserialized using `bytemuck::pod_read_unaligned()`. The first byte of each
40-byte chunk is the `entry_type` discriminant, used to select the correct struct for
deserialization.

```rust
pub fn serialize_wal_records(entry: &WalEntry) -> &[u8]   // zero-copy
pub fn parse_wal_record(data: &[u8]) -> Result<WalEntry>   // pod_read_unaligned
```

---

## Snapshot Design

### Snapshot stage (in-memory)

The `Snapshot` stage sits at the end of the pipeline, consuming entries from the
WAL stage via `ArrayQueue<WalEntry>`. It maintains `Arc<Vec<AtomicI64>>` — the live
in-memory balance cache. For every `TxEntry` it receives, it stores
`entry.computed_balance` into `balances[account_id]` using `Ordering::Release`.

No arithmetic is performed in the Snapshot stage. The balance value comes directly
from `TxEntry.computed_balance`. Balance reads use `Ordering::Acquire`.

### Snapshot storing (part of Seal thread)

Writing snapshots to disk is the responsibility of the **Seal thread**, not the
Snapshot stage. The Seal thread conditionally writes a snapshot based on
`StorageConfig.snapshot_frequency`:

- `snapshot_frequency = 0` — disabled, no snapshots written
- `snapshot_frequency = N` — snapshot written when `segment_id % N == 0`
- Default: `snapshot_frequency = 4` (snapshot every 4th sealed segment)

The Seal thread maintains its own `Vec<i64>` balance cache, updated by replaying
WAL records from each sealed segment. Snapshots are written from the Seal thread's
balance state, not from the Snapshot stage's `AtomicI64` vector.

### Snapshot file format

New binary format with magic number, LZ4 compression, and two-level CRC:

**36-byte header:**

```
Offset  Size  Field            Value
0       4     magic            0x534E4150 ("SNAP")
4       1     version          1
5       4     segment_id       segment being snapshotted
9       8     checkpoint_id    = segment_id as u64
17      8     account_count    number of account records
25      1     compressed       1 (always compressed for new snapshots)
26      6     pad              reserved zeros
32      4     data_crc32c      CRC32C of raw (uncompressed) record data
```

**Body (after header):**

LZ4-compressed (using `lz4_flex::compress_prepend_size`). After decompression,
each account record is 16 bytes:

```
[account_id: u64 LE][balance: i64 LE]   per account, 16 bytes
```

**Sidecar (`.crc`):**

```
snapshot_000004.crc:
  [crc32c: 4 bytes LE]   ← CRC32C of entire snapshot_000004.bin
  [size:   8 bytes LE]   ← expected file size
  [magic:  4 bytes LE]   ← SNAPSHOT_MAGIC (0x534E4150)
                          = 16 bytes
```

**Two-level CRC:**

- **Data CRC** (in header): CRC32C of raw uncompressed record data — catches
  decompression errors
- **File CRC** (in sidecar): CRC32C of entire snapshot file including header —
  catches on-disk corruption

### Atomic writes via temp files

Snapshots use temp files for crash safety:

1. Write header + LZ4 body to `snapshot_NNNNNN.bin.tmp`
2. Compute file CRC, write sidecar to `snapshot_NNNNNN.crc.tmp`
3. Atomic rename `snapshot_NNNNNN.bin.tmp` → `snapshot_NNNNNN.bin`
4. Atomic rename `snapshot_NNNNNN.crc.tmp` → `snapshot_NNNNNN.crc`

Crash during write leaves only `.tmp` files — recovery ignores them.

### Sparse storage

Only non-zero balances are saved. Accounts with zero balance are filtered out.
Records are sorted by account ID for deterministic output.

---

## Seal Thread Design

### Responsibilities

The Seal thread is a dedicated background thread (not a pipeline stage) with two
responsibilities:

1. **WAL segment sealing** — load segment, compute CRC, write `.crc` sidecar,
   write `.seal` marker
2. **Conditional snapshot storing** — collect balances from WAL replay, write
   `snapshot_NNNNNN.bin` + `.crc` at configured frequency

### Polling loop

The Seal thread runs a loop:

```
loop:
  1. Call seal_pending_segments()
       → list_all_segments()
       → skip segments with status == SEALED
       → process_seal() for each non-sealed segment
  2. Check running flag → exit if false
  3. Sleep for seal_check_interval (configurable Duration)
```

### Process seal sequence

For each non-sealed segment:

```
process_seal(segment):
  1. segment.load()         → read wal_NNNNNN.bin into memory
  2. segment.seal()         → compute CRC, write .crc sidecar, write .seal marker
  3. Update last_sealed_id  → AtomicU32::store(segment_id)
  4. segment.visit_wal_records() → replay entries, update balances Vec<i64>
  5. If segment_id % snapshot_frequency == 0:
       → filter non-zero balances
       → sort by account_id
       → segment.save_snapshot(records)
```

### Segment.close() — called by WAL thread

```
WAL thread (segment full):
  1. Append WalEntry::SegmentSealed to active segment
  2. fsync active segment
  3. Rename wal.bin → wal_NNNNNN.bin          ← status becomes CLOSED
  4. Drop old segment handle
  5. Open new active segment (ID = last_segment_id + 1)
  6. Continue writing (SegmentHeader on first write)
```

### Segment.seal() — called by Seal thread

```
Seal thread (on discovering CLOSED segment):
  1. Compute CRC32C of entire wal_NNNNNN.bin data
  2. Write wal_NNNNNN.crc (16 bytes: crc32c + size + magic)
  3. fsync wal_NNNNNN.crc
  4. Create empty wal_NNNNNN.seal marker file
  5. fsync wal_NNNNNN.seal                    ← status becomes SEALED
```

The WAL thread is never blocked by sealing I/O. Sealing happens asynchronously
on the Seal thread.

### Balance tracking

The Seal thread maintains its own `Vec<i64>` (sized to `account_count`). During
`process_seal`, it replays all `TxEntry` records from the segment and updates
`balances[account_id] = entry.computed_balance`. This provides the balance state
needed for snapshot writing without depending on the Snapshot stage's atomics.

---

## Storage Module

### Module structure

All storage I/O is organized under `src/storage/`:

```
src/storage/
  mod.rs              ← re-exports public API
  layout.rs           ← file path conventions, segment ID parsing
  segment.rs          ← Segment struct, lifecycle (ACTIVE/CLOSED/SEALED)
  snapshot.rs         ← snapshot save/load (impl on Segment)
  storage.rs          ← Storage facade, StorageConfig, segment factory
  wal_reader.rs       ← WAL binary loading, CRC verification
  wal_serializer.rs   ← zero-copy record codec (bytemuck)
```

### StorageConfig

```rust
pub struct StorageConfig {
    pub data_dir: String,           // where to store files (default: "data/")
    pub temporary: bool,            // if true, delete data_dir on drop
    pub wal_segment_size_mb: u64,   // segment size threshold (default: 64)
    pub snapshot_frequency: u32,    // every Nth sealed segment (default: 4, 0=disabled)
}
```

### Storage facade

`Storage` provides the entry point for segment operations:

- `active_segment()` — open or create the current `wal.bin` for writes
- `segment(id)` — open an existing segment by ID
- `list_all_segments()` — enumerate all `wal_NNNNNN.bin` segments, sorted by ID
- `last_segment_id()` — find the maximum segment ID (0 if none)

If `temporary = true`, the data directory is recursively deleted on `Drop`.

### Segment struct

```rust
pub struct Segment {
    data_dir: String,
    segment_id: u32,
    loaded: bool,

    wal_file: Option<File>,     // file handle (None when unloaded)
    wal_data: Vec<u8>,          // raw binary data
    status: SegmentStaus,       // ACTIVE, CLOSED, or SEALED

    // ACTIVE segment only
    wal_buffer: Vec<u8>,        // 16 MB write buffer
    wal_position: usize,        // current file offset
    record_count: u64,          // count of appended records
}
```

**Lazy loading:** CLOSED/SEALED segments can be opened without reading data into
memory. `load()` must be called explicitly before accessing WAL records.

**State assertions:** Operations panic if called in the wrong state (e.g.,
`append_entries` only works on ACTIVE, `seal` only works on CLOSED).

---

## Storage Layout

```
data/
  wal_000001.bin          ← sealed WAL segment
  wal_000001.crc          ← WAL segment integrity sidecar (16 bytes)
  wal_000001.seal         ← seal marker (empty file)
  wal_000002.bin
  wal_000002.crc
  wal_000002.seal
  ...
  wal.bin                 ← current active segment, 0–64MB
  snapshot_000004.bin     ← snapshot at segment 4 (if frequency=4)
  snapshot_000004.crc     ← snapshot integrity sidecar (16 bytes)
  snapshot_000008.bin     ← snapshot at segment 8
  snapshot_000008.crc
```

Snapshot files use the segment_id of the sealed segment at which they were written.
On recovery, the latest valid `snapshot_NNNNNN.bin` is loaded first to seed balances,
then WAL segments from that point forward are replayed.

---

## Recovery

Recovery is coordinated by the `Recover` struct (`src/recover.rs`), which holds
mutable references to all pipeline stages:

```rust
pub struct Recover<'r> {
    transactor: &'r mut Transactor,
    wal: &'r Wal,
    snapshot: &'r Snapshot,
    seal: &'r mut Seal,
    sequencer: &'r Sequencer,
    storage: &'r Storage,
    segments: Vec<Segment>,
}
```

### Recovery algorithm

```
1. List all segments (sorted by ID)
2. Locate latest snapshot:
     → scan segments for has_snapshot()
     → use the highest segment_id with a snapshot
3. Restore snapshot balances (if found):
     → load_snapshot() from snapshot segment
     → for each (account_id, balance):
          restore to Transactor, Snapshot stage, and Seal runner
     → set last_tx_id from snapshot
4. Replay sealed segments after snapshot:
     for each segment with id > snapshot_segment_id:
       → if not SEALED, pre-seal it (seal.recover_pre_seal())
       → load and visit WAL records
       → update last_tx_id from TxMetadata
       → restore balances from TxEntry.computed_balance
         to Transactor and Snapshot stage
5. Replay active segment (wal.bin):
     → visit WAL records
     → update last_tx_id, restore balances
6. Restore sequencer state:
     → sequencer.set_next_id(last_tx_id + 1)
     → transactor.store_last_processed_id(last_tx_id)
     → snapshot.store_last_processed_id(last_tx_id)
```

### Recovery guarantees

- Unsealed segments encountered during recovery are pre-sealed before replay
- Balance state is restored to three systems: Transactor, Snapshot stage, Seal runner
- Sequencer resumes with the next ID after the last recovered transaction
- Active segment (`wal.bin`) is replayed last, handling partial writes at EOF

---

## Source Module Organization

```
src/
  entities.rs           ← TxMetadata, TxEntry, SegmentHeader, SegmentSealed, WalEntry
  entries.rs            ← constructor helpers for SegmentHeader, SegmentSealed
  wal.rs                ← WAL pipeline stage (WalRunner thread)
  snapshot.rs           ← Snapshot pipeline stage (in-memory balance cache)
  seal.rs               ← Seal background thread (sealing + snapshot storing)
  recover.rs            ← Recover struct (crash recovery coordinator)
  sequencer.rs          ← Sequencer (atomic tx_id generator)
  transactor.rs         ← Transactor (balance validation, CRC computation)
  ledger.rs             ← Ledger orchestrator (pipeline assembly, startup)
  storage/
    mod.rs              ← re-exports
    layout.rs           ← file path conventions
    segment.rs          ← Segment lifecycle
    snapshot.rs         ← snapshot format (impl on Segment)
    storage.rs          ← Storage facade, StorageConfig
    wal_reader.rs       ← WAL loading and verification
    wal_serializer.rs   ← zero-copy record codec
```

**Deleted modules:** `src/replay.rs`, `src/snapshot_store.rs` — functionality
absorbed into `src/recover.rs` and `src/storage/snapshot.rs`.

---

## Consequences

### Positive

- Per-transaction CRC32C catches bit rot before it corrupts balances
- File-level CRC catches full-file corruption and truncation
- SegmentHeader catches wrong-file errors immediately on replay
- Three-state segment lifecycle (ACTIVE → CLOSED → SEALED) provides clear phase
  boundaries and crash-safe transitions
- `.seal` marker file makes sealed status deterministic via filesystem inspection
- Seal thread is fully independent — WAL writer never blocks on fsync or CRC
- `computed_balance` in `TxEntry` eliminates balance recomputation downstream;
  Snapshot and Seal stages are read-only consumers of pre-computed balances
- Single authoritative balance computation point (Transactor) simplifies correctness
- Seal thread discovers unsealed work by directory listing — robust across crash/restart
- Configurable snapshot frequency balances storage cost vs recovery time
- LZ4-compressed snapshots reduce disk footprint; two-level CRC catches both
  decompression errors and on-disk corruption
- Atomic temp-file writes for snapshots prevent partial-write corruption
- `bytemuck` zero-copy serialization avoids allocation on the write path
- 16MB write buffer batches I/O for throughput
- Storage module separation provides clean layering between I/O and pipeline logic
- `Recover` struct coordinates multi-system state restoration in a single pass

### Negative

- `TxEntry._pad1` repurposed as `computed_balance: i64` — breaking wire format change
  for any existing WAL files
- `TxMetadata` layout changed (added `flags`, `tag`, moved `crc32c`) — breaking change
- SegmentHeader and SegmentSealed add two new WalEntry variants — code change
  across WAL reader, WAL writer, recovery path
- New crate dependencies: `crc32c`, `lz4_flex`, `bytemuck`
- Seal thread has two responsibilities (sealing + snapshot storing) — more complex
  than a single-purpose process, but tightly coupled by design
- Seal thread maintains its own balance vector (duplicated from Snapshot stage),
  needed because Seal runs independently without queue access

### Neutral

- Snapshot format is completely new (not backward-compatible with old format)
- `snapshot_interval` config field replaced by `snapshot_frequency` (segment-based)
- Existing tmp → rename atomic write pattern kept for snapshot files
- Zero-balance filtering for snapshots preserved from original design

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
Superseded — moving snapshot storing into the Seal thread reduces thread count and
makes the snapshot-per-seal relationship explicit. No separate coordination needed.

**Seal as pipeline stage (receiving SegmentSealed via queue)**
Superseded — making Seal a queue consumer would couple it to the pipeline and require
the WAL to wait for Seal backpressure. A polling thread is simpler, crash-safe
(discovers work by listing), and allows the WAL to proceed without blocking.

**Per-record CRC instead of per-transaction CRC**
Rejected — CRC per `TxEntry` individually does not verify that entries belong to
the correct transaction. Per-transaction CRC covers the complete logical unit.

**Synchronous sealing on WAL writer thread**
Rejected — fsync latency (1-10ms) on the WAL writer thread would directly reduce
transaction throughput. Async Seal thread keeps the write path clean.

**Two-state segment lifecycle (ACTIVE → SEALED)**
Superseded — without an intermediate CLOSED state, a crash between rename and CRC
write would leave a segment in an ambiguous state. The three-state model with
`.seal` marker makes each transition atomic and inspectable.

---

## References

- ADR-001 — entries-based execution model, TxMetadata structure, WAL record format
- ADR-002 — Vec-based balance storage, Vec<AtomicI64> balance cache
- ADR-003 — Ledger API, LedgerConfig
- ADR-005 — PressureManager, park/unpark, Seal thread idle behaviour
- `src/entities.rs` — TxMetadata, TxEntry, SegmentHeader, SegmentSealed, WalEntry
- `src/entries.rs` — SegmentHeader/SegmentSealed constructors
- `src/wal.rs` — WAL pipeline stage
- `src/snapshot.rs` — Snapshot pipeline stage (in-memory)
- `src/seal.rs` — Seal background thread
- `src/recover.rs` — Recovery coordinator
- `src/storage/segment.rs` — Segment lifecycle
- `src/storage/snapshot.rs` — Snapshot serialization format
- `src/storage/storage.rs` — Storage facade, StorageConfig
- `src/storage/wal_serializer.rs` — Zero-copy record codec
- PostgreSQL WAL — CRC32C per record, segmented files
- TigerBeetle — CRC32C, explicit corruption rejection
- RocksDB — segmented SST files, async compaction
- `crc32c` crate — hardware-accelerated CRC32C
- `lz4_flex` crate — pure Rust LZ4 compression
- `bytemuck` crate — zero-copy Pod/Zeroable serialization
