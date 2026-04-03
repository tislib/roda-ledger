# ADR-007: CLI Operational Tools

**Status:** Proposed  
**Date:** 2026-04-03  
**Author:** Taleh Ibrahimli  
**Branch:** `feature/adr-007-cli-tools`

---

## Context

roda-ledger stores all state in binary WAL segments and snapshot files (ADR-006).
These files are the source of truth — but they are opaque. An operator who needs to
inspect a specific transaction, verify data integrity, or recover from a manual edit
has no way to do so without writing custom tooling.

Three operational needs have been identified:

**Inspection** — read a WAL segment and view its contents as human-readable JSON.
Needed for debugging, auditing, and understanding what is in a specific segment.

**Verification** — check that WAL segments and snapshot files are intact. Needed
after a disk event, before an archival operation, or to confirm recovery is safe.

**Re-sealing** — after a manual edit (e.g. removing a corrupt record, correcting
a CRC), recompute the `.crc` sidecar and restore the `.seal` marker. Without this,
the modified segment is permanently in CLOSED state and recovery will treat it as
unverified.

These tools operate at the **segment level** — one segment at a time. They do not
interact with a running ledger process.

---

## Decision

Add a `roda-ctl` binary (`src/bin/roda_ctl.rs`) with three subcommands:

```
roda-ctl unpack <segment.bin> [--out <file.json>]
roda-ctl pack   <file.json>   [--out <segment.bin>]
roda-ctl verify [<path>] [--segment <id>] [--range <from>..<to>]
roda-ctl seal   <segment.bin>
```

No interaction with a running ledger. All commands operate on files directly.
The running ledger is not affected — these are offline tools.

---

## Command: `unpack`

Reads a WAL segment binary file and writes its contents as JSON.

```bash
roda-ctl unpack wal_000004.bin
roda-ctl unpack wal_000004.bin --out wal_000004.json
roda-ctl unpack wal_000004.bin --out -            # stdout
```

### Output format

One JSON object per line (NDJSON / JSON Lines). Each line is one WAL record:

```json
{"type":"SegmentHeader","segment_id":4,"version":1,"magic":"RODA","first_tx_id":441001}
{"type":"TxMetadata","tx_id":441001,"entry_count":2,"fail_reason":0,"crc32c":"0x4a2f1c3d","user_ref":0,"timestamp":1743600000}
{"type":"TxEntry","tx_id":441001,"account_id":42,"amount":100,"kind":"Debit","computed_balance":1200}
{"type":"TxEntry","tx_id":441001,"account_id":0,"amount":100,"kind":"Credit","computed_balance":-4200}
{"type":"TxMetadata","tx_id":441002,...}
...
{"type":"SegmentSealed","segment_id":4,"last_tx_id":974231,"record_count":533230}
```

One line per record. Streaming — does not load entire segment into memory before
writing output. Large segments (64MB, ~1.6M records) can be unpacked and piped to
`grep`, `jq`, or other tools without loading everything into memory first.

### CRC verification during unpack

`unpack` verifies per-transaction CRC32C as it reads. If a record fails CRC:

```
ERROR: CRC mismatch at record 12441 (tx_id 453442)
  expected: 0x4a2f1c3d
  actual:   0x7f3a9b11
  offset:   498040
```

Unpack stops at the first CRC failure by default. Use `--ignore-crc` to continue
past failures and emit the corrupt record with a `"crc_error": true` field.

---

## Command: `pack`

Reads a JSON Lines file produced by `unpack` and writes a WAL segment binary.

```bash
roda-ctl pack wal_000004.json
roda-ctl pack wal_000004.json --out wal_000004_fixed.bin
roda-ctl pack - --out wal_000004_fixed.bin   # read from stdin
```

### Use case: manual editing

The intended workflow for manual recovery:

```bash
# 1. unpack
roda-ctl unpack wal_000004.bin --out wal_000004.json

# 2. edit wal_000004.json in any text editor
#    e.g. remove a corrupt transaction, correct an amount

# 3. pack back to binary
roda-ctl pack wal_000004.json --out wal_000004_fixed.bin

# 4. replace original (keep backup)
cp wal_000004.bin wal_000004.bin.bak
cp wal_000004_fixed.bin wal_000004.bin

# 5. reseal (see seal command)
roda-ctl seal wal_000004.bin
```

### CRC recomputation

`pack` always recomputes per-transaction CRC32C from the packed data. The `crc32c`
field in the JSON input is ignored — the output always has fresh, correct CRCs.
This ensures that editing a transaction and repacking produces a valid binary.

### Validation during pack

`pack` validates the JSON input before writing:

- `SegmentHeader` must be the first record
- `SegmentSealed` must be the last record
- `segment_id` in `SegmentHeader` and `SegmentSealed` must match
- `record_count` in `SegmentSealed` must match actual record count
- `tx_id` sequence must be monotonically increasing within the segment

Validation failures abort the pack with an error. Use `--no-validate` to skip
validation and pack regardless (dangerous — for forensics only).

---

## Command: `verify`

Verifies the integrity of WAL segments and snapshot files. Can verify a single
segment, a range of segments, or the entire data directory.

```bash
roda-ctl verify data/               # whole: E2E — all segments + any snapshots found
roda-ctl verify data/ --segment 4   # segment 4 + snapshot_000004 if it exists
roda-ctl verify data/ --range 1..10 # segments 1-10 + any snapshots found alongside
```

No direct file paths. Always operates on a data directory with an optional segment
filter. Snapshots are never verified independently — they are verified as part of
their paired segment check. If `snapshot_NNNN.bin` exists alongside `wal_NNNN.bin`,
it is verified when that segment is checked. If no snapshot exists for a segment,
nothing is expected — no warning, no error.

### Snapshot verification rule

```
For each segment being verified:
  → verify WAL segment (file CRC + record CRC)
  → if snapshot_NNNN.bin exists alongside → verify it (file CRC + data CRC)
  → if no snapshot_NNNN.bin → nothing to check, move on

For each snapshot_NNNN.bin found in data/:
  → wal_NNNN.bin must exist and be SEALED → if not, WARNING (orphan snapshot)
  → verify balances in snapshot against balances in wal_NNNN.bin (and for accounts found only in previouse segments)
```

No assumptions about which segments should have snapshots. `verify` does not know
or care what `snapshot_frequency` was configured as. It verifies what exists.

### Verification levels

**File-level** (always performed for WAL segments):
- `.crc` sidecar exists for SEALED segments
- File size matches `.crc` recorded size
- CRC32C of entire file matches `.crc` recorded checksum

**Snapshot-level** (performed when snapshot exists alongside segment):
- File CRC32C matches `.crc` sidecar
- File size matches `.crc` recorded size
- Data CRC32C in snapshot header matches decompressed record data
- Check balances in snapshot against balances in WAL segment (and for accounts found only in previous segments)

**Record-level** (performed on WAL segments):
- `SegmentHeader` magic and version valid
- `segment_id` matches filename
- Per-transaction CRC32C valid for every `TxMetadata`
- `record_count` in `SegmentSealed` matches actual count
- `tx_id` monotonically increasing throughout segment

**Cross-segment** (performed on directory or range only):
- No gaps in segment numbering
- `first_tx_id` of segment N+1 = `last_tx_id` of segment N + 1
- Active segment (`wal.bin`) tx_id continues from last sealed segment

### Output

```
Verifying data/ ...

  wal_000001.bin   SEALED    533,230 records   tx 1..533230        OK
  wal_000002.bin   SEALED    533,231 records   tx 533231..1066461  OK
  wal_000003.bin   SEALED    412,000 records   tx 1066462..1478461 OK
  wal_000004.bin   SEALED    533,230 records   tx 1478462..2011691 OK
    snapshot_000004.bin      1,200 accounts                        OK
  wal_000005.bin   CLOSED    (awaiting seal)                       OK
  wal.bin          ACTIVE    12,441 records    tx 2011692..        OK

Summary: 6 segments, 1 snapshot, 2,024,133 transactions — all OK
```

Snapshot verification appears indented under the segment it belongs to — makes
the pairing explicit. If no snapshot exists for a segment, nothing is shown.

On failure:

```
  wal_000003.bin   SEALED    412,000 records   tx 1066462..1478461 FAIL
    Record CRC mismatch at record 204,441 (tx_id 1,270,902)
    expected: 0x4a2f1c3d  actual: 0x7f3a9b11  offset: 7,763,358

  wal_000004.bin   SEALED    533,230 records   tx 1478462..2011691 OK
    snapshot_000004.bin      1,200 accounts                        FAIL
    Data CRC mismatch: expected 0x1a2b3c4d  actual 0x9f8e7d6c
```

Exit code 0 on success, 1 on any failure. Suitable for scripts and CI.

---

## Command: `seal`

Recomputes the `.crc` sidecar and restores the `.seal` marker for a WAL segment
in CLOSED state. Used after manual editing to restore a segment to SEALED state.

```bash
roda-ctl seal wal_000004.bin
roda-ctl seal data/ --segment 4     # equivalent
```

### Seal sequence

```
1. Verify segment is in CLOSED state (wal_NNNNNN.bin exists, no .seal marker)
2. Read entire wal_NNNNNN.bin into memory
3. Compute CRC32C of file contents
4. Write wal_NNNNNN.crc (16 bytes: crc32c + size + magic)
5. fsync wal_NNNNNN.crc
6. Create empty wal_NNNNNN.seal marker file
7. fsync wal_NNNNNN.seal
```

This is identical to `Segment.seal()` in ADR-006. The CLI command exposes the same
operation for manual use.

### Guards

`seal` refuses to run if:
- The file does not exist
- The segment is already SEALED (`.seal` marker exists) — use `--force` to reseal
- The segment is ACTIVE (`wal.bin`) — cannot seal the active segment

`--force` removes existing `.crc` and `.seal` files before resealing. Used when
a manual edit requires resealing a previously sealed segment.

```bash
# After editing an already-sealed segment:
roda-ctl seal wal_000004.bin --force
```

---

## Safety Notes

**`pack` and `seal` are dangerous.** They modify the source of truth. Always back up
the original files before using them.

**Do not run CLI tools while the ledger is running.** These are offline tools. Running
`seal` or `pack` on files that the ledger is actively reading or writing will produce
undefined behaviour.

**CRC recomputation in `pack` is not magic.** Packing a file with logically wrong
data (e.g. wrong amounts) will produce a valid binary with correct CRCs. The tools
verify format integrity, not business logic correctness.

---

## Implementation Notes

### Binary location

```
src/bin/roda_ctl.rs
```

Compiled as a separate binary alongside `stress_testing`. Uses the `storage` module
from ADR-006 (`src/storage/`) directly — same serialization, same CRC computation,
same segment lifecycle logic.

### Dependencies

No new dependencies beyond what ADR-006 already requires (`crc32c`, `lz4_flex`,
`bytemuck`). JSON output uses `serde_json` already present in `Cargo.toml`.

### Feature flag

```toml
[[bin]]
name = "roda-ctl"
path = "src/bin/roda_ctl.rs"
```

No feature flag — always built. The binary is small and adds no compile-time cost
to the library.

---

## Consequences

### Positive

- Operators can inspect WAL contents without writing custom code
- Integrity verification is scriptable and CI-compatible (exit codes)
- Manual recovery from corruption is possible via unpack → edit → pack → seal
- `pack` always recomputes CRC — edited segments are always valid after packing
- `verify` covers file-level, record-level, and cross-segment consistency
- Streaming unpack handles large segments without memory pressure
- Tools reuse existing `storage` module — no new serialization logic

### Negative

- `pack` and `seal` are dangerous — documented but not preventable
- No protection against running while ledger is live — operator responsibility
- `pack` recomputes CRC but cannot validate business logic correctness
- `--force` on `seal` allows resealing without any warning beyond documentation

### Neutral

- `roda-ctl` is a separate binary — not part of the server, not part of the library
- All commands are offline — no gRPC, no running ledger required
- NDJSON output format is streamable and compatible with standard Unix tools

---

## Alternatives Considered

**Single `roda-ctl inspect` command with flags**
Rejected — `unpack`, `pack`, `verify`, and `seal` are distinct operations with
distinct safety profiles. Separate subcommands make intent clear and allow separate
`--help` documentation per command.

**Binary-level hex dump instead of JSON**
Rejected — JSON is human-readable and editable without knowledge of the binary
format. The purpose of `unpack` is inspection and editing, not low-level debugging.

**Verify as part of recovery (not a separate command)**
Recovery already verifies during replay. The `verify` command is for offline
pre-checks — before archiving, after a disk event, in CI. Different use case.

**Integrate tools into the server binary as subcommands**
Rejected — mixing operational tools with the server binary conflates concerns.
`roda-ctl` is a standalone tool that can be run on any machine with access to
the data directory, not just on the server.

---

## References

- ADR-006 — WAL segment lifecycle (ACTIVE/CLOSED/SEALED), `.seal` marker,
  CRC sidecar format, `Segment.seal()`, `Storage` facade
- `src/storage/segment.rs` — Segment struct, lifecycle states
- `src/storage/wal_serializer.rs` — zero-copy record codec (reused by pack/unpack)
- `src/storage/wal_reader.rs` — WAL loading and CRC verification (reused by verify)
- `src/storage/snapshot.rs` — snapshot format (reused by verify)
- `src/bin/stress_testing.rs` — existing binary pattern