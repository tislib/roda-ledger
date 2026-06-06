# ADR-020: Trailer Metadata — Commit-Record WAL Transaction Layout

**Status:** Accepted  
**Date:** 2026-06-06  
**Author:** Taleh Ibrahimli  

**Amends:**
- ADR-006 — changes the on-disk record order within a transaction and the recovery scan
- ADR-019 (Transaction Ring) — removes the in-ring metadata back-fill (`patch`) step
- ADR-001 — record ordering of the entries-based execution model

---

## Context

Each atomic transaction is a `TxMetadata` record followed by its sub-items (`TxEntry`,
`TxLink`, `TxTerm`, `FunctionRegistered`). Every record is a fixed 40-byte POD struct
whose first byte is the `WalEntryKind` discriminant. Today the metadata is written
**first**:

```
TxMetadata, TxEntry, TxEntry, …      (header layout)
```

The metadata carries two fields that can only be known once the whole transaction has
been built: `sub_item_count` (how many followers belong to it) and `crc32c` (a checksum
over the metadata plus every follower). Because the metadata is written *before* its
followers exist, the writer must go back and **patch** it after the fact:

```rust
// finalize_committed_meta — transactor.rs
let sub_item_count = head - meta_idx - 1;
self.tx_ring_pusher.patch(meta_idx, |m| { m.sub_item_count = …; m.crc32c = 0; });
self.tx_ring_pusher.walk(meta_idx, head, |entry| { /* fold CRC over meta + followers */ });
self.tx_ring_pusher.patch(meta_idx, |m| { m.crc32c = digest; });   // back-patch again
```

Two structural costs follow from "metadata first":

1. **Write-side back-patching.** The metadata is touched three times — written, then
   patched for the count, then patched again for the CRC computed by *walking* the
   followers ([transactor.rs:263-290](../../crates/ledger/src/transactor.rs)). This is
   the sole reason `TxRingWriter::patch` exists (mutate an already-written,
   not-yet-published slot). The checksum simply cannot be finalized until the followers
   are on the ring.

2. **Trusting an unverified count on read.** Recovery reads the leading metadata, takes
   its `sub_item_count` on faith to know how far to scan, and only *then* checks that
   that many valid followers are present and that the CRC matches
   ([recover.rs:118-180](../../crates/ledger/src/recover.rs)). Detecting a partial tail
   means asking "did all the followers the header promised actually arrive?" — but the
   header is the very thing being validated.

The classic remedy is the **commit record**: write the followers first and the metadata
last, so the metadata becomes the transaction's terminator.

---

## Decision

Flip the on-disk order so the metadata is the **trailer**:

```
TxEntry, TxEntry, …, TxMetadata      (trailer layout)
```

The trailing `TxMetadata` is the transaction's **commit record**. A transaction exists,
atomically, exactly when its metadata has been written; until then its followers are an
uncommitted prefix. The 40-byte record layout and `sub_item_count` are unchanged — only
the *position* of the metadata and the *direction* of the scan change.

### The commit-record invariant

> A complete transaction always **ends** with a `TxMetadata`. If the WAL stream (or any
> prefix of it) does not end on a `TxMetadata` boundary, the trailing bytes are a
> partial transaction and are discarded.

This collapses completeness checking, partial-data detection, and partial-data removal
into one structural question — *"does the valid region end at a metadata?"* — instead of
"trust the leading count, then verify."

### Write side — autocompute, no back-patch

`TransactorState` accumulates the two derived fields as it goes, rather than
reconstructing them afterwards:

- a **running CRC digest** folded over each follower as it is pushed, and
- a **running follower count** (`pending_items`).

When the transaction finalizes, the metadata is assembled **once** in a local with these
values already known and pushed last. Nothing is patched.

**Exact CRC rule** (writer and reader must compute the identical bytes):

- Start `running = 0`. For each follower, in push order:
  `running = crc32c_append(running, bytes_of(follower))`.
  (`crc32c_append(0, x) == crc32c(x)`, so the first follower needs no special case.)
- At materialize: build the `TxMetadata` with `crc32c = 0` and every header field set,
  then `crc32c = crc32c_append(running, bytes_of(meta_zeroed))`. Push it. Reset
  `running = 0` and `pending_items = 0` for the next transaction.
- A reader recomputes over the buffered followers in stream order, then the zeroed-crc
  metadata, and compares.

The CRC therefore covers **the followers (in order) followed by the zeroed-crc
metadata** — the same byte set today's checksum covers, so `tx_id`, `user_ref`,
`timestamp`, and `tag` remain integrity-protected; only the order within the digest
changes (followers first). A rejected or otherwise empty transaction has no followers,
so `running` is still `0` and the stored CRC reduces to `crc32c(meta_zeroed)` —
identical to today's lone-metadata coverage.

This removes `TxRingWriter::patch` entirely (its only callers were the two finalizers).
The in-ring `walk` stays — `verify` (the zero-sum check) and `rollback` still range over
the in-flight transaction's slots. The ring's "back-fill the metadata once the sub-item
count and checksum are known" step described in ADR-019 is eliminated: the writer
assembles the metadata complete and publishes it in a single push.

### Read / recovery — buffer until the trailer

Reads flip from "metadata announces N, consume N" to "buffer followers until a metadata
closes the group." The metadata still validates the group via `sub_item_count` and the
CRC; only the scan direction changes.

- **Crash recovery / tail validation.** Forward-scan, buffering follower records, until
  a `TxMetadata` is reached; the metadata closes and validates the group (buffered count
  equals `sub_item_count`, CRC matches). The valid region must end exactly at a metadata
  boundary. If the scan ends with buffered followers and no closing metadata — or with a
  torn sub-40-byte record — that dangling tail is truncated back to the last metadata
  boundary. The existing "broken in the *middle* ⇒ non-recoverable" rule is preserved.
- **Replay.** `recover_until` buffers a group's followers and applies them only when the
  trailing metadata arrives — `tx_id`, `timestamp`, `fail_reason`, and the watermark
  skip/apply decision all live in the trailer now. The previous `skipping_tx` flag
  (carried across records, trusting every follower to honor it) disappears: the
  skip/apply decision is made once, at the trailer, with the authoritative `tx_id` in
  hand. The buffer is a single reusable `Vec`, bounded by `sub_item_count ≤ u16::MAX`
  (the writer already rejects larger transactions with `ENTRY_LIMIT_EXCEEDED`).

### Segments are one logical stream

The WAL is a single logical byte stream, physically partitioned into sealed
`wal_NNNNNN.bin` files plus the active `wal.bin` — conceptually one file cut into pieces.

A segment cut **may fall inside a transaction** (some followers in one segment, the
trailing metadata in the next). This is fine and explicitly allowed: the transaction is
contiguous in the logical stream, so the commit-record invariant is a property of that
**logical** stream, not of any individual segment file. Rotation stays a pure
size/physical concern and is *not* forced onto transaction boundaries.

Two facts keep this safe:

- **Sealed segments are immutable and CRC-checked** (the `.crc` sidecar covers the whole
  segment image), so they cannot develop a torn tail.
- Therefore partial data can only ever appear at the **active segment's tail** — written
  by the run that crashed.

Crash recovery truncates the *active* segment to its last `TxMetadata` boundary, and
replay carries the follower buffer **across** segment seams so a transaction split over a
boundary reassembles correctly.

### Affected components

The "metadata precedes its followers" assumption is read in several places; all flip
together. `sub_item_count` stays authoritative and records stay 40 bytes — only the scan
direction and the moment a group is recognized change.

| Component | Change |
|---|---|
| `transactor.rs` | autocompute CRC/count; assemble + push metadata last; drop the double back-patch |
| `tx_ring/writer.rs` | remove `patch` (now unused); keep `walk` |
| `wal.rs`, `snapshot.rs` | recognize a transaction as complete at the trailing metadata |
| `recover.rs` | buffer-until-trailer scan; truncate the active tail to the last metadata; cross-seam buffer |
| `wal_tail.rs`, `index.rs`, `wal_zero_copy.rs` | the same group-recognition flip for tailing / indexing |
| `entities.rs` | document the trailer order and CRC rule (no layout change) |

Per-file implementation detail is tracked outside this ADR.

---

## Consequences

### Positive

- **Completeness is structural.** "Is the WAL complete?" becomes "does it end on a
  `TxMetadata`?" — no trust in an unverified leading count; simpler partial-tail
  detection and removal.
- **Single-push write path.** The metadata is assembled once with known
  `sub_item_count`/`crc32c` and pushed last; `TxRingWriter::patch` and the post-hoc
  `walk`-to-checksum are gone.
- **`TransactorState` autocomputes.** The CRC and follower count are folded as followers
  are pushed and reset when the metadata is materialized — no separate finalize pass that
  re-reads the transaction.
- **Simpler replay.** The `skipping_tx` flag and its "did a follower leak through?"
  failure mode disappear; the watermark decision is made once, at the trailer.

### Negative

- **Breaking on-disk format.** Existing metadata-first WAL files are incompatible and are
  *not* migrated (development phase) — consistent with ADR-011's stance on format breaks.
  The data directory must be recreated on upgrade.
- **Replay buffers a transaction.** Recovery holds one transaction's followers in memory
  until its trailer arrives — bounded by `sub_item_count ≤ u16::MAX` (~2.5 MiB worst
  case) — and the buffer must carry across segment seams.
- **CRC byte order changes.** The digest now covers followers-then-metadata rather than
  metadata-then-followers; writer and reader are updated together.

### Neutral

- Record layout and size are unchanged (still 40-byte PODs; `sub_item_count` stays on
  `TxMetadata`).
- Segment rotation arithmetic is unchanged; a transaction may now span a seal boundary,
  and segments remain a pure physical partition of one logical stream.
- A rejected transaction is still recorded as a lone trailing `TxMetadata`
  (`sub_item_count = 0`, `fail_reason` set) for deduplication and rejection tracking.

---

## References

- ADR-001 — Entries-Based Execution Model (record types and ordering; amended)
- ADR-006 — WAL, Snapshot, and Seal Durability (recovery and on-disk order; amended)
- ADR-019 — Transaction Ring (in-ring transaction assembly; the metadata back-fill step is removed)
- ADR-013 — Transaction-Count-Based Segments (rotation; segments as a physical partition)
- ADR-009 — Transaction Links, Deduplication, and Reversal (`emit_duplicate` now writes the link before the trailing metadata)
- `crates/ledger/src/transactor.rs` — `TransactorState`, finalize path
- `crates/ledger/src/tx_ring/writer.rs` — `patch` removal
- `crates/ledger/src/recover.rs` — `validate_wal_transactions`, `recover_until`
- `crates/ledger/src/wal.rs`, `crates/ledger/src/snapshot.rs` — group recognition
- `crates/storage/src/entities.rs` — `TxMetadata` / `WalEntryKind` docs
- `crates/storage/src/wal_tail.rs`, `index.rs`, `wal_zero_copy.rs` — grouping consumers
