# ADR-009: Transaction Links, Deduplication, and Reversal

**Status:** Accepted (TxLink + Dedup implemented; Reversal deferred)
**Date:** 2026-04-04
**Author:** Taleh Ibrahimli

**Amends:**
- ADR-001 — adds `WalEntryKind::Link`, `TxLink` record, `link_count` to `TxMetadata`
- ADR-003 — adds `FailReason::DUPLICATE`, deduplication to Transactor

---

## Context

Three related concerns are addressed together because they share the same underlying
mechanism — `TxLink` — and are best understood as a coherent feature:

**1. Transactions have no relationships.**
A duplicate rejection carries `FailReason::DUPLICATE` but no reference to the
original committed transaction. A reversal is just a regular transaction with no
link to what it reverses. Audit trails are incomplete and callers must maintain
their own cross-reference tables outside the ledger.

**2. No duplicate protection.**
`user_ref` was designed as an opaque external reference but nothing enforces
uniqueness. A client that retries after a network failure or crash recovery will
create duplicate transactions — double charges, duplicate deposits. The crash
recovery resubmission pattern in ADR-008 explicitly requires retry after crash,
which without deduplication corrupts balances.

**3. No reversal mechanism.**
Reversing a transaction requires submitting a mirror `Complex` operation. The
relationship between the reversal and the original exists only in the application
layer, not in the ledger itself.

---

## Decision

### TxLink — first-class WAL record

Introduce `TxLink` as a new WAL record type. `TxLink` is a sub-record of a
transaction — it follows the `TxEntry` records for the same `TxMetadata` and is
counted in a new `link_count` field. The `tx_id` of the link is the enclosing
`TxMetadata.tx_id` — not stored in `TxLink` itself.

### Deduplication — flip-flop HashMap in Transactor

The Transactor maintains two `HashMap<u64, u64>` maps (user_ref → tx_id) that
flip on a configurable time window. A duplicate `user_ref` within the window
is rejected with `FailReason::DUPLICATE` and a `TxLink { kind: Duplicate }` is
written to the WAL pointing to the original committed transaction.

### Reversal Operation *(deferred — not yet implemented)*

A reversal will be submitted as `Operation::Reversal { to_tx_id, user_ref }`.
The ledger will locate the original transaction, create opposite entries for it
(debit <=> credit, credit <=> debit), and add a
`TxLink { kind: Reversal, to_tx_id: original_tx_id }`.

The `TxLinkKind::Reversal` variant is reserved in the WAL format (value = 1)
but the `Operation::Reversal` variant and its Transactor logic are not yet
implemented. This will be added in a future ADR or follow-up to this one.

---

## WAL Record Changes

### Updated TxMetadata (40 bytes, unchanged total size)

`link_count` is added at offset 2, shifting `fail_reason` and replacing `flags`.
Total size remains 40 bytes.

```rust
#[repr(C)]
pub struct TxMetadata {
    pub entry_type:  u8,           // 1 @ 0  — WalEntryKind::TxMetadata
    pub entry_count: u8,           // 1 @ 1  — number of TxEntry records following
    pub link_count:  u8,           // 1 @ 2  — number of TxLink records after entries
    pub fail_reason: FailReason,   // 1 @ 3
    pub crc32c:      u32,          // 4 @ 4
    pub tx_id:       u64,          // 8 @ 8
    pub timestamp:   u64,          // 8 @ 16
    pub user_ref:    u64,          // 8 @ 24
    pub tag:         [u8; 8],      // 8 @ 32
}                                  // 40 bytes total
```

### TxLink (40 bytes)

```rust
#[repr(C)]
pub struct TxLink {
    pub entry_type: u8,            // 1 @ 0  — WalEntryKind::Link (4)
    pub link_kind:  TxLinkKind,    // 1 @ 1
    pub _pad:       [u8; 6],       // 6 @ 2
    pub to_tx_id:   u64,           // 8 @ 8  — referenced transaction
    pub _pad2:      [u8; 24],      // 24 @ 16
}                                  // 40 bytes total

#[repr(u8)]
pub enum TxLinkKind {
    Duplicate = 0,   // this transaction is a duplicate of to_tx_id
    Reversal  = 1,   // this transaction reverses to_tx_id
}
```

### Updated WalEntry

```rust
pub enum WalEntry {
    Metadata(TxMetadata),          // kind = 0
    Entry(TxEntry),                // kind = 1
    SegmentHeader(SegmentHeader),  // kind = 2
    SegmentSealed(SegmentSealed),  // kind = 3
    Link(TxLink),                  // kind = 4 ← new
}
```

### WAL sequence

```
TxMetadata { tx_id, entry_count: N, link_count: M, ... }
TxEntry    × N
TxLink     × M
```

Reader advances through `entry_count` TxEntry records then `link_count` TxLink
records. Exact counts known from TxMetadata — no scanning needed.

### CRC coverage

CRC32C in `TxMetadata.crc32c` covers the complete logical transaction:

```
CRC input:
  TxMetadata bytes (crc32c field zeroed)
  + all TxEntry records (entry_count)
  + all TxLink records (link_count)
```

Computed by the Transactor before pushing any record to the WAL queue. Corrupt
or missing links detected at WAL replay time.

---

## Deduplication

### Flip-flop HashMap in Transactor

```rust
struct DedupCache {
    active:       HashMap<u64, u64>,   // user_ref → tx_id, current window
    previous:     HashMap<u64, u64>,   // user_ref → tx_id, previous window
    window_ms:    u64,                 // deduplication window in milliseconds
    window_start: u64,                 // timestamp when active window started
}
```

**Flip logic — checked on every transaction:**

```
now = current_time_ms()
if now - window_start >= window_ms:
    swap(active, previous)   // previous becomes active's old data
    active.clear()           // reuse allocation, zero re-alloc
    window_start = now
```

**Lookup:**

```
if user_ref == 0 → skip check (no idempotency key provided)

check active[user_ref]   → found → DUPLICATE
check previous[user_ref] → found → DUPLICATE
not found → proceed, insert into active after commit
found  → proceed, insert into active after commit (rejected transactions also kept in wal)
```

**Effective window: `window_ms` to `2 × window_ms`.**
An entry inserted at the start of a window lives until the end of the next window.
An entry inserted at the end lives for at least `window_ms`. A 10-second window
gives 10–20 seconds of deduplication coverage — sufficient for all retry scenarios.

**Memory:**

```
At 100K submissions per 10-second window:
  active:   100K entries × ~50 bytes = ~5MB
  previous: 100K entries × ~50 bytes = ~5MB
  Total:    ~10MB — bounded by submission rate, not pipeline throughput
```

`clear()` reuses the HashMap allocation. No GC pressure on flip.

**On crash recovery:**

Rebuild from WAL replay. For each committed `TxMetadata` with `user_ref != 0`
and `timestamp` within last `2 × window_ms`, insert into `active` or `previous`
based on timestamp. Deduplication window fully restored from WAL after recovery.

### New FailReason

```rust
pub const DUPLICATE: Self = Self(7);
// user_ref already committed within deduplication window
```

### LedgerConfig additions

```rust
pub struct LedgerConfig {
    // existing fields...
    pub dedup_enabled:    bool,   // default: true
    pub dedup_window_ms:  u64,    // default: 10_000 (10 seconds)
}
```

`dedup_enabled: false` disables the cache entirely — zero overhead, no check.
Useful for internal pipelines where user_ref is not used.

---

## Duplicate Transaction Flow

Transactor detects duplicate `user_ref` within the window:

```
TxMetadata {
    tx_id:       new_tx_id,
    entry_count: 0,              // no balance change
    link_count:  1,
    fail_reason: DUPLICATE,
    user_ref:    duplicate_user_ref,
}
TxLink {
    kind:      Duplicate,
    to_tx_id:  original_tx_id,
}
```

**What the caller sees:**

```
submit(op, user_ref=12345) → tx_id = 441100
GetTransactionStatus(441100) → ERROR, fail_reason=DUPLICATE
GetTransaction(441100) → link: Duplicate → to_tx_id=441001
GetTransaction(441001) → original committed transaction
```

The caller always recovers the original `tx_id` regardless of how many retries
were attempted. The duplicate record in the WAL provides a permanent audit trail
of the retry attempt.

---

## Reversal Flow *(deferred)*

> **Not yet implemented.** The `TxLinkKind::Reversal` value (1) is reserved in
> the WAL format so that future reversal records are forward-compatible with
> WAL files written today. The `Operation::Reversal` variant, Transactor
> reversal index, and gRPC `Reversal` message will be added in a follow-up.

---

## gRPC API (amends ADR-004)

`TxLink` records included in `GetTransactionResponse`:

```protobuf
message GetTransactionResponse {
  uint64               tx_id       = 1;
  uint64               user_ref    = 2;
  uint64               timestamp   = 3;
  uint32               fail_reason = 4;
  repeated TxEntryRecord entries   = 5;
  repeated TxLinkRecord  links     = 6;
}

message TxLinkRecord {
  uint64   to_tx_id = 1;
  LinkKind kind     = 2;
}

enum LinkKind {
  DUPLICATE = 0;
  REVERSAL  = 1;
}
```

---

## CLI Impact (amends ADR-007)

`roda-ctl unpack` emits `TxLink` records as JSON Lines:

```json
{"type":"TxMetadata","tx_id":441099,"entry_count":2,"link_count":1,...}
{"type":"TxEntry","tx_id":441099,"account_id":1,"amount":100,"kind":"Credit",...}
{"type":"TxEntry","tx_id":441099,"account_id":0,"amount":100,"kind":"Debit",...}
{"type":"TxLink","tx_id":441099,"kind":"Reversal","to_tx_id":441001}
```

`roda-ctl verify` validates:
- `link_count` in TxMetadata matches actual TxLink records read

---

## Consequences

### Positive

- Transaction relationships are first-class WAL records — auditable, covered by CRC
- Duplicate detection protects against network retry and crash recovery
  resubmission — exactly-once semantics achievable with `user_ref` + retry
- Caller always recovers original `tx_id` from a duplicate via `TxLink`
- Deduplication cache rebuilt from WAL on crash recovery — window survives crash
- Reversal audit trail is explicit in the WAL — no external cross-reference needed *(deferred)*
- Flip-flop HashMap — exact match, no false positives, bounded memory, O(1) check
- `link_count = 0` is the common case — zero overhead for normal transactions
- `TxLinkKind` 128–255 available for user-defined link kinds

### Negative

- `link_count` added to `TxMetadata` — breaking format change (alongside
  ADR-006 changes, one migration covers both)
- New `WalEntryKind::Link = 4` — all WAL readers and replay paths must handle it
- `Operation::Reversal` deferred — requires Transactor reversal index (future work)
- Deduplication cache lost on crash — rebuilt from WAL replay (startup cost)
- `dedup_window_ms` and `dedup_enabled` added to `LedgerConfig`

### Neutral

- `TxLink` is 40 bytes — consistent with all other WAL record types
- `user_ref = 0` skips deduplication check — no overhead for operations without key
- Deduplication memory bounded by submission rate, not pipeline throughput (~10MB)

---

## Alternatives Considered

**Store to_tx_id as a field in TxMetadata**
Rejected — only one link per transaction, not extensible. Separate TxLink records
allow multiple links and are consistent with the entry model.

**Circular buffer for deduplication cache**
Rejected — hash collisions cause false positives (legitimate transactions rejected).
Flip-flop HashMap gives exact match with no false positives at comparable memory cost.

**Enforce reversal entries match original**
Rejected — the system now handles reversal entries generation automatically,
ensuring they always correctly mirror the original transaction.

**Separate deduplication ADR**
Rejected — deduplication, TxLink, and reversal are coupled: deduplication emits
a TxLink on rejection, reversal is expressed as a TxLink. A single ADR covering
all three is more coherent than three separate ADRs with circular references.

**Links on simple operations**
Rejected — Deposit, Withdrawal, and Transfer are standalone by design. Links are
only meaningful on Complex operations where the caller controls the full
transaction structure.

---

## References

- ADR-001 — TxMetadata, TxEntry, WalEntry, CRC coverage (amended by this ADR)
- ADR-003 — Operation enum, ComplexOperation, FailReason (amended by this ADR)
- ADR-004 — gRPC interface (amended by this ADR)
- ADR-006 — WAL segment lifecycle, Seal thread, storage layout
- ADR-007 — CLI tools (amended by this ADR)
- ADR-008 — Transaction index, GetTransaction response
- `src/entities.rs` — TxLink, TxLinkKind, updated WalEntry, updated TxMetadata
- `src/dedup.rs` — DedupCache (flip-flop HashMap)
- `src/transactor.rs` — duplicate detection, DedupCache integration