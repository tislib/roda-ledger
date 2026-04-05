# ADR-008: Transaction Index and Query Serving

**Status:** Accepted
**Date:** 2026-04-04  
**Author:** Taleh Ibrahimli  

---

## Context

roda-ledger currently has no way to retrieve a specific transaction by ID or retrieve
the transaction history for a specific account. Both are fundamental operations for a
production financial ledger:

- **`GetTransaction(tx_id)`** — retrieve the full details of a specific transaction.
  Required for payment status checks, reconciliation, and audit.

- **`GetAccountHistory(account_id, from_tx_id, limit)`** — retrieve transactions
  involving a specific account, newest first, with pagination. Required for account
  statements, balance verification, and dispute resolution.

**`GetBalance`** is already served correctly via `Vec<AtomicI64>` indexed directly
by `account_id` — a direct atomic read from any gRPC thread. No changes needed.

### Query frequency ratio

```
SubmitTransaction  :  GetTransaction  :  GetAccountHistory  :  GetBalance
      100          :       10         :          1          :     50
```

`GetAccountHistory` is the rarest operation. The index design reflects this — hot
path is optimized for `GetTransaction`, account history is best-effort in-memory
with disk fallback.

---

## Decision

### Two-tier query architecture

```
Hot tier  (active segment):   Snapshot stage owns TransactionIndexer
                              queries routed via SnapshotMessage queue
                              no shared mutable state between threads

Cold tier (sealed segments):  immutable index files on disk
                              gRPC reads directly, concurrently safe
```

### Single queue for WAL entries and queries

Both `WalEntry` records and `QueryRequest` messages flow through a single
`ArrayQueue<SnapshotMessage>`:

```rust
pub enum SnapshotMessage {
  Entry(WalEntry),
  Query(QueryRequest),
}
```

A single FIFO queue gives **read-your-own-writes consistency for free**. A query
submitted after a transaction is enqueued after its `WalEntry`. The Snapshot stage
processes them in order — the query always sees the correct committed state. Two
separate queues would lose this ordering guarantee.

### Snapshot stage owns TransactionIndexer exclusively

`TransactionIndexer` is owned by the Snapshot stage thread. No other thread
accesses it. This eliminates all thread safety concerns — no locks, no atomics,
no unsafe. gRPC threads never touch the indexer directly; they send queries via
`SnapshotMessage::Query` and receive responses via a per-request callback channel.

---

## In-Memory Index: TransactionIndexer

Three pre-allocated, fixed-size structures. Zero heap allocation after construction.
No cold start on segment rotation — circular buffers self-evict, never reset.

```rust
pub struct TransactionIndexer {
  circle1:            Vec<TxSlot>,          // tx_id → location in circle2
  circle2:            Vec<IndexedTxEntry>,  // entry storage + account prev_link chain
  account_heads:      Vec<(u64, u32)>,      // account_id → latest circle2 index
  circle1_mask:       usize,                // circle1_size - 1, for bitmask
  circle2_mask:       usize,                // circle2_size - 1
  account_heads_mask: usize,                // account_heads_size - 1
  write_head2:        usize,                // monotonically increasing circle2 position
}
```

### Circle 1 — transaction index (TxSlot, 16 bytes)

Maps `tx_id` to its starting position in circle2. Direct-mapped cache —
`tx_id & mask` gives the slot index. Self-evicting: a newer transaction with
the same masked tx_id overwrites the slot. `slot.tx_id != queried_tx_id` = evicted.

```rust
pub struct TxSlot {
  pub tx_id:       u64,      // 0 = empty slot; mismatch = evicted
  pub offset:      u32,      // start index in circle2 for this transaction's entries
  pub entry_count: u8,       // number of IndexedTxEntry records for this transaction
  pub _pad:        [u8; 3],
}                              // 16 bytes
```

### Circle 2 — entry storage with account link chain (IndexedTxEntry, 48 bytes)

Stores `TxEntry` records sequentially. Each entry carries a `prev_link` field —
the **same account previous transaction link in circle2**. This forms a per-account
backward linked list through the buffer without any separate data structure.

```rust
pub struct IndexedTxEntry {
  pub entry_type:       u8,        // WalEntryKind::TxEntry
  pub kind:             EntryKind, // Credit or Debit
  pub _pad:             [u8; 6],
  pub tx_id:            u64,
  pub account_id:       u64,
  pub amount:           u64,
  pub computed_balance: i64,       // balance after this entry (set by Transactor)
  pub prev_link:        u32,       // same account previous transaction link in circle2
  // 1-based: 0 = no previous entry in buffer for this account
  //          N = entry is at circle2 slot N-1
  pub _pad_end:         [u8; 4],
}                                    // 48 bytes
```

`prev_link` is 1-based to distinguish "no link" (0) from "slot 0" (1). When a
slot is overwritten, any `prev_link` pointing to it encounters an `account_id`
mismatch — the chain breaks naturally. No cleanup needed.

### Account heads — direct-mapped

```rust
account_heads: Vec<(u64, u32)>
// index:   account_id & mask
// value:   (account_id, circle2_slot_index)
// purpose: entry point into prev_link chain for each account
```

Same direct-mapped collision handling as circle1. If `account_heads[slot].0 !=
account_id` — head overwritten by another account — treat as cache miss, fall
back to cold tier.

### Write path (per WalEntry in Snapshot stage)

```
On TxMetadata(m):
  circle1[m.tx_id & mask] = TxSlot {
      tx_id:       m.tx_id,
      offset:      write_head2 & circle2_mask,
      entry_count: m.entry_count,
  }

On TxEntry(e):
  head_slot = e.account_id & account_heads_mask
  prev_link = if account_heads[head_slot].0 == e.account_id {
      account_heads[head_slot].1 + 1   // 1-based: existing head slot → link
  } else {
      0                                 // no previous entry for this account
  }

  circle2[write_head2 & circle2_mask] = IndexedTxEntry {
      tx_id, account_id, amount, kind, computed_balance,
      prev_link,
      ...
  }

  account_heads[head_slot] = (e.account_id, write_head2 & circle2_mask)
  write_head2 += 1
```

### GetTransaction (hot)

```
slot = circle1[tx_id & mask]
if slot.tx_id != tx_id → miss → cold tier

for i in 0..slot.entry_count:
    entry = circle2[(slot.offset + i) & mask]
    if entry.tx_id != tx_id → evicted → miss → cold tier
    collect entry

return entries
```

### GetAccountHistory (hot)

```
head_slot = account_id & account_heads_mask
if account_heads[head_slot].0 != account_id → miss → cold tier

current = account_heads[head_slot].1

loop:
    entry = circle2[current & circle2_mask]
    if entry.account_id != account_id → evicted, stop
    if from_tx_id != 0 && entry.tx_id < from_tx_id → stop
    collect entry
    if collected == limit → stop
    if entry.prev_link == 0 → stop
    current = entry.prev_link - 1   // 1-based → 0-based slot

return collected (newest first)
```

### Sizing

Both circle sizes must be power-of-2 for bitmask indexing. Configured via
`LedgerConfig`:

```rust
pub struct LedgerConfig {
  pub circle1_size:       usize,   // default: 0 (auto — fit active segment at startup)
  pub circle2_size:       usize,   // default: 0 (auto — circle1_size × avg_entries)
  pub account_heads_size: usize,   // default: 1_048_576 (1M slots)
}
```

Auto-sizing: count records in active `wal.bin` at startup, round up to next power
of two. Guarantees active segment fits in cache on startup.

**No cold start on segment rotation.** Segment sealing does not reset the circles.
Buffers continue filling with new entries. Old entries from the previous segment
naturally expire as slots are overwritten. Latency is stable across rotation.

**Memory at default config (64MB segment, ~1.6M entries):**

```
circle1:       2M slots × 16 bytes = 32MB
circle2:       2M slots × 48 bytes = 96MB   (circle2 ≥ circle1 × avg_entries_per_tx)
account_heads: 1M slots × 12 bytes = 12MB
Total:         ~140MB fixed, pre-allocated, never grows
```

---

## Query Serving

### SnapshotMessage and QueryRequest

```rust
pub enum SnapshotMessage {
  Entry(WalEntry),
  Query(QueryRequest),
}

pub struct QueryRequest {
  pub kind:    QueryKind,
  pub respond: FnOnce(QueryResponse),
}

pub enum QueryKind {
  GetTransaction    { tx_id: u64 },
  GetAccountHistory { account_id: u64, from_tx_id: u64, limit: usize },
}

pub enum QueryResponse {
  Transaction(Option<TransactionRecord>),
  AccountHistory(Vec<TransactionRecord>),
}
```

### Per-request response channel

Each `QueryRequest` carries its own `mpsc::SyncSender<QueryResponse>`. The Snapshot
stage calls `respond.send(result)` directly — no shared response queue, no routing
ambiguity when multiple gRPC threads query simultaneously. Each caller blocks on its
own private `rx.recv()`.

```rust
// gRPC handler:
let (tx, rx) = std::sync::mpsc::sync_channel(1);  // oneshot semantics
snapshot_queue.push(SnapshotMessage::Query(QueryRequest {
kind:    QueryKind::GetTransaction { tx_id },
respond: |entry| {tx.send(entry).unwrap();},
}));
let response = rx.recv().unwrap();  // blocks on private channel only
```

No tokio dependency. Pure std. One sender, one receiver, capacity 1 = oneshot.

### Snapshot stage loop

```
loop:
  match inbound.pop():
    Some(SnapshotMessage::Entry(e)):
      apply entry to balances Vec<AtomicI64>     (existing)
      indexer.insert_tx or insert_entry           (new)
      pressure.on_work()

    Some(SnapshotMessage::Query(q)):
      result = match q.kind:
        GetTransaction(tx_id):
          indexer.get_transaction(tx_id)
          → Some → return TransactionRecord
          → None → cold tier lookup
        GetAccountHistory(account_id, from, limit):
          indexer.get_account_history(account_id, from, limit)
          → non-empty → return results
          → empty → cold tier lookup
      q.respond.send(result)
      pressure.on_work()

    None:
      pressure.on_retry_receive(retry, last_tx_id)
      retry++
```

---

## Cold Tier — Sealed Segments

gRPC handler reads sealed files directly when Snapshot stage returns a miss.
Sealed files are immutable — concurrent reads from multiple gRPC threads are safe
with no coordination.

### Sealed segment indexes loaded at startup

```rust
pub struct SealedSegmentIndex {
  pub segment_id:  u32,
  pub first_tx_id: u64,
  pub last_tx_id:  u64,
  pub wal_offsets: Vec<(u64, u64)>,   // (tx_id, byte_offset), sorted by tx_id
  pub account_txs: Vec<(u64, u64)>,   // (account_id, tx_id), sorted by (account_id, tx_id)
}

// atomic swap when new segment sealed:
sealed_indexes: ArcSwap<Vec<SealedSegmentIndex>>
```

Loaded once at startup, never mutated. `ArcSwap` updated when Seal thread seals a
new segment and writes index files. gRPC threads holding old Arc finish safely.

### GetTransaction (cold)

```
find segment: binary search sealed_indexes for tx_id in [first_tx_id..last_tx_id]
binary search wal_offsets for tx_id → byte_offset
seek to byte_offset in wal_NNNN.bin
read TxMetadata + TxEntry records
return TransactionRecord
```

### GetAccountHistory (cold)

```
for each sealed segment, newest to oldest:
    binary search account_txs for account_id range
    collect tx_ids >= from_tx_id, up to limit
    for each tx_id → GetTransaction cold
    if limit reached → stop
return merged results
```

---

## On-Disk Index Files (built by Seal thread)

Built as side effect of existing WAL replay loop in Seal thread. Two flat
`Vec<(u64, u64)>` appended during replay, one sort at seal time.

```
wal_index_000004.bin:
  [record_count: 8 bytes LE]
  [tx_id: 8][offset: 8] × record_count   ← naturally sorted (WAL append-only)
wal_index_000004.crc

account_index_000004.bin:
  [record_count: 8 bytes LE]
  [account_id: 8][tx_id: 8] × record_count  ← sort_unstable at seal time
account_index_000004.crc
```

**Memory during seal:**

```rust
wal_entries:     Vec<(u64, u64)>   // (tx_id, offset)      — one per TxMetadata
account_entries: Vec<(u64, u64)>   // (account_id, tx_id)  — one per TxEntry
```

One flat allocation each. No per-account allocation. ~51MB total during seal, freed
after write.

---

## Storage Layout (updated from ADR-006)

```
data/
  wal_000004.bin
  wal_000004.crc
  wal_000004.seal
  wal_index_000004.bin
  wal_index_000004.crc
  account_index_000004.bin
  account_index_000004.crc
  snapshot_000004.bin
  snapshot_000004.crc
  wal.bin
```

---

## GetBalance — unchanged

Direct `Vec<AtomicI64>` read with `Ordering::Acquire` from any gRPC thread. O(1),
lock-free, no queue involvement. Routing through `SnapshotMessage` would add
enormous overhead for a single atomic load instruction.

---

## gRPC API (ADR-004 extension)

```protobuf
rpc GetTransaction(GetTransactionRequest) returns (GetTransactionResponse);
        rpc GetAccountHistory(GetAccountHistoryRequest) returns (GetAccountHistoryResponse);

message GetTransactionRequest  { uint64 tx_id = 1; }

message GetTransactionResponse {
  uint64               tx_id       = 1;
  uint64               user_ref    = 2;
  uint64               timestamp   = 3;
  uint32               fail_reason = 4;
  repeated TxEntryRecord entries   = 5;
}

message TxEntryRecord {
  uint64    account_id       = 1;
  uint64    amount           = 2;
  EntryKind kind             = 3;
  int64     computed_balance = 4;
}

enum EntryKind { CREDIT = 0; DEBIT = 1; }

message GetAccountHistoryRequest {
  uint64 account_id = 1;
  uint64 from_tx_id = 2;  // 0 = latest
  uint32 limit      = 3;  // default 20, max 1000
}

message GetAccountHistoryResponse {
  repeated TxEntryRecord entries    = 1;
  uint64                 next_tx_id = 2;  // 0 = no more entries
}
```

---

## Consequences

### Positive

- Single queue gives read-your-own-writes consistency for free — no special handling
- Snapshot stage owns TransactionIndexer exclusively — zero thread safety complexity
- No locks, no atomics, no unsafe anywhere in the index
- circle1 + circle2 + account_heads are pre-allocated, fixed size, never grow
- Self-evicting circular buffers — no cold start on segment rotation
- prev_link chain gives O(account_transactions_in_buffer) account history — no scan
- Per-request `mpsc::sync_channel(1)` — oneshot semantics, no tokio, pure std
- Cold tier reads immutable files — concurrent, no coordination
- `ArcSwap` for sealed indexes — atomic update, readers never blocked
- On-disk indexes built as side effect of existing Seal thread WAL replay

### Negative

- Hot query adds queue round-trip latency vs direct memory access
  — acceptable for a cold query path (10:1 ratio vs submit)
- gRPC caller thread blocks on `rx.recv()` — wrap in `spawn_blocking` for async handlers
- account_heads collisions cause cache misses for affected accounts — disk fallback
- circle1 collisions evict older transactions — expected, graceful disk fallback
- account_heads at 1M slots: higher collision rate at 50M accounts — tunable
- Deep account history across many sealed segments requires multiple disk seeks
  — application layer should cache account history

### Neutral

- `SnapshotMessage` enum wraps existing `WalEntry` — minimal change to Snapshot stage
- circle sizes power-of-2 enforced at startup via `next_power_of_two()`
- GetBalance unchanged — Vec<AtomicI64> direct read, no queue involvement
- Application layer responsible for caching account history for user-facing queries

---

## Alternatives Considered

**WalBuffer (append-only byte buffer)**
Evaluated extensively. Rejected — thread safety between Snapshot stage writer and
gRPC reader threads required unsafe code, atomics, or locks. The query message
passing approach eliminates the problem entirely: Snapshot stage owns the buffer,
gRPC never touches it directly.

**Two separate queues (WalEntry queue + Query queue)**
Rejected — loses ordering between WAL entries and queries. A query submitted after
a transaction could be processed before the transaction's WalEntry, returning a
stale miss. Single queue preserves FIFO ordering and gives read-your-own-writes.

**Linear scan of active segment for GetTransaction**
Rejected — O(segment_size) for a query that occurs 10:1 vs submit. circle1 direct-
mapped cache gives O(1) for the common case.

**Linear scan for GetAccountHistory**
Acceptable for active segment but replaced by prev_link chain — O(account_txs)
traversal is strictly better than O(all_entries) scan.

**Tokio oneshot channel for query response**
Rejected — introduces tokio dependency into the core ledger. `mpsc::sync_channel(1)`
provides identical oneshot semantics with pure std.

**Global account index in memory**
Rejected — grows unboundedly, expensive to rebuild on every seal. Per-segment
account_index files loaded into `SealedSegmentIndex` at startup provide the same
capability with bounded memory.

---

## References

- ADR-001 — TxMetadata, TxEntry, WalEntry, computed_balance
- ADR-002 — Vec<AtomicI64> balance cache
- ADR-004 — gRPC interface, existing RPCs
- ADR-006 — WAL segment lifecycle, Seal thread, storage layout
- ADR-007 — CLI tools (verify extended to cover index files)
- `src/snapshot.rs` — Snapshot stage (extended with TransactionIndexer + query handling)
- `src/transaction_indexer.rs` — TransactionIndexer implementation (already written)
- `src/storage/segment.rs` — Seal thread WAL replay (extended for index file build)
- `std::sync::mpsc::sync_channel` — per-request oneshot response channel
- `arc-swap` crate — ArcSwap for sealed index atomic update (already in Cargo.toml)