# ADR-001: Entries-based Execution Model

**Status:** Accepted  
**Date:** 2026-03-25  
**Author:** Taleh Ibrahimli

---

## Context

Roda-ledger currently uses a generic `Ledger<TransactionType, BalanceType>` model where user-defined transaction logic executes in two places:

- **Transactor** — executes business logic, produces balance changes
- **Snapshotter** — re-executes the same business logic to materialize balances

This double-execution model creates several compounding problems:

**1. Double execution problem**  
Business logic runs twice per transaction — once in the Transactor, once in the Snapshotter. Even though all operations are deterministic by design (including future WASM operations which enforce determinism architecturally via sandbox constraints), executing business logic twice is wasteful, tightly couples the Snapshotter to user code, and complicates the recovery path unnecessarily.

**2. WAL stores logical operations, not physical changes**  
The WAL currently stores raw `Transaction<Data>` structs — the user's operation, not its result. On crash recovery, the system must re-execute business logic to reconstruct balances. This couples the WAL format to the user's code and makes the WAL format non-generic — it cannot be read or replayed by any component that does not understand the user's transaction type.

**3. Snapshot stage re-executes business logic**  
The Snapshotter receives raw transactions and must execute them to derive balance changes. This means the Snapshotter must be aware of user-defined logic, preventing a clean separation between the execution layer and the persistence layer.

**4. WASM custom operations require single execution**  
Future WASM sandbox support requires that custom logic executes exactly once in a controlled environment and produces a set of entries. Re-execution in the Snapshotter is architecturally incompatible with this model.

---

## Decision

Replace the generic transaction execution model with an **entries-based execution model**.

Business logic executes exactly once — in the Transactor. The result of execution is a set of immutable `Credit` and `Debit` entries. Everything downstream — WAL Storer, Snapshotter — operates only on entries, never on business logic.

The Transactor produces WAL records directly. The WAL record format is the single shared structure used by all pipeline stages. There is no intermediate representation — no copying from one structure to another between stages.

### Core types

```rust
#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum WalEntryKind {
    TxMetadata = 0,
    TxEntry    = 1,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum EntryKind {
    Credit = 0,
    Debit  = 1,
}
```

`FailReason` is a newtype wrapper over `u8` to support user-defined custom codes in the 128–255 range while remaining `bytemuck` compatible:

```rust
pub struct FailReason(u8);
impl FailReason {
    pub const NONE:                  Self = Self(0);  // success
    pub const INSUFFICIENT_FUNDS:    Self = Self(1);
    pub const ACCOUNT_NOT_FOUND:     Self = Self(2);
    pub const ZERO_SUM_VIOLATION:    Self = Self(3);
    pub const ENTRY_LIMIT_EXCEEDED:  Self = Self(4);
    pub const INVALID_OPERATION:     Self = Self(5);
    // 6–127   reserved for future standard reasons
    // 128–255 user-defined custom reasons

    pub fn is_success(&self) -> bool { self.0 == 0 }
    pub fn is_failure(&self) -> bool { self.0 != 0 }
}
```

### WAL record structures

Both record types are exactly 32 bytes. The WAL scanner reads 1 discriminant byte, then always reads exactly 32 bytes of payload — no branching on size, no variable-length parsing.

```rust
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct TxMetadata {
    pub tx_id:       u64,        // 8
    pub timestamp:   u64,        // 8
    pub user_ref:    u64,        // 8 — opaque external reference, caller owns context
    pub entry_count: u8,         // 1 — 0 if transaction failed
    pub fail_reason: FailReason, // 1 — FailReason::NONE if succeeded
    pub _pad:        [u8; 6],    // 6 — total: 32 bytes
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct TxEntry {
    pub tx_id:      u64,       // 8 — links back to TxMetadata
    pub account_id: u64,       // 8
    pub amount:     u64,       // 8 — integer minor units, no floats, no decimals
    pub kind:       EntryKind, // 1 — Credit or Debit
    pub _pad:       [u8; 7],   // 7 — total: 32 bytes
}
```

### Transaction status

Transaction status is not stored. It is derived at query time from pipeline stage indexes:

```
tx_id <= transactor_index  →  Computed
tx_id <= wal_index         →  Committed
tx_id <= snapshot_index    →  Snapshoted
```

Transactions are immutable once written. Status is a view over pipeline progress, not a property of the record itself.

### Failed transactions

A failed transaction is written to WAL as a `TxMetadata` record with `fail_reason != 0` and `entry_count == 0`. No `TxEntry` records follow. This preserves the audit trail — every attempt is recorded — while consuming minimal space (32 bytes per failed attempt).

```
fail_reason == 0, entry_count > 0  →  succeeded, entries follow
fail_reason != 0, entry_count == 0 →  failed, no entries
```

### Execution flow

```
User Operation
    ↓ execute once (Transactor)
    ↓ produces WAL records directly
TxMetadata + TxEntry[]
    ↓
WAL Storer     — persists records, advances wal_index
Snapshotter    — applies entries to balance cache, advances snapshot_index
```

The WAL record format is the single shared structure across all stages. The Transactor produces it, the WAL Storer persists it, the Snapshotter consumes it. No intermediate copying between representations.

### External accounts

For operations where value enters or leaves the system (deposit, withdrawal), reserved system accounts absorb the imbalance to maintain the zero-sum invariant:

```
Deposit 100 to account A:
  Credit(SYSTEM, 100)
  Debit(A,              100)
  sum = 0 ✓

Withdrawal 50 from account A:
  Credit(A,           50)
  Debit(SYSTEM,  50)
  sum = 0 ✓
```

`SYSTEM` is reserved system accounts. Their balances are not tracked — they exist only as entry anchors. Every transaction in the system balances to zero.

### Zero-sum invariant

The ledger enforces automatically:

```
sum(credits) == sum(debits)
```

Any operation violating this is rejected with `FailReason::ZERO_SUM_VIOLATION` before reaching the WAL. This invariant ensures no value is created or destroyed inside the ledger — every unit that enters via `SYSTEM` must exit via `SYSTEM` or reside in a user account.

### Why manual discriminant, not Rust enum

A Rust enum wrapping both `TxMetadata` and `TxEntry` variants would be sized to the largest variant plus discriminant alignment padding — likely 40 or 48 bytes. Manual discriminant keeps both record types at exactly 32 bytes payload with no compiler padding tax, and makes the WAL format language-agnostic — any language can read it from the spec alone.

### Metadata and strings

The WAL stores no strings. All metadata is encoded as fixed-width integer types:

- `fail_reason` — `FailReason` newtype, 1 byte
- `user_ref` — opaque `u64`, the external system's reference key

Human-readable context (description, notes, reason text) is the external system's responsibility. They store it in their own database and join on `user_ref`. The ledger does not own this concern.

---

## Consequences

### Positive

- Business logic executes exactly once — clean separation between execution and persistence
- WAL format is generic and language-agnostic — readable by any component without knowing user code
- Snapshotter applies entries directly — no dependency on user-defined transaction types
- WASM sandbox becomes viable — custom logic runs once, produces entries, done
- Crash recovery replays entries — no dependency on code version at recovery time
- Immutable audit trail — every attempt recorded, entries append-only, never edited
- Zero-sum invariant enforced architecturally — no value created or destroyed inside the ledger
- Single shared structure across pipeline stages — no copying between representations

### Negative

- Breaking API change — existing `Ledger<TransactionType, BalanceType>` generics are removed
- `Wallet` example requires rewriting against the new entry-based API
- User migration required for anyone using custom `TransactionDataType` implementations
- Failed transactions occupy WAL space (32 bytes per `TxMetadata`) even with no entries — acceptable given audit trail value

---

## References

- PostgreSQL WAL internals — SQL executes once on primary, WAL contains physical page changes, replicas replay WAL
- TigerBeetle transfer model — explicit debit/credit primitives, no custom logic
- ISO 20022 — `EndToEndId` as opaque external reference key (maps to `user_ref`)
- Double-entry bookkeeping — every transaction balances: sum(debits) = sum(credits)