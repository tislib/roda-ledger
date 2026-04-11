# ADR-003: Ledger API — Removal of Generics and Operation Model

**Status:** Accepted  
**Date:** 2026-03-28  
**Author:** Taleh Ibrahimli

---

## Context

The original `Ledger<Data, BalanceData>` design used Rust generics to allow user-defined transaction types. After the entries-based refactor (ADR-001), this design has several problems:

**1. Generics no longer serve a purpose**

After ADR-001, the Transactor executes business logic once and produces `Credit`/`Debit` entries. The WAL stores entries, not raw transactions. The Snapshotter applies entries. None of the pipeline stages need to know the user's transaction type after the Transactor. The generic parameter leaked into every stage unnecessarily.

**2. `Wallet` was a confusing abstraction**

`Wallet` was introduced as a concrete example wrapping `Ledger` with three operations: `deposit`, `withdraw`, `transfer`. After the API simplification, `Wallet` becomes redundant — the Ledger itself can expose these operations directly. Having two separate types for the same thing confused users and complicated the codebase.

**3. Custom `TransactionDataType` trait was the wrong extensibility model**

The trait required users to implement Rust code compiled into the binary. This is incompatible with the WASM sandbox direction (ADR-008) where custom logic is uploaded at runtime, not compiled in.

**4. No way to submit user-defined multi-step atomic operations**

Simple operations (deposit, withdrawal, transfer) cover common cases but financial systems frequently need multi-step atomic operations — fee deductions, multi-leg settlements, conditional transfers. These require user-defined steps with declarative constraints evaluated atomically.

---

## Decision

Remove all generics from `Ledger`. Replace `TransactionDataType` trait with a concrete `Operation` enum. Remove `Wallet` entirely. Add `Complex` variant for user-defined multi-step atomic operations.

### Operation enum

```rust
pub enum Operation {
    Transfer {
        from:     u64,
        to:       u64,
        amount:   u64,
        user_ref: u64,
    },
    Deposit {
        account:  u64,
        amount:   u64,
        user_ref: u64,
    },
    Withdrawal {
        account:  u64,
        amount:   u64,
        user_ref: u64,
    },
    Complex(Box<ComplexOperation>),
    // Custom(Vec<u8>) — reserved for WASM, ADR-008, not yet implemented
}
```

Simple operations are stack-allocated, zero heap allocation. `Complex` uses `Box<ComplexOperation>` to keep the enum small — pointer-sized regardless of complex operation size.

### ComplexOperation

```rust
pub struct ComplexOperation {
    pub steps:    SmallVec<[Step; 8]>,      // inline up to 8 steps, heap beyond
    pub flags:    ComplexOperationFlags,    // u64 bitfield, controls checks
    pub user_ref: u64,
}

pub enum Step {
    Credit { account_id: u64, amount: u64 },
    Debit  { account_id: u64, amount: u64 },
}

bitflags::bitflags! {
    pub struct ComplexOperationFlags: u64 {
        // Reject entire operation if any account balance goes negative after execution
        const CHECK_NEGATIVE_BALANCE = 0b00000001;
        // 1–63 reserved for future checks
    }
}
```

**Why flags instead of Vec<Check>:**

A `Vec<Check>` would require heap allocation per complex operation. Checks are a small, enumerable set — a `u64` bitfield covers all realistic checks with zero allocation. Single bitwise AND to evaluate any check — one instruction, branch predictor friendly.

**Why `SmallVec<[Step; 8]>` instead of `Vec<Step>`:**

`Vec<Step>` always allocates on the heap. `SmallVec<[Step; 8]>` stores up to 8 steps inline on the stack — zero heap allocation for the common case. 8 covers fee deduction (3-4 steps), multi-leg settlement (4-6 steps), and most real-world complex operations. A 9+ step atomic operation is rare and can afford a heap allocation.

**Allocation analysis:**

```
Operation::Transfer/Deposit/Withdrawal  → zero allocation, stack only
Operation::Complex with ≤8 steps       → 1 allocation (Box)
Operation::Complex with >8 steps       → 2 allocations (Box + SmallVec spill)
```

### Execution model

Complex operations execute atomically in the Transactor:

```
1. Apply all steps to balance cache (tentative)
2. Evaluate flags:
   CHECK_NEGATIVE_BALANCE → if any touched account balance < 0, rollback
3. Verify zero-sum invariant — always enforced regardless of flags
   sum(credits) == sum(debits), else FailReason::ZERO_SUM_VIOLATION
4. If all checks pass → commit entries to WAL
5. If any check fails → rollback all steps, emit TxMetadata with fail_reason
```

Atomicity is guaranteed — either all steps commit or none do. Partial execution is impossible.

### Operation vs Transaction distinction

`Operation` is what the client submits — the intent. `Transaction` is what the server creates internally — the immutable fact stored in the WAL.

```
Client submits  → Operation   (intent)
Server creates  → Transaction (fact, tx_id + TxMetadata + Vec<TxEntry>)
Client receives → tx_id       (reference to the fact)
```

This maps to ISO 20022 — `PaymentInstruction` (intent) vs settled `Transaction` (fact).

### Built-in operation entry mapping

```
Deposit { account, amount }:
  Debit(account, amount)
  Credit(SYSTEM_SOURCE, amount)
  Precondition: none — deposits always succeed

Withdrawal { account, amount }:
  Credit(account, amount)
  Debit(SYSTEM_SINK, amount)
  Precondition: balance(account) >= amount → FailReason::INSUFFICIENT_FUNDS

Transfer { from, to, amount }:
  Credit(from, amount)
  Debit(to, amount)
  Precondition: balance(from) >= amount → FailReason::INSUFFICIENT_FUNDS
  Precondition: from != to → no-op if equal
```

### Ledger public API

```rust
pub struct Ledger { /* no generics */ }

impl Ledger {
    pub fn new(config: LedgerConfig) -> Self
    pub fn start(&mut self)
    pub fn submit(&self, operation: Operation) -> u64
    pub fn get_balance(&self, account_id: u64) -> Balance
    pub fn get_transaction_status(&self, tx_id: u64) -> TransactionStatus
    pub fn wait_for_transaction(&mut self, tx_id: u64)
    pub fn last_compute_id(&self) -> u64
    pub fn last_commit_id(&self) -> u64
    pub fn last_snapshot_id(&self) -> u64
}
```

### Example usage

```rust
// simple operations
ledger.submit(Operation::Deposit    { account: 1, amount: 1000, user_ref: 0 });
ledger.submit(Operation::Withdrawal { account: 1, amount: 500,  user_ref: 0 });
ledger.submit(Operation::Transfer   { from: 1, to: 2, amount: 200, user_ref: 0 });

// complex — fee deduction: user pays amount + fee, merchant gets amount, fee_account gets fee
ledger.submit(Operation::Complex(Box::new(ComplexOperation {
    steps: smallvec![
        Step::Credit { account_id: user,        amount: amount + fee },
        Step::Debit  { account_id: merchant,    amount: amount },
        Step::Debit  { account_id: fee_account, amount: fee },
    ],
    flags: ComplexOperationFlags::CHECK_NEGATIVE_BALANCE,
    user_ref: order_id,
})));
```

---

## Consequences

### Positive

- Public API is simple — no generics, no trait implementations required
- `Operation` enum is self-documenting — exhaustive, immediately understandable
- `Wallet` removed — one less type, no confusion
- `Complex` enables multi-step atomic operations without WASM
- Flags as `u64` bitfield — zero allocation, single instruction evaluation
- `SmallVec<[Step; 8]>` — zero allocation for common case (≤8 steps)
- WASM extensibility path preserved — `Custom` variant reserved, no breaking change needed
- ISO 20022 alignment — Operation (intent) vs Transaction (fact)

### Negative

- Breaking API change — existing `Ledger<Data, BalanceData>` code must be rewritten
- `TransactionDataType` trait removed — users with custom logic must wait for WASM (ADR-008)
- `Complex` with >8 steps causes SmallVec heap spill — acceptable, rare in practice
- `Box<ComplexOperation>` — one heap allocation per complex operation

### Neutral

- Internal pipeline unchanged — Transactor produces `Vec<TxEntry>`, WAL stores entries
- Balance type remains `i64` — supports negative balances for liability accounts

---

## Alternatives considered

**Keep generics, add Operation enum alongside**
Rejected — two parallel extensibility models. Generic model incompatible with WASM direction.

**Keep Wallet as convenience wrapper**
Rejected — adds no value after simplification. `Ledger` with `Operation` is already as simple.

**Vec<Check> instead of flags**
Rejected — heap allocation per complex operation. Checks are a small enumerable set — u64 bitfield covers all realistic checks with zero allocation.

**SmallVec<[Step; 16]> instead of 8**
Rejected — would make `ComplexOperation` much larger. 8 covers all common financial operations. A 9+ step transaction is rare and can afford heap spill.

**No Box — put ComplexOperation inline in enum**
Rejected — would make every `Operation` the size of `ComplexOperation`. Simple operations (95% of traffic) would pay the memory cost of complex operations.

## References

- ADR-001 — entries-based execution model
- ADR-002 — Vec-based balance storage
- ADR-008 — WASM sandbox (planned)
- ISO 20022 — PaymentInstruction vs Transaction terminology
- TigerBeetle — single Transfer primitive, no custom logic
- smallvec crate — stack-allocated Vec with heap fallback
- bitflags crate — type-safe bitfield flags