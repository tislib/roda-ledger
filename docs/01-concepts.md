# Concepts

This document establishes the mental model and vocabulary that all other roda-ledger documentation builds on. Read it before anything else. No implementation details are discussed here — only what the system is, what it guarantees, and what it does not do.

---

## What is a Ledger

A ledger is an append-only record of financial facts. In roda-ledger, **transactions are the source of truth**. Balances are derived state — they are computed by applying all transactions that have touched an account, in order. A balance is never stored as a primary fact; it is always the result of replaying history.

This has a concrete implication: if you lose your balances but keep your transactions, you lose nothing. If you lose your transactions, no balance snapshot can recover them. The log is the ledger.

---

## Accounts and Balances

An **account** is an identifier — a `u64` that represents a participant in the ledger. Accounts are not created explicitly; they come into existence the first time a transaction references them.

A **balance** is the current state of an account, represented as an `i64`. Balances can be positive or negative. By default, roda-ledger protects accounts from going below zero — a transaction that would produce a negative balance is rejected. This protection can be intentionally bypassed in composite operations and, in the future, via WASM-defined logic (for example, to model overdraft accounts or internal system accounts).

**Account 0** is the system account. It serves as the source and sink for all money entering or leaving the ledger. Deposits credit a user account and debit account 0. Withdrawals debit a user account and credit account 0. This is not a convention — it is required by the zero-sum invariant described below.

---

## Operations and Transactions

These two terms are distinct and the distinction matters.

An **operation** is the client's intent. roda-ledger provides four operation types:

- **Deposit** — credits a user account, debiting account 0 as the source
- **Withdrawal** — debits a user account, crediting account 0 as the sink
- **Transfer** — moves an amount from one account to another
- **Composite** — a caller-defined sequence of `Credit` and `Debit` steps, executed atomically. This is the escape hatch for logic that does not fit the named operation types.

Every operation carries a `user_ref` — a caller-supplied value that serves two purposes: an idempotency key to prevent duplicate processing, and a reference stored alongside the transaction for correlation or auditing. Idempotency is described in detail below.

The client submits operations. It never writes transactions directly.

A **transaction** is the ledger's immutable record of what happened. When an operation is accepted and executed, roda-ledger creates a transaction — a permanent, ordered fact in the log. Transactions are never modified, never deleted, and never reordered. The caller can query a transaction directly to check its status or inspect its details.

When querying a transaction, the caller sees **entries** — the individual credits and debits the Transactor produced from the original operation — not the operation itself. A `Transfer` becomes two entries: a debit on the sender and a credit on the receiver. A `Composite` becomes one entry per step. The operation is the intent; the entries are the facts.

The client thinks in operations. The ledger thinks in transactions. The Transactor translates between the two.

---

## Atomicity

A transaction is the atomicity boundary for failure. All steps within a single transaction either all succeed or all fail — if any condition fails at any point during execution (a balance check, a custom rule, or the zero-sum invariant), the entire transaction is rolled back, no balance is updated, and the caller is notified. The **Transactor** — the single deterministic writer at the heart of roda-ledger — enforces this.

**No torn writes.** Each individual internal credit or debit is a single atomic store. A concurrent reader will never observe a partially-written balance for any single account. Every balance a reader sees is always a complete, valid value.

**Intermediate state is visible across accounts.** In a Composite operation, the Transactor applies each step in sequence. A concurrent reader can observe some of these stores before others have landed — for example, seeing a Credit applied to one account before the corresponding Debit on another has landed. This is by design. The `last_tx_id` returned by `get_balance` is the signal that a transaction is fully settled: it is only updated after all steps in the transaction are complete. If a caller needs to know that a specific transaction is fully reflected across all accounts, they should wait for `last_tx_id` to advance past their transaction ID, or use `submit_wait(snapshot)` for a linearizable read.

---

## The Zero-Sum Invariant

Every transaction must net to zero. The sum of all credits in a transaction must equal the sum of all debits. roda-ledger enforces this invariant on every transaction — a transaction that violates it is rejected before any balance is updated.

Because every individual transaction nets to zero, the sum of all balances across all accounts in the ledger is always zero. This is not a configuration option — it is an emergent property of the per-transaction invariant. Money is never created or destroyed inside the ledger. It only moves.

This is why account 0 exists. When money enters the system (a deposit), it must come from somewhere — account 0 is debited. When money leaves the system (a withdrawal), it must go somewhere — account 0 is credited. The ledger always balances.

---

## Execution Model

The Transactor is a single, deterministic writer. There is no concurrency within transaction execution — one transaction at a time, in strict order. This is not a limitation. It is the source of correctness.

Because only one transaction executes at a time, there are no races, no conflicts, and no need for locks or conflict resolution. The Transactor maintains a hot in-memory cache of all account balances. Every write path — every operation submitted to the ledger — executes against this cache, which always reflects the latest committed state. The write path is always linearizable: no transaction ever makes a decision based on stale data.

The throughput of roda-ledger does not come from parallelizing execution. It comes from the pipeline that surrounds the Transactor, which allows sequencing, durability, and snapshotting to proceed concurrently with each other, without blocking the Transactor.

---

## Staged Guarantees

A transaction does not go from submitted to done in a single step. It moves through a pipeline of stages, and each stage adds a specific guarantee. The caller chooses which stage to wait for.

**Stage 1 — Sequencer**
The transaction receives a unique, monotonic ID and a permanent position in the global order. From this point, its place in history is fixed.

**Stage 2 — Transactor**
The transaction is executed. Business logic runs, balances are updated in the Transactor's in-memory cache, and the result (committed or rejected) is known. After this stage, the transaction has happened — but only in memory.

**Stage 3 — WAL Storer**
The transaction is written to the Write-Ahead Log on disk. After this stage, the transaction survives a process crash or power loss. This is the durability boundary.

**Stage 4 — Snapshotter**
The updated balances are written to the snapshot, making them visible to readers via `get_balance`. After this stage, reads reflect this transaction and all transactions before it.

The gap between Stage 3 and Stage 4 is typically tens to hundreds of nanoseconds — the Snapshotter runs continuously as part of the pipeline and is not a slow checkpoint process.

The significant gap is between Stage 2 and Stage 3. WAL writes are batched dynamically and flushed to disk via `fdatasync`. The throughput and latency of this step are bounded by sequential disk write speed. The maximum batch buffer size is configurable.

---

## Consistency Model

roda-ledger offers two consistency levels, and the caller controls which one they get.

**Serializability — always, on the write path**
Every transaction is executed by the single Transactor against the latest in-memory state. Execution is strictly ordered. This gives serializability by default — the result of any set of concurrent submissions is equivalent to some serial execution.

**Linearizability — on the read path, opt-in**
By default, `get_balance` reads from the snapshot, which may be a few nanoseconds behind the latest committed transaction. The response includes `last_tx_id` — the ID of the most recent transaction reflected in the returned balance. The caller can use this to reason about freshness without blocking.

If the caller needs a linearizable read — a guarantee that the balance reflects all transactions up to and including a specific one — they use `submit_wait(snapshot)`. This blocks until the Snapshotter has processed the transaction, after which `get_balance` is guaranteed to reflect it and everything before it.

To summarize:

| Path | Consistency | How |
|---|---|---|
| Write | Always linearizable | Single Transactor, in-memory latest state |
| Read (default) | Eventually consistent | `get_balance` returns balance + `last_tx_id` |
| Read (explicit wait) | Linearizable | `submit_wait(snapshot)` then `get_balance` |

---

## Idempotency and the Active Window

Duplicate protection is always on — it cannot be disabled. When `user_ref > 0`, the ledger guarantees that submitting the same `user_ref` twice within the **active window** produces a single committed transaction. The second submission is recorded as a duplicate, linked to the original, and rejected with `DUPLICATE` — the caller always recovers the original `tx_id`.

The **active window** is the range of the last N transactions, where N equals the configured segment transaction count (`transaction_count_per_segment`). This is the same boundary that controls WAL segment rotation. Because the deduplication cache uses a flip-flop mechanism (active + previous), the effective coverage is N to 2N transactions — at least one full segment worth, at most two.

The window is defined by transaction count, not wall-clock time. This makes idempotency guarantees deterministic regardless of throughput or load spikes — the same number of transactions always provides the same protection, whether they arrive in one second or one hour.

`user_ref = 0` opts out of the idempotency check for that individual transaction. This is the mechanism, not a global toggle — there is no configuration to disable deduplication system-wide.

---

## Flexibility

roda-ledger is designed around two dimensions of flexibility that set it apart from opinionated ledger systems.

**Composite operations.** Beyond the built-in `Deposit`, `Withdrawal`, and `Transfer` operation types, the caller can express arbitrary multi-account logic through `Composite` operations — a caller-defined sequence of `Credit` and `Debit` steps executed atomically as a single transaction. This covers any financial logic that does not fit the named types: split payments, fee deductions, multi-leg settlements, or domain-specific balance rules. The ledger engine handles ordering, durability, and atomicity — the caller defines the steps.

**WASM-sandboxed operations (near future).** The `Named` operation type is the extension point for pre-registered WASM modules. The caller uploads compiled logic, registers it under a name, and invokes it by name with parameters. The WASM module executes within a sandboxed API surface — it can credit and debit accounts but cannot escape the ledger's invariants. This brings programmable logic to the ledger without recompiling or redeploying it.

**Choose your guarantee level.** The staged pipeline exposes a dial between performance and consistency. The caller chooses how much to wait per submission:

- `submit` — returns a transaction ID immediately, waits for nothing. Maximum throughput.
- `submit_wait(transactor)` — waits for execution. The caller knows whether the transaction committed or was rejected.
- `submit_wait(wal)` — waits for durability. The transaction is on disk and survives a crash.
- `submit_wait(snapshot)` — waits for the balance to be visible. Guarantees linearizable reads via `get_balance`.

The caller makes this choice per submission based on what their use case requires.

---

## What Roda-Ledger Is Not

Understanding the boundaries is as important as understanding the capabilities.

**Not a general-purpose database.** There is no query language, no secondary indexes, no ad-hoc reads beyond balance lookup by account ID or transaction ID.

**Not an authorization layer.** roda-ledger does not authenticate callers or enforce access control. This is the responsibility of the layer above it.

**Not a distributed system today.** roda-ledger is a single-node engine. There is no replication, no leader election, no multi-node coordination. This is a current limitation, not a design principle — Raft-based multi-node replication is planned.

**What it does have today.** A gRPC interface and a Docker image. roda-ledger is not only an embedded library — it can run as a standalone service.

**Planned additions.** Raft-based multi-node replication, mTLS authentication, and WASM-sandboxed custom logic.x