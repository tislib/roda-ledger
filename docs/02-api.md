# API

This document covers the full API surface of roda-ledger. Every operation is shown in both interfaces — gRPC for standalone server usage and Rust library for embedded usage. The concepts are identical; only the syntax differs.

For a full understanding of what operations, transactions, and wait levels mean, read [Concepts](./01-concepts.md) first.

---

## Setup

### gRPC Server

Start the server with Docker:

```bash
docker run -p 50051:50051 -v $(pwd)/data:/data tislib/roda-ledger:latest
```

To customize the server, mount a TOML config file:

```bash
docker run -p 50051:50051 \
  -v $(pwd)/data:/data \
  -v $(pwd)/config.toml:/config.toml \
  tislib/roda-ledger:latest --config /config.toml
```

**`config.toml` — full reference:**

```toml
[server]
host                  = "0.0.0.0"
port                  = 50051
max_connections       = 1000
max_message_size_bytes = 4194304  # 4MB

[ledger]
max_accounts    = 1000000
wait_strategy   = "balanced"   # low_latency | balanced | low_cpu
dedup_enabled   = true
dedup_window_ms = 10000        # 10 seconds

[ledger.storage]
data_dir             = "/data"
wal_segment_size_mb  = 2048
snapshot_frequency   = 2       # take a snapshot every N sealed WAL segments
```

**`wait_strategy` options:**

| Mode | Behavior | Use when |
|---|---|---|
| `low_latency` | Spins indefinitely under backpressure | Latency is critical, CPU is dedicated |
| `balanced` | Spins briefly, then yields, then parks | Default — good for most workloads |
| `low_cpu` | Parks quickly under backpressure | CPU efficiency matters more than latency |

### Rust Library

Add to `Cargo.toml`:

```toml
[dependencies]
roda-ledger = { git = "https://github.com/tislib/roda-ledger" }
```

Initialize and start the ledger:

```rust
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::StorageConfig;

let config = LedgerConfig {
max_accounts: 1_000_000,
wait_strategy: WaitStrategy::Balanced,
dedup_enabled: true,
dedup_window_ms: 10_000,
storage: StorageConfig {
data_dir: "./data".to_string(),
wal_segment_size_mb: 2048,
snapshot_frequency: 2,
temporary: false,
},
..LedgerConfig::default()
};

let mut ledger = Ledger::new(config);
ledger.start()?;
```

The ledger recovers its state automatically on start — it finds the latest snapshot, loads it, and replays any WAL segments that followed. No manual recovery step is needed.

---

## Submitting Operations

### Fire and Forget

Submit an operation and get a `transaction_id` back immediately. The operation is sequenced but not yet executed.

**gRPC:**

```bash
# Deposit
grpcurl -plaintext -d '{
  "deposit": {"account": 1, "amount": "1000", "user_ref": "42"}
}' localhost:50051 roda.ledger.v1.Ledger/SubmitOperation

# Withdrawal
grpcurl -plaintext -d '{
  "withdrawal": {"account": 1, "amount": "500", "user_ref": "43"}
}' localhost:50051 roda.ledger.v1.Ledger/SubmitOperation

# Transfer
grpcurl -plaintext -d '{
  "transfer": {"from": 1, "to": 2, "amount": "300", "user_ref": "44"}
}' localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

Response:

```json
{"transactionId": "42"}
```

**Rust library:**

```rust
use roda_ledger::transaction::Operation;

let tx_id = ledger.submit(Operation::Deposit {
    account: 1,
    amount: 1000,
    user_ref: 42,
});

let tx_id = ledger.submit(Operation::Withdrawal {
    account: 1,
    amount: 500,
    user_ref: 43,
});

let tx_id = ledger.submit(Operation::Transfer {
    from: 1,
    to: 2,
    amount: 300,
    user_ref: 44,
});
```

### Operation Types

**Deposit** — credits a user account. Account 0 (the system account) is debited automatically to satisfy the zero-sum invariant. Deposits never fail due to insufficient funds.

**Withdrawal** — debits a user account. Fails with `INSUFFICIENT_FUNDS` if the account balance is below the requested amount. Account 0 is credited automatically.

**Transfer** — moves funds between two accounts atomically. Fails with `INSUFFICIENT_FUNDS` if the source account balance is insufficient. If `from == to`, the operation succeeds immediately as a no-op.

**Composite** — a caller-defined sequence of `Credit` and `Debit` steps executed as a single atomic transaction. The zero-sum invariant is always enforced. See [Composite Operations](#composite-operations) below.

### The `user_ref` Field

Every operation carries a `user_ref` — a `uint64` supplied by the caller. It serves two purposes:

- **Idempotency key** — when deduplication is enabled (default) and `user_ref > 0`, if the same `user_ref` appears within the `dedup_window_ms` window, the second submission is detected as a duplicate. It is sequenced and recorded, but linked to the original via a `TxLinkRecord { kind: DUPLICATE }` rather than re-executed. This prevents double-processing on client retries. Pass `user_ref = 0` to opt out of the idempotency check entirely.
- **Correlation reference** — stored in the WAL alongside the transaction. Use it to link to your own database record — an order ID, payment ID, or any external reference.

---

## Composite Operations

Composite operations let you express arbitrary multi-account logic as an atomic sequence of `Credit` and `Debit` steps.

**gRPC:**

```bash
# Fee deduction: debit sender, credit receiver and fee account
grpcurl -plaintext -d '{
  "composite": {
    "steps": [
      {"credit": {"account_id": "1", "amount": "1050"}},
      {"debit":  {"account_id": "2", "amount": "1000"}},
      {"debit":  {"account_id": "99", "amount": "50"}}
    ],
    "flags": 1,
    "user_ref": "45"
  }
}' localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

**Rust library:**

```rust
use roda_ledger::transaction::{Operation, CompositeOperation, Step};

let tx_id = ledger.submit(Operation::Composite(Box::new(CompositeOperation {
    steps: smallvec![
        Step::Credit { account_id: 1, amount: 1050 },
        Step::Debit  { account_id: 2, amount: 1000 },
        Step::Debit  { account_id: 99, amount: 50  },
    ],
    flags: CompositeOperationFlags::CHECK_NEGATIVE_BALANCE,
    user_ref: 45,
})));
```

**Flags:**

| Flag | Value | Effect |
|---|---|---|
| `CHECK_NEGATIVE_BALANCE` | `0x01` | Reject if any touched account would go negative. Without this flag, negative balances are permitted — useful for liability accounts or account 0. |

Maximum 255 steps per Composite operation. The zero-sum invariant is always enforced regardless of flags — sum of credits must equal sum of debits.

---

## Submit and Wait

Block until the operation reaches a specific pipeline stage. Returns the full result including whether the transaction was rejected and why.

**gRPC:**

```bash
grpcurl -plaintext -d '{
  "deposit": {"account": 1, "amount": "1000", "user_ref": "46"},
  "wait_level": "WAIT_LEVEL_SNAPSHOT"
}' localhost:50051 roda.ledger.v1.Ledger/SubmitAndWait
```

Response:

```json
{"transactionId": "43", "failReason": 0}
```

**Rust library:**

```rust
use roda_ledger::transaction::WaitLevel;

let result = ledger.submit_and_wait(
    Operation::Deposit { account: 1, amount: 1000, user_ref: 46 },
    WaitLevel::Snapshotted,
);

if result.fail_reason.is_none() {
    println!("committed: tx {}", result.tx_id);
}
```

**Wait levels:**

| Level       | gRPC | Library                  |
|-------------|---|--------------------------|
| Computed    | `WAIT_LEVEL_COMPUTED` | `WaitLevel::Computed`    |
| Committed   | `WAIT_LEVEL_COMMITTED` | `WaitLevel::Committed`   |
| Snapshotted | `WAIT_LEVEL_SNAPSHOT` | `WaitLevel::Snapshotted` |

**`WAIT_LEVEL_COMPUTED`** — The Transactor has executed the operation. Validation has run. If the operation violated any constraint (insufficient funds, zero-sum violation, etc.), `fail_reason` is set and the transaction is permanently rejected. If `fail_reason = 0`, the transaction is accepted and balance changes are live in memory — but not yet durable. A crash at this point would lose the transaction.

**`WAIT_LEVEL_COMMITTED`** — The WAL Storer has flushed the transaction to disk. Durability is guaranteed. The transaction will survive a crash and be replayed on restart. Balance changes are not yet visible via `get_balance`.

**`WAIT_LEVEL_SNAPSHOT`** — The Snapshotter has applied the transaction to the balance cache. `get_balance` now reflects this transaction and all transactions before it. This is the linearizable read guarantee — any `get_balance` call after this point will see a consistent, fully settled balance.

To get a transaction ID without waiting for anything, use `SubmitOperation` / `submit` instead.

---

## Batch Operations

Submit multiple operations in a single round trip. Each operation is independent — failure of one does not affect others. There is no atomicity guarantee across operations in a batch.

**gRPC:**

```bash
grpcurl -plaintext -d '{
  "operations": [
    {"deposit":  {"account": 1, "amount": "1000", "user_ref": "50"}},
    {"deposit":  {"account": 2, "amount": "2000", "user_ref": "51"}},
    {"transfer": {"from": 1, "to": 2, "amount": "500", "user_ref": "52"}}
  ]
}' localhost:50051 roda.ledger.v1.Ledger/SubmitBatch

# With wait:
grpcurl -plaintext -d '{
  "operations": [
    {"deposit": {"account": 1, "amount": "1000", "user_ref": "53"},
     "wait_level": "WAIT_LEVEL_COMMITTED"}
  ]
}' localhost:50051 roda.ledger.v1.Ledger/SubmitBatchAndWait
```

**Rust library:**

```rust
let ops = vec![
    Operation::Deposit { account: 1, amount: 1000, user_ref: 50 },
    Operation::Deposit { account: 2, amount: 2000, user_ref: 51 },
    Operation::Transfer { from: 1, to: 2, amount: 500, user_ref: 52 },
];

let results = ledger.submit_batch_and_wait(ops, WaitLevel::Committed);

for result in results {
    println!("tx {}: {:?}", result.tx_id, result.fail_reason);
}
```

Operations within a batch are sequenced in the order provided. However, other clients' transactions may be interleaved between them — there is no reservation of a contiguous block in the global order.

Prefer batch submission over repeated single calls for high-throughput ingestion — it reduces round trips significantly.

---

## Transaction Status

Poll the pipeline stage of a transaction by ID.

**gRPC:**

```bash
# Single
grpcurl -plaintext -d '{"transaction_id": "42"}' \
  localhost:50051 roda.ledger.v1.Ledger/GetTransactionStatus

# Batch
grpcurl -plaintext -d '{"transaction_ids": ["42", "43", "44"]}' \
  localhost:50051 roda.ledger.v1.Ledger/GetTransactionStatuses
```

**Rust library:**

```rust
let status = ledger.get_transaction_status(tx_id);
```

**Transaction status values:**

| Status | Meaning |
|---|---|
| `PENDING` | Sequenced, not yet executed. Balance unchanged. Not durable. |
| `COMPUTED` | Executed by Transactor. Balance updated in memory. Not yet durable. |
| `COMMITTED` | Flushed to WAL. Durable. Survives a crash. `get_balance` does not yet reflect it. |
| `ON_SNAPSHOT` | Applied to balance cache. `get_balance` reflects it. Final successful state. |
| `ERROR` | Rejected. No balance changes. Check `fail_reason` for the cause. |

**Rejection codes (`fail_reason`):**

| Code | Name | Meaning |
|---|---|---|
| `0` | `NONE` | Success |
| `1` | `INSUFFICIENT_FUNDS` | Account balance too low for the requested operation |
| `2` | `ACCOUNT_NOT_FOUND` | Referenced account does not exist |
| `3` | `ZERO_SUM_VIOLATION` | Credits and debits in the transaction do not net to zero |
| `4` | `ENTRY_LIMIT_EXCEEDED` | Composite operation exceeds the 255 step limit |
| `5` | `INVALID_OPERATION` | Operation is malformed or contains invalid parameters |
| `6` | `ACCOUNT_LIMIT_EXCEEDED` | Account ID exceeds `max_accounts` configuration |
| `7` | `DUPLICATE` | `user_ref` was seen within the dedup window — linked to the original transaction |
| `8–127` | — | Reserved for future standard reasons |
| `128–255` | — | User-defined custom reasons |

---

## Reading Balances

### Single Account

**gRPC:**

```bash
grpcurl -plaintext -d '{"account_id": 1}' \
  localhost:50051 roda.ledger.v1.Ledger/GetBalance
```

Response:

```json
{"balance": "1500", "lastSnapshotTxId": "42"}
```

**Rust library:**

```rust
let balance = ledger.get_balance(1);
// balance.balance: i64
// balance.last_snapshot_tx_id: u64
```

### Multiple Accounts

**gRPC:**

```bash
grpcurl -plaintext -d '{"account_ids": [1, 2, 3]}' \
  localhost:50051 roda.ledger.v1.Ledger/GetBalances
```

**Rust library:**

```rust
// call get_balance per account
```

### Linearizable Reads

`get_balance` reflects all transactions that have reached `ON_SNAPSHOT`. By default this lags behind committed transactions by nanoseconds — almost always negligible.

If you need a guaranteed linearizable read after a specific write — the balance must reflect your transaction and everything before it — you have two options:

**Option 1 — wait at submit time:**
```bash
# gRPC
grpcurl -plaintext -d '{
  "deposit": {"account": 1, "amount": "1000", "user_ref": "55"},
  "wait_level": "WAIT_LEVEL_SNAPSHOT"
}' localhost:50051 roda.ledger.v1.Ledger/SubmitAndWait
# then call GetBalance — guaranteed to reflect your transaction
```

```rust
// Library
ledger.submit_and_wait(op, WaitLevel::Snapshotted);
let balance = ledger.get_balance(1); // linearizable
```

**Option 2 — poll `last_snapshot_tx_id`:**
```rust
let tx_id = ledger.submit(op);
loop {
let balance = ledger.get_balance(1);
if balance.last_snapshot_tx_id >= tx_id {
// balance is now linearizable with respect to your transaction
break;
}
}
```

---

## Pipeline Index

Observe the current progress of each pipeline stage. Useful for health checks, lag monitoring, and determining when a bulk load is complete.

**gRPC:**

```bash
grpcurl -plaintext -d '{}' \
  localhost:50051 roda.ledger.v1.Ledger/GetPipelineIndex
```

Response:

```json
{
  "computedIndex": "10500",
  "committedIndex": "10498",
  "snapshotIndex": "10496"
}
```

**Rust library:**

```rust
let computed   = ledger.last_compute_id();   // processed by Transactor
let committed  = ledger.last_commit_id();  // flushed to WAL
let snapshotted = ledger.last_snapshot_id();  // applied to balance cache
```

**Common patterns:**

- **Health check** — all three indexes should be advancing. A stalled index indicates a stuck pipeline stage.
- **Commit lag** — `computed_index - committed_index`. High lag means WAL writes are falling behind.
- **Snapshot lag** — `committed_index - snapshot_index`. Should be near zero under normal load.
- **Bulk load completion** — submit all operations, then poll until `snapshot_index >= last_tx_id`.

---

## Transaction Details

Retrieve the full record of a transaction — its entries and any links to related transactions.

**gRPC:**

```bash
grpcurl -plaintext -d '{"tx_id": "42"}' \
  localhost:50051 roda.ledger.v1.Ledger/GetTransaction
```

Response:

```json
{
  "txId": "42",
  "entries": [
    {"accountId": "1", "amount": "1000", "kind": "DEBIT",  "computedBalance": "1000"},
    {"accountId": "0", "amount": "1000", "kind": "CREDIT", "computedBalance": "-1000"}
  ],
  "links": []
}
```

Each entry shows the account, amount, direction (`CREDIT` or `DEBIT`), and the balance of that account immediately after the entry was applied.

**Links** connect related transactions:

| Link kind | Meaning |
|---|---|
| `DUPLICATE` | This transaction is a duplicate of the linked one (same `user_ref` within dedup window) |
| `REVERSAL` | This transaction reverses the linked one |

---

## Account History

Retrieve the transaction history for an account, newest first, with pagination.

**gRPC:**

```bash
# First page
grpcurl -plaintext -d '{"account_id": 1, "from_tx_id": 0, "limit": 20}' \
  localhost:50051 roda.ledger.v1.Ledger/GetAccountHistory

# Next page — pass next_tx_id from previous response
grpcurl -plaintext -d '{"account_id": 1, "from_tx_id": "38", "limit": 20}' \
  localhost:50051 roda.ledger.v1.Ledger/GetAccountHistory
```

Response:

```json
{
  "entries": [
    {"accountId": "1", "amount": "500", "kind": "CREDIT", "computedBalance": "500"},
    ...
  ],
  "nextTxId": "38"
}
```

`next_tx_id = 0` means there are no more entries. Pass `from_tx_id = 0` to start from the latest transaction. Default limit is `20`, maximum is `1000`.

Use cases: account statements, balance verification, dispute resolution, reconciliation.

---

## Library Extras

These methods are available in the Rust library only and have no gRPC equivalent.

**`wait_for_transaction(tx_id)`** — blocks until the given transaction reaches `ON_SNAPSHOT`. Times out after 10 seconds.

**`wait_for_transaction_until(tx_id, duration)`** — same as above with a custom timeout.

**`wait_for_transaction_level(tx_id, level)`** — blocks until the given transaction reaches the specified `WaitLevel`. Returns immediately on rejection regardless of wait level.

**`wait_for_pass()`** — blocks until the last submitted transaction reaches `ON_SNAPSHOT`. Useful after a bulk load to confirm everything is visible.

**`get_rejected_count()`** — returns the total number of rejected transactions since startup. Useful for metrics and health monitoring.

**`query(request)` / `query_block(request)`** — low-level access to the Snapshot stage query queue. `query` is non-blocking; `query_block` blocks until the Snapshot stage processes the request. Used for advanced read patterns not covered by `get_balance`.