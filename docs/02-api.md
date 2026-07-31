# API

This document covers the full API surface of roda-ledger. Every operation is shown in both interfaces — gRPC for standalone server usage and Rust library for embedded usage. The concepts are identical; only the syntax differs.

For a full understanding of what operations, transactions, and wait levels mean, read [Concepts](./01-concepts.md) first.

---

## Setup

### gRPC Server

Start the server with Docker:

```bash
docker run -p 50051:50051 -v $(pwd)/data:/app/data tislib/roda-ledger:latest
```

To customize the server, mount a TOML config file:

```bash
docker run -p 50051:50051 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/config.toml:/app/config.toml \
  tislib/roda-ledger:latest
```

**`config.toml` — full reference:**

```toml
[server]
host                  = "0.0.0.0"
port                  = 50051
max_connections       = 1000
max_message_size_bytes = 4194304  # 4MB

[ledger]
initial_account_size = 1000000      # starting account-array capacity; grows on demand
resize_factor        = 0.75         # geometric growth factor when capacity is exceeded
wait_strategy        = "balanced"   # low_latency | balanced | low_cpu

[ledger.storage]
data_dir                       = "/data"
transaction_count_per_segment  = 10000000  # 10M transactions per segment
snapshot_frequency             = 2         # take a snapshot every N sealed WAL segments
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
    initial_account_size: 1_000_000,
    wait_strategy: WaitStrategy::Balanced,
    storage: StorageConfig {
        data_dir: "./data".to_string(),
        transaction_count_per_segment: 10_000_000,
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

# OpenAccount — open 3 sequential accounts (read the id range back via GetTransaction)
grpcurl -plaintext -d '{
  "open_account": {"count": 3, "user_ref": "45"}
}' localhost:50051 roda.ledger.v1.Ledger/SubmitOperation

# Function — invoke a registered WASM function (params padded/truncated to 8 i64)
grpcurl -plaintext -d '{
  "function": {"name": "my_fn", "params": ["1", "2"], "user_ref": "46"}
}' localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

Response (`term` is the leader term at reply time — `0` in single-node mode):

```json
{"transactionId": "42", "term": "0"}
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

let tx_id = ledger.submit(Operation::OpenAccount { count: 3, user_ref: 45 });

// `params` is a fixed [i64; 8]; pass 0 for unused slots.
let tx_id = ledger.submit(Operation::Function {
    name: "my_fn".to_string(),
    params: [1, 2, 0, 0, 0, 0, 0, 0],
    user_ref: 46,
});
```

### Operation Types

**Deposit** — credits a user account. Account 0 (the system account) is debited automatically to satisfy the zero-sum invariant. Deposits never fail due to insufficient funds.

**Withdrawal** — debits a user account. Fails with `INSUFFICIENT_FUNDS` if the account balance is below the requested amount. Account 0 is credited automatically.

**Transfer** — moves funds between two accounts atomically. Fails with `INSUFFICIENT_FUNDS` if the source account balance is insufficient. If `from == to`, the operation succeeds immediately as a no-op.

**OpenAccount** — opens `count` sequential accounts (account ids are allocated sequentially from 1; `count == 0` is treated as 1). The committed transaction carries an `AccountOpened` record with the first allocated id and the count — read it back via `GetTransaction`, or use the library's `open_accounts`, which returns the id range directly.

**Function** — invokes a registered WASM function by name. The ABI is a fixed `8 × i64`: over gRPC, `params` is a `repeated int64` that the server zero-pads (short lists) or truncates (lists longer than 8) to exactly 8; in the Rust library, `params` is a `[i64; 8]` (pass `0` for unused slots). The guest entry point is `execute(i64 × 8) -> i32`. See [WASM Runtime](./wasm-runtime.md) for the full guide on writing, registering, versioning, and invoking functions.

#### Account layouts / linked buckets

Beyond the flat `u64 → i64` balances, programs can carve out **linked buckets** — child accounts attached to a parent under a program-defined `type_id` (e.g. a `HOLD` or `BONUS` lane). Every account also carries an 8-lane `flags` word (lane 0 is the status lane). `GetBalance` exposes this:

- `include_linked` (request) — when set, the response's `linked` list carries one `LinkedBalance { type_id, balance }` per bucket linked under the queried account.
- `flags` (response) — the account's raw 8-lane flags word.

Accounts whose status lane marks them `PROGRAMMED` hold program-internal money and are **not** directly queryable — `GetBalance` rejects them with a `failed_precondition` status. Account `0` (the system source/sink) stays observable. See [WASM Runtime](./wasm-runtime.md) for how programs open and manipulate these layouts.

### The `user_ref` Field

Every operation carries a `user_ref` — a `uint64` supplied by the caller. It serves two purposes:

- **Idempotency key** — when `user_ref > 0`, if the same `user_ref` appears within the active window — the deduplication cache uses a flip-flop (active + previous) keyed off `transaction_count_per_segment`, giving an effective window of N to 2N transactions — the second submission is detected as a duplicate. It is sequenced and recorded, but linked to the original via a `TxLinkRecord { kind: DUPLICATE }` rather than re-executed. This prevents double-processing on client retries. Deduplication is always on and cannot be disabled. Pass `user_ref = 0` to opt out of the idempotency check for individual transactions.
- **Correlation reference** — stored in the WAL alongside the transaction. Use it to link to your own database record — an order ID, payment ID, or any external reference.

Separately, every transaction's WAL metadata also carries a `tag` — an 8-byte free-form field (rendered as trailing-null-trimmed UTF-8 when valid, otherwise 16-char lowercase hex). Unlike `user_ref`, `tag` plays no part in deduplication; it is a pure annotation surfaced when reading a transaction back.

---

## Custom Operations (WASM Function)

Arbitrary multi-account, multi-step atomic logic is expressed as a registered WASM function and invoked via `Operation::Function`. The function can call the `ledger.credit` / `ledger.debit` / `ledger.get_balance` host imports; the zero-sum invariant is enforced after it returns, identically to built-in operations.

See the **[WASM Runtime guide](./wasm-runtime.md)** for the full story: writing functions in Rust / AssemblyScript / WAT, registering them over gRPC or the Rust library, versioning, recovery, and performance numbers.

---

## Submit and Wait

Block until the operation reaches a specific pipeline stage, then return its transaction id (and, in cluster mode, the leader term). This does **not** return the entry-level outcome — to learn whether the transaction was rejected and why, either poll `GetTransactionStatus` afterwards or use [Submit and Wait (Result)](#submit-and-wait-result) below.

**gRPC:**

```bash
grpcurl -plaintext -d '{
  "deposit": {"account": 1, "amount": "1000", "user_ref": "46"},
  "wait_level": "WAIT_LEVEL_SNAPSHOT"
}' localhost:50051 roda.ledger.v1.Ledger/SubmitAndWait
```

Response (`term` is `0` in single-node mode):

```json
{"transactionId": "43", "term": "0"}
```

**Rust library:**

`submit_and_wait` returns a `TransactionStatus` — the pipeline stage the transaction settled at. This is purely a stage indicator and carries no accept/reject signal: a rejected transaction still gets a metadata record written to the WAL for audit, so it advances through commit and snapshot like any other and reports `OnSnapshot`. To learn whether it was accepted, read the transaction back (`submit_and_wait_result`, below) and inspect its `fail_reason`.

```rust
use roda_ledger::transaction::WaitLevel;

let status = ledger.submit_and_wait(
    Operation::Deposit { account: 1, amount: 1000, user_ref: 46 },
    WaitLevel::OnSnapshot,
);

if status == TransactionStatus::OnSnapshot {
    println!("reached snapshot (durable + visible)");
}
```

**Wait levels:**

| Level          | gRPC                        | Library                 |
|----------------|-----------------------------|-------------------------|
| Computed       | `WAIT_LEVEL_COMPUTED`       | `WaitLevel::Computed`   |
| Committed      | `WAIT_LEVEL_COMMITTED`      | `WaitLevel::Committed`  |
| On Snapshot    | `WAIT_LEVEL_SNAPSHOT`       | `WaitLevel::OnSnapshot` |
| Cluster Commit | `WAIT_LEVEL_CLUSTER_COMMIT` | —                       |

`WAIT_LEVEL_COMPUTED` is the proto zero value — it is the level used by fire-and-forget `SubmitOperation` when `wait_level` is omitted.

**`WAIT_LEVEL_COMPUTED`** — The Transactor has executed the operation. Validation has run. If the operation violated any constraint (insufficient funds, zero-sum violation, etc.), the transaction is permanently rejected: its balance changes are rolled back and a metadata record carrying the `fail_reason` is written for audit (read it via `GetTransaction`). The rejection is final, but the record still advances through commit and snapshot — the pipeline does not halt. If the operation succeeded, balance changes are live in memory but not yet durable; a crash at this point would lose the transaction.

**`WAIT_LEVEL_COMMITTED`** — The WAL Storer has flushed the transaction to disk. Local durability is guaranteed. The transaction will survive a crash and be replayed on restart. Balance changes are not yet visible via `get_balance`.

**`WAIT_LEVEL_SNAPSHOT`** — The Snapshotter has applied the transaction to the balance cache. `get_balance` now reflects this transaction and all transactions before it. This is the linearizable read guarantee — any `get_balance` call after this point will see a consistent, fully settled balance.

**`WAIT_LEVEL_CLUSTER_COMMIT`** — The transaction is quorum-durable: replicated to a majority of cluster nodes. This is the strongest level and is meaningful only in cluster mode; in single-node mode it coincides with local commit. It is gRPC-only — the embedded `WaitLevel` enum has just the three local stages — and is reached via the `cluster_wait` flag on the `*_AndWaitResult` RPCs below.

To get a transaction ID without waiting for anything, use `SubmitOperation` / `submit` instead.

### Submit and Wait (Result)

`SubmitAndWait` returns only an id. When you want the **committed transaction read back** — its entries, computed balances, and any links — use `SubmitAndWaitResult` (or `SubmitBatchAndWaitResult` for batches). These always wait at least for the snapshot stage and return the full record.

A boolean `cluster_wait` flag selects the durability gate: `false` (default) waits for the local snapshot stage; `true` waits for `CLUSTER_COMMIT` (quorum-durable) before reading the result back. Because `CLUSTER_COMMIT` implies snapshot, the committed transaction is always present once the call returns.

**gRPC:**

```bash
# Wait for the local snapshot stage (cluster_wait omitted / false)
grpcurl -plaintext -d '{
  "deposit": {"account": 1, "amount": "1000", "user_ref": "47"}
}' localhost:50051 roda.ledger.v1.Ledger/SubmitAndWaitResult

# Wait for quorum durability before returning the result
grpcurl -plaintext -d '{
  "deposit": {"account": 1, "amount": "1000", "user_ref": "48"},
  "cluster_wait": true
}' localhost:50051 roda.ledger.v1.Ledger/SubmitAndWaitResult
```

Response (a `CommitedTransaction` — see [Transaction Details](#transaction-details) for its shape):

```json
{
  "transaction": {
    "meta": {"txId": "44", "failReason": 0, "userRef": "47"},
    "items": [
      {"accountId": "1", "amount": "1000", "kind": "DEBIT",  "computedBalance": "1000"},
      {"accountId": "0", "amount": "1000", "kind": "CREDIT", "computedBalance": "-1000"}
    ]
  },
  "term": "0"
}
```

**Rust library:**

`submit_and_wait_result` waits for the snapshot stage and returns the `CommittedTransaction`; `submit_batch_and_wait_result` does the same for a batch.

```rust
let tx = ledger.submit_and_wait_result(
    Operation::Deposit { account: 1, amount: 1000, user_ref: 47 },
);

if tx.is_err() {
    println!("rejected: {:?}", tx.get_fail_reason());
} else {
    println!("committed: tx {}", tx.tx_id());
}
```

---

## Batch Operations

Submit multiple operations in a single round trip. The whole batch is sequenced and executed as **one contiguous, in-order block** — no other client's transactions interleave between them. The batch is **not** atomic as a unit: a crash mid-batch keeps the operations already committed (it is not rolled back wholesale), and a rejection of one operation does not roll back the others. Each operation is individually atomic — it commits all-or-nothing on its own.

**gRPC:**

```bash
grpcurl -plaintext -d '{
  "operations": [
    {"deposit":  {"account": 1, "amount": "1000", "user_ref": "50"}},
    {"deposit":  {"account": 2, "amount": "2000", "user_ref": "51"}},
    {"transfer": {"from": 1, "to": 2, "amount": "500", "user_ref": "52"}}
  ]
}' localhost:50051 roda.ledger.v1.Ledger/SubmitBatch

# With wait — wait_level is set once at the request level and applies to the
# whole batch (the server waits for the last operation to reach it):
grpcurl -plaintext -d '{
  "operations": [
    {"deposit": {"account": 1, "amount": "1000", "user_ref": "53"}},
    {"deposit": {"account": 2, "amount": "2000", "user_ref": "54"}}
  ],
  "wait_level": "WAIT_LEVEL_COMMITTED"
}' localhost:50051 roda.ledger.v1.Ledger/SubmitBatchAndWait
```

**Rust library:**

```rust
let ops = vec![
    Operation::Deposit { account: 1, amount: 1000, user_ref: 50 },
    Operation::Deposit { account: 2, amount: 2000, user_ref: 51 },
    Operation::Transfer { from: 1, to: 2, amount: 500, user_ref: 52 },
];

// One TransactionStatus per operation, in submit order.
let statuses = ledger.submit_batch_and_wait(ops, WaitLevel::Committed);

for status in statuses {
    println!("{:?}", status);
}
```

`submit_batch_and_wait` returns one `TransactionStatus` per operation, in submit order. To get the committed transactions read back (entries, computed balances, links), use `submit_batch_and_wait_result` / `SubmitBatchAndWaitResult` instead — see [Submit and Wait (Result)](#submit-and-wait-result).

Operations within a batch occupy a contiguous run of transaction ids in the global order; the block is sequenced and executed in the order provided, with no other client's transactions interleaved.

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

The gRPC `GetStatusRequest` also accepts an optional fencing `term` (pass `0` to skip the check). When set and the transaction's actual term differs, the response carries `term_mismatch = true` plus the superseding `term` and its `term_start_tx_id` — letting a cluster client detect that its transaction was lost to a leader change and redirect.

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
| `ON_SNAPSHOT` | Applied to balance cache. `get_balance` reflects it. Terminal stage. |
| `ERROR` | Defined in the proto enum but **not returned by this RPC** — see the note below. |
| `TX_NOT_FOUND` | The id was never sequenced on this node (too old, never existed, or — under a term fence — not covered by the supplied term). |

> **Status is a stage, not a verdict.** `GetTransactionStatus` reports only the pipeline stage, and a rejected transaction still advances through it (its metadata record is written for audit). The status RPC therefore never returns `ERROR` — a rejected transaction reports `COMPUTED` / `COMMITTED` / `ON_SNAPSHOT` like any other. The `ERROR` enum value exists in the proto but is not emitted here; to tell a rejection from a success, read the transaction's `fail_reason` (via `GetTransaction` / `get_transaction_block`, or use `SubmitAndWaitResult`). The **embedded** `TransactionStatus` enum has no `Error` variant at all — its values are `NotFound` / `Pending` / `Computed` / `Committed` / `OnSnapshot`.

**Rejection codes (`fail_reason`):**

| Code | Name | Meaning |
|---|---|---|
| `0` | `NONE` | Success |
| `1` | `INSUFFICIENT_FUNDS` | Account balance too low for the requested operation |
| `2` | `ACCOUNT_NOT_FOUND` | Referenced account does not exist |
| `3` | `ZERO_SUM_VIOLATION` | Credits and debits in the transaction do not net to zero |
| `4` | `ENTRY_LIMIT_EXCEEDED` | Transaction emitted more than 255 entries (per-transaction limit) |
| `5` | `INVALID_OPERATION` | Operation is malformed or contains invalid parameters |
| `6` | — | Retired (formerly `ACCOUNT_LIMIT_EXCEEDED`) |
| `7` | `DUPLICATE` | (Not a rejection — see below.) `user_ref` seen within the dedup window; the duplicate is linked, not failed |
| `8` | `PROHIBITED_HOST_CALL` | A WASM module called a host verb prohibited in the current phase (e.g. a constant-registration verb from `execute`) |
| `9` | `CONSTANT_NOT_FOUND` | A WASM module looked up a constant name that was never registered |
| `10` | `CONSTANT_NAME_TOO_LONG` | A constant name exceeds the maximum byte length and cannot be stored |
| `11–127` | — | Reserved for future standard reasons |
| `128–255` | — | User-defined custom reasons |

> **`DUPLICATE` is not an `ERROR`.** A transaction whose `user_ref` was seen within the dedup window is *not* rejected — it is sequenced, recorded, and linked to the original via a `TxLinkRecord { kind: DUPLICATE }` rather than re-executed. It does not surface as an `ERROR` status. Code `7` is reserved as the link/dedup discriminant, not a failure reason.

---

## Reading Balances

### Single Account

**gRPC:**

```bash
grpcurl -plaintext -d '{"account_id": 1}' \
  localhost:50051 roda.ledger.v1.Ledger/GetBalance

# Also return the account's flags word and per-type linked-bucket balances:
grpcurl -plaintext -d '{"account_id": 1, "include_linked": true}' \
  localhost:50051 roda.ledger.v1.Ledger/GetBalance
```

Response (`linked` is present only when `include_linked` was set):

```json
{
  "balance": "1500",
  "lastSnapshotTxId": "42",
  "flags": "1",
  "linked": [{"typeId": 1, "balance": "200"}]
}
```

`flags` is the account's 8-lane flags word (lane 0 = status). See [Account layouts / linked buckets](#account-layouts--linked-buckets). Querying a `PROGRAMMED` bucket returns a `failed_precondition` error.

**Rust library:**

```rust
let balance = ledger.get_balance(1);
// balance.balance: i64
// balance.last_snapshot_tx_id: u64

let flags  = ledger.get_flags(1);         // u64, the 8-lane flags word
let linked = ledger.linked_balances(1);   // Vec<(u16 type_id, Balance)>
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
ledger.submit_and_wait(op, WaitLevel::OnSnapshot);
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

## Key-Value Store

Beyond balances, the engine holds a programmable key-value state — keys and values written by WASM functions during execution and committed to the WAL alongside the transaction's entries. The read side is exposed for forward lookups.

A KV **key** is an ordered path of typed components (a `WalKvKeyPath`), rendered as a `"/"`-joined string such as `products/123`. A **value** is either an interned constant string or an `i64`. Programs may also register **constants** — a `u32` key bound to a fixed string — which both keys and values can reference. Writing KV state and registering constants happen only from inside WASM functions; see [WASM Runtime](./wasm-runtime.md) for the host API.

**gRPC — `GetKv`:** resolves the value for a key. Provide the key **either** as `key.str` (a `"/"`-joined path, parsed server-side) **or** as structured `key.items` — passing both is `INVALID_ARGUMENT`.

```bash
grpcurl -plaintext -d '{"key": {"str": "products/123"}}' \
  localhost:50051 roda.ledger.v1.Ledger/GetKv
```

Response:

```json
{"found": true, "value": {"integer": "42", "str": "42"}}
```

`found` is `false` (and `value` absent) when the key is unset.

**Rust library:**

```rust
// Forward KV lookup; returns None if the key is unset.
let value: Option<storage::Value> = ledger.get_kv(key);

// Resolve an interned constant name to its id; None if unregistered.
let id: Option<u32> = ledger.get_constant("usd");
```

---

## Pipeline Index

Observe the current progress of each pipeline stage. Useful for health checks, lag monitoring, and determining when a bulk load is complete.

**gRPC:**

```bash
grpcurl -plaintext -d '{}' \
  localhost:50051 roda.ledger.v1.Ledger/GetPipelineIndex
```

Response (`term`, `clusterCommitIndex`, and `isLeader` are cluster fields — in single-node mode `term`/`clusterCommitIndex` are `0` and `isLeader` is always `true`):

```json
{
  "computeIndex": "10500",
  "commitIndex": "10498",
  "snapshotIndex": "10496",
  "term": "0",
  "clusterCommitIndex": "0",
  "isLeader": true
}
```

**Rust library:**

```rust
let computed    = ledger.last_compute_id();   // executed by Transactor (in memory)
let written     = ledger.last_write_id();     // written to the WAL page cache (pre-fsync)
let committed   = ledger.last_commit_id();    // flushed (fsync'd) to WAL
let snapshotted = ledger.last_snapshot_id();  // applied to balance cache
```

`last_write_id` (the buffered, pre-fsync WAL position) has no field in the gRPC response — it is an embedded-only accessor.

**Common patterns:**

- **Health check** — the indexes should be advancing. A stalled index indicates a stuck pipeline stage.
- **Commit lag** — `compute_index - commit_index`. High lag means WAL writes are falling behind.
- **Snapshot lag** — `commit_index - snapshot_index`. Should be near zero under normal load.
- **Cluster lag** — `commit_index - cluster_commit_index`. How far local durability is ahead of quorum durability.
- **Bulk load completion** — submit all operations, then poll until `snapshot_index >= last_tx_id`.

---

## Transaction Details

Retrieve the full record of a transaction — its entries and any links to related transactions.

**gRPC:**

```bash
grpcurl -plaintext -d '{"tx_id": "42"}' \
  localhost:50051 roda.ledger.v1.Ledger/GetTransaction
```

Response — a `CommitedTransaction { meta, items }`. `meta` is the transaction's closing metadata (`txId`, `failReason`, `userRef`, `tag`, `timestamp`); `items` is the list of follower `WalEntry` records that make up the transaction — balance entries, plus any links, account-layout records (`accountOpened` / `accountLinked` / `accountFlagsUpdated`), or KV records:

```json
{
  "transaction": {
    "meta": {"txId": "42", "failReason": 0, "userRef": "42", "tag": ""},
    "items": [
      {"txEntry": {"accountId": "1", "amount": "1000", "kind": "DEBIT",  "computedBalance": "1000"}},
      {"txEntry": {"accountId": "0", "amount": "1000", "kind": "CREDIT", "computedBalance": "-1000"}}
    ]
  }
}
```

Each balance entry shows the account, amount, direction (`CREDIT` or `DEBIT`), and the balance of that account immediately after the entry was applied. There is no separate top-level `txId` / `entries` / `links` — everything lives under `transaction.meta` and `transaction.items`.

**`link` items** connect related transactions (a `WalTxLink` carries `toTxId` and a `kind`):

| Link kind | Meaning |
|---|---|
| `DUPLICATE` | This transaction is a duplicate of the linked one (same `user_ref` within dedup window) |
| `REVERSAL` | This transaction reverses the linked one |

**Rust library:** `get_transaction_block(tx_id)` returns `Option<CommittedTransaction>` (`meta` + an `entries: Vec<WalEntry>`); `get_transactions_block(&[tx_id])` fetches a batch.

---

## Account History

Retrieve the transaction history for an account, newest first. The request is a transaction-id **range**, not a count: the server scans backward from `from_tx_id` (`0` = latest) and stops below `to_tx_id` (the oldest id to include; `0` = scan to the start of the WAL). It returns the matching transactions plus `scan_last_tx_id` — the oldest id the scan reached. To page further into the past, re-query with `from_tx_id = scan_last_tx_id`.

**gRPC:**

```bash
# Most recent transactions touching account 1, back to the WAL start
grpcurl -plaintext -d '{"account_id": 1, "from_tx_id": 0, "to_tx_id": 0}' \
  localhost:50051 roda.ledger.v1.Ledger/GetAccountHistory

# Next page — resume from the previous response's scan_last_tx_id
grpcurl -plaintext -d '{"account_id": 1, "from_tx_id": "38", "to_tx_id": 0}' \
  localhost:50051 roda.ledger.v1.Ledger/GetAccountHistory
```

Response — `transactions` is a list of `CommitedTransaction` (same shape as [Transaction Details](#transaction-details)), newest first:

```json
{
  "transactions": [
    {
      "meta": {"txId": "42", "failReason": 0},
      "items": [
        {"txEntry": {"accountId": "1", "amount": "500", "kind": "CREDIT", "computedBalance": "500"}}
      ]
    }
  ],
  "scanLastTxId": "38"
}
```

**Rust library:** `get_account_history(account_id, from_tx_id, to_tx_id)` returns an `AccountHistory { transactions, scan_last_tx_id }`.

Use cases: account statements, balance verification, dispute resolution, reconciliation.

---

## Other RPCs

The `Ledger` service exposes several more RPCs not detailed above. Most have a (sometimes thinner) embedded equivalent on `Ledger`, noted per entry; the cluster-only RPCs do not.

**`WaitForTransaction`** — blocks until the transaction reaches a given `wait_level`, or until its outcome is otherwise known. The response is a `WaitOutcome`: `REACHED`, `NOT_FOUND` (unknown id), or `TERM_MISMATCH` (the term changed before commit and the transaction was lost — the response carries the superseding `term` / `term_start_tx_id`). An optional request `term` fences the wait (`0` = no check). Embedded: `wait_for_transaction_level`.

**WASM function registry:**

- **`RegisterFunction`** — registers a WASM binary under a name (`snake_case`, max 32 bytes; the binary must export `execute(i64 × 8) -> i32`). Returns the new per-name `version` (monotonic from 1) and the binary's `crc32c`. With `override_existing = false`, re-registering a name returns `ALREADY_EXISTS`. Blocks until the runtime reflects the new handler, so a subsequent `Function` op sees it. Embedded: `register_function(name, binary, override_existing)`.
- **`UnregisterFunction`** — removes a handler; later `Function` ops on the name fail with `INVALID_OPERATION`. Returns the version stamped on the unregister record. Embedded: `unregister_function(name)`.
- **`ListFunctions`** — lists registered functions as `FunctionInfo { name, version, crc32c }`. Embedded: `list_functions()`.

See [WASM Runtime](./wasm-runtime.md) for the full registration / versioning story.

**`GetLog`** — reads raw `WalEntry` records over the range `[from_tx_id, to_tx_id]` from this node's **local** WAL (never returns uncommitted bytes). This is the count-paginated reader: `limit` defaults to `1000` with a server hard-cap of `10000`, and the response's `next_tx_id` is the cursor for the next page (`0` = no more records in range on this node). The response also carries `last_commit_tx_id`, the node's commit watermark at read time.

**`GetTerms`** (cluster only) — returns this node's Raft term boundaries (`term.log`) and vote decisions (`vote.log`) as `TermInfo` rows, paginated by `from_term` / `limit` with a `next_term` cursor. A term may appear in only one log — check `has_term_record` / `has_vote_record`.

Operational and diagnostic RPCs (e.g. latency probing) live on the cluster side and are feature-gated — they are not part of this client-facing `Ledger` API surface.

---

## Library Extras

These methods are available in the Rust library only and have no gRPC equivalent.

**`wait_for_transaction(tx_id)`** — blocks until the given transaction reaches `ON_SNAPSHOT`. Times out after 100 seconds.

**`wait_for_transaction_until(tx_id, duration)`** — same as above with a custom timeout.

**`wait_for_transaction_level(tx_id, level)`** — blocks until the given transaction reaches the specified `WaitLevel` (times out after 10 seconds). Returns once a rejected transaction settles at its terminal stage.

**`wait_for_pass()`** — blocks until the last submitted transaction reaches `ON_SNAPSHOT`. Useful after a bulk load to confirm everything is visible.

**`open_accounts(count)`** — opens `count` accounts and returns an `OpenAccountsResult { tx_id, fail_reason, begin_account_id, count }` with the allocated id range (the gRPC path uses the `OpenAccount` operation plus `GetTransaction` to read the range back).

**`query(request)` / `query_block(request)`** — low-level access to the Snapshot stage query queue. `query` is non-blocking; `query_block` blocks until the Snapshot stage processes the request. Used for advanced read patterns not covered by `get_balance`.