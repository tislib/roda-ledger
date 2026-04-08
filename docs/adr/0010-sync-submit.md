# ADR-010: Synchronous Submit with Wait Levels

**Status:** Proposed  
**Date:** 2026-04-06  
**Author:** Taleh Ibrahimli  

**Amends:**
- ADR-004 — adds `SubmitAndWait` RPC, `WaitLevel` enum, `SubmitResult`
- ADR-003 — adds `submit_and_wait()` to Ledger API

---

## Context

`submit()` is the primary and recommended submission method in roda-ledger. It
is fire-and-forget — returns `tx_id` immediately, never blocks, and allows the
pipeline to run at full throughput. For high-throughput workloads, internal
pipelines, and batch processing, `submit()` is the correct choice.

However, not every caller needs maximum throughput. Some callers have simpler
requirements:

- **Rejection feedback** — know immediately if the transaction was rejected
  (insufficient funds, duplicate) without a polling loop.
- **Durability confirmation** — know the transaction is durable on disk before
  returning a receipt to an end user.
- **Balance consistency** — know that `GetBalance` reflects the transaction
  before querying it.

These callers can implement polling on top of `submit()` — but this places
the burden of timeout handling, polling frequency, and pipeline stage reasoning
on every integrator. `submit_and_wait()` is an ergonomic alternative that
handles this internally, at the cost of lower throughput per calling thread.

`submit()` remains the primary API. `submit_and_wait()` makes roda-ledger
accessible to a broader range of use cases without compromising the core
design.

---

## Decision

Introduce `submit_and_wait()` as an ergonomic alternative to `submit()` for
callers that prioritize simplicity over throughput. The caller specifies a
`WaitLevel` — the pipeline stage to wait for — and blocks until that stage
has processed the transaction, receiving a complete `SubmitResult` in return.

`submit()` remains the primary API and the recommended choice for
high-throughput workloads. `submit_and_wait()` trades per-thread throughput
for ease of use.

`ASYNC` is not a valid `WaitLevel`. Fire-and-forget is served by `submit()`.
A `WaitLevel` with no waiting is a contradiction — and returning a meaningful
`SubmitResult` is impossible before any pipeline stage has processed the
transaction.

---

## WaitLevel

```rust
pub enum WaitLevel {
  Processed,    // Transactor has applied the transaction
  // caller knows immediately: accepted or rejected
  // transaction is already queued for WAL write — it will be
  // persisted durably very soon after this notification
  // the only loss scenario is a crash in the very narrow window
  // between Transactor output and WAL fdatasync completing
  // in practice this window is measured in microseconds

  Committed,    // WAL has been flushed to disk (fdatasync)
  // durable — survives crash
  // standard guarantee for financial transactions

  Snapshotted,  // Snapshotter has applied entries to Vec<AtomicI64>
  // GetBalance is guaranteed to reflect this transaction
  // highest latency — pipeline depth after Committed
}
```

### Latency profile

```
Processed:    nanoseconds — Transactor processes in pipeline order
              first to respond, lowest latency
              transaction queued for WAL immediately — durable within microseconds
              in normal operation. loss only if crash occurs in that narrow window.
              useful for: rejection detection, internal high-throughput pipelines

Committed:    ~676µs avg (WAL fsync latency, group commit amortized)
              durable on disk
              useful for: external payments, receipts, user-facing confirmations

Snapshotted:  Committed latency + Snapshotter pipeline lag
              slightly higher than Committed
              useful for: callers who immediately query GetBalance after submit
                          eliminates read-your-own-writes consistency bugs
```

### Guarantee matrix

```
Level          Survives crash    GetBalance consistent    Rejection known
─────────────────────────────────────────────────────────────────────────
Processed      almost always*    no                       yes ← immediately
Committed      yes               no                       yes
Snapshotted    yes               yes ← guaranteed         yes

* transaction queued for WAL immediately after Processed notification.
  loss only if crash occurs in the microsecond window before WAL fdatasync.
```

---

## Ledger API (amends ADR-003)

```rust
pub struct SubmitResult {
  pub tx_id:       u64,
  pub status:      TxStatus,
  pub fail_reason: Option<FailReason>,
}

pub enum TxStatus {
  Committed,
  Error,
}

// existing — unchanged
pub fn submit(&self, op: Operation) -> u64;

// new — blocks until WaitLevel reached
pub fn submit_and_wait(&self, op: Operation, level: WaitLevel) -> SubmitResult;
```

`submit_and_wait()` blocks the calling thread until the specified pipeline stage
has processed the transaction. On error (rejection by Transactor), returns
immediately with `TxStatus::Error` and the appropriate `FailReason` regardless
of the requested `WaitLevel` — there is no point waiting for Committed or
Snapshotted if the transaction was rejected at Processed.

---

## Wait Mechanism

### Two distinct waiting paths

**Ledger API (`submit_and_wait`)** — blocking, for non-async callers:

Calls `submit()` then `wait_for_transaction(tx_id, level)` internally.
`wait_for_transaction` is a blocking call that uses the pipeline's existing
`wait_strategy` — the same adaptive spin/yield/park used by pipeline stages
via `PressureManager`. No extra threads, no tokio involvement.

```rust
pub fn submit_and_wait(&self, op: Operation, level: WaitLevel) -> SubmitResult {
  let tx_id = self.submit(op);
  self.wait_for_transaction(tx_id, level);
  self.get_transaction_status(tx_id)
}
```

When no transactions are advancing, `wait_strategy` backs off using:

```rust
self.config.wait_strategy.wait_strategy(retry_count);
retry_count += 1;
```

This prevents CPU spinning at 100% when the pipeline is idle or slow —
the same adaptive pressure management used throughout the pipeline.

**gRPC server** — async, tokio-native:

The gRPC server does not use `wait_for_transaction`. Instead, a single
background tokio task continuously polls `last_computed_id()`,
`last_committed_id()`, and `last_snapshot_id()`. When any value advances,
it wakes all waiters whose `tx_id` is now satisfied at their requested level.

This means:
- One poller drives all concurrent gRPC waiters
- Waiters sleep until woken — zero CPU between notifications
- Pipeline stages are completely untouched
- `wait_strategy` applies to the poller when no progress is detected —
  preventing the poller from spinning at 100% when the pipeline is idle

### Pipeline stages untouched

`last_computed_id()`, `last_committed_id()`, and `last_snapshot_id()` are
read-only observations of existing monotonic counters already maintained by
each pipeline stage. No changes to Transactor, WAL, or Snapshotter internals.
The wait mechanism is entirely in the Ledger and gRPC layers.

---

## gRPC API (amends ADR-004)

```protobuf
enum WaitLevel {
  PROCESSED   = 1;
  COMMITTED   = 2;
  SNAPSHOTTED = 3;
}

message SubmitOperationRequest {
  Operation  operation  = 1;
  WaitLevel  wait_level = 2;   // if omitted → use existing SubmitOperation (async)
}

message SubmitOperationResponse {
  uint64    tx_id       = 1;
  uint32    fail_reason = 2;   // 0 if status == COMMITTED
}

// New RPC — blocks until WaitLevel reached
        rpc SubmitAndWait(SubmitOperationRequest) returns (SubmitOperationResponse);

// Existing RPC — unchanged, fire and forget
        rpc SubmitOperation(SubmitOperationRequest) returns (SubmitOperationResponse);
```

The existing `SubmitOperation` RPC returns immediately with `tx_id` only.
`SubmitAndWait` blocks the gRPC stream until the specified `WaitLevel` is
reached, then returns the full `SubmitResult`.

gRPC handler calls `submit_and_wait()` on a `spawn_blocking` thread — the
blocking call must not park a tokio async task.

---

## Batch Submit

`submit_batch_and_wait()` submits multiple operations and waits until all reach
the specified `WaitLevel`:

```rust
pub fn submit_batch_and_wait(
  &self,
  ops:   Vec<Operation>,
  level: WaitLevel,
) -> Vec<SubmitResult>;
```

Each operation in the batch gets its own `PendingWait`. The caller blocks until
all are notified. At `Committed` level, group commit naturally covers entire
batches under a single `fdatasync()` — efficient.

```protobuf
rpc SubmitBatchAndWait(SubmitBatchRequest) returns (SubmitBatchResponse);

message SubmitBatchRequest {
  repeated Operation operations = 1;
  WaitLevel          wait_level = 2;
}

message SubmitBatchResponse {
  repeated SubmitOperationResponse results = 1;
}
```

---

## Usage Patterns

### User-facing payment — Committed

```rust
// external payment, return receipt to user
let result = ledger.submit_and_wait(
Operation::Transfer { from: 1, to: 2, amount: 500 },
WaitLevel::Committed,
);

let success = result.fail_reason != 0;
```

### Immediate balance read — Snapshotted

```rust
// caller needs consistent balance immediately after submit
let result = ledger.submit_and_wait(
Operation::Deposit { account: 1, amount: 1000 },
WaitLevel::Snapshotted,
);

// guaranteed: GetBalance reflects this deposit
let balance = ledger.get_balance(1);
```

### High-throughput with fast rejection — Processed

```rust
// internal pipeline — want fast rejection feedback, durability not critical
let result = ledger.submit_and_wait(
Operation::Transfer { from: 1, to: 2, amount: 999_999 },
WaitLevel::Processed,
);

if result.status == TxStatus::Error {
// know immediately — no polling needed
handle_rejection(result.fail_reason);
}
```

---

## Consequences

### Positive

- Callers choose the durability/latency tradeoff explicitly per submission
- No polling loop needed — `submit_and_wait()` blocks until the right stage
- `Committed` level maps directly to the standard financial transaction guarantee
- `Snapshotted` eliminates read-your-own-writes consistency bugs for balance queries
- `Processed` gives fast rejection feedback without durability overhead
- On rejection — all wait levels return immediately, no wasted pipeline time
- Group commit at `Committed` level batches multiple waiters under one `fdatasync()`
- `submit()` unchanged — existing fire-and-forget callers unaffected
- Pipeline stages completely untouched — wait mechanism is in Ledger and gRPC layers
- `wait_strategy` prevents CPU spinning when pipeline is idle — adaptive backoff
- One background tokio task drives all gRPC waiters — zero per-waiter polling

### Negative

- `submit_and_wait()` blocks the calling thread — not suitable for async contexts
  directly, use the gRPC path for async callers
- `Snapshotted` latency is non-deterministic — depends on Snapshotter pipeline lag
- Timeout handling not defined — caller responsible for wrapping in a timeout

### Neutral

- `WaitLevel` has no `ASYNC` variant — `submit()` covers fire-and-forget
- Batch submit follows the same pattern — no new mechanism needed
- gRPC `SubmitOperation` unchanged — existing clients unaffected
- `last_computed_id()`, `last_committed_id()`, `last_snapshot_id()` are
  read-only observations — no pipeline stage modifications required

---

## Alternatives Considered

**Polling-based approach (existing)**
Insufficient for general use — callers must implement polling, handle timeouts,
and reason about pipeline stage semantics. `submit_and_wait()` internalizes this.

**Single blocking level (Committed only)**
Rejected — `Processed` is genuinely useful for high-throughput pipelines that
want fast rejection feedback without durability cost. `Snapshotted` eliminates
a real class of consistency bugs. Three levels cover the three meaningful pipeline
boundaries.

**yield_now() per waiter**
Rejected — 10K concurrent waiters each calling `yield_now()` in a loop is
effectively 10K busy-waiters. One background poller driving all waiters via
waker notifications is strictly better.

**Modifying pipeline stages for notification**
Rejected — pipeline stages are the performance-critical core. Adding notification
logic couples the wait mechanism to the pipeline internals. Reading existing
monotonic ID counters from outside the pipeline is the correct separation.

---

## References

- ADR-003 — Ledger API, Operation enum, submit() (amended by this ADR)
- ADR-004 — gRPC interface, SubmitOperation RPC (amended by this ADR)
- ADR-006 — WAL Sync thread, group commit, fdatasync
- ADR-008 — SnapshotMessage queue, mpsc::sync_channel(1) oneshot pattern
- `src/ledger.rs` — submit_and_wait(), PendingWait, pending_waits DashMap
- `src/transactor.rs` — Processed notification, rejection notification
- `src/storage/wal.rs` — Committed notification after fdatasync
- `src/snapshot.rs` — Snapshotted notification after balance update