# Operations and Consistency Model

Roda-Ledger provides a high-performance, durable, and strictly ordered execution environment for financial transactions. This document details the consistency guarantees, isolation levels, and the underlying mechanisms that ensure the integrity of the ledger.

## 1. Core Guarantees

The system is designed around several key distributed systems and database properties:

| Property | Guarantee | Mechanism |
| :--- | :--- | :--- |
| **Atomicity** | All-or-Nothing | Transactions are processed as single units; either all entries are applied or none are. |
| **Consistency** | Strong | The Zero-Sum Invariant ensures that no value is created or destroyed within the system. |
| **Isolation** | Serializability | A single-threaded deterministic Transactor ensures a total global order of all operations. |
| **Durability** | Strict | Write-Ahead Logging (WAL) ensures every transaction is persisted before it is considered "Committed". |
| **Linearizability** | Provided | Monotonic Transaction IDs assigned by a single Sequencer and polling for pipeline status enable linearizability. |

---

## 2. Linearizability (Atomic Consistency)

Linearizability is the strongest consistency model for single-object operations. In Roda-Ledger, every transaction is assigned a unique, monotonic `transaction_id` by the **Sequencer** (Core 0).

- **Total Order**: Every operation that enters the system is placed in a single, global timeline by the Sequencer. This timeline is immutable; once a `transaction_id` is assigned, its relative order to all other transactions is fixed.
- **Real-time Guarantee**: If a client receives a confirmation for transaction A before they start submitting transaction B, A will always have a lower `transaction_id` than B.
- **Single Source of Truth**: The Sequencer is the only component that assigns IDs, preventing any race conditions or ordering conflicts at the ingestion layer. This is equivalent to a "single leader" model in distributed systems, but within a single multi-core machine.

---

## 3. Serializability

Roda-Ledger achieves **Serializability** — the highest level of isolation — through its "Core-Per-Stage" pipelined architecture.

### The Single-Threaded Transactor
All business logic, balance checks, and state transitions happen in the **Transactor** (Core 1). Because the Transactor is single-threaded:
1. **No Concurrency Anomalies**: Issues like Dirty Reads, Non-Repeatable Reads, or Phantom Reads are architecturally impossible.
2. **Deterministic Execution**: Given the same initial state and the same sequence of transactions, the Transactor will always produce the same resulting state.
3. **Implicit Locking**: There are no database locks. The single-threaded nature of the stage acts as a global lock without the overhead of lock contention or deadlocks.

---

## 4. Strong Consistency & Pipeline Stages

Consistency in Roda-Ledger is viewed through the progress of a transaction through the pipeline. A transaction's status is defined by its position relative to three global indices:

1. **Compute Index (`last_compute_id`)**:
   - The Transactor has processed the transaction.
   - The result is reflected in the Transactor's hot memory cache.
   - **Guarantee**: Read-your-writes (if querying the Transactor's state).

2. **Commit Index (`last_commit_id`)**:
   - The WAL Storer has persisted the transaction (and its resulting entries) to disk.
   - **Guarantee**: Durability. Even if the system crashes now, the transaction will be recovered.

3. **Snapshot Index (`last_snapshot_id`)**:
   - The Snapshotter has applied the entries to the persistent balance store.
   - **Guarantee**: The transaction is now part of the "base state" used for fast recovery and balance queries.

### Client-Side Consistency
Clients can choose their desired level of consistency by polling the transaction status:
- **Fast Path**: Assume success once `transaction_id` is received (Optimistic).
- **Safe Path**: Wait until `status == COMMITTED` (Durable).
- **Global Path**: Wait until `status == ON_SNAPSHOT` (Fully Integrated).

---

## 5. The Zero-Sum Invariant

A fundamental safety property of Roda-Ledger is the **Zero-Sum Invariant**:
> `sum(credits) == sum(debits)` for every transaction.

- Every unit of value entering the system (Deposit) is balanced by a credit from a reserved `SYSTEM` account.
- Every unit leaving (Withdrawal) is balanced by a debit to a `SYSTEM` account.
- Transfers move value between accounts, naturally balancing to zero.
- **Enforcement**: The Transactor validates this invariant for every operation. Any operation that would violate it is rejected immediately and never reaches the WAL.

---

## 6. gRPC and Consistency

The gRPC interface (defined in [ADR-004](adr/0004-grpc-interface.md)) is designed to maintain these core guarantees for external clients:

- **Unary Operations**: Each `SubmitOperation` request is synchronous and returns the `transaction_id` once the Sequencer has accepted it.
- **Polling for Finality**: Because gRPC is stateless and doesn't support server-push for every transaction, clients use `GetTransactionStatuses` or `GetPipelineIndex` to poll for `COMMITTED` status.
- **Batching and Ordering**: `SubmitBatch` guarantees that operations within the same batch are submitted to the Sequencer in the order they appear in the request. However, operations from other clients can be interleaved between these batch elements once they reach the Sequencer.
- **Consistency vs. Freshness**: `GetBalance` returns the `last_snapshot_tx_id`. If a client needs a more up-to-date balance, they must wait until the `last_snapshot_tx_id` is greater than or equal to their latest `transaction_id`.

## 7. Recovery and Fault Tolerance

In the event of a crash, the system restores its strongly consistent state:
1. **Snapshot Loading**: The latest snapshot is loaded into the Snapshotter and Transactor.
2. **WAL Replay**: All transactions in the WAL with an ID greater than the snapshot ID are replayed in exact order.
3. **Index Alignment**: The Compute, Commit, and Snapshot indices are restored to the last known valid state on disk.

Because the execution is deterministic and the WAL contains physical entries (credits/debits) rather than logical operations, the recovery process is guaranteed to reconstruct the exact state prior to the crash.
