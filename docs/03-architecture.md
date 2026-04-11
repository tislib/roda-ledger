# Architecture

This document explains the design of roda-ledger — what each part does, why it exists, and why the tradeoffs were made the way they were. Implementation details (data structures, binary formats, lock mechanics) are covered in [Internals](./04-internals.md).

---

## Design philosophy

roda-ledger is built around two problems that pull in opposite directions.

**The correctness problem.** A financial ledger must be exactly right, every time. Transactions must execute in a defined order. Balances must reflect every committed transaction and nothing else. The zero-sum invariant must hold across every operation. Concurrent writers make this hard — races, conflicts, and partial states become failure modes that are difficult to reason about and catastrophic when they occur.

**The throughput problem.** A ledger that is correct but slow is not useful. Modern hardware is fast, but only if you use it well. Sequential processing, lock contention, and unnecessary synchronization leave most of that hardware idle.

The solution roda-ledger uses is a **staged pipeline**. Each stage does one job, runs on its own thread, and communicates with adjacent stages through lock-free queues. There is no shared mutable state between stages. This separates the correctness problem from the throughput problem cleanly: correctness is handled by the single-writer Transactor, throughput is achieved by running all other work concurrently around it.

The key insight: **parallelism around execution, not within it.** The Transactor executes transactions one at a time, in order — this is the source of all correctness guarantees. But while the Transactor is executing transaction N, the WAL is persisting transaction N-1, and the Snapshotter is making transaction N-2 visible to readers. All four stages run simultaneously on separate cores. The pipeline keeps every stage busy without ever requiring coordination between them.

---

## Pipeline overview

<img src="./resources/pipeline.png" class="wide-image" />
<!-- Source: docs/images/pipeline.excalidraw -->

A transaction moves through four stages. Each stage adds a guarantee:

| Stage | Guarantee added | Transaction status |
|---|---|---|
| Sequencer | Monotonic ID, permanent global order | `PENDING` |
| Transactor | Executed, serializable, result known | `COMPUTED` |
| WAL | Durable on disk, crash-safe | `COMMITTED` |
| Snapshotter | Balances visible, indexes updated | `ON_SNAPSHOT` |

The caller chooses which stage to wait for. This is the performance/guarantee dial — see [API](./02-api.md) for wait level details.

---

## The Sequencer

The Sequencer is the entry point. It has one job: assign a unique, monotonically increasing ID to every incoming transaction and fix its position in the global order.

This ordering decision is permanent. Once a transaction has a sequence number, its position in history is immutable — no later stage can change it, and no other transaction can be inserted before it.

The Sequencer has no dedicated thread. It runs synchronously on the caller's thread at submit time, which keeps its overhead minimal — it is nothing more than an atomic increment and a queue push. The dedicated threads begin at the Transactor.

Why is global ordering important? Because the Transactor must execute transactions deterministically. If two clients submit transactions simultaneously, the system must pick one ordering and commit to it. The Sequencer makes that choice and enforces it for the rest of the pipeline.

---

## The Transactor

The Transactor is the heart of roda-ledger. It is a single-threaded, deterministic execution engine that processes one transaction at a time, in strict sequence order.

**Why single-threaded?** Because concurrency within execution would require locks, conflict detection, or optimistic retry — all of which add complexity and failure modes. A single writer eliminates these problems entirely. There are no races because there is only one writer. There are no conflicts because transactions execute one at a time. The result is correct by construction, not by careful coordination.

<img src="./resources/transactor-flow.png" class="less-wide-image"/>
<!-- Source: docs/images/transactor-flow.excalidraw -->

**What it does.** For each transaction, the Transactor reads the current balances of all accounts involved, applies the operation (Deposit, Withdrawal, Transfer, or Composite steps), validates all invariants (zero-sum, negative balance if the flag is set), and writes the resulting entries. If any validation fails, the entire transaction is rolled back — no balance is updated, and the transaction is marked as `ERROR`.

**Deduplication.** The Transactor checks `user_ref` against a sliding window of recent transactions. If a duplicate is detected within the configured window, the transaction is recorded but linked to the original via a `DUPLICATE` link rather than re-executed. This prevents double-processing on client retries without requiring the client to track idempotency state externally. Deduplication is only active when `user_ref > 0`.

**The balance cache.** The Transactor maintains a hot in-memory cache of all account balances. Every operation reads from and writes to this cache. The cache always reflects the latest committed state — writes from the Transactor are immediately visible to subsequent transactions in the same stage. This is why the write path is always linearizable: the Transactor never makes a decision based on stale data.

**What it produces.** Each committed transaction becomes a set of entries — individual credit and debit records — that flow to the WAL stage. The entries carry the computed balance of each account after the operation, calculated once here and stored. This balance is never recalculated downstream.

---

## The WAL

The WAL (Write-Ahead Log) stage is responsible for one thing: making transactions durable. A transaction that has been executed by the Transactor exists only in memory — the WAL stage writes it to disk before it can be considered safe.

**Why a WAL?** Because memory is volatile. If the process crashes after the Transactor executes a transaction but before it is persisted, that transaction is lost. The WAL ensures that once a transaction is confirmed as `COMMITTED`, it survives crashes and power loss.

<img src="./resources/wal-flow.png" class="less-wide-image"/>
<!-- Source: docs/images/wal-flow.excalidraw -->

**Why batching?** Writing one transaction to disk at a time would be slow — each write would require a separate `fdatasync` call, and `fdatasync` latency is dominated by disk hardware, typically hundreds of microseconds. Instead, the WAL stage batches transactions dynamically: it accumulates whatever transactions the Transactor has produced since the last flush, writes them all in a single sequential write, and calls `fdatasync` once for the batch. This amortizes the disk latency across many transactions, dramatically improving throughput at the cost of a small, bounded increase in commit latency.

**Why sequential writes?** Sequential disk writes are the fastest possible disk operation — orders of magnitude faster than random writes. The WAL is append-only by design, which guarantees sequential access patterns regardless of workload.

**Segmented WAL.** The WAL is not a single file. It is divided into segments of configurable size. When a segment is full, it is sealed — closed and made immutable. Sealing is important for recovery: a sealed segment is a complete, consistent unit that can be replayed independently. The segment structure also bounds recovery time: the system only needs to replay segments that postdate the latest snapshot.

**The bottleneck.** The WAL stage is the slowest stage in the pipeline by design. Disk writes take hundreds of microseconds; everything else takes nanoseconds. This is the right tradeoff — durability requires disk persistence, and the pipeline architecture ensures that this latency affects only the `COMMITTED` wait level, not the Transactor's throughput.

---

## The Snapshotter

The Snapshotter is the final stage. It takes the committed entries produced by the WAL and applies them to the readable state: the balance cache, the transaction index, and the account history index.

**Why a separate stage?** Because reads and writes need to be decoupled. The Transactor's balance cache is the write-side truth — it is private to the Transactor and optimized for write performance. The Snapshotter maintains the read-side truth — the state that `get_balance`, `GetTransaction`, and `GetAccountHistory` query. Separating these two caches means readers never block writers and writers never block readers.

**What it updates.** Three things:
- **Balance cache** — the readable balance per account, updated atomically per entry. Once the Snapshotter processes a transaction, `get_balance` reflects it.
- **Transaction index** — an index mapping transaction ID to its entries. This powers `GetTransaction` for payment status checks and audit.
- **Account history index** — an index mapping account ID to its transaction history. This powers `GetAccountHistory` for statements and reconciliation.

**Why it is fast.** The Snapshotter does no computation — the balances were already calculated by the Transactor and stored in the entries. The Snapshotter simply applies pre-computed values to the read-side caches. The gap between `COMMITTED` and `ON_SNAPSHOT` is typically tens to hundreds of nanoseconds.

**Linearizable reads.** Once a transaction reaches `ON_SNAPSHOT`, any subsequent `get_balance` call is guaranteed to reflect it and all transactions before it. This is the linearizable read guarantee. The `last_snapshot_tx_id` returned by `get_balance` tells the caller exactly which transactions are reflected in the balance, allowing non-blocking freshness checks without waiting for `ON_SNAPSHOT`.

---

## Inter-stage communication

Stages communicate exclusively through **SPSC lock-free queues** (Single Producer, Single Consumer). There is no shared mutable state between stages — each stage owns its data completely, and the only thing that crosses stage boundaries is the queue.

**Why SPSC?** Because each queue has exactly one producer and one consumer — the stage upstream writes, the stage downstream reads. SPSC queues are the simplest possible concurrent data structure for this pattern: no CAS loops, no contention, minimal memory overhead. They are also cache-friendly, which matters at nanosecond timescales.

**Queue sizes.** The queue between Sequencer and Transactor is `config.queue_size` (default 1024). The queue between Transactor and WAL is larger (131,072 entries) because the WAL is the slow stage — the larger buffer absorbs bursts and prevents the Transactor from stalling while the WAL is flushing to disk.

**Backpressure.** When a queue is full, the producer waits. The wait strategy is controlled by `pipeline_mode`:

- `low_latency` — spins indefinitely. Burns CPU but eliminates scheduler latency. Use when CPU is dedicated and latency is the priority.
- `balanced` — spins briefly, then yields, then parks. Good default for most workloads.
- `low_cpu` — parks quickly. Higher latency, CPU-friendly. Use in resource-constrained environments.

Backpressure propagates naturally through the pipeline: if the WAL is slow (disk pressure), the Transactor-to-WAL queue fills, the Transactor stalls, and the Sequencer-to-Transactor queue fills, which eventually stalls the caller at `submit()`. The system self-regulates without any explicit flow control mechanism.

---

## Recovery design

roda-ledger is designed to recover automatically after a crash. When the ledger starts, it restores its state from disk before accepting any new transactions.

**The strategy: snapshot + WAL replay.** Rather than replaying the entire WAL from the beginning on every restart (which would take longer as the log grows), roda-ledger periodically checkpoints the full balance state into a snapshot file. On recovery, the system loads the latest snapshot — which captures all balances at a specific transaction ID — and then replays only the WAL segments that came after that snapshot. This bounds recovery time regardless of how long the ledger has been running.

**Why segments matter for recovery.** Because a sealed WAL segment is a complete, consistent unit. The system knows exactly which transactions are in each segment and whether the segment is fully written. An unsealed segment at the end of the log may be partially written — the recovery process handles this by replaying only the complete, valid entries it finds.

**Snapshot frequency.** The `snapshot_frequency` configuration controls how often a snapshot is taken, expressed in sealed WAL segments. A lower value means more frequent snapshots and faster recovery, at the cost of more snapshot I/O during normal operation. A higher value means less snapshot overhead but longer recovery time after a crash.

**Correctness guarantee.** Once a transaction is `COMMITTED`, it is in the WAL. The WAL is replayed on recovery. Therefore, a committed transaction is never lost. The recovery process is deterministic — given the same snapshot and WAL segments, it always produces the same state.

---

## Design boundaries

Understanding what roda-ledger is optimized for is as important as understanding what it can do.

**Single node.** roda-ledger is a single-node engine. All four pipeline stages run on one machine. There is no replication or multi-node coordination today. Raft-based multi-node replication is planned — when it arrives, the WAL's segmented, append-only design will make it a natural fit for log replication.

**Pre-allocated account space.** Account balances are stored in a pre-allocated structure sized to `max_accounts`. This gives O(1) balance reads and writes at all times — there is no hash map lookup, no dynamic allocation, no memory fragmentation. The tradeoff is that `max_accounts` must be set at startup and memory is allocated upfront. At the default of 1,000,000 accounts this is negligible, but it is a capacity planning decision that should be made deliberately.

**Disk-bound throughput.** The WAL stage is the ceiling for `COMMITTED` throughput. Sequential write speed and `fdatasync` latency determine how fast transactions can be durably committed. On NVMe storage this is typically in the hundreds of microseconds per batch; on spinning disks it is significantly higher. If `COMPUTED` throughput is all that is needed (in-memory only, no crash safety), the Transactor can sustain millions of transactions per second.

**Single-writer ceiling.** The Transactor processes transactions sequentially. This is the source of correctness but also the upper bound on execution throughput. In practice, the bottleneck is almost always the WAL, not the Transactor — but at sufficiently high load on sufficiently fast storage, the Transactor's single-threaded nature becomes the limit.