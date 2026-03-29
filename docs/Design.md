# Roda-Ledger Architecture: Pipelined Execution

![Process Diagram](process_diagram.svg)

Roda-Ledger is built on a "Core-Per-Stage" model, utilizing asynchronous pipelining and lock-free ArrayQueues to eliminate thread contention and maximize mechanical sympathy.

For detailed information on the consistency guarantees (serializability, linearizability) and the zero-sum invariant, see the [Operations & Consistency Model](operations_consistency.md).

## 1. Pipeline Stages

### A. Core 0: The Sequencer
The entry point for all system interactions.
- **Identity Assignment:** Assigns unique, monotonic IDs to incoming transactions.
- **Ingestion:** Manages the interface with the Actor, providing immediate transaction ID feedback and status checks.
- **Push:** Handoff to the next stage via a lock-free ArrayQueue.

### B. Core 1: The Transactor (The Engine)
The deterministic heart of the system.
- **Logic Execution:** Performs all business logic, balance checks, and state transitions.
- **Compute Index:** Tracks the furthest point of successfully processed transactions in memory.
- **Isolation:** Because it is isolated on Core 1, it maintains its own hot cache of the ledger state, free from interference by the Sequencer or I/O threads.


### C. Core 2: The WAL Storer (The Sink)
The durability layer.
- **Persistence:** Responsible for writing transactions to the Write-Ahead Log (WAL).
- **Commit Index:** Marks the transaction as "Durable" once it has been successfully flushed to disk.
- **Finality:** Once the WAL Storer confirms a write, the transaction is considered persistent and ready for the Snapshot stage.

### D. Core 3: The Snapshotter (The Archiver)
The state management layer.
- **State Materialization:** Periodically materializes the in-memory state into persistent snapshots.
- **Log Truncation:** Provides the necessary state to allow for WAL truncation (in future versions) and significantly speeds up crash recovery.
- **Checkpointing:** Coordinates with the Transactor and WAL Storer to ensure snapshots are taken at consistent transaction boundaries.

## 2. Communication: ArrayQueue Interconnects
Stages are linked by bounded, pre-allocated **ArrayQueues**.
- **SPSC Pattern:** Typically operates as Single-Producer/Single-Consumer between cores to minimize atomic contention.
- **Backpressure:** The bounded nature of the queues provides natural backpressure; if any stage (e.g., WAL Storer or Snapshotter) slows down, the previous stages (Transactor, Sequencer) will eventually throttle, preventing memory exhaustion.


## 3. Logical View: Multi-Index Tracking
The system maintains a clear separation of progress:
- **Compute Index:** Updated immediately after Core 1 (Transactor) processes a transaction.
- **Commit Index:** Updated after Core 2 (WAL Storer) confirms disk persistence.
- **Snapshot Index:** Updated after Core 3 (Snapshotter) processes the transaction and it becomes part of the potential next snapshot.
