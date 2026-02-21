# Roda-Ledger Architecture: Pipelined Execution

Roda-Ledger is built on a "Core-Per-Stage" model, utilizing asynchronous pipelining and lock-free ArrayQueues to eliminate thread contention and maximize mechanical sympathy.

<img src="process_diagram.svg" width="800"/>

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
- **Finality:** Only when the WAL Storer confirms a write is the transaction considered fully committed in the system lifecycle.

## 2. Communication: ArrayQueue Interconnects
Stages are linked by bounded, pre-allocated **ArrayQueues**.
- **SPSC Pattern:** Typically operates as Single-Producer/Single-Consumer between cores to minimize atomic contention.
- **Backpressure:** The bounded nature of the queues provides natural backpressure; if the WAL Storer slows down, the Transactor and Sequencer will eventually throttle, preventing memory exhaustion.


## 3. Logical View: Dual-Index Tracking
The system maintains a clear separation of progress:
- **Compute Index:** Updated immediately after Core 1 processes a transaction. This reflects the "current" state for the next incoming transaction.
- **Commit Index:** Updated only after Core 2 confirms disk persistence. This is the "safe" state used for crash recovery.


## 4. Hardware Optimization
- **Core Pinning:** Each stage is affinitized to a specific physical core to prevent L1 cache trashing.
- **Memory Ordering:** Utilizes `Ordering::Acquire` and `Ordering::Release` to synchronize the transfer of transaction ownership between cores without full pipeline flushes.