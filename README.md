# Roda-Ledger

A high-performance, durable, and crash-consistent financial ledger and transaction executor built in Rust.

## Overview

Roda-Ledger is designed for low-latency, high-throughput financial applications. It utilizes a pipelined architecture to
maximize hardware efficiency while ensuring strict transaction ordering and durability. It is built to handle millions
of transactions per second with microsecond-level latency.

## Key Features

- **High Performance:** Pipelined execution model optimized for modern CPU architectures.
- **Strict Durability:** Write-Ahead Logging (WAL) ensures every transaction is persisted before confirmation.
- **Crash Consistency:** Automatic state recovery via snapshot loading and WAL replay.
- **Customizable:** Use the built-in `Operation` enum for standard financial transactions or define multi-step `Complex` atomic operations.
- **Thread-Safe:** Lock-free communication between pipeline stages.

## Architecture: Pipelined Execution

Roda-Ledger uses a "Core-Per-Stage" model, utilizing asynchronous pipelining and lock-free `ArrayQueue`s to eliminate
thread contention and maximize mechanical sympathy.

1. **Sequencer (Core 0):** The entry point. Assigns unique, monotonic IDs to incoming transactions and manages the
   ingestion interface.
2. **Transactor (Core 1):** The deterministic engine. Performs all business logic, balance checks, and state
   transitions. It maintains a hot cache of the ledger state in memory.
3. **WAL Storer (Core 2):** The durability layer. Responsible for writing transactions to the Write-Ahead Log (WAL) on
   disk.
4. **Snapshotter (Core 3):** The archival layer. Periodically takes snapshots of the ledger state to optimize recovery
   and manage WAL growth.

For a deep dive into the architecture, see [Design.md](docs/Design.md) and our [Architectural Decision Records](docs/adr/).

## Core Components

### Ledger

The `Ledger` is the primary interface. It coordinates the pipeline stages and provides methods for submitting
transactions and retrieving balances.

```rust
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::Operation;

let config = LedgerConfig {
    location: Some("ledger_data".to_string()),
    in_memory: false,
    ..Default::default()
};

let mut ledger = Ledger::new(config);
ledger.start();

// Submit a transaction (Deposit)
let account_id = 1;
let tx_id = ledger.submit(Operation::Deposit { 
    account: account_id, 
    amount: 100, 
    user_ref: 0 
});

// Wait for the transaction to be fully processed across all pipeline stages
ledger.wait_for_transaction(tx_id);

// Retrieve the resulting balance
let balance = ledger.get_balance(account_id);
```

### Operations

Roda-Ledger uses a concrete `Operation` enum for all interactions. It supports `Deposit`, `Withdrawal`, `Transfer`, and `Complex` multi-step atomic operations.

```rust
use roda_ledger::transaction::{Operation, ComplexOperation, Step, ComplexOperationFlags};
use smallvec::smallvec;

// 1. Simple Deposit
ledger.submit(Operation::Deposit { account: 1, amount: 1000, user_ref: 0 });

// 2. Simple Transfer
ledger.submit(Operation::Transfer { from: 1, to: 2, amount: 500, user_ref: 0 });

// 3. Complex Atomic Operation (e.g., Transfer with a Fee)
ledger.submit(Operation::Complex(Box::new(ComplexOperation {
    steps: smallvec![
        Step::Credit { account_id: 1, amount: 105 }, // Sender pays amount + fee
        Step::Debit  { account_id: 2, amount: 100 }, // Receiver gets amount
        Step::Debit  { account_id: 0, amount: 5   }, // System gets fee
    ],
    flags: ComplexOperationFlags::CHECK_NEGATIVE_BALANCE,
    user_ref: 12345,
})));
```

## Durability, Persistence, and Crash Recovery

Roda-Ledger is built to be "crash-safe" by design, ensuring that once a transaction is confirmed as committed, it will
never be lost.

- **Write-Ahead Log (WAL):** Every transaction is appended to a persistent, append-only log on disk before it is marked
  as `Committed`.
- **Snapshots:** To prevent the WAL from growing indefinitely and to speed up startup, the system periodically takes
  snapshots of all account balances.
- **Persistence:** You can choose between full persistence (on-disk) or high-performance in-memory mode via
  `LedgerConfig`.
- **Crash Recovery:** Upon restart, Roda-Ledger automatically:
    1. Identifies the latest consistent snapshot.
    2. Loads all balances from that snapshot.
    3. Replays all transactions from the WAL that occurred *after* the snapshot was taken.
    4. Resumes normal operation from the exact point where it left off.

## Benchmarks

Roda-Ledger is highly optimized for throughput and latency. Below are the results from the `wallet_bench`:

| Operation           | Latency (Avg) | Throughput (Avg)      |
|:--------------------|:--------------|:----------------------|
| **Wallet Deposit**  | 150.59 ns     | **6.64 Million tx/s** |
| **Wallet Transfer** | 165.61 ns     | **6.04 Million tx/s** |

*Benchmarks performed using Criterion.rs with persistence enabled (WAL active).*

## Stress Testing Performance

The system has been extensively stress-tested across various scenarios. Below are some highlights from the latest reports:

- **Maximum Throughput:** Reached up to **5.5 Million TPS** during high-contention stress tests.
- **Ultra-Low Latency:** Achieved as low as **102ns** mean latency during load ramp-up.
- **Sustained Performance:** Maintains over **650K TPS** with nanosecond-level latency in long-running stability tests.

For detailed reports and more scenarios, see the [Stress Testing Reports](docs/reporting/README.md).

## Getting Started

Add Roda-Ledger to your `Cargo.toml`:

```toml
[dependencies]
roda-ledger = { git = "https://github.com/tislib/roda-ledger" }
```

Check out the [examples](examples/) and [tests](tests/) directories for more examples of how to use the ledger and its features.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
