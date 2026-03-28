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
- **Customizable:** Easily define custom transaction logic and balance types.
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

For a deep dive into the architecture, see [Design.md](Design.md) and our [Architectural Decision Records](docs/adr/).

## Core Components

### Ledger

The `Ledger` is the primary interface. It coordinates the pipeline stages and provides methods for submitting
transactions and retrieving balances.

```rust
use roda_ledger::ledger::{Ledger, LedgerConfig};

let config = LedgerConfig {
    location: Some("ledger_data".to_string()),
    in_memory: false,
    ..Default::default ()
};

// Data: Your transaction data type
let mut ledger = Ledger::<Data>::new(config);
ledger.start();

// Submit a transaction
let account_id = 1;
let tx_id = ledger.submit(Transaction::new(Data::deposit(account_id, 100)));

// Wait for the transaction to be fully processed across all pipeline stages
ledger.wait_for_transaction(tx_id);

// Retrieve the resulting balance
let balance = ledger.get_balance(account_id);
```

### Example: Wallet

The `Wallet` is a built-in example application that demonstrates how to use the `Ledger` for common banking operations.

```rust
use roda_ledger::wallet::{Wallet, WalletConfig};

let mut wallet = Wallet::new_with_config(WalletConfig::default());
wallet.start();

// Simple API for financial operations
let tx_id1 = wallet.deposit(1, 1000);              // Deposit $1000 to account 1
let tx_id2 = wallet.transfer(1, 2, 500);           // Transfer $500 from account 1 to 2

// Ensure all operations are completed and reflected in the balance
// This waits until the pipeline stages have processed the transactions
wallet.wait_pending_operations();

// Retrieve account balances
let balance1 = wallet.get_balance(1);              // Result: 500
let balance2 = wallet.get_balance(2);              // Result: 500

// Check transaction status
let status = wallet.get_transaction_status(tx_id2);
if status.is_committed() {
    println!("Transfer was successful!");
} else if status.is_err() {
    println!("Transaction failed: {}", status.error_reason());
}
```

### Custom Transactions

You can define your own transaction logic by implementing the `TransactionDataType` trait. This allows the `Ledger` to
execute any deterministic logic you require.

```rust
use roda_ledger::transaction::{TransactionDataType, TransactionExecutionContext};
use roda_ledger::entities::{FailReason, TxEntry};
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, Default)]
pub struct MyTransaction {
    pub account_id: u64,
    pub amount: u64,
}

impl TransactionDataType for MyTransaction {
    fn process(
        &self,
        ctx: &mut impl TransactionExecutionContext,
    ) -> (FailReason, Vec<TxEntry>) {
        let mut balance = ctx.get_balance(self.account_id);
        balance += self.amount;
        ctx.update_balance(self.account_id, balance);
        (FailReason::NONE, Vec::new())
    }
}
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
