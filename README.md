# Roda-Ledger

### 🚀 Ultra-High Performance Ledger for Modern Finance

A high-performance, durable, and crash-consistent financial ledger and transaction executor built in Rust, capable of processing **6.6 Million+ transactions per second** with **nanosecond-level latency**.

---

## What is Roda-Ledger?

Roda-Ledger is a specialized database engine designed for recording and executing financial transactions with industry-leading performance. It serves as a high-performance "source of truth" for account balances, providing strict durability, deterministic execution, and the reliability required for mission-critical financial systems.

### Target Use Cases
- **Core Banking**: High-volume retail and investment banking ledger systems.
- **Crypto-Exchanges**: Real-time matching engines and wallet management.
- **Payment Gateways**: High-volume transaction clearing and settlement.
- **Gaming Economies**: Managing in-game currencies and item trades at scale.

---

## Why Roda-Ledger?

Roda-Ledger is built for scale. While traditional databases struggle with the lock contention of high-frequency ledger updates, Roda-Ledger's architecture allows it to outpace everything in its class.

### The "Plus" (Strengths)
- **🚀 Industry-Leading Throughput**: Process over **6.6 Million transactions per second** (TPS) on CX33 server at Hetzner.
- **⏱️ Predictable Low Latency**: Nanosecond to microsecond level execution times, ensuring your system never bottlenecks.
- **💾 Strict Durability**: Every transaction is persisted via Write-Ahead Logging (WAL) before confirmation—zero data loss.
- **⚛️ Atomic Composite Operations**: Perform multi-step transfers (e.g., Transfer + Fee) as a single atomic unit.
- **🔄 Crash Consistency**: Automatic state recovery from snapshots and WAL replay after a crash.
- **🛠️ Flexible Integration**: Run as a standalone gRPC server or embed it as a Rust library.

### The "Minus" (Trade-offs)
- **Single-Node Focus**: Optimized for vertical scaling; currently operates as a single-leader instance.
- **Memory-Bound**: For peak performance, the "hot" state (account balances) should fit in RAM.
- **Specialized Engine**: Not a general-purpose database; designed specifically for financial ledger operations.

---

## ⚡ Quick Start: Server Mode (Docker)

The fastest way to deploy Roda-Ledger is using the official Docker image.

### 1. Run the Server
```bash
docker run -p 50051:50051 -v $(pwd)/data:/data tislib/roda-ledger:latest
```

### 2. Interact via gRPC
Use `grpcurl` to submit operations:

**Deposit Funds:**
```bash
grpcurl -plaintext -d '{"deposit": {"account": 1, "amount": "1000", "user_ref": "123"}}' \
  localhost:50051 roda.ledger.v1.Ledger/SubmitOperation
```

**Get Balance:**
```bash
grpcurl -plaintext -d '{"account_id": 1}' \
  localhost:50051 roda.ledger.v1.Ledger/GetBalance
```

---

## 🦀 Quick Start: Library Mode (Rust)

Integrate Roda-Ledger directly into your Rust application for maximum performance.

### 1. Add Dependency
Add this to your `Cargo.toml`:
```toml
[dependencies]
roda-ledger = { git = "https://github.com/tislib/roda-ledger" }
```

### 2. Basic Usage
```rust
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::Operation;

fn main() {
    // Initialize with default config
    let mut ledger = Ledger::new(LedgerConfig::default());
    ledger.start();

    // Submit a deposit
    let tx_id = ledger.submit(Operation::Deposit { 
        account: 1, 
        amount: 1000, 
        user_ref: 0 
    });

    // Wait for the transaction to be persisted (WAL)
    ledger.wait_for_transaction(tx_id);

    // Retrieve balance
    let balance = ledger.get_balance(1);
    println!("Account 1 balance: {}", balance);
}
```

---

## Key Features

- **Pipelined Architecture**: Separates Sequencing, Execution, Persistence, and Snapshotting into dedicated CPU cores to eliminate lock contention.
- **Zero-Sum Invariant**: Ensures that value is never created or destroyed; every credit is balanced by a corresponding debit.
- **Complex Operations**: Support for `Composite` operations allowing complex business logic within a single transaction.
- **Lock-Free Communication**: Uses high-speed `ArrayQueue`s for inter-stage communication.

## Architecture

Roda-Ledger utilizes a **Core-Per-Stage** model to maximize mechanical sympathy:
1. **Sequencer**: Assigns monotonic IDs and manages ingestion.
2. **Transactor**: Single-threaded deterministic execution engine (No locks needed!).
3. **WAL Storer**: Handles high-speed disk persistence.
4. **Snapshotter**: Periodically captures state for fast recovery.

Detailed docs: [Benchmarks](docs/benchmarks.md) | [Architecture & Design](docs/Design.md) | [Consistency Model](docs/operations_consistency.md)

## Performance Benchmarks

| Operation | Latency (Avg) | Throughput (Avg) |
| :--- | :--- | :--- |
| **Wallet Deposit** | 198 ns | **5.0 Million tx/s** |
| **Wallet Transfer** | 151 ns | **6.6 Million tx/s** |

*Benchmarks performed with persistence enabled (WAL active) on CX33 server at Hetzner. Full report: [docs/benchmarks.md](docs/benchmarks.md)*

---

## The Road Ahead

Roda-Ledger is evolving from a high-performance core into a full-featured distributed financial infrastructure.

- **🌐 Distribution via Raft**: High availability and horizontal read scalability through a Raft-based cluster mode.
- **🏗️ Account Hierarchies**: Support for complex sub-account structures and parent-child balance aggregation.
- **⚡ WASM Runtime**: Sandbox for user-defined transaction logic, allowing custom business rules to run at native speeds.
- **🛡️ Extreme Resilience**: Continued hardening with advanced crash-simulations (OOMKill, MultiCrash) and checksum-validated recovery.

---

## License
Apache License 2.0. See [LICENSE](LICENSE) for details.
