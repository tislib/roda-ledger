# Introduction

Roda-Ledger is a high-performance, durable, and crash-consistent financial ledger and transaction executor built in Rust, capable of processing **6.6 Million+ transactions per second** with **nanosecond-level latency**.

## What is Roda-Ledger?

Roda-Ledger is a specialized database engine designed for recording and executing financial transactions with industry-leading performance. It serves as a high-performance "source of truth" for account balances, providing strict durability, deterministic execution, and the reliability required for mission-critical financial systems.

## Target Use Cases

- **Core Banking**: High-volume retail and investment banking ledger systems.
- **Crypto-Exchanges**: Real-time matching engines and wallet management.
- **Payment Gateways**: High-volume transaction clearing and settlement.
- **Gaming Economies**: Managing in-game currencies and item trades at scale.

## Why Roda-Ledger?

Roda-Ledger is built for scale. While traditional databases struggle with the lock contention of high-frequency ledger updates, Roda-Ledger's architecture allows it to outpace everything in its class.

### Strengths

- **Industry-Leading Throughput**: Process over **6.6 Million transactions per second** (TPS).
- **Predictable Low Latency**: Nanosecond to microsecond level execution times.
- **Strict Durability**: Every transaction is persisted via Write-Ahead Logging (WAL) before confirmation.
- **Atomic Composite Operations**: Perform multi-step transfers (e.g., Transfer + Fee) as a single atomic unit.
- **Crash Consistency**: Automatic state recovery from snapshots and WAL replay after a crash.
- **Flexible Integration**: Run as a standalone gRPC server or embed it as a Rust library.

### Trade-offs

- **Single-Node Focus**: Optimized for vertical scaling; currently operates as a single-leader instance.
- **Memory-Bound**: For peak performance, the "hot" state (account balances) should fit in RAM.
- **Specialized Engine**: Not a general-purpose database; designed specifically for financial ledger operations.

## Key Features

- **Pipelined Architecture**: Separates Sequencing, Execution, Persistence, and Snapshotting into dedicated CPU cores to eliminate lock contention.
- **Zero-Sum Invariant**: Ensures that value is never created or destroyed; every credit is balanced by a corresponding debit.
- **Complex Operations**: Support for `Composite` operations allowing complex business logic within a single transaction.
- **Lock-Free Communication**: Uses high-speed `ArrayQueue`s for inter-stage communication.

## Performance

| Operation | Latency (Avg) | Throughput (Avg) |
| :--- | :--- | :--- |
| **Wallet Deposit** | 198 ns | **5.0 Million tx/s** |
| **Wallet Transfer** | 151 ns | **6.6 Million tx/s** |

*Benchmarks performed with persistence enabled (WAL active). Full report: [Benchmarks](architecture/benchmarks.md)*

## License

Apache License 2.0.
