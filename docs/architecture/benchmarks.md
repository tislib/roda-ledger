# Performance Benchmarks

This document contains the latest performance benchmarks for Roda-Ledger.

**Date:** 2026-04-05
**Environment:** CX33 server at Hetzner

## Snapshot Performance

| Operation | Time (Avg) | Throughput (Avg) |
| :--- | :--- | :--- |
| `snapshot/process` | 123.64 ns | 8.0879 Melem/s |

## Transactor Performance

| Operation | Time (Avg) | Throughput (Avg) |
| :--- | :--- | :--- |
| `transactor/process` | 284.16 ns | 3.5191 Melem/s |


## Ledger Operations Performance

| Operation | Time (Avg) | Throughput (Avg) |
| :--- | :--- | :--- |
| `ledger/deposit_1000` | 302.73 ns | 3.3032 Melem/s |
| `ledger/deposit_1000000` | 337.59 ns | 2.9621 Melem/s |
| `ledger/deposit_10000000` | 350.05 ns | 2.8567 Melem/s |
| `ledger/deposit_50000000` | 577.87 ns | 1.7305 Melem/s |
| `ledger/complex_operation` | 538.06 ns | 1.8585 Melem/s |

## gRPC Performance

### Submit Operation (Unary)

| Operation | Time (Avg) |
| :--- | :--- |
| `grpc_submit_operation/unary_deposit` | 62.881 µs |

### Submit Batch

| Batch Size | Time (Avg) | Throughput (Avg) |
| :--- | :--- | :--- |
| 10 | 6.7757 µs | 147.59 Kelem/s |
| 100 | 840.84 ns | 1.1893 Melem/s |
| 1000 | 298.70 ns | 3.3478 Melem/s |

---
*Benchmarks performed with persistence enabled (WAL active) on CX33 server at Hetzner.*
