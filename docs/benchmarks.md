# Performance Benchmarks

This document contains the latest performance benchmarks for Roda-Ledger.

**Date:** 2026-03-29
**Environment:** CX33 server at Hetzner

## Snapshot Performance

| Operation | Time (Avg) | Throughput (Avg) |
| :--- | :--- | :--- |
| `snapshot/process` | 123.64 ns | 8.0879 Melem/s |

## Transactor Performance

| Operation | Time (Avg) | Throughput (Avg) |
| :--- | :--- | :--- |
| `transactor/process` | 284.16 ns | 3.5191 Melem/s |

## WAL (Write-Ahead Log) Performance

| Operation | Time (Avg) | Throughput (Avg) |
| :--- | :--- | :--- |
| `wal_in_memory/append` | 18.237 ns | 54.835 Melem/s |
| `wal_on_disk/append` | 40.873 ns | 24.466 Melem/s |

## Ledger Operations Performance

| Operation | Time (Avg) | Throughput (Avg) |
| :--- | :--- | :--- |
| `ledger/deposit` | 197.94 ns | 5.0520 Melem/s |
| `ledger/transfer_1000` | 172.18 ns | 5.8080 Melem/s |
| `ledger/transfer_1000000` | 150.53 ns | 6.6432 Melem/s |
| `ledger/transfer_10000000` | 206.21 ns | 4.8493 Melem/s |
| `ledger/transfer_50000000` | 219.65 ns | 4.5526 Melem/s |
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
