# Stress Testing Reports

This directory contains reports and raw data for various stress test scenarios performed on `roda-ledger`. These tests are designed to evaluate the system's performance, stability, and scalability under different load conditions.

## Purpose of Stress Testing

Stress testing aims to:
- Identify the maximum capacity (TPS) of the ledger.
- Measure transaction latencies under load.
- Observe system behavior and resource usage (CPU/Memory) during peaks and sustained load.
- Evaluate recovery speed after sudden spikes.
- Assess the performance impact of background operations like snapshotting and WAL management.

## Test Summary

| Scenario | Duration | TPS | Mean | p99 | p999 | Notes |
|----------|----------|-----|------|-----|------|-------|
| [Load Ramp](load_ramp.md) | 120.6s | 489,437 | 102ns | 270ns | 380ns | Gradually increases load |
| [Peak Load](peak.md) | 120.6s | 3,319,115 | 123ns | 170ns | 28.97µs | Constant high load |
| [Spike](spike.md) | 21.2s | 3,972,065 | 141ns | 150ns | 40.22µs | Sudden burst of transactions |
| [Spike Recovery](spike_recovery.md) | 120.6s | 2,139,939 | 205ns | 290ns | 46.88µs | Recovery after spike |
| [Account Scale](account_scale.md) | 120.6s | 4,612,645 | 135ns | 330ns | 450ns | Many accounts performance |
| [Hot Account Contention](hot_account_contention.md) | 120.7s | 5,378,702 | 116ns | 300ns | 390ns | High account contention |
| [Snapshot Impact](snapshot_impact.md) | 120.7s | 5,511,294 | 150ns | 340ns | 480ns | Background snapshotting overhead |
| [Sustain Load](sustain_load.md) | 1200.1s | 654,262 | 149ns | 390ns | 530ns | Long-running stability test |

## Test Scenarios

Each report document corresponds to a specific test scenario:

- **[Load Ramp](load_ramp.md):** Gradually increases the transaction rate to identify the system's breaking point and performance degradation characteristics.
- **[Peak Load](peak.md):** Tests the system's ability to handle high, but consistent, peak transaction volumes for a set duration.
- **[Spike](spike.md):** Applies a sudden, massive burst of transactions to test immediate system responsiveness.
- **[Spike Recovery](spike_recovery.md):** Measures how quickly the system returns to normal operation after a significant load spike.
- **[Account Scale](account_scale.md):** Evaluates performance as the total number of accounts in the system grows significantly.
- **[Mixed Workload](mixed_workload.md):** Simulates a realistic environment with a combination of different transaction types and sizes.
- **[Hot Account Contention](hot_account_contention.md):** Tests system performance when many transactions compete for the same accounts (lock contention).
- **[Snapshot Impact](snapshot_impact.md):** Measures the overhead and latency fluctuations caused by the snapshotting process.
- **[WAL Growth](wal_growth.md):** Observes performance as the Write-Ahead Log (WAL) grows over time.
- **[Sustain Load](sustain_load.md):** A long-running test to ensure stability and detect resource leaks under constant load.

## Report Outcomes

Each markdown report provides the following key metrics:

- **Summary:** Total duration and average Transactions Per Second (TPS) for compute, commit, and snapshot stages.
- **Latencies:** Detailed latency statistics including Mean, P50 (median), P99, P99.9, and P99.99 percentiles.
- **System Usage:** Current CPU and Memory consumption.
- **Metrics Over Time:** A time-series table showing TPS, latency, and resource usage throughout the test duration.

Raw data for each test is also available in `.json` format for further analysis.
