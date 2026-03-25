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
| **Sustain Load** | 1200s | 100K | 173ns | 520ns | 650ns | Performance stabilizes as the system warms up. |
| **WAL Growth** | 600s | 1.3M | 316ns | 670ns | 2.09µs | Zero throughput impact as the Write-Ahead Log grows. |
| **Snapshot Impact** | 600s | 1.4M | 351ns | 660ns | 1.85µs | Background snapshotting has negligible latency impact. |
| **Spike** | 21s | 1.5M | 45ns | 140ns | 200ns | Handles massive transaction bursts without queue build-up. |
| **Spike Recovery** | 120s | 1.3M | 46ns | 160ns | 350ns | Rapid recovery and queue drainage after a load spike. |
| **Peak Load** | 120s | 1.3M | 4.3µs | 280ns | 632µs | Maintains high capacity during sustained peak loads. |
| **Load Ramp** | 120s | Gradual | 149ns | 410ns | 600ns | Linear scaling with predictable latency as volume increases. |
| **Mixed Workload** | 600s | 1.3M | 284ns | 550ns | 1.37µs | Optimal handling of diverse transaction types. |
| **Hot Account Contention** | 600s | 1.3M | 310ns | 590ns | 1.5µs | Minimal performance impact from high-traffic account access. |
| **Account Scale** | 600s | 1.3M | 423ns | 490ns | 1.45µs | Maintains performance as the total number of accounts grows. |

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
