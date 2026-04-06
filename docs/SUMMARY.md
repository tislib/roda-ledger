# Summary

[Introduction](introduction.md)

---

# Getting Started

- [Server Mode (Docker)](getting-started/server-mode.md)
- [Library Mode (Rust)](getting-started/library-mode.md)

# Architecture

- [Design & Pipeline](architecture/design.md)
- [Consistency Model](architecture/consistency.md)
- [Benchmarks](architecture/benchmarks.md)

# Guides

- [gRPC API Reference](guides/grpc-api.md)

# Architecture Decision Records

- [Overview](adr/README.md)
- [ADR-001: Entries-Based Execution Model](adr/0001-entries-based-execution-model.md)
- [ADR-002: Vec-Based Balance Storage](adr/0002-vec-based-balance-storage.md)
- [ADR-003: Ledger API Redesign](adr/0003-ledger-api-redesign.md)
- [ADR-004: gRPC External Interface](adr/0004-grpc-interface.md)
- [ADR-005: Adaptive Pipeline Execution Mode](adr/0005-pressure-manager.md)
- [ADR-006: WAL, Snapshot, and Seal Durability](adr/0006-wal-snapshot-durability.md)
- [ADR-007: CLI Operational Tools](adr/0007-cli-tools.md)
- [ADR-008: Transaction Index and Query Serving](adr/0008-transaction-account-index.md)
- [ADR-009: Transaction Links, Deduplication, and Reversal](adr/0009-tx-link-dedup-reversal.md)

# Stress Test Reports

- [Overview](reporting/README.md)
- [Load Ramp](reporting/load_ramp.md)
- [Peak Load](reporting/peak.md)
- [Spike](reporting/spike.md)
- [Spike Recovery](reporting/spike_recovery.md)
- [Account Scale](reporting/account_scale.md)
- [Hot Account Contention](reporting/hot_account_contention.md)
- [Snapshot Impact](reporting/snapshot_impact.md)
- [Sustain Load](reporting/sustain_load.md)
