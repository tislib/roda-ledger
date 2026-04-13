# ADR-012: End-to-End Testing Strategy and Correctness Framework

**Status:** Proposed  
**Date:** 2026-04-06  
**Author:** Taleh Ibrahimli  

---

## Context

roda-ledger has integration tests that verify individual components in-process.
These are fast and run on every CI push. However, they cannot verify system-level
correctness properties that only manifest in a real running process:

- WAL recovery after crash — does the ledger restore to exactly the committed
  state after a kill -9?
- Segment rotation correctness — do WAL segments seal, rotate, and recover
  correctly under load?
- Double-entry invariant — do all account balances sum to zero across millions
  of transactions?
- WAL integrity — are all records CRC-valid after a long run?
- OOMKill recovery — does the ledger recover correctly after an out-of-memory kill?
- Runtime degradation — does committed throughput remain correct under CPU or
  disk pressure?

A second concern is future Raft cluster testing. The E2E framework must support
multi-node scenarios without requiring the framework itself to be redesigned.
Node count must be a first-class configuration parameter from the start.

---

## Decision

Introduce a dedicated E2E test suite under `tests/e2e/` that:

- Runs against real roda-ledger server processes (not in-process)
- Is excluded from normal CI — triggered manually
- Uses a macro-based DSL for high-level test definition — tests read as
  specifications, not implementations
- Separates server configuration (profiles) from runtime intervention
  (dynamic constraints applied during test execution)
- Supports multiple backends for flexibility across environments
- Is designed to accommodate multi-node Raft scenarios without framework
  changes — only the profile and backend change

---

## Execution

E2E tests are excluded from normal CI. They are triggered manually via GitHub
Actions `workflow_dispatch`. The infrastructure follows the same schema as the
existing load test workflow — a dedicated server is provisioned per run,
tests execute, results are collected, server is destroyed. The backend
determines where nodes run (see Backend section below).

---

## Backend

The backend controls how roda-ledger nodes are started and managed during a
test run. It is selected at test time, not at compile time. This allows the
same test scenarios to run in different environments for debugging, analysis,
and production-parity validation.

```rust
pub enum E2EBackend {
    /// Nodes run in-process alongside the test.
    /// Fastest startup, easiest debugging.
    /// For local development and bug reproduction only.
    Inline,

    /// Each node runs as a separate OS process on the same machine as the test.
    /// Default mode — fast setup, no Docker required, suitable for CI.
    Process,

    /// Each node runs in a Docker container on the same machine.
    /// Closer to production — real network stack, real filesystem isolation.
    Docker,

    /// Each node runs on a separate physical/cloud server.
    /// Planned for future use — not implemented.
    /// Intended for Raft testing under real network conditions.
    Cloud,
}
```

Backend is specified at runtime — via environment variable or CLI argument —
not hardcoded in the test. The same test scenario runs against any backend
without modification.

`Process` is the default — no extra tooling required, fast iteration, suitable
for CI runners. `Docker` is used when production-parity is needed. `Inline` is
reserved for debugging only. `Cloud` is planned for Raft cluster testing under
real network partitions.

---

## Server Profiles

A profile describes how many nodes to start. Persistence is always enabled.
Raft is always the consensus protocol (once implemented). Profile = node count.

```toml
# tests/e2e/profiles.toml

[single_node]
nodes = 1

[three_node_cluster]
nodes = 3
```

Profiles are declared once and referenced via the `#[e2e_test]` attribute:

```rust
#[e2e_test(profile = "single_node")]
async fn test_crash_recovery(ctx: &E2EContext) { ... }

#[e2e_test(profile = "three_node_cluster")]
async fn test_leader_failover(ctx: &E2EContext) { ... }
```

The `E2EContext` (`ctx`) is the single handle passed to every macro. It knows
about all running nodes, their clients, their data directories, and their
runtime state. Adding Raft means `ctx` manages N processes or containers
instead of 1 — the macro interface does not change.

---

## Test Structure

```
tests/
  e2e/
    mod.rs            ← E2EContext, backend abstraction, profile loading
    correctness.rs    ← balance invariants, WAL checksums, double-entry
    crash.rs          ← crash recovery, random crash points
    durability.rs     ← committed survives kill -9
    rotation.rs       ← segment rotation under load
    oom.rs            ← OOMKill recovery
    long_run.rs       ← sustained load, memory stability
    inspection.rs     ← WAL segment deep reads, pipeline state

  e2e/
    profiles.toml     ← server profile definitions
```

---

## DSL — Macro Categories

All test logic is expressed through macros. Tests read as high-level
specifications. Implementation complexity is hidden in the macro layer.

`ctx` is always the first argument. `node: N` (default: 0) targets a specific
node — meaningful today for kill/restart, essential for Raft multi-node tests.

### 1. Actions

```rust
deposit!(ctx, account: 1, amount: 1000, wait: committed);
withdraw!(ctx, account: 1, amount: 500, wait: committed);
transfer!(ctx, from: 1, to: 2, amount: 200, wait: committed);
batch_deposit!(ctx, account: 1, amount: 100, count: 10_000, wait: committed);
```

### 2. Verifications

```rust
assert_balance!(ctx, account: 1, eq: 1000);
assert_balance_sum!(ctx, eq: 0);    // double-entry: all balances net to zero
assert_wal_checksum!(ctx);
assert_wal_valid!(ctx, node: 0);
assert_tx_status!(ctx, tx_id, committed);
```

### 3. Reading

```rust
let balance          = get_balance!(ctx, account: 1);
let tx               = get_transaction!(ctx, tx_id);
let committed_id     = get_last_committed_id!(ctx, node: 0);
let segments         = get_segments!(ctx, node: 0);
```

### 4. Runtime Intervention

Dynamic constraints applied to a running node during test execution.

```rust
kill!(ctx, node: 0);
restart!(ctx, node: 0);
kill_and_restart!(ctx, node: 0);

slow_cpu!(ctx, node: 0, factor: 10);
slow_disk!(ctx, node: 0, latency_ms: 100);
limit_memory!(ctx, node: 0, mb: 64);
restore!(ctx, node: 0);

wait_ms!(500);
wait_until_committed!(ctx, tx_id);
```

### 5. Deep Inspection

Inspect WAL segments, pipeline state, and storage internals directly — without
going through the gRPC API.

```rust
assert_segment_sealed!(ctx, node: 0, segment_id: 4);
assert_segment_count!(ctx, node: 0, gte: 3);
assert_snapshot_valid!(ctx, node: 0);
assert_pipeline_caught_up!(ctx, node: 0);
assert_index_valid!(ctx, node: 0);
```

---

## Example per Category

One representative example per macro category to illustrate the DSL style.
Full test scenarios live in the test files, not in this ADR.

**Action + Verification:**
```rust
#[e2e_test(profile = "single_node")]
async fn deposit_committed_reflects_in_balance(ctx: &E2EContext) {
    deposit!(ctx, account: 1, amount: 1000, wait: committed);
    assert_balance!(ctx, account: 1, eq: 1000);
}
```

**Runtime Intervention:**
```rust
#[e2e_test(profile = "single_node")]
async fn committed_survives_kill9(ctx: &E2EContext) {
    deposit!(ctx, account: 1, amount: 1000, wait: committed);
    kill_and_restart!(ctx, node: 0);
    assert_balance!(ctx, account: 1, eq: 1000);
}
```

**Deep Inspection:**
```rust
#[e2e_test(profile = "single_node")]
async fn segment_rotation_produces_sealed_segments(ctx: &E2EContext) {
    batch_deposit!(ctx, account: 1, amount: 1, count: 5_000_000, wait: committed);
    assert_segment_count!(ctx, node: 0, gte: 3);
    assert_wal_valid!(ctx, node: 0);
}
```

**Multi-node (future Raft):**
```rust
#[e2e_test(profile = "three_node_cluster")]
async fn leader_failover_preserves_committed(ctx: &E2EContext) {
    deposit!(ctx, account: 1, amount: 1000, wait: committed);
    kill!(ctx, node: 0);   // kill leader
    wait_until_leader!(ctx);
    assert_balance!(ctx, account: 1, eq: 1000);
}
```

---

## Consequences

### Positive

- E2E tests excluded from normal CI — zero impact on PR velocity
- Backend abstraction allows the same test to run in different environments
  without modification — `Inline` for debugging, `Process` for CI,
  `Docker` for production parity, `Cloud` for Raft
- Server profiles are forward-compatible — adding Raft = adding `nodes = 3`
  profile, no framework changes
- `ctx` handle abstracts node count — multi-node Raft tests use the same macros
- Macro DSL hides infrastructure complexity — tests read as specifications
- Deep inspection macros verify properties invisible to the gRPC API
- Runtime intervention models real failure conditions, not mocked ones

### Negative

- Runtime intervention macros (`slow_disk`, `limit_memory`) require elevated
  permissions — Linux cgroups needs `CAP_SYS_ADMIN` or Docker `--privileged`
- `Inline` backend cannot test WAL recovery or crash behavior — limited to
  functional correctness only
- `Cloud` backend is not implemented — Raft network partition testing deferred
- Proc macro implementation for `#[e2e_test]` is non-trivial — start with
  regular `#[tokio::test]` + helper functions, migrate when DSL stabilizes

### Neutral

- `Process` is the default backend — no extra tooling, fast iteration
- `profiles.toml` is the single source of truth for node configuration
- Backend is a runtime selection — same binary, different environment

---

## Alternatives Considered

**JSON/YAML scenario files**
Rejected — insufficient expressiveness for loops, random crash points, and
conditional assertions. Rust macros give full language power with comparable
readability.

**Python/shell scripts**
Rejected — separate language, separate toolchain, no compile-time validation,
harder to maintain alongside the Rust codebase.

**Single backend**
Rejected — different environments expose different classes of bugs. `Process`
is fast but cannot isolate filesystem or network. `Docker` catches containerization
issues. Abstracting the backend from the test is strictly better than hardcoding.

**Running E2E in normal CI**
Rejected — heavy scenarios take 30-60 minutes. Running on every PR would make
CI unusable.

---

## References

- ADR-001 — WAL record format, CRC (assert_wal_checksum)
- ADR-006 — WAL segment lifecycle (assert_segment_sealed, assert_snapshot_valid)
- ADR-007 — CLI tools (roda-ctl verify used in deep inspection)
- ADR-008 — Transaction index (assert_index_valid)
- ADR-010 — WaitLevel (committed/processed/snapshotted in action macros)
- `.github/workflows/load-test.yml` — Hetzner provisioning schema reused for Cloud backend
- `tests/e2e/profiles.toml` — server profile definitions
- `tests/e2e/mod.rs` — E2EContext, backend abstraction, profile loading