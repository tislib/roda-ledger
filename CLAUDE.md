# CLAUDE.md — roda-ledger

A programmable financial ledger engine: choose your consistency level per call, define custom
transaction logic as sandboxed WebAssembly, ~5.7M tx/s. Rust, edition 2024, MSRV 1.85.

> **Status nuance (read this):** The public `README.md` describes a *single-node* engine and marks
> cluster/raft as "planned." The code is ahead — the `cluster`, `raft`, `client`, and `control`
> crates implement Raft consensus + replication and are under **active development**. Recovery is
> mid-redesign on branch `feature/redesign-recover-process` (ADR-019/020/021 + ActiveSnapshot).
> Treat README's "single node" claims as marketing-lag, not ground truth — check the code.

## Mental model

- **Transactions are the source of truth; balances are derived state.** The WAL stores immutable
  Credit/Debit *entries* (not operations), so recovery and replication never re-execute logic (ADR-001).
- **Observable Staged Commitment.** A submit flows through a 4-stage pipeline, each stage on its own
  thread, connected by lock-free queues, each advancing a monotonic index the caller can observe:

  `submit() → [Sequencer] → [Transactor] → [WAL] → [Snapshotter] → get_balance()`
  `              PENDING       COMPUTED      COMMITTED   ON_SNAPSHOT`

  The caller picks how far to wait (fire-and-forget → durable → linearizable read). A background
  **Seal** thread is the 5th stage: it seals full segments and writes balance snapshots.
- **Single-threaded Transactor** = serializable writes with no locks. Linearizable reads are opt-in
  (wait for `snapshot_index`).
- **Zero-sum invariant**: every transaction nets to zero; enforced before WAL; non-zero WASM return
  codes roll back the whole tx.

## Operations

`Deposit`, `Withdrawal`, `Transfer`, and `Function` (invoke a registered WASM module as a first-class
atomic op — the only extension point). Accounts are `u64`, balances `i64`, account `0` is the system
source/sink. WASM host API is deliberately narrow (`credit`, `debit`, `get_balance`) and fully
deterministic so followers can replay entries without re-running code. See `docs/wasm-runtime.md`.

## Repository map (9-crate Cargo workspace)

| Crate | Path | Role |
|---|---|---|
| `ledger`  | `crates/ledger`  | **Core engine.** Pipeline, transactor, WAL driver, snapshot, seal, recovery, dedup, index, WASM runtime. Most logic lives here. |
| `storage` | `crates/storage` | Low-level durability: segmented WAL files, 40-byte records, CRC32C, fdatasync, LZ4 snapshots, term/vote logs. |
| `raft`    | `crates/raft`    | **Pure** Raft state machine — no async, no I/O, no upward deps. Emits actions / consumes events (ADR-017). |
| `cluster` | `crates/cluster` | Multi-node node: wires ledger + raft + storage; gRPC servers; election + replication drivers. Builds `roda-server` (default bin). |
| `proto`   | `crates/proto`   | tonic/prost gRPC defs: `ledger`, `node`, `fault`, `control` services (`.proto` under `crates/proto/proto`). |
| `client`  | `crates/client`  | gRPC client library. |
| `control` | `crates/control` | Control plane (web/gRPC) for multi-node scenarios; drives the `ui/`. |
| `ctl`     | `crates/ctl`     | Offline CLI (`roda-ctl`): pack/unpack/validate WAL segments. |
| `testing` | `crates/testing` | Lightweight test-scenario primitives (feature-gated). |

`cluster` is the default workspace member, so `cargo run` starts `roda-server`.

## Three-layer cluster architecture

`cluster` separates concerns as **up** (consensus + replication: `consensus/`), **down** (the ledger:
the `ledger` crate, wrapped in `LedgerSlot` = `Arc<ArcSwap<Ledger>>` for atomic reseed on divergence),
and **driver** (gRPC: `handlers/ledger_handler.rs` client API, `handlers/node_handler.rs` peer API).
`ClusterNode` (`node.rs`) owns everything and spawns the server + consensus threads; `Consensus`
(`consensus/state.rs`) owns the pure `RaftNode` plus durable term/vote state.

## Where things live (common tasks)

- Transaction flow: `ledger/src/{sequencer,transactor/runner,transactor/computer}.rs`
- Lock-free SPSC ring (transactor→WAL): `ledger/src/tx_ring/` (`TxRingWriter` writes, the WAL reads
  *and* releases — single releaser, ADR-021). See `crates/ledger/src/tx_ring/README.md`.
- WAL write/commit split: `ledger/src/wal.rs` (writer thread + committer/fdatasync thread, ADR-011).
- Snapshot read side: `ledger/src/snapshot.rs`; seal: `ledger/src/seal.rs`.
- Recovery: `ledger/src/recover.rs` (`Recover`, `ActiveSnapshot`); WAL-only, derived files are
  rebuildable (ADR-020/021).
- WAL record types (40 bytes each): `storage/src/entities.rs`; segment lifecycle: `storage/src/segment.rs`.
- Pipeline indexes / queues: `ledger/src/pipeline.rs`.

## Build / test / run

```bash
# Full local gate (matches CI) — needs cargo-nextest + protoc installed:
./scripts/check.sh
# which runs:
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --cargo-profile ci --all-features
cargo test --doc --profile ci

cargo run                                   # build + run roda-server (cluster) with ./config.toml
cargo build --release -p cluster --bin roda-server
cargo build -p ctl --bin roda-ctl          # offline WAL tool

cargo bench -p ledger                       # criterion benches (also: -p storage)
cargo bench -p ledger --bench transactor_bench

# fault-injection feature (deterministic chaos, ADR-018):
cargo nextest run --workspace --features fault-injection
```

Docker quick start: `docker run -p 50051:50051 -v $(pwd)/data:/app/data tislib/roda-ledger:latest`.

## Conventions (house style — match existing code)

- **Comments: 1–2 lines max.** Default to none; never write a paragraph comment.
- **Short, named helper methods.** Decompose long nested functions; an outcome-enum is usually the
  cleanest helper boundary. No giant `run()` bodies.
- **RAII / drop-as-shutdown.** Lifecycle via `Drop`, not explicit `shutdown()` methods; `Drop` awaits
  tasks cooperatively — it does not abort them.
- **Event-driven freshness over periodic tickers.** Refresh derived state through a named primitive
  called at each read site, not a background timer.
- **Replication throughput rules.** Never sleep while WAL is pending; sender and receiver run as
  parallel tasks after handshake.
- **Hot-path buffers stay in callbacks.** Don't return a hot-path buffer ref out of its owning module;
  pass it into a callback and keep the consumer dumb (e.g. tx-ring `WalRecord` → WAL `read_batch`).
- `raft` crate must stay pure: no `tokio`/`tonic`/`ledger` deps (it's the consensus state machine).

## Documentation & ADRs

Read in order: `README.md` → `docs/01-concepts.md` → `docs/02-api.md` → `docs/03-architecture.md`
→ `docs/internal.md` → `docs/wasm-runtime.md`. Perf numbers: `docs/load.md`.
(Note: README links `docs/04-internals.md`, but the file on disk is `docs/internal.md`.)

ADRs live in `docs/adr/` (`docs/adr/README.md` indexes them, though the index table lags behind disk —
ADRs 021/022 exist but aren't listed). Status ≠ implementation state; the most load-bearing
**implemented** ones:

- **001** entries-based execution · **002** Vec balance storage · **006** WAL/snapshot/seal durability
- **009** tx links + dedup · **011** WAL write/commit split · **019** transaction ring
- **020** trailer/commit-record WAL layout · **021** WAL sole releaser, snapshot tails durable WAL

Cluster/raft (015/016/017), WASM registry (014), account layouts (022), and index/query (008) are
where doc status and code most diverge — verify against source, not the ADR status field.

## ADR-based development

This project is **ADR-driven**: every significant design decision is captured as a numbered ADR in
`docs/adr/` before (or alongside) the code that implements it, and the code points back to it. ADRs
are the design record — rationale lives here, not only in commit messages.

- **ADRs are an append-only log, not living docs.** You don't rewrite an accepted ADR when the design
  evolves — you write a *new* ADR that **Amends** the old one. E.g. ADR-021 amends ADR-019 and ADR-006;
  each ADR's header lists what it `Amends` and a `References` footer links the chain. To understand a
  subsystem, read its ADR *and* every later ADR that amends it.
- **Template** (see any file in `docs/adr/`): `Status` · `Date` · `Author` · optional `Amends` →
  `Context` (the problem/forces) → `Decision` (what we do + the invariants it establishes) →
  `Consequences` (Positive / Negative / Neutral, often with load-test numbers) → `References`.
- **Statuses**: `Proposed` (decision agreed, maybe not built) · `Accepted` (the standing decision) ·
  `Postponed` (parked) · `—` (draft). Status describes the *decision*, not implementation progress.
- **Code cites ADRs by number and section** — 100+ refs like `// Per ADR-0017 §"Required Invariants" #7`
  or `(ADR-022 §8)`. **Before changing such code, read the cited ADR** so you don't silently break an
  invariant it depends on.
- **Making a non-trivial design decision yourself?** Add a new ADR (next number, template above) or
  amend an existing one, link it from `docs/adr/README.md`, and reference it from the code comment —
  match the existing house style.
