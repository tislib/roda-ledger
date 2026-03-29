# ADR-004: gRPC External Interface

**Status:** Proposed  
**Date:** 2026-03-29  
**Author:** Taleh Ibrahimli

## Context

roda-ledger currently exposes a custom binary TCP protocol (`src/protocol/`, `src/server/`,
`src/client/`) implemented over raw Tokio TCP streams. The protocol uses fixed-size
`bytemuck`-compatible structs (`ProtocolHeader`, `RegisterTransactionResponse`, etc.) with a
`serde_json` payload for `Operation`.

This works for the benchmark harness (`wallet_server_bench`) and internal testing, but blocks
external adoption:

**1. Protocol is opaque**  
There is no machine-readable API contract. Clients must read Rust source to understand the wire
format. `OperationKind::REGISTER_TRANSACTION`, `BATCH`, `GET_STATUS`, `GET_BALANCE` are defined
only in `src/protocol/definitions.rs`. There is no schema file.

**2. Cross-language usage requires reimplementing the protocol**  
The binary framing (8-byte `ProtocolHeader` + variable payload) and `serde_json`-encoded
`Operation` body must be re-implemented per client language. No tooling exists to generate this.

**3. Blocks Docker adoption**  
A Docker image without a language-agnostic interface requires users to write Rust or
reverse-engineer the binary protocol. The planned `docker run tislib/roda-ledger` is not usable
without this.

**4. No standard tooling**  
The current protocol has no equivalent of `grpcurl`, `grpcui`, or Postman support. Testing
requires writing a client from scratch.

---

## Decision

Replace the existing TCP binary protocol with **gRPC** as the sole external interface for the
standalone server mode. The TCP binary protocol (`src/protocol/`, `src/server/`, `src/client/`)
is removed entirely.

roda-ledger continues to support two usage modes:

**Library mode** — embed `roda-ledger` as a Rust crate. Interact directly with `Ledger` via the
`Operation` API. Zero network overhead, zero serialization cost. This is the mode used by
benchmarks, stress tests, and Rust applications embedding the ledger.

**Server mode** — run roda-ledger as a standalone Docker container. Interact via gRPC over
HTTP/2. This is the mode for cross-language clients, Docker deployments, and external users who
do not write Rust.

Both modes are first-class. The library API (`src/ledger.rs`, `src/transaction.rs`) is
unchanged. gRPC is a thin translation layer on top of it.

### All RPCs are unary

Every RPC in this API is unary — one request, one response. No server-side streaming, no
client-side streaming, no bidirectional streaming.

**Why not streaming:**

gRPC streaming operates per-message: each message sent or received triggers a Tokio wakeup.
At the throughput roda-ledger targets, streaming would produce millions of Tokio wakeups per
second on the gRPC layer alone — before any pipeline work is done. The Tokio async runtime is
not designed to handle millions of independent message events as a transport mechanism. This
overhead would make the gRPC layer the bottleneck, not the pipeline.

Batching solves the same problem correctly: the client groups N operations into one
`SubmitBatchRequest`, pays one round-trip, one proto deserialization, one Tokio task wakeup,
and receives N results in one `SubmitBatchResponse`. Throughput scales with batch size, which
the client controls. No streaming state machine, no flow control windows to tune, no
wakeup-per-message.

---

## Interface Design

The gRPC interface is designed for high-throughput financial operations while maintaining a simple mental model for clients.

### Key Principles

1. **Unary Only**: All RPCs are unary. No streaming is used to avoid Tokio wakeup overhead and complex flow control at the gRPC layer.
2. **Client-Side Batching**: High throughput is achieved by grouping multiple operations or IDs into a single request. This minimizes network round-trips and deserialization costs.
3. **Monotonic Transaction IDs**: Every successful submission receives a `transaction_id` which defines the global order of operations.
4. **Atomicity and Ordering**:
   - `SubmitOperation`: Each operation is atomic and strictly serialized.
   - `SubmitBatch`: Operations are executed in the order provided in the request. However, the batch itself is **not atomic** as a whole (failures are independent), and there is no guarantee of global contiguous order — operations from other clients may be interleaved between them.
5. **Explicit Pipeline Stages**: Clients can poll for the status of a transaction through strictly ordered stages: `PENDING` -> `COMPUTED` -> `COMMITTED` -> `ON_SNAPSHOT`.
6. **Zero-Sum Invariant**: All operations, including composite ones, must maintain a zero-sum balance across the system.
7. **Isolation Guarantees**:

| Guarantee | Provided | Notes |
| :--- | :--- | :--- |
| Atomicity | ✓ | Each operation all-or-nothing |
| Serializability | ✓ | Single-threaded Transactor, global serial order |
| Linearizability | optional | Poll until last_snapshot_tx_id >= tx_id |
| Durability | ✓ | WAL flush before COMMITTED |

### Core Operations

The interface supports:
- **Deposit/Withdrawal**: Moving funds in/out of the system.
- **Transfer**: Atomic movement between accounts.
- **Composite**: User-defined atomic multi-step operations with optional validation flags (e.g., overdraft protection).
- **Named**: Invoke a registered operation by name with positional `repeated int64` params. Ties to WASM roadmap — no breaking change when ADR-008 lands.
- **Queries**: Account balances (returning `last_snapshot_tx_id` to indicate currency/freshness) and transaction status polling.

The full protobuf definition is maintained in [proto/ledger.proto](../../proto/ledger.proto).

---

## Deployment Modes

### Library mode (unchanged)

```rust
// Cargo.toml
roda-ledger = { git = "https://github.com/tislib/roda-ledger" }

// Usage — zero network, zero serialization overhead
let mut ledger = Ledger::new(LedgerConfig::default());
ledger.start();
let tx_id = ledger.submit(Operation::Deposit { account: 1, amount: 1000, user_ref: 0 });
```

The `grpc` feature is opt-in. Library users pull zero gRPC dependencies and do not require
`protoc` in their build environment.

### Server mode (Docker)

```bash
docker run -p 50051:50051 -v ./data:/data tislib/roda-ledger

# Health check
grpcurl -plaintext localhost:50051 roda.ledger.v1.Ledger/GetPipelineIndex

# Submit a deposit
grpcurl -plaintext -d '{"deposit": {"account": "1", "amount": "1000", "user_ref": "0"}}' \
  localhost:50051 roda.ledger.v1.Ledger/SubmitOperation

# Submit a batch
grpcurl -plaintext -d '{"operations": [
  {"deposit":  {"account": "1", "amount": "500",  "user_ref": "1"}},
  {"transfer": {"from":    "1", "to":     "2",    "amount": "200", "user_ref": "2"}}
]}' localhost:50051 roda.ledger.v1.Ledger/SubmitBatch
```

Configuration via environment variables:

```
RODA_GRPC_ADDR           default: 0.0.0.0:50051
RODA_DATA_DIR            default: /data
RODA_MAX_ACCOUNTS        default: 1000000
RODA_SNAPSHOT_INTERVAL   default: 600 (seconds)
RODA_IN_MEMORY           default: false
```

---

## Removal of TCP Binary Protocol

`src/protocol/`, `src/server/`, `src/client/` are removed entirely. `wallet_server_bench` is
removed — it measured TCP throughput with a network layer, which is not a meaningful pipeline
ceiling. The direct library benchmark `wallet_bench` measures pipeline throughput correctly with
zero network overhead.

The TCP binary protocol served two purposes — external interface and benchmark harness. gRPC
replaces the external interface. The library API is the correct benchmark harness.

---

## Server Structure

```
src/
  grpc/
    mod.rs       — feature gate, pub mod
    server.rs    — GrpcServer struct, tonic::transport::Server setup, env config
    handler.rs   — LedgerService impl (generated tonic trait)
    mapping.rs   — proto ↔ Operation, proto ↔ TransactionStatus conversion
  bin/
    server.rs    — main() for Docker binary: reads env vars, starts GrpcServer
proto/
  ledger.proto
build.rs         — tonic_build::compile_protos("proto/ledger.proto")
```

`GrpcServer` wraps `Arc<Ledger>`. The handler maps proto messages to `Operation` via
`mapping.rs` and calls `ledger.submit()`. No new pipeline stage. No changes to `Ledger`,
`Transactor`, `Wal`, or `Snapshot`.

---

## Crate Additions

```toml
[dependencies]
tonic = { version = "0.12", features = ["transport"], optional = true }
prost = { version = "0.13", optional = true }

[features]
default = []
grpc    = ["dep:tonic", "dep:prost"]

[build-dependencies]
tonic-build = { version = "0.12", optional = true }
```

---

## Consequences

### Positive

- Proto file is the API contract — self-documenting, version-controlled, language-agnostic
- Auto-generated clients in Go, Python, Java, TypeScript with no manual protocol work
- `grpcurl` works immediately after `docker run`
- All RPCs unary — one Tokio task per call, no streaming wakeup overhead
- Client controls batch size via `SubmitBatch` and `GetBalances` — throughput scales naturally
- `GetTransactionStatuses` enables polling a bulk load without N individual round trips
- `GetBalance` and `GetBalances` return `last_snapshot_tx_id`, allowing clients to track exactly what transactions have been applied to the balance.
- `GetPipelineIndex` enables lag monitoring and health checks without per-transaction queries
- Library mode unchanged — zero overhead added for Rust users
- TCP protocol removal simplifies codebase — three source directories deleted
- `grpc` feature opt-in — zero dependency cost for library users

### Negative

- Protobuf serialization overhead vs direct library calls — expected, this is the external interface
- `protoc` required in Docker build image
- HTTP/2 framing overhead vs raw TCP — acceptable for an external interface
- No push notifications — callers poll `GetTransactionStatuses` for finality

### Neutral

- `wallet_server_bench` removed — `wallet_bench` already covers pipeline throughput correctly
- `FailReason` encoded as `uint32` in proto (u8 in Rust) — proto3 has no uint8, no data loss

---

## Alternatives Considered

**gRPC streaming for SubmitBatch**  
Evaluated. Rejected — streaming operates per-message, meaning one Tokio wakeup per operation.
At millions of transactions per second this makes the gRPC layer the bottleneck. Unary with
`repeated` gives the client control over batch size and reduces wakeups to one per batch.

**Keep TCP binary protocol alongside gRPC**  
Rejected — the binary protocol existed as external interface and benchmark harness. gRPC
replaces the external interface. The library API replaces the benchmark harness. Keeping both
adds maintenance overhead with no benefit.

**REST/HTTP JSON API**  
Rejected — no schema enforcement, no generated clients, more boilerplate for equivalent
batch semantics. No path to streaming-equivalent patterns if needed in future.

---

## References

- ADR-001 — entries-based execution model
- ADR-003 — `Operation` enum, `Composite`, `CompositeOperationFlags` (`u64` bitfield)
- `src/ledger.rs` — `Ledger::last_computed_id()`, `last_committed_id()`, `last_snapshot_id()`
- `src/transaction.rs` — `Operation`, `TransactionStatus`, `CompositeOperationFlags`
- `src/entities.rs` — `FailReason` codes
- `tonic` — https://github.com/hyperium/tonic