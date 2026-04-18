# WASM Runtime — Programmable Ledger

roda-ledger lets you extend the transaction set at runtime with sandboxed WebAssembly functions. A registered function becomes a first-class `Operation::Function`: it is sequenced, executed atomically by the Transactor, and produces normal `TxEntry` records in the WAL alongside every other transaction.

This document covers everything about writing, registering, invoking, and operating programmable functions: ABI, lifecycle, storage layout, recovery guarantees, and gRPC surface.

The design is specified in [ADR-014](./adr/0014-wasm-function-registry.md).

---

## Table of contents

- [When to use it](#when-to-use-it)
- [Function ABI](#function-abi)
- [Host API](#host-api)
- [Return values & rollback](#return-values--rollback)
- [Writing a function](#writing-a-function)
  - [Rust](#rust)
  - [AssemblyScript](#assemblyscript)
  - [Hand-written WAT](#hand-written-wat)
- [Registering a function](#registering-a-function)
  - [gRPC](#grpc)
  - [Rust library](#rust-library)
- [Invoking a function](#invoking-a-function)
- [Listing & unregistering](#listing--unregistering)
- [Versioning](#versioning)
- [Storage layout on disk](#storage-layout-on-disk)
- [Durability & recovery](#durability--recovery)
- [Determinism & isolation](#determinism--isolation)
- [Performance](#performance)
- [Limits & validation](#limits--validation)
- [Troubleshooting](#troubleshooting)

---

## When to use it

Use a function when the built-in operations aren't enough:

- **Fee splits** — deduct a fee to one account, forward the net to another, in one atomic transaction.
- **Multi-leg settlements** — move value between three or more accounts with custom rules (e.g. escrow release + platform cut + counterparty payout).
- **Conditional logic** — decline the transaction when a business rule fails (insufficient collateral, failed KYC flag, overdraft policy).
- **Accounting templates** — reusable "compliance check", "tax withholding", "loyalty accrual" rules shared across submitters.

If your operation is expressible as one of `Deposit`, `Withdrawal`, or `Transfer`, use those — they are faster and need no registration step.

---

## Function ABI

Every registered function must export **one** symbol named `execute` with a fixed signature:

```
execute(i64, i64, i64, i64, i64, i64, i64, i64) -> i32
```

- Exactly **8** `i64` positional parameters. Unused slots are conventionally passed as `0`.
- Exactly **1** `i32` return value, interpreted as a `u8` status code:
  - `0` — success (the transaction's host-side credits / debits are committed);
  - `1..=127` — standard failure reasons (defined by the ledger, full rollback);
  - `128..=255` — user-defined custom reasons (full rollback).

The fixed arity eliminates any need for linear memory pointer / length passing: account ids, amounts, rates, flags, timestamps all fit into 8 `i64` slots.

---

## Host API

A function can only call **three** host imports. All three live in the `ledger` module:

```
(import "ledger" "credit"      (func (param i64 i64)))
(import "ledger" "debit"       (func (param i64 i64)))
(import "ledger" "get_balance" (func (param i64) (result i64)))
```

| Host call | Signature | Effect |
|-----------|-----------|--------|
| `credit(account_id, amount)` | `(u64, u64) -> ()` | Decreases `account_id`'s balance by `amount`. Cannot fail individually. |
| `debit(account_id, amount)`  | `(u64, u64) -> ()` | Increases `account_id`'s balance by `amount`. Cannot fail individually. |
| `get_balance(account_id)` | `(u64) -> i64` | Reads the current balance (signed). |

Convention in roda-ledger: **credit subtracts, debit adds.** A `Transfer { from, to, amount }` is `credit(from, amount)` + `debit(to, amount)` — credits move value *out* of an account, debits move value *in*. Same convention applies to WASM functions.

**Zero-sum invariant.** The ledger verifies `sum(credits) == sum(debits)` after the function returns. If the function emits an unbalanced set of credits / debits, the transaction is rejected and rolled back with `Status::ZERO_SUM_VIOLATION`.

**Overdraft policy.** Host credits / debits never fail individually, and the ledger does not enforce negative-balance checks inside functions. If you need overdraft protection, call `get_balance` and return a non-zero status to trigger rollback:

```rust
if get_balance(sender) < amount as i64 {
    return 1; // INSUFFICIENT_FUNDS
}
```

No randomness, no wall clock, no I/O, no WASM threads, no atomics. The ABI is deliberately small so every execution is deterministic.

---

## Return values & rollback

The `execute` return value is the transaction's final status:

- `0` — the host-side credits and debits become part of the WAL transaction; the zero-sum invariant is verified; if it holds, the transaction commits.
- any non-zero value — the transaction is **fully rolled back**: every credit / debit the function applied is reversed in the same way a failed built-in operation would be. The `TxMetadata` record is still written, tagged with the function's CRC32C, and its `status` field carries the returned `u8` so auditors can distinguish unsuccessful named operations by exact reason.

Host failures (linking, instantiation, traps) surface as `Status::INVALID_OPERATION` (numeric `5`).

Standard status values used by the ledger:

| Value | Name |
|-------|------|
| `0` | `NONE` (success) |
| `1` | `INSUFFICIENT_FUNDS` |
| `2` | `ACCOUNT_NOT_FOUND` |
| `3` | `ZERO_SUM_VIOLATION` |
| `4` | `ENTRY_LIMIT_EXCEEDED` |
| `5` | `INVALID_OPERATION` |
| `6` | `ACCOUNT_LIMIT_EXCEEDED` |
| `7` | `DUPLICATE` |
| `8..=127` | reserved for future standard reasons |
| `128..=255` | user-defined |

Returning `128..=255` is how functions report domain-specific errors without polluting the standard status namespace.

---

## Writing a function

### Rust

```toml
# Cargo.toml
[lib]
crate-type = ["cdylib"]

[profile.release]
opt-level = "z"
lto = true
strip = true
```

```rust
// src/lib.rs
#[link(wasm_import_module = "ledger")]
unsafe extern "C" {
    fn credit(account_id: u64, amount: u64);
    fn debit(account_id: u64, amount: u64);
    fn get_balance(account_id: u64) -> i64;
}

/// Transfer `amount` from `from` to `to`, taking a `fee_bps` (basis
/// points) fee into `fee_acct`.
///
/// Params:
///   param0: sender
///   param1: receiver
///   param2: fee_acct
///   param3: amount
///   param4: fee_bps (e.g. 50 = 0.5%)
#[unsafe(no_mangle)]
pub extern "C" fn execute(
    param0: i64, param1: i64, param2: i64, param3: i64,
    param4: i64, _p5: i64, _p6: i64, _p7: i64,
) -> i32 {
    let sender   = param0 as u64;
    let receiver = param1 as u64;
    let fee_acct = param2 as u64;
    let amount   = param3 as u64;
    let fee_bps  = param4 as u64;
    let fee      = amount * fee_bps / 10_000;

    unsafe {
        if get_balance(sender) < (amount + fee) as i64 {
            return 1; // INSUFFICIENT_FUNDS
        }
        credit(sender, amount + fee);
        debit(receiver, amount);
        debit(fee_acct, fee);
    }
    0
}
```

Compile:

```bash
cargo build --target wasm32-unknown-unknown --release
# produces target/wasm32-unknown-unknown/release/<crate>.wasm
```

### AssemblyScript

```typescript
// assembly/index.ts
@external("ledger", "credit")
declare function credit(account_id: u64, amount: u64): void;

@external("ledger", "debit")
declare function debit(account_id: u64, amount: u64): void;

@external("ledger", "get_balance")
declare function get_balance(account_id: u64): i64;

export function execute(
    param0: i64, param1: i64, param2: i64, param3: i64,
    param4: i64, _p5: i64, _p6: i64, _p7: i64,
): i32 {
    const sender   = <u64>param0;
    const receiver = <u64>param1;
    const feeAcct  = <u64>param2;
    const amount   = <u64>param3;
    const feeBps   = <u64>param4;
    const fee      = amount * feeBps / 10_000;

    if (get_balance(<i64>sender) < <i64>(amount + fee)) {
        return 1; // INSUFFICIENT_FUNDS
    }
    credit(sender, amount + fee);
    debit(receiver, amount);
    debit(feeAcct, fee);
    return 0;
}
```

Compile:

```bash
asc assembly/index.ts -o build/function.wasm --optimize --runtime stub
```

### Hand-written WAT

Useful for minimal-size tests and examples. Every WAT snippet in the test suite follows this template:

```wat
(module
  (import "ledger" "credit" (func $credit (param i64 i64)))
  (import "ledger" "debit"  (func $debit  (param i64 i64)))
  (import "ledger" "get_balance" (func $get_balance (param i64) (result i64)))

  (func (export "execute")
    (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)

    ;; credit(param0, param1)
    local.get 0 local.get 1 call $credit

    ;; debit(param2, param1)
    local.get 2 local.get 1 call $debit

    i32.const 0))
```

Compile via the `wat` crate (already a dependency in test / bench paths):

```rust
let bytes = wat::parse_str(WAT)?;
```

---

## Registering a function

Registration is atomic and durable: it returns only after the WAL record has been committed and the handler is loaded into the live runtime. The next `Operation::Function { name }` is guaranteed to see it.

### gRPC

```proto
rpc RegisterFunction(RegisterFunctionRequest) returns (RegisterFunctionResponse);
rpc UnregisterFunction(UnregisterFunctionRequest) returns (UnregisterFunctionResponse);
rpc ListFunctions(ListFunctionsRequest)         returns (ListFunctionsResponse);

message RegisterFunctionRequest {
    string name              = 1; // snake_case, max 32 bytes
    bytes  binary            = 2; // WASM binary, max 4 MB
    bool   override_existing = 3; // false → ALREADY_EXISTS if name is taken
}

message RegisterFunctionResponse {
    uint32 version = 1; // monotonic per name, starts at 1
    uint32 crc32c  = 2; // CRC32C of the binary
}
```

Example with `grpcurl`:

```bash
grpcurl -d "$(jq -n --arg bin "$(base64 -w0 function.wasm)" \
  '{name:"fee_transfer", binary:$bin, override_existing:false}')" \
  -plaintext localhost:50051 \
  roda.ledger.v1.Ledger/RegisterFunction
```

### Rust library

```rust
use roda_ledger::client::LedgerClient;

let client = LedgerClient::connect("127.0.0.1:50051".parse()?).await?;

let binary = std::fs::read("function.wasm")?;
let (version, crc) = client
    .register_function("fee_transfer", &binary, /* override_existing = */ false)
    .await?;

println!("registered fee_transfer v{} (crc={:08x})", version, crc);
```

Or in embedded mode (one process, no gRPC):

```rust
use roda_ledger::ledger::{Ledger, LedgerConfig};

let mut ledger = Ledger::new(LedgerConfig::temp());
ledger.start()?;

let binary = std::fs::read("function.wasm")?;
let (version, crc) = ledger.register_function("fee_transfer", &binary, false)?;
```

Re-registering the same name without `override_existing = true` returns `AlreadyExists`. With the flag set, the new binary becomes `version = previous + 1` and replaces the old one in the live registry atomically.

---

## Invoking a function

A registered function is invoked as `Operation::Function { name, params, user_ref }`:

### gRPC

```proto
message Function {
    string         name     = 1; // max 32 bytes
    repeated int64 params   = 2; // 0..=8 values; short lists are zero-padded
    uint64         user_ref = 3; // idempotency / dedup key, same as other ops
}
```

```bash
grpcurl -d '{
    "function": {
        "name": "fee_transfer",
        "params": [101, 202, 999, 10000, 50],
        "user_ref": 123456
    },
    "wait_level": "COMMITTED"
}' -plaintext localhost:50051 \
    roda.ledger.v1.Ledger/SubmitAndWait
```

### Rust library

```rust
use roda_ledger::transaction::{Operation, WaitLevel};

let result = ledger.submit_and_wait(
    Operation::Function {
        name: "fee_transfer".into(),
        params: [101, 202, 999, 10_000, 50, 0, 0, 0],
        user_ref: 123_456,
    },
    WaitLevel::Committed,
);

if result.fail_reason.is_failure() {
    eprintln!("fee_transfer failed: {:?}", result.fail_reason);
}
```

### gRPC client (Rust)

```rust
use roda_ledger::client::LedgerClient;
use roda_ledger::grpc::proto::WaitLevel;

let result = client
    .submit_function_and_wait(
        "fee_transfer",
        [101, 202, 999, 10_000, 50, 0, 0, 0],
        123_456, // user_ref
        WaitLevel::Committed,
    )
    .await?;
```

Every `Operation::Function` produces a normal transaction in the WAL with a `TxMetadata.tag` of the form:

```
b"fnw\n"  ++  crc32c[0..4]     (8 bytes total)
```

`roda-ctl unpack` renders it as:

```json
{"type": "TxMetadata", "tx_id": 441001, "tag": "fnw\n4a2f1c3d", ...}
```

The CRC32C identifies the exact binary that executed — cross-reference it with the `FunctionRegistered` WAL record or with a `ListFunctions` response to resolve the name / version.

---

## Listing & unregistering

**List** — returns every currently-loaded handler:

```rust
for info in client.list_functions().await? {
    println!("{} v{} (crc={:08x})", info.name, info.version, info.crc32c);
}
```

**Unregister** — writes an empty file under the next version and a `FunctionRegistered` WAL record with `crc32c = 0`. The call blocks until the handler is gone from the live registry; any `Operation::Function { name }` submitted after it returns will fail with `INVALID_OPERATION`.

```rust
let unregistered_version = client.unregister_function("fee_transfer").await?;
```

Unregister is **durable**: the 0-byte-file + `crc32c = 0` pair survives restarts and crashes, and is replayed during recovery.

---

## Versioning

- Every register or override bumps the version by `+1`, starting at `1`.
- Version counter is stored in the live registry (sourced from `FunctionRegistered` WAL records + the function snapshot). The on-disk `functions/` directory is reference data only.
- A `Function` operation always uses the current latest version — there is no pinning to a specific version at submit time.
- The CRC32C embedded in `TxMetadata.tag` identifies which version actually ran, so auditors can always tell which binary produced any given set of entries.

---

## Storage layout on disk

```
data/
├── wal.bin                            # active WAL segment
├── wal_000001.bin                     # sealed segment (+ .crc, .seal, indexes)
├── wal_000002.bin
├── snapshot_000002.bin                # balance snapshot
├── snapshot_000002.crc
├── function_snapshot_000002.bin       # function-registry snapshot (same trigger)
├── function_snapshot_000002.crc
└── functions/
    ├── fee_transfer_v1.wasm           # binary under its version
    ├── fee_transfer_v2.wasm           # replaced version (override)
    └── removed_fn_v3.wasm             # 0 bytes — unregister marker
```

Function binaries are written atomically (temp file + rename). Unregister truncates the file to 0 bytes — it is **not** deleted, preserving the audit trail.

The function snapshot is emitted on the same `snapshot_frequency` trigger as the balance snapshot, so recovery always finds a paired `snapshot_{N}.bin` + `function_snapshot_{N}.bin` at the same segment boundary.

---

## Durability & recovery

Registration is durable **before** `register_function` returns. The call blocks until:

1. The binary is written atomically to `functions/{name}_v{N}.wasm`.
2. A `FunctionRegistered` WAL record is committed to the active segment.
3. The live `WasmRuntime` has compiled and installed the handler.

**Recovery on clean restart or crash** proceeds as follows:

1. Load the latest `function_snapshot_{N}.bin`. For each record with `crc32c != 0`, read `functions/{name}_v{version}.wasm` and compile it into the runtime.
2. Replay every `FunctionRegistered` WAL record in segments after the snapshot:
   - `crc32c != 0` → load the referenced version (replaces any older handler).
   - `crc32c == 0` → unload the handler.
3. Resume normal operation.

A failure to read a binary or install a handler during recovery is **non-recoverable**: the server aborts startup rather than continue with a registry that diverges from the WAL. The CRC32C embedded in every `FunctionRegistered` record lets recovery detect silent disk corruption before anything transactional runs.

---

## Determinism & isolation

The runtime is intentionally narrow:

- **No randomness.** No host API exposes a PRNG.
- **No wall clock.** Functions do not see time; tag timestamps are decided by the host.
- **No I/O, no network, no filesystem.** Only the 3 ledger host calls are available.
- **No threads, no atomics.** Functions run single-threaded.
- **No persistent memory.** A function has no state that survives between invocations: every call runs against a fresh wasmtime instance (internally cached by the engine for speed; no durable state is carried).
- **Sandboxed.** A trap or infinite loop cannot corrupt ledger state — the transaction is rolled back and the handler is left intact for subsequent calls.

This is the property we need for future Raft replication: the leader executes the WASM function, and followers apply the resulting entries directly — no re-execution, no divergence.

---

## Performance

The runtime is tuned for low per-call overhead on the hot path.

- **Per-call cost**: one `HashMap::get` on the per-Transactor caller cache, one `TypedFunc::call`, two host imports per credit / debit crossing.
- **Cache invalidation** is *per-name*: registering `foo` does not evict the cached entry for `bar`. Each cached entry stores the `update_seq` it was verified at; the next lookup either short-circuits (seq unchanged) or does a shared-registry read to reconcile just that one name.
- **One wasmtime `Engine`** per ledger (shared via `Arc<WasmRuntime>`).
- **One `Linker`** with host imports wired exactly once at ledger startup.
- **One `Store`** per Transactor, long-lived across calls. Function state is carried in host `TransactorState`, not in WASM-visible globals.
- **Instantiation** happens once per `(name, crc)` pair on first lookup; the resulting `TypedFunc` is cached and reused.

Empirical numbers from the current build (Apple M-series, release build):

| Benchmark | Native `Deposit` | WASM `Function` (same effect) |
|-----------|------------------|-------------------------------|
| `TransactorRunner::process_direct` (1 tx)         | ~132 ns/op | ~335 ns/op |
| `TransactorRunner::process_direct_batch` (1000)   | ~133 ns/op | ~286 ns/op |
| End-to-end `--wait` load test TPS, 1M accounts    | ~819 k     | ~814 k     |

The per-op overhead at the Transactor level is ~200 ns — pure host-crossing cost. At the pipeline level (gRPC → WAL commit → response) it is invisible: the WAL commit path dominates.

Run the comparison yourself:

```bash
cargo bench --bench transaction_runner_bench       # native
cargo bench --bench transaction_runner_bench_wasm  # WASM
cargo run --release --bin load      -- --wait --duration 30
cargo run --release --bin load_wasm -- --wait --duration 30
```

---

## Limits & validation

Validation runs at `register_function` before any disk write:

| Rule | Limit |
|------|-------|
| Name charset | ASCII letters, digits, `_` |
| Name starts with | ASCII letter |
| Name length | 1..=32 bytes |
| Binary size | 4 MB (gRPC-enforced via `max_message_size_bytes`) |
| Module parses | validated by wasmtime |
| Exports `execute` | required |
| `execute` signature | exactly `(i64 × 8) -> i32` |
| `execute` host imports | only `ledger.credit` / `ledger.debit` / `ledger.get_balance` |

A binary that fails any of these returns `InvalidArgument` from gRPC or `io::ErrorKind::InvalidData` / `InvalidInput` from the Rust API. Nothing is written to disk and no WAL record is produced.

---

## Troubleshooting

**`ALREADY_EXISTS` on `RegisterFunction`**
A function by this name is already loaded. Pass `override_existing = true` to replace.

**Operations return `INVALID_OPERATION` (status `5`)**
- The function is not registered, or was unregistered.
- The function trapped (out-of-bounds memory, stack overflow, unreachable, division by zero).
- The WASM binary parsed but failed instantiation. Check `tonic` / ledger logs around the call.

**Operations return `ZERO_SUM_VIOLATION` (status `3`)**
The function's total credits did not match total debits. Every `credit(a, N)` needs a matching `debit(b, N)` somewhere else in the same invocation (or vice versa).

**Unexpected balances after a restart**
Inspect both snapshots at the last sealed segment id:
```bash
ls data/function_snapshot_*.bin
roda-ctl unpack data/wal_000NNN.bin
```
You should see a `FunctionRegistered` record for every function that should be loaded and a matching snapshot file at a later or equal segment id.

**`Snapshot: read_function(name vN) failed` during startup**
The WAL references a binary that is missing from `data/functions/`. This is non-recoverable. Either restore the binary from backup, or manually remove the stale `FunctionRegistered` record from the WAL using `roda-ctl` (at your own risk).

---

## See also

- [ADR-014 — WASM Function Registry and Function Operation Execution](./adr/0014-wasm-function-registry.md)
- [Architecture](./03-architecture.md) — for context on the Transactor / WAL / Snapshot pipeline.
- [API](./02-api.md) — full API reference for the ledger service.
