# WASM Runtime — Programmable Ledger

roda-ledger lets you extend the transaction set at runtime with sandboxed WebAssembly functions. A registered function becomes a first-class `Operation::Function`: it is sequenced, executed atomically by the Transactor, and produces normal `TxEntry` records in the WAL alongside every other transaction.

This document covers everything about writing, registering, invoking, and operating programmable functions: ABI, lifecycle, host API, programmable KV state, storage layout, recovery guarantees, and gRPC surface.

The design spans four ADRs: [ADR-014](./adr/0014-wasm-function-registry.md) (the function registry + `execute`), [ADR-022](./adr/0022-account-layouts-and-program-defined-accounts.md) (account existence, flags, linked accounts), [ADR-023](./adr/0023-programmable-state.md) (the typed KV store, constants, and the `register` phase), and [ADR-026](./adr/0026-roda-wasm-abi.md) (the official `roda-wasm-abi` guest SDK).

---

## Table of contents

- [When to use it](#when-to-use-it)
- [Function ABI](#function-abi)
- [Host API](#host-api)
- [Programmable KV state](#programmable-kv-state)
- [Named constants & the register phase](#named-constants--the-register-phase)
- [Return values & rollback](#return-values--rollback)
- [Writing a function](#writing-a-function)
  - [The `roda-wasm-abi` guest SDK](#the-roda-wasm-abi-guest-sdk)
  - [Appendix: hand-written ABI](#appendix-hand-written-abi)
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

Every registered function must export a symbol named `execute` with a fixed signature:

```
execute(i64, i64, i64, i64, i64, i64, i64, i64) -> i32
```

- Exactly **8** `i64` positional parameters. Unused slots are conventionally passed as `0`.
- Exactly **1** `i32` return value, interpreted as a `u8` status code:
  - `0` — success (the transaction's host-side credits / debits are committed);
  - `1..=127` — standard failure reasons (defined by the ledger, full rollback);
  - `128..=255` — user-defined custom reasons (full rollback).

The fixed arity eliminates any need for linear memory pointer / length passing: account ids, amounts, rates, flags, timestamps all fit into 8 `i64` slots.

A function **may** also export a second symbol named `register` with signature `() -> ()`. It is optional and defines the module's named constants; see [Named constants & the register phase](#named-constants--the-register-phase).

---

## Host API

A function imports its host verbs from the WASM module named `ledger`. There are **eleven** verbs across three groups: balances (ADR-014), accounts & flags (ADR-022), and the typed KV store + constants (ADR-023). A module imports only the verbs it uses — there is **no import allow-list**; importing a verb the module never calls is harmless, and importing a name that is *not* one of the eleven fails at instantiation (the function then surfaces as `INVALID_OPERATION`).

The names and signatures below are the raw WASM imports. The official [`roda-wasm-abi`](#the-roda-wasm-abi-guest-sdk) crate exposes safe Rust wrappers over all of them (its wrapper for `get_balance` is named `balance`).

### Balances

```
(import "ledger" "credit"      (func (param i64 i64)))
(import "ledger" "debit"       (func (param i64 i64)))
(import "ledger" "get_balance" (func (param i64) (result i64)))
```

| Host verb | Signature | Effect |
|-----------|-----------|--------|
| `credit(account_id, amount)` | `(u64, u64) -> ()` | Decreases `account_id`'s balance by `amount`. |
| `debit(account_id, amount)`  | `(u64, u64) -> ()` | Increases `account_id`'s balance by `amount`. |
| `get_balance(account_id)`    | `(u64) -> i64`     | Reads the current balance (signed). Absent account reads `0`. |

Convention in roda-ledger: **credit subtracts, debit adds.** A `Transfer { from, to, amount }` is `credit(from, amount)` + `debit(to, amount)` — credits move value *out* of an account, debits move value *in*. Same convention applies to WASM functions.

### Accounts & flags (ADR-022)

```
(import "ledger" "linked_account" (func (param i64 i32) (result i64)))
(import "ledger" "get_flag"       (func (param i64 i32) (result i32)))
(import "ledger" "has_flag"       (func (param i64 i32 i32) (result i32)))
(import "ledger" "set_flag"       (func (param i64 i32 i32)))
```

| Host verb | Signature | Effect |
|-----------|-----------|--------|
| `linked_account(account_id, type_id)` | `(u64, u16) -> u64` | Get-or-create the sub-account linked to `account_id` under `type_id` (ADR-022 §6); returns the child account id. A first call lazily opens a `PROGRAMMED` account and emits `AccountOpened` + `AccountLinked` followers. |
| `get_flag(account_id, lane)` | `(u64, u8) -> u8` | Read the flag byte at `lane` (0..=7) of `account_id`. Lane 0 is the status lane. |
| `has_flag(account_id, lane, value)` | `(u64, u8, u8) -> bool` | True iff `account_id`'s `lane` byte equals `value`. |
| `set_flag(account_id, lane, value)` | `(u64, u8, u8) -> ()` | Set `account_id`'s `lane` byte to `value`. WASM may write any lane, including the status lane 0 (ADR-022 §6). |

`type_id`, `lane`, and `value` cross the WASM boundary as `i32` (the host narrows them to `u16` / `u8`).

### KV store & constants (ADR-023)

```
(import "ledger" "kv_get" (func (param i32 i32 i32 i32) (result i64)))
(import "ledger" "kv_set" (func (param i32 i32 i32 i32 i64)))
(import "ledger" "kv_register_constant" (func (param i32)))
(import "ledger" "kv_get_constant"      (func (param i32) (result i32)))
```

| Host verb | Signature | Effect |
|-----------|-----------|--------|
| `kv_get(k0, k1, k2, k3)` | `([u32; 4]) -> i64` | Read the integer value at the four-component key; `0` if absent (or non-integer). |
| `kv_set(k0, k1, k2, k3, value)` | `([u32; 4], i64) -> ()` | Set the key to a signed integer value. |
| `kv_register_constant(name_ptr)` | `(*const u8) -> ()` | Register a constant by null-terminated UTF-8 name (create-if-absent). **Only callable from `register`** — calling it elsewhere fails the tx with `PROHIBITED_HOST_CALL`. |
| `kv_get_constant(name_ptr)` | `(*const u8) -> u32` | Resolve a registered constant's id, for use as a key component. An unknown name fails the tx with `CONSTANT_NOT_FOUND` (the host stops execution rather than return a bogus id). |

See [Programmable KV state](#programmable-kv-state) and [Named constants & the register phase](#named-constants--the-register-phase) below.

**Account-existence rule.** Unlike pre-ADR-022, host verbs are **existence-aware**. `credit`, `debit`, `get_flag`, `has_flag`, and `set_flag` operate only on an account whose status lane is non-zero (an account that has been opened — `Operation::OpenAccount`, or lazily via `linked_account`). A balance/flag verb on an *unopened* account sets `ACCOUNT_NOT_FOUND` and **fails the whole transaction** (full rollback). It is not a silent no-op. Open accounts up front, or create program sub-accounts with `linked_account`, before crediting/debiting them.

**Zero-sum invariant.** The ledger verifies `sum(credits) == sum(debits)` after the function returns. If the function emits an unbalanced set of credits / debits, the transaction is rejected and rolled back with `ZERO_SUM_VIOLATION`.

**Overdraft policy.** The ledger does not enforce negative-balance checks inside functions — balance arithmetic saturates and a credit may legitimately drive a balance negative. If you need overdraft protection, read the balance and return a non-zero status to trigger rollback:

```rust
if balance(sender) < amount as i64 {
    return Status::fail(1); // INSUFFICIENT_FUNDS
}
```

No randomness, no wall clock, no I/O, no WASM threads, no atomics — every host verb is deterministic, so the ABI stays replayable on followers.

---

## Programmable KV state

ADR-023 adds a single typed key→value map that a function reads and writes **atomically with its balance effects**, replicated through the same WAL. It is intentionally minimal: one map, no scopes, no registers, no tree.

- **Key**: four `u32` components — `[u32; 4]`. Shorter logical keys zero-pad the trailing slots; structure (a "namespace" component, an account component, …) is convention inside those four slots. Build one with the SDK's [`key!`](#the-roda-wasm-abi-guest-sdk) macro.
- **Value**: a signed `i64`.
- **Absent read**: `kv_get` on a key that was never set returns `0` (also `0` if the stored value is somehow non-integer). There is no separate "exists" probe.
- **Mutation & rollback**: each `kv_set` records an undo entry and logs a packed `KvEntry` follower (folded into the trailer CRC, like a credit/debit). If the transaction fails, the whole map is restored; followers and recovery reapply `KvEntry` records without re-running the module.

A key component may also carry a **constant id** rather than a plain integer (see below); the host tags it so the packed key remembers it was a constant.

---

## Named constants & the register phase

A module is **stateless** — it keeps nothing in WASM globals between calls. To give a key component a stable, named meaning, a module declares **named constants** and resolves them by name at execute time; the host owns the name→id map.

This is what the optional second export, `register`, is for:

- A module *may* export `register` with signature `() -> ()` (alongside the required `execute`). It is optional; a module with no constants omits it.
- The host calls `register` **once per instantiation** (and after any re-instantiation), in a dedicated **Register phase**. The only host verb callable in that phase is `kv_register_constant` — any balance / flag / KV-data verb called during `register` fails the tx with `PROHIBITED_HOST_CALL`. Symmetrically, `kv_register_constant` is rejected outside `register`.
- `register` runs at **registration time**, inside the same `FunctionRegistration` transaction that installs the module. If `register` returns non-zero (e.g. a prohibited call), the registration transaction rolls back and the install is reverted — the module is registered atomically with its constants, or not at all.
- In `execute`, resolve a constant by name with `kv_get_constant("NAME")`, which returns the `u32` id to use as a key component. An unknown name fails the tx with `CONSTANT_NOT_FOUND`. Constant names are bounded to **32 bytes** (`KV_CONSTANT_NAME_MAX`); a longer name fails with `CONSTANT_NAME_TOO_LONG`.

```rust
// register the name once …
register!(|| {
    kv_register_constant(c"counter");
});

// … then resolve it by name on every call (no global caches the id)
execute!(|p| {
    let k = key!(kv_get_constant(c"counter"), p.account(0));
    kv_set(k, kv_get(k) + 1);
    Status::OK
});
```

Constants and KV cells are checkpointed together in the per-segment `kv_snapshot_{N}` file (see [Storage layout](#storage-layout-on-disk)).

---

## Return values & rollback

The `execute` return value is the transaction's final status:

- `0` — the host-side credits and debits become part of the WAL transaction; the zero-sum invariant is verified; if it holds, the transaction commits.
- any non-zero value — the transaction is **fully rolled back**: every credit / debit the function applied is reversed in the same way a failed built-in operation would be. The `TxMetadata` record is still written, tagged with the function's CRC32C, and its `fail_reason` field carries the returned `u8` so auditors can distinguish unsuccessful named operations by exact reason.

Host failures (linking, instantiation, traps) surface as `INVALID_OPERATION` (numeric `5`).

Standard status values are defined by `FailReason` in `crates/storage/src/entities.rs`. The guest SDK does not name them — it only offers `Status::OK` (0) and `Status::fail(code)` — but a function returning these codes is interpreted by the ledger as the corresponding reason:

| Value | Name | Notes |
|-------|------|-------|
| `0` | `NONE` (success) | |
| `1` | `INSUFFICIENT_FUNDS` | |
| `2` | `ACCOUNT_NOT_FOUND` | set by a balance/flag verb on an unopened account |
| `3` | `ZERO_SUM_VIOLATION` | |
| `4` | `ENTRY_LIMIT_EXCEEDED` | |
| `5` | `INVALID_OPERATION` | also the catch-all for traps / link / instantiation failures |
| `6` | — | **retired** (was `ACCOUNT_LIMIT_EXCEEDED`; the id allocator panics on exhaustion instead) |
| `7` | `DUPLICATE` | |
| `8` | `PROHIBITED_HOST_CALL` | a verb called in the wrong phase (ADR-023 §6) |
| `9` | `CONSTANT_NOT_FOUND` | `kv_get_constant` on an unregistered name |
| `10` | `CONSTANT_NAME_TOO_LONG` | constant name exceeds `KV_CONSTANT_NAME_MAX` (32 bytes) |
| `11..=127` | reserved for future standard reasons | |
| `128..=255` | user-defined | |

Returning `128..=255` is how functions report domain-specific errors without polluting the standard status namespace.

---

## Writing a function

### The `roda-wasm-abi` guest SDK

The supported way to author a module in Rust is the [`roda-wasm-abi`](../crates/roda-wasm-abi) crate (ADR-026). It wraps the whole guest ABI — the fixed 8×`i64` → `i32` calling convention, every host verb, the `key!` builder, and named constants — behind the `execute!` / `register!` macros and a typed `Params`. It is `#![no_std]`, allocator-free, and dependency-free, so it compiles cleanly to `wasm32-unknown-unknown`.

```toml
# Cargo.toml
[dependencies]
roda-wasm-abi = "0.1"

[lib]
crate-type = ["cdylib"]

[profile.release]
opt-level = "z"
lto = true
strip = true
```

A balanced transfer — debit `account(0)`, credit `account(2)`, by `amount(1)` (the real `examples/transfer`):

```rust
// src/lib.rs
use roda_wasm_abi::{credit, debit, execute, Status};

execute!(|p| {
    let amount = p.amount(1);
    debit(p.account(0), amount);
    credit(p.account(2), amount);
    Status::OK
});
```

The `execute!` body receives a [`Params`] with typed accessors — `p.account(i)`, `p.amount(i)`, `p.get(i)` (raw `i64`) — and returns anything convertible to `Status`: a `Status`, a `Result<(), u8>`, or `()` (unconditional commit). Return `Status::OK` to commit (subject to the zero-sum check) or `Status::fail(code)` to roll the whole transaction back.

A stateful example — a KV counter scoped under a named constant (the real `examples/counter`):

```rust
use roda_wasm_abi::{
    credit, debit, execute, key, kv_get, kv_get_constant, kv_register_constant, kv_set, register,
    Status,
};

register!(|| {
    kv_register_constant(c"counter");
});

execute!(|p| {
    let account = p.account(0);
    let k = key!(kv_get_constant(c"counter"), account);
    let next = kv_get(k) + 1;
    kv_set(k, next);

    debit(0, next as u64);        // into the system account
    credit(account, next as u64); // out of the caller's account
    Status::OK
});
```

The SDK's safe verb wrappers (`crates/roda-wasm-abi/src/{account,kv}.rs`) mirror the host signedness: `balance` (the wrapper over `get_balance`), `credit`, `debit`, `linked_account`, `get_flag` / `has_flag` / `set_flag`, and `kv_get` / `kv_set` / `kv_register_constant` / `kv_get_constant`.

Compile:

```bash
rustup target add wasm32-unknown-unknown
cargo build --release --target wasm32-unknown-unknown
# produces target/wasm32-unknown-unknown/release/<crate>.wasm
```

For tests and tooling, the host-only `tools` feature (`roda-wasm-abi = { version = "0.1", features = ["tools"] }`) compiles module source to wasm in-process via `roda_wasm_abi::tools::compile_to_wasm`.

### Appendix: hand-written ABI

You do not need this for Rust — prefer the SDK above. It documents the raw ABI for other toolchains (AssemblyScript, hand-written WAT) and for understanding what the macros emit. Hand-written modules are the form used throughout the engine's own test/bench suite.

The contract is unchanged: export `execute` with `(i64 × 8) -> i32`, optionally export `register` with `() -> ()`, and import host verbs from module `ledger`.

Raw `extern "C"` in Rust (equivalent to the macro-based transfer above):

```rust
#[link(wasm_import_module = "ledger")]
unsafe extern "C" {
    fn credit(account_id: u64, amount: u64);
    fn debit(account_id: u64, amount: u64);
}

#[unsafe(no_mangle)]
pub extern "C" fn execute(
    p0: i64, p1: i64, p2: i64, _p3: i64,
    _p4: i64, _p5: i64, _p6: i64, _p7: i64,
) -> i32 {
    unsafe {
        debit(p0 as u64, p1 as u64);
        credit(p2 as u64, p1 as u64);
    }
    0
}
```

AssemblyScript:

```typescript
// assembly/index.ts
@external("ledger", "credit")
declare function credit(account_id: u64, amount: u64): void;
@external("ledger", "debit")
declare function debit(account_id: u64, amount: u64): void;

export function execute(
    p0: i64, p1: i64, p2: i64, _p3: i64,
    _p4: i64, _p5: i64, _p6: i64, _p7: i64,
): i32 {
    debit(<u64>p0, <u64>p1);
    credit(<u64>p2, <u64>p1);
    return 0;
}
```

```bash
asc assembly/index.ts -o build/function.wasm --optimize --runtime stub
```

Hand-written WAT — the form used by the engine's tests and the `load_wasm` generator:

```wat
(module
  (import "ledger" "credit" (func $credit (param i64 i64)))
  (import "ledger" "debit"  (func $debit  (param i64 i64)))

  (func (export "execute")
    (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)

    ;; debit(param0, param1)
    local.get 0 local.get 1 call $debit

    ;; credit(param2, param1)
    local.get 2 local.get 1 call $credit

    i32.const 0))
```

Compile via the `wat` crate (a dependency in test / bench paths):

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

On the wire `params` is a `repeated int64`, but the Rust `Operation::Function.params` type is a **fixed `[i64; 8]`** — the server zero-pads a short list and truncates anything beyond 8 slots to land on that fixed arity.

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

Every `Operation::Function` produces a normal transaction in the WAL with an 8-byte `TxMetadata.tag` built by `build_wasm_tag`:

```
[ b'f', b'n', b'w', b'\n', crc[0], crc[1], crc[2], crc[3] ]   // crc = crc32c.to_le_bytes()
```

i.e. a 4-byte literal prefix `fnw\n` followed by the binary's CRC32C in **little-endian** byte order.

How `roda-ctl unpack` renders it depends on the CRC bytes. `encode_tag` (`storage/src/entities.rs`) trims trailing NULs and prints the tag as UTF-8 *only if the whole thing is valid UTF-8*; otherwise it dumps all 8 bytes as 16 lowercase hex digits. Because the CRC bytes are usually not valid UTF-8, the typical render is the hex form — the `fnw\n` prefix is then visible as its hex bytes `666e770a`:

```json
{"type": "TxMetadata", "tx_id": 441001, "tag": "666e770a3d1c2f4a", ...}
```

(here CRC `0x4a2f1c3d` little-endian is `3d 1c 2f 4a`). The embedded CRC32C identifies the exact binary that executed — cross-reference it with the `FunctionRegistered` WAL record or a `ListFunctions` response to resolve the name / version.

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
├── kv_snapshot_000002.bin             # KV state + interned constants (same trigger, ADR-023)
├── kv_snapshot_000002.crc
└── functions/
    ├── fee_transfer_v1.wasm           # binary under its version
    ├── fee_transfer_v2.wasm           # replaced version (override)
    └── removed_fn_v3.wasm             # 0 bytes — unregister marker
```

Function binaries are written atomically (temp file + rename). Unregister truncates the file to 0 bytes — it is **not** deleted, preserving the audit trail.

The function snapshot is emitted on the same `snapshot_frequency` trigger as the balance snapshot, so recovery always finds a paired `snapshot_{N}.bin` + `function_snapshot_{N}.bin` at the same segment boundary. The **KV snapshot** (`kv_snapshot_{N}.bin` + `.crc`, ADR-023) is written on that same seal trigger — it holds the full programmable KV map plus the interned constants (`id → name`) as-of that segment, in one file.

---

## Durability & recovery

Registration is durable **before** `register_function` returns. The call blocks until:

1. The binary is written atomically to `functions/{name}_v{N}.wasm`.
2. A `FunctionRegistered` WAL record is committed to the active segment.
3. The live `WasmRuntime` has compiled and installed the handler.

**Recovery on clean restart or crash** proceeds as follows:

1. Load the latest `function_snapshot_{N}.bin`. For each record with `crc32c != 0`, read `functions/{name}_v{version}.wasm` and compile it into the runtime.
2. Seed the KV map and constant registry from the paired `kv_snapshot_{N}.bin` at the same segment boundary (ADR-023 §7).
3. Replay every WAL record in segments after the snapshot:
   - `FunctionRegistered`, `crc32c != 0` → load the referenced version (replaces any older handler).
   - `FunctionRegistered`, `crc32c == 0` → unload the handler.
   - `KvEntry` / `KvConstant` → reapply to the KV map / constant registry (no re-execution).
4. Resume normal operation.

A failure to read a binary or install a handler during recovery is **non-recoverable**: the server aborts startup rather than continue with a registry that diverges from the WAL. The CRC32C embedded in every `FunctionRegistered` record lets recovery detect silent disk corruption before anything transactional runs.

---

## Determinism & isolation

The runtime is intentionally narrow:

- **No randomness.** No host API exposes a PRNG.
- **No wall clock.** Functions do not see time; tag timestamps are decided by the host.
- **No I/O, no network, no filesystem.** Only the `ledger` host verbs are available, and every one of them is deterministic.
- **No threads, no atomics.** Functions run single-threaded.
- **No persistent memory.** A function has no WASM-global state that survives between invocations: it resolves named constants by name each call (`kv_get_constant`) and reads/writes durable state only through the KV verbs and balances. The host owns the name→id map; the module stays stateless.
- **Sandboxed.** A trap or infinite loop cannot corrupt ledger state — the transaction is rolled back and the handler is left intact for subsequent calls.

This is the property we need for future Raft replication: the leader executes the WASM function, and followers apply the resulting entries directly — no re-execution, no divergence.

---

## Performance

The runtime is tuned for low per-call overhead on the hot path.

- **Per-call cost**: one `HashMap::get` on the per-Transactor caller cache, one `TypedFunc::call`, plus one host crossing per verb the body invokes.
- **Cache invalidation** is *per-name*: registering `foo` does not evict the cached entry for `bar`. Each cached entry stores the `update_seq` it was verified at; the next lookup either short-circuits (seq unchanged) or does a shared-registry read to reconcile just that one name.
- **One wasmtime `Engine`** per ledger (shared via `Arc<WasmRuntime>`).
- **One `Linker`** with host imports wired exactly once at ledger startup.
- **One `Store`** per Transactor, long-lived across calls. The transactor state (`Computer`) is reached through the store's `WasmStoreData`, not via WASM-visible globals.
- **Instantiation** happens once per `(name, crc)` pair on first lookup; the resulting `execute` (and optional `register`) `TypedFunc` is cached and reused.

Indicative numbers (Apple M-series, release build). These predate the ADR-022/023 (#110) programmable-state rewrite — treat them as ballpark, not current; re-run the benches below for live figures:

| Benchmark | Native `Deposit` | WASM `Function` (same effect) |
|-----------|------------------|-------------------------------|
| `transaction_runner_bench` (1 tx)          | ~132 ns/op | ~335 ns/op |
| `transaction_runner_bench` (1000-batch)    | ~133 ns/op | ~286 ns/op |
| End-to-end `--wait` load test TPS, 1M accounts | ~819 k | ~814 k |

The per-op overhead at the Transactor level is roughly a couple hundred ns — pure host-crossing cost. At the pipeline level (gRPC → WAL commit → response) it is invisible: the WAL commit path dominates.

Run the comparison yourself:

```bash
cargo bench -p ledger --bench transaction_runner_bench       # native
cargo bench -p ledger --bench transaction_runner_bench_wasm  # WASM
cargo run -p ledger --release --bin load      -- --wait --duration 30
cargo run -p ledger --release --bin load_wasm -- --wait --duration 30
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
| `register` signature | if present, exactly `() -> ()` (the export is optional) |
| Host imports | **not** restricted by an allow-list — validation checks only the `execute` / `register` *signatures*. Any of the eleven `ledger` verbs may be imported; importing an unknown name fails later, at instantiation. |

A binary that fails any of these returns `InvalidArgument` from gRPC or `io::ErrorKind::InvalidData` / `InvalidInput` from the Rust API. Nothing is written to disk and no WAL record is produced. (A module that imports a name outside the `ledger` verb set passes validation but fails to instantiate at first invocation, surfacing as `INVALID_OPERATION`.)

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

**Operations return `ACCOUNT_NOT_FOUND` (status `2`)**
The function called a balance or flag verb on an account that was never opened. Open it first (`Operation::OpenAccount`) or create the program sub-account with `linked_account` before crediting/debiting it (ADR-022).

**Operations return `PROHIBITED_HOST_CALL` (status `8`)**
A verb was called in the wrong phase (ADR-023 §6): `kv_register_constant` outside `register`, or any balance / flag / KV-data verb *inside* `register`. Register constants only in `register`; do everything else in `execute`.

**Operations return `CONSTANT_NOT_FOUND` (status `9`) or `CONSTANT_NAME_TOO_LONG` (status `10`)**
`kv_get_constant` resolved a name the module never registered in `register` (`9`), or a constant name exceeded 32 bytes (`10`). Ensure `register` declares every name `execute` resolves, and keep names ≤ 32 bytes.

**Unexpected balances or KV state after a restart**
Inspect the snapshots at the last sealed segment id:
```bash
ls data/function_snapshot_*.bin data/kv_snapshot_*.bin
roda-ctl unpack data/wal_000NNN.bin
```
You should see a `FunctionRegistered` record for every function that should be loaded and matching `function_snapshot_{N}` / `kv_snapshot_{N}` files at a later or equal segment id.

**`Snapshot: read_function(name vN) failed` during startup**
The WAL references a binary that is missing from `data/functions/`. This is non-recoverable. Either restore the binary from backup, or manually remove the stale `FunctionRegistered` record from the WAL using `roda-ctl` (at your own risk).

---

## See also

- [ADR-014 — WASM Function Registry and Function Operation Execution](./adr/0014-wasm-function-registry.md)
- [ADR-022 — Account Layouts and Program-Defined Accounts](./adr/0022-account-layouts-and-program-defined-accounts.md)
- [ADR-023 — Programmable State (typed KV store + constants)](./adr/0023-programmable-state.md)
- [ADR-026 — `roda-wasm-abi` guest SDK](./adr/0026-roda-wasm-abi.md)
- [`roda-wasm-abi` crate](../crates/roda-wasm-abi) — the official guest SDK, with `examples/transfer` and `examples/counter`.
- [Architecture](./03-architecture.md) — for context on the Transactor / WAL / Snapshot pipeline.
- [API](./02-api.md) — full API reference for the ledger service.
