# ADR-014: WASM Function Registry and Named Operation Execution

**Status:** Proposed  
**Date:** 2026-04-16  
**Author:** Taleh Ibrahimli

**Amends:**
- ADR-003 — deprecates `Operation::Composite`, implements `Operation::Named` execution path
- ADR-001 — adds `WalEntryKind::FunctionRegistered` (kind=5)
- ADR-006 — adds `function_snapshot_{N}.bin` to storage layout

---

## Context

`Operation::Named` was reserved in ADR-003 and ADR-004 as a forward-compatible extension point
for user-defined logic. The placeholder existed but had no implementation.

`Operation::Composite` was the interim escape hatch — a caller-defined sequence of `Credit` and
`Debit` steps executed atomically. It solves the immediate problem but has significant limitations:

- Anonymous — no name, no audit trail beyond entries
- Inline — defined per-call, not reusable
- No versioning — no way to know which logic produced which entries
- No registry — cannot be inspected, listed, or managed
- Bypasses the registry entirely — invisible to operators

With a proper Function registry, `Composite` has no remaining purpose. It is deprecated by this
ADR and will be removed in a future ADR once WASM is stable.

The missing piece was a safe, high-performance runtime for user-defined logic that:

- Executes exactly once in the Transactor — consistent with ADR-001 entries-based model
- Cannot escape the ledger's invariants — sandboxed API surface
- Is registered by name and versioned — auditable, reusable
- Survives crash recovery — WAL-recorded, snapshot-backed
- Supports any language that compiles to WASM — Rust, AssemblyScript, C, Zig
- Executes atomically — failure causes full rollback, same as all other operations

---

## Decision

### Runtime: wasmtime

wasmtime is the WASM runtime. Rationale:

- Bytecode Alliance backed — production-grade, actively maintained
- Used by Fastly for untrusted code execution at the edge — strongest safety story
- Component Model support — clean interface types between host and WASM
- Best-in-class Rust API — `Engine`, `Module`, `Store`, `Linker`
- Hardware-accelerated JIT — minimal per-execution overhead

wasmer rejected — API instability, weaker safety guarantees for untrusted code.

---

### Host API Surface

The WASM function can only call four host functions. Nothing else is exposed:

```rust
// namespace: "ledger"
fn credit(account_id: u64, amount: u64);              // guaranteed, no return
fn debit(account_id: u64, amount: u64);               // guaranteed, no return
fn get_balance(account_id: u64) -> i64;
fn check_negative_balance(account_id: u64) -> i32;    // 1 if balance < 0, 0 otherwise
```

**`credit` and `debit` return nothing.** They are guaranteed operations — they update the
Transactor's balance Vec directly. They cannot fail individually. Failure is expressed through
`check_negative_balance` or via the zero-sum invariant enforced after execution.

**`check_negative_balance`** is the mechanism for overdraft protection. The function calls it
after applying credits/debits to determine whether to proceed or signal failure by returning a
non-zero fail reason. This gives the function full control over balance validation without
exposing unsafe escape hatches.

Integer-only interface — no memory sharing beyond the params array, no pointers, no strings. All
WASM value types map directly to native types.

The host functions close over the Transactor's balance Vec during execution — same mutable
reference used by all built-in operations.

---

### Function Entry Point

Every WASM function must export a single entry point with exactly 8 `i64` parameters:

```rust
// Rust
#[no_mangle]
pub extern "C" fn execute(
    param1: i64, param2: i64, param3: i64, param4: i64,
    param5: i64, param6: i64, param7: i64, param8: i64,
) -> i32;
```

```typescript
// AssemblyScript
export function execute(
    param1: i64, param2: i64, param3: i64, param4: i64,
    param5: i64, param6: i64, param7: i64, param8: i64,
): i32;
```

**Why exactly 8 params:** Fixed arity eliminates the need for linear memory pointer/length
passing. 8 `i64` values cover all practical financial use cases — account IDs, amounts, rates,
flags, timestamps. Unused params are passed as 0. This keeps the ABI simple, safe, and
verifiable at registration time.

**Return value:** `0` = success. Any non-zero value is a `FailReason` — the same `u8` type
used throughout the system. Values 0–127 map to standard `FailReason` constants. Values 128–255
are user-defined custom reasons, consistent with the existing `FailReason` design.

**Atomicity:** Functions execute atomically. If the function returns a non-zero fail reason,
all credits and debits applied during execution are rolled back. The Transactor's rollback
mechanism is identical to that used for built-in operations.

---

### Example: Fee Deduction

```rust
// Rust — compile with: cargo build --target wasm32-unknown-unknown --release
#[link(wasm_import_module = "ledger")]
extern "C" {
    fn credit(account_id: u64, amount: u64);
    fn debit(account_id: u64, amount: u64);
    fn get_balance(account_id: u64) -> i64;
    fn check_negative_balance(account_id: u64) -> i32;
}

// param1 = sender account
// param2 = receiver account
// param3 = fee account
// param4 = amount
// param5 = fee_rate (basis points, e.g. 50 = 0.5%)
#[no_mangle]
pub extern "C" fn execute(
    param1: i64, param2: i64, param3: i64, param4: i64,
    param5: i64, _: i64, _: i64, _: i64,
) -> i32 {
    let sender   = param1 as u64;
    let receiver = param2 as u64;
    let fee_acct = param3 as u64;
    let amount   = param4 as u64;
    let fee      = (amount * param5 as u64) / 10_000;

    unsafe {
        credit(sender, amount + fee);
        debit(receiver, amount);
        debit(fee_acct, fee);

        if check_negative_balance(sender) != 0 {
            return 1; // INSUFFICIENT_FUNDS
        }
    }
    0
}
```

```typescript
// AssemblyScript — compile with: asc function.ts -o function.wasm --optimize
@external("ledger", "credit")
declare function credit(account_id: u64, amount: u64): void;

@external("ledger", "debit")
declare function debit(account_id: u64, amount: u64): void;

@external("ledger", "get_balance")
declare function get_balance(account_id: u64): i64;

@external("ledger", "check_negative_balance")
declare function check_negative_balance(account_id: u64): i32;

export function execute(
    param1: i64, param2: i64, param3: i64, param4: i64,
    param5: i64, _p6: i64, _p7: i64, _p8: i64,
): i32 {
    const sender   = param1 as u64;
    const receiver = param2 as u64;
    const feeAcct  = param3 as u64;
    const amount   = param4 as u64;
    const fee      = (amount * (param5 as u64)) / 10_000;

    credit(sender, amount + fee);
    debit(receiver, amount);
    debit(feeAcct, fee);

    if (check_negative_balance(sender) != 0) {
        return 1; // INSUFFICIENT_FUNDS
    }
    return 0;
}
```

---

### Determinism

Functions must be pure and deterministic — same params always produce the same credits/debits.
This is enforced architecturally:

- Host API exposes no randomness, no wall-clock time, no I/O
- No WASM threads, no atomics
- wasmtime fuel metering can limit execution cycles

**With Raft replication (ADR-015):** only the leader executes WASM functions. Entries produced
by execution are replicated as binary WAL data to followers. Followers apply entries directly
without re-executing the function. When a leader switch occurs, the new leader executes all
subsequent `Named` operations from that point forward. User-supplied params are not stored or
replayed — the function is trusted to be deterministic given the same params, but re-execution
is not required for correctness because entries (not operations) are what is replicated.

---

### Registration Path

```
gRPC RegisterFunction(name, binary)
  → validate WASM binary (wasmtime::Module::validate)
  → verify binary exports execute(i64×8) -> i32
  → compute CRC32C of binary
  → write to disk: functions/{name}_v{N}.wasm
  → write WAL record FunctionRegistered directly to active segment (bypasses Transactor)
  → Snapshot stage receives WAL record, updates FunctionRegistry
  → Pipeline atomicLastFunctionSequence incremented
  → Transactor detects sequence change on next cycle, clones registry from Arc<RwLock>
  → return: version number, crc32c
```

Registration bypasses the Transactor. It is not a financial transaction — no entries, no
zero-sum invariant. WAL write goes directly to the active segment, same path used by crash
recovery markers.

---

### WAL Record: FunctionRegistered (40 bytes)

```rust
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct FunctionRegistered {
    pub entry_type:  u8,        // 1 @ 0  — WalEntryKind::FunctionRegistered (5)
    pub version:     u8,        // 1 @ 1  — monotonic per name, starts at 1
    pub name_len:    u8,        // 1 @ 2  — byte length of name (max 16)
    pub status:      u8,        // 1 @ 3  — 0=active, 1=unregistered
    pub crc32c:      u32,       // 4 @ 4  — CRC32C of WASM binary
    pub name:        [u8; 16],  // 16 @ 8 — function name, UTF-8, null-padded
    pub _pad:        [u8; 16],  // 16 @ 24
}                               // total: 40 bytes
```

Function name is capped at 16 bytes — names are identifiers, not descriptions.

File path is derived deterministically from name and version:
```
functions/{name}_v{version}.wasm
```

No path stored in WAL — always reconstructible from name and version.

The `status` field supports unregistration without a separate record type — a
`FunctionRegistered` record with `status=1` marks the function as dead.

---

### WasmRuntime Struct

All wasmtime concerns are encapsulated in a dedicated `src/wasm_runtime.rs` module:

```rust
pub struct WasmRuntime {
    engine: wasmtime::Engine,
}

impl WasmRuntime {
    pub fn new() -> Self;
    pub fn compile(&self, binary: &[u8]) -> Result<wasmtime::Module, WasmError>;
    pub fn execute(
        &self,
        module: &wasmtime::Module,
        params: [i64; 8],
        balances: &mut Vec<Balance>,
    ) -> Result<(), FailReason>;
    pub fn validate_exports(module: &wasmtime::Module) -> Result<(), WasmError>;
}
```

One `Engine` per `Ledger` — shared, thread-safe, JIT compiled once at registration.
One `Module` per registered function version — compiled at registration, stored in registry.
One `Store` per execution — created per `Named` operation, dropped immediately after.

`Engine` is `Send + Sync`. `Module` is `Send + Sync`. `Store` is per-execution only.

---

### FunctionRegistry and Transactor Hot Cache

```rust
// Shared between registration path and Transactor
pub struct FunctionRegistry {
    functions: HashMap<String, Vec<FunctionVersion>>,
}

pub struct FunctionVersion {
    pub version:  u8,
    pub crc32c:   u32,
    pub module:   wasmtime::Module,   // compiled, ready to execute
    pub status:   FunctionStatus,
}

pub enum FunctionStatus {
    Active,
    Unregistered,
}
```

**Shared state via pipeline:**

```rust
// In Pipeline — cache-padded atomic, shared between registration path and Transactor
last_function_registration_sequence: CachePadded<AtomicU64>
function_registry: Arc<RwLock<FunctionRegistry>>
```

**Transactor hot cache:**

The Transactor maintains its own local copy of `FunctionRegistry`. It never reads from
`Arc<RwLock<FunctionRegistry>>` during transaction execution — that would introduce lock
contention on the hot path.

On every processing cycle the Transactor checks `last_function_registration_sequence`. If it
has advanced since the last check, the Transactor clones the registry from
`Arc<RwLock<FunctionRegistry>>` into its local copy. This clone happens outside the
transaction processing loop — never mid-transaction.

```rust
// Transactor cycle (simplified)
fn run_step(&mut self, ctx: &TransactorContext) {
    // Check for new function registrations
    let seq = ctx.last_function_registration_sequence();
    if seq > self.local_function_seq {
        let registry = ctx.function_registry().read().unwrap();
        self.local_functions = registry.clone();
        self.local_function_seq = seq;
    }

    // Process transactions using local_functions — no locks
    // ...
}
```

The Transactor always reads from `local_functions`. Zero lock contention on the execution path.

---

### Execution Path

```
Operation::Named { name, params: [i64; 8], user_ref }
  → Transactor receives
  → dedup check on user_ref (same as all operations)
  → lookup FunctionVersion from local_functions (no lock)
  → if not found or status=Unregistered → FailReason::INVALID_OPERATION
  → meta(tx_id, "NAMED   ", user_ref, timestamp)
  → WasmRuntime::execute(module, params, &mut self.balances)
      → create Store with host functions closing over balances
      → call execute(p1..p8)
      → WASM calls credit/debit/get_balance/check_negative_balance via host
      → entries accumulate in Transactor as normal
      → function returns i32
  → if return != 0 → rollback, FailReason::from_u8(return_value)
  → zero-sum invariant verified (as always)
  → entries written to WAL
```

---

### Unregister Function

```
gRPC UnregisterFunction(name)
  → lookup latest version in registry
  → write WAL record FunctionRegistered { status: 1 (unregistered), same name/version/crc }
  → Snapshot stage updates registry: mark version as Unregistered
  → Pipeline atomicLastFunctionRegistrationSequence incremented
  → Transactor picks up on next cycle: local_functions updated
  → subsequent Named { name } → FailReason::INVALID_OPERATION
```

Unregistration does not delete the `.wasm` file from disk — the WAL record and file remain
as audit trail. A subsequent `RegisterFunction` with `override=true` adds a new version
(version+1) and marks it active. Old versions remain in the registry with their status intact.

**Override flag on RegisterFunction:**

```protobuf
message RegisterFunctionRequest {
  string name     = 1;
  bytes  binary   = 2;   // WASM binary, max 4MB
  bool   override = 3;   // if true, adds new version even if name exists
}
```

Without `override=true`, registering an existing active name returns an error.

---

### Function Snapshot

Separate file from balance snapshot. Same Seal thread, same `snapshot_frequency` trigger,
same LZ4 compression and sidecar CRC pattern.

```
data/
  function_snapshot_{N}.bin
  function_snapshot_{N}.crc
```

**Format:**

```
Header (36 bytes):
  magic(4)          0x46554E43 "FUNC"
  version(1)        1
  segment_id(4)
  function_count(8) number of function records
  pad(19)

Body (per function, 40 bytes each, LZ4 compressed):
  name_len(1)
  name(16)          UTF-8, null-padded
  version(1)        latest version number
  status(1)         0=active, 1=unregistered
  crc32c(4)         CRC32C of WASM binary
  _pad(17)

Sidecar .crc (16 bytes):
  file_crc32c(4)
  file_size(8)
  magic(4)          0x46554E43
```

**Recovery:**

1. Load `function_snapshot_{N}.bin` → rebuild FunctionRegistry
2. Replay WAL records after snapshot → apply any `FunctionRegistered` records
3. For each active function: verify CRC32C of `.wasm` file on disk
4. Compile each active function via `WasmRuntime::compile` → modules ready
5. Publish registry to `Arc<RwLock<FunctionRegistry>>`
6. Increment `last_function_registration_sequence` once

---

### Operation::Composite Deprecation

`Composite` is deprecated by this ADR. It remains in the API for backward compatibility but is
marked deprecated in code and documentation.

```rust
#[deprecated(
    since = "0.2.0",
    note = "use Operation::Named with a registered WASM function"
)]
Composite(Box<CompositeOperation>)
```

Migration: any `Composite` operation is expressible as a WASM function. The Rust SDK provides
a template. Removal is planned in a future ADR once WASM is stable.

---

### gRPC API Additions

```protobuf
service Ledger {
  rpc RegisterFunction(RegisterFunctionRequest)     returns (RegisterFunctionResponse);
  rpc UnregisterFunction(UnregisterFunctionRequest) returns (UnregisterFunctionResponse);
  rpc ListFunctions(ListFunctionsRequest)           returns (ListFunctionsResponse);
}

message RegisterFunctionRequest {
  string name     = 1;
  bytes  binary   = 2;   // WASM binary, max 4MB (enforced by max_message_size_bytes)
  bool   override = 3;
}

message RegisterFunctionResponse {
  uint32 version = 1;
  uint32 crc32c  = 2;
}

message UnregisterFunctionRequest {
  string name = 1;
}

message UnregisterFunctionResponse {
  uint32 version = 1;   // version that was unregistered
}

message ListFunctionsRequest {}

message ListFunctionsResponse {
  repeated FunctionInfo functions = 1;
}

message FunctionInfo {
  string name    = 1;
  uint32 version = 2;
  uint32 crc32c  = 3;
  bool   active  = 4;
}
```

`Operation::Named` already exists in the proto (ADR-004) — no change needed for invocation.

---

### Storage Layout Addition

```
data/
  ...existing files...
  function_snapshot_000004.bin    ← new
  function_snapshot_000004.crc    ← new

functions/
  fee_calculation_v1.wasm
  fee_calculation_v2.wasm
  compliance_check_v1.wasm
```

The `functions/` directory lives alongside `data/`. Path:
`{storage.data_dir}/../functions/` or configurable via `StorageConfig`.

---

### Source Layout

```
src/
  wasm_runtime.rs       ← WasmRuntime struct, Engine, execute, validate_exports
  function_registry.rs  ← FunctionRegistry, FunctionVersion, FunctionStatus
  ...existing files...

Cargo.toml additions:
  wasmtime = { version = "...", features = ["cranelift"] }
```

---

## Consequences

### Positive

- User-defined financial logic without recompiling or redeploying the ledger
- Sandboxed — WASM cannot escape the host API
- Auditable — every registration is a WAL record with CRC, every invocation produces standard entries
- Versioned — multiple versions per function, override path, unregistration
- Language agnostic — Rust, AssemblyScript, C, Zig, any WASM target
- Atomicity guaranteed — failure causes full rollback, identical to built-in operations
- Transactor hot path lock-free — local cache, sequence check only
- Raft-compatible — leader executes, entries replicated, followers apply directly
- `Composite` deprecated — one extensibility model, not two
- Single WAL source of truth — function registration ordered relative to transactions
- `check_negative_balance` gives functions explicit overdraft control

### Negative

- wasmtime JIT compilation at registration — ~10-100ms per function (one-time cost)
- `Store` creation per Named operation — small allocation on hot path
- 16-byte name limit — intentional but constraining for verbose naming
- 8-param limit — covers all practical cases but not unlimited flexibility
- `Composite` deprecated but not removed — temporary API surface debt
- `functions/` directory adds operational surface to manage

### Neutral

- Registration bypasses Transactor — correct, consistent with crash recovery markers
- Function snapshot is a separate file — consistent with balance snapshot separation
- `WasmRuntime` is a separate struct/module — clean separation of concerns
- Unregistered functions remain on disk — audit trail preserved

---

## References

- ADR-001 — entries-based execution model, single execution in Transactor
- ADR-003 — Operation enum, Composite deprecation, Named reserved
- ADR-004 — gRPC interface, Named operation proto
- ADR-006 — WAL segment lifecycle, storage layout, snapshot format
- ADR-009 — FailReason, user-defined range 128-255
- ADR-011 — WAL write/commit separation, active segment direct write
- ADR-013 — transaction-count-based segments, active window
- wasmtime — https://github.com/bytecodealliance/wasmtime
- Bytecode Alliance — https://bytecodealliance.org
- WASM spec linear memory — https://webassembly.github.io/spec/core/syntax/modules.html#memories