# ADR-014: WASM Function Registry and Named Operation Execution

**Status:** Proposed  
**Date:** 2026-04-16  
**Author:** Taleh Ibrahimli

**Amends:**
- ADR-003 — deprecates `Operation::Composite`, implements `Operation::Named` execution path
- ADR-001 — adds `WalEntryKind::FunctionRegistered` (kind=5)
- ADR-006 — adds `function_snapshot_{N}.bin` to storage layout
- ADR-001, ADR-003, ADR-009, ADR-010 — renames `fail_reason` to `status` across all structs, APIs, and documentation (see Decision: Rename fail_reason to status)

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

### Decision: Rename fail_reason to status

`fail_reason` is renamed to `status` across the entire project. This is a breaking change
applied as part of this ADR.

**Rationale:** `fail_reason` implies failure is the primary case. `status` is more accurate —
it represents the outcome of a transaction, which may be success (`0`) or a specific condition
code. This aligns with how the field is actually used: success is `status = 0`, any non-zero
value is a condition that caused the transaction not to proceed.

**Scope of rename:**
- `TxMetadata.fail_reason` → `TxMetadata.status`
- `FailReason` type → `Status`
- `FailReason::NONE` → `Status::NONE`
- `FailReason::INSUFFICIENT_FUNDS` → `Status::INSUFFICIENT_FUNDS`
- All other `FailReason` constants follow the same pattern
- gRPC `fail_reason` fields → `status`
- All ADR references updated
- `transaction.fail_reason` → `transaction.status`

The `u8` representation, value ranges (0=success, 1-127 standard, 128-255 user-defined),
and all existing constant values are unchanged. This is a rename only.

---

### Host API Surface

The WASM function can only call three host functions. Nothing else is exposed:

```rust
// namespace: "ledger"
fn credit(account_id: u64, amount: u64);   // guaranteed, no return
fn debit(account_id: u64, amount: u64);    // guaranteed, no return
fn get_balance(account_id: u64) -> i64;
```

**`credit` and `debit` return nothing.** They are guaranteed operations — they update the
Transactor's balance Vec directly. They cannot fail individually. The zero-sum invariant is
enforced after execution completes, as with all operations.

**Negative balance handling is the responsibility of user code.** If the function requires
overdraft protection, it must call `get_balance` before or after applying credits/debits and
return a non-zero `Status` to trigger rollback. The ledger does not enforce negative balance
checks inside WASM functions — this is intentional, as some use cases (liability accounts,
internal system accounts) legitimately require negative balances.

Integer-only interface — no memory sharing, no pointers, no strings. All WASM value types
map directly to native types.

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
) -> u8;
```

```typescript
// AssemblyScript
export function execute(
    param1: i64, param2: i64, param3: i64, param4: i64,
    param5: i64, param6: i64, param7: i64, param8: i64,
): u8;
```

**Why exactly 8 params:** Fixed arity eliminates the need for linear memory pointer/length
passing. 8 `i64` values cover all practical financial use cases — account IDs, amounts, rates,
flags, timestamps. Unused params are passed as 0. This keeps the ABI simple, safe, and
verifiable at registration time.

**Return value:** `0` = success. Any non-zero value is a `Status` — the same `u8` type
used throughout the system. Values 0–127 map to standard `Status` constants. Values 128–255
are user-defined custom reasons, consistent with the existing design.

**Atomicity:** Functions execute atomically. If the function returns a non-zero status,
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
) -> u8 {
    let sender   = param1 as u64;
    let receiver = param2 as u64;
    let fee_acct = param3 as u64;
    let amount   = param4 as u64;
    let fee      = (amount * param5 as u64) / 10_000;

    unsafe {
        credit(sender, amount + fee);
        debit(receiver, amount);
        debit(fee_acct, fee);
    }
    0 // success
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

export function execute(
    param1: i64, param2: i64, param3: i64, param4: i64,
    param5: i64, _p6: i64, _p7: i64, _p8: i64,
): u8 {
    const sender   = param1 as u64;
    const receiver = param2 as u64;
    const feeAcct  = param3 as u64;
    const amount   = param4 as u64;
    const fee      = (amount * (param5 as u64)) / 10_000;

    credit(sender, amount + fee);
    debit(receiver, amount);
    debit(feeAcct, fee);

    return 0; // success
}
```

---

### Determinism

Functions must be pure and deterministic — same params always produce the same credits/debits.
This is enforced architecturally:

- Host API exposes no randomness, no wall-clock time, no I/O
- No WASM threads, no atomics
- wasmtime fuel metering can limit execution cycles

**Future Raft replication:** when distributed replication is added, only the leader will
execute WASM functions. Entries produced by execution will be replicated as binary WAL data
to followers. Followers will apply entries directly without re-executing the function.
Determinism is required for this model to be correct — non-deterministic functions would
produce divergent entries across replicas.

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
    pub entry_type: u8,        // 1 @ 0  — WalEntryKind::FunctionRegistered (5)
    pub _pad0:      u8,        // 1 @ 1
    pub version:    u16,       // 2 @ 2  — monotonic per name, starts at 1, max 65535
    pub crc32c:     u32,       // 4 @ 4  — CRC32C of WASM binary; 0 = unregistered
    pub name:       [u8; 32],  // 32 @ 8 — snake_case, UTF-8, null-padded, max 32 bytes
}                              // total: 40 bytes
```

**Name constraints:** snake_case, ASCII alphanumeric + underscore, must start with a letter.
Validated at registration time. Max 32 bytes — sufficient for any meaningful Rust function name.
Null-padded to fill the field. No length field needed — scan to first null byte.

**`crc32c = 0` signals unregistered.** Two signals always agree:
- WAL record has `crc32c = 0`
- File on disk is 0 bytes (empty file)

On recovery, if they disagree, the WAL record is the source of truth.

File path is derived deterministically from name and version:
```
functions/{name}_v{version}.wasm
```

No path stored in WAL — always reconstructible from name and version.

---

### WasmRuntime

`WasmRuntime` is `Arc<WasmRuntime>` in the pipeline — shared between the Ledger facade,
Snapshot stage, and Transactor. All wasmtime concerns are encapsulated in `src/wasm_runtime.rs`.

```rust
pub struct WasmRuntime {
    engine:     wasmtime::Engine,
    handlers:   RwLock<HandlerMap>,        // name → FunctionCaller
    update_seq: AtomicU32,                 // starts at 0, increments on every change
}

/// Carries both the CRC32C of the WASM binary and the callable handler.
/// CRC32C is embedded in TxMetadata.tag at execution time — single map lookup
/// gives both the audit identifier and the callable, zero extra lookups.
pub struct FunctionCaller {
    pub crc32c:  u32,
    pub handler: Arc<dyn Fn([i64; 8]) -> u8 + Send + Sync>,
}

pub type HandlerMap = HashMap<String, FunctionCaller>;
```

**Public API:**

```rust
impl WasmRuntime {
    // Validates binary — checks execute(i64×8)->u8 export is present and well-formed.
    // Called by Ledger before writing binary to disk during register_function.
    pub fn validate(&self, binary: &[u8]) -> Result<(), std::io::Error>;

    // Validates CRC32C and binary content, compiles, inserts FunctionCaller into HandlerMap.
    // Increments update_seq. crc32c is stored in FunctionCaller for use in TxMetadata.tag.
    // Called by Snapshot stage when WalEntry::FunctionRegistered is committed.
    pub fn load_function(&self, name: &str, binary: &[u8], crc32c: u32) -> Result<(), std::io::Error>;

    // Removes handler from HandlerMap. Increments update_seq.
    // Called by Snapshot stage when WalEntry::FunctionRegistered { crc32c: 0 } is committed.
    pub fn unload_function(&self, name: &str) -> Result<(), std::io::Error>;

    // Returns current update sequence number.
    pub fn last_update_seq(&self) -> u32;

    // Returns owned clone of handler map. Called by Transactor on seq change.
    pub fn get_handler_map(&self) -> HandlerMap;
}
```

`WasmRuntime` has no knowledge of disk, WAL, or pipeline. It only manages compiled handlers
in memory. `validate` is called by `Ledger` before disk write. `load_function` is called by
the Snapshot stage after WAL commit — it validates CRC and binary, then compiles and stores
the handler with its CRC32C for embedding in `TxMetadata.tag` during execution.

One `Engine` per `Ledger` — shared, thread-safe, JIT compiled once per function at registration.
One `Store` per execution — created inside `FunctionCaller`, dropped immediately after.

`Engine` is `Send + Sync`. `WasmRuntime` is `Send + Sync`. `FunctionCaller` is `Send + Sync`.

**Function storage** is handled by `src/storage/functions.rs` — separate from `WasmRuntime`.
`WasmRuntime` receives binary data; it does not read or write disk directly.

---

### Transactor Hot Cache

The Transactor holds an owned local copy of `HandlerMap`. It never acquires the `RwLock`
during transaction execution — zero lock contention on the hot path.

On every **cycle** (not every step) the Transactor checks `wasm_runtime.last_update_seq()`.
If the seq has advanced, it calls `wasm_runtime.get_handler_map()` which acquires a read lock,
clones the map, and returns. The clone is rare — only on function registration or unregistration.

```rust
// Transactor cycle (simplified)
fn run_step(&mut self, ctx: &TransactorContext) {
    // Per-cycle check — single atomic load, no lock
    let seq = ctx.wasm_runtime().last_update_seq();
    if seq != self.local_handler_seq {
        self.local_handlers = ctx.wasm_runtime().get_handler_map(); // owned clone
        self.local_handler_seq = seq;
    }

    // Process transactions using local_handlers — no locks, no Arc overhead
    // ...
}
```

---

### Registration Flow

```
ledger.register_function(name, binary, override)        ← Ledger owns this flow
  → if function exists and override=false → return error
  → wasm_runtime.validate(binary) → check execute(i64×8)->u8 export
  → storage/functions.rs: write functions/{name}_v{N}.wasm to disk
  → push WalEntry::FunctionRegistered onto pipeline WAL input queue
  → return version number to caller
```

```
Snapshot stage receives WalEntry::FunctionRegistered
  → storage/functions.rs: read functions/{name}_v{version}.wasm from disk → binary
  → wasm_runtime.load_function(name, binary, entry.crc32c)
      → verify CRC32C(binary) == crc32c → reject if mismatch
      → validate binary (execute(i64×8)->u8 export)
      → compile via Engine
      → insert FunctionCaller { crc32c, handler } into HandlerMap
      → increment update_seq
```

Transactor picks up on next cycle — `update_seq` changed → clone handlers → ready.

**Terminology:**
- `register_function` — Ledger operation: validation + disk write + WAL entry. Async.
- `load_function` — WasmRuntime operation: compiles binary, inserts handler into memory. Called by Snapshot on commit.

---

### Execution Flow

```
Operation::Named { name, params: [i64; 8], user_ref }
  → Transactor receives
  → dedup check on user_ref (same as all operations)
  → lookup FunctionCaller from local_handlers by name
  → if not found → Status::INVALID_OPERATION
  → build tag: [b'f', b'n', b'w', b'\n', crc32c[0..4]]
      caller.crc32c identifies exactly which function version executed
      cross-reference with FunctionRegistered WAL records for full audit
  → meta(tx_id, tag, user_ref, timestamp)
  → (caller.handler)(params) → returns u8
      → WASM calls credit/debit/get_balance via host functions
      → host functions update self.balances directly
      → entries accumulate in Transactor as normal
  → if return != 0 → rollback, Status::from_u8(return_value)
  → zero-sum invariant verified (as always)
  → entries written to WAL
```

Tag format in WAL output (`roda-ctl unpack`):
```json
{"type":"TxMetadata","tx_id":441001,"tag":"fnw\\n4a2f1c3d",...}
```

`"fnw\n"` — human-readable marker identifying a Named/WASM call.
`4a2f1c3d` — CRC32C of the WASM binary, identifies exact version executed.
Auditors cross-reference with `FunctionRegistered` records to resolve name and version.

---

### Unregister Flow

```
ledger.unregister_function(name)                        ← Ledger owns this flow
  → storage/functions.rs: truncate functions/{name}_v{N}.wasm to 0 bytes
  → push WalEntry::FunctionRegistered { crc32c: 0, name, version+1 } onto pipeline WAL input queue
  → return  ← handler still active until WAL entry is committed
```

```
Snapshot stage receives WalEntry::FunctionRegistered { crc32c: 0 }
  → wasm_runtime.unload_function(name)
      → remove FunctionCaller from HandlerMap
      → increment update_seq
```

Transactor picks up on next cycle — handler gone → subsequent `Named { name }` →
`Status::INVALID_OPERATION`.

**Terminology:**
- `unregister_function` — Ledger operation: disk truncation + WAL entry. Async.
- `unload_function` — WasmRuntime operation: removes handler from memory. Called by Snapshot on commit.

Two signals always agree after unregistration:
- WAL record `crc32c = 0`
- File on disk is 0 bytes — truncated, not deleted, audit trail preserved

WAL record is the source of truth on recovery if signals disagree.

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
  name(32)          snake_case, UTF-8, null-padded
  version(2)        latest version number, u16
  crc32c(4)         CRC32C of WASM binary; 0 = unregistered
  _pad(2)

Sidecar .crc (16 bytes):
  file_crc32c(4)
  file_size(8)
  magic(4)          0x46554E43
```

**Recovery:**

1. Load `function_snapshot_{N}.bin` → for each function record with `crc32c != 0`:
   - Read `functions/{name}_v{version}.wasm` from disk → binary
   - `wasm_runtime.load_function(name, binary, snapshot.crc32c)`
     → verifies CRC and binary internally, aborts recovery on mismatch
2. Replay WAL `FunctionRegistered` records after snapshot:
   - If `crc32c != 0` → read binary from disk → `wasm_runtime.load_function(name, binary, entry.crc32c)`
   - If `crc32c == 0` → `wasm_runtime.unload_function(name)`
3. `update_seq` reflects final state after all loads/unloads

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
  wasm_runtime.rs          ← WasmRuntime, FunctionCaller, HandlerMap
  storage/
    functions.rs           ← read/write .wasm files, CRC32C, truncate on unregister
    ...existing files...
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
- Versioned — multiple versions per function, override path, clean unregistration
- Language agnostic — Rust, AssemblyScript, C, Zig, any WASM target
- Atomicity guaranteed — failure causes full rollback, identical to built-in operations
- Transactor hot path lock-free — local cache, sequence check only
- Future Raft compatible — leader executes, entries replicated, followers apply directly
- `Composite` deprecated — one extensibility model, not two
- Single WAL source of truth — function registration ordered relative to transactions
- Dual-signal unregistration — WAL `crc32c=0` + 0-byte file, WAL wins on conflict
- No `name_len` or status flag fields — simpler struct, less surface area
- 32-byte name — sufficient for any meaningful snake_case Rust function name
- `fail_reason` → `status` rename — clearer semantics project-wide

### Negative

- wasmtime JIT compilation at registration — ~10-100ms per function (one-time cost)
- `Store` creation per Named operation — small allocation on hot path
- 8-param limit — covers all practical cases but not unlimited flexibility
- `Composite` deprecated but not removed — temporary API surface debt
- `functions/` directory adds operational surface to manage
- `fail_reason` → `status` is a breaking rename across all structs, APIs, and proto fields

### Neutral

- Registration bypasses Transactor — correct, consistent with crash recovery markers
- Function snapshot is a separate file — consistent with balance snapshot separation
- `WasmRuntime` is a separate struct/module — clean separation of concerns
- Unregistered function files truncated to 0 bytes — audit trail preserved, no deletion

---

## References

- ADR-001 — entries-based execution model, single execution in Transactor
- ADR-003 — Operation enum, Composite deprecation, Named reserved
- ADR-004 — gRPC interface, Named operation proto
- ADR-006 — WAL segment lifecycle, storage layout, snapshot format
- ADR-009 — Status (formerly FailReason), user-defined range 128-255
- ADR-011 — WAL write/commit separation, active segment direct write
- ADR-013 — transaction-count-based segments, active window
- wasmtime — https://github.com/bytecodealliance/wasmtime
- Bytecode Alliance — https://bytecodealliance.org
- WASM spec linear memory — https://webassembly.github.io/spec/core/syntax/modules.html#memories