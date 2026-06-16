# ADR-026: `roda-wasm-abi` — Guest-Side ABI Crate

**Status:** Proposed
**Date:** 2026-06-16
**Author:** Taleh Ibrahimli

**Amends:**
- ADR-014 — WASM Function Registry: provides the Rust guest bindings for the `execute` / `register`
  calling conventions and the `ledger` host imports a module must satisfy.
- ADR-023 — Programmable State: realizes the Future-Work `roda-abi` crate (published as
  `roda-wasm-abi`) against the KV host ABI as actually implemented (the flattened `[u32;4]` key /
  `i64` value verbs plus the `kv_register_constant` / `kv_get_constant` verbs and the `register()`
  phase, §6).

---

## Context

A roda-ledger transaction module is WASM (ADR-014): it exports one `execute` function with a fixed
8×`i64` → `i32` calling convention and imports a small set of host verbs under WASM module `ledger`
— balances (`credit` / `debit` / `get_balance`), accounts and flags (ADR-022), and the typed
key→value store (ADR-023 §5). Authoring that today means raw `extern "C"` import blocks, manual
unpacking of the eight `i64` slots, hand-built status codes, and bare host imports. This is
error-prone and has no library form; ADR-023's Future-Work section explicitly defers it to a
dedicated crate.

The KV host ABI is integer-flattened: `kv_get(k0..k3) -> i64` and `kv_set(k0..k3, value)` pass a
four-`u32` key and an `i64` value directly — the TLV encoding (ADR-023 §3) lives entirely host-side,
so the guest does no key/value byte packing. Constants are resolved **by name, statelessly**: the
module never caches an id in a global. `kv_register_constant(name)` creates a constant if absent
(register phase only), and `kv_get_constant(name) -> u32` resolves it during `execute`; if the name
is unknown the host fails the transaction rather than returning a bogus id. The guest binds these
verbs; the host (ADR-023 §6) provides them.

## Decision

Add **`crates/roda-wasm-abi`** — a guest-side ABI crate, the only intended way to write ledger
modules in Rust. It is **guest-only**: it binds the host ABI and changes no host code.

### 1. Crate shape

`#![no_std]`, no allocator, no dependencies — it compiles cleanly to `wasm32-unknown-unknown`. The
raw host imports live in a private `ffi` module behind `#[link(wasm_import_module = "ledger")]`;
on non-wasm targets they become panicking stubs so the library and its tests still build and link
on the host (CI runs the unit tests there). The public surface is safe wrappers (`credit`, `debit`,
`balance`, `linked_account`, flag verbs), the typed KV API, a `Params` accessor, a `Status` return
type, and the macros.

An optional, host-only `tools` feature (off by default, `#[cfg(not(target_arch = "wasm32"))]`) adds
`tools::compile_to_wasm`, which compiles guest source to a wasm module by spawning a throwaway Cargo
project that depends on this crate **by path** (local source, not a release), then deletes it. It is
test/tooling convenience; gating it off by default and out of wasm builds keeps the default crate
no_std and prevents the `std`/`cargo`-shelling helper from ever reaching a guest module.

### 2. No guest-side encoding

Because the KV verbs are integer-flattened, the guest does **no** key/value byte packing — there is
no TLV codec on the guest side and nothing to keep byte-compatible with `storage::kv`. A key is just
`[u32; 4]` and a value an `i64`. This keeps the crate dependency-free and removes a whole class of
wire-format drift; the only contract to honor is the integer arity, which is structural.

### 3. Ergonomics — `key!`, `execute!`, `register!`

`key!(...)` builds a four-component `[u32; 4]` key, zero-padding unused slots and casting each
argument to `u32` (so a resolved constant id or a literal works). `execute!(|p| { … })` emits the
`#[unsafe(no_mangle)] extern "C" fn execute(i64×8) -> i32` wrapper, hands the body a typed `Params`,
and accepts any `Into<Status>` return (`Status`, `Result<(), u8>`, or `()` for unconditional
commit). `register!(|| { … })` emits the `extern "C" fn register()` export where constant names are
registered. Two example modules (`examples/transfer`, `examples/counter`) are standalone wasm
crates, detached from the workspace via their own empty `[workspace]` table and built for
`wasm32-unknown-unknown`.

### 4. Stateless constants — resolve by name

The earlier design cached each constant id in a module global, filled in by `register()`. That kept
state in the module (and forced an awkward `Sync` global, since a host call can't run at WASM
static-init time). Instead the module stays **stateless**: the host owns the name→id map, and the
guest resolves by name at each use. Two verbs, both taking a `&CStr` (so names are C-string literals
like `c"PENDING"`, guaranteed null-terminated for the host to read from guest memory):

- `kv_register_constant(name)` — create-if-absent, callable **only** in the `register()` phase
  (host-enforced); idempotent.
- `kv_get_constant(name) -> u32` — resolve in `execute`. If the name was never registered the host
  **fails the transaction** (rather than returning a usable `0`), so the guest never proceeds on a
  bogus id and never has to panic-guard.

This drops the global, the `Sync` cell, and a whole class of state-management bugs; the cost is a
host lookup per use instead of a cached read.

### 5. Independent versioning and crates.io publishing

Unlike the internal crates, `roda-wasm-abi` sets its own `version` (not the workspace version) and
omits `publish = false`, so module authors can `cargo add roda-wasm-abi` and the crate can rev on
its own cadence. A tag-triggered workflow (`.github/workflows/publish-crate.yml`, tags
`roda-wasm-abi-v*`) builds + tests it, builds the examples to wasm, and runs `cargo publish`. The
crates.io namespace is flat (no repo scoping), so the `roda-` prefix is what scopes the crate to
the project — consistent with `roda-server` / `roda-ctl`.

## Consequences

**Positive**
- Writing a ledger module is now safe idiomatic Rust: no `extern "C"`, no manual slot unpacking, no
  bare status codes. The calling conventions and host imports live in one place.
- `no_std`, zero-dependency, allocation-free → tiny wasm output and a trivially publishable crate.
- Integer-flattened KV means no guest-side encoding, so there is no byte-level wire format to keep
  in sync with the host — only the integer arity, enforced structurally by the function signatures.

**Negative**
- The guest key space is fixed at four `u32` components; richer keys (the TLV `KeyPath` the host
  stores internally) are not expressible from the guest until the host ABI widens.
- End-to-end execution against a live ledger is not tested inside this crate (that would require
  touching `ledger`); coverage is the `key!`/`Params`/`Status` unit tests plus building both example
  modules to wasm, with manual e2e via a registered example `.wasm`.

**Neutral**
- The `register()` / `execute()` phase split is host-enforced; the crate mirrors it with the two
  macros and relies on the host to reject misuse (it traps and rolls back).
- Example crates sit outside the workspace, so they are built explicitly (CI / release), not by
  `cargo build --workspace`.

## References
- ADR-014 — WASM Function Registry and Function Operation Execution
- ADR-022 — Account Layouts and Program-Defined Accounts (flags, linked accounts)
- ADR-023 — Programmable State (KV map, §6 constants/`register()`)
- `crates/roda-wasm-abi/` — the crate (`src/{lib,ffi,account,kv,status}.rs`)
- `crates/ledger/src/transactor/wasm_runtime.rs` — host linker (`build_host_linker`, `HOST_MODULE`,
  `REGISTER_FN`, the constant verbs + register-phase guard)
- `.github/workflows/publish-crate.yml` — release publish
