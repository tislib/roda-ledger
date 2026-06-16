# roda-wasm-abi

Guest-side ABI for writing [roda-ledger](https://github.com/yourname/roda-ledger)
WASM transaction modules in Rust.

A ledger module exports one `execute` function (fixed 8×`i64` → `i32`) and imports
a small set of host verbs under WASM module `ledger`: balances, accounts, flags,
and a typed key→value store. Hand-writing that means raw `extern "C"`, manual
argument unpacking, and hand-rolled TLV key bytes. This crate wraps all of it:

- **safe verb wrappers** — `credit`, `debit`, `balance`, `linked_account`, flags;
- **a typed KV API** — `kv_get` / `kv_set` with a `key!` builder, plus named constants;
- **the `execute!` / `register!` macros** — emit the exports and hand you a typed `Params`.

`#![no_std]`, no allocator, no dependencies — compiles cleanly to
`wasm32-unknown-unknown`.

## Add it

```toml
[dependencies]
roda-wasm-abi = "0.1"

[lib]
crate-type = ["cdylib"]
```

## Write a module

```rust,ignore
use roda_wasm_abi::{credit, debit, execute, Status};

// Balanced transfer: debit account(0), credit account(2), by amount(1).
execute!(|p| {
    let amount = p.amount(1);
    debit(p.account(0), amount);
    credit(p.account(2), amount);
    Status::OK
});
```

Return `Status::OK` to commit (subject to the zero-sum check) or
`Status::fail(code)` to roll the whole transaction back. The body may also
return a `Result<(), u8>` or `()`.

### KV state

A key is four `u32` components (build one with `key!`, which zero-pads); the
value is an `i64`, and a read returns `0` for an absent key.

```rust,ignore
use roda_wasm_abi::{execute, key, kv_get, kv_get_constant, kv_register_constant, kv_set, register, Status};

// Register the name once, in the host-called `register` phase.
register!(|| {
    kv_register_constant(c"counter");
});

// Resolve it by name each call — the module keeps no state of its own.
execute!(|p| {
    let k = key!(kv_get_constant(c"counter"), p.account(0));
    kv_set(k, kv_get(k) + 1);
    Status::OK
});
```

A constant names a stable `u32` id for use as a key component. `kv_register_constant`
(register phase only) creates it if absent; `kv_get_constant` resolves it by name in
`execute`. If you resolve a name that was never registered, the host **fails the
transaction** rather than returning a bogus id — so a `0` never reaches your code.

## Build the examples

```bash
rustup target add wasm32-unknown-unknown
cargo build --release --target wasm32-unknown-unknown \
  --manifest-path crates/roda-wasm-abi/examples/transfer/Cargo.toml
cargo build --release --target wasm32-unknown-unknown \
  --manifest-path crates/roda-wasm-abi/examples/counter/Cargo.toml
```

The resulting `.wasm` is what you register with the ledger
(`RegisterFunction` / `roda-ctl`).

## Compile a module programmatically (`tools` feature)

For tests and tooling, the host-only `tools` feature compiles module source to
wasm in-process — it spins up a throwaway Cargo project that depends on this
crate by path, builds it, returns the bytes, and cleans up:

```rust,ignore
// Cargo.toml: roda-wasm-abi = { version = "0.1", features = ["tools"] }
let wasm = roda_wasm_abi::tools::compile_to_wasm(r#"
    use roda_wasm_abi::{credit, debit, execute, Status};
    execute!(|p| {
        debit(p.account(0), p.amount(1));
        credit(p.account(2), p.amount(1));
        Status::OK
    });
"#)?;
assert_eq!(&wasm[..4], b"\0asm");
```

The feature pulls in `std` and shells out to `cargo`, so it is off by default
and never compiled for wasm targets — the default crate stays `no_std`. Requires
the `wasm32-unknown-unknown` target installed.

## Design

See [ADR-026](../../docs/adr/0026-roda-wasm-abi.md) for the crate's design and
[ADR-023](../../docs/adr/0023-programmable-state.md) for the KV host ABI it binds.

License: Apache-2.0
