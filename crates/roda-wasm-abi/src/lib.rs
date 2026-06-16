//! Guest-side ABI for writing [roda-ledger](https://github.com/yourname/roda-ledger)
//! WASM transaction modules in Rust (ADR-026).
//!
//! A ledger module exports an `execute` function with a fixed 8×`i64` calling
//! convention and an `i32` status return, optionally a `register` function for
//! defining constants, and imports a small set of host verbs under WASM module
//! `ledger` (balances, accounts, flags, and a typed KV store). Writing that by
//! hand means raw `extern "C"`, manual parameter unpacking, and bare status
//! codes. This crate wraps all of it: safe verb wrappers, a [`key!`] builder,
//! and the [`execute!`] / [`register!`] macros that emit the exports.
//!
//! `#![no_std]`, no allocator, no dependencies — it compiles cleanly to
//! `wasm32-unknown-unknown`.
//!
//! # Example
//!
//! A balanced transfer — debit `account(0)` and credit `account(2)` by
//! `amount(1)`:
//!
//! ```ignore
//! use roda_wasm_abi::{credit, debit, execute, Status};
//!
//! execute!(|p| {
//!     let amount = p.amount(1);
//!     debit(p.account(0), amount);
//!     credit(p.account(2), amount);
//!     Status::OK
//! });
//! ```
//!
//! A KV counter scoped under a named constant (see `examples/counter`). The
//! module is stateless: register the name once, then resolve it by name in
//! `execute` — the host owns the name→id map.
//!
//! ```ignore
//! use roda_wasm_abi::{execute, key, kv_get, kv_get_constant, kv_register_constant, kv_set, register, Status};
//!
//! register!(|| {
//!     kv_register_constant(c"counter");
//! });
//!
//! execute!(|p| {
//!     let k = key!(kv_get_constant(c"counter"), p.account(0));
//!     kv_set(k, kv_get(k) + 1);
//!     Status::OK
//! });
//! ```

#![no_std]

// `std` is needed only for the host-only `tools` helper and for tests.
#[cfg(any(test, all(feature = "tools", not(target_arch = "wasm32"))))]
extern crate std;

mod account;
mod ffi;
mod kv;
mod status;

/// Host-only build helper (feature `tools`); never compiled for wasm.
#[cfg(all(feature = "tools", not(target_arch = "wasm32")))]
pub mod tools;

pub use account::{balance, credit, debit, get_flag, has_flag, linked_account, set_flag};
pub use kv::{kv_get, kv_get_constant, kv_register_constant, kv_set};
pub use status::Status;

/// The eight `i64` arguments passed to `execute`, with typed accessors.
///
/// Slots are positional and zero-padded by the caller; interpret each one as
/// the op needs — an account id, an amount, or a raw integer.
#[derive(Clone, Copy, Debug)]
pub struct Params([i64; 8]);

impl Params {
    /// Wrap the raw 8-slot argument array (used by [`execute!`]).
    pub const fn new(raw: [i64; 8]) -> Self {
        Params(raw)
    }

    /// Raw signed value of slot `i` (0..=7).
    pub const fn get(&self, i: usize) -> i64 {
        self.0[i]
    }

    /// Slot `i` interpreted as an account id.
    pub const fn account(&self, i: usize) -> u64 {
        self.0[i] as u64
    }

    /// Slot `i` interpreted as an amount.
    pub const fn amount(&self, i: usize) -> u64 {
        self.0[i] as u64
    }

    /// All eight slots.
    pub const fn raw(&self) -> [i64; 8] {
        self.0
    }
}

/// Build a four-component KV key, zero-padding the unused trailing slots.
///
/// Each argument is cast to `u32` (so a constant id from [`kv_get_constant`] or
/// an integer literal works directly).
///
/// ```
/// use roda_wasm_abi::key;
/// assert_eq!(key!(7), [7, 0, 0, 0]);
/// assert_eq!(key!(1, 2), [1, 2, 0, 0]);
/// assert_eq!(key!(1, 2, 3, 4), [1, 2, 3, 4]);
/// ```
#[macro_export]
macro_rules! key {
    ($a:expr $(,)?) => {
        [$a as u32, 0u32, 0u32, 0u32]
    };
    ($a:expr, $b:expr $(,)?) => {
        [$a as u32, $b as u32, 0u32, 0u32]
    };
    ($a:expr, $b:expr, $c:expr $(,)?) => {
        [$a as u32, $b as u32, $c as u32, 0u32]
    };
    ($a:expr, $b:expr, $c:expr, $d:expr $(,)?) => {
        [$a as u32, $b as u32, $c as u32, $d as u32]
    };
}

/// Define a module's `execute` export.
///
/// Wraps the fixed 8×`i64` → `i32` calling convention: the body receives a
/// [`Params`] and returns anything convertible to [`Status`] (a `Status`, a
/// `Result<(), u8>`, or `()` for unconditional commit).
///
/// ```ignore
/// use roda_wasm_abi::{credit, debit, execute, Status};
///
/// execute!(|p| {
///     debit(p.account(0), p.amount(1));
///     credit(p.account(2), p.amount(1));
///     Status::OK
/// });
/// ```
#[macro_export]
macro_rules! execute {
    (|$p:ident| $body:block) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn execute(
            p0: i64,
            p1: i64,
            p2: i64,
            p3: i64,
            p4: i64,
            p5: i64,
            p6: i64,
            p7: i64,
        ) -> i32 {
            let $p = $crate::Params::new([p0, p1, p2, p3, p4, p5, p6, p7]);
            let __status: $crate::Status = $body.into();
            __status.code() as i32
        }
    };
}

/// Define a module's `register` export (ADR-023 §6).
///
/// The host calls it once per instantiation (and after any re-instantiation).
/// Only [`kv_register_constant`] may be called inside — register each constant
/// name your `execute` will later resolve with [`kv_get_constant`]. The module
/// keeps no state of its own; the host owns the name→id map.
///
/// ```ignore
/// use roda_wasm_abi::{kv_register_constant, register};
///
/// register!(|| {
///     kv_register_constant(c"PENDING");
/// });
/// ```
#[macro_export]
macro_rules! register {
    (|| $body:block) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn register() {
            $body
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_accessors() {
        let p = Params::new([10, 5, 20, 0, 0, 0, 0, 0]);
        assert_eq!(p.account(0), 10);
        assert_eq!(p.amount(1), 5);
        assert_eq!(p.get(2), 20);
        assert_eq!(p.raw()[0], 10);
    }

    #[test]
    fn key_builder_zero_pads() {
        assert_eq!(key!(7), [7, 0, 0, 0]);
        assert_eq!(key!(1, 2), [1, 2, 0, 0]);
        assert_eq!(key!(1, 2, 3), [1, 2, 3, 0]);
        assert_eq!(key!(1, 2, 3, 4), [1, 2, 3, 4]);
        // Components are cast to u32, so a u64 account id truncates explicitly.
        let id: u32 = 9;
        assert_eq!(key!(id, 100u64), [9, 100, 0, 0]);
    }

    #[test]
    fn status_mapping() {
        assert_eq!(Status::OK.code(), 0);
        assert_eq!(Status::fail(7).code(), 7);
        assert_eq!(Status::from(()).code(), 0);
        assert_eq!(Status::from(Ok::<(), u8>(())).code(), 0);
        assert_eq!(Status::from(Err::<(), u8>(255)).code(), 255);
        assert_eq!(Status::from(127u8).code(), 127);
    }
}
