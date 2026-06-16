//! Safe wrappers over the programmable KV host verbs (ADR-023).
//!
//! A key is four `u32` components (build one with [`crate::key!`], which
//! zero-pads); the value is a signed `i64`. Reads return `0` for an absent key.
//! Constants name a stable `u32` id usable as a key component: register them by
//! name in a module's [`crate::register!`] export, then resolve them by name in
//! `execute`. The module stays stateless — the host owns the name→id map.

use crate::ffi;

/// Read the value at `key`; `0` if the key is absent.
pub fn kv_get(key: [u32; 4]) -> i64 {
    unsafe { ffi::kv_get(key[0], key[1], key[2], key[3]) }
}

/// Set `key` to `value`.
pub fn kv_set(key: [u32; 4], value: i64) {
    unsafe { ffi::kv_set(key[0], key[1], key[2], key[3], value) }
}

/// Register a constant by name (create-if-absent), so it can be resolved later
/// with [`kv_get_constant`] (ADR-023 §6).
///
/// Idempotent: registering the same name twice is a no-op. Callable **only**
/// from a module's [`crate::register!`] export — calling it from `execute` traps
/// and rolls the transaction back. Pass a C string literal, e.g.
/// `kv_register_constant(c"PENDING")`.
pub fn kv_register_constant(name: &core::ffi::CStr) {
    unsafe { ffi::kv_register_constant(name.as_ptr() as *const u8) }
}

/// Resolve a registered constant's stable `u32` id, for use as a key component.
///
/// Use it in `execute`, by name — the module keeps no state of its own. If the
/// name was never registered the host **fails the transaction** (rather than
/// returning a bogus id), so a `0` never reaches your code. Pass a C string
/// literal, e.g. `kv_get_constant(c"PENDING")`.
pub fn kv_get_constant(name: &core::ffi::CStr) -> u32 {
    unsafe { ffi::kv_get_constant(name.as_ptr() as *const u8) }
}
