//! Raw host imports under WASM module `ledger` (ADR-014, ADR-022, ADR-023).
//!
//! On `wasm32` these resolve to the host shims the ledger wires into the
//! wasmtime linker. On any other target they are stubs that panic, so the
//! library (and its tests) still build and link on the host — the verbs are
//! only meaningful inside the ledger's WASM sandbox.
//!
//! This module is private; only the safe wrappers in `account` / `kv` are part
//! of the public API.

#[cfg(target_arch = "wasm32")]
mod imp {
    #[link(wasm_import_module = "ledger")]
    unsafe extern "C" {
        pub fn credit(account: u64, amount: u64);
        pub fn debit(account: u64, amount: u64);
        pub fn get_balance(account: u64) -> i64;
        pub fn linked_account(account: u64, type_id: u32) -> u64;
        pub fn get_flag(account: u64, lane: u32) -> u32;
        pub fn has_flag(account: u64, lane: u32, value: u32) -> u32;
        pub fn set_flag(account: u64, lane: u32, value: u32);
        // KV key is four u32 components; value is i64 (0 = absent on read).
        pub fn kv_get(k0: u32, k1: u32, k2: u32, k3: u32) -> i64;
        pub fn kv_set(k0: u32, k1: u32, k2: u32, k3: u32, value: i64);
        // Constants by null-terminated UTF-8 name. `register` creates-if-absent
        // (register phase only); `get` returns the id (host fails the tx if the
        // name is unknown).
        pub fn kv_register_constant(name_ptr: *const u8);
        pub fn kv_get_constant(name_ptr: *const u8) -> u32;
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    fn host_only() -> ! {
        panic!("roda-wasm-abi host verbs are only available on wasm32 targets")
    }

    pub unsafe fn credit(_account: u64, _amount: u64) {
        host_only()
    }
    pub unsafe fn debit(_account: u64, _amount: u64) {
        host_only()
    }
    pub unsafe fn get_balance(_account: u64) -> i64 {
        host_only()
    }
    pub unsafe fn linked_account(_account: u64, _type_id: u32) -> u64 {
        host_only()
    }
    pub unsafe fn get_flag(_account: u64, _lane: u32) -> u32 {
        host_only()
    }
    pub unsafe fn has_flag(_account: u64, _lane: u32, _value: u32) -> u32 {
        host_only()
    }
    pub unsafe fn set_flag(_account: u64, _lane: u32, _value: u32) {
        host_only()
    }
    pub unsafe fn kv_get(_k0: u32, _k1: u32, _k2: u32, _k3: u32) -> i64 {
        host_only()
    }
    pub unsafe fn kv_set(_k0: u32, _k1: u32, _k2: u32, _k3: u32, _value: i64) {
        host_only()
    }
    pub unsafe fn kv_register_constant(_name_ptr: *const u8) {
        host_only()
    }
    pub unsafe fn kv_get_constant(_name_ptr: *const u8) -> u32 {
        host_only()
    }
}

pub(crate) use imp::*;
