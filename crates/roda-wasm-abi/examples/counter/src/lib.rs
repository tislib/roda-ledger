//! KV counter module: increment a counter stored under a named-constant
//! namespace, then move the new count from the system account into the caller's
//! account — a balanced transfer that passes the zero-sum check.
//!
//! Stateless: `register` registers the `counter` name; `execute` resolves it by
//! name each call with `kv_get_constant` — the module holds no state of its own.

use roda_wasm_abi::{
    Status, credit, debit, execute, key, kv_get, kv_get_constant, kv_register_constant, kv_set,
    register,
};

register!(|| {
    kv_register_constant(c"counter");
});

execute!(|p| {
    let account = p.account(0);
    let k = key!(kv_get_constant(c"counter"), account);

    let next = kv_get(k) + 1;
    kv_set(k, next);

    debit(0, next as u64); // into the system account
    credit(account, next as u64); // out of the account
    Status::OK
});
