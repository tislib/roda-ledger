//! Balanced transfer module: debit `account(0)` and credit `account(2)` by
//! `amount(1)`. Nets to zero, so the zero-sum check passes.

use roda_wasm_abi::{Status, credit, debit, execute};

execute!(|p| {
    let amount = p.amount(1);
    debit(p.account(0), amount);
    credit(p.account(2), amount);
    Status::OK
});
