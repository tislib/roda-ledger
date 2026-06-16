//! Programmable KV state — billing scenario (ADR-023).
//!
//! Modules are written in Rust against `roda-wasm-abi` and compiled to wasm via
//! `compile_to_wasm` (no hand-written WAT). They build a tariff/discount model
//! in KV, then charge usage:
//!   tariff value:   "tariffs" / tariff_id                                = price/unit
//!   user discount:  "tariffs" / tariff_id / "account_discount" / account = percent off
//!
//! Each module declares its namespace constants in `register()` and resolves
//! them at call time with `kv_get_constant` (stateless — no globals). The test
//! verifies stored KV by querying `Ledger::get_kv` directly — no account misuse.
//!
//! Shells out to `cargo build --target wasm32-unknown-unknown`; self-skips when
//! that target isn't installed.

use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transactor::transaction::Operation;
use storage::entities::FailReason;
use storage::{KeyPath, Value};

/// register_tariff(tariff_id = p0, value = p1): kv["tariffs"/tariff_id] = value.
const REGISTER_TARIFF_SRC: &str = r#"
    use roda_wasm_abi::{execute, key, kv_get_constant, kv_register_constant, kv_set, register, Status};
    register!(|| { kv_register_constant(c"tariffs"); });
    execute!(|p| {
        kv_set(key!(kv_get_constant(c"tariffs"), p.get(0)), p.get(1));
        Status::OK
    });
"#;

/// register_account_discount(account = p0, tariff_id = p1, pct = p2):
///   kv["tariffs"/tariff_id/"account_discount"/account] = pct.
const REGISTER_DISCOUNT_SRC: &str = r#"
    use roda_wasm_abi::{execute, key, kv_get_constant, kv_register_constant, kv_set, register, Status};
    register!(|| {
        kv_register_constant(c"tariffs");
        kv_register_constant(c"account_discount");
    });
    execute!(|p| {
        let k = key!(
            kv_get_constant(c"tariffs"),
            p.get(1),
            kv_get_constant(c"account_discount"),
            p.account(0)
        );
        kv_set(k, p.get(2));
        Status::OK
    });
"#;

/// charge_usage(account = p0, count = p1, tariff_id = p2):
///   unit = tariff - tariff*pct/100; amount = unit*count;
///   credit(account, amount); debit(SYSTEM, amount).
const CHARGE_USAGE_SRC: &str = r#"
    use roda_wasm_abi::{credit, debit, execute, key, kv_get, kv_get_constant, kv_register_constant, register, Status};
    register!(|| {
        kv_register_constant(c"tariffs");
        kv_register_constant(c"account_discount");
    });
    execute!(|p| {
        let t = kv_get_constant(c"tariffs");
        let d = kv_get_constant(c"account_discount");
        let tariff = kv_get(key!(t, p.get(2)));
        let pct = kv_get(key!(t, p.get(2), d, p.account(0)));
        let unit = tariff - tariff * pct / 100;
        let amount = (unit * p.get(1)) as u64;
        credit(p.account(0), amount);
        debit(0, amount);
        Status::OK
    });
"#;

fn wasm_ready() -> bool {
    std::process::Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).contains("wasm32-unknown-unknown"))
        .unwrap_or(false)
}

fn compile(src: &str) -> Vec<u8> {
    roda_wasm_abi::tools::compile_to_wasm(src).expect("compile module to wasm")
}

fn call(ledger: &Ledger, name: &str, params: [i64; 8]) -> FailReason {
    ledger
        .submit_and_wait_result(Operation::Function {
            name: name.to_string(),
            params,
            user_ref: 0,
        })
        .get_fail_reason()
}

#[test]
fn kv_billing_tariffs_discounts_and_usage() {
    if !wasm_ready() {
        eprintln!("skipping: wasm32-unknown-unknown target not installed");
        return;
    }
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().expect("start");
    ledger.open_accounts(20);

    ledger
        .register_function("register_tariff", &compile(REGISTER_TARIFF_SRC), false)
        .expect("register register_tariff");
    ledger
        .register_function(
            "register_account_discount",
            &compile(REGISTER_DISCOUNT_SRC),
            false,
        )
        .expect("register register_account_discount");
    ledger
        .register_function("charge_usage", &compile(CHARGE_USAGE_SRC), false)
        .expect("register charge_usage");

    // Fund users 1..=10 with 1000 each.
    for a in 1..=10 {
        assert_eq!(
            ledger
                .submit_and_wait_result(Operation::Deposit {
                    account: a,
                    amount: 1000,
                    user_ref: 0,
                })
                .get_fail_reason(),
            FailReason::NONE
        );
    }

    // A tariff identifier chosen by the test (a domain id, passed as a param —
    // the modules never hardcode it, and the interned constant ids stay opaque).
    let tariff_id: i64 = 1;

    // Tariff costs 10 per unit.
    assert_eq!(
        call(
            &ledger,
            "register_tariff",
            [tariff_id, 10, 0, 0, 0, 0, 0, 0]
        ),
        FailReason::NONE
    );

    // Charge users 1..=5 three times at count = 2, no discount → 10*2 = 20 each → -60.
    for _ in 0..3 {
        for a in 1..=5 {
            assert_eq!(
                call(&ledger, "charge_usage", [a, 2, tariff_id, 0, 0, 0, 0, 0]),
                FailReason::NONE
            );
        }
    }

    // Apply a 50% discount to all users 1..=10 on the tariff.
    for a in 1..=10 {
        assert_eq!(
            call(
                &ledger,
                "register_account_discount",
                [a, tariff_id, 50, 0, 0, 0, 0, 0]
            ),
            FailReason::NONE
        );
    }

    // Charge users 1..=10 twice at count = 2, 50% off → unit = 5, 5*2 = 10 each → -20.
    for _ in 0..2 {
        for a in 1..=10 {
            assert_eq!(
                call(&ledger, "charge_usage", [a, 2, tariff_id, 0, 0, 0, 0, 0]),
                FailReason::NONE
            );
        }
    }

    // Balances: users 1..=5 = 1000 - 60 - 20 = 920; users 6..=10 = 1000 - 20 = 980.
    for a in 1..=5 {
        assert_eq!(
            ledger.get_balance(a),
            920,
            "user {a} after pre+post charges"
        );
    }
    for a in 6..=10 {
        assert_eq!(
            ledger.get_balance(a),
            980,
            "user {a} after discounted charges"
        );
    }

    // KV readback by querying the store directly. Namespace components are stored
    // as *constants*, so keys read back by NAME (from_string parses them as such).
    let tariff_key = KeyPath::from_string(&format!("tariffs/{tariff_id}/0/0")).unwrap();
    assert_eq!(
        ledger.get_kv(tariff_key),
        Some(Value::Int(10)),
        "tariff value readable by constant name"
    );

    let discount_key =
        KeyPath::from_string(&format!("tariffs/{tariff_id}/account_discount/3")).unwrap();
    assert_eq!(
        ledger.get_kv(discount_key),
        Some(Value::Int(50)),
        "user 3 discount readable by constant name"
    );

    // The namespace is a constant, NOT the integer id: querying with the raw id
    // (an `Int` component) must miss — the type is part of the key's identity.
    let tariffs_id = ledger.get_constant("tariffs").expect("tariffs interned") as i64;
    let int_key = KeyPath::new([
        Value::Int(tariffs_id),
        Value::Int(tariff_id),
        Value::Int(0),
        Value::Int(0),
    ]);
    assert_eq!(
        ledger.get_kv(int_key),
        None,
        "an integer id must NOT match a constant key component"
    );
}
