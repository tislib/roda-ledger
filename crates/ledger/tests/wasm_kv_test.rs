//! Programmable KV state end-to-end tests (ADR-023).
//!
//! Drives the `ledger` host verbs from real WASM through the full pipeline and
//! verifies persistence + recovery. KV state has no client read API in v1, so
//! the modules turn KV reads into balance effects we can observe via
//! `get_balance` (debit adds, credit subtracts; both keep the tx zero-sum).

use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transactor::transaction::Operation;
use std::time::Duration;
use storage::StorageConfig;
use storage::entities::FailReason;

/// counter(account = param0): n = kv_add(global, key=[1,0,0,0], +1);
///   debit(account, n); credit(SYSTEM, n)  →  account balance climbs by the
///   running counter value (1, then 2, …), proving the merge resolves and
///   persists across calls.
const COUNTER_WAT: &str = r#"
    (module
      (import "ledger" "kv_add" (func $add (param i64 i32 i32 i32 i32 i64) (result i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        (local $n i64)
        (local.set $n
          (call $add (i64.const 0) (i32.const 1) (i32.const 0) (i32.const 0) (i32.const 0)
                     (i64.const 1)))
        (call $debit  (local.get 0) (local.get $n))
        (call $credit (i64.const 0) (local.get $n))
        (i32.const 0)))
"#;

/// reg_save(value = param1): register_write(0, value).
const REG_SAVE_WAT: &str = r#"
    (module
      (import "ledger" "register_write" (func $rw (param i32 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        (call $rw (i32.const 0) (local.get 1))
        (i32.const 0)))
"#;

/// reg_load(account = param0): v = register_read(0); debit(account, v); credit(SYSTEM, v)
///   →  account balance jumps to the stored register value.
const REG_LOAD_WAT: &str = r#"
    (module
      (import "ledger" "register_read" (func $rr (param i32) (result i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        (local $v i64)
        (local.set $v (call $rr (i32.const 0)))
        (call $debit  (local.get 0) (local.get $v))
        (call $credit (i64.const 0) (local.get $v))
        (i32.const 0)))
"#;

fn compile(wat_src: &str) -> Vec<u8> {
    wat::parse_str(wat_src).expect("wat parse")
}

fn call(ledger: &Ledger, name: &str, p0: i64, p1: i64) -> FailReason {
    ledger
        .submit_and_wait_result(Operation::Function {
            name: name.to_string(),
            params: [p0, p1, 0, 0, 0, 0, 0, 0],
            user_ref: 0,
        })
        .get_fail_reason()
}

fn register_all(ledger: &Ledger) {
    ledger
        .register_function("counter", &compile(COUNTER_WAT), false)
        .expect("register counter");
    ledger
        .register_function("reg_save", &compile(REG_SAVE_WAT), false)
        .expect("register reg_save");
    ledger
        .register_function("reg_load", &compile(REG_LOAD_WAT), false)
        .expect("register reg_load");
}

#[test]
fn kv_counter_and_register_drive_balances() {
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().expect("start");
    ledger.open_accounts(10);
    register_all(&ledger);

    // Counter climbs 1, 2, 3 → account 5 balance = 1 + 2 + 3 = 6.
    for _ in 0..3 {
        assert_eq!(call(&ledger, "counter", 5, 0), FailReason::NONE);
    }
    assert_eq!(ledger.get_balance(5), 6);

    // Register round-trip: save 100, then load it onto account 6.
    assert_eq!(call(&ledger, "reg_save", 0, 100), FailReason::NONE);
    assert_eq!(call(&ledger, "reg_load", 6, 0), FailReason::NONE);
    assert_eq!(ledger.get_balance(6), 100);
}

// KV has no snapshot in v1 — state is rebuilt by pure WAL replay (ADR-023 §5).
// After a restart the counter must *continue* (not reset) and the saved register
// must still be readable.
#[test]
fn kv_state_survives_recovery() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_kv_recovery_test_{}", nanos);
    let _ = std::fs::remove_dir_all(&temp_dir);

    let make_config = || LedgerConfig {
        storage: StorageConfig {
            data_dir: temp_dir.clone(),
            transaction_count_per_segment: 1000,
            snapshot_frequency: 1,
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    };

    // Phase 1: advance the counter to 3 and save register 0 = 250.
    {
        let mut ledger = Ledger::new(make_config());
        ledger.start().unwrap();
        ledger.open_accounts(10);
        register_all(&ledger);
        for _ in 0..3 {
            assert_eq!(call(&ledger, "counter", 5, 0), FailReason::NONE);
        }
        assert_eq!(ledger.get_balance(5), 6);
        assert_eq!(call(&ledger, "reg_save", 0, 250), FailReason::NONE);

        // Roll past a seal so recovery exercises the sealed-segment KV fold too.
        let mut last = 0;
        for _ in 0..2000 {
            last = ledger.submit(Operation::Deposit {
                account: 2,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last);
        ledger.wait_for_seal();
        drop(ledger);
    }

    // Phase 2: recover from WAL (+snapshot for balances) and verify KV continued.
    {
        let mut ledger = Ledger::new(make_config());
        ledger.start().unwrap();

        // Counter continued from 3 → next call yields 4, lifting account 5 from
        // 6 to 10. (A reset counter would yield 1 and land at 7.)
        assert_eq!(call(&ledger, "counter", 5, 0), FailReason::NONE);
        assert_eq!(ledger.get_balance(5), 10, "counter resumed at 4, not 1");

        // Register survived: load 250 onto a fresh account.
        assert_eq!(call(&ledger, "reg_load", 7, 0), FailReason::NONE);
        assert_eq!(ledger.get_balance(7), 250, "register recovered from WAL");
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
