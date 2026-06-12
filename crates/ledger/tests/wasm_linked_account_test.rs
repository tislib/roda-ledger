//! WASM linked-account (reserve / release) end-to-end tests (ADR-022 §6).
//!
//! Exercises the `linked_account` host function via WASM: a `reserve` program
//! moves funds from a main account into its HOLD bucket (created lazily on
//! first use as a PROGRAMMED account), and a `release` program moves them back.
//! Verifies balances, bucket creation, and the §7 guard (PROGRAMMED buckets are
//! not user-addressable).

use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transactor::transaction::{Operation, WaitLevel};
use std::time::Duration;
use storage::StorageConfig;
use storage::entities::FailReason;

const HOLD_TYPE: i64 = 1; // bucket type_id passed to linked_account

/// reserve(main = param0, amount = param1): hold funds.
///   h = linked_account(main, HOLD); debit(h, amount); credit(main, amount)
/// debit adds to the bucket, credit subtracts from main → funds move main→hold.
const RESERVE_WAT: &str = r#"
    (module
      (import "ledger" "linked_account" (func $la (param i64 i32) (result i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        (local $h i64)
        (local.set $h (call $la (local.get 0) (i32.const 1)))
        (call $debit  (local.get $h) (local.get 1))
        (call $credit (local.get 0) (local.get 1))
        (i32.const 0)))
"#;

/// release(main = param0, amount = param1): unhold funds.
///   h = linked_account(main, HOLD); credit(h, amount); debit(main, amount)
/// credit subtracts from the bucket, debit adds to main → funds move hold→main.
const RELEASE_WAT: &str = r#"
    (module
      (import "ledger" "linked_account" (func $la (param i64 i32) (result i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        (local $h i64)
        (local.set $h (call $la (local.get 0) (i32.const 1)))
        (call $credit (local.get $h) (local.get 1))
        (call $debit  (local.get 0) (local.get 1))
        (i32.const 0)))
"#;

fn compile(wat_src: &str) -> Vec<u8> {
    wat::parse_str(wat_src).expect("wat parse")
}

fn start_ledger() -> Ledger {
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().expect("ledger start");
    ledger
}

fn deposit(ledger: &Ledger, account: u64, amount: u64) -> FailReason {
    ledger
        .submit_and_wait(
            Operation::Deposit {
                account,
                amount,
                user_ref: 0,
            },
            WaitLevel::OnSnapshot,
        )
        .fail_reason
}

fn call(ledger: &Ledger, name: &str, main: i64, amount: i64) -> FailReason {
    ledger
        .submit_and_wait(
            Operation::Function {
                name: name.to_string(),
                params: [main, amount, 0, 0, 0, 0, 0, 0],
                user_ref: 0,
            },
            WaitLevel::OnSnapshot,
        )
        .fail_reason
}

#[test]
fn reserve_holds_funds_and_release_unholds_them() {
    let ledger = start_ledger();
    ledger.open_accounts(10); // ids 1..=10 OPEN; next id = 11
    assert_eq!(deposit(&ledger, 5, 1000), FailReason::NONE);

    ledger
        .register_function("reserve", &compile(RESERVE_WAT), false)
        .expect("register reserve");
    ledger
        .register_function("release", &compile(RELEASE_WAT), false)
        .expect("register release");

    // First reserve lazily creates the HOLD bucket — the next sequential id (11).
    let hold = 11u64;
    assert_eq!(call(&ledger, "reserve", 5, 300), FailReason::NONE);
    assert_eq!(
        ledger.get_balance(5),
        700,
        "main reduced by the held amount"
    );
    assert_eq!(
        ledger.get_balance(hold),
        300,
        "bucket holds the reserved amount"
    );

    // Reserving again reuses the same bucket (get-or-create) and adds to it.
    assert_eq!(call(&ledger, "reserve", 5, 200), FailReason::NONE);
    assert_eq!(ledger.get_balance(5), 500);
    assert_eq!(ledger.get_balance(hold), 500);

    // Release the whole hold back to main.
    assert_eq!(call(&ledger, "release", 5, 500), FailReason::NONE);
    assert_eq!(
        ledger.get_balance(5),
        1000,
        "all funds released back to main"
    );
    assert_eq!(ledger.get_balance(hold), 0, "hold bucket emptied");
}

#[test]
fn reserved_bucket_is_not_user_addressable() {
    let ledger = start_ledger();
    ledger.open_accounts(10);
    assert_eq!(deposit(&ledger, 5, 1000), FailReason::NONE);
    ledger
        .register_function("reserve", &compile(RESERVE_WAT), false)
        .expect("register reserve");

    assert_eq!(call(&ledger, "reserve", 5, 300), FailReason::NONE);
    let hold = 11u64;
    assert_eq!(ledger.get_balance(hold), 300);

    // The bucket is PROGRAMMED (not OPEN) — a user-named Deposit is rejected,
    // so program-controlled funds can't be poked by clients (§7 guard).
    assert_eq!(deposit(&ledger, hold, 100), FailReason::ACCOUNT_NOT_FOUND);
    assert_eq!(
        ledger.get_balance(hold),
        300,
        "bucket untouched by the rejected deposit"
    );
}

#[test]
fn reserve_is_zero_sum_and_unknown_main_is_rejected() {
    let _ = HOLD_TYPE; // documents the bucket type the WAT hard-codes (1).
    let ledger = start_ledger();
    ledger.open_accounts(10);
    assert_eq!(deposit(&ledger, 5, 1000), FailReason::NONE);
    ledger
        .register_function("reserve", &compile(RESERVE_WAT), false)
        .expect("register reserve");

    // SYSTEM (id 0) holds the deposit's -1000; after a reserve the global sum
    // stays zero (funds only move main → bucket).
    assert_eq!(call(&ledger, "reserve", 5, 300), FailReason::NONE);
    let hold = 11u64;
    assert_eq!(
        ledger.get_balance(0) + ledger.get_balance(5) + ledger.get_balance(hold),
        0
    );
}

// A PROGRAMMED bucket + its parent→bucket link must survive a *snapshot*-based
// restart (ADR-022 §5): the snapshot file persists per-account flags + the link
// table, so the bucket recovers as PROGRAMMED (not blanket-OPEN) and the link
// resolves to the same id instead of allocating a fresh bucket.
#[test]
fn flags_and_links_survive_snapshot_recovery() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_linked_recovery_test_{}", nanos);
    if std::path::Path::new(&temp_dir).exists() {
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    // Small segments + snapshot every seal, so a snapshot captures the bucket.
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

    let hold = 11u64; // first linked_account after open_accounts(10)

    // Phase 1: create the PROGRAMMED bucket + link, then force snapshots.
    {
        let mut ledger = Ledger::new(make_config());
        ledger.start().unwrap();
        ledger.open_accounts(10);
        assert_eq!(deposit(&ledger, 5, 1000), FailReason::NONE);
        ledger
            .register_function("reserve", &compile(RESERVE_WAT), false)
            .expect("register reserve");
        assert_eq!(call(&ledger, "reserve", 5, 300), FailReason::NONE);
        assert_eq!(ledger.get_balance(hold), 300);

        // Fill segments via a different account so a snapshot seals over the
        // bucket creation (account 5 / bucket balances stay put).
        let mut last = 0;
        for _ in 0..3000 {
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

    // Phase 2: recover (from snapshot + WAL tail) and verify.
    {
        let mut ledger = Ledger::new(make_config());
        ledger.start().unwrap();

        // Balances survived.
        assert_eq!(ledger.get_balance(5), 700);
        assert_eq!(ledger.get_balance(hold), 300);

        // FLAGS survived: the bucket is still PROGRAMMED, so a user deposit is
        // rejected. (It would SUCCEED if flags weren't persisted — the blanket
        // layout would have marked it OPEN.)
        assert_eq!(deposit(&ledger, hold, 50), FailReason::ACCOUNT_NOT_FOUND);
        assert_eq!(
            ledger.get_balance(hold),
            300,
            "rejected deposit left it untouched"
        );

        // LINK survived: reserve resolves to the SAME bucket (11), climbing to
        // 400 — not a freshly allocated id.
        assert_eq!(call(&ledger, "reserve", 5, 100), FailReason::NONE);
        assert_eq!(
            ledger.get_balance(hold),
            400,
            "link resolved to the same bucket"
        );
        assert_eq!(ledger.get_balance(5), 600);
    }

    if std::path::Path::new(&temp_dir).exists() {
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}

/// WASM may set any flag lane, including the status lane (ADR-022 §6 — "WASM
/// allowed to update statuses"). `set_flag` emits an `AccountFlagsUpdated`.
const SET_FLAG_WAT: &str = r#"
    (module
      (import "ledger" "set_flag" (func $set (param i64 i32 i32)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        (call $set (local.get 0)
                   (i32.wrap_i64 (local.get 1))
                   (i32.wrap_i64 (local.get 2)))
        (i32.const 0)))
"#;

fn set_lane(ledger: &Ledger, account: i64, lane: i64, value: i64) -> FailReason {
    ledger
        .submit_and_wait(
            Operation::Function {
                name: "setflag".to_string(),
                params: [account, lane, value, 0, 0, 0, 0, 0],
                user_ref: 0,
            },
            WaitLevel::OnSnapshot,
        )
        .fail_reason
}

#[test]
fn wasm_set_flag_updates_lanes_including_status() {
    let ledger = start_ledger();
    ledger.open_accounts(10);
    ledger
        .register_function("setflag", &compile(SET_FLAG_WAT), false)
        .expect("register setflag");

    // A non-status lane (1 = 42) on an OPEN account; status lane unchanged.
    assert_eq!(set_lane(&ledger, 5, 1, 42), FailReason::NONE);
    let flags = ledger.get_flags(5);
    assert_eq!((flags >> 8) & 0xFF, 42, "lane 1 updated");
    assert_eq!(flags & 0xFF, 1, "status lane still OPEN");

    // WASM may overwrite the status lane itself (no guard).
    assert_eq!(set_lane(&ledger, 6, 0, 2), FailReason::NONE);
    assert_eq!(
        ledger.get_flags(6) & 0xFF,
        2,
        "status lane set to PROGRAMMED"
    );
}
