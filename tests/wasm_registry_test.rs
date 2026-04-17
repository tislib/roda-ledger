//! ADR-014 WASM Function Registry end-to-end tests.
//!
//! Exercises the full pipeline: `Ledger::register_function` pushes a
//! `FunctionRegistered` WAL record onto `wal_input`, the WAL stage
//! writes it to the active segment, the Snapshot stage reads it back
//! and loads the handler into `WasmRuntime`, then `Operation::Named`
//! invocations succeed. Unregister is the mirror path.
//!
//! Each test spins up its own `Ledger` against a fresh temp data_dir
//! (`LedgerConfig::temp`) so tests are independent and isolable.

use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::{Operation, WaitLevel};

// ──────────────────────────────────────────────────────────────────────────
// WAT fixtures
// ──────────────────────────────────────────────────────────────────────────

/// Balanced transfer: `credit(param0, param1)` + `debit(param2, param1)`.
/// Zero-sum by construction, so the transaction commits successfully.
const TRANSFER_WAT: &str = r#"
    (module
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        local.get 0 local.get 1 call $credit
        local.get 2 local.get 1 call $debit
        i32.const 0))
"#;

/// Same body as [`TRANSFER_WAT`] but returns a non-zero status (47) so
/// the transaction is rolled back.
const REJECT_WAT: &str = r#"
    (module
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        local.get 0 local.get 1 call $credit
        local.get 2 local.get 1 call $debit
        i32.const 47))
"#;

/// No imports, trivial `execute` → returns 0. Used for registry-only tests
/// where we don't care about execution.
const NOOP_WAT: &str = r#"
    (module
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        i32.const 0))
"#;

/// Intentionally broken — parses but lacks the `execute` export, so
/// `register_function` should reject it before any disk I/O.
const BAD_WAT: &str = r#"
    (module
      (func (export "not_execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        i32.const 0))
"#;

fn compile(wat_src: &str) -> Vec<u8> {
    wat::parse_str(wat_src).expect("wat parse")
}

fn start_ledger() -> Ledger {
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().expect("ledger start");
    ledger
}

// ──────────────────────────────────────────────────────────────────────────
// Register / List / Unregister
// ──────────────────────────────────────────────────────────────────────────

#[test]
fn register_single_function_and_list() {
    let ledger = start_ledger();
    let binary = compile(NOOP_WAT);
    let expected_crc = crc32c::crc32c(&binary);

    let (version, crc) = ledger
        .register_function("noop", &binary, false)
        .expect("register_function");
    assert_eq!(version, 1, "first registration starts at v1");
    assert_eq!(crc, expected_crc);

    let mut list = ledger.list_functions();
    assert_eq!(list.len(), 1);
    let info = list.pop().unwrap();
    assert_eq!(info.name, "noop");
    assert_eq!(info.version, 1);
    assert_eq!(info.crc32c, expected_crc);
}

#[test]
fn register_duplicate_without_override_errors() {
    let ledger = start_ledger();
    let binary = compile(NOOP_WAT);

    ledger
        .register_function("noop", &binary, false)
        .expect("first register");

    let err = ledger
        .register_function("noop", &binary, false)
        .expect_err("second register should fail");
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
}

#[test]
fn register_with_override_bumps_version() {
    let ledger = start_ledger();
    let v1_bin = compile(NOOP_WAT);
    let v2_bin = compile(TRANSFER_WAT);

    let (v1, _) = ledger
        .register_function("fn", &v1_bin, false)
        .expect("register v1");
    assert_eq!(v1, 1);

    let (v2, crc2) = ledger
        .register_function("fn", &v2_bin, true)
        .expect("register v2 with override");
    assert_eq!(v2, 2);
    assert_eq!(crc2, crc32c::crc32c(&v2_bin));

    // List shows the latest version only.
    let list = ledger.list_functions();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].version, 2);
    assert_eq!(list[0].crc32c, crc2);
}

#[test]
fn register_rejects_invalid_binary() {
    let ledger = start_ledger();
    let err = ledger
        .register_function("bad", &compile(BAD_WAT), false)
        .expect_err("missing execute export");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    // Garbage bytes also rejected.
    let err = ledger
        .register_function("junk", b"not wasm at all", false)
        .expect_err("not a wasm binary");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn register_rejects_invalid_name() {
    let ledger = start_ledger();
    let bin = compile(NOOP_WAT);
    // Names must start with a letter; `1foo` is rejected.
    let err = ledger
        .register_function("1foo", &bin, false)
        .expect_err("bad name");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    // Empty too.
    let err = ledger
        .register_function("", &bin, false)
        .expect_err("empty name");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn unregister_nonexistent_errors() {
    let ledger = start_ledger();
    let err = ledger
        .unregister_function("nope")
        .expect_err("not registered");
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn unregister_removes_from_list() {
    let ledger = start_ledger();
    let binary = compile(NOOP_WAT);

    ledger.register_function("tmp", &binary, false).unwrap();
    assert_eq!(ledger.list_functions().len(), 1);

    let v = ledger.unregister_function("tmp").expect("unregister");
    // Unregister writes a new version (2) with crc=0.
    assert_eq!(v, 2);

    let list = ledger.list_functions();
    assert!(
        list.is_empty(),
        "handler should be gone after unregister (got: {:?})",
        list
    );
}

#[test]
fn multiple_functions_listed_independently() {
    let ledger = start_ledger();

    ledger
        .register_function("a", &compile(NOOP_WAT), false)
        .unwrap();
    ledger
        .register_function("b", &compile(TRANSFER_WAT), false)
        .unwrap();
    ledger
        .register_function("c", &compile(REJECT_WAT), false)
        .unwrap();

    let list = ledger.list_functions();
    assert_eq!(list.len(), 3);
    let mut names: Vec<String> = list.iter().map(|i| i.name.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    for info in &list {
        assert_eq!(info.version, 1);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// End-to-end: Named execution through register → submit → unregister
// ──────────────────────────────────────────────────────────────────────────

#[test]
fn named_succeeds_after_registration() {
    let ledger = start_ledger();
    let binary = compile(TRANSFER_WAT);
    ledger
        .register_function("transfer", &binary, false)
        .expect("register");

    // Transfer 500 from acct 10 → acct 20 via WASM. The Transactor's
    // built-in credit/debit convention is: `credit(from)` subtracts,
    // `debit(to)` adds (matches `Operation::Transfer`).
    let result = ledger.submit_and_wait(
        Operation::Named {
            name: "transfer".into(),
            params: [10, 500, 20, 0, 0, 0, 0, 0],
            user_ref: 1,
        },
        WaitLevel::OnSnapshot,
    );

    assert!(
        result.fail_reason.is_success(),
        "Named should commit: fail_reason = {:?}",
        result.fail_reason
    );
    assert_eq!(ledger.get_balance(10), -500);
    assert_eq!(ledger.get_balance(20), 500);
}

#[test]
fn named_rolls_back_on_nonzero_status() {
    let ledger = start_ledger();
    ledger
        .register_function("reject", &compile(REJECT_WAT), false)
        .unwrap();

    let result = ledger.submit_and_wait(
        Operation::Named {
            name: "reject".into(),
            params: [1, 100, 2, 0, 0, 0, 0, 0],
            user_ref: 1,
        },
        WaitLevel::Committed,
    );

    assert!(result.fail_reason.is_failure(), "status 47 → failure");
    assert_eq!(result.fail_reason.as_u8(), 47);
    assert_eq!(ledger.get_balance(1), 0);
    assert_eq!(ledger.get_balance(2), 0);
}

#[test]
fn named_fails_for_unknown_function() {
    let ledger = start_ledger();
    let result = ledger.submit_and_wait(
        Operation::Named {
            name: "nonexistent".into(),
            params: [0; 8],
            user_ref: 1,
        },
        WaitLevel::Committed,
    );
    assert!(result.fail_reason.is_failure());
    // Not-registered → INVALID_OPERATION.
    assert_eq!(result.fail_reason.as_u8(), 5);
}

#[test]
fn named_fails_after_unregister() {
    let ledger = start_ledger();
    ledger
        .register_function("transfer", &compile(TRANSFER_WAT), false)
        .unwrap();

    // Works first.
    let ok = ledger.submit_and_wait(
        Operation::Named {
            name: "transfer".into(),
            params: [10, 50, 20, 0, 0, 0, 0, 0],
            user_ref: 1,
        },
        WaitLevel::OnSnapshot,
    );
    assert!(ok.fail_reason.is_success());

    // Unregister and try again.
    ledger.unregister_function("transfer").unwrap();

    let fail = ledger.submit_and_wait(
        Operation::Named {
            name: "transfer".into(),
            params: [10, 50, 20, 0, 0, 0, 0, 0],
            user_ref: 2,
        },
        WaitLevel::Committed,
    );
    assert!(fail.fail_reason.is_failure());
    assert_eq!(fail.fail_reason.as_u8(), 5); // INVALID_OPERATION
}

#[test]
fn named_uses_latest_version_after_override() {
    let ledger = start_ledger();
    // v1: returns 0 (success, does nothing).
    ledger
        .register_function("f", &compile(NOOP_WAT), false)
        .unwrap();
    // v2: returns 47 (failure).
    ledger
        .register_function("f", &compile(REJECT_WAT), true)
        .unwrap();

    let result = ledger.submit_and_wait(
        Operation::Named {
            name: "f".into(),
            params: [1, 10, 2, 0, 0, 0, 0, 0],
            user_ref: 1,
        },
        WaitLevel::Committed,
    );
    // v2 was installed → should fail with 47.
    assert_eq!(result.fail_reason.as_u8(), 47);
}

#[test]
fn register_is_observable_before_and_during_concurrent_txs() {
    // Register a function, then submit a batch of transfers, and
    // confirm all balances agree with the zero-sum invariant.
    let ledger = start_ledger();
    ledger
        .register_function("transfer", &compile(TRANSFER_WAT), false)
        .unwrap();

    let tx_count = 100u64;
    for i in 0..tx_count {
        let src = (i % 8) + 1;
        let dst = src + 100;
        let result = ledger.submit_and_wait(
            Operation::Named {
                name: "transfer".into(),
                params: [src as i64, 1, dst as i64, 0, 0, 0, 0, 0],
                user_ref: i + 1,
            },
            WaitLevel::Committed,
        );
        assert!(
            result.fail_reason.is_success(),
            "tx {} failed: {:?}",
            i,
            result.fail_reason
        );
    }

    // Drain to snapshot, then check a few balances.
    ledger.wait_for_pass();
    let mut credit_sum = 0i64;
    let mut debit_sum = 0i64;
    for acct in 1..=8u64 {
        credit_sum += ledger.get_balance(acct);
    }
    for acct in 101..=108u64 {
        debit_sum += ledger.get_balance(acct);
    }
    assert_eq!(
        credit_sum + debit_sum,
        0,
        "zero-sum invariant across WASM-driven transfers"
    );
    assert_eq!(debit_sum, tx_count as i64);
}
