//! WASM Function Registry end-to-end tests (ADR-014).
//!
//! Exercises the full pipeline: `Ledger::register_function` pushes a
//! `FunctionRegistered` WAL record onto `wal_input`, the WAL stage
//! writes it to the active segment, the Snapshot stage reads it back
//! and loads the handler into `WasmRuntime`, then `Operation::Function`
//! invocations succeed. Unregister is the mirror path.
//!
//! Each test spins up its own `Ledger` against a fresh temp data_dir
//! (`LedgerConfig::temp`) so tests are independent and isolable.

use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::{Operation, WaitLevel};
use storage::StorageConfig;

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

/// Parametrized loop: calls `credit(param0, param1)` + `debit(param2, param1)`
/// `param3` times, producing `2 * param3` entries. Used to exercise the
/// post-WASM `ENTRY_LIMIT_EXCEEDED` check (entry_count is u8, so >255 rejects).
const LOOP_TRANSFER_WAT: &str = r#"
    (module
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        (local $i i64)
        (local.set $i (i64.const 0))
        (block $done
          (loop $loop
            (br_if $done (i64.ge_u (local.get $i) (local.get 3)))
            local.get 0 local.get 1 call $credit
            local.get 2 local.get 1 call $debit
            (local.set $i (i64.add (local.get $i) (i64.const 1)))
            br $loop))
        i32.const 0))
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
    assert_eq!(
        names,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
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
        Operation::Function {
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
        Operation::Function {
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
fn named_exceeding_entry_limit_rejects_and_rolls_back() {
    // TxMetadata.entry_count is a u8, so a successful WASM execution that
    // emits more than 255 credit/debit records cannot be encoded losslessly.
    // The transactor must detect this after `execute` returns and fail the
    // tx with `ENTRY_LIMIT_EXCEEDED` (= 4), triggering the standard rollback
    // path so neither account retains any of the interim balance changes.
    let ledger = start_ledger();
    ledger
        .register_function("loop_transfer", &compile(LOOP_TRANSFER_WAT), false)
        .expect("register");

    // 200 iterations → 400 entries, well past the 255 cap.
    let over = ledger.submit_and_wait(
        Operation::Function {
            name: "loop_transfer".into(),
            params: [1, 10, 2, 200, 0, 0, 0, 0],
            user_ref: 1,
        },
        WaitLevel::Committed,
    );
    assert!(
        over.fail_reason.is_failure(),
        "tx should be rejected: {:?}",
        over.fail_reason
    );
    assert_eq!(
        over.fail_reason.as_u8(),
        4,
        "expected ENTRY_LIMIT_EXCEEDED (4), got {}",
        over.fail_reason.as_u8()
    );
    // Rollback restored both accounts to zero.
    assert_eq!(ledger.get_balance(1), 0, "credit side must roll back");
    assert_eq!(ledger.get_balance(2), 0, "debit side must roll back");

    // Sanity: a run that stays under the limit on the same handler still
    // commits — proves the check fires on size only, not on the loop shape.
    let ok = ledger.submit_and_wait(
        Operation::Function {
            name: "loop_transfer".into(),
            params: [1, 10, 2, 100, 0, 0, 0, 0],
            user_ref: 2,
        },
        WaitLevel::OnSnapshot,
    );
    assert!(
        ok.fail_reason.is_success(),
        "200 entries must commit: {:?}",
        ok.fail_reason
    );
    assert_eq!(ledger.get_balance(1), -1_000);
    assert_eq!(ledger.get_balance(2), 1_000);
}

#[test]
fn named_fails_for_unknown_function() {
    let ledger = start_ledger();
    let result = ledger.submit_and_wait(
        Operation::Function {
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
        Operation::Function {
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
        Operation::Function {
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
        Operation::Function {
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
            Operation::Function {
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

// ──────────────────────────────────────────────────────────────────────────
// Function snapshot persistence across restart
// ──────────────────────────────────────────────────────────────────────────

/// Build a `LedgerConfig` whose data directory survives `Drop` and can be
/// reopened — required to exercise the recover-from-function-snapshot
/// path. The caller is responsible for cleaning the directory afterwards.
fn persistent_config(dir: &std::path::Path) -> LedgerConfig {
    // Small segment so a handful of transactions force a seal quickly.
    // snapshot_frequency = 1 makes every sealed segment emit a snapshot —
    // both balance and function.
    LedgerConfig {
        max_accounts: 16,
        storage: StorageConfig {
            data_dir: dir.to_string_lossy().into_owned(),
            temporary: false,
            transaction_count_per_segment: 5,
            snapshot_frequency: 1,
        },
        ..LedgerConfig::default()
    }
}

/// Create a fresh data directory, return its `PathBuf` and a guard that
/// removes it when dropped — avoids leaking temp dirs between tests.
struct DirGuard(std::path::PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn make_dir() -> DirGuard {
    let mut dir = std::env::temp_dir();
    dir.push(format!("roda_wasm_snapshot_test_{}", rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    DirGuard(dir)
}

#[test]
fn function_snapshot_loads_handler_after_restart() {
    let guard = make_dir();

    // Phase 1: register, submit enough transactions to force a seal
    // (crosses `transaction_count_per_segment`), then close cleanly.
    {
        let mut ledger = Ledger::new(persistent_config(&guard.0));
        ledger.start().expect("first start");
        ledger
            .register_function("transfer", &compile(TRANSFER_WAT), false)
            .expect("register");

        // Fire enough ops to cross the 5-tx segment boundary so at least
        // one segment seals and emits a function snapshot.
        for i in 0..12 {
            let r = ledger.submit_and_wait(
                Operation::Function {
                    name: "transfer".into(),
                    params: [1, 10, 2, 0, 0, 0, 0, 0],
                    user_ref: i + 1,
                },
                WaitLevel::OnSnapshot,
            );
            assert!(r.fail_reason.is_success(), "submit {} failed", i);
        }
        ledger.wait_for_seal();
        // ledger drops here — wal.stop is written, no crash recovery on
        // the next open.
    }

    // Confirm a function snapshot exists on disk.
    let snap_count = std::fs::read_dir(&guard.0)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.starts_with("function_snapshot_") && n.ends_with(".bin"))
                .unwrap_or(false)
        })
        .count();
    assert!(
        snap_count >= 1,
        "expected at least one function_snapshot_*.bin on disk, found {}",
        snap_count
    );

    // Phase 2: reopen the same data_dir. Recovery should reload the
    // handler from the function snapshot before replaying any tail WAL.
    let mut ledger = Ledger::new(persistent_config(&guard.0));
    ledger.start().expect("second start");

    let list = ledger.list_functions();
    assert_eq!(list.len(), 1, "handler missing after restart: {:?}", list);
    assert_eq!(list[0].name, "transfer");
    assert_eq!(list[0].version, 1);

    // Named execution still works against the restored handler.
    let r = ledger.submit_and_wait(
        Operation::Function {
            name: "transfer".into(),
            params: [3, 20, 4, 0, 0, 0, 0, 0],
            user_ref: 999,
        },
        WaitLevel::OnSnapshot,
    );
    assert!(
        r.fail_reason.is_success(),
        "Named after snapshot-restore failed: {:?}",
        r.fail_reason
    );
    assert_eq!(ledger.get_balance(3), -20);
    assert_eq!(ledger.get_balance(4), 20);
}

#[test]
fn function_snapshot_preserves_latest_version() {
    let guard = make_dir();

    // Phase 1: register v1, run some ops, register v2 (override), run more
    // ops so v2 lives in the active segment at seal time and both versions
    // cross into sealed segments.
    {
        let mut ledger = Ledger::new(persistent_config(&guard.0));
        ledger.start().unwrap();

        ledger
            .register_function("fee", &compile(NOOP_WAT), false)
            .unwrap();
        for i in 0..6 {
            ledger.submit_and_wait(
                Operation::Function {
                    name: "fee".into(),
                    params: [1, 1, 2, 0, 0, 0, 0, 0],
                    user_ref: i + 1,
                },
                WaitLevel::OnSnapshot,
            );
        }

        // Override with v2 (REJECT_WAT returns status 47 → easily observable).
        ledger
            .register_function("fee", &compile(REJECT_WAT), true)
            .unwrap();
        for i in 0..6 {
            let _ = ledger.submit_and_wait(
                Operation::Function {
                    name: "fee".into(),
                    params: [1, 1, 2, 0, 0, 0, 0, 0],
                    user_ref: 100 + i,
                },
                WaitLevel::OnSnapshot,
            );
        }
        ledger.wait_for_seal();
    }

    // Phase 2: reopen — only v2 should survive (latest version wins).
    let mut ledger = Ledger::new(persistent_config(&guard.0));
    ledger.start().unwrap();

    let list = ledger.list_functions();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name, "fee");
    assert_eq!(
        list[0].version, 2,
        "expected latest version (v2) after snapshot restore"
    );

    // v2 rejects → fail_reason = 47.
    let r = ledger.submit_and_wait(
        Operation::Function {
            name: "fee".into(),
            params: [1, 1, 2, 0, 0, 0, 0, 0],
            user_ref: 9_999,
        },
        WaitLevel::Committed,
    );
    assert_eq!(r.fail_reason.as_u8(), 47);
}

#[test]
fn function_snapshot_records_unregistered_entries() {
    let guard = make_dir();

    // Phase 1: register two functions, unregister one, force a seal.
    {
        let mut ledger = Ledger::new(persistent_config(&guard.0));
        ledger.start().unwrap();
        ledger
            .register_function("a", &compile(TRANSFER_WAT), false)
            .unwrap();
        ledger
            .register_function("b", &compile(NOOP_WAT), false)
            .unwrap();

        // Drive a few transactions to cross the 5-tx threshold so seal
        // actually fires.
        for i in 0..8 {
            ledger.submit_and_wait(
                Operation::Function {
                    name: "a".into(),
                    params: [1, 1, 2, 0, 0, 0, 0, 0],
                    user_ref: i + 1,
                },
                WaitLevel::OnSnapshot,
            );
        }
        ledger.unregister_function("a").unwrap();
        // More traffic so the unregister record ends up in a sealed segment
        // + triggers another snapshot.
        for i in 0..8 {
            ledger.submit_and_wait(
                Operation::Deposit {
                    account: 5,
                    amount: 1,
                    user_ref: 200 + i,
                },
                WaitLevel::OnSnapshot,
            );
        }
        ledger.wait_for_seal();
    }

    // Phase 2: reopen — only `b` should be loaded; `a` was unregistered.
    let mut ledger = Ledger::new(persistent_config(&guard.0));
    ledger.start().unwrap();

    let names: Vec<String> = ledger
        .list_functions()
        .into_iter()
        .map(|i| i.name)
        .collect();
    assert_eq!(names, vec!["b".to_string()]);
}
