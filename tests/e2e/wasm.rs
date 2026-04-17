//! WASM Function Registry E2E tests (ADR-014).
//!
//! Exercises the full register → submit Named → unregister path through
//! the gRPC surface. Restart / crash scenarios are skipped on the Inline
//! backend (no process isolation), and run against Process backend via
//! `E2E_BACKEND=process` (or whatever future backends support real
//! restart semantics).
//!
//! Test scenarios (per user request):
//!   1. `wasm_runs_properly`                    — basic register + submit Named.
//!   2. `wasm_new_version_runs_properly`        — register v1, v2-with-override,
//!                                                wait 100ms, new version
//!                                                executes.
//!   3. `wasm_loaded_after_restart`             — clean restart (Process).
//!   4. `wasm_loaded_after_crash`               — kill -9 + restart (Process).
//!   5. `wasm_new_version_loaded_after_restart` — register v1 → checkpoint
//!                                                → register v2 → restart →
//!                                                v2 is the one loaded.
//!
//! Scenarios are combined where possible: the restart / crash variants
//! cover both "handler loaded from WAL" (just appended, no seal) and
//! "handler loaded from sealed segment" (after `wait_for_snapshot` has
//! let the seal thread close at least one segment).

use crate::e2e::lib::backend::E2EBackend;
use crate::e2e::lib::profile::profile;
use std::time::Duration;
use tokio::time::sleep;

// ─── WAT fixtures ──────────────────────────────────────────────────────────

/// v1: balanced deposit. `debit(param0, param1)` then
/// `credit(SYSTEM_ACCOUNT_ID=0, param1)` — same shape as `Operation::Deposit`.
const DEPOSIT_WAT_V1: &str = r#"
    (module
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        local.get 0 local.get 1 call $debit
        i64.const 0 local.get 1 call $credit
        i32.const 0))
"#;

/// v2: same as v1 but debits TWICE the amount. Lets the test distinguish
/// which version executed by looking at the balance delta.
const DEPOSIT_WAT_V2: &str = r#"
    (module
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        ;; debit(account, amount * 2)
        local.get 0
        local.get 1 i64.const 2 i64.mul
        call $debit
        ;; credit(SYSTEM, amount * 2)  — keep zero-sum
        i64.const 0
        local.get 1 i64.const 2 i64.mul
        call $credit
        i32.const 0))
"#;

fn compile(src: &str) -> Vec<u8> {
    wat::parse_str(src).expect("wat parse")
}

/// Submit a Named "deposit" for `(account, amount)` and assert success.
/// Centralizes the boilerplate every test here needs.
async fn deposit_via_wasm(
    ctx: &crate::e2e::E2EContext,
    account: u64,
    amount: u64,
    user_ref: u64,
) {
    let result = ctx
        .submit_function(
            0,
            "deposit",
            [account as i64, amount as i64, 0, 0, 0, 0, 0, 0],
            user_ref,
            wait_level!(on_snapshot),
        )
        .await;
    assert_eq!(
        result.fail_reason, 0,
        "deposit via wasm failed: fail_reason={}",
        result.fail_reason
    );
}

// ═════════════════════════════════════════════════════════════════════════
// 1. wasm function runs properly
// ═════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn wasm_runs_properly() {
    let ctx = e2e_ctx!(profile: single_node);

    // Register v1.
    let (version, _crc) = ctx
        .register_function(0, "deposit", &compile(DEPOSIT_WAT_V1), false)
        .await;
    assert_eq!(version, 1);

    // `list_functions` observes it.
    let list = ctx.list_functions(0).await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name, "deposit");
    assert_eq!(list[0].version, 1);

    // Submit one Named deposit and verify balances.
    deposit_via_wasm(&ctx, 1, 1000, 1).await;
    assert_balance!(ctx, account: 1, eq: 1000);
    assert_balance!(ctx, account: 0, eq: -1000);
}

// ═════════════════════════════════════════════════════════════════════════
// 2. wasm function new version runs properly
//    (wait 100ms before sending the next transaction)
// ═════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn wasm_new_version_runs_properly() {
    let ctx = e2e_ctx!(profile: single_node);

    // Register v1 and exercise it once.
    ctx.register_function(0, "deposit", &compile(DEPOSIT_WAT_V1), false)
        .await;
    deposit_via_wasm(&ctx, 1, 100, 1).await;
    assert_balance!(ctx, account: 1, eq: 100); // v1: amount × 1

    // Register v2 with override.
    let (v2, _) = ctx
        .register_function(0, "deposit", &compile(DEPOSIT_WAT_V2), true)
        .await;
    assert_eq!(v2, 2);

    // Per test spec — wait 100ms before the next submit.
    sleep(Duration::from_millis(100)).await;

    // Submit again. v2 doubles the amount.
    deposit_via_wasm(&ctx, 1, 100, 2).await;
    // Total on account 1: v1(100) + v2(200) = 300.
    assert_balance!(ctx, account: 1, eq: 300);

    // list_functions reflects the latest version only.
    let list = ctx.list_functions(0).await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].version, 2);
}

// ═════════════════════════════════════════════════════════════════════════
// 3. wasm loaded after restart (clean kill, graceful replay)
//    — combined with "loaded from WAL" scenario: we never snapshot/seal
//      the FunctionRegistered record before restart.
// ═════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn wasm_loaded_after_restart() {
    if E2EBackend::from_env() == E2EBackend::Inline {
        eprintln!("skipping wasm_loaded_after_restart on Inline backend (needs Process)");
        return;
    }

    let mut ctx = crate::e2e::E2EContext::new(profile("single_node")).await;

    // Register and exercise once so the balance is non-zero pre-restart.
    ctx.register_function(0, "deposit", &compile(DEPOSIT_WAT_V1), false)
        .await;
    deposit_via_wasm(&ctx, 1, 500, 1).await;
    assert_balance!(ctx, account: 1, eq: 500);

    // Clean restart. WAL is intact (wal.stop is written by Drop), and the
    // active segment carries the FunctionRegistered record — the handler
    // has to be reinstalled from WAL replay.
    kill_and_restart!(ctx);

    // list_functions should show `deposit` again.
    let list = ctx.list_functions(0).await;
    assert_eq!(list.len(), 1, "handler missing after restart: {:?}", list);
    assert_eq!(list[0].name, "deposit");
    assert_eq!(list[0].version, 1);

    // And Named execution still works.
    deposit_via_wasm(&ctx, 1, 500, 2).await;
    assert_balance!(ctx, account: 1, eq: 1000);
}

// ═════════════════════════════════════════════════════════════════════════
// 4. wasm loaded after crash (kill -9, no clean shutdown marker)
//    — combined with "loaded from WAL after sealed segment" scenario:
//      we submit enough transactions before the crash to trigger a seal,
//      so the FunctionRegistered record is also present in a sealed
//      segment's WAL. Recovery must still find and apply it.
// ═════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn wasm_loaded_after_crash() {
    if E2EBackend::from_env() == E2EBackend::Inline {
        eprintln!("skipping wasm_loaded_after_crash on Inline backend (needs Process)");
        return;
    }

    // Small segment so we can force a seal before the crash.
    let mut p = profile("single_node");
    p.ledger.storage.transaction_count_per_segment = 100;
    let mut ctx = crate::e2e::E2EContext::new(p).await;

    // Register, then submit enough deposits to exceed one segment.
    ctx.register_function(0, "deposit", &compile(DEPOSIT_WAT_V1), false)
        .await;
    for i in 0..200 {
        deposit_via_wasm(&ctx, 1, 1, i + 1).await;
    }
    assert_balance!(ctx, account: 1, eq: 200);

    // Hard crash (SIGKILL — no wal.stop written → Recover::crash_recover
    // kicks in on restart).
    kill_node!(ctx);
    restart_node!(ctx);

    // Registry survived the crash.
    let list = ctx.list_functions(0).await;
    assert_eq!(
        list.len(),
        1,
        "handler missing after crash+restart: {:?}",
        list
    );
    assert_eq!(list[0].name, "deposit");

    // Balance survived + more Named work still runs.
    assert_balance!(ctx, account: 1, eq: 200);
    deposit_via_wasm(&ctx, 1, 50, 201).await;
    assert_balance!(ctx, account: 1, eq: 250);
}

// ═════════════════════════════════════════════════════════════════════════
// 5. wasm new version loaded fine
//    — combined scenarios (handler evolution across restart + crash):
//      - v1 registered  (first sealed segment)
//      - some deposits  (seal triggered)
//      - v2 registered with override (active segment only — WAL tail)
//      - restart        (v1 read from sealed, v2 read from WAL tail)
//      - then crash     (repeat same state after forced crash)
// ═════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn wasm_new_version_loaded_after_restart_and_crash() {
    if E2EBackend::from_env() == E2EBackend::Inline {
        eprintln!(
            "skipping wasm_new_version_loaded_after_restart_and_crash on Inline backend (needs Process)"
        );
        return;
    }

    let mut p = profile("single_node");
    // Seal quickly so some FunctionRegistered + TxMetadata records end up
    // in a sealed segment before we register v2.
    p.ledger.storage.transaction_count_per_segment = 50;
    let mut ctx = crate::e2e::E2EContext::new(p).await;

    // Register v1 in segment 1.
    ctx.register_function(0, "deposit", &compile(DEPOSIT_WAT_V1), false)
        .await;
    // Enough deposits to overflow segment 1 (and roll over at least once),
    // guaranteeing the v1 FunctionRegistered record is in a *sealed*
    // segment.
    for i in 0..120 {
        deposit_via_wasm(&ctx, 1, 1, i + 1).await;
    }
    let balance_after_v1 = 120i64;
    assert_balance!(ctx, account: 1, eq: balance_after_v1);

    // Register v2 with override — lands in the currently-active segment.
    let (v2, _) = ctx
        .register_function(0, "deposit", &compile(DEPOSIT_WAT_V2), true)
        .await;
    assert_eq!(v2, 2);

    sleep(Duration::from_millis(100)).await;
    deposit_via_wasm(&ctx, 1, 10, 201).await;
    // v2 doubles → +20.
    let balance_after_v2 = balance_after_v1 + 20;
    assert_balance!(ctx, account: 1, eq: balance_after_v2);

    // Clean restart. Recovery replays both segments:
    //   - sealed segment  → FunctionRegistered(v1, crc_v1)   → loads v1
    //   - active segment  → FunctionRegistered(v2, crc_v2)   → replaces with v2
    // Final WasmRuntime state: `deposit` → v2 (latest).
    kill_and_restart!(ctx);

    let list = ctx.list_functions(0).await;
    assert_eq!(list.len(), 1, "handler lost after restart: {:?}", list);
    assert_eq!(list[0].name, "deposit");
    assert_eq!(
        list[0].version, 2,
        "expected latest version (v2) after replay, got v{}",
        list[0].version
    );
    assert_balance!(ctx, account: 1, eq: balance_after_v2);

    // v2 still in effect: another deposit doubles.
    deposit_via_wasm(&ctx, 1, 10, 202).await;
    let balance_after_v2_after_restart = balance_after_v2 + 20;
    assert_balance!(ctx, account: 1, eq: balance_after_v2_after_restart);

    // Now a hard crash. Must recover the same v2 state.
    kill_node!(ctx);
    restart_node!(ctx);

    let list = ctx.list_functions(0).await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].version, 2);
    assert_balance!(ctx, account: 1, eq: balance_after_v2_after_restart);

    deposit_via_wasm(&ctx, 1, 10, 203).await;
    assert_balance!(ctx, account: 1, eq: balance_after_v2_after_restart + 20);
}

// ═════════════════════════════════════════════════════════════════════════
// Bonus: unregister-across-crash coverage
//   Register v1 → deposit → unregister → crash → restart →
//   function should still be absent (unregister WAL record replayed).
// ═════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn wasm_unregister_persists_across_crash() {
    if E2EBackend::from_env() == E2EBackend::Inline {
        eprintln!("skipping wasm_unregister_persists_across_crash on Inline backend");
        return;
    }

    let mut ctx = crate::e2e::E2EContext::new(profile("single_node")).await;

    ctx.register_function(0, "deposit", &compile(DEPOSIT_WAT_V1), false)
        .await;
    deposit_via_wasm(&ctx, 1, 100, 1).await;
    ctx.unregister_function(0, "deposit").await;
    assert!(ctx.list_functions(0).await.is_empty());

    kill_node!(ctx);
    restart_node!(ctx);

    // Unregister replayed — name is still absent.
    assert!(
        ctx.list_functions(0).await.is_empty(),
        "unregister should survive a crash"
    );

    // Balance from the successful deposit survives too.
    assert_balance!(ctx, account: 1, eq: 100);

    // Named now fails because handler is gone.
    let result = ctx
        .submit_function(
            0,
            "deposit",
            [1, 100, 0, 0, 0, 0, 0, 0],
            42,
            wait_level!(committed),
        )
        .await;
    assert_ne!(
        result.fail_reason, 0,
        "Named on unregistered function should fail"
    );
}
