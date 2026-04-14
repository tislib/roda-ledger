//! Crash recovery E2E tests.
//!
//! Verify that the ledger recovers to exactly the committed state
//! after a kill -9. Only supported on the Process backend.

use crate::e2e::lib::backend::E2EBackend;
use crate::e2e::lib::profile::profile;
use std::time::Duration;
use tokio::time::sleep;

/// Simple crash recovery test parameterized by WAL segment size.
///
/// 1. Submit 1M deposits concurrently (1k batches × 1k ops)
/// 2. Kill and restart
/// 3. Verify pipeline index shows 1M for all stages
/// 4. Verify balances
#[tokio::test(flavor = "multi_thread")]
async fn simple_crash_recovery() {
    if E2EBackend::from_env() == E2EBackend::Inline {
        eprintln!("skipping simple_crash_recovery on Inline backend (requires Process)");
        return;
    }

    const TX_COUNT: u64 = 1_000_000;
    const AMOUNT: u64 = 1;
    const ACCOUNT: u64 = 1;

    let segment_configs: Vec<(&str, u64)> = vec![
        ("all_active", TX_COUNT + 10),
        ("single_closed", TX_COUNT * 60 / 100),
        ("ten_closed", TX_COUNT * 10 / 100),
    ];

    for (label, transaction_count_per_segment) in segment_configs {
        eprintln!("[{}] transaction_count_per_segment = {}", label, transaction_count_per_segment);

        let mut profile = profile("single_node");
        profile.ledger.storage.transaction_count_per_segment = transaction_count_per_segment;

        let mut ctx = crate::e2e::E2EContext::new(profile).await;

        // Step 1 — Submit 1M deposits concurrently
        ctx.deposit_batch_concurrent(0, ACCOUNT, AMOUNT, 1000, 1000, wait_level!(committed), 0)
            .await;

        // Step 2 — Kill and restart
        kill_and_restart!(ctx);

        // Step 3 — Verify pipeline index
        ctx.wait_for_snapshot(0, TX_COUNT).await;

        let (compute, commit, snapshot) = get_pipeline_index!(ctx);
        assert_eq!(compute, TX_COUNT, "[{}] compute_index", label);
        assert_eq!(commit, TX_COUNT, "[{}] commit_index", label);
        assert_eq!(snapshot, TX_COUNT, "[{}] snapshot_index", label);

        // Step 4 — Verify balances
        assert_balance!(ctx, account: ACCOUNT, eq: (TX_COUNT * AMOUNT) as i64,
            "[{}] account {}", label, ACCOUNT);
        assert_balance!(ctx, account: 0, eq: -((TX_COUNT * AMOUNT) as i64),
            "[{}] system account", label);

        eprintln!("[{}] PASSED", label);
    }
}

/// Crash-in-the-middle with idempotent retry — no transaction lost.
///
/// Submits 1M deposits concurrently with retries enabled. The server is
/// killed 500ms into the load. Failed batches reconnect and retry with
/// the same user_ref (idempotency key). After all batches complete,
/// verify balance = TX_COUNT × AMOUNT exactly.
#[tokio::test(flavor = "multi_thread")]
async fn crash_in_the_middle() {
    if E2EBackend::from_env() == E2EBackend::Inline {
        eprintln!("skipping crash_in_the_middle on Inline backend (requires Process)");
        return;
    }

    const BATCH_COUNT: usize = 200;
    const BATCH_SIZE: usize = 5000;
    const TX_COUNT: u64 = BATCH_COUNT as u64 * BATCH_SIZE as u64;
    const AMOUNT: u64 = 1;
    const ACCOUNT: u64 = 1;

    // retry 100 times to ensure idempotency
    for i in 0..100 {
        let label = format!("iteration_{}", i);
        eprintln!("iteration: {}", i);
        let mut profile = profile("single_node");
        profile.ledger.storage.transaction_count_per_segment = TX_COUNT + 10;

        let mut ctx = crate::e2e::E2EContext::new(profile).await;

        // initial crash
        kill_node!(ctx);
        eprintln!("[crash_mid:{}] server killed", label);

        // Restart so retry tasks can reconnect.
        restart_node!(ctx);
        eprintln!("[crash_mid:{}] server restarted", label);

        // Spawn concurrent deposit load with retries.
        // deposit_batch_concurrent_detached returns JoinHandles, does NOT
        // join them — the caller can kill/restart while they're in flight.
        let handles = ctx.deposit_batch_concurrent_detached(
            0,
            ACCOUNT,
            AMOUNT,
            BATCH_COUNT,
            BATCH_SIZE,
            wait_level!(committed),
            1000,
        );

        // Let batches start sending, then kill.
        sleep(Duration::from_millis(500)).await;
        for i in 0..10 {
            let crash_timing = 10 * (i % 10);
            sleep(Duration::from_millis(crash_timing)).await;

            kill_node!(ctx);
            eprintln!("[crash_mid:{}] server killed", label);

            // Restart so retry tasks can reconnect.
            restart_node!(ctx);
            eprintln!("[crash_mid:{}] server restarted", label);
        }

        // Wait for all deposit tasks to finish (retries included).
        for handle in handles {
            handle.await.expect("deposit batch task panicked");
        }
        eprintln!("[crash_mid:{}] all batches completed", label);

        // Verify: nothing lost, nothing doubled.
        let (compute, commit, snapshot) = get_pipeline_index!(ctx);
        eprintln!(
            "[crash_mid:{}] pipeline: compute={}, commit={}, snapshot={}",
            label, compute, commit, snapshot
        );

        assert!(
            commit >= TX_COUNT,
            "[{}] commit ({}) < TX_COUNT ({})",
            label,
            commit,
            TX_COUNT
        );
        ctx.wait_for_snapshot(0, commit).await;

        assert_balance!(ctx, account: ACCOUNT, eq: (TX_COUNT * AMOUNT) as i64,
                "[{}] account {}", label, ACCOUNT);
        assert_balance!(ctx, account: 0, eq: -((TX_COUNT * AMOUNT) as i64),
                "[{}] system account", label);

        eprintln!("[crash_mid:{}] PASSED", label);
    }
}
