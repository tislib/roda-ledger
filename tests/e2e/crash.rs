//! Crash recovery E2E tests.
//!
//! Verify that the ledger recovers to exactly the committed state
//! after a kill -9. Only supported on the Process backend.

use crate::e2e::E2EContext;
use crate::e2e::lib::backend::E2EBackend;
use crate::e2e::lib::profile::profile;
use roda_ledger::grpc::proto::submit_and_wait_request::Operation;
use roda_ledger::grpc::proto::{
    Deposit, SubmitAndWaitRequest, SubmitBatchAndWaitRequest, WaitLevel,
};

/// Simple crash recovery test parameterized by WAL segment size.
///
/// 1. Submit 1M deposits to account 1 (1k batches × 1k ops, concurrently)
/// 2. Kill the server process
/// 3. Restart — server recovers from WAL
/// 4. Verify pipeline index shows 1M for all stages
/// 5. Verify balances: account 0 = -(1M × amount), account 1 = 1M × amount
///
/// Three segment-size parameterizations:
/// - all_active:     segment large enough to hold everything in one active segment
/// - single_closed:  segment = 60% of total → 1 closed segment + active remainder
/// - ten_closed:     segment = 10% of total → ~10 closed segments + active remainder
#[tokio::test]
async fn simple_crash_recovery() {
    if E2EBackend::from_env() == E2EBackend::Inline {
        eprintln!("skipping simple_crash_recovery on Inline backend (requires Process)");
        return;
    }

    const TX_COUNT: u64 = 1_000_000;
    const BATCH_SIZE: usize = 1_000;
    const BATCH_COUNT: usize = 1_000; // BATCH_SIZE × BATCH_COUNT = TX_COUNT
    const AMOUNT: u64 = 1;
    const ACCOUNT: u64 = 1;

    // 1 deposit = 1 TxMetadata(40) + 2 TxEntry(80) = 120 bytes.
    // 1M deposits ≈ 120 MB total WAL data.
    let total_wal_mb: u64 = TX_COUNT * 120 / (1024 * 1024);

    let segment_sizes: Vec<(&str, u64)> = vec![
        ("all_active", total_wal_mb + 10), // everything in one active segment
        ("single_closed", total_wal_mb * 60 / 100), // ~1 closed + active
        ("ten_closed", total_wal_mb * 10 / 100), // ~10 closed + active
    ];

    for (label, segment_size_mb) in segment_sizes {
        eprintln!("[{}] wal_segment_size_mb = {}", label, segment_size_mb);

        // Override wal_segment_size_mb from the base profile.
        let mut profile = profile("single_node");
        profile.ledger.storage.wal_segment_size_mb = segment_size_mb;

        let mut ctx = E2EContext::new(profile).await;

        // ==================================================================
        // Step 1 — Submit 1M deposits concurrently (1k batches × 1k ops)
        // ==================================================================

        let mut handles = Vec::new();

        for _ in 0..BATCH_COUNT {
            let mut client = ctx.raw_client(0);
            handles.push(tokio::spawn(async move {
                let operations = (0..BATCH_SIZE)
                    .map(|_| SubmitAndWaitRequest {
                        operation: Some(Operation::Deposit(Deposit {
                            account: ACCOUNT,
                            amount: AMOUNT,
                            user_ref: 0,
                        })),
                        wait_level: 0,
                    })
                    .collect();

                let response = client
                    .submit_batch_and_wait(SubmitBatchAndWaitRequest {
                        operations,
                        wait_level: WaitLevel::Committed as i32,
                    })
                    .await
                    .expect("batch deposit RPC failed")
                    .into_inner();

                let rejected = response
                    .results
                    .iter()
                    .filter(|r| r.fail_reason != 0)
                    .count();
                let last_tx_id = response.results.last().unwrap().transaction_id;
                (last_tx_id, rejected)
            }));
        }

        let mut max_tx_id = 0u64;
        let mut total_rejected = 0usize;

        for handle in handles {
            let (tx_id, rejected) = handle.await.expect("batch task panicked");
            max_tx_id = max_tx_id.max(tx_id);
            total_rejected += rejected;
        }

        assert_eq!(
            total_rejected, 0,
            "[{}] expected 0 rejections, got {}",
            label, total_rejected
        );
        assert_eq!(
            max_tx_id, TX_COUNT,
            "[{}] expected last tx_id={}, got {}",
            label, TX_COUNT, max_tx_id
        );

        // ==================================================================
        // Step 2 — Kill
        // ==================================================================

        ctx.kill_node(0);

        // ==================================================================
        // Step 3 — Restart (server recovers from WAL)
        // ==================================================================

        ctx.restart_node(0).await;

        // ==================================================================
        // Step 4 — Verify pipeline index shows TX_COUNT for all stages
        // ==================================================================

        ctx.wait_for_snapshot(0, TX_COUNT).await;

        let (compute, commit, snapshot) = ctx.get_pipeline_index(0).await;
        assert_eq!(
            compute, TX_COUNT,
            "[{}] compute_index: expected {}, got {}",
            label, TX_COUNT, compute
        );
        assert_eq!(
            commit, TX_COUNT,
            "[{}] commit_index: expected {}, got {}",
            label, TX_COUNT, commit
        );
        assert_eq!(
            snapshot, TX_COUNT,
            "[{}] snapshot_index: expected {}, got {}",
            label, TX_COUNT, snapshot
        );

        // ==================================================================
        // Step 5 — Verify balances
        // ==================================================================

        let balance_account = ctx.get_balance(0, ACCOUNT).await;
        assert_eq!(
            balance_account,
            (TX_COUNT * AMOUNT) as i64,
            "[{}] account {} balance: expected {}, got {}",
            label,
            ACCOUNT,
            TX_COUNT * AMOUNT,
            balance_account
        );

        let balance_system = ctx.get_balance(0, 0).await;
        assert_eq!(
            balance_system,
            -((TX_COUNT * AMOUNT) as i64),
            "[{}] system account balance: expected {}, got {}",
            label,
            -((TX_COUNT * AMOUNT) as i64),
            balance_system
        );

        eprintln!("[{}] PASSED", label);
    }
}
