//! Basic correctness E2E tests.
//!
//! Balance invariants, WAL checksums, double-entry verification.
//! These verify fundamental ledger operations through the E2E DSL,
//! running against whichever backend is selected at runtime.

use crate::e2e::lib::matrix_test::MatrixGrid;
use std::sync::Arc;

#[tokio::test]
async fn deposit_committed_reflects_in_balance() {
    let ctx = e2e_ctx!(profile: single_node);

    deposit!(ctx, account: 1, amount: 1000, wait: on_snapshot);

    assert_balance!(ctx, account: 1, eq: 1000);
}

/// Matrix Testing Scenario — sequential iteration submission.
/// See `tests/e2e/matrix-testing-scenario.md`.
#[tokio::test]
async fn matrix_transfer_grid() {
    let ctx = e2e_ctx!(profile: single_node);
    let grid = MatrixGrid::new(100, 100, 10_000, 10);

    const ITERATION_COUNT: u64 = 100;

    // Step 1 — Fund all accounts
    deposit_all!(ctx, &grid.deposits, wait: on_snapshot);

    for &(account, _) in &grid.deposits {
        assert_balance!(ctx, account: account, eq: grid.initial_balance as i64,
            "step 1: account {}", account);
    }

    // Step 2 — Bulk transfer run (no intermediate checks)
    let mut total_rejected = 0usize;

    for _ in 0..ITERATION_COUNT {
        let (_tx_id, rejected) = transfer_batch!(ctx, &grid.transfers, wait: committed);
        total_rejected += rejected;
    }

    // Read balances at snapshot level after committed transfers.
    let balances = ctx.get_balances(0, &grid.account_ids).await;
    assert_eq!(balances.len(), grid.account_ids.len());
    for (account, actual) in &balances {
        let (r, c) = grid.grid_position(*account);
        let expected = grid.expected_balance(r, c, ITERATION_COUNT);
        assert_eq!(*actual, expected, "step 2: account ({},{})", r, c);
    }

    let sum = ctx.get_balance_sum(0, grid.grid_accounts() + 1).await;
    assert_eq!(sum, 0, "step 2: zero-sum violated, sum={}", sum);
    assert_eq!(total_rejected, 0, "step 2: {} rejections", total_rejected);

    // Step 3 — Checked transfer run (verify after each iteration)
    for iter in 1..=ITERATION_COUNT {
        let t = ITERATION_COUNT + iter;

        let (_tx_id, rejected) = transfer_batch!(ctx, &grid.transfers, wait: on_snapshot);
        assert_eq!(rejected, 0, "step 3 iter {}: {} rejections", iter, rejected);

        let balances = ctx.get_balances(0, &grid.account_ids).await;
        assert_eq!(balances.len(), grid.account_ids.len());
        for (account, actual) in &balances {
            let (r, c) = grid.grid_position(*account);
            let expected = grid.expected_balance(r, c, t);
            assert_eq!(
                *actual, expected,
                "step 3 iter {}: account ({},{})",
                iter, r, c
            );
        }

        let sum = ctx.get_balance_sum(0, grid.grid_accounts() + 1).await;
        assert_eq!(
            sum, 0,
            "step 3 iter {}: zero-sum violated, sum={}",
            iter, sum
        );
    }
}

/// Concurrent variant of `matrix_transfer_grid`.
///
/// Same grid topology and invariants, but iterations fire concurrently.
/// Stresses the pipeline under parallel gRPC load.
#[tokio::test]
async fn matrix_concurrent_transfer_grid() {
    let ctx = Arc::new(e2e_ctx!(profile: single_node));
    let grid = MatrixGrid::new(100, 100, 10_000, 10);

    const ITERATION_COUNT: u64 = 100;
    const CONCURRENCY: usize = 8;

    // Step 1 — Fund all accounts
    ctx.deposit_all(0, &grid.deposits, wait_level!(on_snapshot))
        .await;

    let balances = ctx.get_balances(0, &grid.account_ids).await;
    assert_eq!(balances.len(), grid.account_ids.len());
    for (account, actual) in &balances {
        assert_balance!(ctx, account: *account, eq: *actual, "step 1: account {}", account);
    }

    // Step 2 — All iterations fired concurrently
    let transfers = Arc::new(grid.transfers.clone());
    let mut handles = Vec::new();

    for _ in 0..ITERATION_COUNT {
        let ctx = ctx.clone();
        let transfers = transfers.clone();
        handles.push(tokio::spawn(async move {
            ctx.transfer_batch(0, &transfers, wait_level!(committed))
                .await
        }));
    }

    let mut last_tx_id = 0u64;
    let mut total_rejected = 0usize;

    for handle in handles {
        let (tx_id, rejected) = handle.await.expect("iteration task panicked");
        last_tx_id = last_tx_id.max(tx_id);
        total_rejected += rejected;
    }

    ctx.wait_for_snapshot(0, last_tx_id).await;

    let balances = ctx.get_balances(0, &grid.account_ids).await;
    assert_eq!(balances.len(), grid.account_ids.len());
    for (account, actual) in &balances {
        let (r, c) = grid.grid_position(*account);
        let expected = grid.expected_balance(r, c, ITERATION_COUNT);
        assert_eq!(*actual, expected, "step 2: account ({},{})", r, c);
    }

    let sum = ctx.get_balance_sum(0, grid.grid_accounts() + 1).await;
    assert_eq!(sum, 0, "step 2: zero-sum violated, sum={}", sum);
    assert_eq!(total_rejected, 0, "step 2: {} rejections", total_rejected);

    // Step 3 — Per-iteration check, chunks fired concurrently within each
    let chunk_size = grid.transfers.len().div_ceil(CONCURRENCY);
    let transfer_chunks: Vec<Vec<(u64, u64, u64)>> = grid
        .transfers
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    for iter in 1..=ITERATION_COUNT {
        let t = ITERATION_COUNT + iter;

        let mut handles = Vec::new();
        for chunk in &transfer_chunks {
            let ctx = ctx.clone();
            let chunk = chunk.clone();
            handles.push(tokio::spawn(async move {
                ctx.transfer_batch(0, &chunk, wait_level!(committed)).await
            }));
        }

        let mut iter_last_tx_id = 0u64;
        let mut iter_rejected = 0usize;

        for handle in handles {
            let (tx_id, rejected) = handle.await.expect("chunk task panicked");
            iter_last_tx_id = iter_last_tx_id.max(tx_id);
            iter_rejected += rejected;
        }

        ctx.wait_for_snapshot(0, iter_last_tx_id).await;
        assert_eq!(
            iter_rejected, 0,
            "step 3 iter {}: {} rejections",
            iter, iter_rejected
        );

        let balances = ctx.get_balances(0, &grid.account_ids).await;
        assert_eq!(balances.len(), grid.account_ids.len());
        for (account, actual) in &balances {
            let (r, c) = grid.grid_position(*account);
            let expected = grid.expected_balance(r, c, t);
            assert_eq!(
                *actual, expected,
                "step 3 iter {}: account ({},{})",
                iter, r, c
            );
        }

        let sum = ctx.get_balance_sum(0, grid.grid_accounts() + 1).await;
        assert_eq!(
            sum, 0,
            "step 3 iter {}: zero-sum violated, sum={}",
            iter, sum
        );
    }
}
