//! Basic correctness E2E tests.
//!
//! Balance invariants, WAL checksums, double-entry verification.
//! These verify fundamental ledger operations through the E2E DSL,
//! running against whichever backend is selected at runtime.

#[tokio::test]
async fn deposit_committed_reflects_in_balance() {
    let ctx = e2e_ctx!(profile: single_node);

    deposit!(ctx, account: 1, amount: 1000, wait: on_snapshot);

    assert_balance!(ctx, account: 1, eq: 1000);
}
