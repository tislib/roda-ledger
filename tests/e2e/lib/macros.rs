//! DSL macros for E2E tests.
//!
//! All test logic is expressed through macros. Tests read as high-level
//! specifications. Implementation complexity is hidden in the macro layer.
//!
//! `ctx` is always the first argument. `node: N` (default: 0) targets a
//! specific node — meaningful today for kill/restart, essential for Raft
//! multi-node tests.
//!
//! See ADR-012 for the full macro reference.

// ---------------------------------------------------------------------------
// Context bootstrap
// ---------------------------------------------------------------------------

/// Create an `E2EContext` for the given profile.
#[macro_export]
macro_rules! e2e_ctx {
    (profile: $profile:ident) => {
        $crate::e2e::E2EContext::new($crate::e2e::profile(stringify!($profile))).await
    };
}

// ---------------------------------------------------------------------------
// Wait level mapping
// ---------------------------------------------------------------------------

#[macro_export]
macro_rules! wait_level {
    (committed) => {
        roda_ledger::grpc::proto::WaitLevel::Committed as i32
    };
    (computed) => {
        roda_ledger::grpc::proto::WaitLevel::Computed as i32
    };
    (on_snapshot) => {
        roda_ledger::grpc::proto::WaitLevel::Snapshot as i32
    };
}

// ===========================================================================
// 1. Actions
// ===========================================================================

#[macro_export]
macro_rules! deposit {
    ($ctx:expr, account: $account:expr, amount: $amount:expr, wait: $level:ident) => {
        $ctx.deposit(0, $account, $amount, wait_level!($level))
            .await
    };
    ($ctx:expr, node: $node:expr, account: $account:expr, amount: $amount:expr, wait: $level:ident) => {
        $ctx.deposit($node, $account, $amount, wait_level!($level))
            .await
    };
}

#[macro_export]
macro_rules! withdraw {
    ($ctx:expr, account: $account:expr, amount: $amount:expr, wait: $level:ident) => {
        $ctx.withdraw(0, $account, $amount, wait_level!($level))
            .await
    };
    ($ctx:expr, node: $node:expr, account: $account:expr, amount: $amount:expr, wait: $level:ident) => {
        $ctx.withdraw($node, $account, $amount, wait_level!($level))
            .await
    };
}

#[macro_export]
macro_rules! transfer {
    ($ctx:expr, from: $from:expr, to: $to:expr, amount: $amount:expr, wait: $level:ident) => {
        $ctx.transfer(0, $from, $to, $amount, wait_level!($level))
            .await
    };
    ($ctx:expr, node: $node:expr, from: $from:expr, to: $to:expr, amount: $amount:expr, wait: $level:ident) => {
        $ctx.transfer($node, $from, $to, $amount, wait_level!($level))
            .await
    };
}

#[macro_export]
macro_rules! batch_deposit {
    ($ctx:expr, account: $account:expr, amount: $amount:expr, count: $count:expr, wait: $level:ident) => {
        $ctx.batch_deposit(0, $account, $amount, $count, wait_level!($level))
            .await
    };
    ($ctx:expr, node: $node:expr, account: $account:expr, amount: $amount:expr, count: $count:expr, wait: $level:ident) => {
        $ctx.batch_deposit($node, $account, $amount, $count, wait_level!($level))
            .await
    };
}

/// Deposit to multiple accounts in a batch. `deposits` is `&[(account, amount)]`.
#[macro_export]
macro_rules! deposit_all {
    ($ctx:expr, $deposits:expr, wait: $level:ident) => {
        $ctx.deposit_all(0, $deposits, wait_level!($level)).await
    };
    ($ctx:expr, node: $node:expr, $deposits:expr, wait: $level:ident) => {
        $ctx.deposit_all($node, $deposits, wait_level!($level))
            .await
    };
}

/// Submit a batch of transfers. `transfers` is `&[(from, to, amount)]`.
/// Returns `(last_tx_id, rejected_count)`.
#[macro_export]
macro_rules! transfer_batch {
    ($ctx:expr, $transfers:expr, wait: $level:ident) => {
        $ctx.transfer_batch(0, $transfers, wait_level!($level))
            .await
    };
    ($ctx:expr, node: $node:expr, $transfers:expr, wait: $level:ident) => {
        $ctx.transfer_batch($node, $transfers, wait_level!($level))
            .await
    };
}

// ===========================================================================
// 2. Verifications
// ===========================================================================

#[macro_export]
macro_rules! assert_balance {
    ($ctx:expr, account: $account:expr, eq: $expected:expr) => {
        assert_eq!(
            $ctx.get_balance(0, $account).await,
            $expected,
            "balance mismatch for account {}",
            $account
        )
    };
    ($ctx:expr, account: $account:expr, eq: $expected:expr, $($arg:tt)+) => {
        assert_eq!(
            $ctx.get_balance(0, $account).await,
            $expected,
            $($arg)+
        )
    };
    ($ctx:expr, node: $node:expr, account: $account:expr, eq: $expected:expr) => {
        assert_eq!(
            $ctx.get_balance($node, $account).await,
            $expected,
            "balance mismatch for account {} on node {}",
            $account,
            $node
        )
    };
}

#[macro_export]
macro_rules! assert_balance_sum {
    ($ctx:expr, eq: $expected:expr) => {
        assert_eq!(
            $ctx.get_balance_sum(0, 1_000_000).await,
            $expected,
            "balance sum invariant violated"
        )
    };
    ($ctx:expr, node: $node:expr, eq: $expected:expr) => {
        assert_eq!(
            $ctx.get_balance_sum($node, 1_000_000).await,
            $expected,
            "balance sum invariant violated on node {}",
            $node
        )
    };
}

#[macro_export]
macro_rules! assert_wal_checksum {
    ($ctx:expr) => {
        todo!("assert_wal_checksum! not yet implemented")
    };
    ($ctx:expr, node: $node:expr) => {
        todo!("assert_wal_checksum! not yet implemented")
    };
}

#[macro_export]
macro_rules! assert_wal_valid {
    ($ctx:expr, node: $node:expr) => {
        todo!("assert_wal_valid! not yet implemented")
    };
}

#[macro_export]
macro_rules! assert_tx_status {
    ($ctx:expr, $tx_id:expr, $status:ident) => {
        todo!("assert_tx_status! not yet implemented")
    };
}

// ===========================================================================
// 3. Reading
// ===========================================================================

#[macro_export]
macro_rules! get_balance {
    ($ctx:expr, account: $account:expr) => {
        $ctx.get_balance(0, $account).await
    };
    ($ctx:expr, node: $node:expr, account: $account:expr) => {
        $ctx.get_balance($node, $account).await
    };
}

#[macro_export]
macro_rules! get_pipeline_index {
    ($ctx:expr) => {
        $ctx.get_pipeline_index(0).await
    };
    ($ctx:expr, node: $node:expr) => {
        $ctx.get_pipeline_index($node).await
    };
}

#[macro_export]
macro_rules! get_transaction {
    ($ctx:expr, $tx_id:expr) => {
        todo!("get_transaction! not yet implemented")
    };
}

#[macro_export]
macro_rules! get_last_committed_id {
    ($ctx:expr, node: $node:expr) => {
        $ctx.get_last_committed_id($node).await
    };
}

#[macro_export]
macro_rules! get_segments {
    ($ctx:expr, node: $node:expr) => {
        todo!("get_segments! not yet implemented")
    };
}

// ===========================================================================
// 4. Runtime Intervention
// ===========================================================================

#[macro_export]
macro_rules! kill_node {
    ($ctx:expr) => {
        $ctx.kill_node(0)
    };
    ($ctx:expr, node: $node:expr) => {
        $ctx.kill_node($node)
    };
}

#[macro_export]
macro_rules! restart_node {
    ($ctx:expr) => {
        $ctx.restart_node(0).await
    };
    ($ctx:expr, node: $node:expr) => {
        $ctx.restart_node($node).await
    };
}

#[macro_export]
macro_rules! kill_and_restart {
    ($ctx:expr) => {
        $ctx.kill_node(0);
        $ctx.restart_node(0).await
    };
    ($ctx:expr, node: $node:expr) => {
        $ctx.kill_node($node);
        $ctx.restart_node($node).await
    };
}

#[macro_export]
macro_rules! slow_cpu {
    ($ctx:expr, node: $node:expr, factor: $factor:expr) => {
        todo!("slow_cpu! not yet implemented")
    };
}

#[macro_export]
macro_rules! slow_disk {
    ($ctx:expr, node: $node:expr, latency_ms: $ms:expr) => {
        todo!("slow_disk! not yet implemented")
    };
}

#[macro_export]
macro_rules! limit_memory {
    ($ctx:expr, node: $node:expr, mb: $mb:expr) => {
        todo!("limit_memory! not yet implemented")
    };
}

#[macro_export]
macro_rules! restore {
    ($ctx:expr, node: $node:expr) => {
        todo!("restore! not yet implemented")
    };
}

#[macro_export]
macro_rules! wait_ms {
    ($ms:expr) => {
        tokio::time::sleep(std::time::Duration::from_millis($ms)).await
    };
}

#[macro_export]
macro_rules! wait_until_committed {
    ($ctx:expr, $tx_id:expr) => {
        $ctx.wait_until_committed(0, $tx_id).await
    };
    ($ctx:expr, node: $node:expr, $tx_id:expr) => {
        $ctx.wait_until_committed($node, $tx_id).await
    };
}

// ===========================================================================
// 5. Deep Inspection
// ===========================================================================

#[macro_export]
macro_rules! assert_segment_sealed {
    ($ctx:expr, node: $node:expr, segment_id: $id:expr) => {
        todo!("assert_segment_sealed! not yet implemented")
    };
}

#[macro_export]
macro_rules! assert_segment_count {
    ($ctx:expr, node: $node:expr, gte: $min:expr) => {
        todo!("assert_segment_count! not yet implemented")
    };
}

#[macro_export]
macro_rules! assert_snapshot_valid {
    ($ctx:expr, node: $node:expr) => {
        todo!("assert_snapshot_valid! not yet implemented")
    };
}

#[macro_export]
macro_rules! assert_pipeline_caught_up {
    ($ctx:expr, node: $node:expr) => {
        todo!("assert_pipeline_caught_up! not yet implemented")
    };
}

#[macro_export]
macro_rules! assert_index_valid {
    ($ctx:expr, node: $node:expr) => {
        todo!("assert_index_valid! not yet implemented")
    };
}
