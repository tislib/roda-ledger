//! End-to-end correctness invariants: balance convergence, byte-exact
//! WAL replication, durable acknowledged writes.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::ledger::WaitLevel;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl};
use roda_ledger::entities::SYSTEM_ACCOUNT_ID;
use std::time::Duration;

const ACCOUNT_A: u64 = 11;
const ACCOUNT_B: u64 = 22;
const AMOUNT: u64 = 100;

/// After leader churn under continuous writes, all surviving nodes
/// converge on the same balances for every account.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balances_converge_across_nodes_after_churn() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let mut user_ref: u64 = 1;
    for round in 0..3 {
        for _ in 0..10 {
            ctl.deposit_and_wait(ACCOUNT_A, AMOUNT, user_ref, WaitLevel::ClusterCommit)
                .await
                .unwrap();
            user_ref += 1;
            ctl.deposit_and_wait(ACCOUNT_B, AMOUNT, user_ref, WaitLevel::ClusterCommit)
                .await
                .unwrap();
            user_ref += 1;
        }
        // Kill current leader and let a new one rise (except on last round).
        if round < 2 {
            let li = ctl.leader_index().await.unwrap();
            ctl.stop_node(li).await.expect("stop");
            ctl.start_node(li).await.expect("restart");
            let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
        }
    }

    // Final convergence: every node sees the same A and B balance.
    // 30 deposits per account × 3 rounds = 30 each (the round-2 deposits
    // were also counted) — actually 10 deposits/account × 3 rounds = 30.
    let target_a = (30 * AMOUNT) as i64;
    let target_b = (30 * AMOUNT) as i64;
    for i in 0..ctl.len() {
        ctl.require_balance_on(i, ACCOUNT_A, target_a).await;
        ctl.require_balance_on(i, ACCOUNT_B, target_b).await;
        ctl.require_balance_on(i, SYSTEM_ACCOUNT_ID, -(target_a + target_b))
            .await;
    }
}

/// Total system + account balances sum to 0 on every node (zero-sum
/// invariant) after sustained transfers across nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_sum_holds_on_every_node_after_load() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Bootstrap account A.
    ctl.deposit_and_wait(ACCOUNT_A, 10_000, 0, WaitLevel::ClusterCommit)
        .await
        .unwrap();
    // 50 transfers A → B.
    for _ in 0..50 {
        ctl.transfer_and_wait(ACCOUNT_A, ACCOUNT_B, 100, 0, WaitLevel::ClusterCommit)
            .await
            .unwrap();
    }

    // Every node must converge to the same balances + zero-sum.
    for i in 0..ctl.len() {
        ctl.require_balance_on(i, ACCOUNT_A, 10_000 - 50 * 100).await;
        ctl.require_balance_on(i, ACCOUNT_B, 50 * 100).await;
        ctl.require_balance_on(i, SYSTEM_ACCOUNT_ID, -10_000).await;
    }
}

/// A tx that returned `WaitLevel::ClusterCommit` success is still
/// committed on the cluster after any single-node failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_commit_acked_tx_survives_one_node_failure() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let mut acked = Vec::new();
    for ur in 1..=10u64 {
        let r = ctl
            .deposit_and_wait(ACCOUNT_A, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .unwrap();
        acked.push(r.tx_id);
    }

    // Kill any single node and observe survivors.
    ctl.stop_node(0).await.expect("stop");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    for tx_id in &acked {
        ctl.require_transaction_committed(*tx_id).await;
    }
}

/// Followers' WAL is byte-exact with the leader's after catch-up.
/// Verified by comparing each follower's `wal_tailer().tail()` output
/// to the leader's for the same byte range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_is_byte_exact_across_replicas() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig {
        replication_poll_ms: 5,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // The harness's `deposit_and_wait` already retries through
    // `with_leader_retry` if the cached leader briefly stepped down.
    for ur in 1..=20u64 {
        ctl.deposit_and_wait(ACCOUNT_A, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .expect("deposit");
    }

    let leader_idx = ctl.leader_index().await.unwrap();
    let leader_ledger = ctl.ledger(leader_idx).unwrap();

    // Wait for every follower to catch up at the WAL level.
    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        ctl.require_pipeline_commit_at_least(i, 20).await;
    }

    // Compare WAL bytes for tx range [1, 20] across every node.
    let mut buf = vec![0u8; 64 * 1024];
    let mut leader_tailer = leader_ledger.wal_tailer();
    let n_leader = leader_tailer.tail(1, &mut buf) as usize;
    let leader_bytes = buf[..n_leader].to_vec();

    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        let follower_ledger = ctl.ledger(i).unwrap();
        let mut t = follower_ledger.wal_tailer();
        let mut fbuf = vec![0u8; 64 * 1024];
        let n = t.tail(1, &mut fbuf) as usize;
        assert_eq!(
            n, n_leader,
            "follower {} streamed {} bytes; leader streamed {}",
            i, n, n_leader
        );
        assert_eq!(
            fbuf[..n],
            leader_bytes[..n],
            "WAL bytes diverge between leader and follower {}",
            i
        );
    }
}
