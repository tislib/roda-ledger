//! End-to-end correctness invariants: balance convergence, byte-exact
//! WAL replication, durable acknowledged writes.

use ::proto::ledger::WaitLevel;
use cluster::testing::{ClusterTestingConfig, ClusterTestingControl};
use spdlog::warn;
use std::time::{Duration, Instant};
use storage::entities::SYSTEM_ACCOUNT_ID;

const ACCOUNT_A: u64 = 11;
const ACCOUNT_B: u64 = 22;
const AMOUNT: u64 = 100;

/// After leader churn under continuous writes, all surviving nodes
/// converge on the same balances for every account.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn balances_converge_across_nodes_after_churn() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let mut user_ref: u64 = 1;
    for round in 0..3 {
        for _ in 0..10 {
            let r1 = ctl
                .deposit_and_wait_result(ACCOUNT_A, AMOUNT, user_ref, true)
                .await
                .unwrap();
            warn!("r1:{}", r1.tx_id);
            user_ref += 1;
            let r2 = ctl
                .deposit_and_wait_result(ACCOUNT_B, AMOUNT, user_ref, true)
                .await
                .unwrap();
            warn!("r2:{}", r2.tx_id);
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
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
        ctl.require_balance_on(i, ACCOUNT_A, 10_000 - 50 * 100)
            .await;
        ctl.require_balance_on(i, ACCOUNT_B, 50 * 100).await;
        ctl.require_balance_on(i, SYSTEM_ACCOUNT_ID, -10_000).await;
    }
}

/// A tx that returned `WaitLevel::ClusterCommit` success is still
/// committed on the cluster after any single-node failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cluster_commit_acked_tx_survives_one_node_failure() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let mut acked = Vec::new();
    for ur in 1..=10u64 {
        let r = ctl
            .deposit_and_wait_result(ACCOUNT_A, AMOUNT, ur, true)
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

/// Cluster commit must advance on every node after 10k individual
/// fire-and-forget deposits. Under burst load the leader can be
/// starved of heartbeats and the cluster re-elects mid-burst, which
/// historically dragged in O(extras) §5.3 walk-back recovery; with
/// the ConflictIndex/ConflictTerm fast-backoff in place (Raft paper
/// §5.3, end of section) catch-up is now O(distinct terms).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cluster_commit_advances_under_burst() {
    const N: u64 = 10_000;
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    for ur in 1..=N {
        ctl.deposit(ACCOUNT_A, AMOUNT, ur).await.expect("deposit");
    }

    let deadline = Instant::now() + Duration::from_secs(60);
    for i in 0..ctl.len() {
        loop {
            let pi = ctl.pipeline_index_on(i).await.expect("pipeline_index");
            if pi.cluster_commit >= N {
                break;
            }
            if Instant::now() > deadline {
                panic!(
                    "node {i} cluster_commit stuck at {} (expected >= {N}); compute={} commit={} snapshot={}",
                    pi.cluster_commit, pi.compute, pi.commit, pi.snapshot
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let target = (N as i64) * (AMOUNT as i64);
    for i in 0..ctl.len() {
        ctl.require_balance_on(i, ACCOUNT_A, target).await;
    }
}

/// Followers serve the same committed WAL records as the leader via
/// `GetLog`. Records are canonically-encoded proto3, so record-equality
/// across replicas is the gRPC-visible analogue of byte-equality.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn wal_is_byte_exact_across_replicas() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig {
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // The harness's `deposit_and_wait` already retries through
    // `with_leader_retry` if the cached leader briefly stepped down.
    let mut last_tx_id = 0u64;
    for ur in 1..=20u64 {
        let r = ctl
            .deposit_and_wait_result(ACCOUNT_A, AMOUNT, ur, true)
            .await
            .expect("deposit");
        last_tx_id = r.tx_id;
    }

    let leader_idx = ctl.leader_index().await.unwrap();

    // Wait for every follower to commit up to the leader's last tx_id
    // (NewTerm consumes a tx_id, so this is > 20).
    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        ctl.require_pipeline_commit_at_least(i, last_tx_id).await;
    }

    let leader_log = ctl
        .raw_client_for_slot(leader_idx)
        .expect("leader client")
        .get_log(1, u64::MAX, 0)
        .await
        .expect("leader get_log");

    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        let follower_log = ctl
            .raw_client_for_slot(i)
            .expect("follower client")
            .get_log(1, u64::MAX, 0)
            .await
            .expect("follower get_log");
        assert_eq!(
            follower_log.records.len(),
            leader_log.records.len(),
            "follower {} returned {} records; leader returned {}",
            i,
            follower_log.records.len(),
            leader_log.records.len()
        );
        assert_eq!(
            follower_log.records, leader_log.records,
            "WAL records diverge between leader and follower {}",
            i
        );
    }
}
