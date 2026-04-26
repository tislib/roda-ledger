//! End-to-end correctness invariants: balance convergence, byte-exact
//! WAL replication, durable acknowledged writes.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::ledger::WaitLevel;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl};
use roda_ledger::entities::SYSTEM_ACCOUNT_ID;
use std::time::Duration;
use tokio::time::sleep;

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
        let client = ctl.leader_client().await.unwrap();
        for _ in 0..10 {
            client
                .deposit_and_wait(ACCOUNT_A, AMOUNT, user_ref, WaitLevel::ClusterCommit)
                .await
                .unwrap();
            user_ref += 1;
            client
                .deposit_and_wait(ACCOUNT_B, AMOUNT, user_ref, WaitLevel::ClusterCommit)
                .await
                .unwrap();
            user_ref += 1;
        }
        // Kill current leader and let a new one rise (except on last round).
        if round < 2 {
            let li = ctl.leader_index().await.unwrap();
            drop(client);
            ctl.stop_node(li).expect("stop");
            sleep(Duration::from_millis(150)).await;
            ctl.start_node(li).await.expect("restart");
            let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
        }
    }

    // Final convergence: every node sees the same A and B balance.
    let leader_client = ctl.leader_client().await.unwrap();
    let target_a = leader_client.get_balance(ACCOUNT_A).await.unwrap().balance;
    let target_b = leader_client.get_balance(ACCOUNT_B).await.unwrap().balance;
    let target_commit = leader_client
        .get_pipeline_index()
        .await
        .unwrap()
        .commit;

    for i in 0..ctl.len() {
        let c = ctl.client(i).await.expect("client");
        ctl.wait_for(Duration::from_secs(30), "node commit catches up", || {
            let c = c.clone();
            async move {
                c.get_pipeline_index()
                    .await
                    .map(|p| p.commit >= target_commit)
                    .unwrap_or(false)
            }
        })
        .await
        .expect("convergence");
        let a = c.get_balance(ACCOUNT_A).await.unwrap().balance;
        let b = c.get_balance(ACCOUNT_B).await.unwrap().balance;
        assert_eq!(a, target_a, "node {} disagrees on A", ctl.node_id(i).unwrap());
        assert_eq!(b, target_b, "node {} disagrees on B", ctl.node_id(i).unwrap());
        let sys = c.get_balance(SYSTEM_ACCOUNT_ID).await.unwrap().balance;
        assert_eq!(a + b + sys, 0, "zero-sum violated on node {}", i);
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

    let leader_client = ctl.leader_client().await.unwrap();
    // Bootstrap account A.
    leader_client
        .deposit_and_wait(ACCOUNT_A, 10_000, 0, WaitLevel::ClusterCommit)
        .await
        .unwrap();
    // 50 transfers A → B.
    for _ in 0..50 {
        leader_client
            .transfer_and_wait(ACCOUNT_A, ACCOUNT_B, 100, 0, WaitLevel::ClusterCommit)
            .await
            .unwrap();
    }
    let target_commit = leader_client.get_pipeline_index().await.unwrap().commit;

    for i in 0..ctl.len() {
        let c = ctl.client(i).await.expect("client");
        ctl.wait_for(Duration::from_secs(30), "node catches up", || {
            let c = c.clone();
            async move {
                c.get_pipeline_index()
                    .await
                    .map(|p| p.commit >= target_commit)
                    .unwrap_or(false)
            }
        })
        .await
        .unwrap();
        let a = c.get_balance(ACCOUNT_A).await.unwrap().balance;
        let b = c.get_balance(ACCOUNT_B).await.unwrap().balance;
        let s = c.get_balance(SYSTEM_ACCOUNT_ID).await.unwrap().balance;
        assert_eq!(a + b + s, 0, "node {} violates zero-sum", i);
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
    let leader_client = ctl.leader_client().await.unwrap();

    let mut acked = Vec::new();
    for ur in 1..=10u64 {
        let r = leader_client
            .deposit_and_wait(ACCOUNT_A, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .unwrap();
        acked.push(r.tx_id);
    }
    drop(leader_client);

    // Kill any single node and observe survivors.
    ctl.stop_node(0).expect("stop");
    sleep(Duration::from_millis(150)).await;
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let new_client = ctl.leader_client().await.unwrap();
    for tx_id in &acked {
        let (status, _) = new_client.get_transaction_status(*tx_id).await.unwrap();
        assert!(status >= 2, "tx {} lost after single-node failure", tx_id);
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
    let leader_client = ctl.leader_client().await.unwrap();

    for ur in 1..=20u64 {
        leader_client
            .deposit_and_wait(ACCOUNT_A, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .unwrap();
    }

    let leader_idx = ctl.leader_index().await.unwrap();
    let leader_ledger = ctl.ledger(leader_idx).unwrap();

    // Wait for every follower to catch up at the WAL level.
    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        let c = ctl.client(i).await.unwrap();
        ctl.wait_for(Duration::from_secs(30), "follower catches up", || {
            let c = c.clone();
            async move {
                c.get_pipeline_index()
                    .await
                    .map(|i| i.commit >= 20)
                    .unwrap_or(false)
            }
        })
        .await
        .unwrap();
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
