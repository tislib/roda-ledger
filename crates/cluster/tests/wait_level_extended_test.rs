//! Wait levels and read semantics on cluster.

use ::proto::ledger::WaitLevel;
use cluster::testing::{ClusterTestingConfig, ClusterTestingControl};
use std::time::Duration;

const ACCOUNT: u64 = 1;
const AMOUNT: u64 = 100;

// ── Wait levels via cluster client API -----------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn wait_level_computed_on_cluster() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::Computed)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);
    assert!(r.tx_id > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn wait_level_committed_on_cluster() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::Committed)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn wait_level_snapshot_on_cluster() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::Snapshot)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);
    ctl.require_balance(ACCOUNT, AMOUNT as i64).await;
}

/// `WaitLevel::ClusterCommit` requires snapshot ≥ tx, commit ≥ tx,
/// AND `cluster_commit_index ≥ tx`. End-to-end ack on a 3-node
/// cluster proves the wait succeeds when all three advance.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn wait_level_cluster_commit_requires_full_advance() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);
}

/// `WaitLevel::ClusterCommit` blocks (and eventually times out) when no
/// quorum is reachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn wait_level_cluster_commit_blocks_without_quorum() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Ack one tx.
    ctl.deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();

    // Stop both followers.
    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        ctl.stop_node(i).await.expect("stop");
    }

    let result = ctl
        .deposit_and_wait_no_retry(ACCOUNT, AMOUNT, 2, WaitLevel::ClusterCommit)
        .await;
    assert!(result.is_err());
}

// ── Submit semantics on non-leader roles ---------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn submit_on_follower_returns_failed_precondition() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let follower_idx = ctl.first_follower_index().await.expect("follower idx");
    let err = ctl
        .raw_client_for_slot(follower_idx)
        .expect("raw follower client")
        .deposit(ACCOUNT, AMOUNT, 0)
        .await
        .expect_err("must reject");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}
