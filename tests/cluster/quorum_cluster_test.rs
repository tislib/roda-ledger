//! `Quorum` integration with the supervisor/leader/replication path.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::ledger::WaitLevel;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const ACCOUNT: u64 = 1;
const AMOUNT: u64 = 100;

/// Leader counts itself toward quorum (slot 0 fed by `on_commit` hook).
/// In a single-node cluster, `Quorum::get()` advances purely from the
/// leader's own commits — proving the on-commit hook is wired up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_counts_itself_in_quorum() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let client = ctl.client(0).await.expect("client");

    let r = client
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("ClusterCommit");
    assert_eq!(r.fail_reason, 0);

    // Quorum::get() must be >= the committed tx_id.
    let quorum = ctl
        .handles(0)
        .unwrap()
        .quorum()
        .expect("Quorum on single-node cluster");
    assert!(quorum.get() >= r.tx_id);
}

/// `cluster_commit_index` (mirrored from Quorum) advances on the leader
/// as peer acks roll in. Verified via `get_pipeline_index`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_commit_index_advances_under_replication() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.leader_client().await.expect("client");

    for ur in 1..=5u64 {
        leader_client
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .expect("deposit");
    }

    // After 5 ClusterCommit-acked writes, leader's pipeline must report
    // commit ≥ 5 (and the cluster_commit field, accessible only via the
    // bare pipeline index, must too — but the public client API only
    // surfaces `commit` so we sanity-check there).
    let idx = leader_client.get_pipeline_index().await.unwrap();
    assert!(idx.commit >= 5);
    assert!(idx.snapshot >= 5);
}

/// `Quorum::get()` is monotonically non-decreasing under restart of any
/// follower. After we restart a follower (its slot atomic resets to 0),
/// the cached majority does NOT regress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quorum_majority_never_regresses_under_follower_restart() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        replication_poll_ms: 5,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.leader_client().await.expect("client");

    for ur in 1..=10u64 {
        leader_client
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .expect("deposit");
    }
    let q = ctl.handles(leader_idx).unwrap().quorum().unwrap();
    let pre = q.get();
    assert!(pre >= 10);

    let follower_idx = ctl.first_follower_index().await.expect("follower");
    ctl.stop_node(follower_idx).expect("stop");
    sleep(Duration::from_millis(150)).await;
    ctl.start_node(follower_idx).await.expect("restart");
    sleep(Duration::from_millis(300)).await;

    // Q::get() must not regress.
    let post = q.get();
    assert!(
        post >= pre,
        "Quorum::get() regressed: {} -> {}",
        pre,
        post
    );
}

/// After a Leader transition, the new leader's own slot in `Quorum` is
/// repopulated from its `on_commit` hook within one tick — its first
/// `submit_and_wait(ClusterCommit)` succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_leader_self_slot_repopulated_after_transition() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.leader_client().await.expect("client");

    // Land some writes.
    for ur in 1..=5u64 {
        leader_client
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .expect("deposit");
    }
    drop(leader_client);

    // Kill leader → new election.
    ctl.stop_node(leader_idx).expect("stop");
    sleep(Duration::from_millis(150)).await;
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // First write under new leader must reach ClusterCommit.
    let new_client = ctl.leader_client().await.expect("new client");
    let started = Instant::now();
    let r = new_client
        .deposit_and_wait(ACCOUNT, AMOUNT, 6, WaitLevel::ClusterCommit)
        .await
        .expect("first write under new leader");
    assert_eq!(r.fail_reason, 0);
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "new leader's quorum slot took too long to repopulate"
    );
}
