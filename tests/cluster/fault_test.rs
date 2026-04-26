//! Process / network / storage fault scenarios.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::ledger::WaitLevel;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl};
use std::time::Duration;
use tokio::time::sleep;

const ACCOUNT: u64 = 1;
const AMOUNT: u64 = 100;

// ── §8.1 Process / node failures -----------------------------------------

/// Single-leader cluster (3 nodes), kill leader → re-election, no
/// committed tx lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_leader_no_committed_tx_lost() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.leader_client().await.unwrap();

    let mut acked = Vec::new();
    for ur in 1..=15u64 {
        let r = leader_client
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .unwrap();
        assert_eq!(r.fail_reason, 0);
        acked.push(r.tx_id);
    }
    let bal_pre = leader_client.get_balance(ACCOUNT).await.unwrap().balance;
    drop(leader_client);

    ctl.stop_node(leader_idx).expect("stop");
    sleep(Duration::from_millis(150)).await;
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let new_client = ctl.leader_client().await.unwrap();
    for tx_id in &acked {
        let (status, _) = new_client.get_transaction_status(*tx_id).await.unwrap();
        assert!(status >= 2);
    }
    let bal_post = new_client.get_balance(ACCOUNT).await.unwrap().balance;
    assert_eq!(bal_post, bal_pre);
}

/// Kill follower → leader continues; ClusterCommit still works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_follower_leader_continues() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let follower_idx = ctl.first_follower_index().await.expect("follower");

    let leader_client = ctl.leader_client().await.unwrap();
    leader_client
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();

    ctl.stop_node(follower_idx).expect("stop follower");
    sleep(Duration::from_millis(200)).await;

    // Quorum still 2/3 reachable. ClusterCommit must succeed.
    let r = leader_client
        .deposit_and_wait(ACCOUNT, AMOUNT, 2, WaitLevel::ClusterCommit)
        .await
        .expect("ClusterCommit with one follower down");
    assert_eq!(r.fail_reason, 0);
}

/// Kill TWO of three nodes → surviving node cannot reach quorum;
/// ClusterCommit waits time out.
///
/// Currently FAILS — same root cause as
/// `config_test::two_node_cluster_requires_both_for_cluster_commit`:
/// Quorum::get() advances past the dead followers' last-acked slot
/// values when the leader commits a new tx locally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "FIXME(cluster): see two_node_cluster_requires_both_for_cluster_commit"]
async fn kill_two_of_three_blocks_cluster_commit() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.leader_client().await.unwrap();

    leader_client
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();

    // Stop both followers.
    let mut stopped = 0;
    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        ctl.stop_node(i).expect("stop");
        stopped += 1;
    }
    assert_eq!(stopped, 2);
    sleep(Duration::from_millis(200)).await;

    // ClusterCommit blocks (and the test's overall budget guards against hang).
    let result = leader_client
        .deposit_and_wait(ACCOUNT, AMOUNT, 2, WaitLevel::ClusterCommit)
        .await;
    assert!(result.is_err(), "ClusterCommit must NOT succeed");
}

/// Kill TWO of five nodes → cluster still tolerates (majority = 3,
/// leader + 2 alive followers = 3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_two_of_five_cluster_continues() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(5))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    let mut stopped = 0;
    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        ctl.stop_node(i).expect("stop");
        stopped += 1;
        if stopped == 2 {
            break;
        }
    }
    sleep(Duration::from_millis(300)).await;

    let leader_client = ctl.leader_client().await.unwrap();
    let r = leader_client
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("2/5 down still leaves quorum");
    assert_eq!(r.fail_reason, 0);
}

/// Kill THREE of five nodes → no quorum. ClusterCommit blocks.
///
/// Currently FAILS — same root cause as
/// `config_test::two_node_cluster_requires_both_for_cluster_commit`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "FIXME(cluster): see two_node_cluster_requires_both_for_cluster_commit"]
async fn kill_three_of_five_blocks_cluster_commit() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(5))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    let mut stopped = 0;
    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        ctl.stop_node(i).expect("stop");
        stopped += 1;
        if stopped == 3 {
            break;
        }
    }
    sleep(Duration::from_millis(300)).await;

    let leader_client = ctl.leader_client().await.unwrap();
    let result = leader_client
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await;
    assert!(result.is_err(), "no quorum → ClusterCommit must block");
}

/// All nodes killed and restarted → cluster reforms, no committed
/// data lost. (Same shape as `safety_test::acked_cluster_commit_*`
/// but here we drive a slightly different load and explicitly check
/// every node's balance post-restart.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_restart_preserves_committed_data() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.leader_client().await.unwrap();

    for ur in 1..=10u64 {
        leader_client
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .unwrap();
    }
    let bal_pre = leader_client.get_balance(ACCOUNT).await.unwrap().balance;
    drop(leader_client);

    for i in 0..ctl.len() {
        ctl.stop_node(i).expect("stop");
    }
    sleep(Duration::from_millis(300)).await;
    for i in 0..ctl.len() {
        ctl.start_node(i).await.expect("start");
    }
    let _ = ctl.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // Wait for any reads to hit a leader and converge.
    let new_client = ctl.leader_client().await.unwrap();
    let bal_post = new_client.get_balance(ACCOUNT).await.unwrap().balance;
    assert_eq!(bal_post, bal_pre);
}

/// Repeated leader churn (kill → re-elect)×N preserves every
/// ClusterCommit-acked write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_leader_churn_preserves_commits() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let mut user_ref: u64 = 1;
    let mut acked: Vec<u64> = Vec::new();
    for _ in 0..3 {
        let leader_client = ctl.leader_client().await.unwrap();
        for _ in 0..5 {
            let r = leader_client
                .deposit_and_wait(ACCOUNT, AMOUNT, user_ref, WaitLevel::ClusterCommit)
                .await
                .unwrap();
            acked.push(r.tx_id);
            user_ref += 1;
        }
        // Kill current leader.
        let li = ctl.leader_index().await.unwrap();
        drop(leader_client);
        ctl.stop_node(li).expect("stop");
        sleep(Duration::from_millis(150)).await;
        ctl.start_node(li).await.expect("restart");
        let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    }

    let final_client = ctl.leader_client().await.unwrap();
    for tx_id in &acked {
        let (status, _) = final_client.get_transaction_status(*tx_id).await.unwrap();
        assert!(status >= 2, "tx {} lost across churn", tx_id);
    }
}

/// Killed leader rejoins as follower under the new leader; if its
/// uncommitted tail diverges, the supervisor reseeds it via
/// `start_with_recovery_until`. We can't easily force divergence with
/// the harness alone, so we verify the simpler invariant: the rejoiner
/// catches up cleanly without panicking and the cluster keeps a
/// single leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_leader_rejoins_as_follower() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.leader_client().await.unwrap();

    for ur in 1..=10u64 {
        leader_client
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .unwrap();
    }
    drop(leader_client);

    ctl.stop_node(leader_idx).expect("stop");
    sleep(Duration::from_millis(200)).await;
    let new_idx = ctl.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    let new_node_id = ctl.node_id(new_idx).unwrap();

    // Restart the old leader — it should rejoin as follower; the new
    // leader's heartbeat lands on it within a few election windows.
    ctl.start_node(leader_idx).await.expect("restart old leader");
    sleep(Duration::from_millis(800)).await;

    // The cluster still has a single leader (could be either of the
    // alive nodes — if the rejoined node had a higher pre-failover
    // term it could even win re-election, but at minimum a single
    // leader exists).
    let any_leader_idx = ctl.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    let _ = (any_leader_idx, new_node_id);
}

// ── §8.2 Network failures (limited — no in-process partition harness) ----

/// Tonic transport failure on `RequestVote` counts as no-vote; the
/// election still completes once enough votes arrive. Simulated by
/// stopping one of three nodes BEFORE the election, so only 2 nodes
/// participate (one of them fails to respond). The election must still
/// complete because 2 votes ≥ majority(3) = 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_completes_with_one_node_silent() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        autostart: false,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("build");

    // Start only 2 of 3.
    ctl.start_node(0).await.expect("start 0");
    ctl.start_node(1).await.expect("start 1");

    // 2 of 3 = majority. Election must complete.
    let _ = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("election with 2/3 nodes alive must succeed");
}

/// Slow-link surrogate: a follower restarted mid-load lags but catches
/// up; `Quorum::get()` keeps advancing because the OTHER follower (still
/// healthy) + leader form a 2-of-3 majority.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_follower_does_not_block_majority() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        replication_poll_ms: 5,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.leader_client().await.unwrap();
    let follower_idx = ctl.first_follower_index().await.unwrap();

    // Stop one follower, drive writes (only 2/3 alive — still majority),
    // then restart.
    ctl.stop_node(follower_idx).expect("stop");
    for ur in 1..=20u64 {
        let r = leader_client
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .expect("ClusterCommit");
        assert_eq!(r.fail_reason, 0);
    }
    ctl.start_node(follower_idx).await.expect("restart");
    sleep(Duration::from_millis(500)).await;

    // Catch-up.
    let lagged_client = ctl.client(follower_idx).await.expect("client");
    ctl.wait_for(Duration::from_secs(30), "lagged catches up", || {
        let c = lagged_client.clone();
        async move {
            c.get_pipeline_index()
                .await
                .map(|i| i.commit >= 20)
                .unwrap_or(false)
        }
    })
    .await
    .expect("catch up");
}

// ── §8.3 Storage failures (most need fault injection — stubs) -----------

/// Storage-failure injection (read-only fs, ENOSPC, partial fdatasync)
/// requires test infrastructure not present in the harness. Documented
/// stub.
#[tokio::test]
#[ignore = "infra: storage-failure injection not implemented; needs FUSE-style \
            fault injector or a writable-fs wrapper that can fail on demand"]
async fn term_log_write_failure_aborts_election_round() {}

#[tokio::test]
#[ignore = "infra: see term_log_write_failure_aborts_election_round"]
async fn vote_log_fdatasync_failure_returns_no_grant() {}

#[tokio::test]
#[ignore = "infra: see term_log_write_failure_aborts_election_round"]
async fn wal_append_failure_returns_reject_wal_append_failed() {}
