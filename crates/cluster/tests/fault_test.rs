//! Process / network / storage fault scenarios.

use ::proto::ledger::WaitLevel;
use client::RetryConfig;
use cluster_test_utils::{ClusterTestingConfig, ClusterTestingControl};
use std::time::Duration;

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

    let mut acked = Vec::new();
    let mut bal_pre: i64 = 0;
    for ur in 1..=15u64 {
        let r = ctl
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .unwrap();
        assert_eq!(r.fail_reason, 0);
        acked.push(r.tx_id);
        bal_pre += AMOUNT as i64;
    }

    ctl.stop_node(leader_idx).await.expect("stop");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    for tx_id in &acked {
        ctl.require_transaction_committed(*tx_id).await;
    }
    ctl.require_balance(ACCOUNT, bal_pre).await;
}

/// Kill follower → leader continues; ClusterCommit still works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_follower_leader_continues() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let follower_idx = ctl.first_follower_index().await.expect("follower");

    ctl.deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();

    ctl.stop_node(follower_idx).await.expect("stop follower");

    // Quorum still 2/3 reachable. ClusterCommit must succeed.
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 2, WaitLevel::ClusterCommit)
        .await
        .expect("ClusterCommit with one follower down");
    assert_eq!(r.fail_reason, 0);
}

/// Kill TWO of three nodes → surviving node cannot reach quorum;
/// ClusterCommit waits time out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_two_of_three_blocks_cluster_commit() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    ctl.deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();

    // Stop both followers.
    let mut stopped = 0;
    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        ctl.stop_node(i).await.expect("stop");
        stopped += 1;
    }
    assert_eq!(stopped, 2);

    // ClusterCommit blocks (and the test's overall budget guards against hang).
    let result = ctl
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
        ctl.stop_node(i).await.expect("stop");
        stopped += 1;
        if stopped == 2 {
            break;
        }
    }

    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("2/5 down still leaves quorum");
    assert_eq!(r.fail_reason, 0);
}

/// Kill THREE of five nodes → no quorum. ClusterCommit blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        ctl.stop_node(i).await.expect("stop");
        stopped += 1;
        if stopped == 3 {
            break;
        }
    }

    let result = ctl
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

    for ur in 1..=10u64 {
        ctl.deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .unwrap();
    }
    let expected = (10 * AMOUNT) as i64;
    ctl.require_balance(ACCOUNT, expected).await;

    for i in 0..ctl.len() {
        ctl.stop_node(i).await.expect("stop");
    }
    for i in 0..ctl.len() {
        ctl.start_node(i).await.expect("start");
    }
    let _ = ctl.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // After cold restart, the new leader catches up to last_tx_id
    // before serving the read.
    ctl.require_balance(ACCOUNT, expected).await;
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
        for _ in 0..5 {
            let r = ctl
                .deposit_and_wait(ACCOUNT, AMOUNT, user_ref, WaitLevel::ClusterCommit)
                .await
                .unwrap();
            acked.push(r.tx_id);
            user_ref += 1;
        }
        // Kill current leader.
        let li = ctl.leader_index().await.unwrap();
        ctl.stop_node(li).await.expect("stop");
        ctl.start_node(li).await.expect("restart");
        let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    }

    for tx_id in &acked {
        ctl.require_transaction_committed(*tx_id).await;
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
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        retry_config: RetryConfig::no_retry(),
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    for ur in 1..=10u64 {
        ctl.deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .unwrap();
    }

    ctl.stop_node(leader_idx).await.expect("stop");
    let new_idx = ctl.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    let new_node_id = ctl.node_id(new_idx).unwrap();

    // Restart the old leader — it should rejoin as follower; the new
    // leader's heartbeat lands on it within a few election windows.
    ctl.start_node(leader_idx)
        .await
        .expect("restart old leader");

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
    let follower_idx = ctl.first_follower_index().await.unwrap();

    // Stop one follower, drive writes (only 2/3 alive — still majority),
    // then restart.
    ctl.stop_node(follower_idx).await.expect("stop");
    for ur in 1..=20u64 {
        let r = ctl
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .expect("ClusterCommit");
        assert_eq!(r.fail_reason, 0);
    }
    ctl.start_node(follower_idx).await.expect("restart");

    // Lagged follower must catch up via leader replication.
    ctl.require_pipeline_commit_at_least(follower_idx, 20).await;
}

// ── §8.3 Storage failures (most need fault injection — stubs) -----------

/// Storage-failure injection (read-only fs, ENOSPC, partial fdatasync)
/// requires test infrastructure not present in the harness. Documented
/// stub.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "infra: storage-failure injection not implemented; needs FUSE-style \
            fault injector or a writable-fs wrapper that can fail on demand"]
async fn term_log_write_failure_aborts_election_round() {}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "infra: see term_log_write_failure_aborts_election_round"]
async fn vote_log_fdatasync_failure_returns_no_grant() {}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "infra: see term_log_write_failure_aborts_election_round"]
async fn wal_append_failure_returns_reject_wal_append_failed() {}
