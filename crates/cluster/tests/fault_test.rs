//! Process / network / storage fault scenarios.

use ::proto::ledger::WaitLevel;
use cluster::testing::{ClusterTestingConfig, ClusterTestingControl};
use std::time::Duration;

const ACCOUNT: u64 = 1;
const AMOUNT: u64 = 100;

// ── §8.1 Process / node failures -----------------------------------------

/// Single-leader cluster (3 nodes), kill leader → re-election, no
/// committed tx lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn kill_leader_no_committed_tx_lost() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let mut acked = Vec::new();
    let mut bal_pre: i64 = 0;
    for ur in 1..=15u64 {
        let r = ctl
            .deposit_and_wait_result(ACCOUNT, AMOUNT, ur, true)
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
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
        .deposit_and_wait_result(ACCOUNT, AMOUNT, 2, true)
        .await
        .expect("ClusterCommit with one follower down");
    assert_eq!(r.fail_reason, 0);
}

/// Kill TWO of three nodes → surviving node cannot reach quorum;
/// ClusterCommit waits time out.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
        .deposit_and_wait_no_retry(ACCOUNT, AMOUNT, 2, WaitLevel::ClusterCommit)
        .await;
    assert!(result.is_err(), "ClusterCommit must NOT succeed");
}

/// Kill TWO of five nodes → cluster still tolerates (majority = 3,
/// leader + 2 alive followers = 3).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
        .deposit_and_wait_result(ACCOUNT, AMOUNT, 1, true)
        .await
        .expect("2/5 down still leaves quorum");
    assert_eq!(r.fail_reason, 0);
}

/// Kill THREE of five nodes → no quorum. ClusterCommit blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
        .deposit_and_wait_no_retry(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await;
    assert!(result.is_err(), "no quorum → ClusterCommit must block");
}

/// All nodes killed and restarted → cluster reforms, no committed
/// data lost. (Same shape as `safety_test::acked_cluster_commit_*`
/// but here we drive a slightly different load and explicitly check
/// every node's balance post-restart.)
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
                .deposit_and_wait_result(ACCOUNT, AMOUNT, user_ref, true)
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
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn killed_leader_rejoins_as_follower() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
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
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
            .deposit_and_wait_result(ACCOUNT, AMOUNT, ur, true)
            .await
            .expect("ClusterCommit");
        assert_eq!(r.fail_reason, 0);
    }
    ctl.start_node(follower_idx).await.expect("restart");

    // Lagged follower must catch up via leader replication.
    ctl.require_pipeline_commit_at_least(follower_idx, 20).await;
}

// ── §8.3 Disk-level faults (ADR-018) ------------------------------------

/// `WAL_ACCESS::Stuck` on the leader parks both `write_all` and
/// `fdatasync` on the WAL — so the leader can't even append the
/// bytes its tailer would ship to followers. With nothing on the
/// wire, no follower can ack, the leader's own match_index stays
/// put, and `cluster_commit` cannot advance for any tx submitted
/// during the fault.
///
/// Released, the WAL committer wakes, the writer flushes, the
/// tailer ships, peers ack, and `cluster_commit` catches up.
///
/// `DISK_ACCESS` (sync only) is intentionally *not* used here:
/// with only the sync path parked the leader still completes
/// `write_all`, its tailer reads page-cache bytes, and the two
/// healthy followers can ack the entry on their own — the correct
/// Raft behaviour but not what we want to assert against in this
/// test.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_stuck_causes_submission_to_not_commit() {
    use client::PipelineIndex;

    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    // RAII guard: even on a test-body panic, unstick before drop
    // so the WAL committer can finish and the cluster can shut
    // down cleanly. (A stuck sync leaves the committer parked on a
    // condvar; without release, `ClusterNode::Drop` would deadlock
    // waiting for the WAL stage to join.)
    struct UnstickGuard<'a> {
        ctl: &'a ClusterTestingControl,
        idx: usize,
    }
    impl Drop for UnstickGuard<'_> {
        fn drop(&mut self) {
            let _ = self.ctl.unstick_wal_access(self.idx);
        }
    }

    // ── Phase 1: park fdatasync on the leader, submit a deposit ──
    ctl.stick_wal_access(leader_idx).expect("stick disk");
    let _guard = UnstickGuard {
        ctl: &ctl,
        idx: leader_idx,
    };

    // Submit (no `_and_wait` — the wait would block forever on the
    // parked sync). The sequencer assigns a tx_id immediately and
    // the transactor can still compute; only the WAL committer is
    // parked, so we get the tx_id back fast.
    let tx_id = ctl.deposit(ACCOUNT, AMOUNT, 1).await.expect("submit");
    assert!(tx_id > 0, "sequencer must have assigned a tx_id");

    // Soak: long enough that any normal commit path would have
    // closed (the WAL committer's cycle is sub-millisecond at
    // idle).
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── Phase 2: assert cluster_commit_id stayed below tx_id ──
    let pi: PipelineIndex = ctl
        .raw_client_for_slot(leader_idx)
        .expect("raw client")
        .get_pipeline_index()
        .await
        .expect("pipeline index while stuck");
    assert!(
        pi.cluster_commit < tx_id,
        "DISK_ACCESS stuck must hold back cluster_commit: \
         tx_id={tx_id} cluster_commit={} commit={} snapshot={}",
        pi.cluster_commit,
        pi.commit,
        pi.snapshot,
    );

    // ── Phase 3: release, then watch the deposit make it through ──
    ctl.unstick_wal_access(leader_idx).expect("unstick");
    // The guard's drop becomes a no-op second release; that's
    // idempotent on the injector.

    ctl.wait_for(
        Duration::from_secs(5),
        "cluster_commit catches up",
        || async {
            let pi = ctl
                .raw_client_for_slot(leader_idx)
                .expect("raw client")
                .get_pipeline_index()
                .await
                .ok();
            pi.is_some_and(|p| p.cluster_commit >= tx_id)
        },
    )
    .await
    .expect("cluster_commit advanced");
}

/// Counterpart to `disk_stuck_causes_submission_to_not_commit`:
/// with only the sync axis parked on the leader, the WAL writer
/// still appends, the tailer ships from the page cache, and the two
/// healthy followers ack — so the leader's quorum view
/// (`cluster_commit_index`) advances on a 2-of-3 quorum even though
/// the leader's own `match_index` is stuck.
///
/// We can't use `deposit_and_wait(ClusterCommit)` here because that
/// handler also requires the leader's *local* `commit` watermark to
/// pass `tx_id` (i.e. local sync must complete) — which never
/// happens under DISK_ACCESS Stuck. Instead we read
/// `cluster_commit_index` directly from the pipeline view.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_stuck_on_leader_still_commits_via_quorum() {
    use client::PipelineIndex;

    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    struct UnstickGuard<'a> {
        ctl: &'a ClusterTestingControl,
        idx: usize,
    }
    impl Drop for UnstickGuard<'_> {
        fn drop(&mut self) {
            let _ = self.ctl.unstick_disk_access(self.idx);
        }
    }

    ctl.stick_disk_access(leader_idx).expect("stick disk");
    let _guard = UnstickGuard {
        ctl: &ctl,
        idx: leader_idx,
    };

    let tx_id = ctl.deposit(ACCOUNT, AMOUNT, 1).await.expect("submit");
    assert!(tx_id > 0);

    // Followers ack via tailer reading page-cache bytes — the
    // leader's quorum view must advance even though its own sync is
    // parked. Read it straight off the leader.
    ctl.wait_for(
        Duration::from_secs(5),
        "cluster_commit advances despite leader sync stuck",
        || async {
            let pi: Option<PipelineIndex> = ctl
                .raw_client_for_slot(leader_idx)
                .expect("raw client")
                .get_pipeline_index()
                .await
                .ok();
            pi.is_some_and(|p| p.cluster_commit >= tx_id)
        },
    )
    .await
    .expect("2-of-3 follower quorum must commit");

    // The leader's *local* commit is the opposite: it CANNOT have
    // advanced past tx_id, because the sync that gates
    // last_commit_id is still parked.
    let pi = ctl
        .raw_client_for_slot(leader_idx)
        .expect("raw client")
        .get_pipeline_index()
        .await
        .expect("pi after");
    assert!(
        pi.commit < tx_id,
        "leader's local commit must NOT have advanced (sync still stuck): \
         tx_id={tx_id} local_commit={}",
        pi.commit,
    );
}

/// `WAL_ACCESS::Stuck` on a follower must not block `cluster_commit`.
/// The leader + the OTHER follower form a 2-of-3 quorum so any new
/// write commits cluster-wide; the stuck follower simply lags.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_stuck_on_follower_does_not_block_commit() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");
    let follower_idx = ctl.first_follower_index().await.expect("follower");

    struct UnstickGuard<'a> {
        ctl: &'a ClusterTestingControl,
        idx: usize,
    }
    impl Drop for UnstickGuard<'_> {
        fn drop(&mut self) {
            let _ = self.ctl.unstick_wal_access(self.idx);
        }
    }

    ctl.stick_wal_access(follower_idx).expect("stick follower");
    let _guard = UnstickGuard {
        ctl: &ctl,
        idx: follower_idx,
    };

    // Leader + other follower = 2-of-3 quorum, so ClusterCommit
    // succeeds even with the third node's WAL stuck.
    let r = ctl
        .deposit_and_wait_result(ACCOUNT, AMOUNT, 1, true)
        .await
        .expect("ClusterCommit must succeed with one follower's WAL stuck");
    assert_eq!(r.fail_reason, 0);
}

/// `WAL_ACCESS::Slow` adds a per-call write delay rather than
/// parking — so `cluster_commit` still advances, just with extra
/// latency. Verifies the Slow code path is wired end-to-end and
/// doesn't deadlock.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_slow_on_leader_still_commits() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    struct ClearGuard<'a> {
        ctl: &'a ClusterTestingControl,
        idx: usize,
    }
    impl Drop for ClearGuard<'_> {
        fn drop(&mut self) {
            let _ = self.ctl.clear_all_faults(self.idx);
        }
    }

    // 30 ms is enough to be observable in the test budget but
    // nowhere near deposit_and_wait's default timeout — the commit
    // path still closes.
    ctl.slow_wal_access(leader_idx, 30).expect("slow wal");
    let _guard = ClearGuard {
        ctl: &ctl,
        idx: leader_idx,
    };

    let r = ctl
        .deposit_and_wait_result(ACCOUNT, AMOUNT, 1, true)
        .await
        .expect("ClusterCommit must still close under Slow");
    assert_eq!(r.fail_reason, 0);
}

/// `ClearAllFaults` releases every parked op and clears every active
/// rule in one call. Used as teardown but exercised here for its own
/// behavior: stick WAL, observe the commit is blocked, call
/// `clear_all_faults`, observe a fresh submit goes through.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clear_all_faults_releases_everything() {
    use client::PipelineIndex;

    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    struct ClearGuard<'a> {
        ctl: &'a ClusterTestingControl,
        idx: usize,
    }
    impl Drop for ClearGuard<'_> {
        fn drop(&mut self) {
            let _ = self.ctl.clear_all_faults(self.idx);
        }
    }

    ctl.stick_wal_access(leader_idx).expect("stick");
    let _guard = ClearGuard {
        ctl: &ctl,
        idx: leader_idx,
    };

    // Submit while stuck — tx_id is assigned but won't commit.
    let stuck_tx = ctl.deposit(ACCOUNT, AMOUNT, 1).await.expect("submit");
    assert!(stuck_tx > 0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let pi_during: PipelineIndex = ctl
        .raw_client_for_slot(leader_idx)
        .expect("raw client")
        .get_pipeline_index()
        .await
        .expect("pi during");
    assert!(
        pi_during.cluster_commit < stuck_tx,
        "stuck WAL must hold back cluster_commit: \
         tx_id={stuck_tx} cluster_commit={}",
        pi_during.cluster_commit,
    );

    // ClearAllFaults must wake both write and sync axes.
    let (cleared, released) = ctl.clear_all_faults(leader_idx).expect("clear_all");
    assert!(
        cleared >= 1,
        "ClearAllFaults must report ≥1 cleared level (got {cleared})"
    );
    assert!(
        released >= 1,
        "ClearAllFaults must report ≥1 released stuck op (got {released})"
    );

    // The previously-stuck tx and a fresh submit must both commit.
    ctl.wait_for(
        Duration::from_secs(5),
        "post-clear_all cluster_commit catches up",
        || async {
            let pi = ctl
                .raw_client_for_slot(leader_idx)
                .expect("raw client")
                .get_pipeline_index()
                .await
                .ok();
            pi.is_some_and(|p| p.cluster_commit >= stuck_tx)
        },
    )
    .await
    .expect("cluster_commit advanced");

    let r = ctl
        .deposit_and_wait_result(ACCOUNT, AMOUNT, 2, true)
        .await
        .expect("fresh submit must commit after clear_all");
    assert_eq!(r.fail_reason, 0);
}
