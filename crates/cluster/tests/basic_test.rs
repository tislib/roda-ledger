//! End-to-end basic cluster mode test:
//! - Spin up one leader + one follower in the same tokio runtime.
//! - Leader accepts client writes on its Ledger gRPC port.
//! - Follower rejects writes (read_only handler) but serves reads.
//! - Leader's replication thread ships WAL bytes via AppendEntries.
//! - Follower's balances catch up to the leader's.

use ::proto::ledger::WaitLevel;
use cluster_test_utils::{ClusterTestingConfig, ClusterTestingControl};
use std::time::Duration;
use storage::entities::SYSTEM_ACCOUNT_ID;

// Stage 4: with the Candidate loop landed, multi-node clusters
// elect a leader from cold boot — no static role is assigned. The
// follower no longer rejects writes deterministically (whichever
// node loses the election becomes the follower), so this test
// drives writes against the elected leader, identified via the
// `Ping` RPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cluster_leader_replicates_to_follower() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "basic".to_string(),
        replication_poll_ms: 2,
        append_entries_max_bytes: 256 * 1024,
        transaction_count_per_segment: 20_000,
        snapshot_frequency: 2,
        ..ClusterTestingConfig::cluster(2)
    })
    .await
    .expect("start");

    // ── Wait for an election to settle on a unique Leader ───────────────
    let leader_idx = ctl
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("leader");
    eprintln!(
        "election settled: node_id={} is leader",
        ctl.node_id(leader_idx).unwrap()
    );

    // ── Follower rejects writes (negative-path uses raw escape hatch) ──
    let follower_idx = ctl.first_follower_index().await.expect("follower index");
    let write_err = ctl
        .raw_client_for_slot(follower_idx)
        .expect("raw follower client")
        .deposit(1, 10, 0)
        .await
        .expect_err("follower must reject writes");
    assert_eq!(
        write_err.code(),
        tonic::Code::FailedPrecondition,
        "follower should return FAILED_PRECONDITION on submit"
    );

    // ── Drive writes on the leader (bumps last_tx_id internally) ────────
    let account = 7u64;
    let amount = 100u64;
    let total_tx = 200u64;
    let deposits: Vec<(u64, u64, u64)> = (0..total_tx).map(|i| (account, amount, i + 1)).collect();
    let results = ctl
        .deposit_batch_and_wait(&deposits, WaitLevel::ClusterCommit)
        .await
        .expect("leader batch deposit");
    assert_eq!(results.len(), total_tx as usize);
    for r in &results {
        assert!(
            r.fail_reason == 7 || r.fail_reason == 0,
            "leader deposit must succeed"
        );
    }

    // ── Verification: leader and follower converge on the same balance ──
    // `require_balance{,_on}` blocks each node's snapshot watermark
    // until it has caught up to last_tx_id, then asserts.
    let expected_abs = (total_tx * amount) as i64;
    ctl.require_balance(account, expected_abs).await;
    ctl.require_balance_on(follower_idx, account, expected_abs)
        .await;
    ctl.require_balance(SYSTEM_ACCOUNT_ID, -expected_abs).await;
    ctl.require_balance_on(follower_idx, SYSTEM_ACCOUNT_ID, -expected_abs)
        .await;
    ctl.require_zero_sum(&[account, SYSTEM_ACCOUNT_ID]).await;

    // ── Shutdown ────────────────────────────────────────────────────────
    ctl.stop_all().await;
}
