//! End-to-end basic cluster mode test:
//! - Spin up one leader + one follower in the same tokio runtime.
//! - Leader accepts client writes on its Ledger gRPC port.
//! - Follower rejects writes (read_only handler) but serves reads.
//! - Leader's replication thread ships WAL bytes via AppendEntries.
//! - Follower's balances catch up to the leader's.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::ledger::WaitLevel;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl};
use roda_ledger::entities::SYSTEM_ACCOUNT_ID;
use std::time::Duration;

// Stage 4: with the Candidate loop landed, multi-node clusters
// elect a leader from cold boot — no static role is assigned. The
// follower no longer rejects writes deterministically (whichever
// node loses the election becomes the follower), so this test
// drives writes against the elected leader, identified via the
// `Ping` RPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    let leader_client = ctl.leader_client().await.expect("leader client");
    let follower_client = ctl.follower_client().await.expect("follower client");

    // ── Follower rejects writes ─────────────────────────────────────────
    let write_err = follower_client
        .deposit(1, 10, 0)
        .await
        .expect_err("follower must reject writes");
    assert_eq!(
        write_err.code(),
        tonic::Code::FailedPrecondition,
        "follower should return FAILED_PRECONDITION on submit"
    );

    // ── Drive writes on the leader ──────────────────────────────────────
    // Single batch + wait so we don't pay 200 round-trips just to
    // confirm the leader committed everything.
    let account = 7u64;
    let amount = 100u64;
    let total_tx = 200u64;
    let deposits: Vec<(u64, u64, u64)> =
        (0..total_tx).map(|_| (account, amount, 0u64)).collect();
    let results = leader_client
        .deposit_batch_and_wait(&deposits, WaitLevel::Committed)
        .await
        .expect("leader batch deposit");
    assert_eq!(results.len(), total_tx as usize);
    for r in &results {
        assert_eq!(r.fail_reason, 0, "leader deposit must succeed");
    }

    // ── Wait for follower to catch up ───────────────────────────────────
    ctl.wait_for(Duration::from_secs(60), "follower commit_index", || {
        let fc = follower_client.clone();
        async move {
            fc.get_pipeline_index()
                .await
                .map(|i| i.commit >= total_tx)
                .unwrap_or(false)
        }
    })
    .await
    .expect("follower commit");

    ctl.wait_for(Duration::from_secs(60), "follower snapshot_index", || {
        let fc = follower_client.clone();
        async move {
            fc.get_pipeline_index()
                .await
                .map(|i| i.snapshot >= total_tx)
                .unwrap_or(false)
        }
    })
    .await
    .expect("follower snapshot");

    // ── Verification: balances match ─────────────────────────────────────
    let leader_account_balance = leader_client.get_balance(account).await.unwrap().balance;
    let follower_account_balance = follower_client.get_balance(account).await.unwrap().balance;
    let leader_system_balance = leader_client
        .get_balance(SYSTEM_ACCOUNT_ID)
        .await
        .unwrap()
        .balance;
    let follower_system_balance = follower_client
        .get_balance(SYSTEM_ACCOUNT_ID)
        .await
        .unwrap()
        .balance;

    assert_eq!(
        leader_account_balance, follower_account_balance,
        "user account balance must match between leader and follower"
    );
    assert_eq!(
        leader_system_balance, follower_system_balance,
        "system account balance must match"
    );
    let expected_abs = (total_tx * amount) as i64;
    assert_eq!(leader_account_balance.abs(), expected_abs);
    assert_eq!(leader_account_balance + leader_system_balance, 0);

    // ── Shutdown ────────────────────────────────────────────────────────
    ctl.stop_all();
}
