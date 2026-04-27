//! Integration tests for [`ClusterClient`] — the cluster-aware
//! gRPC facade that auto-routes writes to the current leader and
//! round-robins reads across all nodes.
//!
//! What we cover:
//! - Connecting to a 3-node cluster via URLs.
//! - Writes (deposit / transfer) auto-route to the leader, no
//!   manual leader-resolution from the test.
//! - Reads (get_balance, get_pipeline_index) succeed across the
//!   round-robin even when one node is mid-restart.
//! - Killing the leader and continuing to write works — the
//!   `ClusterLeaderClient` rotates its cached leader index until
//!   it lands on whoever won the new election.
//! - The standalone [`NodeClient`] still works (regression guard
//!   for the rename + retry refactor).

#![cfg(feature = "cluster")]

use roda_ledger::client::{ClusterClient, NodeClient, RetryConfig};
use roda_ledger::cluster::proto::ledger::WaitLevel;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl};
use roda_ledger::entities::SYSTEM_ACCOUNT_ID;
use std::time::Duration;

const ACCOUNT_A: u64 = 100;
const ACCOUNT_B: u64 = 200;
const AMOUNT: u64 = 50;

/// Build a list of `http://127.0.0.1:<client_port>` URLs for every
/// running node in the harness.
fn cluster_urls(ctl: &ClusterTestingControl) -> Vec<String> {
    (0..ctl.len())
        .map(|i| {
            let port = ctl
                .client_port(i)
                .expect("running node must have a client port");
            format!("http://127.0.0.1:{}", port)
        })
        .collect()
}

/// Happy path: a 3-node cluster, ClusterClient auto-routes a
/// `deposit_and_wait(ClusterCommit)` to the leader. After waiting for
/// every node's snapshot to catch up, balance reads via round-robin
/// all return the same value — the cluster client is not biased
/// toward any one node for read traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_client_routes_writes_to_leader_and_reads_round_robin() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    let cluster = ClusterClient::connect_with_retry(&cluster_urls(&ctl), RetryConfig::default())
        .await
        .expect("connect");
    assert_eq!(cluster.node_count(), 3);

    // Write — ClusterClient finds the leader on its own.
    let r = cluster
        .deposit_and_wait(ACCOUNT_A, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("deposit");
    assert_eq!(r.fail_reason, 0);
    let target_tx = r.tx_id;

    // ClusterCommit guarantees majority WAL durability + leader-local
    // snapshot. Followers replicate asynchronously and apply to their
    // snapshot a moment later; for a round-robin read to be observed
    // consistent on every node, we wait for each follower's
    // snapshot_index to reach our tx_id first.
    for i in 0..ctl.len() {
        let ledger = ctl.ledger(i).expect("ledger");
        ctl.wait_for_snapshot_id(&ledger, target_tx, Duration::from_secs(15))
            .await
            .expect("follower snapshot");
    }

    // Round-robin reads — every node now has the deposit applied, so
    // every iteration returns the same value regardless of which
    // node the rotation lands on.
    for _ in 0..6 {
        let bal = cluster.get_balance(ACCOUNT_A).await.expect("read balance");
        assert_eq!(bal.balance, AMOUNT as i64);
    }
    let sys = cluster
        .get_balance(SYSTEM_ACCOUNT_ID)
        .await
        .expect("system balance");
    assert_eq!(bal_sum(AMOUNT as i64, sys.balance), 0);

    ctl.stop_all().await;
}

/// `cluster.leader()` exposes the leader-only client; `cluster.node(i)`
/// exposes a single node directly. Both should be reachable through the
/// facade without needing a separate connect call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_client_exposes_leader_and_per_node_clients() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(2))
        .await
        .expect("start");
    let _ = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    let cluster = ClusterClient::connect_with_retry(&cluster_urls(&ctl), RetryConfig::default())
        .await
        .expect("connect");

    // Leader client (writes only succeed via leader).
    let r = cluster
        .leader()
        .deposit_and_wait(ACCOUNT_A, AMOUNT, 7, WaitLevel::ClusterCommit)
        .await
        .expect("leader deposit");
    assert_eq!(r.fail_reason, 0);

    // Per-node client — read directly off node 0 even if node 0 is
    // a follower. (Read RPCs are accepted on every role.)
    let pi = cluster
        .node(0)
        .get_pipeline_index()
        .await
        .expect("node[0] pipeline_index");
    assert!(pi.commit >= 1, "node[0] commit should reflect deposit");

    ctl.stop_all().await;
}

/// Kill the leader mid-flight; subsequent writes against the same
/// `ClusterClient` succeed because `ClusterLeaderClient` rotates its
/// cached leader index on `FailedPrecondition` until it lands on
/// whichever follower won the new election.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_client_writes_survive_leader_failover() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader1_idx = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    let cluster = ClusterClient::connect_with_retry(&cluster_urls(&ctl), RetryConfig::default())
        .await
        .expect("connect");

    // Pre-failover write.
    let r = cluster
        .deposit_and_wait(ACCOUNT_A, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("pre-failover deposit");
    assert_eq!(r.fail_reason, 0);

    // Kill the leader. `stop_node` waits for the OS to release the port
    // before returning, so the next `deposit_and_wait` against the
    // killed node will deterministically fail with `Unavailable` (not
    // hit a stale-leader race) and let the cluster client rotate.
    ctl.stop_node(leader1_idx).await.expect("stop leader");

    // Wait for the survivors to elect a new leader so the `Ping`-based
    // probe in the harness sees a stable Leader before we keep going.
    let _ = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("new leader");

    // Post-failover writes — the cluster client's leader rotation +
    // built-in retry should land on the new leader without any
    // intervention from the test.
    let r = cluster
        .deposit_and_wait(ACCOUNT_A, AMOUNT, 2, WaitLevel::ClusterCommit)
        .await
        .expect("post-failover deposit");
    assert_eq!(r.fail_reason, 0);

    let r = cluster
        .deposit_and_wait(ACCOUNT_A, AMOUNT, 3, WaitLevel::ClusterCommit)
        .await
        .expect("second post-failover deposit");
    assert_eq!(r.fail_reason, 0);

    let bal = cluster.get_balance(ACCOUNT_A).await.expect("balance");
    assert_eq!(bal.balance, (AMOUNT as i64) * 3);

    ctl.stop_all().await;
}

/// `transfer_and_wait` exercises the same leader-routed retry path
/// across a kill+restart cycle of the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_client_transfer_after_leader_restart() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader1_idx = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    let cluster = ClusterClient::connect_with_retry(&cluster_urls(&ctl), RetryConfig::default())
        .await
        .expect("connect");

    // Fund A.
    cluster
        .deposit_and_wait(ACCOUNT_A, 1000, 1, WaitLevel::ClusterCommit)
        .await
        .expect("deposit A");

    // Cycle the leader.
    ctl.stop_node(leader1_idx).await.expect("stop");
    ctl.start_node(leader1_idx).await.expect("restart");
    let _ = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("post-restart leader");

    // Transfer through the cluster client — should auto-route.
    let r = cluster
        .transfer_and_wait(ACCOUNT_A, ACCOUNT_B, 400, 2, WaitLevel::ClusterCommit)
        .await
        .expect("transfer");
    assert_eq!(r.fail_reason, 0);

    let a = cluster.get_balance(ACCOUNT_A).await.expect("A").balance;
    let b = cluster.get_balance(ACCOUNT_B).await.expect("B").balance;
    let s = cluster
        .get_balance(SYSTEM_ACCOUNT_ID)
        .await
        .expect("sys")
        .balance;
    assert_eq!(a, 600);
    assert_eq!(b, 400);
    assert_eq!(a + b + s, 0, "zero-sum invariant");

    ctl.stop_all().await;
}

/// Regression: the standalone [`NodeClient`] (no cluster wrapper)
/// still works against a single node — it isn't exclusively a
/// cluster-internal building block.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_client_works_standalone() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::standalone())
        .await
        .expect("start standalone");
    let port = ctl.client_port(0).expect("port");

    let client = NodeClient::connect_url(&format!("http://127.0.0.1:{}", port))
        .await
        .expect("connect node");

    let r = client
        .deposit_and_wait(ACCOUNT_A, AMOUNT, 1, WaitLevel::Snapshot)
        .await
        .expect("deposit");
    assert_eq!(r.fail_reason, 0);

    let bal = client.get_balance(ACCOUNT_A).await.expect("balance");
    assert_eq!(bal.balance, AMOUNT as i64);

    ctl.stop_all().await;
}

// ── helpers ────────────────────────────────────────────────────────────

fn bal_sum(a: i64, sys: i64) -> i64 {
    a + sys
}
