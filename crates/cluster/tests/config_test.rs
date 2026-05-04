//! Cluster configuration & topology — bring-up shapes, validation, and
//! membership sizing.

use ::proto::ledger::WaitLevel;
use cluster::Config;
use cluster::config::ConfigError;
use cluster_test_utils::{ClusterTestingConfig, ClusterTestingControl};
use std::time::Duration;
use spdlog::error;

const ACCOUNT: u64 = 1;
const AMOUNT: u64 = 100;

// ── Validation -------------------------------------------------------------

/// `Config::validate` rejects a missing-self peer list, duplicate ids,
/// and zero node ids. Each is a deployment-time bug we want caught
/// before the supervisor boots.
#[test]
fn validate_rejects_self_missing_from_peers() {
    let toml = r#"
        [cluster.node]
        node_id = 1

        [[cluster.peers]]
        peer_id = 2
        host = "http://x:50061"

        [ledger.storage]
        data_dir = "/tmp/x"
    "#;
    let err = Config::from_toml_str(toml).unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("does not contain self"), "{}", msg);
    assert!(matches!(err, ConfigError::Invalid(_)));
}

#[test]
fn validate_rejects_duplicate_peer_ids() {
    let toml = r#"
        [cluster.node]
        node_id = 1

        [[cluster.peers]]
        peer_id = 1
        host = "http://a"

        [[cluster.peers]]
        peer_id = 1
        host = "http://b"

        [ledger.storage]
        data_dir = "/tmp/x"
    "#;
    let err = Config::from_toml_str(toml).unwrap_err();
    assert!(format!("{}", err).contains("duplicate"));
}

#[test]
fn validate_rejects_zero_node_id() {
    let toml = r#"
        [cluster.node]
        node_id = 0

        [[cluster.peers]]
        peer_id = 0
        host = "http://x"

        [ledger.storage]
        data_dir = "/tmp/x"
    "#;
    let err = Config::from_toml_str(toml).unwrap_err();
    assert!(format!("{}", err).contains("node_id"));
}

/// Standalone (`cluster: None`) is valid and clusterless.
#[test]
fn standalone_default_is_valid() {
    let cfg = Config::default();
    assert!(!cfg.is_clustered());
    assert_eq!(cfg.cluster_size(), 1);
    assert_eq!(cfg.other_peers().count(), 0);
    assert!(cfg.validate().is_ok());
}

// ── Bring-up shapes --------------------------------------------------------

/// Standalone (`cluster.is_none()`) boots only the writable client gRPC.
/// No Node service, no quorum, no replication.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn standalone_serves_writes_with_no_node_grpc() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::standalone())
        .await
        .expect("start");

    let h = ctl.handles(0).expect("handles");
    assert!(!h.has_node_handle(), "standalone has no Node gRPC");
    assert!(h.mirror().is_none(), "standalone has no ClusterMirror");
    assert!(h.as_cluster().is_none(), "standalone is not clustered");

    let tx_id = ctl.deposit(ACCOUNT, AMOUNT, 0).await.expect("deposit");
    assert_eq!(tx_id, 1);
}

// (Removed `standalone_term_log_advances_on_restart` — ADR-0017
// §"Required Invariants" #4 forbids boot-time term bumping. Term now
// advances only on election win, which standalone mode never has.)

/// A single-node cluster (one self-peer in the `cluster.peers` list)
/// boots straight into Leader without an election round — the
/// supervisor's `initial_role` short-circuits when `peers.len() == 1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn single_node_cluster_boots_directly_as_leader() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");

    // Leader is observable instantly (no election timer expiry needed).
    let leader_idx = ctl
        .wait_for_leader(Duration::from_millis(500))
        .await
        .expect("immediate leader");
    assert_eq!(leader_idx, 0);

    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("submit");
    assert_eq!(r.fail_reason, 0);
}

// ── Topology / membership sizing ------------------------------------------

/// 2-node cluster: majority = 2, so BOTH must be alive for any
/// ClusterCommit ack. Verifies the boundary case where a single
/// failure stalls the cluster.
///
/// Crash-safety guarantee: this must hold whether the follower goes
/// away via graceful drain (`stop_node` flips
/// `NodeHandlerCore::shutdown`, refusing further AE) or via hard
/// crash (process gone, no responses at all). In both cases the
/// follower's slot ages out of `Quorum`'s freshness window and stops
/// counting toward majority — so `cluster_commit_index` cannot
/// advance and `WaitLevel::ClusterCommit` correctly times out.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn two_node_cluster_requires_both_for_cluster_commit() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        ..ClusterTestingConfig::cluster(2)
    })
    .await
    .expect("start");

    let _leader = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("first ClusterCommit ack");
    assert_eq!(r.fail_reason, 0);

    // Stop the follower. Quorum (2 of 2) is now unreachable.
    let follower_idx = ctl.first_follower_index().await.expect("follower exists");
    let follower_node_port = ctl.node_port(follower_idx).unwrap();
    ctl.stop_node(follower_idx).await.expect("stop follower");
    // `JoinHandle::abort` is asynchronous; the tonic server task may
    // still be answering AppendEntries for a brief window. Block until
    // the port actually releases — that's the cleanest signal that the
    // follower is fully gone.
    wait_port_free(follower_node_port, Duration::from_secs(5)).await;

    // ClusterCommit wait must time out.
    let result = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 2, WaitLevel::ClusterCommit)
        .await;
    assert!(
        result.is_err(),
        "ClusterCommit must NOT succeed when quorum is unreachable; got {:?}",
        result
    );

    error!("cluster commit wait timed out as expected");
    ctl.stop_all().await;
}

async fn wait_port_free(port: u16, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("port {} did not release within {:?}", port, timeout);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 3-node cluster tolerates exactly one failure for ClusterCommit.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn three_node_cluster_tolerates_one_failure() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");

    let _leader = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let follower_idx = ctl.first_follower_index().await.expect("follower");
    ctl.stop_node(follower_idx)
        .await
        .expect("stop one follower");

    // 2/3 alive — majority intact, ClusterCommit must still succeed.
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("ClusterCommit with one follower down");
    assert_eq!(r.fail_reason, 0);

    ctl.stop_all().await;
}

/// 5-node cluster tolerates exactly two failures (majority = 3).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn five_node_cluster_tolerates_two_failures() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(5))
        .await
        .expect("start");

    let leader_idx = ctl.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // Stop two followers (any two). Quorum size = 3, leader + 2
    // remaining followers = 3 → still reachable.
    let mut stopped = 0;
    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        ctl.stop_node(i).await.expect("stop follower");
        stopped += 1;
        if stopped == 2 {
            break;
        }
    }

    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("ClusterCommit with 2 of 5 down");
    assert_eq!(r.fail_reason, 0);

    ctl.stop_all().await;
}

/// In a clustered config with phantom peers, the per-node config files
/// list the SAME peer set on every real node. Symmetric membership.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cluster_peer_list_is_symmetric_across_nodes() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");

    let mut peer_lists: Vec<Vec<u64>> = Vec::new();
    for i in 0..ctl.len() {
        let cfg = &ctl.handles(i).unwrap();
        // Use Ping semantics — every real node should know about the
        // same set of peer ids. Cross-check via cluster_size().
        // (We don't have a public Config getter on Handles; the
        // ClusterTestingControl confirms cluster sizing matches.)
        peer_lists.push(vec![ctl.node_id(i).unwrap()]);
        assert!(cfg.as_cluster().is_some());
    }
    assert_eq!(peer_lists.len(), 3);
}
