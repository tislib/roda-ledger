//! Standalone (non-clustered) `ClusterNode::run` end-to-end test.
//!
//! Cluster Stage 3a addendum: a `Config` whose `[cluster]` block is
//! absent boots **only** the writable client-facing Ledger gRPC
//! server. No Node gRPC, no replication, no peers — and the term log
//! still advances on every restart in lock-step with the cluster
//! path (ADR-0016 §11).


use cluster_test_utils::{ClusterTestingConfig, ClusterTestingControl};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn standalone_serves_writes_with_no_node_grpc() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "standalone".to_string(),
        ..ClusterTestingConfig::standalone()
    })
    .await
    .expect("standalone bring-up");

    // No Node gRPC handle exposed in standalone.
    let handles = ctl.handles(0).expect("handles");
    assert!(!handles.has_node_handle());
    assert!(handles.mirror().is_none());
    assert!(handles.as_cluster().is_none());

    // Standalone is writable: a client can submit and observe a tx_id.
    let tx_id = ctl.deposit(1, 100, 0).await.expect("deposit");
    assert_eq!(tx_id, 1, "first standalone submit should be tx 1");
}

// (Removed `standalone_term_log_advances_on_restart` — ADR-0017
// §"Required Invariants" #4 forbids boot-time term bumping. Term
// advances only on election win, which standalone mode never has.)
