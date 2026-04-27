//! Standalone (non-clustered) `ClusterNode::run` end-to-end test.
//!
//! Cluster Stage 3a addendum: a `Config` whose `[cluster]` block is
//! absent boots **only** the writable client-facing Ledger gRPC
//! server. No Node gRPC, no replication, no peers — and the term log
//! still advances on every restart in lock-step with the cluster
//! path (ADR-0016 §11).

#![cfg(feature = "cluster")]

use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl, Term};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_serves_writes_with_no_node_grpc() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "standalone".to_string(),
        ..ClusterTestingConfig::standalone()
    })
    .await
    .expect("standalone bring-up");

    // No Node gRPC handle exposed in standalone.
    let handles = ctl.handles(0).expect("handles");
    assert!(handles.node_handle().is_none());
    assert!(handles.quorum().is_none());
    assert!(handles.as_cluster().is_none());

    // Standalone is writable: a client can submit and observe a tx_id.
    let client = ctl.client().node(0).clone();
    let tx_id = client.deposit(1, 100, 0).await.expect("deposit");
    assert_eq!(tx_id, 1, "first standalone submit should be tx 1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_term_log_advances_on_restart() {
    // Both standalone and clustered modes bump term on every Ledger
    // start. Verify that across restarts, term.log carries forward
    // and the next term opens above the previous one.
    //
    // Pin the harness's root data dir to a fixed path so the second
    // harness instance reopens the same on-disk state. Caller-owned
    // dir → harness does not auto-clean on drop.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut root = std::env::current_dir().unwrap();
    root.push(format!("temp_standalone_term_advance_{}", nanos));

    let cfg = || ClusterTestingConfig {
        label: "term_advance".to_string(),
        snapshot_frequency: 2,
        data_dir_root: Some(root.clone()),
        ..ClusterTestingConfig::standalone()
    };

    // Slot 0's data dir is `<root>/0/`.
    let data_dir = root.join("0");

    {
        let _ctl = ClusterTestingControl::start(cfg()).await.unwrap();
        let term = Term::open_in_dir(&data_dir.to_string_lossy()).unwrap();
        assert_eq!(term.get_current_term(), 1, "first start opens term 1");
        // _ctl drops here, aborting the server but leaving the dir intact.
    }

    // Allow servers to fully release ports + files.
    tokio::time::sleep(Duration::from_millis(100)).await;

    {
        let _ctl = ClusterTestingControl::start(cfg()).await.unwrap();
        let term = Term::open_in_dir(&data_dir.to_string_lossy()).unwrap();
        assert_eq!(term.get_current_term(), 2, "second start bumps to term 2");
    }

    let _ = std::fs::remove_dir_all(&root);
}
