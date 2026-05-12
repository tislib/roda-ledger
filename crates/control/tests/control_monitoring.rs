//! Integration tests for read-only Control RPCs the UI uses on the
//! Dashboard and Logs pages. Each test bootstraps and owns its own
//! cluster — a static `OnceCell` would leak children at process exit
//! because Rust does not call `Drop` on statics.

use std::time::Duration;

use proto::control::control_server::Control;
use proto::control::{
    Capability, ClusterHealth, GetClusterSnapshotRequest, GetNodeWalLogRequest,
    GetRecentElectionsRequest, GetServerInfoRequest, NodeRole, WatchClusterSnapshotRequest,
};
use tokio_stream::StreamExt;
use tonic::{Code, Request};

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_server_info_reports_version_and_capabilities() {
    let (svc, _events, _h) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .get_server_info(Request::new(GetServerInfoRequest {}))
        .await
        .expect("get_server_info")
        .into_inner();

    assert_eq!(resp.api_version, 1);
    assert!(
        resp.version.starts_with("control-real-"),
        "{}",
        resp.version
    );
    assert!(
        resp.capabilities.contains(&(Capability::Kill as i32)),
        "expected KILL capability, got {:?}",
        resp.capabilities
    );
    assert!(
        !resp
            .capabilities
            .contains(&(Capability::NetworkPartition as i32)),
        "did not expect NETWORK_PARTITION; got {:?}",
        resp.capabilities
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_cluster_snapshot_returns_three_nodes_with_one_leader() {
    let (svc, _events, _h) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let snap = svc
        .get_cluster_snapshot(Request::new(GetClusterSnapshotRequest {}))
        .await
        .expect("get_cluster_snapshot")
        .into_inner();

    assert_eq!(snap.nodes.len(), 3);
    let leaders: Vec<_> = snap
        .nodes
        .iter()
        .filter(|n| n.role == NodeRole::Leader as i32)
        .collect();
    assert_eq!(leaders.len(), 1, "expected exactly one leader");
    assert_ne!(snap.leader_node_id, 0);
    assert_eq!(snap.cluster_health, ClusterHealth::Healthy as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_cluster_snapshot_streams_frames() {
    let (svc, _events, _h) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let stream = svc
        .watch_cluster_snapshot(Request::new(WatchClusterSnapshotRequest {
            interval_ms: 100,
        }))
        .await
        .expect("watch_cluster_snapshot")
        .into_inner();
    tokio::pin!(stream);

    let mut count = 0usize;
    let collected = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = stream.next().await {
            let snap = frame.expect("stream frame");
            assert_eq!(snap.nodes.len(), 3);
            count += 1;
            if count >= 2 {
                break;
            }
        }
        count
    })
    .await
    .expect("stream did not produce frames in time");

    assert!(collected >= 2, "expected >=2 frames, got {}", collected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_cluster_snapshot_clamps_interval() {
    let (svc, _events, _h) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    // interval_ms=10 is below the 50 ms floor; the server clamps it.
    let stream = svc
        .watch_cluster_snapshot(Request::new(WatchClusterSnapshotRequest {
            interval_ms: 10,
        }))
        .await
        .expect("watch_cluster_snapshot")
        .into_inner();
    tokio::pin!(stream);

    let mut count = 0usize;
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = stream.next().await {
            frame.expect("stream frame");
            count += 1;
            if count >= 4 {
                break;
            }
        }
    })
    .await;
    assert!(count >= 4, "expected >=4 frames in 2s, got {}", count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_recent_elections_returns_term() {
    let (svc, _events, _h) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .get_recent_elections(Request::new(GetRecentElectionsRequest { limit: 0 }))
        .await
        .expect("get_recent_elections")
        .into_inner();

    assert!(
        !resp.events.is_empty(),
        "expected at least one election event"
    );
    assert!(resp.events[0].term >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_node_wal_log_for_valid_node() {
    let (svc, _events, _h) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .get_node_wal_log(Request::new(GetNodeWalLogRequest {
            node_id: 1,
            from_tx_id: 0,
            to_tx_id: 0,
            limit: 10,
        }))
        .await
        .expect("get_node_wal_log");
    let _ = resp.into_inner();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_node_wal_log_unknown_node_returns_not_found() {
    let (svc, _events, _h) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let err = svc
        .get_node_wal_log(Request::new(GetNodeWalLogRequest {
            node_id: 999,
            from_tx_id: 0,
            to_tx_id: 0,
            limit: 10,
        }))
        .await
        .expect_err("expected Status::not_found for unknown node_id");
    assert_eq!(err.code(), Code::NotFound, "{:?}", err);
}
