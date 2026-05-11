//! Integration tests for provisioning RPCs the UI uses on the Settings
//! page (GetClusterConfig, UpdateClusterConfig, SetNodeCount, ResetCluster).
//! Every test bootstraps a fresh cluster because each RPC reprovisions
//! and there is no meaningful state to share.

use std::time::Duration;

use proto::control::control_server::Control;
use proto::control::{
    FaultEvent, FaultKind, GetClusterConfigRequest, GetClusterSnapshotRequest,
    GetFaultHistoryRequest, ResetClusterRequest, SetNodeCountRequest, UpdateClusterConfigRequest,
};
use tonic::Request;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_cluster_config_returns_bootstrapped_config() {
    let (svc, _events, _h) = common::build_control(2).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .get_cluster_config(Request::new(GetClusterConfigRequest {}))
        .await
        .expect("get_cluster_config")
        .into_inner();
    let cfg = resp.config.expect("config present");
    assert_eq!(cfg, common::default_cluster_config());
    let mem = resp.membership.expect("membership present");
    assert_eq!(mem.target_count, 2);
    assert_eq!(mem.nodes.len(), 2);
    for n in &mem.nodes {
        assert!(!n.address.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_cluster_config_accepted_for_valid_config() {
    let (svc, _events, _h) = common::build_control(1).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let mut new_cfg = common::default_cluster_config();
    new_cfg.snapshot_frequency = 4;

    let resp = svc
        .update_cluster_config(Request::new(UpdateClusterConfigRequest {
            config: Some(new_cfg),
        }))
        .await
        .expect("update_cluster_config")
        .into_inner();
    assert!(resp.accepted, "rejected: {}", resp.error);

    common::wait_for_leader(&svc, Duration::from_secs(10)).await;
    let got = svc
        .get_cluster_config(Request::new(GetClusterConfigRequest {}))
        .await
        .expect("get_cluster_config")
        .into_inner()
        .config
        .expect("config present");
    assert_eq!(got.snapshot_frequency, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_cluster_config_rejected_for_zero_max_accounts() {
    let (svc, _events, _h) = common::build_control(1).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let mut bad = common::default_cluster_config();
    bad.max_accounts = 0;
    let resp = svc
        .update_cluster_config(Request::new(UpdateClusterConfigRequest { config: Some(bad) }))
        .await
        .expect("update_cluster_config returns Ok with accepted=false")
        .into_inner();
    assert!(!resp.accepted);
    assert!(
        resp.error.contains("max_accounts"),
        "error should mention max_accounts: {}",
        resp.error
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_cluster_config_rejected_when_config_missing() {
    let (svc, _events, _h) = common::build_control(1).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .update_cluster_config(Request::new(UpdateClusterConfigRequest { config: None }))
        .await
        .expect("returns Ok with accepted=false")
        .into_inner();
    assert!(!resp.accepted);
    assert!(
        resp.error.contains("required"),
        "error should mention 'required': {}",
        resp.error
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_node_count_rejects_zero() {
    let (svc, _events, _h) = common::build_control(1).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .set_node_count(Request::new(SetNodeCountRequest { target_count: 0 }))
        .await
        .expect("returns Ok with accepted=false")
        .into_inner();
    assert!(!resp.accepted);
    assert!(resp.error.contains(">= 1"), "{}", resp.error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_node_count_grows_cluster() {
    let (svc, _events, _h) = common::build_control(1).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .set_node_count(Request::new(SetNodeCountRequest { target_count: 3 }))
        .await
        .expect("set_node_count")
        .into_inner();
    assert!(resp.accepted, "rejected: {}", resp.error);
    assert_eq!(resp.target_count, 3);
    assert_eq!(resp.current_count, 3);

    common::wait_for_leader(&svc, Duration::from_secs(10)).await;
    let snap = svc
        .get_cluster_snapshot(Request::new(GetClusterSnapshotRequest {}))
        .await
        .expect("snapshot")
        .into_inner();
    assert_eq!(snap.nodes.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_node_count_shrinks_cluster() {
    let (svc, _events, _h) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .set_node_count(Request::new(SetNodeCountRequest { target_count: 1 }))
        .await
        .expect("set_node_count")
        .into_inner();
    assert!(resp.accepted, "rejected: {}", resp.error);
    assert_eq!(resp.current_count, 1);

    common::wait_for_leader(&svc, Duration::from_secs(10)).await;
    let snap = svc
        .get_cluster_snapshot(Request::new(GetClusterSnapshotRequest {}))
        .await
        .expect("snapshot")
        .into_inner();
    assert_eq!(snap.nodes.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_cluster_clears_fault_history() {
    let (svc, events, _h) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    events.record_fault(FaultEvent {
        at_ms: 1,
        kind: FaultKind::Stop as i32,
        node_id: 2,
        peer_node_id: 0,
        description: "synthetic".into(),
    });

    let resp = svc
        .reset_cluster(Request::new(ResetClusterRequest {}))
        .await
        .expect("reset_cluster")
        .into_inner();
    assert!(resp.accepted, "rejected: {}", resp.error);

    let history = svc
        .get_fault_history(Request::new(GetFaultHistoryRequest { limit: 0 }))
        .await
        .expect("get_fault_history")
        .into_inner();
    assert!(history.events.is_empty(), "fault history not cleared");

    common::wait_for_leader(&svc, Duration::from_secs(10)).await;
    let snap = svc
        .get_cluster_snapshot(Request::new(GetClusterSnapshotRequest {}))
        .await
        .expect("snapshot")
        .into_inner();
    assert_eq!(snap.nodes.len(), 3);
    assert_eq!(
        snap.cluster_health,
        proto::control::ClusterHealth::Healthy as i32
    );
}
