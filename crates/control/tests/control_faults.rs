//! Integration tests for fault-injection RPCs the UI uses on the Faults
//! page. Each test gets a fresh 3-node cluster so quorum survives one
//! fault and tests don't bleed state into each other.

use std::sync::Arc;
use std::time::Duration;

use control::EventStore;
use control::service::ControlService;
use control::ClusterHandle;
use proto::control::control_server::Control;
use proto::control::{
    FaultEvent, FaultKind, GetFaultHistoryRequest, HealPartitionRequest, KillNodeRequest,
    NodeHealth, PartitionPairRequest, RestartNodeRequest, StartNodeRequest, StopNodeRequest,
};
use tonic::{Code, Request};

mod common;

async fn setup() -> (ControlService, Arc<EventStore>, Arc<ClusterHandle>) {
    let (svc, events, handle) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;
    (svc, events, handle)
}

async fn current_snapshot(svc: &ControlService) -> proto::control::GetClusterSnapshotResponse {
    svc.get_cluster_snapshot(Request::new(proto::control::GetClusterSnapshotRequest {}))
        .await
        .expect("snapshot")
        .into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_node_records_fault_event_and_marks_stopped() {
    let (svc, _events, _h) = setup().await;
    let follower = common::pick_follower(&current_snapshot(&svc).await);

    let resp = svc
        .stop_node(Request::new(StopNodeRequest { node_id: follower }))
        .await
        .expect("stop_node")
        .into_inner();
    assert!(resp.accepted, "stop_node rejected: {}", resp.error);

    let history = svc
        .get_fault_history(Request::new(GetFaultHistoryRequest { limit: 0 }))
        .await
        .expect("get_fault_history")
        .into_inner();
    let event = history
        .events
        .iter()
        .find(|e| e.node_id == follower && e.kind == FaultKind::Stop as i32)
        .expect("stop event for follower");
    assert_eq!(event.node_id, follower);

    common::wait_for_snapshot(&svc, Duration::from_secs(2), |s| {
        s.nodes
            .iter()
            .any(|n| n.node_id == follower && n.health == NodeHealth::Stopped as i32)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_node_restores_stopped_follower() {
    let (svc, _events, _h) = setup().await;
    let follower = common::pick_follower(&current_snapshot(&svc).await);

    svc.stop_node(Request::new(StopNodeRequest { node_id: follower }))
        .await
        .expect("stop_node");
    common::wait_for_snapshot(&svc, Duration::from_secs(2), |s| {
        s.nodes
            .iter()
            .any(|n| n.node_id == follower && n.health == NodeHealth::Stopped as i32)
    })
    .await;

    let resp = svc
        .start_node(Request::new(StartNodeRequest { node_id: follower }))
        .await
        .expect("start_node")
        .into_inner();
    assert!(resp.accepted, "start_node rejected: {}", resp.error);

    common::wait_for_snapshot(&svc, Duration::from_secs(5), |s| {
        s.nodes
            .iter()
            .any(|n| n.node_id == follower && n.health == NodeHealth::Up as i32)
    })
    .await;

    let history = svc
        .get_fault_history(Request::new(GetFaultHistoryRequest { limit: 0 }))
        .await
        .expect("get_fault_history")
        .into_inner();
    assert!(
        history
            .events
            .iter()
            .any(|e| e.kind == FaultKind::Stop as i32 && e.node_id == follower)
    );
    assert!(
        history
            .events
            .iter()
            .any(|e| e.kind == FaultKind::Start as i32 && e.node_id == follower)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_node_records_kill_fault() {
    let (svc, _events, _h) = setup().await;
    let follower = common::pick_follower(&current_snapshot(&svc).await);

    let resp = svc
        .kill_node(Request::new(KillNodeRequest { node_id: follower }))
        .await
        .expect("kill_node")
        .into_inner();
    assert!(resp.accepted, "kill_node rejected: {}", resp.error);

    common::wait_for_snapshot(&svc, Duration::from_secs(2), |s| {
        s.nodes
            .iter()
            .any(|n| n.node_id == follower && n.health == NodeHealth::Stopped as i32)
    })
    .await;

    let history = svc
        .get_fault_history(Request::new(GetFaultHistoryRequest { limit: 0 }))
        .await
        .expect("get_fault_history")
        .into_inner();
    assert!(
        history
            .events
            .iter()
            .any(|e| e.kind == FaultKind::Kill as i32 && e.node_id == follower)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_node_brings_node_back_up() {
    let (svc, _events, _h) = setup().await;
    let follower = common::pick_follower(&current_snapshot(&svc).await);

    let resp = svc
        .restart_node(Request::new(RestartNodeRequest { node_id: follower }))
        .await
        .expect("restart_node")
        .into_inner();
    assert!(resp.accepted, "restart_node rejected: {}", resp.error);

    common::wait_for_snapshot(&svc, Duration::from_secs(5), |s| {
        s.nodes
            .iter()
            .any(|n| n.node_id == follower && n.health == NodeHealth::Up as i32)
    })
    .await;

    let history = svc
        .get_fault_history(Request::new(GetFaultHistoryRequest { limit: 0 }))
        .await
        .expect("get_fault_history")
        .into_inner();
    assert!(
        history
            .events
            .iter()
            .any(|e| e.kind == FaultKind::Restart as i32 && e.node_id == follower)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_node_unknown_id_returns_accepted_false() {
    let (svc, _events, _h) = setup().await;
    let resp = svc
        .stop_node(Request::new(StopNodeRequest { node_id: 999 }))
        .await
        .expect("stop_node returns Ok with accepted=false")
        .into_inner();
    assert!(!resp.accepted);
    assert!(!resp.error.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_node_unknown_id_returns_accepted_false() {
    let (svc, _events, _h) = setup().await;
    let resp = svc
        .kill_node(Request::new(KillNodeRequest { node_id: 999 }))
        .await
        .expect("kill_node returns Ok with accepted=false")
        .into_inner();
    assert!(!resp.accepted);
    assert!(!resp.error.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_node_unknown_id_returns_accepted_false() {
    let (svc, _events, _h) = setup().await;
    let resp = svc
        .start_node(Request::new(StartNodeRequest { node_id: 999 }))
        .await
        .expect("start_node returns Ok with accepted=false")
        .into_inner();
    assert!(!resp.accepted);
    assert!(!resp.error.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_node_unknown_id_returns_accepted_false() {
    let (svc, _events, _h) = setup().await;
    let resp = svc
        .restart_node(Request::new(RestartNodeRequest { node_id: 999 }))
        .await
        .expect("restart_node returns Ok with accepted=false")
        .into_inner();
    assert!(!resp.accepted);
    assert!(!resp.error.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_pair_returns_unimplemented() {
    let (svc, _events, _h) = setup().await;
    let err = svc
        .partition_pair(Request::new(PartitionPairRequest {
            node_a: 1,
            node_b: 2,
        }))
        .await
        .expect_err("partition_pair should be unimplemented");
    assert_eq!(err.code(), Code::Unimplemented);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heal_partition_returns_unimplemented() {
    let (svc, _events, _h) = setup().await;
    let err = svc
        .heal_partition(Request::new(HealPartitionRequest {
            node_a: 1,
            node_b: 2,
        }))
        .await
        .expect_err("heal_partition should be unimplemented");
    assert_eq!(err.code(), Code::Unimplemented);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_fault_history_orders_newest_first() {
    let (svc, _events, _h) = setup().await;
    let snap = current_snapshot(&svc).await;
    let followers: Vec<u64> = snap
        .nodes
        .iter()
        .filter(|n| n.role == proto::control::NodeRole::Follower as i32)
        .map(|n| n.node_id)
        .collect();
    assert!(followers.len() >= 2, "expected >=2 followers in a 3-node cluster");

    svc.stop_node(Request::new(StopNodeRequest {
        node_id: followers[0],
    }))
    .await
    .expect("stop first");
    svc.stop_node(Request::new(StopNodeRequest {
        node_id: followers[1],
    }))
    .await
    .expect("stop second");

    let history = svc
        .get_fault_history(Request::new(GetFaultHistoryRequest { limit: 0 }))
        .await
        .expect("get_fault_history")
        .into_inner();
    assert_eq!(history.events.len(), 2);
    assert_eq!(history.events[0].node_id, followers[1], "newest first");
    assert_eq!(history.events[1].node_id, followers[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_fault_history_limit_zero_uses_default_64() {
    let (svc, events, _h) = setup().await;
    for i in 0..100 {
        events.record_fault(FaultEvent {
            at_ms: i,
            kind: FaultKind::Stop as i32,
            node_id: 1,
            peer_node_id: 0,
            description: format!("synthetic {}", i),
        });
    }
    let resp = svc
        .get_fault_history(Request::new(GetFaultHistoryRequest { limit: 0 }))
        .await
        .expect("get_fault_history")
        .into_inner();
    assert_eq!(resp.events.len(), 64);
}
