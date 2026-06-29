//! Shared helpers for the control crate's integration tests.
//!
//! Tests link against this module via `mod common;` at the top of each
//! `tests/*.rs` file. Cargo doesn't pick up `tests/common.rs` as a test
//! binary by itself — only files at the `tests/` root.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use control::service::ControlService;
use control::{ClusterHandle, EventStore, LedgerProxy};
use proto::control::control_server::Control;
use proto::control::{ClusterConfig, GetClusterSnapshotRequest, GetClusterSnapshotResponse};
use tonic::Request;

/// Locate `target/{profile}/roda-server` from the integration test
/// binary's own location. Panics with a clear message if the binary is
/// missing so CI failure mode is obvious.
pub fn locate_roda_server() -> PathBuf {
    let exe = std::env::current_exe().expect("current_exe");
    let deps_dir = exe.parent().expect("test exe has no parent dir");
    let profile_dir = deps_dir.parent().expect("deps dir has no parent");
    let candidate = profile_dir.join(if cfg!(windows) {
        "roda-server.exe"
    } else {
        "roda-server"
    });
    if !candidate.exists() {
        panic!(
            "roda-server binary not found at {}.\n\
             Build it first:\n  cargo build -p cluster --bin roda-server",
            candidate.display()
        );
    }
    candidate
}

/// The cluster configuration tests bootstrap with by default. Matches the
/// values used in `process_provisioner_smoke.rs`.
pub fn default_cluster_config() -> ClusterConfig {
    // transaction_count_per_segment drives the in-memory index buffer
    // size (~16 bytes per slot × 4× the segment count). 50_000 keeps
    // each child server at ~6 MB of index allocs vs ~120 MB at 1M.
    ClusterConfig {
        max_accounts: 10_000,
        queue_size: 1024,
        transaction_count_per_segment: 50_000,
        snapshot_frequency: 2,
        append_entries_max_bytes: 4 * 1024 * 1024,
    }
}

/// Bootstrap a fresh cluster with `node_count` real `roda-server` children.
/// The returned `ClusterHandle` owns the children; drop it to tear them down.
pub async fn bootstrap_cluster(node_count: u32) -> Arc<ClusterHandle> {
    ClusterHandle::bootstrap(locate_roda_server(), default_cluster_config(), node_count)
        .await
        .expect("bootstrap cluster")
}

/// Bootstrap a cluster and build the `ControlService` + `EventStore`
/// around it. The handle must stay in scope so the cluster lives long
/// enough for the test to finish.
pub async fn build_control(
    node_count: u32,
) -> (ControlService, Arc<EventStore>, Arc<ClusterHandle>) {
    let handle = bootstrap_cluster(node_count).await;
    let events = Arc::new(EventStore::new());
    let svc = ControlService::new(handle.clone(), events.clone());
    (svc, events, handle)
}

/// Bootstrap a cluster and build the `LedgerProxy` around it.
pub async fn build_ledger_proxy(node_count: u32) -> (LedgerProxy, Arc<ClusterHandle>) {
    let handle = bootstrap_cluster(node_count).await;
    let proxy = LedgerProxy::new(handle.clone());
    (proxy, handle)
}

/// Poll `get_cluster_snapshot` until a leader is elected. Returns the
/// elected `leader_node_id`. Panics on timeout.
pub async fn wait_for_leader(svc: &ControlService, timeout: Duration) -> u64 {
    let snap = wait_for_snapshot(svc, timeout, |s| s.leader_node_id != 0).await;
    snap.leader_node_id
}

/// Poll `get_cluster_snapshot` until `pred(snapshot)` holds. Returns the
/// first snapshot that satisfies the predicate.
pub async fn wait_for_snapshot<F>(
    svc: &ControlService,
    timeout: Duration,
    pred: F,
) -> GetClusterSnapshotResponse
where
    F: Fn(&GetClusterSnapshotResponse) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let snap = svc
            .get_cluster_snapshot(Request::new(GetClusterSnapshotRequest {}))
            .await
            .expect("get_cluster_snapshot")
            .into_inner();
        if pred(&snap) {
            return snap;
        }
        if Instant::now() >= deadline {
            panic!(
                "wait_for_snapshot: predicate never held within {:?}; last snapshot: {:?}",
                timeout, snap
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Pick the node_id of a follower from a snapshot. Returns the first one
/// found. Panics if no follower exists.
pub fn pick_follower(snap: &GetClusterSnapshotResponse) -> u64 {
    use proto::control::NodeRole;
    snap.nodes
        .iter()
        .find(|n| n.role == NodeRole::Follower as i32)
        .map(|n| n.node_id)
        .expect("snapshot has no follower")
}
