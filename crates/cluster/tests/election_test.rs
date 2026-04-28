//! End-to-end election test (ADR-0016 §5).
//!
//! Boots a three-node cluster with no static role assignment and
//! asserts:
//!
//! 1. **Cold-boot election** — within ~5× the election-timer max,
//!    exactly one node observes itself as `Leader` while the other
//!    two settle as `Follower` (or `Initializing` waiting for the
//!    first heartbeat from the new leader).
//! 2. **Single-leader invariant** — at no observed point are there
//!    two nodes simultaneously reporting `NodeRole::Leader` for the
//!    same term.
//! 3. **Failover** — killing the elected leader's bring-up handle
//!    causes one of the surviving two nodes to win a new election
//!    within another ~5× window.
//!
//! All Pings are observation-only; the test makes no writes.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::node as nproto;
use roda_ledger::cluster::proto::node::node_client::NodeClient;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Read the role + term any node currently reports via `Ping`.
/// Returns `None` on transport error or non-OK response (treated as
/// "not reachable" for the purpose of leader-counting).
async fn ping_role(node_port: u16) -> Option<(nproto::NodeRole, u64)> {
    let mut client = NodeClient::connect(format!("http://127.0.0.1:{}", node_port))
        .await
        .ok()?;
    let resp = client
        .ping(nproto::PingRequest {
            from_node_id: 0,
            nonce: 0,
        })
        .await
        .ok()?;
    let r = resp.into_inner();
    let role = nproto::NodeRole::try_from(r.role).ok()?;
    Some((role, r.term))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_elect_a_unique_leader_from_cold_boot() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "election".to_string(),
        replication_poll_ms: 5,
        append_entries_max_bytes: 256 * 1024,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    // ── Cold-boot election within 5× max election timeout ──────────────
    let leader_idx = ctl
        .wait_for_leader(Duration::from_millis(5 * 300))
        .await
        .expect("cold-boot leader");
    let leader_node_id = ctl.node_id(leader_idx).unwrap();
    eprintln!(
        "cold-boot election: node_id={} won (port {})",
        leader_node_id,
        ctl.node_port(leader_idx).unwrap()
    );

    // Snapshot current leadership for a brief window to exercise the
    // "no two leaders in the same term" invariant under healthy
    // heartbeats. The harness deliberately doesn't expose `term` on
    // its leader poll, so the term-disjoint variant stays inline.
    let invariant_deadline = Instant::now() + Duration::from_millis(400);
    while Instant::now() < invariant_deadline {
        let mut leader_terms: Vec<u64> = Vec::new();
        for i in 0..ctl.len() {
            let port = ctl.node_port(i).unwrap();
            if let Some((nproto::NodeRole::Leader, term)) = ping_role(port).await {
                leader_terms.push(term);
            }
        }
        let unique_terms: std::collections::HashSet<u64> = leader_terms.iter().copied().collect();
        if leader_terms.len() > 1 && unique_terms.len() == 1 {
            panic!(
                "two leaders in the same term {}: {} observed",
                leader_terms[0],
                leader_terms.len()
            );
        }
        sleep(Duration::from_millis(40)).await;
    }

    // ── Failover: kill the leader, expect a new one ────────────────────
    let killed_node_id = leader_node_id;
    ctl.stop_node(leader_idx).await.expect("stop leader");

    let new_leader_idx = ctl
        .wait_for_leader(Duration::from_millis(5 * 300))
        .await
        .expect("new leader");
    let new_leader_node_id = ctl.node_id(new_leader_idx).unwrap();
    assert_ne!(
        new_leader_node_id, killed_node_id,
        "the killed node cannot also be the new leader"
    );
    eprintln!(
        "failover: node_id={} elected as new leader after node_id={} died",
        new_leader_node_id, killed_node_id
    );

    // ── Shutdown ────────────────────────────────────────────────────────
    ctl.stop_all().await;
}
