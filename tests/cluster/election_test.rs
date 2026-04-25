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
use roda_ledger::cluster::{
    self, ClusterNode, ClusterNodeSection, ClusterSection, Handles, PeerConfig, ServerSection,
};
use roda_ledger::config::{LedgerConfig, StorageConfig};
use roda_ledger::wait_strategy::WaitStrategy;
use spdlog::Level;
use std::net::TcpListener;
use std::time::{Duration, Instant};
use tokio::time::sleep;

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn tmp_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut d = std::env::current_dir().unwrap();
    d.push(format!("temp_election_{}_{}", name, nanos));
    d.to_string_lossy().into_owned()
}

fn ledger_cfg(data_dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: data_dir.to_string(),
            temporary: true,
            transaction_count_per_segment: 10_000_000,
            snapshot_frequency: 2,
        },
        wait_strategy: WaitStrategy::Balanced,
        log_level: Level::Critical,
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::default()
    }
}

#[derive(Clone, Copy)]
struct NodeAddr {
    node_id: u64,
    node_port: u16,
    client_port: u16,
}

fn build_config(self_id: u64, addrs: &[NodeAddr], data_dir: &str) -> cluster::Config {
    let me = addrs
        .iter()
        .find(|a| a.node_id == self_id)
        .copied()
        .expect("self in addrs");
    let peers: Vec<PeerConfig> = addrs
        .iter()
        .map(|a| PeerConfig {
            peer_id: a.node_id,
            host: format!("http://127.0.0.1:{}", a.node_port),
        })
        .collect();
    cluster::Config {
        cluster: Some(ClusterSection {
            node: ClusterNodeSection {
                node_id: self_id,
                host: "127.0.0.1".to_string(),
                port: me.node_port,
            },
            peers,
            replication_poll_ms: 5,
            append_entries_max_bytes: 256 * 1024,
        }),
        server: ServerSection {
            host: "127.0.0.1".to_string(),
            port: me.client_port,
            ..Default::default()
        },
        ledger: ledger_cfg(data_dir),
    }
}

async fn wait_for_tcp(addr: String) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("tcp {} never bound", addr);
        }
        sleep(Duration::from_millis(10)).await;
    }
}

/// Read the role any node currently reports via `Ping`. Returns
/// `None` on transport error or non-OK response (treated as "not
/// reachable" for the purpose of leader-counting).
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

/// Poll every node's `Ping` until exactly one reports `Leader`.
/// Panics on timeout or simultaneous-leaders.
async fn wait_for_unique_leader(addrs: &[NodeAddr], timeout: Duration) -> NodeAddr {
    let deadline = Instant::now() + timeout;
    loop {
        let mut leaders: Vec<NodeAddr> = Vec::new();
        let mut leader_terms: Vec<u64> = Vec::new();
        for a in addrs {
            if let Some((nproto::NodeRole::Leader, term)) = ping_role(a.node_port).await {
                leaders.push(*a);
                leader_terms.push(term);
            }
        }
        // ADR-0016 invariant: at most one Leader per term. Two
        // simultaneous Leaders are tolerated only if they're in
        // different terms (i.e. we're observing the in-flight
        // step-down of the old leader); even then the test should
        // flag and retry rather than fall into the "exactly one"
        // path.
        let unique_terms: std::collections::HashSet<u64> = leader_terms.iter().copied().collect();
        if leaders.len() > 1 && unique_terms.len() == 1 {
            panic!(
                "two leaders in the same term {}: {:?}",
                leader_terms[0],
                leaders.iter().map(|a| a.node_id).collect::<Vec<_>>()
            );
        }
        if leaders.len() == 1 {
            return leaders[0];
        }
        if Instant::now() >= deadline {
            panic!(
                "no unique leader within {:?}; observed leaders: {:?}",
                timeout,
                leaders.iter().map(|a| a.node_id).collect::<Vec<_>>()
            );
        }
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_elect_a_unique_leader_from_cold_boot() {
    let addrs: Vec<NodeAddr> = (1u64..=3)
        .map(|id| NodeAddr {
            node_id: id,
            node_port: free_port(),
            client_port: free_port(),
        })
        .collect();

    // Boot all three nodes. No node has a "leader" config — they
    // start in Initializing and run an election.
    let mut nodes: Vec<ClusterNode> = Vec::new();
    let mut handles: Vec<Handles> = Vec::new();
    for a in &addrs {
        let cfg = build_config(a.node_id, &addrs, &tmp_dir(&format!("n{}", a.node_id)));
        let n = ClusterNode::new(cfg).expect("node ledger");
        let h = n.run().await.expect("node run");
        nodes.push(n);
        handles.push(h);
    }
    for a in &addrs {
        wait_for_tcp(format!("127.0.0.1:{}", a.node_port)).await;
    }

    // ── Cold-boot election within 5× max election timeout ──────────────
    let leader = wait_for_unique_leader(&addrs, Duration::from_millis(5 * 300)).await;
    eprintln!(
        "cold-boot election: node_id={} won (port {})",
        leader.node_id, leader.node_port
    );

    // Snapshot current leadership for a brief window to exercise the
    // "no two leaders" invariant under healthy heartbeats.
    let invariant_deadline = Instant::now() + Duration::from_millis(400);
    while Instant::now() < invariant_deadline {
        let mut count = 0usize;
        for a in &addrs {
            if matches!(
                ping_role(a.node_port).await,
                Some((nproto::NodeRole::Leader, _))
            ) {
                count += 1;
            }
        }
        assert!(
            count <= 1,
            "two simultaneous Leaders observed (count={})",
            count
        );
        sleep(Duration::from_millis(40)).await;
    }

    // ── Failover: kill the leader, expect a new one ────────────────────
    let leader_idx = addrs.iter().position(|a| a.node_id == leader.node_id).unwrap();
    handles[leader_idx].abort();
    // Drop the killed handle so we don't accidentally wait on it.
    let _killed = handles.remove(leader_idx);
    let killed_addr = addrs[leader_idx];
    let surviving_addrs: Vec<NodeAddr> = addrs
        .iter()
        .copied()
        .filter(|a| a.node_id != killed_addr.node_id)
        .collect();

    // Wait briefly so the abort actually drops the bind listener.
    sleep(Duration::from_millis(100)).await;

    let new_leader =
        wait_for_unique_leader(&surviving_addrs, Duration::from_millis(5 * 300)).await;
    assert_ne!(
        new_leader.node_id, killed_addr.node_id,
        "the killed node cannot also be the new leader"
    );
    eprintln!(
        "failover: node_id={} elected as new leader after node_id={} died",
        new_leader.node_id, killed_addr.node_id
    );

    // ── Shutdown ────────────────────────────────────────────────────────
    for h in handles {
        h.abort();
    }
}
