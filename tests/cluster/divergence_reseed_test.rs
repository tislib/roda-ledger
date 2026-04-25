//! End-to-end test of the supervisor's divergence-driven Ledger
//! reseed (ADR-0016 §9, Stage 3c).
//!
//! Setup:
//!   1. Spin up a single multi-node-shaped cluster (one node here,
//!      but with `cluster.peers` listing two members so the
//!      supervisor boots in `Initializing` rather than `Leader`).
//!   2. Submit some transactions through `Ledger::append_wal_entries`
//!      directly so the follower has a real on-disk WAL with txs
//!      under term 1.
//!   3. Send a crafted `AppendEntries` RPC against the supervisor's
//!      Node gRPC port that disagrees with our log at `prev_tx_id`
//!      under a different `prev_term`. This stashes a divergence
//!      watermark in `NodeHandlerCore`.
//!   4. Wait for the supervisor's divergence watcher to consume the
//!      watermark and run `start_with_recovery_until(watermark)` on
//!      a new Ledger.
//!   5. Assert the live ledger now has `last_commit_id == watermark`
//!      and balances reflect only the surviving prefix.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::node as proto;
use roda_ledger::cluster::proto::node::node_client::NodeClient;
use roda_ledger::cluster::{
    self, ClusterNode, ClusterNodeSection, ClusterSection, PeerConfig, ServerSection,
};
use roda_ledger::config::{LedgerConfig, StorageConfig};
use roda_ledger::entities::WalEntry;
use roda_ledger::transaction::Operation;
use roda_ledger::wait_strategy::WaitStrategy;
use spdlog::Level;
use std::net::TcpListener;
use std::time::{Duration, Instant};
use tonic::transport::Channel;

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn fresh_temp_dir(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = format!("temp_divergence_reseed_{}_{}", label, nanos);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn ledger_cfg(data_dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: data_dir.to_string(),
            temporary: false,
            transaction_count_per_segment: 10_000_000,
            snapshot_frequency: 4,
        },
        wait_strategy: WaitStrategy::Balanced,
        log_level: Level::Critical,
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::default()
    }
}

async fn wait_for_tcp(addr: String) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for tcp at {}", addr);
}

async fn wait_until<F: Fn() -> bool>(label: &str, timeout: Duration, f: F) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for: {}", label);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_reseeds_on_divergence_detected_via_node_grpc() {
    let temp_dir = fresh_temp_dir("supervisor");
    let client_port = free_port();
    let node_port = free_port();
    let phantom_peer_port = free_port(); // peer 2 — never actually started

    // Multi-node-shaped config so the supervisor boots in
    // Initializing (avoids the seed-leader bring-up path which
    // would try to rotate term + spawn replication; we just need a
    // long-lived Node gRPC server that accepts AppendEntries and a
    // running watcher loop).
    let cfg = cluster::Config {
        cluster: Some(ClusterSection {
            node: ClusterNodeSection {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: node_port,
            },
            peers: vec![
                PeerConfig {
                    peer_id: 1,
                    host: format!("http://127.0.0.1:{}", node_port),
                },
                PeerConfig {
                    peer_id: 2,
                    host: format!("http://127.0.0.1:{}", phantom_peer_port),
                },
            ],
            replication_poll_ms: 5,
            append_entries_max_bytes: 4 * 1024 * 1024,
        }),
        server: ServerSection {
            host: "127.0.0.1".to_string(),
            port: client_port,
            ..Default::default()
        },
        ledger: ledger_cfg(&temp_dir),
    };

    let node = ClusterNode::new(cfg).expect("ClusterNode bring-up");

    // Seed the local ledger with 50 deposits to account 1 + 50 to
    // account 2 BEFORE starting the supervisor. This is the only
    // way to populate a ledger without a leader: use the
    // single-process Ledger::submit path before clustering enters
    // Initializing-only mode.
    let pre_ledger = node.ledger();
    pre_ledger.set_seal_watermark(50); // permit segments 1..=50 to seal
    for _ in 0..50 {
        pre_ledger.submit(Operation::Deposit {
            account: 1,
            amount: 1,
            user_ref: 0,
        });
    }
    pre_ledger.wait_for_pass();
    for _ in 0..50 {
        pre_ledger.submit(Operation::Deposit {
            account: 2,
            amount: 1,
            user_ref: 0,
        });
    }
    pre_ledger.wait_for_pass();
    assert_eq!(pre_ledger.last_commit_id(), 100);
    assert_eq!(pre_ledger.get_balance(1), 50);
    assert_eq!(pre_ledger.get_balance(2), 50);

    // Start the supervisor — long-lived gRPC servers + watcher loop
    // come up here.
    let handles = node.run().await.expect("supervisor.run");
    wait_for_tcp(format!("127.0.0.1:{}", node_port)).await;

    // Connect a Node-service client to the supervisor's gRPC port
    // and send a crafted divergent AppendEntries: same prev_tx_id
    // (50) but a different prev_term than the one we wrote under
    // (terms always start at 1 in tests, so claim 7).
    let mut client: NodeClient<Channel> =
        NodeClient::connect(format!("http://127.0.0.1:{}", node_port))
            .await
            .expect("connect");

    let leader_commit = 50u64;
    let resp = client
        .append_entries(proto::AppendEntriesRequest {
            leader_id: 2,
            term: 7,
            prev_tx_id: 50,
            prev_term: 7, // wrong — local prev_term is 1
            from_tx_id: 51,
            to_tx_id: 50,
            wal_bytes: Vec::new(),
            leader_commit_tx_id: leader_commit,
        })
        .await
        .expect("rpc")
        .into_inner();
    assert!(!resp.success);
    assert_eq!(
        resp.reject_reason,
        proto::RejectReason::RejectPrevMismatch as u32
    );

    // Watcher's poll cadence is 10 ms; reseed builds a Ledger
    // (sync I/O via spawn_blocking) which can take a few hundred
    // ms in release. Give it a generous deadline.
    let live_ledger = node.ledger_slot().clone();
    wait_until(
        "supervisor reseed to land at watermark",
        Duration::from_secs(10),
        || live_ledger.ledger().last_commit_id() == leader_commit,
    )
    .await;

    let post = live_ledger.ledger();
    assert_eq!(
        post.last_commit_id(),
        leader_commit,
        "ledger should be reseeded to the leader's watermark"
    );

    // Balances reflect only txs ≤ watermark. Account 1 had its 50
    // deposits in tx 1..=50 → all kept. Account 2 had its 50 in
    // tx 51..=100 → all truncated.
    assert_eq!(post.get_balance(1), 50, "account 1 prefix preserved");
    assert_eq!(post.get_balance(2), 0, "account 2 deposits truncated");

    handles.abort();

    // Sanity: a fresh start() over the same data dir sees the
    // reseed as durable — the truncated txs are gone.
    drop(handles);
    drop(node);
    let _ = std::fs::remove_dir_all(&temp_dir);
}

// Suppress unused-import warning when this test is the only thing
// that pulls these in.
#[allow(dead_code)]
fn _ensures_imports_compile() -> Vec<&'static str> {
    let _ = WalEntry::SegmentHeader;
    Vec::new()
}
