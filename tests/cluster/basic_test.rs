//! End-to-end basic cluster mode test:
//! - Spin up one leader + one follower in the same tokio runtime.
//! - Leader accepts client writes on its Ledger gRPC port.
//! - Follower rejects writes (read_only handler) but serves reads.
//! - Leader's replication thread ships WAL bytes via AppendEntries.
//! - Follower's balances catch up to the leader's.

#![cfg(feature = "cluster")]

use lproto::submit_operation_request::Operation;
use roda_ledger::cluster::proto::ledger::ledger_client::LedgerClient;
use roda_ledger::cluster::proto::ledger::{self as lproto, Deposit, SubmitOperationRequest};
use roda_ledger::cluster::proto::node as nproto;
use roda_ledger::cluster::proto::node::node_client::NodeClient;
use roda_ledger::cluster::{
    self, ClusterNode, ClusterNodeSection, ClusterSection, PeerConfig, ServerSection,
};
use roda_ledger::config::{LedgerConfig, StorageConfig};
use roda_ledger::entities::SYSTEM_ACCOUNT_ID;
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
    d.push(format!("temp_cluster_{}_{}", name, nanos));
    d.to_string_lossy().into_owned()
}

fn ledger_cfg(data_dir: &str, tx_per_seg: u64) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: data_dir.to_string(),
            temporary: true,
            transaction_count_per_segment: tx_per_seg,
            snapshot_frequency: 2,
        },
        wait_strategy: WaitStrategy::Balanced,
        log_level: Level::Critical,
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::default()
    }
}

// Stage 4: with the Candidate loop landed, multi-node clusters
// elect a leader from cold boot — no static role is assigned. The
// follower no longer rejects writes deterministically (whichever
// node loses the election becomes the follower), so this test
// drives writes against the elected leader, identified via the
// `Ping` RPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_leader_replicates_to_follower() {
    let n1_client_port = free_port();
    let n1_node_port = free_port();
    let n2_client_port = free_port();
    let n2_node_port = free_port();

    // Symmetric peer list — both nodes see each other plus self.
    let all_peers = vec![
        PeerConfig {
            peer_id: 1,
            host: format!("http://127.0.0.1:{}", n1_node_port),
        },
        PeerConfig {
            peer_id: 2,
            host: format!("http://127.0.0.1:{}", n2_node_port),
        },
    ];

    let n1_cfg = cluster::Config {
        cluster: Some(ClusterSection {
            node: ClusterNodeSection {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: n1_node_port,
            },
            peers: all_peers.clone(),
            replication_poll_ms: 2,
            append_entries_max_bytes: 256 * 1024,
        }),
        server: ServerSection {
            host: "127.0.0.1".to_string(),
            port: n1_client_port,
            ..Default::default()
        },
        ledger: ledger_cfg(&tmp_dir("n1"), 10_000_000),
    };

    let n2_cfg = cluster::Config {
        cluster: Some(ClusterSection {
            node: ClusterNodeSection {
                node_id: 2,
                host: "127.0.0.1".to_string(),
                port: n2_node_port,
            },
            peers: all_peers,
            replication_poll_ms: 2,
            append_entries_max_bytes: 256 * 1024,
        }),
        server: ServerSection {
            host: "127.0.0.1".to_string(),
            port: n2_client_port,
            ..Default::default()
        },
        ledger: ledger_cfg(&tmp_dir("n2"), 20_000),
    };

    let n2 = ClusterNode::new(n2_cfg).expect("n2 ledger");
    let n2_handles = n2.run().await.expect("n2 run");

    let n1 = ClusterNode::new(n1_cfg).expect("n1 ledger");
    let n1_handles = n1.run().await.expect("n1 run");

    // Wait for both gRPC servers to be bound.
    wait_for_tcp(format!("127.0.0.1:{}", n1_client_port)).await;
    wait_for_tcp(format!("127.0.0.1:{}", n2_client_port)).await;
    wait_for_tcp(format!("127.0.0.1:{}", n1_node_port)).await;
    wait_for_tcp(format!("127.0.0.1:{}", n2_node_port)).await;

    // ── Wait for an election to settle on a unique Leader ───────────────
    let (leader_node_id, leader_client_port, follower_client_port) = wait_for_leader(&[
        (1, n1_node_port, n1_client_port),
        (2, n2_node_port, n2_client_port),
    ])
    .await;
    eprintln!(
        "election settled: node_id={} is leader (client port {})",
        leader_node_id, leader_client_port
    );

    // ── Connect clients to both Ledger servers ───────────────────────────
    let mut leader_client =
        LedgerClient::connect(format!("http://127.0.0.1:{}", leader_client_port))
            .await
            .expect("leader client connect");
    let mut follower_client =
        LedgerClient::connect(format!("http://127.0.0.1:{}", follower_client_port))
            .await
            .expect("follower client connect");

    // ── Follower rejects writes ─────────────────────────────────────────
    let write_err = follower_client
        .submit_operation(SubmitOperationRequest {
            operation: Some(Operation::Deposit(Deposit {
                account: 1,
                amount: 10,
                user_ref: 0,
            })),
        })
        .await
        .expect_err("follower must reject writes");
    assert_eq!(
        write_err.code(),
        tonic::Code::FailedPrecondition,
        "follower should return FAILED_PRECONDITION on submit"
    );

    // ── Drive writes on the leader ──────────────────────────────────────
    let account = 7u64;
    let amount = 100u64;
    let total_tx = 200u64;

    for _ in 0..total_tx {
        leader_client
            .submit_operation(SubmitOperationRequest {
                operation: Some(Operation::Deposit(Deposit {
                    account,
                    amount,
                    user_ref: 0,
                })),
            })
            .await
            .expect("leader submit");
    }

    // ── Wait for follower to catch up ────────────────────────────────────
    let (leader_ledger, follower_ledger) = if leader_node_id == 1 {
        (n1.ledger(), n2.ledger())
    } else {
        (n2.ledger(), n1.ledger())
    };

    // Leader commit first.
    wait_for(Duration::from_secs(30), "leader commit_index", || {
        leader_ledger.last_commit_id() >= total_tx
    })
    .await;

    // Follower replication lag → commit.
    wait_for(Duration::from_secs(60), "follower commit_index", || {
        follower_ledger.last_commit_id() >= leader_ledger.last_commit_id()
    })
    .await;

    wait_for(Duration::from_secs(60), "follower snapshot_index", || {
        follower_ledger.last_snapshot_id() >= leader_ledger.last_snapshot_id()
    })
    .await;

    // ── Verification: balances match ─────────────────────────────────────
    assert_eq!(
        leader_ledger.get_balance(account),
        follower_ledger.get_balance(account),
        "user account balance must match between leader and follower"
    );
    assert_eq!(
        leader_ledger.get_balance(SYSTEM_ACCOUNT_ID),
        follower_ledger.get_balance(SYSTEM_ACCOUNT_ID),
        "system account balance must match"
    );
    let expected_abs = (total_tx * amount) as i64;
    assert_eq!(leader_ledger.get_balance(account).abs(), expected_abs);
    assert_eq!(
        leader_ledger.get_balance(account) + leader_ledger.get_balance(SYSTEM_ACCOUNT_ID),
        0
    );

    // ── Shutdown ────────────────────────────────────────────────────────
    n1_handles.abort();
    n2_handles.abort();
}

/// Poll every node's `Ping` endpoint until exactly one reports
/// `NodeRole::Leader`. Returns `(leader_node_id, leader_client_port,
/// follower_client_port)`. Panics on timeout (5 s) or on observing
/// two simultaneous leaders.
///
/// Each tuple is `(node_id, peer_node_port, client_port)`. Only
/// peer-facing ports run the `Node` gRPC service.
async fn wait_for_leader(nodes: &[(u64, u16, u16)]) -> (u64, u16, u16) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut leaders: Vec<(u64, u16)> = Vec::new();
        let mut other_clients: Vec<u16> = Vec::new();
        for (node_id, node_port, client_port) in nodes {
            let role = ping_role(*node_port).await;
            if matches!(role, Some(nproto::NodeRole::Leader)) {
                leaders.push((*node_id, *client_port));
            } else {
                other_clients.push(*client_port);
            }
        }
        match leaders.len() {
            1 => return (leaders[0].0, leaders[0].1, other_clients[0]),
            n if n > 1 => panic!("two leaders observed simultaneously: {:?}", leaders),
            _ => {}
        }
        if Instant::now() >= deadline {
            panic!("no leader elected within 5s");
        }
        sleep(Duration::from_millis(20)).await;
    }
}

async fn ping_role(node_port: u16) -> Option<nproto::NodeRole> {
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
    nproto::NodeRole::try_from(resp.into_inner().role).ok()
}

// ─── Helpers ────────────────────────────────────────────────────────────────

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

async fn wait_for<F: FnMut() -> bool>(timeout: Duration, label: &str, mut f: F) {
    let deadline = Instant::now() + timeout;
    while !f() {
        if Instant::now() >= deadline {
            panic!("timeout waiting for: {label}");
        }
        sleep(Duration::from_millis(10)).await;
    }
}
