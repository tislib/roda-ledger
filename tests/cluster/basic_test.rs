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
use roda_ledger::cluster::{self, ServerSection, ClusterNode, PeerConfig};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_leader_replicates_to_follower() {
    let leader_client_port = free_port();
    let leader_node_port = free_port();
    let follower_client_port = free_port();
    let follower_node_port = free_port();

    let leader_cfg = cluster::Config {
        mode: cluster::Mode::Leader,
        node_id: 1,
        term: 1,
        peers: vec![PeerConfig {
            id: 2,
            node_addr: format!("http://127.0.0.1:{}", follower_node_port),
        }],
        server: ServerSection {
            host: "127.0.0.1".to_string(),
            port: leader_client_port,
            ..Default::default()
        },
        node: roda_ledger::cluster::config::NodeServerSection {
            host: "127.0.0.1".to_string(),
            port: leader_node_port,
        },
        ledger: ledger_cfg(&tmp_dir("leader"), 10_000_000),
        replication_poll_ms: 2,
        append_entries_max_bytes: 256 * 1024,
    };

    let follower_cfg = cluster::Config {
        mode: cluster::Mode::Follower,
        node_id: 2,
        term: 1,
        peers: Vec::new(),
        server: ServerSection {
            host: "127.0.0.1".to_string(),
            port: follower_client_port,
            ..Default::default()
        },
        node: roda_ledger::cluster::config::NodeServerSection {
            host: "127.0.0.1".to_string(),
            port: follower_node_port,
        },
        ledger: ledger_cfg(&tmp_dir("follower"), 20_000),
        replication_poll_ms: 2,
        append_entries_max_bytes: 256 * 1024,
    };

    // Follower first so its node port is listening when the leader's replication
    // task attempts to connect.
    let follower = ClusterNode::new(follower_cfg).expect("follower ledger");
    let follower_handles = follower.run().await.expect("follower run");

    let leader = ClusterNode::new(leader_cfg).expect("leader ledger");
    let leader_handles = leader.run().await.expect("leader run");

    // Wait for both gRPC servers to be bound.
    wait_for_tcp(format!("127.0.0.1:{}", leader_client_port)).await;
    wait_for_tcp(format!("127.0.0.1:{}", follower_client_port)).await;
    wait_for_tcp(format!("127.0.0.1:{}", follower_node_port)).await;

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
    let leader_ledger = leader.ledger();
    let follower_ledger = follower.ledger();

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
    leader_handles.abort();
    follower_handles.abort();
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
