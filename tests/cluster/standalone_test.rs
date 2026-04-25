//! Standalone (non-clustered) `ClusterNode::run` end-to-end test.
//!
//! Cluster Stage 3a addendum: a `Config` whose `[cluster]` block is
//! absent boots **only** the writable client-facing Ledger gRPC
//! server. No Node gRPC, no replication, no peers — and the term log
//! still advances on every restart in lock-step with the cluster
//! path (ADR-0016 §11).

#![cfg(feature = "cluster")]

use lproto::submit_operation_request::Operation;
use roda_ledger::cluster::proto::ledger::ledger_client::LedgerClient;
use roda_ledger::cluster::proto::ledger::{self as lproto, Deposit, SubmitOperationRequest};
use roda_ledger::cluster::{self, ClusterNode, Term};
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
    let mut dir = std::env::current_dir().unwrap();
    dir.push(format!("temp_standalone_{}_{}", name, nanos));
    dir.to_string_lossy().into_owned()
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

async fn wait_for_tcp(addr: String) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return;
        }
        sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for tcp at {}", addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_serves_writes_with_no_node_grpc() {
    let client_port = free_port();

    let cfg = cluster::Config {
        // No [cluster] block at all → standalone mode.
        cluster: None,
        server: cluster::ServerSection {
            host: "127.0.0.1".to_string(),
            port: client_port,
            ..Default::default()
        },
        ledger: ledger_cfg(&tmp_dir("standalone")),
    };
    cfg.validate().expect("standalone validate");
    assert!(!cfg.is_clustered());

    let node = ClusterNode::new(cfg).expect("standalone bring-up");
    let handles = node.run().await.expect("standalone run");

    // Client server should be up.
    wait_for_tcp(format!("127.0.0.1:{}", client_port)).await;

    // No Node gRPC handle exposed in standalone.
    assert!(handles.node_handle().is_none());
    assert!(handles.quorum().is_none());
    assert!(handles.as_cluster().is_none());

    // Standalone is writable: a client can submit and observe a tx_id.
    let mut client = LedgerClient::connect(format!("http://127.0.0.1:{}", client_port))
        .await
        .expect("connect");
    let resp = client
        .submit_operation(SubmitOperationRequest {
            operation: Some(Operation::Deposit(Deposit {
                account: 1,
                amount: 100,
                user_ref: 0,
            })),
            ..Default::default()
        })
        .await
        .expect("submit_operation");
    let tx_id = resp.into_inner().transaction_id;
    assert_eq!(tx_id, 1, "first standalone submit should be tx 1");

    handles.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_term_log_advances_on_restart() {
    // Both standalone and clustered modes bump term on every Ledger
    // start. Verify that across restarts, term.log carries forward
    // and the next term opens above the previous one.
    let dir = tmp_dir("standalone_term_advance");

    let cfg1 = cluster::Config {
        cluster: None,
        server: cluster::ServerSection {
            host: "127.0.0.1".to_string(),
            port: free_port(),
            ..Default::default()
        },
        ledger: LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                temporary: false, // we want to reopen after first run
                transaction_count_per_segment: 10_000_000,
                snapshot_frequency: 2,
            },
            wait_strategy: WaitStrategy::Balanced,
            log_level: Level::Critical,
            seal_check_internal: Duration::from_millis(10),
            ..LedgerConfig::default()
        },
    };

    {
        let node = ClusterNode::new(cfg1.clone()).unwrap();
        let _handles = node.run().await.unwrap();
        let term = Term::open_in_dir(&dir).unwrap();
        assert_eq!(
            term.get_current_term(),
            1,
            "first start opens term 1"
        );
        // Drop node + handles
    }

    // Restart with the same dir.
    {
        let node = ClusterNode::new(cfg1).unwrap();
        let _handles = node.run().await.unwrap();
        let term = Term::open_in_dir(&dir).unwrap();
        assert_eq!(
            term.get_current_term(),
            2,
            "second start bumps to term 2"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
