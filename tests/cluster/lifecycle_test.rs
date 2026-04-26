//! Resource lifecycle: harness drop/cleanup, supervisor abort, restart
//! semantics on the same data dir.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::ledger::WaitLevel;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl};
use std::time::Duration;
use tokio::time::sleep;

const ACCOUNT: u64 = 1;
const AMOUNT: u64 = 100;

/// `SupervisorHandles::abort` aborts the driver, watcher, both servers,
/// and all transient peer tasks. Verified by checking that ports are
/// freed afterwards (proxy for "all server tasks exited").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_abort_releases_all_ports() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let cport = ctl.client_port(0).unwrap();
    let nport = ctl.node_port(0).unwrap();
    ctl.stop_node(0).await.expect("stop");

    // `stop_node` awaits join handles + port release; both ports must
    // be immediately bindable.
    assert!(
        std::net::TcpListener::bind(("127.0.0.1", cport)).is_ok(),
        "client port {} still bound after stop_node returned",
        cport
    );
    assert!(
        std::net::TcpListener::bind(("127.0.0.1", nport)).is_ok(),
        "node port {} still bound after stop_node returned",
        nport
    );
}

/// `Drop` of a harness with a temp data dir removes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_drop_removes_owned_temp_dir() {
    let temp_root_path: std::path::PathBuf;
    {
        let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
            .await
            .expect("start");
        let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        temp_root_path = ctl.root_data_dir().to_path_buf();
        assert!(
            temp_root_path.exists(),
            "harness should have created the dir"
        );
    }
    sleep(Duration::from_millis(100)).await;
    assert!(
        !temp_root_path.exists(),
        "harness should have removed temp dir on Drop"
    );
}

/// Caller-supplied `data_dir_root` survives Drop (no rm).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_owned_data_dir_survives_drop() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut root = std::env::current_dir().unwrap();
    root.push(format!("temp_caller_owned_{}", nanos));

    {
        let _ctl = ClusterTestingControl::start(ClusterTestingConfig {
            data_dir_root: Some(root.clone()),
            ..ClusterTestingConfig::standalone()
        })
        .await
        .expect("start");
    }
    sleep(Duration::from_millis(100)).await;
    assert!(
        root.exists(),
        "caller-owned data dir must NOT be removed by harness Drop"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// Restart cycle on the same harness slot — data dir reopened, term
/// bumps, ledger keeps its committed state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_node_restart_preserves_state() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    let client = ctl.client(0).await.unwrap();
    let r = client
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);
    drop(client);

    ctl.stop_node(0).await.expect("stop");
    ctl.start_node(0).await.expect("restart");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    let new_client = ctl.client(0).await.unwrap();
    let bal = new_client.get_balance(ACCOUNT).await.unwrap().balance;
    assert_eq!(bal, AMOUNT as i64);
}

/// Per-leader peer tasks observe the supervisor flag and drain on
/// abort. Indirect: a stopped leader's port is fully free within a
/// short window, meaning the per-peer tasks (and the gRPC servers
/// they depended on) have all stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_tasks_drain_on_supervisor_abort() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let nport = ctl.node_port(leader_idx).unwrap();
    ctl.stop_node(leader_idx).await.expect("stop");

    // `stop_node` awaits the supervisor's join handles + the OS port
    // release before returning, so binding must succeed immediately.
    assert!(
        std::net::TcpListener::bind(("127.0.0.1", nport)).is_ok(),
        "leader's Node port {} still bound after stop_node returned",
        nport
    );
}

/// Standalone harness drop releases its single port.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_drop_releases_port() {
    let port: u16;
    {
        let ctl = ClusterTestingControl::start(ClusterTestingConfig::standalone())
            .await
            .expect("start");
        port = ctl.client_port(0).unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("standalone port {} not released after Drop", port);
        }
        sleep(Duration::from_millis(50)).await;
    }
}
