//! Idle CPU probe: bring up a cluster, submit a handful of transactions,
//! then sit idle for 1 minute. Used to investigate why the leader pegs
//! a core at 100% CPU when there is no work to do. Attach a profiler
//! (e.g. `samply record -p <pid>`) during the sleep window.
//!
//! Not exercised in CI by default — it just runs for >60s and asserts
//! nothing meaningful. Run explicitly with:
//!   cargo test -p cluster --test idle_cpu_test -- --nocapture --ignored

use ::proto::ledger::WaitLevel;
use cluster::testing::{ClusterTestingConfig, ClusterTestingControl};
use std::time::Duration;
use spdlog::Level::Critical;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-running idle probe; run manually when investigating CPU"]
async fn cluster_idle_cpu_probe() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "idle-cpu".to_string(),
        ledger_log_level: Critical,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    let leader_idx = ctl
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("leader");
    eprintln!(
        "leader elected: slot={} node_id={} pid={}",
        leader_idx,
        ctl.node_id(leader_idx).unwrap(),
        std::process::id(),
    );

    let account = 1u64;
    let amount = 100u64;
    let tx_count = 10u64;
    let deposits: Vec<(u64, u64, u64)> = (0..tx_count).map(|i| (account, amount, i + 1)).collect();
    let results = ctl
        .deposit_batch_and_wait(&deposits, WaitLevel::ClusterCommit)
        .await
        .expect("batch deposit");
    assert_eq!(results.len(), tx_count as usize);

    eprintln!(
        "submitted {} tx; now idling for 60s — attach profiler to pid {}",
        tx_count,
        std::process::id(),
    );
    sleep(Duration::from_secs(600)).await;
    eprintln!("idle window over; shutting down");

    ctl.stop_all().await;
}
