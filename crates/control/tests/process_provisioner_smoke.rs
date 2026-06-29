//! End-to-end smoke test: the runner drives a real cluster spawned
//! by `ProcessProvisioner` for the simplest seed scenario.
//!
//! Requires `roda-server` to be built — `common::locate_roda_server`
//! resolves it relative to the test binary and panics with a clear
//! message if missing.

use std::sync::Arc;

use control::provisioner::process::ProcessProvisioner;
use control::runner::{MetricsCollector, ProvisionConfig, ScenarioRunner};
use control::scenarios;

mod common;

fn smoke_config(node_count: u32) -> ProvisionConfig {
    ProvisionConfig {
        node_count,
        cluster: common::default_cluster_config(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_provisioner_runs_single_deposit_committed() {
    let server_bin = common::locate_roda_server();
    let provisioner = Arc::new(ProcessProvisioner::new(server_bin));
    let runner = ScenarioRunner::new(provisioner);

    let scenario = scenarios::list()
        .into_iter()
        .find(|s| s.name == "single_deposit_committed")
        .expect("seed catalogue must contain single_deposit_committed");

    let metrics = Arc::new(MetricsCollector::new());
    let report = runner.run(&scenario, &smoke_config(1), metrics).await;
    report
        .result
        .expect("scenario should pass against real roda-server cluster");
}
