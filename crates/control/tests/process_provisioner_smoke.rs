//! End-to-end smoke test: the runner drives a real cluster spawned
//! by `ProcessProvisioner` for the simplest seed scenario.
//!
//! Requires `roda-server` to be built — the test resolves the binary
//! relative to its own `current_exe()` (`target/{profile}/roda-server`).
//! If the binary is missing the test panics with a clear message
//! pointing at the build command, which is preferable to a silent
//! skip in CI.

use std::path::PathBuf;
use std::sync::Arc;

use control::provisioner::process::ProcessProvisioner;
use control::runner::ScenarioRunner;
use control::runner::ProvisionConfig;
use proto::control::ClusterConfig;
use testing::scenarios;

/// Resolve `target/{profile}/roda-server` from the test's own binary
/// location. The integration-test binary lives at
/// `target/{profile}/deps/<test>`, so we step up two levels.
fn locate_roda_server() -> PathBuf {
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

fn smoke_config(node_count: u32) -> ProvisionConfig {
    ProvisionConfig {
        node_count,
        cluster: ClusterConfig {
            max_accounts: 10_000,
            queue_size: 1024,
            transaction_count_per_segment: 1_000_000,
            snapshot_frequency: 2,
            replication_poll_ms: 5,
            append_entries_max_bytes: 4 * 1024 * 1024,
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_provisioner_runs_single_deposit_committed() {
    let server_bin = locate_roda_server();
    let provisioner = Arc::new(ProcessProvisioner::new(server_bin));
    let runner = ScenarioRunner::new(provisioner);

    let scenario = scenarios::list()
        .into_iter()
        .find(|s| s.name == "single_deposit_committed")
        .expect("seed catalogue must contain single_deposit_committed");

    runner
        .run(&scenario, &smoke_config(1))
        .await
        .expect("scenario should pass against real roda-server cluster");
}
