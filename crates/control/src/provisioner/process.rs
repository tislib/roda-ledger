//! Process-based provisioner — each node runs as its own
//! `roda-server` child process.
//!
//! Layout per `provision()` call:
//!
//! ```text
//! <base_temp_dir>/roda-cluster-<rand>/
//!     node_1/
//!         config.toml
//!         data/    ← per-node WAL + snapshots
//!     node_2/
//!         ...
//! ```
//!
//! Free ports are picked via the `bind(0) → drop` trick before
//! spawning so each node's `[server]` and `[cluster.node]` blocks
//! reference an actually-available port. The full peers list is
//! known at config-render time, so each node's TOML lists every
//! sibling's `cluster.host`.
//!
//! Capabilities: `kill = true`, `network_partition = false`. The
//! provisioner owns the OS processes, so it can `SIGKILL` them, but
//! it cannot drop traffic between two nodes without root or a
//! container substrate. Scenarios needing partition will be rejected
//! by the runner's pre-flight check before `provision()` is called.
//!
//! Teardown is RAII: `Drop` kills any remaining children and
//! recursively removes the cluster temp directory. There is no
//! `destroy()` method per the `Provisioner` contract.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use proto::control::ClusterConfig;
use tokio::time::{Instant, sleep};

use super::{Capabilities, ProvisionConfig, Provisioner, ProvisionerError};

/// Spawns one `roda-server` child per node. See module docs.
pub struct ProcessProvisioner {
    server_bin: PathBuf,
    base_temp_dir: PathBuf,
    /// When `true`, child processes' stdout/stderr is discarded.
    /// Default `false` — logs flow through to the parent's
    /// stdout/stderr, which is what most tests want. The scenario
    /// CLI flips this to `true` so per-scenario output stays clean.
    quiet: bool,
    /// Per-node deadline applied by `wait_one_ready`. Must cover the
    /// worst-case cold-start WAL replay, since the gRPC port isn't
    /// bound until `Ledger::start()` returns. Default 60s — comfortable
    /// for the 1M-tx crash-recovery scenario on a CI worker.
    readiness_timeout: Duration,
    state: Mutex<State>,
}

/// Default deadline `wait_one_ready` applies when none is overridden.
/// See [`ProcessProvisioner::readiness_timeout`] for the rationale.
pub const DEFAULT_READINESS_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Default)]
struct State {
    cluster_dir: Option<PathBuf>,
    nodes: Vec<NodeRecord>,
    last_config: Option<ProvisionConfig>,
}

struct NodeRecord {
    /// 1-based id matching the `[cluster.node].node_id` in the
    /// rendered config. Carried for diagnostics and future
    /// `NodeSelector::Id` resolution; unused at the trait surface.
    #[allow(dead_code)]
    node_id: u64,
    client_addr: SocketAddr,
    /// Peer-facing (Raft) bind. Carried for diagnostics; the
    /// provisioner doesn't dial peers directly.
    #[allow(dead_code)]
    cluster_addr: SocketAddr,
    config_path: PathBuf,
    /// Per-node data directory (WAL + snapshots). Removed transitively
    /// when the parent cluster temp dir is deleted on Drop.
    #[allow(dead_code)]
    data_dir: PathBuf,
    /// `None` while the process is stopped (between `kill_node` /
    /// `stop_node` and the next `start_node` / `restart_node`).
    child: Option<Child>,
}

impl ProcessProvisioner {
    /// Build a provisioner that will spawn `server_bin` for each
    /// node. Per-cluster temp directories are created under
    /// `std::env::temp_dir()`; override via [`with_base_temp_dir`].
    pub fn new(server_bin: PathBuf) -> Self {
        Self {
            server_bin,
            base_temp_dir: std::env::temp_dir(),
            quiet: false,
            readiness_timeout: DEFAULT_READINESS_TIMEOUT,
            state: Mutex::new(State::default()),
        }
    }

    /// Override the per-node readiness deadline. Useful for tests that
    /// know recovery will be much shorter (tight timeout = faster fail)
    /// or much longer (huge WAL replay) than the default.
    pub fn with_readiness_timeout(mut self, timeout: Duration) -> Self {
        self.readiness_timeout = timeout;
        self
    }

    /// Override the parent directory used for per-cluster temp dirs.
    pub fn with_base_temp_dir(mut self, dir: PathBuf) -> Self {
        self.base_temp_dir = dir;
        self
    }

    /// When `true`, discard each child server's stdout / stderr.
    /// Useful for keeping the parent process's output clean (e.g.
    /// when the parent is itself rendering scenario progress).
    /// Force a fresh provision on the next `provision()` call by clearing
    /// the cached last_config + tearing down current nodes + removing the
    /// cluster temp dir. After this returns, the next `provision()` call
    /// will go through the cold-provision path even with an identical
    /// config (which is what `ResetCluster` needs).
    pub fn reset_for_full_reprovision(&self) {
        let mut state = self.state.lock();
        teardown_existing(&mut state);
        if let Some(dir) = state.cluster_dir.take() {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    pub fn quiet(mut self, quiet: bool) -> Self {
        self.quiet = quiet;
        self
    }
}

#[async_trait]
impl Provisioner for ProcessProvisioner {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            kill: true,
            network_partition: false,
        }
    }

    async fn provision(&self, config: &ProvisionConfig) -> Result<Vec<String>, ProvisionerError> {
        // Fast path: identical config and a cluster already running.
        {
            let state = self.state.lock();
            if let Some(prev) = state.last_config.as_ref()
                && prev == config
                && all_running(&state.nodes)
            {
                return Ok(client_urls(&state.nodes));
            }
        }

        // Cold provision: tear down anything stale, build fresh.
        // (Reconciliation against a partially-matching cluster is
        // future work; for now we rebuild on any diff.)
        teardown_existing(&mut self.state.lock());

        let cluster_dir = create_cluster_dir(&self.base_temp_dir)?;

        // Reserve addresses + create per-node data dirs *before*
        // rendering any config — every node's TOML needs the full
        // peers list, so all addresses must be known up front.
        let mut plans: Vec<NodePlan> = Vec::with_capacity(config.node_count as usize);
        for i in 0..config.node_count.max(1) as u64 {
            let client_addr = pick_free_addr()?;
            let cluster_addr = pick_free_addr()?;
            let data_dir = cluster_dir.join(format!("node_{}", i + 1));
            std::fs::create_dir_all(&data_dir).map_err(|e| {
                ProvisionerError::ProvisionFailed(format!(
                    "create data dir {}: {e}",
                    data_dir.display()
                ))
            })?;
            plans.push(NodePlan {
                node_id: i + 1,
                client_addr,
                cluster_addr,
                data_dir,
            });
        }

        // Render configs against the fully-populated peer list.
        for plan in &plans {
            let toml = render_config_toml(plan, &plans, &config.cluster);
            let config_path = plan.data_dir.join("config.toml");
            std::fs::write(&config_path, &toml).map_err(|e| {
                ProvisionerError::ProvisionFailed(format!(
                    "write config {}: {e}",
                    config_path.display()
                ))
            })?;
        }

        // Spawn every node, then wait for all to accept gRPC. Order
        // matters less than spawning before anyone waits — a follower
        // can't elect anyone until its peers are also up.
        let mut nodes = Vec::with_capacity(plans.len());
        for plan in plans {
            let config_path = plan.data_dir.join("config.toml");
            let child = spawn_server(&self.server_bin, &config_path, self.quiet)?;
            nodes.push(NodeRecord {
                node_id: plan.node_id,
                client_addr: plan.client_addr,
                cluster_addr: plan.cluster_addr,
                config_path,
                data_dir: plan.data_dir,
                child: Some(child),
            });
        }

        let urls = client_urls(&nodes);
        wait_all_ready(&urls, self.readiness_timeout).await?;

        let mut state = self.state.lock();
        state.cluster_dir = Some(cluster_dir);
        state.nodes = nodes;
        state.last_config = Some(config.clone());

        Ok(urls)
    }

    async fn stop_node(&self, idx: usize) -> Result<(), ProvisionerError> {
        // The server doesn't have a well-defined SIGTERM-driven
        // graceful shutdown today; stop is a synonym for kill until
        // that's wired up. Documented at the trait level — happy-
        // path scenarios should not depend on the distinction.
        self.kill_node(idx).await
    }

    async fn kill_node(&self, idx: usize) -> Result<(), ProvisionerError> {
        let mut state = self.state.lock();
        let count = state.nodes.len();
        let node = state
            .nodes
            .get_mut(idx)
            .ok_or(ProvisionerError::NodeIndexOutOfBounds(idx, count))?;
        if let Some(mut child) = node.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        Ok(())
    }

    async fn start_node(&self, idx: usize) -> Result<(), ProvisionerError> {
        // Pull the spawn args out under the lock, drop it, then
        // spawn + wait without holding the lock so we don't block
        // other provisioner ops on the readiness probe.
        let (config_path, url, already_running) = {
            let state = self.state.lock();
            let count = state.nodes.len();
            let node = state
                .nodes
                .get(idx)
                .ok_or(ProvisionerError::NodeIndexOutOfBounds(idx, count))?;
            (
                node.config_path.clone(),
                client_url(node.client_addr),
                node.child.is_some(),
            )
        };
        if already_running {
            return Ok(());
        }

        let child = spawn_server(&self.server_bin, &config_path, self.quiet)?;
        wait_one_ready(&url, self.readiness_timeout).await?;

        let mut state = self.state.lock();
        if let Some(node) = state.nodes.get_mut(idx) {
            node.child = Some(child);
        }
        Ok(())
    }

    async fn restart_node(&self, idx: usize) -> Result<(), ProvisionerError> {
        self.kill_node(idx).await?;
        self.start_node(idx).await
    }

    async fn partition_pair(&self, _a: usize, _b: usize) -> Result<(), ProvisionerError> {
        Err(ProvisionerError::Unimplemented(
            "partition_pair (network_partition capability not advertised)",
        ))
    }

    async fn heal_partition(&self, _a: usize, _b: usize) -> Result<(), ProvisionerError> {
        Err(ProvisionerError::Unimplemented(
            "heal_partition (network_partition capability not advertised)",
        ))
    }
}

impl Drop for ProcessProvisioner {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        teardown_existing(&mut state);
        if let Some(dir) = state.cluster_dir.take() {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

// ============================================================
// Internal helpers
// ============================================================

struct NodePlan {
    node_id: u64,
    client_addr: SocketAddr,
    cluster_addr: SocketAddr,
    data_dir: PathBuf,
}

fn create_cluster_dir(base: &Path) -> Result<PathBuf, ProvisionerError> {
    let rand = rand::random::<u64>() % 1_000_000_000;
    let dir = base.join(format!("roda-cluster-{rand}"));
    std::fs::create_dir_all(&dir).map_err(|e| {
        ProvisionerError::ProvisionFailed(format!("create_dir_all({}): {e}", dir.display()))
    })?;
    Ok(dir)
}

fn pick_free_addr() -> Result<SocketAddr, ProvisionerError> {
    use std::collections::HashSet;
    use std::sync::LazyLock;
    static ALLOCATED: LazyLock<std::sync::Mutex<HashSet<u16>>> =
        LazyLock::new(|| std::sync::Mutex::new(HashSet::new()));
    let mut allocated = ALLOCATED.lock().expect("port set");
    let mut held: Vec<TcpListener> = Vec::new();
    loop {
        let listener = TcpListener::bind("127.0.0.1:0")
            .map_err(|e| ProvisionerError::ProvisionFailed(format!("bind 127.0.0.1:0: {e}")))?;
        let addr = listener
            .local_addr()
            .map_err(|e| ProvisionerError::ProvisionFailed(format!("local_addr: {e}")))?;
        if allocated.insert(addr.port()) {
            return Ok(addr);
        }
        held.push(listener);
    }
}

fn spawn_server(bin: &Path, config_path: &Path, quiet: bool) -> Result<Child, ProvisionerError> {
    let (stdout, stderr) = if quiet {
        (Stdio::null(), Stdio::null())
    } else {
        (Stdio::inherit(), Stdio::inherit())
    };
    // `log_level` isn't part of the TOML schema (`#[serde(skip)]` on
    // `LedgerConfig`); the e2e harness drives it via `RODA_LOG_LEVEL`
    // so spawned servers default to info-level output during scenario
    // runs. Callers that already set `RODA_LOG_LEVEL` upstream win —
    // we only seed the default. Bump to `debug` ad-hoc with
    // `RODA_LOG_LEVEL=debug cargo run -p control --bin scenario …`.
    let mut cmd = Command::new(bin);
    cmd.arg(config_path).stdout(stdout).stderr(stderr);
    if std::env::var_os("RODA_LOG_LEVEL").is_none() {
        cmd.env("RODA_LOG_LEVEL", "info");
    }
    cmd.spawn().map_err(|e| {
        ProvisionerError::ProvisionFailed(format!(
            "spawn `{} {}`: {e}",
            bin.display(),
            config_path.display()
        ))
    })
}

fn client_url(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

fn client_urls(nodes: &[NodeRecord]) -> Vec<String> {
    nodes.iter().map(|n| client_url(n.client_addr)).collect()
}

fn all_running(nodes: &[NodeRecord]) -> bool {
    !nodes.is_empty() && nodes.iter().all(|n| n.child.is_some())
}

fn teardown_existing(state: &mut State) {
    for node in state.nodes.drain(..) {
        if let Some(mut child) = node.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    state.last_config = None;
}

async fn wait_one_ready(url: &str, timeout: Duration) -> Result<(), ProvisionerError> {
    let start = Instant::now();
    loop {
        match client::NodeClient::connect_url(url).await {
            Ok(_) => return Ok(()),
            Err(_) if start.elapsed() < timeout => {
                sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
                return Err(ProvisionerError::ProvisionFailed(format!(
                    "node {url} did not become ready within {:?}: {e}",
                    timeout
                )));
            }
        }
    }
}

async fn wait_all_ready(urls: &[String], timeout: Duration) -> Result<(), ProvisionerError> {
    for url in urls {
        wait_one_ready(url, timeout).await?;
    }
    Ok(())
}

/// Render a single node's `config.toml` against the full peer list.
/// Always emits a `[cluster]` block — even single-node clusters take
/// the clustered code path so `is_leader` and `current_leader_index`
/// behave consistently.
fn render_config_toml(node: &NodePlan, all: &[NodePlan], cluster: &ClusterConfig) -> String {
    let mut peers = String::new();
    for p in all {
        peers.push_str(&format!(
            "\n[[cluster.peers]]\npeer_id = {pid}\nhost = \"http://{host}\"\n",
            pid = p.node_id,
            host = p.cluster_addr,
        ));
    }

    // Defaults sane enough that a `ClusterConfig::default()` (all
    // zeros) doesn't produce an unbootable cluster. Real callers
    // pass non-zero values; this just keeps tests honest.
    let max_accounts = if cluster.max_accounts > 0 {
        cluster.max_accounts
    } else {
        1_000_000
    };
    let tx_per_seg = if cluster.transaction_count_per_segment > 0 {
        cluster.transaction_count_per_segment
    } else {
        1_000_000
    };
    let snap_freq = if cluster.snapshot_frequency > 0 {
        cluster.snapshot_frequency
    } else {
        2
    };
    let repl_poll = if cluster.replication_poll_ms > 0 {
        cluster.replication_poll_ms
    } else {
        5
    };
    let ae_max = if cluster.append_entries_max_bytes > 0 {
        cluster.append_entries_max_bytes
    } else {
        4 * 1024 * 1024
    };

    format!(
        r#"[server]
host = "{client_host}"
port = {client_port}
max_connections = 1000
max_message_size_bytes = {ae_max}

[cluster]
replication_poll_ms = {repl_poll}
append_entries_max_bytes = {ae_max}

[cluster.node]
node_id = {self_id}
host = "{cluster_host}"
port = {cluster_port}
{peers}
[ledger]
max_accounts = {max_accounts}
wait_strategy = "balanced"

[ledger.storage]
data_dir = "{data_dir}"
transaction_count_per_segment = {tx_per_seg}
snapshot_frequency = {snap_freq}
"#,
        client_host = node.client_addr.ip(),
        client_port = node.client_addr.port(),
        self_id = node.node_id,
        cluster_host = node.cluster_addr.ip(),
        cluster_port = node.cluster_addr.port(),
        data_dir = node.data_dir.display(),
    )
}
