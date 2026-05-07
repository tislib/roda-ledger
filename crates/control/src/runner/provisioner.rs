//! Provisioner trait — the cluster-lifecycle and fault-injection
//! surface the scenario runner targets.
//!
//! The provisioner is responsible for *bringing the cluster into
//! existence and breaking it on demand*. It is not responsible for
//! ledger-level operations: writes, reads, and waits go through
//! [`client::ClusterClient`] which the runner builds on top of the
//! addresses returned by [`Provisioner::provision`].
//!
//! Provisioners can be backed by anything — an in-process cluster
//! spawned via `cluster-test-utils` for CI, a Docker swarm for local
//! integration runs, a long-running pre-provisioned cluster the
//! provisioner just hands out addresses to, or the existing
//! `roda-server` binary spawned as a child process. The runner does
//! not care which.
//!
//! Node identity is by *index* in the address list returned by
//! `provision`. The runner is responsible for mapping
//! `NodeSelector::Leader` (and friends) to a concrete index via the
//! `ClusterClient` before calling fault methods.
//!
//! ## Lifecycle (RAII teardown)
//!
//! There is no explicit `destroy()`. Cleanup happens when the
//! provisioner value is dropped — `impl Drop` on the concrete type is
//! the contract. Implementations that need to kill child processes,
//! free Docker containers, etc. do so in `Drop`. Drop must be
//! cooperative (e.g. block on a tokio runtime if needed) and must
//! never abort tasks abruptly.
//!
//! `provision` is idempotent and reconciliation-style: callers may
//! invoke it more than once on the same provisioner with different
//! [`ProvisionConfig`]s. Implementations compare the requested config
//! against current cluster state and apply only the diff (resize,
//! reconfigure, etc.) rather than tearing down and rebuilding.

use async_trait::async_trait;

use proto::control::ClusterConfig;

/// What a provisioner can do beyond the always-on lifecycle / graceful
/// shutdown / network-up baseline. Implementations declare these up
/// front so the runner can refuse a scenario before provisioning when
/// it would inevitably fail mid-way.
///
/// Mirrors `proto::control::Capability` so the same flags can be
/// surfaced to the UI via `GetServerInfo`. Add a field here when a new
/// optional fault is introduced.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// Abrupt termination (SIGKILL-equivalent). Implementations
    /// without process-level control over the cluster may not be
    /// able to do this; graceful `stop_node` is always available.
    pub kill: bool,
    /// Drop the network link between two specific nodes (and heal it
    /// later). Requires OS-level network control or a Docker-like
    /// substrate; not available against pre-provisioned remote
    /// clusters the provisioner doesn't fully own.
    pub network_partition: bool,
}

impl Capabilities {
    /// All optional capabilities enabled. Convenient for in-process
    /// test provisioners that fully own the cluster.
    pub fn all() -> Self {
        Self {
            kill: true,
            network_partition: true,
        }
    }

    /// No optional capabilities. Convenient for stubs / mocks.
    pub fn none() -> Self {
        Self::default()
    }
}

/// Desired cluster shape + cluster-wide ledger configuration. Passed
/// to [`Provisioner::provision`]; implementations reconcile the live
/// cluster against this on every call.
///
/// `cluster` mirrors `proto::control::ClusterConfig` field-for-field
/// so the same value can flow from `UpdateClusterConfig` requests
/// through the runner without a translation layer. `node_count` is
/// kept separate because `ClusterConfig` does not carry membership
/// shape — that lives in `ClusterMembership` proto-side.
#[derive(Clone, Debug, PartialEq)]
pub struct ProvisionConfig {
    /// Desired number of nodes. Reconciliation: implementations
    /// add/remove nodes to match.
    pub node_count: u32,
    /// Cluster-wide ledger / replication settings. Reconciliation:
    /// implementations apply the diff against current settings.
    pub cluster: ClusterConfig,
}

impl Default for ProvisionConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            cluster: ClusterConfig::default(),
        }
    }
}

/// What can go wrong on the lifecycle / fault side.
#[derive(Debug, thiserror::Error)]
pub enum ProvisionerError {
    #[error("provision failed: {0}")]
    ProvisionFailed(String),

    #[error("node index {0} out of bounds (cluster has {1} nodes)")]
    NodeIndexOutOfBounds(usize, usize),

    #[error("provisioner does not implement {0}")]
    Unimplemented(&'static str),

    #[error("provisioner error: {0}")]
    Other(String),
}

/// Provision a cluster and inject faults on it.
///
/// Implementations are async and `Send + Sync` so the runner can
/// share `Arc<dyn Provisioner>` across spawned async-branch tasks.
/// `async-trait` is used so the trait can be held behind `dyn`.
///
/// Teardown is via `Drop` on the concrete type — see the module-level
/// docs. There is no `destroy()` method by design.
#[async_trait]
pub trait Provisioner: Send + Sync {
    /// Capabilities this provisioner exposes beyond the always-on
    /// baseline (provision, graceful stop/start/restart). The runner
    /// inspects this before provisioning to fail fast on scenarios
    /// that would hit unsupported faults.
    fn capabilities(&self) -> Capabilities;

    // ---- Lifecycle ----

    /// Reconcile the live cluster against `config` and return the
    /// gRPC URLs of every node in stable index order. Idempotent:
    /// calling repeatedly with the same config is a no-op; calling
    /// with a different config applies the diff. Implementations
    /// decide whether changes can be applied online (e.g. node count
    /// adjustment) or require a brief unhealthy window.
    async fn provision(
        &self,
        config: &ProvisionConfig,
    ) -> Result<Vec<String>, ProvisionerError>;

    // ---- Per-node faults (idx into provision()'s result) ----

    async fn stop_node(&self, idx: usize) -> Result<(), ProvisionerError>;
    async fn kill_node(&self, idx: usize) -> Result<(), ProvisionerError>;
    async fn start_node(&self, idx: usize) -> Result<(), ProvisionerError>;
    async fn restart_node(&self, idx: usize) -> Result<(), ProvisionerError>;

    // ---- Network faults ----

    async fn partition_pair(&self, a: usize, b: usize) -> Result<(), ProvisionerError>;
    async fn heal_partition(&self, a: usize, b: usize) -> Result<(), ProvisionerError>;
}
