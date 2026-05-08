//! Long-lived handle to the live cluster.
//!
//! The control server owns one `ClusterHandle` for the duration of the
//! process. It wraps a single [`ProcessProvisioner`] (the same one the
//! standalone scenario CLI uses) and the [`ClusterClient`] currently
//! pointed at the cluster, so RPCs and the snapshot poller can grab the
//! live client without any locking on the read path.
//!
//! Reads use [`arc_swap::ArcSwap::load_full`] — atomic load + Arc
//! clone. Writes (bootstrap, reprovision) are atomic swaps.
//! Concurrent RPCs that already grabbed an `Arc<ClusterClient>` keep
//! using it; their tonic Channels either complete on the old cluster
//! or fail with connection-refused once the old children are killed.
//! Long-running scenarios that hold the client across a reprovision
//! will fail — that's intentional for the demo.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use client::ClusterClient;
use proto::control::ClusterConfig;
use thiserror::Error;
use tracing::{info, warn};

use crate::provisioner::process::ProcessProvisioner;
use crate::provisioner::{ProvisionConfig, Provisioner, ProvisionerError};

/// What can go wrong when bringing up or reshaping the cluster.
#[derive(Debug, Error)]
pub enum HandleError {
    #[error("provision: {0}")]
    Provision(#[from] ProvisionerError),
    #[error("connect: {0}")]
    Connect(String),
    #[error("invalid: {0}")]
    Invalid(String),
}

pub struct ClusterHandle {
    /// Singleton process provisioner — kills children on drop via
    /// [`ProcessProvisioner`]'s existing `Drop` impl.
    provisioner: Arc<ProcessProvisioner>,

    /// Current connected client. Lock-free read on every RPC; replaced
    /// atomically on `reprovision`.
    client: ArcSwap<ClusterClient>,

    /// Current ledger/replication config the cluster was provisioned
    /// with. `GetClusterConfig` reads this; `UpdateClusterConfig`
    /// rebuilds the cluster with a new value.
    config: ArcSwap<ClusterConfig>,

    /// gRPC client URLs in stable index order, as returned by the
    /// provisioner. `node_id = idx + 1`. RPCs that take a `node_id`
    /// translate via this list.
    node_addrs: ArcSwap<Vec<String>>,

    /// How many nodes the cluster currently has.
    node_count: AtomicU32,

    /// Wall-clock epoch-ms when the handle bootstrapped. Used by
    /// `GetServerInfo` so the UI can show uptime.
    bootstrap_epoch_ms: i64,

    /// Monotonic instant of bootstrap, for relative timing.
    bootstrap_at: Instant,
}

impl ClusterHandle {
    /// Provision a fresh cluster and connect to it. Returns `Ok(Self)`
    /// only after both succeed — there is no half-built handle state.
    pub async fn bootstrap(
        server_bin: std::path::PathBuf,
        config: ClusterConfig,
        node_count: u32,
    ) -> Result<Arc<Self>, HandleError> {
        let provisioner = Arc::new(ProcessProvisioner::new(server_bin).quiet(true));
        // Disambiguate the trait-method call so rustc doesn't try the
        // `From<Vec<T>> for Arc<[T]>` coercion when looking up the
        // return type.
        let urls: Vec<String> = Provisioner::provision(
            provisioner.as_ref(),
            &ProvisionConfig {
                node_count,
                cluster: config.clone(),
            },
        )
        .await?;
        let cc = ClusterClient::connect(&urls)
            .await
            .map_err(|e| HandleError::Connect(e.to_string()))?;

        info!(node_count, "cluster up: {}", urls.join(", "));

        let urls_arc: Arc<Vec<String>> = Arc::new(urls);
        Ok(Arc::new(Self {
            provisioner,
            client: ArcSwap::from(Arc::new(cc)),
            config: ArcSwap::from(Arc::new(config)),
            node_addrs: ArcSwap::from(urls_arc),
            node_count: AtomicU32::new(node_count),
            bootstrap_epoch_ms: epoch_ms_now(),
            bootstrap_at: Instant::now(),
        }))
    }

    /// Lock-free read — current cluster client. Cheap clone of an Arc.
    pub fn client(&self) -> Arc<ClusterClient> {
        self.client.load_full()
    }

    /// The provisioner. Used by the scenario runner so fault steps
    /// (KillNode, etc.) hit the same provisioner that owns the cluster.
    pub fn provisioner(&self) -> Arc<dyn Provisioner> {
        // Coerce Arc<ProcessProvisioner> → Arc<dyn Provisioner>.
        self.provisioner.clone() as Arc<dyn Provisioner>
    }

    /// Borrow the concrete `ProcessProvisioner` for direct fault
    /// methods (matches the `Provisioner` trait surface but without
    /// the `dyn` indirection — saves a vtable hop on the hot fault
    /// path that the UI hits).
    pub fn process_provisioner(&self) -> Arc<ProcessProvisioner> {
        self.provisioner.clone()
    }

    /// Snapshot of the active `ClusterConfig`. Cheap.
    pub fn config(&self) -> Arc<ClusterConfig> {
        self.config.load_full()
    }

    /// Snapshot of node URLs in stable index order.
    pub fn node_addrs(&self) -> Arc<Vec<String>> {
        self.node_addrs.load_full()
    }

    pub fn node_count(&self) -> u32 {
        self.node_count.load(Ordering::Acquire)
    }

    pub fn bootstrap_epoch_ms(&self) -> i64 {
        self.bootstrap_epoch_ms
    }

    pub fn uptime(&self) -> std::time::Duration {
        self.bootstrap_at.elapsed()
    }

    /// Map a proto `node_id` (1-based) to a provisioner index (0-based).
    /// Returns `None` for ids that don't exist in the current cluster.
    pub fn idx_for_node_id(&self, node_id: u64) -> Option<usize> {
        let count = self.node_count() as u64;
        if node_id == 0 || node_id > count {
            None
        } else {
            Some((node_id - 1) as usize)
        }
    }

    /// Tear down + reprovision the cluster with new config and/or node
    /// count. Per the design decision: this is destructive — ledger
    /// state is lost. The old `ClusterClient` Arc remains valid (and
    /// useless — its tonic Channels point at killed children) until
    /// the new client is published, at which point it is dropped by
    /// the swap.
    ///
    /// On failure the old client Arc stays in the swap. New RPCs will
    /// keep getting connection-refused (the children are gone), but
    /// the handle itself is still alive and `reprovision` can be
    /// retried with a different config.
    pub async fn reprovision(
        &self,
        new_config: Option<ClusterConfig>,
        new_count: Option<u32>,
    ) -> Result<(), HandleError> {
        let cfg = new_config
            .unwrap_or_else(|| (*self.config.load_full()).clone());
        let count = new_count.unwrap_or_else(|| self.node_count());
        if count == 0 {
            return Err(HandleError::Invalid("node_count must be >= 1".into()));
        }
        let urls: Vec<String> = Provisioner::provision(
            self.provisioner.as_ref(),
            &ProvisionConfig {
                node_count: count,
                cluster: cfg.clone(),
            },
        )
        .await?;
        let cc = ClusterClient::connect(&urls)
            .await
            .map_err(|e| HandleError::Connect(e.to_string()))?;
        let urls_arc: Arc<Vec<String>> = Arc::new(urls);
        // All-or-nothing publish.
        self.client.store(Arc::new(cc));
        self.config.store(Arc::new(cfg));
        self.node_addrs.store(urls_arc);
        self.node_count.store(count, Ordering::Release);
        info!(
            node_count = count,
            "cluster reprovisioned"
        );
        Ok(())
    }
}

impl Drop for ClusterHandle {
    fn drop(&mut self) {
        // ProcessProvisioner's own Drop kills child processes and
        // removes the temp dir when the last Arc is released.
        warn!("ClusterHandle dropping; provisioner teardown follows");
    }
}

fn epoch_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
