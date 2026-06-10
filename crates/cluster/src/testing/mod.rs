#![allow(clippy::result_large_err)]
//! High-level test harness. `ClusterTestingControl` owns per-node
//! data dirs, ports, configs, and the running `ClusterNode`s.
//!
//! Two deployment shapes are supported via [`ClusterTestingMode`]:
//! - [`ClusterTestingMode::Standalone`] — single node, no `[cluster]`
//!   block, role pinned to Leader, only the client-facing Ledger
//!   gRPC.
//! - [`ClusterTestingMode::Cluster`] — `n`-node cluster with
//!   election + replication.
//!
//! All test interaction with the cluster goes through gRPC: the
//! ledger service via `client::ClusterClient` / `client::NodeClient`
//! and, where the harness needs to identify the leader, the peer
//! node service (`proto::node::Ping`). No test code reaches into
//! in-process internals like `Arc<Ledger>`, `Arc<Term>`, or
//! `LedgerHandler`.
//!
//! Data-directory ownership is *cluster-level*: when
//! `config.data_dir_root` is `None`, the harness creates
//! `temp_cluster_<label>_<nanos>/`, puts each node's ledger under
//! `<root>/<i>/`, and removes the root in `Drop`. When the caller
//! supplies a `data_dir_root`, the harness uses it as-is and does
//! **not** remove it on drop.

use crate::{ClusterNode, ClusterNodeSection, ClusterSection, Config, PeerConfig, ServerSection};
use ::proto::node::{self as nproto, node_client::NodeClient as ProtoNodeClient};
use client::{ClusterClient, NodeClient, PipelineIndex, RetryConfig, SubmitResult};
use ledger::config::LedgerConfig;
use ledger::wait_strategy::WaitStrategy;
use proto::ledger as lproto;
use spdlog::{Level, info, warn};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use storage::StorageConfig;
use tokio::sync::Mutex;
use tokio::time::sleep;

// ── Errors ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ClusterTestingError {
    Build(std::io::Error),
    Run(String),
    NoLeader {
        timeout: Duration,
    },
    TwoLeaders {
        ids: Vec<u64>,
    },
    TcpBindTimeout(String),
    Connect(tonic::transport::Error),
    OutOfRange {
        which: &'static str,
        idx: usize,
        len: usize,
    },
    NotStarted {
        idx: usize,
    },
    NoFollower,
    /// [`ClusterTestingControl::wait_for`] timed out without the
    /// predicate ever returning `true`.
    WaitTimeout {
        label: String,
        timeout: Duration,
    },
    /// `ensure_caught_up_at` did not observe the slot's `snapshot`
    /// watermark reach the harness's `last_tx_id` within the budget.
    /// `last_observed` is the most recent `PipelineIndex` we read.
    CatchUpTimedOut {
        slot: usize,
        target: u64,
        last_observed: PipelineIndex,
    },
    /// A `require_*` call's underlying RPC failed.
    Rpc {
        op: &'static str,
        source: tonic::Status,
    },
    /// Embedded `ClusterClient` hasn't been built yet — call
    /// `start_node` / `wait_for_bind` to bring up at least one slot
    /// (or use `autostart = true`, the default).
    ClientNotReady,
}

impl std::fmt::Display for ClusterTestingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Build(e) => write!(f, "ClusterNode::new failed: {e}"),
            Self::Run(s) => write!(f, "ClusterNode::run failed: {s}"),
            Self::NoLeader { timeout } => {
                write!(f, "no unique leader observed within {timeout:?}")
            }
            Self::TwoLeaders { ids } => {
                write!(f, "two simultaneous leaders observed: {ids:?}")
            }
            Self::TcpBindTimeout(addr) => write!(f, "tcp {addr} never bound"),
            Self::Connect(e) => write!(f, "gRPC connect failed: {e}"),
            Self::OutOfRange { which, idx, len } => {
                write!(f, "{which} index {idx} out of range (len={len})")
            }
            Self::NotStarted { idx } => write!(f, "node {idx} is not started"),
            Self::NoFollower => write!(f, "no follower available"),
            Self::WaitTimeout { label, timeout } => {
                write!(f, "timed out after {timeout:?} waiting for: {label}")
            }
            Self::CatchUpTimedOut {
                slot,
                target,
                last_observed,
            } => write!(
                f,
                "slot {slot} did not catch up to last_tx_id={target}: \
                 last observed pipeline_index = compute={} commit={} snapshot={} \
                 cluster_commit={} is_leader={}",
                last_observed.compute,
                last_observed.commit,
                last_observed.snapshot,
                last_observed.cluster_commit,
                last_observed.is_leader,
            ),
            Self::Rpc { op, source } => write!(f, "{op} RPC failed: {source}"),
            Self::ClientNotReady => write!(
                f,
                "ClusterClient not built yet — use autostart=true or call \
                 wait_for_bind after starting a node manually"
            ),
        }
    }
}

impl std::error::Error for ClusterTestingError {}

// ── Mode + Config ─────────────────────────────────────────────────────────

/// Deployment shape selector for the harness.
#[derive(Clone, Debug)]
pub enum ClusterTestingMode {
    /// `Config { cluster: None, .. }`. Single node, role pinned to
    /// Leader, only the client-facing Ledger gRPC.
    Standalone,
    /// `Config { cluster: Some(_), .. }` with `nodes` real members
    /// in the symmetric peer list. Election + replication enabled.
    Cluster { nodes: usize },
}

/// Knobs for [`ClusterTestingControl`]. Use [`Self::default`] for a
/// 1-node cluster, [`Self::standalone`] / [`Self::cluster`] for the
/// other shapes. All fields are public so callers can override
/// individual knobs with struct-update syntax:
/// `ClusterTestingConfig { label: "x".into(), ..Default::default() }`.
#[derive(Clone, Debug)]
pub struct ClusterTestingConfig {
    pub mode: ClusterTestingMode,
    /// Additional peers listed in the symmetric peer list but never
    /// started. Useful for tests that want a multi-node-shaped
    /// cluster with only a subset of real processes
    /// (`divergence_reseed_test`). Cluster-mode-only.
    pub phantom_peer_count: usize,
    /// Label embedded in the temp data dir name + log lines.
    pub label: String,
    pub replication_poll_ms: u64,
    pub append_entries_max_bytes: usize,
    pub transaction_count_per_segment: u64,
    pub snapshot_frequency: u32,
    pub ledger_log_level: Level,
    /// If `false`, [`ClusterTestingControl::start`] only assembles
    /// configs and reserves ports — it does not call
    /// `ClusterNode::new` or `node.start()`. Callers can then drive
    /// each node manually with [`ClusterTestingControl::start_node`].
    pub autostart: bool,
    /// If `Some`, use this absolute path as the cluster's root data
    /// directory and **do not remove it on `Drop`** — the caller
    /// owns it. If `None`, the harness creates
    /// `temp_cluster_<label>_<nanos>/` under `current_dir()` and
    /// removes it in `Drop`.
    pub data_dir_root: Option<PathBuf>,
    pub retry_config: RetryConfig,
    /// Pre-open this many accounts (ids `1..=N`) once the leader settles, so
    /// tests can deposit without an explicit `OpenAccount` (ADR-022 existence
    /// enforcement). `0` disables it (for tests that assert exact tx_ids).
    pub auto_open_accounts: u32,
}

impl Default for ClusterTestingConfig {
    /// 1-node cluster (cluster block with one self-peer), autostart,
    /// auto temp dir cleanup.
    fn default() -> Self {
        Self {
            mode: ClusterTestingMode::Cluster { nodes: 1 },
            phantom_peer_count: 0,
            label: "cluster".to_string(),
            replication_poll_ms: 5,
            append_entries_max_bytes: 256 * 1024,
            transaction_count_per_segment: 10_000,
            snapshot_frequency: 2,
            ledger_log_level: Level::Debug,
            autostart: true,
            data_dir_root: None,
            retry_config: RetryConfig::default(),
            auto_open_accounts: 1024,
        }
    }
}

impl ClusterTestingConfig {
    /// Standalone (no `[cluster]` block): single node, role pinned
    /// to Leader, only the client-facing Ledger gRPC.
    pub fn standalone() -> Self {
        Self {
            mode: ClusterTestingMode::Standalone,
            label: "standalone".to_string(),
            ..Default::default()
        }
    }

    /// `n`-node cluster with election + replication.
    pub fn cluster(nodes: usize) -> Self {
        Self {
            mode: ClusterTestingMode::Cluster { nodes },
            label: "cluster".to_string(),
            ..Default::default()
        }
    }
}

// ── Internal types ────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct NodeAddr {
    node_id: u64,
    host: String,
    client_port: u16,
    /// Peer-facing Node gRPC port. `0` (unused) in standalone mode.
    node_port: u16,
}

struct NodeSlot {
    addr: NodeAddr,
    config: Config,
    /// `<root>/<i>/` — the per-node data dir. Identical to
    /// `config.ledger.storage.data_dir` but kept as a `PathBuf` for
    /// ergonomic test access.
    data_dir: PathBuf,
    // `Some` once `start_node` has built and run the node; dropping
    // it triggers cooperative shutdown via `ClusterNode::Drop`.
    node: Option<ClusterNode>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct PhantomPeer {
    peer_id: u64,
    node_port: u16,
}

// ── Top-level harness ─────────────────────────────────────────────────────

/// High-level harness that owns an in-process cluster end-to-end:
/// per-node configs, ports, data dirs, and the running
/// `ClusterNode`s.
pub struct ClusterTestingControl {
    mode: ClusterTestingMode,
    root_data_dir: PathBuf,
    /// True iff the harness created `root_data_dir` itself (and so
    /// should remove it on `Drop`). False when the caller supplied
    /// a pre-existing `data_dir_root`.
    harness_owns_root_dir: bool,
    slots: Vec<NodeSlot>,
    #[allow(dead_code)]
    phantom_peers: Vec<PhantomPeer>,
    cached_leader_idx: Arc<Mutex<Option<usize>>>,
    /// The cluster-aware [`ClusterClient`] for tests. Built once in
    /// [`Self::start`] (when autostart=true) against every node's
    /// client port. `None` in `autostart = false` flows where no
    /// servers have bound yet. All test code that needs to talk to
    /// the cluster goes through this client.
    cluster_client: Option<ClusterClient>,
    /// Highest tx_id any submit through this harness has produced.
    /// Bumped via `fetch_max` on every successful `*_and_wait` /
    /// `submit_*` call so concurrent submits never regress the
    /// watermark. Used by [`Self::ensure_caught_up_at`] as the catch-
    /// up target for `require_*` reads.
    last_tx_id: Arc<AtomicU64>,
    retry_config: RetryConfig,
}

impl ClusterTestingControl {
    /// Upper bound on how long `ensure_caught_up_at` will wait for a
    /// node's snapshot watermark to reach the harness's
    /// `last_tx_id`. Generous enough to cover post-restart election
    /// and replication windows; if a real test needs more, that's a
    /// signal something is wrong, not a knob to tune.
    pub const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(15);

    /// Build a harness from a config. Unless
    /// [`ClusterTestingConfig::autostart`] is `false`, every slot's
    /// servers are brought up before returning.
    pub async fn start(config: ClusterTestingConfig) -> Result<Self, ClusterTestingError> {
        // 1) Resolve root data dir.
        let (root_data_dir, harness_owns_root_dir) = match config.data_dir_root {
            Some(p) => (p, false),
            None => (make_root_tmp_dir(&config.label), true),
        };
        std::fs::create_dir_all(&root_data_dir).map_err(ClusterTestingError::Build)?;

        // 2) Per-mode slot count + per-slot port allocation.
        let real_node_count = match config.mode {
            ClusterTestingMode::Standalone => 1,
            ClusterTestingMode::Cluster { nodes } => nodes,
        };
        let addrs: Vec<NodeAddr> = (0..real_node_count)
            .map(|i| {
                let (client_port, node_port) = match config.mode {
                    ClusterTestingMode::Standalone => (free_port(), 0),
                    ClusterTestingMode::Cluster { .. } => (free_port(), free_port()),
                };
                NodeAddr {
                    node_id: (i as u64) + 1,
                    host: "127.0.0.1".to_string(),
                    client_port,
                    node_port,
                }
            })
            .collect();

        // 3) For cluster mode, build the symmetric peer list (real
        //    + phantom). Standalone skips this entirely.
        let phantom_peers: Vec<PhantomPeer> = match config.mode {
            ClusterTestingMode::Cluster { .. } => (0..config.phantom_peer_count)
                .map(|i| PhantomPeer {
                    peer_id: (real_node_count as u64) + (i as u64) + 1,
                    node_port: free_port(),
                })
                .collect(),
            _ => Vec::new(),
        };
        let mut all_peers: Vec<PeerConfig> = addrs
            .iter()
            .map(|a| PeerConfig {
                peer_id: a.node_id,
                host: format!("http://{}:{}", a.host, a.node_port),
            })
            .collect();
        for ph in &phantom_peers {
            all_peers.push(PeerConfig {
                peer_id: ph.peer_id,
                host: format!("http://127.0.0.1:{}", ph.node_port),
            });
        }

        // 4) Build per-node Configs + slot scaffolding.
        let mut slots: Vec<NodeSlot> = Vec::with_capacity(addrs.len());
        for (i, a) in addrs.iter().enumerate() {
            let mut data_dir = root_data_dir.clone();
            data_dir.push(i.to_string());

            let cluster_section = match config.mode {
                ClusterTestingMode::Standalone => None,
                ClusterTestingMode::Cluster { .. } => Some(ClusterSection {
                    node: ClusterNodeSection {
                        node_id: a.node_id,
                        host: a.host.clone(),
                        port: a.node_port,
                    },
                    peers: all_peers.clone(),
                    replication_poll_ms: config.replication_poll_ms,
                    append_entries_max_bytes: config.append_entries_max_bytes,
                }),
            };

            let cfg = Config {
                cluster: cluster_section,
                server: ServerSection {
                    host: a.host.clone(),
                    port: a.client_port,
                    ..Default::default()
                },
                ledger: LedgerConfig {
                    storage: StorageConfig {
                        data_dir: data_dir.to_string_lossy().into_owned(),
                        temporary: false,
                        transaction_count_per_segment: config.transaction_count_per_segment,
                        snapshot_frequency: config.snapshot_frequency,
                    },
                    wait_strategy: WaitStrategy::Balanced,
                    log_level: config.ledger_log_level,
                    seal_check_internal: Duration::from_millis(10),
                    ..LedgerConfig::default()
                },
            };

            slots.push(NodeSlot {
                addr: a.clone(),
                config: cfg,
                data_dir,
                node: None,
            });
        }

        let mut ctl = ClusterTestingControl {
            mode: config.mode,
            root_data_dir,
            harness_owns_root_dir,
            slots,
            phantom_peers,
            cached_leader_idx: Arc::new(Mutex::new(None)),
            cluster_client: None,
            last_tx_id: Arc::new(AtomicU64::new(0)),
            retry_config: config.retry_config.clone(),
        };

        // 5) Optionally start every slot, then wait for gRPC binds
        //    and open a ClusterClient against every running node.
        if config.autostart {
            ctl.start_all().await?;
            ctl.wait_for_all_tcp(Duration::from_secs(5)).await?;
            ctl.rebuild_cluster_client().await?;
            // Pre-open a block of accounts (ids 1..=N) so tests can deposit
            // without an explicit OpenAccount (replicated at ClusterCommit).
            ctl.open_accounts(config.auto_open_accounts).await?;
        }
        Ok(ctl)
    }

    /// Open `count` accounts (ids `1..=count`) on the current leader and wait
    /// for ClusterCommit, so subsequent deposits pass existence enforcement
    /// (ADR-022). `count == 0` is a no-op. Used by `start` for auto-open and by
    /// tests that disable auto-open but still need accounts.
    pub async fn open_accounts(&self, count: u32) -> Result<(), ClusterTestingError> {
        if count == 0 {
            return Ok(());
        }
        self.wait_for_leader(Duration::from_secs(5)).await?;
        self.client()
            .open_account_and_wait(count, 0, lproto::WaitLevel::ClusterCommit)
            .await
            .map_err(|e| ClusterTestingError::Run(format!("open_accounts: {e}")))?;
        Ok(())
    }

    /// (Re)build the embedded [`ClusterClient`] from every currently
    /// started slot's client URL. Called automatically inside
    /// [`Self::start`] for autostart flows; tests that bring nodes up
    /// manually (`autostart = false`) should call this once after the
    /// first round of `start_node` so [`Self::client`] becomes usable.
    async fn rebuild_cluster_client(&mut self) -> Result<(), ClusterTestingError> {
        let urls: Vec<String> = self
            .slots
            .iter()
            .filter(|s| s.node.is_some() && s.addr.client_port != 0)
            .map(|s| format!("http://127.0.0.1:{}", s.addr.client_port))
            .collect();
        if urls.is_empty() {
            self.cluster_client = None;
            return Ok(());
        }
        let cc = ClusterClient::connect_with_retry(&urls, self.retry_config.clone())
            .await
            .map_err(ClusterTestingError::Connect)?;
        self.cluster_client = Some(cc);
        Ok(())
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    /// Bring slot `i` up: builds (`ClusterNode::new`), starts
    /// (`ClusterNode::start`) the node, and waits for the gRPC
    /// servers to bind so the slot is immediately usable on return.
    /// Idempotent.
    pub async fn start_node(&mut self, i: usize) -> Result<(), ClusterTestingError> {
        {
            let cfg = self.slot(i)?.config.clone();
            if self.slot(i)?.node.is_none() {
                let node = ClusterNode::new(cfg).map_err(ClusterTestingError::Run)?;
                let node = node.run().map_err(ClusterTestingError::Run)?;
                self.slot_mut(i)?.node = Some(node);
            }
        }
        *self.cached_leader_idx.lock().await = None;
        self.wait_for_bind(i, Duration::from_secs(5)).await?;
        // Refresh the embedded cluster client now that this slot is
        // bound — covers the `autostart = false` flow where tests
        // bring nodes up manually and expect `ctl.client()` to "just
        // work" right after.
        self.rebuild_cluster_client().await?;
        Ok(())
    }

    /// Shut down node `i`'s spawned threads by dropping its
    /// `ClusterNode` — which cancels the cancellation token and
    /// joins all spawned threads — and waits for the OS to release
    /// the bound ports before returning. Safe to call on
    /// already-stopped slots.
    ///
    /// Port-release waits are bounded so a stuck task can't wedge the
    /// test harness. If a port doesn't release in time we log and
    /// continue rather than fail the call — the caller likely doesn't
    /// care about that specific port (it's either restarting on a new
    /// free port via `start_all`, or it's done with this slot).
    pub async fn stop_node(&mut self, i: usize) -> Result<(), ClusterTestingError> {
        let (node_opt, addr) = {
            let slot = self.slot_mut(i)?;
            let n = slot.node.take();
            (n, slot.addr.clone())
        };

        if let Some(n) = node_opt {
            // Dropping the `ClusterNode` cancels its `CancellationToken`
            // and joins every spawned thread (`ClusterNode::Drop`).
            drop(n);
            if addr.client_port != 0
                && let Err(e) = wait_for_tcp_release(addr.client_port, Duration::from_secs(2)).await
            {
                spdlog::warn!(
                    "stop_node({i}): client_port {} not released: {e}",
                    addr.client_port
                );
            }
            if addr.node_port != 0
                && let Err(e) = wait_for_tcp_release(addr.node_port, Duration::from_secs(2)).await
            {
                spdlog::warn!(
                    "stop_node({i}): node_port {} not released: {e}",
                    addr.node_port
                );
            }
        }

        if let Ok(mut g) = self.cached_leader_idx.try_lock() {
            *g = None;
        }
        Ok(())
    }

    pub async fn start_all(&mut self) -> Result<(), ClusterTestingError> {
        for i in 0..self.slots.len() {
            self.start_node(i).await?;
        }
        Ok(())
    }

    pub async fn stop_all(&mut self) {
        for i in 0..self.slots.len() {
            let _ = self.stop_node(i).await;
        }
    }

    // ── Per-index queries ────────────────────────────────────────────────

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn mode(&self) -> &ClusterTestingMode {
        &self.mode
    }

    pub fn node_id(&self, i: usize) -> Result<u64, ClusterTestingError> {
        Ok(self.slot(i)?.addr.node_id)
    }

    pub fn client_port(&self, i: usize) -> Result<u16, ClusterTestingError> {
        Ok(self.slot(i)?.addr.client_port)
    }

    /// Peer-facing Node gRPC port for slot `i`. Returns `0` for
    /// standalone mode (no Node gRPC binds).
    pub fn node_port(&self, i: usize) -> Result<u16, ClusterTestingError> {
        Ok(self.slot(i)?.addr.node_port)
    }

    /// Per-slot data directory (`<root>/<i>/`).
    pub fn data_dir(&self, i: usize) -> Result<&Path, ClusterTestingError> {
        Ok(self.slot(i)?.data_dir.as_path())
    }

    pub fn root_data_dir(&self) -> &Path {
        &self.root_data_dir
    }

    pub fn index_of(&self, node_id: u64) -> Option<usize> {
        self.slots.iter().position(|s| s.addr.node_id == node_id)
    }

    /// **Internal-only** accessor for the embedded `ClusterClient`.
    /// Tests must use the higher-level [`Self::deposit_and_wait`] /
    /// [`Self::require_balance`] / etc. APIs instead — those handle
    /// leader pinning, post-stop catch-up, and status-code interpretation
    /// uniformly. For negative-path RPCs that need raw client access
    /// (e.g. asserting a non-leader rejects a write), use
    /// [`Self::raw_client_for_slot`].
    fn client(&self) -> &ClusterClient {
        self.cluster_client.as_ref().expect(
            "ClusterTestingControl::client() called before the cluster client was built — \
             use autostart=true (the default) or call rebuild_cluster_client() after \
             starting nodes manually",
        )
    }

    // ── Leader resolution ────────────────────────────────────────────────

    /// Block until exactly one Leader is observed (or `timeout`
    /// elapses). In Standalone mode returns slot 0 immediately
    /// without polling.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<usize, ClusterTestingError> {
        if matches!(self.mode, ClusterTestingMode::Standalone) {
            if self.slots.is_empty() || !self.slot_started(&self.slots[0]) {
                return Err(ClusterTestingError::NotStarted { idx: 0 });
            }
            *self.cached_leader_idx.lock().await = Some(0);
            return Ok(0);
        }

        let deadline = Instant::now() + timeout;
        loop {
            let mut leaders: Vec<usize> = Vec::new();
            for (i, slot) in self.slots.iter().enumerate() {
                if slot.node.is_none() {
                    continue;
                }
                if let Some(nproto::NodeRole::Leader) = ping_role(slot.addr.node_port).await {
                    leaders.push(i);
                }
            }
            match leaders.len() {
                1 => {
                    let idx = leaders[0];
                    *self.cached_leader_idx.lock().await = Some(idx);
                    return Ok(idx);
                }
                n if n > 1 => {
                    warn!("More than one leader found: {:?}", leaders);
                }
                _ => {}
            }
            if Instant::now() >= deadline {
                return Err(ClusterTestingError::NoLeader { timeout });
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Returns the cached leader index if known, otherwise polls
    /// once with a 5 s default.
    pub async fn leader_index(&self) -> Result<usize, ClusterTestingError> {
        if let Some(i) = *self.cached_leader_idx.lock().await {
            return Ok(i);
        }
        self.wait_for_leader(Duration::from_secs(5)).await
    }

    pub async fn leader_node_id(&self) -> Result<u64, ClusterTestingError> {
        let i = self.leader_index().await?;
        self.node_id(i)
    }

    /// Index of an arbitrary follower (running, non-leader) slot.
    /// Errors with [`ClusterTestingError::NoFollower`] in single-node
    /// modes.
    pub async fn first_follower_index(&self) -> Result<usize, ClusterTestingError> {
        let leader = self.leader_index().await?;
        (0..self.slots.len())
            .find(|i| *i != leader && self.slot_started(&self.slots[*i]))
            .ok_or(ClusterTestingError::NoFollower)
    }

    // ── Polling helpers ──────────────────────────────────────────────────

    /// Poll `predicate` every 10 ms until it returns `true` or
    /// `timeout` elapses. The predicate is an async closure so it
    /// can drive client RPCs on each iteration.
    ///
    /// Tip: if the predicate captures a client, clone it inside the
    /// closure so each iteration owns its own handle:
    ///
    /// ```ignore
    /// let lc = ctl.client().leader().clone();
    /// ctl.wait_for(timeout, "leader commit", || {
    ///     let lc = lc.clone();
    ///     async move {
    ///         lc.get_pipeline_index().await
    ///             .map(|i| i.commit >= target).unwrap_or(false)
    ///     }
    /// }).await?;
    /// ```
    pub async fn wait_for<F, Fut>(
        &self,
        timeout: Duration,
        label: &str,
        mut predicate: F,
    ) -> Result<(), ClusterTestingError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = Instant::now() + timeout;
        loop {
            if predicate().await {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ClusterTestingError::WaitTimeout {
                    label: label.to_string(),
                    timeout,
                });
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Log a per-node table of pipeline progression watermarks
    /// (compute, commit, snapshot) followed by the cluster-wide
    /// `cluster_commit_index`. Useful for diagnosing where a
    /// stalled pipeline is stuck.
    ///
    /// Values are fetched per-node over gRPC via
    /// `ClusterClient::node(i)`. Never fails for individual node
    /// RPC errors — unreachable nodes show dashes so the rest of the
    /// matrix is still visible.
    pub async fn show_pipeline_matrix(&self) -> Result<(), ClusterTestingError> {
        info!(
            "{:<8} {:<26} {:<14} {:<13} {:<14}",
            "node_id", "node_url", "compute_index", "commit_index", "snapshot_index"
        );

        let mut cluster_commit: u64 = 0;
        for (i, slot) in self.slots.iter().enumerate() {
            let node_id = slot.addr.node_id;
            let url = format!("http://{}:{}", slot.addr.host, slot.addr.client_port);

            if slot.node.is_some() {
                match self.client().node(i).get_pipeline_index().await {
                    Ok(pi) => {
                        cluster_commit = cluster_commit.max(pi.cluster_commit);
                        info!(
                            "{:<8} {:<26} {:<14} {:<13} {:<14}",
                            node_id, url, pi.compute, pi.commit, pi.snapshot
                        );
                    }
                    Err(e) => {
                        info!(
                            "{:<8} {:<26} {:<14} {:<13} {:<14}  (rpc error: {})",
                            node_id, url, "—", "—", "—", e
                        );
                    }
                }
            } else {
                info!(
                    "{:<8} {:<26} {:<14} {:<13} {:<14}  (not started)",
                    node_id, url, "—", "—", "—"
                );
            }
        }

        info!("cluster_commit_index: {}", cluster_commit);
        Ok(())
    }

    // ── High-level test API: submit + require + catch-up ─────────────────
    //
    // These methods are the supported way for tests to drive the
    // cluster. They:
    // 1. Track the harness's `last_tx_id` watermark on every submit.
    // 2. Resolve the live leader on every leader-routed call (no
    //    stale cached-leader-handle hazard).
    // 3. Block reads until the responding node's snapshot watermark
    //    has caught up to `last_tx_id`, so a `require_*` assertion
    //    always reflects the cumulative effect of every prior submit.
    //
    // Tests should NOT bypass them by calling `client()` directly —
    // the leader cache, catch-up, and status-code interpretation are
    // all easy to get subtly wrong, and getting them wrong silently
    // hides cluster bugs as opaque "balance regressed" failures.

    /// The harness's watermark of the highest tx_id any submit has
    /// produced so far. Catch-up reads target this value.
    #[inline]
    pub fn last_tx_id(&self) -> u64 {
        self.last_tx_id.load(Ordering::Acquire)
    }

    /// Block until slot `slot`'s `snapshot` watermark reaches
    /// [`Self::last_tx_id`], or [`Self::CATCH_UP_TIMEOUT`] elapses.
    ///
    /// Gates on `snapshot_id` (not `commit_id`, not `cluster_commit`)
    /// because that's the watermark `GetBalance` reads against — by
    /// the time `snapshot >= last_tx_id`, every submit through this
    /// harness is observable in queries against this slot.
    ///
    /// Skips the wait (returns `Ok`) when the slot is not running —
    /// the responding read will fail naturally with a clearer error
    /// than a silent harness hang.
    pub async fn ensure_caught_up_at(&self, slot: usize) -> Result<(), ClusterTestingError> {
        let target = self.last_tx_id();
        if target == 0 {
            return Ok(());
        }
        let _ = self.slot(slot)?;
        if !self.slot_started(&self.slots[slot]) {
            return Ok(());
        }

        let client = self.cluster_client_or_err()?;
        let deadline = Instant::now() + Self::CATCH_UP_TIMEOUT;
        let mut last_observed: Option<PipelineIndex> = None;
        loop {
            // Re-check started state inside the loop so a slot that
            // gets stopped mid-wait short-circuits cleanly.
            if !self.slot_started(&self.slots[slot]) {
                return Ok(());
            }
            match client.node(slot).get_pipeline_index().await {
                Ok(idx) => {
                    if idx.snapshot >= target {
                        return Ok(());
                    }
                    last_observed = Some(idx);
                }
                Err(e) => {
                    // Transient RPC errors are normal during election
                    // windows; keep polling until the deadline.
                    if Instant::now() >= deadline {
                        return Err(ClusterTestingError::Rpc {
                            op: "get_pipeline_index (catch-up)",
                            source: e,
                        });
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err(ClusterTestingError::CatchUpTimedOut {
                    slot,
                    target,
                    last_observed: last_observed.unwrap_or(PipelineIndex {
                        compute: 0,
                        commit: 0,
                        snapshot: 0,
                        cluster_commit: 0,
                        term: 0,
                        is_leader: false,
                    }),
                });
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Resolve the live leader and ensure it has caught up to the
    /// harness's `last_tx_id`. Used internally by the no-suffix
    /// `require_*` variants.
    async fn ensure_caught_up_at_leader(&self) -> Result<usize, ClusterTestingError> {
        let leader = self.wait_for_leader(Duration::from_secs(10)).await?;
        self.ensure_caught_up_at(leader).await?;
        Ok(leader)
    }

    fn cluster_client_or_err(&self) -> Result<&ClusterClient, ClusterTestingError> {
        self.cluster_client
            .as_ref()
            .ok_or(ClusterTestingError::ClientNotReady)
    }

    /// Escape hatch: borrow slot `i`'s raw [`NodeClient`]. Tests that
    /// need negative-path RPC behavior (e.g. asserting a non-leader
    /// rejects a write with `FailedPrecondition`) use this to bypass
    /// the harness's leader-routing + catch-up. Prefer the
    /// `require_*` API for any positive-path assertion.
    pub fn raw_client_for_slot(&self, i: usize) -> Result<&NodeClient, ClusterTestingError> {
        let _ = self.slot(i)?;
        Ok(self.cluster_client_or_err()?.node(i))
    }

    // ── Submit wrappers (write through leader, bump last_tx_id) ──

    /// Bumps the harness's `last_tx_id` watermark from a known good
    /// tx_id (post-successful submit). Uses `fetch_max` so concurrent
    /// submits from multiple tasks never regress the watermark.
    #[inline]
    fn bump_last_tx_id(&self, tx_id: u64) {
        if tx_id > 0 {
            self.last_tx_id.fetch_max(tx_id, Ordering::Release);
        }
    }

    /// Submit a deposit (fire-and-forget). Routed to the live leader.
    /// Bumps `last_tx_id` on success.
    pub async fn deposit(
        &self,
        account: u64,
        amount: u64,
        user_ref: u64,
    ) -> Result<u64, ClusterTestingError> {
        let tx_id = self
            .cluster_client_or_err()?
            .deposit(account, amount, user_ref)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "deposit",
                source: e,
            })?;
        self.bump_last_tx_id(tx_id);
        Ok(tx_id)
    }

    /// Submit a deposit and wait for the requested `WaitLevel`.
    pub async fn deposit_and_wait(
        &self,
        account: u64,
        amount: u64,
        user_ref: u64,
        wait_level: lproto::WaitLevel,
    ) -> Result<SubmitResult, ClusterTestingError> {
        let r = self
            .cluster_client_or_err()?
            .deposit_and_wait(account, amount, user_ref, wait_level)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "deposit_and_wait",
                source: e,
            })?;
        self.bump_last_tx_id(r.tx_id);
        Ok(r)
    }

    /// Submit a deposit and wait for the requested `WaitLevel`.
    pub async fn deposit_and_wait_no_retry(
        &self,
        account: u64,
        amount: u64,
        user_ref: u64,
        wait_level: lproto::WaitLevel,
    ) -> Result<SubmitResult, ClusterTestingError> {
        let r = self
            .cluster_client_or_err()?
            .deposit_and_wait_no_retry(account, amount, user_ref, wait_level)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "deposit_and_wait",
                source: e,
            })?;
        self.bump_last_tx_id(r.tx_id);
        Ok(r)
    }

    /// Submit a transfer (fire-and-forget).
    pub async fn transfer(
        &self,
        from: u64,
        to: u64,
        amount: u64,
        user_ref: u64,
    ) -> Result<u64, ClusterTestingError> {
        let tx_id = self
            .cluster_client_or_err()?
            .transfer(from, to, amount, user_ref)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "transfer",
                source: e,
            })?;
        self.bump_last_tx_id(tx_id);
        Ok(tx_id)
    }

    /// Submit a transfer and wait for the requested `WaitLevel`.
    pub async fn transfer_and_wait(
        &self,
        from: u64,
        to: u64,
        amount: u64,
        user_ref: u64,
        wait_level: lproto::WaitLevel,
    ) -> Result<SubmitResult, ClusterTestingError> {
        let r = self
            .cluster_client_or_err()?
            .transfer_and_wait(from, to, amount, user_ref, wait_level)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "transfer_and_wait",
                source: e,
            })?;
        self.bump_last_tx_id(r.tx_id);
        Ok(r)
    }

    /// Submit a batch of deposits (fire-and-forget).
    pub async fn deposit_batch(
        &self,
        deposits: &[(u64, u64, u64)],
    ) -> Result<Vec<u64>, ClusterTestingError> {
        let tx_ids = self
            .cluster_client_or_err()?
            .deposit_batch(deposits)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "deposit_batch",
                source: e,
            })?;
        if let Some(&max) = tx_ids.iter().max() {
            self.bump_last_tx_id(max);
        }
        Ok(tx_ids)
    }

    /// Submit a batch of deposits and wait for the requested
    /// `WaitLevel` against the *last* element of the batch.
    pub async fn deposit_batch_and_wait(
        &self,
        deposits: &[(u64, u64, u64)],
        wait_level: lproto::WaitLevel,
    ) -> Result<Vec<SubmitResult>, ClusterTestingError> {
        let results = self
            .cluster_client_or_err()?
            .deposit_batch_and_wait(deposits, wait_level)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "deposit_batch_and_wait",
                source: e,
            })?;
        if let Some(max) = results.iter().map(|r| r.tx_id).max() {
            self.bump_last_tx_id(max);
        }
        Ok(results)
    }

    /// Submit a batch of transfers and wait.
    pub async fn transfer_batch_and_wait(
        &self,
        transfers: &[(u64, u64, u64)],
        wait_level: lproto::WaitLevel,
    ) -> Result<Vec<SubmitResult>, ClusterTestingError> {
        let results = self
            .cluster_client_or_err()?
            .transfer_batch_and_wait(transfers, wait_level)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "transfer_batch_and_wait",
                source: e,
            })?;
        if let Some(max) = results.iter().map(|r| r.tx_id).max() {
            self.bump_last_tx_id(max);
        }
        Ok(results)
    }

    // ── Require methods (catch up, read, assert) ─────────────────

    /// Resolve the live leader, ensure it has caught up to the
    /// harness's `last_tx_id`, then assert the account's balance
    /// equals `expected`. Panics on mismatch with a diagnostic
    /// message that includes the responding slot, `last_tx_id`, and
    /// pipeline index.
    pub async fn require_balance(&self, account: u64, expected: i64) {
        let slot = self
            .ensure_caught_up_at_leader()
            .await
            .unwrap_or_else(|e| panic!("require_balance({account}) catch-up: {e}"));
        self.assert_balance_at(slot, account, expected).await;
    }

    /// Pin to a specific slot. Catch up + read + assert.
    pub async fn require_balance_on(&self, slot: usize, account: u64, expected: i64) {
        self.ensure_caught_up_at(slot)
            .await
            .unwrap_or_else(|e| panic!("require_balance_on({slot}, {account}) catch-up: {e}"));
        self.assert_balance_at(slot, account, expected).await;
    }

    async fn assert_balance_at(&self, slot: usize, account: u64, expected: i64) {
        let client = self
            .cluster_client_or_err()
            .unwrap_or_else(|e| panic!("require_balance: {e}"));
        let bal = client
            .node(slot)
            .get_balance(account)
            .await
            .unwrap_or_else(|e| {
                panic!("require_balance: get_balance(account={account}, slot={slot}) failed: {e}")
            });
        if bal.balance != expected {
            let pi = client.node(slot).get_pipeline_index().await.ok();
            panic!(
                "require_balance(account={}, slot={}, last_tx_id={}): \
                 expected {}, got {} (last_snapshot_tx_id={}; pipeline_index={:?})",
                account,
                slot,
                self.last_tx_id(),
                expected,
                bal.balance,
                bal.last_snapshot_tx_id,
                pi,
            );
        }
    }

    /// Assert that the sum of `accounts`' balances on the live
    /// leader equals zero (the cluster-wide accounting invariant).
    /// Reads against the same node so balances are taken from a
    /// consistent snapshot.
    pub async fn require_zero_sum(&self, accounts: &[u64]) {
        let slot = self
            .ensure_caught_up_at_leader()
            .await
            .unwrap_or_else(|e| panic!("require_zero_sum catch-up: {e}"));
        let client = self
            .cluster_client_or_err()
            .unwrap_or_else(|e| panic!("require_zero_sum: {e}"));
        let mut total: i64 = 0;
        let mut per_account: Vec<(u64, i64)> = Vec::with_capacity(accounts.len());
        for &acct in accounts {
            let bal = client
                .node(slot)
                .get_balance(acct)
                .await
                .unwrap_or_else(|e| {
                    panic!("require_zero_sum: get_balance({acct}, slot={slot}) failed: {e}")
                });
            total = total.saturating_add(bal.balance);
            per_account.push((acct, bal.balance));
        }
        if total != 0 {
            panic!(
                "require_zero_sum(slot={}, last_tx_id={}): sum != 0; per-account = {:?}, sum = {}",
                slot,
                self.last_tx_id(),
                per_account,
                total,
            );
        }
    }

    /// Assert a transaction's status equals `expected_status` on the
    /// live leader (catching up first). For positive-path tests:
    /// pass `proto::TransactionStatus::Committed` to require that
    /// the tx has been durably persisted (Committed or higher would
    /// also satisfy the design intent — see
    /// [`Self::require_transaction_committed`] for that variant).
    pub async fn require_transaction_status(
        &self,
        tx_id: u64,
        expected_status: lproto::TransactionStatus,
        expected_fail_reason: u32,
    ) {
        let slot = self
            .ensure_caught_up_at_leader()
            .await
            .unwrap_or_else(|e| panic!("require_transaction_status({tx_id}) catch-up: {e}"));
        self.assert_transaction_status_at(slot, tx_id, expected_status, expected_fail_reason)
            .await;
    }

    /// Convenience: assert the tx is `Committed` or `OnSnapshot`
    /// (i.e. durable) with `fail_reason == 0`. This is the right
    /// check for "the cluster preserved this acked write".
    pub async fn require_transaction_committed(&self, tx_id: u64) {
        let slot = self
            .ensure_caught_up_at_leader()
            .await
            .unwrap_or_else(|e| panic!("require_transaction_committed({tx_id}) catch-up: {e}"));
        let client = self
            .cluster_client_or_err()
            .unwrap_or_else(|e| panic!("require_transaction_committed: {e}"));
        let (status, fail_reason) = client
            .node(slot)
            .get_transaction_status(tx_id)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "require_transaction_committed: get_transaction_status(tx_id={tx_id}, slot={slot}) failed: {e}"
                )
            });
        let committed = lproto::TransactionStatus::Committed as i32;
        let on_snapshot = lproto::TransactionStatus::OnSnapshot as i32;
        if status != committed && status != on_snapshot {
            let pi = client.node(slot).get_pipeline_index().await.ok();
            panic!(
                "require_transaction_committed(tx_id={}, slot={}, last_tx_id={}): \
                 expected status in {{COMMITTED({}), ON_SNAPSHOT({})}} with fail_reason=0, \
                 got status={} fail_reason={} (pipeline_index={:?})",
                tx_id,
                slot,
                self.last_tx_id(),
                committed,
                on_snapshot,
                status,
                fail_reason,
                pi,
            );
        }
        if fail_reason != 0 {
            panic!(
                "require_transaction_committed(tx_id={}, slot={}, last_tx_id={}): \
                 fail_reason={} (status={})",
                tx_id,
                slot,
                self.last_tx_id(),
                fail_reason,
                status,
            );
        }
    }

    async fn assert_transaction_status_at(
        &self,
        slot: usize,
        tx_id: u64,
        expected_status: lproto::TransactionStatus,
        expected_fail_reason: u32,
    ) {
        let client = self
            .cluster_client_or_err()
            .unwrap_or_else(|e| panic!("require_transaction_status: {e}"));
        let (status, fail_reason) = client
            .node(slot)
            .get_transaction_status(tx_id)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "require_transaction_status: get_transaction_status(tx_id={tx_id}, slot={slot}) failed: {e}"
                )
            });
        let expected_code = expected_status as i32;
        if status != expected_code || fail_reason != expected_fail_reason {
            let pi = client.node(slot).get_pipeline_index().await.ok();
            panic!(
                "require_transaction_status(tx_id={}, slot={}, last_tx_id={}): \
                 expected status={:?}({}) fail_reason={}, \
                 got status={} fail_reason={} (pipeline_index={:?})",
                tx_id,
                slot,
                self.last_tx_id(),
                expected_status,
                expected_code,
                expected_fail_reason,
                status,
                fail_reason,
                pi,
            );
        }
    }

    /// Read slot `slot`'s balance for `account` without catch-up
    /// or assertion. Useful for diagnostic logging or for tests
    /// that need to compare two nodes' balances directly. For
    /// positive-path assertions, prefer
    /// [`Self::require_balance`] / [`Self::require_balance_on`].
    pub async fn get_balance_on(
        &self,
        slot: usize,
        account: u64,
    ) -> Result<client::Balance, ClusterTestingError> {
        self.cluster_client_or_err()?
            .node(slot)
            .get_balance(account)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "get_balance",
                source: e,
            })
    }

    /// Read slot `slot`'s pipeline index without catch-up or
    /// assertion.
    pub async fn pipeline_index_on(
        &self,
        slot: usize,
    ) -> Result<PipelineIndex, ClusterTestingError> {
        self.cluster_client_or_err()?
            .node(slot)
            .get_pipeline_index()
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "get_pipeline_index",
                source: e,
            })
    }

    /// Read slot `slot`'s `(status, fail_reason)` for `tx_id`
    /// without catch-up or assertion.
    pub async fn get_transaction_status_on(
        &self,
        slot: usize,
        tx_id: u64,
    ) -> Result<(i32, u32), ClusterTestingError> {
        self.cluster_client_or_err()?
            .node(slot)
            .get_transaction_status(tx_id)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "get_transaction_status",
                source: e,
            })
    }

    /// Submit a WASM function call and wait. Routes to leader,
    /// bumps `last_tx_id`.
    pub async fn submit_function_and_wait(
        &self,
        name: &str,
        params: [i64; 8],
        user_ref: u64,
        wait_level: lproto::WaitLevel,
    ) -> Result<SubmitResult, ClusterTestingError> {
        let r = self
            .cluster_client_or_err()?
            .submit_function_and_wait(name, params, user_ref, wait_level)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "submit_function_and_wait",
                source: e,
            })?;
        self.bump_last_tx_id(r.tx_id);
        Ok(r)
    }

    /// Register a WASM function. Routes to leader.
    pub async fn register_function(
        &self,
        name: &str,
        binary: &[u8],
        override_existing: bool,
    ) -> Result<(u16, u32), ClusterTestingError> {
        self.cluster_client_or_err()?
            .register_function(name, binary, override_existing)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "register_function",
                source: e,
            })
    }

    /// Unregister a WASM function. Routes to leader.
    pub async fn unregister_function(&self, name: &str) -> Result<u16, ClusterTestingError> {
        self.cluster_client_or_err()?
            .unregister_function(name)
            .await
            .map_err(|e| ClusterTestingError::Rpc {
                op: "unregister_function",
                source: e,
            })
    }

    /// Block (with [`Self::CATCH_UP_TIMEOUT`]) until slot `slot`'s
    /// `commit` watermark settles at exactly `target`. Use this for
    /// observing **truncation** events (e.g. divergence reseed)
    /// where the watermark drops down to a known point — the
    /// `_at_least` variant returns immediately if commit is already
    /// above the target.
    pub async fn require_pipeline_commit_eq(&self, slot: usize, target: u64) {
        let deadline = Instant::now() + Self::CATCH_UP_TIMEOUT;
        loop {
            match self.pipeline_index_on(slot).await {
                Ok(idx) if idx.commit == target => return,
                Ok(idx) => {
                    if Instant::now() >= deadline {
                        panic!(
                            "require_pipeline_commit_eq(slot={}, target={}): \
                             commit settled at {} (pipeline_index={:?})",
                            slot, target, idx.commit, idx
                        );
                    }
                }
                Err(e) => {
                    if Instant::now() >= deadline {
                        panic!(
                            "require_pipeline_commit_eq(slot={}, target={}): RPC failure: {}",
                            slot, target, e
                        );
                    }
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Block (with [`Self::CATCH_UP_TIMEOUT`]) until slot `slot`'s
    /// `commit` watermark reaches `target`. Useful when a test
    /// needs to wait on a specific commit point that doesn't track
    /// the harness's `last_tx_id` (e.g. cross-node convergence
    /// checks where the target is one node's view of another's
    /// progress).
    pub async fn require_pipeline_commit_at_least(&self, slot: usize, target: u64) {
        let client = self
            .cluster_client_or_err()
            .unwrap_or_else(|e| panic!("require_pipeline_commit_at_least: {e}"));
        let deadline = Instant::now() + Self::CATCH_UP_TIMEOUT;
        loop {
            match client.node(slot).get_pipeline_index().await {
                Ok(idx) if idx.commit >= target => return,
                Ok(idx) => {
                    if Instant::now() >= deadline {
                        panic!(
                            "require_pipeline_commit_at_least(slot={}, target={}): \
                             commit stuck at {} (pipeline_index={:?})",
                            slot, target, idx.commit, idx
                        );
                    }
                }
                Err(e) => {
                    if Instant::now() >= deadline {
                        panic!(
                            "require_pipeline_commit_at_least(slot={}, target={}): \
                             RPC failure: {}",
                            slot, target, e
                        );
                    }
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Wait for slot `i`'s gRPC servers to bind. Useful after a
    /// manual [`Self::start_node`] call (`autostart = false` flow).
    pub async fn wait_for_bind(
        &mut self,
        i: usize,
        timeout: Duration,
    ) -> Result<(), ClusterTestingError> {
        let slot = self.slot(i)?;
        if slot.node.is_none() {
            return Err(ClusterTestingError::NotStarted { idx: i });
        }
        wait_for_tcp(format!("127.0.0.1:{}", slot.addr.client_port), timeout).await?;
        if !matches!(self.mode, ClusterTestingMode::Standalone) && slot.addr.node_port != 0 {
            wait_for_tcp(format!("127.0.0.1:{}", slot.addr.node_port), timeout).await?;
        }
        // The slot is now bound — refresh the embedded cluster client
        // so `client()` reflects the new node. Covers both the
        // `start()`-internal autostart loop and the
        // `autostart = false` flow where tests bring nodes up
        // manually via `start_node` + `wait_for_bind`.
        self.rebuild_cluster_client().await?;
        Ok(())
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    async fn wait_for_all_tcp(&self, timeout: Duration) -> Result<(), ClusterTestingError> {
        for slot in &self.slots {
            if slot.node.is_none() {
                continue;
            }
            wait_for_tcp(format!("127.0.0.1:{}", slot.addr.client_port), timeout).await?;
            if !matches!(self.mode, ClusterTestingMode::Standalone) && slot.addr.node_port != 0 {
                wait_for_tcp(format!("127.0.0.1:{}", slot.addr.node_port), timeout).await?;
            }
        }
        Ok(())
    }

    fn slot_started(&self, slot: &NodeSlot) -> bool {
        slot.node.is_some()
    }

    fn slot(&self, i: usize) -> Result<&NodeSlot, ClusterTestingError> {
        let len = self.slots.len();
        self.slots.get(i).ok_or(ClusterTestingError::OutOfRange {
            which: "node",
            idx: i,
            len,
        })
    }

    // ── Fault-injection API (gated by `fault-injection`) ─────────
    //
    // Tests reach the per-node `ClusterFaultInjector` directly —
    // no gRPC round-trip — through these helpers. The injector is
    // the same `Arc` the node's `FaultHandler` is backed by, so
    // either driving channel produces the same observable effect.

    /// Per-node fault injector handle for slot `i`. Errors if the
    /// slot is out of range or the node hasn't been started yet.
    #[cfg(feature = "fault-injection")]
    pub fn fault_injector(
        &self,
        i: usize,
    ) -> Result<std::sync::Arc<crate::fault::ClusterFaultInjector>, ClusterTestingError> {
        let slot = self.slot(i)?;
        let node = slot
            .node
            .as_ref()
            .ok_or(ClusterTestingError::NotStarted { idx: i })?;
        Ok(node.fault_injector())
    }

    /// Convenience: park `fdatasync` on slot `i` (the
    /// `DISK_ACCESS` bucket from ADR-018 — covers both term + vote
    /// log writes via the WAL syncer the storage layer shares).
    /// Returns `Ok(())` — DISK_ACCESS stuck doesn't need a
    /// per-call id because there's no other test handle in flight
    /// on this slot.
    #[cfg(feature = "fault-injection")]
    pub fn stick_disk_access(&self, i: usize) -> Result<(), ClusterTestingError> {
        use proto::fault::fault_level::Bucket;
        use proto::fault::{FaultLevel, FaultOutcome, Stuck, fault_outcome};
        let level = FaultLevel {
            bucket: Bucket::DiskAccess as i32,
            peer_id: 0,
        };
        let outcome = FaultOutcome {
            kind: Some(fault_outcome::Kind::Stuck(Stuck {
                stuck_id: String::new(),
            })),
        };
        self.fault_injector(i)?
            .set_fault(&level, &outcome)
            .map_err(|e| ClusterTestingError::Run(format!("set_fault disk_access: {e}")))?;
        Ok(())
    }

    /// Undo `stick_disk_access` on slot `i`. Idempotent.
    #[cfg(feature = "fault-injection")]
    pub fn unstick_disk_access(&self, i: usize) -> Result<(), ClusterTestingError> {
        use proto::fault::FaultLevel;
        use proto::fault::fault_level::Bucket;
        let level = FaultLevel {
            bucket: Bucket::DiskAccess as i32,
            peer_id: 0,
        };
        self.fault_injector(i)?.clear_fault(&level);
        Ok(())
    }

    /// Convenience: park *both* WAL `write_all` and `fdatasync` on
    /// slot `i` (the `WAL_ACCESS` bucket). Unlike `DISK_ACCESS`
    /// (sync only), this also blocks the WAL writer's append — so
    /// the leader can't even replicate the new bytes out to its
    /// peers. Use this when a test needs `cluster_commit` to stop
    /// advancing on a single-node fault, not just `commit`.
    #[cfg(feature = "fault-injection")]
    pub fn stick_wal_access(&self, i: usize) -> Result<(), ClusterTestingError> {
        use proto::fault::fault_level::Bucket;
        use proto::fault::{FaultLevel, FaultOutcome, Stuck, fault_outcome};
        let level = FaultLevel {
            bucket: Bucket::WalAccess as i32,
            peer_id: 0,
        };
        let outcome = FaultOutcome {
            kind: Some(fault_outcome::Kind::Stuck(Stuck {
                stuck_id: String::new(),
            })),
        };
        self.fault_injector(i)?
            .set_fault(&level, &outcome)
            .map_err(|e| ClusterTestingError::Run(format!("set_fault wal_access: {e}")))?;
        Ok(())
    }

    /// Undo `stick_wal_access` on slot `i`. Idempotent.
    #[cfg(feature = "fault-injection")]
    pub fn unstick_wal_access(&self, i: usize) -> Result<(), ClusterTestingError> {
        use proto::fault::FaultLevel;
        use proto::fault::fault_level::Bucket;
        let level = FaultLevel {
            bucket: Bucket::WalAccess as i32,
            peer_id: 0,
        };
        self.fault_injector(i)?.clear_fault(&level);
        Ok(())
    }

    /// Add a per-call sleep to the WAL writer on slot `i` (routes
    /// through `LedgerFaultInjector::set_write_delay`). Use `None` /
    /// `clear_all_faults` to release.
    #[cfg(feature = "fault-injection")]
    pub fn slow_wal_access(&self, i: usize, delay_ms: u64) -> Result<(), ClusterTestingError> {
        use proto::fault::fault_level::Bucket;
        use proto::fault::{FaultLevel, FaultOutcome, Slow, fault_outcome};
        let level = FaultLevel {
            bucket: Bucket::WalAccess as i32,
            peer_id: 0,
        };
        let outcome = FaultOutcome {
            kind: Some(fault_outcome::Kind::Slow(Slow { delay_ms })),
        };
        self.fault_injector(i)?
            .set_fault(&level, &outcome)
            .map_err(|e| ClusterTestingError::Run(format!("set_fault wal_access slow: {e}")))?;
        Ok(())
    }

    /// Add a per-call sleep to the WAL syncer on slot `i` (routes
    /// through `LedgerFaultInjector::set_sync_delay`).
    #[cfg(feature = "fault-injection")]
    pub fn slow_disk_access(&self, i: usize, delay_ms: u64) -> Result<(), ClusterTestingError> {
        use proto::fault::fault_level::Bucket;
        use proto::fault::{FaultLevel, FaultOutcome, Slow, fault_outcome};
        let level = FaultLevel {
            bucket: Bucket::DiskAccess as i32,
            peer_id: 0,
        };
        let outcome = FaultOutcome {
            kind: Some(fault_outcome::Kind::Slow(Slow { delay_ms })),
        };
        self.fault_injector(i)?
            .set_fault(&level, &outcome)
            .map_err(|e| ClusterTestingError::Run(format!("set_fault disk_access slow: {e}")))?;
        Ok(())
    }

    /// Wipe every active fault on slot `i` and release every parked
    /// stuck op. Mirrors the `ClearAllFaults` RPC.
    #[cfg(feature = "fault-injection")]
    pub fn clear_all_faults(&self, i: usize) -> Result<(u32, u32), ClusterTestingError> {
        Ok(self.fault_injector(i)?.clear_all())
    }

    fn slot_mut(&mut self, i: usize) -> Result<&mut NodeSlot, ClusterTestingError> {
        let len = self.slots.len();
        self.slots
            .get_mut(i)
            .ok_or(ClusterTestingError::OutOfRange {
                which: "node",
                idx: i,
                len,
            })
    }
}

impl Drop for ClusterTestingControl {
    fn drop(&mut self) {
        // Drop slots (and their nested `ClusterNode`s) before removing
        // the temp dir so the cooperative teardown finishes flushing to
        // disk while the data dir is still there. Each `ClusterNode`
        // runs its own RAII shutdown in `ClusterNode::Drop` — cancels
        // its cancellation token and joins every spawned thread.
        self.cluster_client = None;
        self.slots.clear();
        if self.harness_owns_root_dir {
            let _ = std::fs::remove_dir_all(&self.root_data_dir);
        }
    }
}

// ── Free helpers ──────────────────────────────────────────────────────────

fn free_port() -> u16 {
    use std::collections::HashSet;
    use std::sync::LazyLock;
    static ALLOCATED: LazyLock<std::sync::Mutex<HashSet<u16>>> =
        LazyLock::new(|| std::sync::Mutex::new(HashSet::new()));
    let mut allocated = ALLOCATED.lock().expect("port set");
    let mut held: Vec<TcpListener> = Vec::new();
    loop {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
        let port = l.local_addr().expect("local_addr").port();
        if allocated.insert(port) {
            return port;
        }
        held.push(l);
    }
}

fn make_root_tmp_dir(label: &str) -> PathBuf {
    // Combine nanos with a process-global counter so concurrent
    // harness instances under `cargo test` (default parallelism)
    // can't collide on dir names even if SystemTime is too coarse
    // to distinguish them.
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let mut d = std::env::current_dir().expect("current_dir");
    d.push(format!("temp_cluster_{}_{}_{}_{}", label, pid, nanos, n));
    d
}

async fn wait_for_tcp(addr: String, timeout: Duration) -> Result<(), ClusterTestingError> {
    let deadline = Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ClusterTestingError::TcpBindTimeout(addr));
        }
        sleep(Duration::from_millis(10)).await;
    }
}

/// Polls `TcpListener::bind` on `127.0.0.1:port` until success or
/// `timeout` — used by `stop_node` to confirm the OS has released a
/// port that the now-aborted server task held. No event source for
/// this; the kernel is the authority.
async fn wait_for_tcp_release(port: u16, timeout: Duration) -> Result<(), ClusterTestingError> {
    let addr = format!("127.0.0.1:{port}");
    let deadline = Instant::now() + timeout;
    loop {
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ClusterTestingError::TcpBindTimeout(addr));
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn ping_role(node_port: u16) -> Option<nproto::NodeRole> {
    let mut client = ProtoNodeClient::connect(format!("http://127.0.0.1:{}", node_port))
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
