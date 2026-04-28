//! High-level test harness. `ClusterTestingControl` is the single
//! entry point for cluster-style tests: it owns per-node data dirs,
//! ports, configs, and either the running `ClusterNode`s or the
//! bare components (Ledger / Term / Vote / RoleFlag / LedgerSlot /
//! ClusterCommitIndex) needed to drive `LedgerHandler` /
//! `NodeHandler` directly.
//!
//! Three deployment shapes are supported via [`ClusterTestingMode`]:
//! - [`ClusterTestingMode::Bare`] — single node, no servers. Each
//!   slot gets the bare components only; tests construct
//!   `LedgerHandler` / `NodeHandler` over them and call methods
//!   directly (no networking).
//! - [`ClusterTestingMode::Standalone`] — single node, no `[cluster]`
//!   block, role pinned to Leader, only the client-facing Ledger
//!   gRPC.
//! - [`ClusterTestingMode::Cluster`] — `n`-node cluster with
//!   election + replication.
//!
//! Data-directory ownership is *cluster-level*: when
//! `config.data_dir_root` is `None`, the harness creates
//! `temp_cluster_<label>_<nanos>/`, puts each node's ledger under
//! `<root>/<i>/`, and removes the root in `Drop`. When the caller
//! supplies a `data_dir_root`, the harness uses it as-is and does
//! **not** remove it on drop.

use crate::client::{ClusterClient, NodeClient, PipelineIndex, SubmitResult};
use crate::cluster::cluster_commit::ClusterCommitIndex;
use crate::cluster::config::{
    ClusterNodeSection, ClusterSection, Config, PeerConfig, ServerSection,
};
use crate::cluster::ledger_handler::LedgerHandler;
use crate::cluster::ledger_slot::LedgerSlot;
use crate::cluster::node::{ClusterNode, Handles};
use crate::cluster::node_handler::{NodeHandler, NodeHandlerCore};
use crate::cluster::proto::ledger as lproto;
use crate::cluster::proto::node as nproto;
use crate::cluster::proto::node::node_client::NodeClient as ProtoNodeClient;
use crate::cluster::raft::{Role, RoleFlag, Term, Vote};
use crate::config::{LedgerConfig, StorageConfig};
use crate::ledger::Ledger;
use crate::wait_strategy::WaitStrategy;
use spdlog::{info, warn, Level};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
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
    /// Operation is only valid for [`ClusterTestingMode::Bare`].
    BareModeOnly {
        op: &'static str,
    },
    /// Operation is not valid for [`ClusterTestingMode::Bare`].
    NotInBareMode {
        op: &'static str,
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
            Self::BareModeOnly { op } => {
                write!(f, "{op} is only valid in Bare mode")
            }
            Self::NotInBareMode { op } => {
                write!(f, "{op} is not valid in Bare mode")
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
        }
    }
}

impl std::error::Error for ClusterTestingError {}

// ── Mode + Config ─────────────────────────────────────────────────────────

/// Deployment shape selector for the harness.
#[derive(Clone, Debug)]
pub enum ClusterTestingMode {
    /// Single node, no servers. Each slot gets `Ledger`, `Term`,
    /// `Vote`, `RoleFlag` (with `role`), `LedgerSlot`, and
    /// `ClusterCommitIndex` — enough to construct `LedgerHandler`
    /// / `NodeHandler` and drive them directly. Used for handler-
    /// level integration tests that don't go through gRPC.
    Bare { role: Role },
    /// `Config { cluster: None, .. }`. Single node, role pinned to
    /// Leader, only the client-facing Ledger gRPC.
    Standalone,
    /// `Config { cluster: Some(_), .. }` with `nodes` real members
    /// in the symmetric peer list. Election + replication enabled.
    Cluster { nodes: usize },
}

/// Knobs for [`ClusterTestingControl`]. Use [`Self::default`] for a
/// 1-node cluster, [`Self::standalone`] / [`Self::cluster`] /
/// [`Self::bare`] for the other shapes. All fields are public so
/// callers can override individual knobs with struct-update syntax:
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
    /// `ClusterNode::new`, `node.run()`, or build Bare components.
    /// Callers can then drive each node manually with
    /// [`ClusterTestingControl::start_node`] / `build_node` /
    /// `run_node`.
    pub autostart: bool,
    /// If `Some`, use this absolute path as the cluster's root data
    /// directory and **do not remove it on `Drop`** — the caller
    /// owns it. If `None`, the harness creates
    /// `temp_cluster_<label>_<nanos>/` under `current_dir()` and
    /// removes it in `Drop`.
    pub data_dir_root: Option<PathBuf>,
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
            ledger_log_level: Level::Info,
            autostart: true,
            data_dir_root: None,
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

    /// Bare components (no servers) — for handler-level tests.
    pub fn bare(role: Role) -> Self {
        Self {
            mode: ClusterTestingMode::Bare { role },
            label: "bare".to_string(),
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
    /// Peer-facing Node gRPC port. `0` (unused) in standalone /
    /// bare modes.
    node_port: u16,
}

/// Bare-mode components. Populated by [`ClusterTestingControl::start_node`]
/// when the harness is in [`ClusterTestingMode::Bare`].
struct BareComponents {
    ledger: Arc<Ledger>,
    term: Arc<Term>,
    vote: Arc<Vote>,
    role_flag: Arc<RoleFlag>,
    ledger_slot: Arc<LedgerSlot>,
    cluster_commit_index: Arc<ClusterCommitIndex>,
}

struct NodeSlot {
    addr: NodeAddr,
    config: Config,
    /// `<root>/<i>/` — the per-node data dir. Identical to
    /// `config.ledger.storage.data_dir` but kept as a `PathBuf` for
    /// ergonomic test access.
    data_dir: PathBuf,
    // Cluster/Standalone-mode bring-up state.
    node: Option<ClusterNode>,
    handles: Option<Handles>,
    // Bare-mode bring-up state.
    bare: Option<BareComponents>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct PhantomPeer {
    peer_id: u64,
    node_port: u16,
}

// ── Top-level harness ─────────────────────────────────────────────────────

/// High-level harness that owns an in-process cluster end-to-end:
/// per-node configs, ports, data dirs, and either the running
/// `ClusterNode`s or the bare handler components.
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
    /// [`Self::start`] (when autostart=true and the mode has servers)
    /// against every node's client port. `None` in Bare mode and in
    /// `autostart = false` flows where no servers have bound yet.
    /// All test code that needs to talk to the cluster goes through
    /// [`Self::client`].
    cluster_client: Option<ClusterClient>,
    /// Highest tx_id any submit through this harness has produced.
    /// Bumped via `fetch_max` on every successful `*_and_wait` /
    /// `submit_*` call so concurrent submits never regress the
    /// watermark. Used by [`Self::ensure_caught_up_at`] as the catch-
    /// up target for `require_*` reads.
    last_tx_id: Arc<AtomicU64>,
}

impl ClusterTestingControl {
    /// Upper bound on how long `ensure_caught_up_at` will wait for a
    /// node's snapshot watermark to reach the harness's
    /// `last_tx_id`. Generous enough to cover post-restart election
    /// + replication windows; if a real test needs more, that's a
    /// signal something is wrong, not a knob to tune.
    pub const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(15);

    /// Build a harness from a config. Unless
    /// [`ClusterTestingConfig::autostart`] is `false`, every slot's
    /// components / servers are brought up before returning.
    pub async fn start(config: ClusterTestingConfig) -> Result<Self, ClusterTestingError> {
        // 1) Resolve root data dir.
        let (root_data_dir, harness_owns_root_dir) = match config.data_dir_root {
            Some(p) => (p, false),
            None => (make_root_tmp_dir(&config.label), true),
        };
        std::fs::create_dir_all(&root_data_dir).map_err(ClusterTestingError::Build)?;

        // 2) Per-mode slot count + per-slot port allocation.
        let real_node_count = match config.mode {
            ClusterTestingMode::Bare { .. } | ClusterTestingMode::Standalone => 1,
            ClusterTestingMode::Cluster { nodes } => nodes,
        };
        let addrs: Vec<NodeAddr> = (0..real_node_count)
            .map(|i| {
                let (client_port, node_port) = match config.mode {
                    ClusterTestingMode::Bare { .. } => (0, 0),
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
        //    + phantom). Bare/Standalone skip this entirely.
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
                ClusterTestingMode::Bare { .. } | ClusterTestingMode::Standalone => None,
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
                handles: None,
                bare: None,
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
        };

        // 5) Optionally start every slot. Standalone/Cluster also
        //    waits for gRPC binds and then opens a ClusterClient
        //    against every running node so tests can route through a
        //    single `client()` accessor instead of resolving leader
        //    vs follower channels by hand.
        if config.autostart {
            ctl.start_all().await?;
            if !matches!(ctl.mode, ClusterTestingMode::Bare { .. }) {
                ctl.wait_for_all_tcp(Duration::from_secs(5)).await?;
                ctl.rebuild_cluster_client().await?;
            }
        }
        Ok(ctl)
    }

    /// (Re)build the embedded [`ClusterClient`] from every currently
    /// started slot's client URL. Called automatically inside
    /// [`Self::start`] for autostart flows; tests that bring nodes up
    /// manually (`autostart = false`) should call this once after the
    /// first round of `start_node` so [`Self::client`] becomes usable.
    async fn rebuild_cluster_client(&mut self) -> Result<(), ClusterTestingError> {
        if matches!(self.mode, ClusterTestingMode::Bare { .. }) {
            // Bare mode has no client gRPC; nothing to connect.
            self.cluster_client = None;
            return Ok(());
        }
        let urls: Vec<String> = self
            .slots
            .iter()
            .filter(|s| s.handles.is_some() && s.addr.client_port != 0)
            .map(|s| format!("http://127.0.0.1:{}", s.addr.client_port))
            .collect();
        if urls.is_empty() {
            self.cluster_client = None;
            return Ok(());
        }
        let cc = ClusterClient::connect(&urls)
            .await
            .map_err(ClusterTestingError::Connect)?;
        self.cluster_client = Some(cc);
        Ok(())
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    /// Bring slot `i` up. In Cluster/Standalone modes this builds
    /// (`ClusterNode::new`), runs (`ClusterNode::run`) the node, and
    /// waits for the gRPC servers to bind so the slot is immediately
    /// usable on return. In Bare mode it constructs `Ledger`/`Term`/
    /// `Vote`/`RoleFlag`/`LedgerSlot`/`ClusterCommitIndex`.
    /// Idempotent.
    pub async fn start_node(&mut self, i: usize) -> Result<(), ClusterTestingError> {
        match self.mode {
            ClusterTestingMode::Bare { role } => {
                self.bare_build(i, role)?;
            }
            _ => {
                self.build_node(i)?;
                self.run_node(i).await?;
                self.wait_for_bind(i, Duration::from_secs(5)).await?;
                // Refresh the embedded cluster client now that this
                // slot is bound — covers the `autostart = false` flow
                // where tests bring nodes up manually and expect
                // `ctl.client()` to "just work" right after.
                self.rebuild_cluster_client().await?;
            }
        }
        Ok(())
    }

    /// Aborts node `i`'s spawned tasks (Cluster/Standalone), awaits
    /// their join handles so the runtime drives teardown to
    /// completion, and waits for the OS to release the bound ports
    /// before returning. Safe to call on already-stopped slots.
    ///
    /// The shutdown + port-release waits are bounded so a stuck task
    /// can't wedge the test harness. If a port doesn't release in
    /// time we log and continue rather than fail the call — the
    /// caller likely doesn't care about that specific port (it's
    /// either restarting on a new free port via `start_all`, or it's
    /// done with this slot for the rest of the test).
    pub async fn stop_node(&mut self, i: usize) -> Result<(), ClusterTestingError> {
        let (handles_opt, addr) = {
            let slot = self.slot_mut(i)?;
            let h = slot.handles.take();
            slot.node = None;
            slot.bare = None;
            (h, slot.addr.clone())
        };

        if let Some(h) = handles_opt {
            // Dropping `h` triggers cooperative teardown via the
            // `Drop` impls on `StandaloneHandles` / `SupervisorHandles`.
            // The drop is bounded internally by `drain_in_drop`'s
            // 5-second timeout, so a wedged task can't hang us.
            drop(h);
            if addr.client_port != 0 {
                if let Err(e) = wait_for_tcp_release(addr.client_port, Duration::from_secs(2)).await
                {
                    spdlog::warn!(
                        "stop_node({i}): client_port {} not released: {e}",
                        addr.client_port
                    );
                }
            }
            if addr.node_port != 0 {
                if let Err(e) = wait_for_tcp_release(addr.node_port, Duration::from_secs(2)).await {
                    spdlog::warn!(
                        "stop_node({i}): node_port {} not released: {e}",
                        addr.node_port
                    );
                }
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

    // ── Pre-run access (Cluster/Standalone) ─────────────────────────────

    /// Construct the `ClusterNode` for slot `i` (which builds and
    /// starts the underlying `Ledger`) but do not call `run()` yet.
    /// Cluster/Standalone modes only — errors with
    /// [`ClusterTestingError::NotInBareMode`] in Bare mode.
    pub fn build_node(&mut self, i: usize) -> Result<(), ClusterTestingError> {
        if matches!(self.mode, ClusterTestingMode::Bare { .. }) {
            return Err(ClusterTestingError::NotInBareMode { op: "build_node" });
        }
        let slot = self.slot_mut(i)?;
        if slot.node.is_some() {
            return Ok(());
        }
        let node = ClusterNode::new(slot.config.clone()).map_err(ClusterTestingError::Build)?;
        slot.node = Some(node);
        Ok(())
    }

    /// `Arc<Ledger>` of a node that has been built but possibly not
    /// yet `run()`. Useful for seeding the ledger between `new` and
    /// `run` (see `divergence_reseed_test`).
    pub fn pre_run_ledger(&self, i: usize) -> Result<Arc<Ledger>, ClusterTestingError> {
        let slot = self.slot(i)?;
        let node = slot
            .node
            .as_ref()
            .ok_or(ClusterTestingError::NotStarted { idx: i })?;
        Ok(node.ledger())
    }

    /// Call `node.run().await` on a slot already built via
    /// [`build_node`] (or [`start_node`]). Errors if the slot was
    /// never built. Cluster/Standalone modes only.
    pub async fn run_node(&mut self, i: usize) -> Result<(), ClusterTestingError> {
        if matches!(self.mode, ClusterTestingMode::Bare { .. }) {
            return Err(ClusterTestingError::NotInBareMode { op: "run_node" });
        }
        let slot = self.slot_mut(i)?;
        if slot.handles.is_some() {
            return Ok(());
        }
        let node = slot
            .node
            .as_ref()
            .ok_or(ClusterTestingError::NotStarted { idx: i })?;
        let handles = node
            .run()
            .await
            .map_err(|e| ClusterTestingError::Run(format!("{e}")))?;
        slot.handles = Some(handles);
        *self.cached_leader_idx.lock().await = None;
        Ok(())
    }

    // ── Bare-mode bring-up (internal) ────────────────────────────────────

    fn bare_build(&mut self, i: usize, role: Role) -> Result<(), ClusterTestingError> {
        let slot = self.slot_mut(i)?;
        if slot.bare.is_some() {
            return Ok(());
        }
        std::fs::create_dir_all(&slot.data_dir).map_err(ClusterTestingError::Build)?;
        let dir_str = slot.data_dir.to_string_lossy().into_owned();

        let mut ledger = Ledger::new(slot.config.ledger.clone());
        ledger.start().map_err(ClusterTestingError::Build)?;
        let ledger = Arc::new(ledger);

        let term = Arc::new(Term::open_in_dir(&dir_str).map_err(ClusterTestingError::Build)?);
        let vote = Arc::new(Vote::open_in_dir(&dir_str).map_err(ClusterTestingError::Build)?);
        let role_flag = Arc::new(RoleFlag::new(role));
        let ledger_slot = Arc::new(LedgerSlot::new(ledger.clone()));
        let cci = ClusterCommitIndex::from_ledger(&ledger);

        slot.bare = Some(BareComponents {
            ledger,
            term,
            vote,
            role_flag,
            ledger_slot,
            cluster_commit_index: cci,
        });
        Ok(())
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
    /// standalone / bare modes (no Node gRPC binds).
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

    /// Direct (sync) access to a started node's live `Arc<Ledger>`.
    /// Works in all modes — Bare returns the `BareComponents.ledger`,
    /// Cluster/Standalone returns the running node's ledger.
    pub fn ledger(&self, i: usize) -> Result<Arc<Ledger>, ClusterTestingError> {
        let slot = self.slot(i)?;
        if let Some(b) = &slot.bare {
            return Ok(b.ledger.clone());
        }
        let node = slot
            .node
            .as_ref()
            .ok_or(ClusterTestingError::NotStarted { idx: i })?;
        Ok(node.ledger())
    }

    /// Live `Arc<LedgerSlot>` (the swap point used by divergence
    /// reseed). Works in all modes.
    pub fn ledger_slot(&self, i: usize) -> Result<Arc<LedgerSlot>, ClusterTestingError> {
        let slot = self.slot(i)?;
        if let Some(b) = &slot.bare {
            return Ok(b.ledger_slot.clone());
        }
        let node = slot
            .node
            .as_ref()
            .ok_or(ClusterTestingError::NotStarted { idx: i })?;
        Ok(node.ledger_slot().clone())
    }

    /// Bare-mode-only accessors (the running modes hide these
    /// inside the supervisor).
    pub fn term(&self, i: usize) -> Result<Arc<Term>, ClusterTestingError> {
        let bare = self.bare(i)?;
        Ok(bare.term.clone())
    }

    pub fn vote(&self, i: usize) -> Result<Arc<Vote>, ClusterTestingError> {
        let bare = self.bare(i)?;
        Ok(bare.vote.clone())
    }

    pub fn role_flag(&self, i: usize) -> Result<Arc<RoleFlag>, ClusterTestingError> {
        let bare = self.bare(i)?;
        Ok(bare.role_flag.clone())
    }

    pub fn cluster_commit_index(
        &self,
        i: usize,
    ) -> Result<Arc<ClusterCommitIndex>, ClusterTestingError> {
        let bare = self.bare(i)?;
        Ok(bare.cluster_commit_index.clone())
    }

    /// Build a `LedgerHandler` over slot `i`'s bare components.
    /// Bare-mode-only.
    pub fn ledger_handler(&self, i: usize) -> Result<LedgerHandler, ClusterTestingError> {
        let bare = self.bare(i)?;
        Ok(LedgerHandler::new(
            bare.ledger_slot.clone(),
            bare.role_flag.clone(),
            bare.term.clone(),
            bare.cluster_commit_index.clone(),
        ))
    }

    /// Build a `NodeHandler` over slot `i`'s bare components, with
    /// a caller-chosen `claimed_node_id` (often a peer's id rather
    /// than this node's). Bare-mode-only.
    pub fn node_handler(
        &self,
        i: usize,
        claimed_node_id: u64,
    ) -> Result<NodeHandler, ClusterTestingError> {
        let bare = self.bare(i)?;
        let core = Arc::new(NodeHandlerCore::new(
            bare.ledger_slot.clone(),
            claimed_node_id,
            bare.term.clone(),
            bare.vote.clone(),
            bare.role_flag.clone(),
            None,
        ));
        Ok(NodeHandler::new(core))
    }

    pub fn index_of(&self, node_id: u64) -> Option<usize> {
        self.slots.iter().position(|s| s.addr.node_id == node_id)
    }

    pub fn handles(&self, i: usize) -> Result<&Handles, ClusterTestingError> {
        let slot = self.slot(i)?;
        slot.handles
            .as_ref()
            .ok_or(ClusterTestingError::NotStarted { idx: i })
    }

    /// The cluster-aware [`ClusterClient`] for this harness.
    ///
    /// Tests get every cluster operation through this accessor:
    /// - leader-routed writes via `ctl.client().deposit(…)` or
    ///   `ctl.client().leader().deposit_and_wait(…)`.
    /// - per-node access via `ctl.client().node(i)`.
    /// - read-style RPCs via `ctl.client().get_balance(…)` (round-
    ///   robin across all nodes).
    ///
    /// Panics if the harness is in Bare mode (no client gRPC) or
    /// `start()` was called with `autostart = false` and the caller
    /// has not yet invoked [`Self::rebuild_cluster_client`].
    pub fn client(&self) -> &ClusterClient {
        self.cluster_client.as_ref().expect(
            "ClusterTestingControl::client() called before the cluster client was built — \
             use autostart=true (the default) or call rebuild_cluster_client() after \
             starting nodes manually",
        )
    }

    // ── Leader resolution ────────────────────────────────────────────────

    /// Block until exactly one Leader is observed (or `timeout`
    /// elapses). In single-node modes (Standalone / Bare) returns
    /// slot 0 immediately without polling.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<usize, ClusterTestingError> {
        if matches!(
            self.mode,
            ClusterTestingMode::Standalone | ClusterTestingMode::Bare { .. }
        ) {
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
                if slot.handles.is_none() {
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

    pub async fn leader_ledger(&self) -> Result<Arc<Ledger>, ClusterTestingError> {
        let i = self.leader_index().await?;
        self.ledger(i)
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

    /// Wait for `ledger.last_commit_id()` to reach `tx_id`. Polls at
    /// 10ms via [`Self::wait_for`]. Replaces the duplicated
    /// `wait_committed` helpers that used to live in each test file.
    pub async fn wait_for_commit(
        &self,
        ledger: &Ledger,
        tx_id: u64,
        timeout: Duration,
    ) -> Result<(), ClusterTestingError> {
        self.wait_for(timeout, &format!("commit ≥ {tx_id}"), || async {
            ledger.last_commit_id() >= tx_id
        })
        .await
    }

    /// Wait for `ledger.last_snapshot_id()` to reach `tx_id`. Mirrors
    /// [`Self::wait_for_commit`] but waits on the snapshot stage —
    /// the one that actually publishes balance changes that
    /// `get_balance` reads. Use whenever a test reads a balance after
    /// a write that targeted a level below `WaitLevel::Snapshot`.
    pub async fn wait_for_snapshot_id(
        &self,
        ledger: &Ledger,
        tx_id: u64,
        timeout: Duration,
    ) -> Result<(), ClusterTestingError> {
        self.wait_for(timeout, &format!("snapshot ≥ {tx_id}"), || async {
            ledger.last_snapshot_id() >= tx_id
        })
        .await
    }

    /// Log a per-node table of pipeline progression watermarks
    /// (compute, commit, snapshot) followed by the cluster-wide
    /// `cluster_commit_index`. Useful for diagnosing where a
    /// stalled pipeline is stuck.
    ///
    /// Works in all modes: in Cluster/Standalone the values are
    /// fetched per-node over gRPC via `ClusterClient::node(i)`; in
    /// Bare mode they're read directly from the bare components.
    /// Never fails for individual node RPC errors — unreachable
    /// nodes show dashes so the rest of the matrix is still
    /// visible.
    pub async fn show_pipeline_matrix(&self) -> Result<(), ClusterTestingError> {
        info!(
            "{:<8} {:<26} {:<14} {:<13} {:<14}",
            "node_id", "node_url", "compute_index", "commit_index", "snapshot_index"
        );

        let mut cluster_commit: u64 = 0;
        for (i, slot) in self.slots.iter().enumerate() {
            let node_id = slot.addr.node_id;
            let url = if slot.addr.client_port == 0 {
                "<bare>".to_string()
            } else {
                format!("http://{}:{}", slot.addr.host, slot.addr.client_port)
            };

            if let Some(b) = &slot.bare {
                let compute = b.ledger.last_compute_id();
                let commit = b.ledger.last_commit_id();
                let snapshot = b.ledger.last_snapshot_id();
                cluster_commit = cluster_commit.max(b.cluster_commit_index.get());
                info!(
                    "{:<8} {:<26} {:<14} {:<13} {:<14}",
                    node_id, url, compute, commit, snapshot
                );
            } else if slot.handles.is_some() {
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
            .ok_or(ClusterTestingError::NotInBareMode {
                op: "cluster client access",
            })
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
    /// manual [`run_node`] call (`autostart = false` flow).
    pub async fn wait_for_bind(
        &mut self,
        i: usize,
        timeout: Duration,
    ) -> Result<(), ClusterTestingError> {
        let slot = self.slot(i)?;
        if slot.handles.is_none() {
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
        // manually via `build_node` + `run_node` + `wait_for_bind`.
        self.rebuild_cluster_client().await?;
        Ok(())
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    async fn wait_for_all_tcp(&self, timeout: Duration) -> Result<(), ClusterTestingError> {
        for slot in &self.slots {
            if slot.handles.is_none() {
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
        slot.handles.is_some() || slot.bare.is_some()
    }

    fn slot(&self, i: usize) -> Result<&NodeSlot, ClusterTestingError> {
        let len = self.slots.len();
        self.slots.get(i).ok_or(ClusterTestingError::OutOfRange {
            which: "node",
            idx: i,
            len,
        })
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

    fn bare(&self, i: usize) -> Result<&BareComponents, ClusterTestingError> {
        if !matches!(self.mode, ClusterTestingMode::Bare { .. }) {
            return Err(ClusterTestingError::BareModeOnly {
                op: "bare component access",
            });
        }
        let slot = self.slot(i)?;
        slot.bare
            .as_ref()
            .ok_or(ClusterTestingError::NotStarted { idx: i })
    }
}

impl Drop for ClusterTestingControl {
    fn drop(&mut self) {
        // Drop slots (and their nested `ClusterNode` / `Ledger` /
        // `Handles`) before removing the temp dir so the cooperative
        // teardown finishes flushing to disk while the data dir is
        // still there. Each `slot.handles: Option<Handles>` runs its
        // own RAII shutdown via `StandaloneHandles::Drop` /
        // `SupervisorHandles::Drop`, so we don't manually abort.
        self.cluster_client = None;
        self.slots.clear();
        if self.harness_owns_root_dir {
            let _ = std::fs::remove_dir_all(&self.root_data_dir);
        }
    }
}

// ── Free helpers ──────────────────────────────────────────────────────────

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    l.local_addr().expect("local_addr").port()
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
