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

use crate::client::LedgerClient;
use crate::cluster::cluster_commit::ClusterCommitIndex;
use crate::cluster::config::{
    ClusterNodeSection, ClusterSection, Config, PeerConfig, ServerSection,
};
use crate::cluster::ledger_handler::LedgerHandler;
use crate::cluster::ledger_slot::LedgerSlot;
use crate::cluster::node::{ClusterNode, Handles};
use crate::cluster::node_handler::{NodeHandler, NodeHandlerCore};
use crate::cluster::proto::node as nproto;
use crate::cluster::proto::node::node_client::NodeClient;
use crate::cluster::raft::{Role, RoleFlag, Term, Vote};
use crate::config::{LedgerConfig, StorageConfig};
use crate::ledger::Ledger;
use crate::wait_strategy::WaitStrategy;
use spdlog::Level;
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
            transaction_count_per_segment: 10_000_000,
            snapshot_frequency: 2,
            ledger_log_level: Level::Critical,
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
}

impl ClusterTestingControl {
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
        };

        // 5) Optionally start every slot. Standalone/Cluster also
        //    waits for gRPC binds.
        if config.autostart {
            ctl.start_all().await?;
            if !matches!(ctl.mode, ClusterTestingMode::Bare { .. }) {
                ctl.wait_for_all_tcp(Duration::from_secs(5)).await?;
            }
        }
        Ok(ctl)
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
            let _ = tokio::time::timeout(Duration::from_secs(2), h.shutdown()).await;
            if addr.client_port != 0 {
                if let Err(e) =
                    wait_for_tcp_release(addr.client_port, Duration::from_secs(2)).await
                {
                    spdlog::warn!(
                        "stop_node({i}): client_port {} not released: {e}",
                        addr.client_port
                    );
                }
            }
            if addr.node_port != 0 {
                if let Err(e) =
                    wait_for_tcp_release(addr.node_port, Duration::from_secs(2)).await
                {
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

    /// Open a fresh high-level [`LedgerClient`] against node `i`'s
    /// client port. Each call returns a new channel — gRPC channels
    /// are cheap. Errors in Bare mode (no servers).
    pub async fn client(&self, i: usize) -> Result<LedgerClient, ClusterTestingError> {
        if matches!(self.mode, ClusterTestingMode::Bare { .. }) {
            return Err(ClusterTestingError::NotInBareMode { op: "client" });
        }
        let port = self.client_port(i)?;
        LedgerClient::connect_url(&format!("http://127.0.0.1:{}", port))
            .await
            .map_err(ClusterTestingError::Connect)
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
                    return Err(ClusterTestingError::TwoLeaders {
                        ids: leaders
                            .iter()
                            .map(|i| self.slots[*i].addr.node_id)
                            .collect(),
                    });
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

    pub async fn leader_client(&self) -> Result<LedgerClient, ClusterTestingError> {
        let i = self.leader_index().await?;
        self.client(i).await
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

    pub async fn follower_client(&self) -> Result<LedgerClient, ClusterTestingError> {
        let i = self.first_follower_index().await?;
        self.client(i).await
    }

    // ── Polling helpers ──────────────────────────────────────────────────

    /// Poll `predicate` every 10 ms until it returns `true` or
    /// `timeout` elapses. The predicate is an async closure so it
    /// can drive client RPCs on each iteration.
    ///
    /// Tip: if the predicate captures a `LedgerClient`, clone it
    /// inside the closure so each iteration owns its own handle:
    ///
    /// ```ignore
    /// let lc = ctl.leader_client().await?;
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

    /// Wait for slot `i`'s gRPC servers to bind. Useful after a
    /// manual [`run_node`] call (`autostart = false` flow).
    pub async fn wait_for_bind(
        &self,
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
        for slot in &mut self.slots {
            if let Some(h) = slot.handles.take() {
                h.abort();
            }
            slot.node = None;
            slot.bare = None;
        }
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
    let mut client = NodeClient::connect(format!("http://127.0.0.1:{}", node_port))
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
