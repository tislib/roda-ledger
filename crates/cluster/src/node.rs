//! `ClusterNode` — top-level entry point for both deployment shapes.
//!
//! - **Standalone** (`config.cluster.is_none()`): spawns only the
//!   writable client-facing Ledger gRPC server. No Node service, no
//!   replication, no raft loop. Returns [`Handles::Standalone`].
//! - **Clustered**: spawns the raft loop, the peer-facing Node gRPC
//!   server, and the client-facing Ledger gRPC server. Returns
//!   [`Handles::Cluster`].
//!
//! Per ADR-0017 §"Required Invariants" #4 the term log is **not**
//! bumped on boot — it stays at whatever the durable persistence layer
//! reports until an actual election win advances it.
//!
//! Lifecycle is RAII: dropping a `Handles` is the shutdown. The
//! clustered shutdown protocol:
//!
//!   1. `Drop` fires `notify_waiters()` on both gRPC servers'
//!      shutdown notifies — they stop accepting new connections.
//!   2. `Drop` drops the supervisor's `mpsc::Sender<Command>` clone.
//!      In-flight gRPC handlers still hold sender clones, so the
//!      channel doesn't close yet.
//!   3. The two gRPC server tasks drain their in-flight handlers and
//!      exit. As each handler exits, its `cmd_tx` clone drops.
//!   4. When the last handler exits, the loop's `cmd_rx.recv()`
//!      returns `None`. The loop drops its parked replies (the
//!      oneshots' `Drop` surfaces as `Status::internal` to any
//!      lingering caller) and exits.
//!   5. `Drop` awaits all three task handles via `drain_in_drop`.

use crate::LedgerSlot;
use crate::cluster_mirror::ClusterMirror;
use crate::config::Config;
use crate::durable::DurablePersistence;
use crate::lifecycle::drain_in_drop;
use crate::node_handler::{NodeHandler, NodeHandlerCore};
use crate::raft_loop::RaftLoop;
use crate::server::{NodeServerRuntime, Server};
use ledger::ledger::Ledger;
use raft::{RaftConfig, RaftNode, Role};
use spdlog::{error, info};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use crate::command::Command;

pub struct ClusterNode {
    config: Config,
    /// Wrapping the live `Arc<Ledger>` in a slot lets the raft loop
    /// swap it during a divergence reseed (ADR-0016 §9) without
    /// tearing down the gRPC servers.
    ledger_slot: Arc<LedgerSlot>,
    /// Durable term + vote logs. Always opened, even in standalone
    /// mode, so the cluster path can pick up where the standalone
    /// path left off (and vice-versa). Per ADR-0017 §"Required
    /// Invariants" #4 the term is **not** bumped on boot.
    durable: Arc<DurablePersistence>,
}

impl ClusterNode {
    /// Build (and start) the embedded Ledger and open the durable
    /// term + vote logs under `ledger.storage.data_dir`. The gRPC
    /// server(s) and the raft loop are launched by
    /// [`ClusterNode::run`].
    pub fn new(config: Config) -> std::io::Result<Self> {
        let mut ledger = Ledger::new(config.ledger.clone());
        ledger.start()?;
        let durable = Arc::new(DurablePersistence::open(&config.ledger.storage.data_dir)?);
        info!(
            "cluster::new: opened durable persistence (term={}, voted_for={:?}, clustered={})",
            durable.term.get_current_term(),
            durable.vote.get_voted_for(),
            config.is_clustered()
        );
        Ok(Self {
            config,
            ledger_slot: Arc::new(LedgerSlot::new(Arc::new(ledger))),
            durable,
        })
    }

    /// Return the **currently-live** `Arc<Ledger>`. After a
    /// divergence reseed the returned `Arc` may differ from the one
    /// observed before — callers must not retain it across reseeds.
    pub fn ledger(&self) -> Arc<Ledger> {
        self.ledger_slot.ledger()
    }
    
    pub fn ledger_slot(&self) -> Arc<LedgerSlot> {
        self.ledger_slot.clone()
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Dispatch to the standalone or clustered bring-up.
    pub async fn run(&self) -> Result<Handles, Box<dyn std::error::Error + Send + Sync>> {
        if self.config.is_clustered() {
            Ok(Handles::Cluster(self.run_clustered()?))
        } else {
            Ok(Handles::Standalone(self.run_standalone()?))
        }
    }

    // ── Standalone bring-up ──────────────────────────────────────────────

    /// Standalone: only the writable client-facing Ledger gRPC. No
    /// raft loop, no Node service, no peers.
    fn run_standalone(
        &self,
    ) -> Result<StandaloneHandles, Box<dyn std::error::Error + Send + Sync>> {
        let client_addr = self.config.server.socket_addr()?;

        // Pin the mirror to Leader so `LedgerHandler` accepts writes.
        // Raft never runs in this mode, so nothing else mutates the
        // mirror.
        let mirror = ClusterMirror::new();
        mirror.set_role_for_standalone(Role::Leader);

        let client_shutdown = Arc::new(Notify::new());
        let server = Server::new(
            self.ledger_slot.clone(),
            client_addr,
            mirror,
            self.durable.term.clone(),
            client_shutdown.clone(),
        );
        let client_handle = tokio::spawn(async move {
            if let Err(e) = server.run().await {
                error!("standalone ledger gRPC server exited: {}", e);
            }
        });
        info!(
            "standalone: client-facing Ledger gRPC up on {}",
            client_addr
        );

        Ok(StandaloneHandles {
            client_handle: Some(client_handle),
            client_shutdown,
        })
    }

    // ── Clustered bring-up ───────────────────────────────────────────────

    /// Clustered: build the raft loop, the peer-facing Node gRPC
    /// server, and the client-facing Ledger gRPC server. The three
    /// communicate through the `Command` mpsc channel and the
    /// `ClusterMirror` snapshot — there is no shared lock.
    fn run_clustered(&self) -> Result<ClusterHandles, Box<dyn std::error::Error + Send + Sync>> {
        let cluster = self
            .config
            .cluster
            .as_ref()
            .expect("run_clustered requires a clustered config");

        let client_addr = self.config.server.socket_addr()?;
        let node_addr = cluster.node.socket_addr()?;
        let self_id = cluster.node.node_id;
        let peer_ids: Vec<u64> = cluster.peers.iter().map(|p| p.peer_id).collect();

        // Build the raft state machine. Once spawned, the loop owns
        // it as `&mut self.node` for the rest of the process lifetime
        // — no other reference exists.
        let mirror = ClusterMirror::new();
        let raft_cfg = RaftConfig::default();
        let seed = seed_for(self_id);
        let persistence = DurablePersistence {
            term: self.durable.term.clone(),
            vote: self.durable.vote.clone(),
        };
        let mut node = RaftNode::new(self_id, peer_ids, persistence, raft_cfg, seed);

        // Inform raft of any durable log entries so §5.4.1's
        // up-to-date check is accurate before the first inbound RPC.
        let durable_last_tx_id = self.ledger_slot.ledger().last_commit_id();
        if durable_last_tx_id > 0 {
            let _ = node.step(
                Instant::now(),
                raft::Event::LogAppendComplete {
                    tx_id: durable_last_tx_id,
                },
            );
        }
        // Snapshot initial state so consumers see post-construction values.
        mirror.snapshot_from(&node);

        // Spawn the raft loop. `cmd_tx` is the only handle into raft
        // mutation; clones go to the gRPC handler and into outbound
        // dispatch tasks (the loop spawns those itself).
        let (cmd_tx, raft_handle) = RaftLoop::spawn(
            node,
            self.ledger_slot.clone(),
            mirror.clone(),
            Arc::new(self.config.clone()),
        );

        // Client-facing Ledger gRPC server (writable on leader,
        // read-only on follower; consults mirror for routing).
        let client_shutdown = Arc::new(Notify::new());
        let client_server = Server::new(
            self.ledger_slot.clone(),
            client_addr,
            mirror.clone(),
            self.durable.term.clone(),
            client_shutdown.clone(),
        );
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client_server.run().await {
                error!("ledger gRPC server exited: {}", e);
            }
        });

        // Peer-facing Node gRPC server.
        let node_core = Arc::new(NodeHandlerCore::new(
            self.ledger_slot.clone(),
            self_id,
            mirror.clone(),
            cmd_tx.clone(),
        ));
        let node_handler = NodeHandler::new(node_core);
        let node_max_bytes = cluster.append_entries_max_bytes * 2 + 4 * 1024;
        let node_shutdown = Arc::new(Notify::new());
        let node_runtime = NodeServerRuntime::new(
            node_addr,
            node_handler,
            node_max_bytes,
            node_shutdown.clone(),
        );
        let node_grpc_handle = tokio::spawn(async move {
            if let Err(e) = node_runtime.run().await {
                error!("node gRPC server exited: {}", e);
            }
        });

        info!(
            "cluster: bring-up complete (node_id={}, peers={}, client={}, node={})",
            self_id,
            cluster.peers.len(),
            client_addr,
            node_addr,
        );

        Ok(ClusterHandles {
            raft_handle: Some(raft_handle),
            client_handle: Some(client_handle),
            node_grpc_handle: Some(node_grpc_handle),
            mirror,
            cmd_tx: Some(cmd_tx),
            client_shutdown,
            node_shutdown,
        })
    }
}

/// Per-process seed for the election-timer RNG. XOR-ing in `self_id`
/// keeps two nodes that booted at the same `SystemTime` from drawing
/// the same first-round timeout, which would create deterministic
/// split-vote storms in CI.
fn seed_for(self_id: u64) -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ self_id
}

// ── Unified handle ──────────────────────────────────────────────────────

/// Unified handles view across standalone / clustered bring-ups.
///
/// Lifecycle is RAII: dropping the value triggers cooperative shutdown
/// of every spawned task this bring-up owns. There is no separate
/// `shutdown()` method.
pub enum Handles {
    Standalone(StandaloneHandles),
    Cluster(ClusterHandles),
}

impl Handles {
    /// Lock-free read surface for raft state (clustered only). Returns
    /// `None` in standalone mode. Use `mirror.cluster_commit_index()`
    /// for the quorum-committed watermark, `mirror.role()` for the
    /// current role, etc.
    pub fn mirror(&self) -> Option<Arc<ClusterMirror>> {
        match self {
            Handles::Cluster(h) => Some(h.mirror.clone()),
            _ => None,
        }
    }

    /// Whether the peer-facing Node gRPC server task is present.
    /// `false` in standalone mode (no Node service runs).
    pub fn has_node_handle(&self) -> bool {
        matches!(self, Handles::Cluster(_))
    }
}

// ── Standalone handles ──────────────────────────────────────────────────

/// Handles produced by a successful standalone bring-up. Single
/// gRPC server, nothing else.
pub struct StandaloneHandles {
    pub client_handle: Option<JoinHandle<()>>,
    /// Cooperative shutdown trigger for the client-facing Ledger
    /// gRPC server. `Drop` calls `notify_waiters()` so
    /// `serve_with_shutdown` resolves and the server task exits.
    pub client_shutdown: Arc<Notify>,
}

impl Drop for StandaloneHandles {
    fn drop(&mut self) {
        self.client_shutdown.notify_waiters();
        let client = self.client_handle.take();
        drain_in_drop("standalone", [client]);
    }
}

// ── Clustered handles ───────────────────────────────────────────────────

/// Handles produced by a successful clustered bring-up. Owns the raft
/// loop's join handle, both gRPC server tasks, and the mirror clone.
pub struct ClusterHandles {
    /// Raft loop task. Exits when the last `cmd_tx` clone drops, which
    /// happens after both gRPC server tasks have drained their
    /// in-flight handlers.
    pub raft_handle: Option<JoinHandle<()>>,
    /// Client-facing Ledger gRPC server task.
    pub client_handle: Option<JoinHandle<()>>,
    /// Peer-facing Node gRPC server task.
    pub node_grpc_handle: Option<JoinHandle<()>>,
    /// Lock-free read surface for raft state.
    pub mirror: Arc<ClusterMirror>,
    /// Sender into the raft loop. Held here only so `Drop` can drop
    /// it explicitly (the gRPC handlers each hold their own clones).
    /// Wrapped in `Option` so `Drop` can take it before awaiting the
    /// loop handle.
    cmd_tx: Option<mpsc::Sender<Command>>,
    /// Cooperative shutdown trigger for the client-facing gRPC server.
    pub client_shutdown: Arc<Notify>,
    /// Cooperative shutdown trigger for the peer-facing gRPC server.
    pub node_shutdown: Arc<Notify>,
}

impl Drop for ClusterHandles {
    fn drop(&mut self) {
        // Stop both gRPC servers from accepting new connections.
        // In-flight handlers continue running and are awaited below;
        // new connections immediately fail at the tonic layer.
        self.client_shutdown.notify_waiters();
        self.node_shutdown.notify_waiters();

        // Drop our cmd_tx clone. The gRPC handlers still hold theirs
        // (one per in-flight RPC), so the channel doesn't close yet.
        // It closes when the last handler exits, at which point the
        // raft loop's `recv()` returns `None` and the loop exits.
        self.cmd_tx.take();

        // Drain in dependency order:
        //   gRPC servers → drains their handlers → handlers' cmd_tx
        //   clones drop → channel closes → raft loop exits.
        let client = self.client_handle.take();
        let node = self.node_grpc_handle.take();
        let raft = self.raft_handle.take();
        drain_in_drop("cluster", [client, node, raft]);
    }
}
