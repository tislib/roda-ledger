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
//! Each gRPC server runs on its own OS thread with a private
//! `current_thread` tokio runtime. The client-facing Ledger server
//! gets a plain dedicated thread ([`spawn_grpc_thread`]); the
//! peer-facing Node server's thread additionally wraps a
//! `tokio::task::LocalSet` ([`spawn_local_thread`]) so the raft loop
//! can be `spawn_local`'d on the same runtime — the loop owns
//! `Rc<RefCell<RaftNode>>` and is `!Send`, and co-locating it with
//! the inbound RPC handlers means there's no cross-runtime hop on
//! the AppendEntries / RequestVote path.
//!
//! Lifecycle is RAII: dropping a `Handles` is the shutdown. The
//! clustered shutdown protocol:
//!
//!   1. `Drop` fires `notify_waiters()` on both gRPC servers'
//!      shutdown notifies — they stop accepting new connections.
//!   2. `Drop` drops the supervisor's `mpsc::Sender<Command>` clone.
//!      In-flight gRPC handlers still hold sender clones, so the
//!      channel doesn't close yet.
//!   3. `Drop` joins the two gRPC OS threads. `serve_with_shutdown`
//!      drains in-flight handlers and exits; as each handler exits,
//!      its `cmd_tx` clone drops.
//!   4. When the last handler exits, the loop's `cmd_rx.recv()`
//!      returns `None`. The loop drops its parked replies (the
//!      oneshots' `Drop` surfaces as `Status::internal` to any
//!      lingering caller) and exits. The node-grpc thread's outer
//!      `LocalSet` future awaits the raft future, then returns —
//!      the OS thread joins.

use crate::LedgerSlot;
use crate::cluster_mirror::ClusterMirror;
use crate::command::{AppendEntriesMsg, RequestVoteMsg};
use crate::config::Config;
use crate::durable::DurablePersistence;
use crate::lifecycle::{join_grpc_threads, spawn_grpc_thread, spawn_local_thread};
use crate::node_handler::{NodeHandler, NodeHandlerCore};
use crate::raft_loop::{COMMAND_CHANNEL_DEPTH, RaftLoop};
use crate::server::{NodeServerRuntime, Server};
use ledger::ledger::Ledger;
use raft::{RaftConfig, RaftNode, Role};
use spdlog::{error, info};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tokio::sync::{Notify, mpsc};

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
    /// raft loop, no Node service, no peers. The Ledger gRPC runs on
    /// its own OS thread + dedicated `current_thread` tokio runtime
    /// (matches the clustered shape so single-node and clustered code
    /// paths share the same isolation model).
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
        let client_thread = spawn_grpc_thread("ledger-grpc", async move {
            if let Err(e) = server.run().await {
                error!("standalone ledger gRPC server exited: {}", e);
            }
        })?;
        info!(
            "standalone: client-facing Ledger gRPC up on {} (dedicated thread)",
            client_addr
        );

        Ok(StandaloneHandles {
            client_thread: Some(client_thread),
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
        // The ledger only surfaces a single watermark today (its
        // committed end) — pre-refactor this was conflated with the
        // raft-log write extent. ADR-0017 §"Required Invariants"
        // notes the implicit recovery invariant
        // `ledger.last_commit_id <= raft_wal.last_tx_id`; until the
        // production ledger surfaces the WAL extent separately we
        // collapse write and commit at boot.
        let durable_last_tx_id = self.ledger_slot.ledger().last_commit_id();
        if durable_last_tx_id > 0 {
            node.advance(durable_last_tx_id, durable_last_tx_id);
            // Lazy-arm the election timer so the raft loop's first
            // iteration sees a populated wakeup deadline.
            let _ = node.election().tick(Instant::now());
        }
        // Snapshot initial state so consumers see post-construction values.
        mirror.snapshot_from(&node);

        // Two inbound channels: AE → raft command loop, RV →
        // consensus loop. The supervisor keeps a clone of each
        // (held in `ClusterHandles` so Drop can release them
        // explicitly), gRPC handlers clone per request, and the
        // receivers move into the raft loop on the node-grpc thread
        // (where `RaftLoop::run` spawns `ConsensusLoop` and gives it
        // `rv_rx`).
        let (ae_tx, ae_rx) = mpsc::channel::<AppendEntriesMsg>(COMMAND_CHANNEL_DEPTH);
        let (rv_tx, rv_rx) = mpsc::channel::<RequestVoteMsg>(COMMAND_CHANNEL_DEPTH);

        // Client-facing Ledger gRPC server (writable on leader,
        // read-only on follower; consults mirror for routing). Runs on
        // its own OS thread + dedicated `current_thread` runtime so
        // ledger I/O scheduling cannot starve the peer-facing service.
        let client_shutdown = Arc::new(Notify::new());
        let client_server = Server::new(
            self.ledger_slot.clone(),
            client_addr,
            mirror.clone(),
            self.durable.term.clone(),
            client_shutdown.clone(),
        );
        let client_thread = spawn_grpc_thread("ledger-grpc", async move {
            if let Err(e) = client_server.run().await {
                error!("ledger gRPC server exited: {}", e);
            }
        })?;

        // Peer-facing Node gRPC server. Runs on its own OS thread +
        // dedicated `current_thread` runtime, co-located with the raft
        // loop on a `LocalSet`. The raft loop owns
        // `Rc<RefCell<RaftNode>>` and is `!Send`, so a LocalSet (rather
        // than the multi-threaded host runtime) is what lets it run at
        // all. Co-locating it with the gRPC handlers means inbound
        // RequestVote / AppendEntries don't pay a cross-runtime hop:
        // handler → mpsc::Sender → cmd_rx.recv() all happens on the
        // same OS thread.
        let node_core = Arc::new(NodeHandlerCore::new(
            self.ledger_slot.clone(),
            self_id,
            mirror.clone(),
            ae_tx.clone(),
            rv_tx.clone(),
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

        let raft_inputs = (
            node,
            self.ledger_slot.clone(),
            mirror.clone(),
            Arc::new(self.config.clone()),
            ae_rx,
            rv_rx,
        );
        let node_grpc_thread = spawn_local_thread("node-grpc", move || async move {
            let (node, ledger_slot, mirror, config, ae_rx, rv_rx) = raft_inputs;
            let raft_loop = RaftLoop::new(node, ledger_slot, mirror, config, ae_rx, rv_rx);
            // Drive both the gRPC server and the raft loop on the
            // same LocalSet. The raft loop exits when the last
            // `cmd_tx` clone drops; we await its handle after the
            // gRPC server returns so the LocalSet doesn't tear down
            // pending raft work mid-step.
            let raft_local = tokio::task::spawn_local(raft_loop.run());
            if let Err(e) = node_runtime.run().await {
                panic!("node gRPC server exited: {}", e);
            }
            // Top-end of the raft loop's spawn: a panic anywhere
            // inside `RaftLoop::run` (or any task it spawns and joins)
            // surfaces here as `JoinError::is_panic()`. Re-raise it so
            // the LocalSet thread aborts and the supervisor sees the
            // failure — silent loop death has corrupted-state risk.
            match raft_local.await {
                Ok(()) => {}
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => panic!("raft_local task cancelled: {}", e),
            }
        })?;

        info!(
            "cluster: bring-up complete (node_id={}, peers={}, client={}, node={}; \
             ledger-grpc on its own thread, node-grpc + raft co-hosted on a LocalSet thread)",
            self_id,
            cluster.peers.len(),
            client_addr,
            node_addr,
        );

        Ok(ClusterHandles {
            client_thread: Some(client_thread),
            node_grpc_thread: Some(node_grpc_thread),
            mirror,
            ae_tx: Some(ae_tx),
            rv_tx: Some(rv_tx),
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

    /// Borrow the clustered handles, or `None` for a standalone
    /// bring-up. Symmetric with [`Self::as_standalone`].
    pub fn as_cluster(&self) -> Option<&ClusterHandles> {
        match self {
            Handles::Cluster(h) => Some(h),
            _ => None,
        }
    }

    /// Borrow the standalone handles, or `None` for a clustered
    /// bring-up. Symmetric with [`Self::as_cluster`].
    pub fn as_standalone(&self) -> Option<&StandaloneHandles> {
        match self {
            Handles::Standalone(h) => Some(h),
            _ => None,
        }
    }
}

// ── Standalone handles ──────────────────────────────────────────────────

/// Handles produced by a successful standalone bring-up. Single
/// gRPC server, nothing else.
pub struct StandaloneHandles {
    /// OS thread hosting the client-facing Ledger gRPC server's
    /// dedicated `current_thread` tokio runtime. Joined cooperatively
    /// in `Drop` after `client_shutdown` fires.
    pub client_thread: Option<thread::JoinHandle<()>>,
    /// Cooperative shutdown trigger for the client-facing Ledger
    /// gRPC server. `Drop` calls `notify_waiters()` so
    /// `serve_with_shutdown` resolves and the dedicated runtime exits.
    pub client_shutdown: Arc<Notify>,
}

impl Drop for StandaloneHandles {
    fn drop(&mut self) {
        self.client_shutdown.notify_waiters();
        let client = self.client_thread.take();
        join_grpc_threads("standalone", [client]);
    }
}

// ── Clustered handles ───────────────────────────────────────────────────

/// Handles produced by a successful clustered bring-up. Owns both
/// gRPC server OS threads (the node-grpc thread also hosts the raft
/// loop on a `LocalSet`) and the mirror clone.
pub struct ClusterHandles {
    /// OS thread hosting the client-facing Ledger gRPC server's
    /// dedicated `current_thread` tokio runtime.
    pub client_thread: Option<thread::JoinHandle<()>>,
    /// OS thread hosting the peer-facing Node gRPC server **and** the
    /// raft loop, both driven by a single `current_thread` runtime
    /// wrapped in a `LocalSet`. Joining this thread implicitly drains
    /// the raft loop (it exits when the last `cmd_tx` clone drops,
    /// which the gRPC server's `serve_with_shutdown` causes during
    /// drain).
    pub node_grpc_thread: Option<thread::JoinHandle<()>>,
    /// Lock-free read surface for raft state.
    pub mirror: Arc<ClusterMirror>,
    /// AE channel sender, held here only so `Drop` can drop it
    /// explicitly (the gRPC handlers each hold their own clones).
    /// Wrapped in `Option` so `Drop` can take it before joining the
    /// node-grpc thread — otherwise the raft loop would never see its
    /// channel close and the thread would never exit.
    ae_tx: Option<mpsc::Sender<AppendEntriesMsg>>,
    /// RV channel sender, twin of `ae_tx`. Drops to release the
    /// consensus loop's receiver.
    rv_tx: Option<mpsc::Sender<RequestVoteMsg>>,
    /// Cooperative shutdown trigger for the client-facing gRPC server.
    pub client_shutdown: Arc<Notify>,
    /// Cooperative shutdown trigger for the peer-facing gRPC server.
    pub node_shutdown: Arc<Notify>,
}

impl Drop for ClusterHandles {
    fn drop(&mut self) {
        // Stop both gRPC servers from accepting new connections.
        // In-flight handlers continue running on their dedicated
        // runtimes and are joined below; new connections immediately
        // fail at the tonic layer.
        self.client_shutdown.notify_waiters();
        self.node_shutdown.notify_waiters();

        // Drop our channel-sender clones before joining the node-grpc
        // thread. The gRPC handlers still hold their per-request
        // clones, so each channel only closes once the last handler
        // completes — at which point the raft / consensus loops see
        // `recv() == None` and the LocalSet thread exits.
        self.ae_tx.take();
        self.rv_tx.take();

        // Drain in dependency order:
        //   gRPC threads → tonic drains in-flight handlers → handlers'
        //   cmd_tx clones drop → channel closes → raft loop exits →
        //   node-grpc LocalSet thread exits.
        let client = self.client_thread.take();
        let node = self.node_grpc_thread.take();
        join_grpc_threads("cluster", [client, node]);
    }
}
