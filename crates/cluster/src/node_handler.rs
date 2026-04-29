//! `NodeHandler` — gRPC service implementation for peer-to-peer RPCs
//! (`AppendEntries`, `Ping`, `RequestVote`, `InstallSnapshot`).
//!
//! After the ADR-0017 migration the handler is a thin translation layer:
//! every mutating RPC is forwarded to [`RaftHandle`] which holds the raft
//! state machine behind one `tokio::sync::Mutex`. The handler itself owns
//! no raft state.
//!
//! `Ping` is read-only (consults [`ClusterMirror`] + the ledger), so it
//! does not lock the raft mutex.

use crate::cluster_mirror::ClusterMirror;
use crate::ledger_slot::LedgerSlot;
use crate::raft_driver::RaftHandle;
use ::proto::node as proto;
use ::proto::node::node_server::Node;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tonic::{Request, Response, Status};

/// State shared across the gRPC service handler and the supervisor.
/// Constructed once at cluster bring-up and kept alive for the process
/// lifetime; the gRPC server holds an `Arc` and observes every
/// raft-driver mutation atomically.
pub struct NodeHandlerCore {
    /// Indirection to the live `Arc<Ledger>` (ADR-0016 §9). Used by the
    /// read-only `Ping` handler — every mutating RPC goes through the
    /// raft driver, which holds its own `LedgerSlot` reference.
    pub ledger: Arc<LedgerSlot>,
    pub node_id: u64,
    /// Lock-free read surface for role / term stamping in `Ping` and
    /// for shutdown latch checks.
    pub mirror: Arc<ClusterMirror>,
    /// The raft driver. All mutating RPCs delegate here.
    pub raft: Arc<RaftHandle>,
    /// Shutdown latch — when set, every gRPC handler returns
    /// `Status::unavailable` immediately.
    ///
    /// This is the in-process equivalent of "the process is gone".
    /// Tonic's `JoinHandle::abort` cancels the listen task but cannot
    /// stop already-spawned per-RPC handler tasks; without this latch,
    /// the dying follower's in-flight handlers complete normally,
    /// returning fresh `last_tx_id` values that the leader's outbound
    /// dispatch advances into raft's quorum tracker — which is the
    /// exact behaviour a hard crash would NOT exhibit.
    ///
    /// The supervisor's shutdown path flips this latch *before*
    /// aborting handles, so any handler that hasn't yet entered its
    /// state-touching body refuses; in-flight handlers that already
    /// passed the check finish naturally but no new ones start.
    pub shutdown: AtomicBool,
}

impl NodeHandlerCore {
    pub fn new(
        ledger: Arc<LedgerSlot>,
        node_id: u64,
        mirror: Arc<ClusterMirror>,
        raft: Arc<RaftHandle>,
    ) -> Self {
        Self {
            ledger,
            node_id,
            mirror,
            raft,
            shutdown: AtomicBool::new(false),
        }
    }

    /// Flip the shutdown latch. Idempotent.
    #[inline]
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Check the shutdown latch. Inlined into every handler's first line.
    #[inline]
    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

/// Thin wrapper that owns an `Arc<NodeHandlerCore>` and implements the
/// tonic `Node` service.
pub struct NodeHandler {
    core: Arc<NodeHandlerCore>,
}

impl NodeHandler {
    pub fn new(core: Arc<NodeHandlerCore>) -> Self {
        Self { core }
    }

    /// Test/observability accessor — exposes the shared core.
    pub fn core(&self) -> &Arc<NodeHandlerCore> {
        &self.core
    }
}

#[tonic::async_trait]
impl Node for NodeHandler {
    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let core = &self.core;
        if core.is_shutdown() {
            return Err(Status::unavailable("node shutting down"));
        }
        let req = request.into_inner();
        let resp = core.raft.handle_append_entries(req).await;
        Ok(Response::new(resp))
    }

    async fn request_vote(
        &self,
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        let core = &self.core;
        if core.is_shutdown() {
            return Err(Status::unavailable("node shutting down"));
        }
        let req = request.into_inner();
        let resp = core.raft.handle_request_vote(req).await;
        Ok(Response::new(resp))
    }

    async fn ping(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        let core = &self.core;
        if core.is_shutdown() {
            return Err(Status::unavailable("node shutting down"));
        }
        let req = request.into_inner();
        Ok(Response::new(proto::PingResponse {
            node_id: core.node_id,
            term: core.mirror.current_term(),
            last_tx_id: core.ledger.ledger().last_commit_id(),
            role: core.mirror.role_proto() as i32,
            nonce: req.nonce,
        }))
    }

    async fn install_snapshot(
        &self,
        _request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        if self.core.is_shutdown() {
            return Err(Status::unavailable("node shutting down"));
        }
        Err(Status::unimplemented("InstallSnapshot deferred to ADR-016"))
    }
}
