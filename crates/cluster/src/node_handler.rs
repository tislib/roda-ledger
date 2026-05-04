//! `NodeHandler` — gRPC service implementation for peer-to-peer RPCs
//! (`AppendEntries`, `Ping`, `RequestVote`, `InstallSnapshot`).
//!
//! Each mutating RPC posts a typed message to its dedicated channel:
//! [`AppendEntriesMsg`] → `RaftLoop`'s command loop, [`RequestVoteMsg`]
//! → `ConsensusLoop`. The handler owns no raft state; it just bridges
//! the gRPC framing and the loop-side `oneshot` reply channel.
//!
//! `Ping` is read-only (consults [`ClusterMirror`] + the ledger) so it
//! does not go through either loop.
//!
//! Shutdown is RAII: when every sender clone of a channel drops the
//! corresponding loop exits. From the handler's side a `send().await`
//! failure surfaces as `Status::unavailable`. There is no separate
//! latch and no race window between "check latch" and "enter raft body".

use crate::cluster_mirror::ClusterMirror;
use crate::command::{AppendEntriesMsg, RequestVoteMsg};
use crate::ledger_slot::LedgerSlot;
use ::proto::node as proto;
use ::proto::node::node_server::Node;
use spdlog::error;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

/// State shared across the gRPC service handler. Constructed once at
/// cluster bring-up and kept alive for the process lifetime; the gRPC
/// server holds an `Arc` and clones the per-RPC senders per-request.
pub struct NodeHandlerCore {
    /// Indirection to the live `Arc<Ledger>` (ADR-0016 §9). Used by
    /// the read-only `Ping` handler — every mutating RPC goes through
    /// the relevant loop.
    pub ledger: Arc<LedgerSlot>,
    pub node_id: u64,
    /// Lock-free read surface for role / term stamping in `Ping`.
    pub mirror: Arc<ClusterMirror>,
    /// Sender into `RaftLoop`'s AE command loop.
    pub ae_tx: mpsc::Sender<AppendEntriesMsg>,
    /// Sender into `ConsensusLoop`'s RV channel.
    pub rv_tx: mpsc::Sender<RequestVoteMsg>,
}

impl NodeHandlerCore {
    pub fn new(
        ledger: Arc<LedgerSlot>,
        node_id: u64,
        mirror: Arc<ClusterMirror>,
        ae_tx: mpsc::Sender<AppendEntriesMsg>,
        rv_tx: mpsc::Sender<RequestVoteMsg>,
    ) -> Self {
        Self {
            ledger,
            node_id,
            mirror,
            ae_tx,
            rv_tx,
        }
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
        let node_id = self.core.node_id;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.core
            .ae_tx
            .send(AppendEntriesMsg {
                req: request.into_inner(),
                reply: reply_tx,
            })
            .await
            .map_err(|e| {
                error!(
                    "node_handler[{}]: AppendEntries ae_tx.send failed (raft loop gone): {}",
                    node_id, e
                );
                Status::unavailable("raft loop unavailable")
            })?;
        let resp = reply_rx.await.map_err(|e| {
            error!(
                "node_handler[{}]: AppendEntries reply oneshot recv failed (raft loop dropped reply): {}",
                node_id, e
            );
            Status::internal("raft loop dropped reply")
        })?;
        Ok(Response::new(resp))
    }

    async fn request_vote(
        &self,
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        let node_id = self.core.node_id;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.core
            .rv_tx
            .send(RequestVoteMsg {
                req: request.into_inner(),
                reply: reply_tx,
            })
            .await
            .map_err(|e| {
                error!(
                    "node_handler[{}]: RequestVote rv_tx.send failed (consensus loop gone): {}",
                    node_id, e
                );
                Status::unavailable("consensus loop unavailable")
            })?;
        let resp = reply_rx.await.map_err(|e| {
            error!(
                "node_handler[{}]: RequestVote reply oneshot recv failed (consensus loop dropped reply): {}",
                node_id, e
            );
            Status::internal("consensus loop dropped reply")
        })?;
        Ok(Response::new(resp))
    }

    async fn ping(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        let core = &self.core;
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
        Err(Status::unimplemented("InstallSnapshot deferred to ADR-016"))
    }
}
