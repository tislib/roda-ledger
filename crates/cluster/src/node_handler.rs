//! `NodeHandler` — gRPC service implementation for peer-to-peer RPCs
//! (`AppendEntries`, `Ping`, `RequestVote`, `InstallSnapshot`).
//!
//! The handler is a thin translation layer: every mutating RPC is
//! wrapped in a [`Command`] and posted to the raft loop's mpsc channel
//! along with a `oneshot` reply channel. The handler awaits the
//! oneshot and returns the response. The handler owns no raft state.
//!
//! `Ping` is read-only (consults [`ClusterMirror`] + the ledger) so it
//! does not go through the raft loop.
//!
//! Shutdown is RAII on the `cmd_tx` side: when the supervisor drops
//! its sender clone and the loop drains, this handler's
//! `cmd_tx.send()` calls return errors and the handler reports
//! `Status::unavailable`. Closing the channel *is* the shutdown
//! signal — there is no separate latch and no race window between
//! "check latch" and "enter raft body".

use std::cell::RefCell;
use std::rc::Rc;
use crate::cluster_mirror::ClusterMirror;
use crate::ledger_slot::LedgerSlot;
use ::proto::node as proto;
use ::proto::node::node_server::Node;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};
use crate::command::Command;

/// State shared across the gRPC service handler. Constructed once at
/// cluster bring-up and kept alive for the process lifetime; the gRPC
/// server holds an `Arc` and clones the `cmd_tx` per-request.
pub struct NodeHandlerCore {
    /// Indirection to the live `Arc<Ledger>` (ADR-0016 §9). Used by
    /// the read-only `Ping` handler — every mutating RPC goes through
    /// the raft loop, which holds its own `LedgerSlot` reference.
    pub ledger: Arc<LedgerSlot>,
    pub node_id: u64,
    /// Lock-free read surface for role / term stamping in `Ping`.
    pub mirror: Arc<ClusterMirror>,
    /// Sender into the raft loop. Cloned per RPC; dropping the loop's
    /// receiver causes `send().await` to fail, which surfaces as
    /// `Status::unavailable` to the caller — the in-process equivalent
    /// of "the process is gone".
    pub cmd_tx: mpsc::Sender<Command>,
}

impl NodeHandlerCore {
    pub fn new(
        ledger: Arc<LedgerSlot>,
        node_id: u64,
        mirror: Arc<ClusterMirror>,
        cmd_tx: mpsc::Sender<Command>,
    ) -> Self {
        Self {
            ledger,
            node_id,
            mirror,
            cmd_tx,
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
        let (reply_tx, reply_rx) = oneshot::channel();
        self.core
            .cmd_tx
            .send(Command::AppendEntries {
                req: request.into_inner(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| Status::unavailable("raft loop unavailable"))?;
        let resp = reply_rx
            .await
            .map_err(|_| Status::internal("raft loop dropped reply"))?;
        Ok(Response::new(resp))
    }

    async fn request_vote(
        &self,
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.core
            .cmd_tx
            .send(Command::RequestVote {
                req: request.into_inner(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| Status::unavailable("raft loop unavailable"))?;
        let resp = reply_rx
            .await
            .map_err(|_| Status::internal("raft loop dropped reply"))?;
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