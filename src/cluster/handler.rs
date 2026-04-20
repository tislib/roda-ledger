//! gRPC server-side implementation of `roda.node.v1.Node` (ADR-015).
//!
//! The leader returns `REJECT_NOT_FOLLOWER` on `AppendEntries`; only
//! followers hand off to the `Replication` stage. `Ping` works in both
//! roles. `RequestVote` and `InstallSnapshot` are `UNIMPLEMENTED` until
//! ADR-016.

use crate::cluster::config::{ClusterConfig, NodeMode};
use crate::cluster::proto;
use crate::cluster::proto::node_server::Node;
use crate::ledger::Ledger as InternalLedger;
use crate::replication::{AppendEntries, AppendError, Replication};
use spdlog::{debug, warn};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Follower-side handler. On the leader, the `Replication` field is
/// unused and every `AppendEntries` returns `REJECT_NOT_FOLLOWER`.
pub struct NodeHandler {
    ledger: Arc<InternalLedger>,
    cluster: Arc<ClusterConfig>,
    /// Present only when `cluster.mode == Follower`. `Arc`ed so it can
    /// be cloned into the blocking task the handler spawns per RPC.
    replication: Option<Arc<Replication>>,
}

impl NodeHandler {
    pub fn new(ledger: Arc<InternalLedger>, cluster: Arc<ClusterConfig>) -> Self {
        let replication = if cluster.mode == NodeMode::Follower {
            Some(Arc::new(Replication::new(
                ledger.ledger_context(),
                cluster.node_id,
                cluster.current_term,
            )))
        } else {
            None
        };
        Self {
            ledger,
            cluster,
            replication,
        }
    }

    /// Observe the follower's local `last_tx_id` — currently the WAL
    /// commit watermark. On a follower that has fsynced through tx N,
    /// `last_commit_id()` == N.
    fn last_local_tx_id(&self) -> u64 {
        self.ledger.last_commit_id()
    }
}

#[tonic::async_trait]
impl Node for NodeHandler {
    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        // Leader-side: refuse. Leaders never accept AppendEntries in
        // ADR-015's static-leader model (only ADR-016's demoted leader
        // would — and that's a follower for protocol purposes).
        let Some(replication) = self.replication.clone() else {
            debug!("AppendEntries received on leader node — rejecting");
            return Ok(Response::new(proto::AppendEntriesResponse {
                term: self.cluster.current_term,
                success: false,
                last_tx_id: self.ledger.last_commit_id(),
                reject_reason: proto::RejectReason::RejectNotFollower as u32,
            }));
        };

        let req = request.into_inner();
        let last_local_tx_id = self.last_local_tx_id();

        // Ownership dance: `wal_bytes` is a `Vec<u8>` owned by `req`.
        // The blocking task needs to own it too, so we move `req` in.
        let result = tokio::task::spawn_blocking(move || {
            let append = AppendEntries {
                term: req.term,
                prev_tx_id: req.prev_tx_id,
                prev_term: req.prev_term,
                from_tx_id: req.from_tx_id,
                to_tx_id: req.to_tx_id,
                wal_bytes: &req.wal_bytes,
                leader_commit_tx_id: req.leader_commit_tx_id,
            };
            replication.process(append, last_local_tx_id)
        })
        .await
        .map_err(|e| Status::internal(format!("replication join error: {}", e)))?;

        match result {
            Ok(ok) => Ok(Response::new(proto::AppendEntriesResponse {
                term: ok.term,
                success: true,
                last_tx_id: ok.last_tx_id,
                reject_reason: proto::RejectReason::RejectNone as u32,
            })),
            Err(AppendError {
                term,
                reason,
                detail,
            }) => {
                warn!("AppendEntries rejected: {:?}: {}", reason, detail);
                Ok(Response::new(proto::AppendEntriesResponse {
                    term,
                    success: false,
                    last_tx_id: self.ledger.last_commit_id(),
                    reject_reason: map_reject_reason(reason) as u32,
                }))
            }
        }
    }

    async fn ping(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        let req = request.into_inner();
        let role = match self.cluster.mode {
            NodeMode::Leader => proto::NodeRole::Leader,
            NodeMode::Follower => proto::NodeRole::Follower,
        };
        Ok(Response::new(proto::PingResponse {
            node_id: self.cluster.node_id,
            term: self.cluster.current_term,
            last_tx_id: self.ledger.last_commit_id(),
            role: role as i32,
            nonce: req.nonce,
        }))
    }

    async fn request_vote(
        &self,
        _request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        Err(Status::unimplemented(
            "RequestVote is reserved for ADR-016 (elections)",
        ))
    }

    async fn install_snapshot(
        &self,
        _request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        Err(Status::unimplemented(
            "InstallSnapshot is reserved for ADR-016 (snapshot catch-up)",
        ))
    }
}

fn map_reject_reason(r: crate::replication::RejectReason) -> proto::RejectReason {
    use crate::replication::RejectReason as R;
    match r {
        R::None => proto::RejectReason::RejectNone,
        R::TermStale => proto::RejectReason::RejectTermStale,
        R::PrevMismatch => proto::RejectReason::RejectPrevMismatch,
        R::CrcFailed => proto::RejectReason::RejectCrcFailed,
        R::SequenceInvalid => proto::RejectReason::RejectSequenceInvalid,
        R::WalAppendFailed => proto::RejectReason::RejectWalAppendFailed,
        R::NotFollower => proto::RejectReason::RejectNotFollower,
    }
}
