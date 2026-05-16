use crate::consensus::consensus::Consensus;
use ::proto::node::node_server::Node;
use ledger::ledger::Ledger;
use proto::node::{
    InstallSnapshotRequest, InstallSnapshotResponse, PingRequest, PingResponse,
    ReplicationFollowerMessage, ReplicationLeaderMessage, RequestVoteRequest, RequestVoteResponse,
};
use spdlog::error;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// Thin wrapper that owns an `Arc<NodeHandlerCore>` and implements the
/// tonic `Node` service.
pub struct NodeHandler {
    ledger: Arc<Ledger>,
    consensus: Arc<Consensus>,
}

impl NodeHandler {
    pub fn new(ledger: Arc<Ledger>, consensus: Arc<Consensus>) -> Self {
        Self { ledger, consensus }
    }
}

#[tonic::async_trait]
impl Node for NodeHandler {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        Ok(Response::new(
            self.consensus.request_vote(request.into_inner()),
        ))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(self.consensus.ping(request.into_inner())))
    }

    async fn install_snapshot(
        &self,
        _request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        Err(Status::unimplemented("InstallSnapshot deferred to ADR-016"))
    }

    type ReplicationStream = ReceiverStream<Result<ReplicationFollowerMessage, Status>>;

    async fn replication(
        &self,
        request: Request<Streaming<ReplicationLeaderMessage>>,
    ) -> Result<Response<Self::ReplicationStream>, Status> {
        let stream = self
            .consensus
            .replication_stream(request.into_inner())
            .await;
        Ok(Response::new(stream))
    }
}
