//! Node service server. Handles peer-to-peer RPCs (`AppendEntries`, `Ping`).

use crate::cluster::proto;
use crate::cluster::proto::node_server::{Node, NodeServer};
use crate::ledger::Ledger;
use crate::wal_tail::decode_records;
use spdlog::{info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub struct NodeHandler {
    ledger: Arc<Ledger>,
    node_id: u64,
    term: u64,
    role: proto::NodeRole,
}

impl NodeHandler {
    pub fn new(ledger: Arc<Ledger>, node_id: u64, term: u64, role: proto::NodeRole) -> Self {
        Self {
            ledger,
            node_id,
            term,
            role,
        }
    }
}

#[tonic::async_trait]
impl Node for NodeHandler {
    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let req = request.into_inner();

        // Leader never accepts AppendEntries under ADR-015 (static roles).
        if self.role == proto::NodeRole::Leader {
            return Ok(Response::new(proto::AppendEntriesResponse {
                term: self.term,
                success: false,
                last_tx_id: 0,
                reject_reason: proto::RejectReason::RejectNotFollower as u32,
            }));
        }
        let last = self.ledger.last_commit_id();

        let entries = decode_records(&req.wal_bytes);
        if entries.is_empty() {
            return Ok(Response::new(proto::AppendEntriesResponse {
                term: self.term,
                success: true,
                last_tx_id: last,
                reject_reason: proto::RejectReason::RejectNone as u32,
            }));
        }

        match self.ledger.append_wal_entries(entries) {
            Ok(()) => Ok(Response::new(proto::AppendEntriesResponse {
                term: self.term,
                success: true,
                last_tx_id: last,
                reject_reason: proto::RejectReason::RejectNone as u32,
            })),
            Err(e) => {
                warn!("append_entries failed on node {}: {}", self.node_id, e);
                Ok(Response::new(proto::AppendEntriesResponse {
                    term: self.term,
                    success: false,
                    last_tx_id: last,
                    reject_reason: proto::RejectReason::RejectWalAppendFailed as u32,
                }))
            }
        }
    }

    async fn ping(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(proto::PingResponse {
            node_id: self.node_id,
            term: self.term,
            last_tx_id: self.ledger.last_commit_id(),
            role: self.role as i32,
            nonce: req.nonce,
        }))
    }

    async fn request_vote(
        &self,
        _request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        Err(Status::unimplemented("RequestVote deferred to ADR-016"))
    }

    async fn install_snapshot(
        &self,
        _request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        Err(Status::unimplemented("InstallSnapshot deferred to ADR-016"))
    }
}

pub struct NodeServerRuntime {
    addr: SocketAddr,
    handler: NodeHandler,
    max_message_bytes: usize,
}

impl NodeServerRuntime {
    /// `max_message_bytes` bounds both inbound `AppendEntries` decoding and
    /// outbound response encoding. Must be at least as large as the leader's
    /// `append_entries_max_bytes` + protobuf framing overhead.
    pub fn new(addr: SocketAddr, handler: NodeHandler, max_message_bytes: usize) -> Self {
        Self {
            addr,
            handler,
            max_message_bytes,
        }
    }

    fn service(handler: NodeHandler, max_bytes: usize) -> NodeServer<NodeHandler> {
        NodeServer::new(handler)
            .max_decoding_message_size(max_bytes)
            .max_encoding_message_size(max_bytes)
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Node gRPC server listening on {}", self.addr);
        Server::builder()
            .add_service(Self::service(self.handler, self.max_message_bytes))
            .serve(self.addr)
            .await?;
        Ok(())
    }

    pub async fn run_with_shutdown<F>(
        self,
        shutdown: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Future<Output = ()>,
    {
        info!("Node gRPC server listening on {}", self.addr);
        Server::builder()
            .add_service(Self::service(self.handler, self.max_message_bytes))
            .serve_with_shutdown(self.addr, shutdown)
            .await?;
        Ok(())
    }
}
