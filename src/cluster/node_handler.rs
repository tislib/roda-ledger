//! `NodeHandler` — gRPC service implementation for peer-to-peer RPCs
//! (`AppendEntries`, `Ping`). The server runtime that hosts it lives in
//! [`crate::cluster::server`].

use crate::cluster::proto::node as proto;
use crate::cluster::proto::node::node_server::Node;
use crate::cluster::{ClusterCommitIndex, Term};
use crate::ledger::Ledger;
use crate::wal_tail::decode_records;
use spdlog::warn;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct NodeHandler {
    ledger: Arc<Ledger>,
    node_id: u64,
    /// Shared term state. Followers `observe()` on every incoming
    /// `AppendEntries` so their durable log tracks the leader's term
    /// transitions; all handlers read `get_current_term()` for the
    /// response `term` field.
    term: Arc<Term>,
    role: proto::NodeRole,
    /// Follower-only: watermark updated on every successful
    /// `AppendEntries`. `None` on the leader (its NodeHandler rejects
    /// incoming AppendEntries and never reaches the success path).
    cluster_commit_index: Option<Arc<ClusterCommitIndex>>,
}

impl NodeHandler {
    pub fn new(
        ledger: Arc<Ledger>,
        node_id: u64,
        term: Arc<Term>,
        role: proto::NodeRole,
        cluster_commit_index: Option<Arc<ClusterCommitIndex>>,
    ) -> Self {
        Self {
            ledger,
            node_id,
            term,
            role,
            cluster_commit_index,
        }
    }

    #[inline]
    fn current_term(&self) -> u64 {
        self.term.get_current_term()
    }

    /// Advance the follower's cluster-commit watermark to the leader's
    /// advertised value. No clamping — `LedgerHandler::wait_for_transaction_level`
    /// guards any use by also requiring the follower's own `commit_index`
    /// and `snapshot_index` to have caught up. No-op on the leader
    /// (which holds `None` and never reaches the success path anyway).
    #[inline]
    fn advance_cluster_commit(&self, leader_commit_tx_id: u64) {
        if let Some(ref cci) = self.cluster_commit_index {
            cci.set_from_leader(leader_commit_tx_id);
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
                term: self.current_term(),
                success: false,
                last_tx_id: 0,
                reject_reason: proto::RejectReason::RejectNotFollower as u32,
            }));
        }

        // Follower side: every AppendEntries carries the leader's current
        // term + the first tx_id the leader was writing. Durably observe
        // the term (no-op on same-term, error on regression) *before*
        // applying any entries so the term log is always ≥ the committed
        // log. `from_tx_id` is the start of the batch — a good proxy for
        // the term's start when the leader bumps term on boot at tx 0.
        if req.term != 0
            && let Err(e) = self.term.observe(req.term, req.from_tx_id)
        {
            warn!(
                "append_entries: term observe failed on node {} (incoming={}): {}",
                self.node_id, req.term, e
            );
            return Ok(Response::new(proto::AppendEntriesResponse {
                term: self.current_term(),
                success: false,
                last_tx_id: self.ledger.last_commit_id(),
                reject_reason: proto::RejectReason::RejectTermStale as u32,
            }));
        }

        let last = self.ledger.last_commit_id();

        let entries = decode_records(&req.wal_bytes);
        if entries.is_empty() {
            // Empty heartbeat: publish the leader's advertised watermark.
            // Readers also consult this follower's own commit / snapshot
            // indices, so there's no correctness need to clamp here.
            self.advance_cluster_commit(req.leader_commit_tx_id);
            return Ok(Response::new(proto::AppendEntriesResponse {
                term: self.current_term(),
                success: true,
                last_tx_id: last,
                reject_reason: proto::RejectReason::RejectNone as u32,
            }));
        }

        match self.ledger.append_wal_entries(entries) {
            Ok(()) => {
                self.advance_cluster_commit(req.leader_commit_tx_id);
                Ok(Response::new(proto::AppendEntriesResponse {
                    term: self.current_term(),
                    success: true,
                    last_tx_id: last,
                    reject_reason: proto::RejectReason::RejectNone as u32,
                }))
            }
            Err(e) => {
                warn!("append_entries failed on node {}: {}", self.node_id, e);
                Ok(Response::new(proto::AppendEntriesResponse {
                    term: self.current_term(),
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
            term: self.current_term(),
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
