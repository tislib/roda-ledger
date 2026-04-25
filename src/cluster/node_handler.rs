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
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as AsyncMutex;
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
    /// ADR-0016 §7. Single per-node mutex serialising every Node
    /// service handler that mutates persistent state
    /// (`AppendEntries`, `RequestVote`, `InstallSnapshot`). tonic
    /// dispatches RPCs in parallel by default; without this lock the
    /// `(currentTerm, votedFor, log)` invariant could be violated by
    /// interleaved handlers. `Ping` is read-only and intentionally
    /// **not** gated by this lock.
    node_mutex: Arc<AsyncMutex<()>>,
    /// ADR-0016 §9. Whenever `append_entries` detects log divergence
    /// (`prev_tx_id` exists locally but the term that covered it does
    /// not match `prev_term`), the rejecting request's
    /// `leader_commit_tx_id` is stashed here for the cluster
    /// supervisor (Stage 3) to consume as the recovery-mode reseed
    /// watermark.
    ///
    /// `0` means "no divergence observed yet". The supervisor reads
    /// and resets this from a single thread, so a plain `AtomicU64`
    /// is sufficient.
    last_divergence_watermark: Arc<AtomicU64>,
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
            node_mutex: Arc::new(AsyncMutex::new(())),
            last_divergence_watermark: Arc::new(AtomicU64::new(0)),
        }
    }

    #[inline]
    fn current_term(&self) -> u64 {
        self.term.get_current_term()
    }

    /// Read (and clear) the most recent divergence watermark stashed
    /// by `append_entries`. Returns `None` when no divergence has
    /// been observed since the last call. Used by the cluster
    /// supervisor (Stage 3) to drive the recovery-mode reseed.
    pub fn take_divergence_watermark(&self) -> Option<u64> {
        let v = self.last_divergence_watermark.swap(0, Ordering::AcqRel);
        if v == 0 { None } else { Some(v) }
    }

    /// Observability handle on the divergence watermark. Used by
    /// tests; the supervisor uses `take_divergence_watermark`.
    #[inline]
    pub fn divergence_watermark(&self) -> u64 {
        self.last_divergence_watermark.load(Ordering::Acquire)
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

    /// ADR-0016 §8 prev-log-entry consistency check.
    ///
    /// Returns `Ok(())` when the follower's log at `prev_tx_id` was
    /// written under `prev_term` (Raft's §5.3 log-matching guarantee
    /// is locally satisfied), `Err(reject_reason)` otherwise.
    ///
    /// Specifically:
    /// - `prev_tx_id == 0` is the leader's "I have nothing before
    ///   `from_tx_id`" sentinel — accept unconditionally.
    /// - `prev_tx_id > our last_commit_id` → gap; we don't have it
    ///   yet. The leader will retry on the next tick after we
    ///   advertise our `last_tx_id` in the response.
    /// - We have `prev_tx_id` but the term that covered it differs
    ///   from `prev_term` → **divergence**. Stash
    ///   `leader_commit_tx_id` as the reseed watermark for the
    ///   supervisor and reject. Idempotent: if a previous divergence
    ///   was stashed and not yet consumed, we update to the latest
    ///   value.
    fn check_prev_log(
        &self,
        prev_tx_id: u64,
        prev_term: u64,
        leader_commit_tx_id: u64,
    ) -> Result<(), proto::RejectReason> {
        if prev_tx_id == 0 {
            return Ok(());
        }
        let our_last = self.ledger.last_commit_id();
        if prev_tx_id > our_last {
            return Err(proto::RejectReason::RejectPrevMismatch);
        }
        match self.term.get_term_at_tx(prev_tx_id) {
            Ok(Some(record)) if record.term == prev_term => Ok(()),
            Ok(Some(record)) => {
                warn!(
                    "append_entries: prev term mismatch on node {} at tx {} (leader_term={}, our_term={}); divergence detected, watermark={}",
                    self.node_id, prev_tx_id, prev_term, record.term, leader_commit_tx_id,
                );
                self.last_divergence_watermark
                    .store(leader_commit_tx_id, Ordering::Release);
                Err(proto::RejectReason::RejectPrevMismatch)
            }
            Ok(None) => {
                // Our log knows about prev_tx_id (it's ≤ last_commit_id)
                // but the term log doesn't have a covering record.
                // That's only possible if term records older than the
                // hot ring AND not on disk — in practice a corruption.
                // Treat as divergence to be safe.
                warn!(
                    "append_entries: no term record covering tx {} on node {}; treating as divergence",
                    prev_tx_id, self.node_id
                );
                self.last_divergence_watermark
                    .store(leader_commit_tx_id, Ordering::Release);
                Err(proto::RejectReason::RejectPrevMismatch)
            }
            Err(e) => {
                warn!(
                    "append_entries: term lookup at tx {} failed on node {}: {}",
                    prev_tx_id, self.node_id, e
                );
                Err(proto::RejectReason::RejectPrevMismatch)
            }
        }
    }
}

#[tonic::async_trait]
impl Node for NodeHandler {
    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        // ADR-0016 §7: serialise every mutating Node-service handler.
        // Held for the entire body so `(currentTerm, votedFor, log)`
        // observations and writes appear atomic to peers.
        let _guard = self.node_mutex.lock().await;

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

        // ADR-0016 §8 prev-log-entry consistency check. Runs after
        // term observation so a higher-term advance is still durably
        // recorded even on rejection — Raft's term invariant must
        // outlive the per-RPC outcome.
        if let Err(reason) =
            self.check_prev_log(req.prev_tx_id, req.prev_term, req.leader_commit_tx_id)
        {
            return Ok(Response::new(proto::AppendEntriesResponse {
                term: self.current_term(),
                success: false,
                last_tx_id: self.ledger.last_commit_id(),
                reject_reason: reason as u32,
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
        // Read-only — intentionally NOT serialised through node_mutex.
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
        // Acquire the per-node mutex so that when Stage 4 lands the
        // real implementation, vote-grant durability is automatically
        // serialised against `AppendEntries` term observations.
        let _guard = self.node_mutex.lock().await;
        Err(Status::unimplemented("RequestVote deferred to ADR-016"))
    }

    async fn install_snapshot(
        &self,
        _request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        let _guard = self.node_mutex.lock().await;
        Err(Status::unimplemented("InstallSnapshot deferred to ADR-016"))
    }
}
