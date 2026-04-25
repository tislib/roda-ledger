//! `NodeHandler` — gRPC service implementation for peer-to-peer RPCs
//! (`AppendEntries`, `Ping`, scaffolded `RequestVote` / `InstallSnapshot`).
//!
//! Stage 3b split: the mutating state lives in [`NodeHandlerCore`],
//! which the supervisor + the gRPC handler share via `Arc`. The
//! `NodeHandler` itself is a thin wrapper that delegates every RPC
//! to its `core`. This lets the gRPC server bind once at startup
//! and keep serving across role transitions, while the supervisor
//! mutates the shared state (role, ledger, term, vote, divergence
//! watermark) underneath.
//!
//! - `role` is an `Arc<RoleFlag>` shared with the LedgerHandler and
//!   the supervisor. Every handler reads it on the hot path.
//! - `node_mutex` (per ADR-0016 §7) is owned by the core, not held
//!   behind `Arc`. Sharing the core via `Arc` already gives every
//!   call site the same lock instance.

use crate::cluster::election_timer::ElectionTimer;
use crate::cluster::proto::node as proto;
use crate::cluster::proto::node::node_server::Node;
use crate::cluster::role_flag::Role;
use crate::cluster::{ClusterCommitIndex, LedgerSlot, RoleFlag, Term, Vote};
use crate::wal_tail::decode_records;
use spdlog::warn;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as AsyncMutex;
use tonic::{Request, Response, Status};

/// State shared across the gRPC service handler and the supervisor.
/// Constructed once when the cluster comes up and kept alive for the
/// process lifetime; the gRPC server holds an `Arc` and observes
/// every supervisor-driven mutation atomically.
pub struct NodeHandlerCore {
    /// Indirection to the live `Arc<Ledger>` (ADR-0016 §9). Cloned
    /// out under a brief mutex on every RPC; the supervisor swaps
    /// the underlying `Arc` during a divergence reseed without
    /// having to tear down the gRPC server.
    pub ledger: Arc<LedgerSlot>,
    pub node_id: u64,
    /// Shared term state. Followers `observe()` on every incoming
    /// `AppendEntries` so their durable log tracks the leader's term
    /// transitions; all handlers read `get_current_term()` for the
    /// response `term` field.
    pub term: Arc<Term>,
    /// Durable Raft persistent vote (ADR-0016 §4). Stage 4's
    /// `RequestVote` handler grants/observes through this; Stage 3b
    /// holds it but doesn't consult it on the hot path yet.
    pub vote: Arc<Vote>,
    /// Shared role state. The handlers consult this on every RPC
    /// (e.g. `AppendEntries` rejects with `REJECT_NOT_FOLLOWER` when
    /// `role == Leader`). Supervisor flips it on transitions.
    pub role: Arc<RoleFlag>,
    /// Follower-only: watermark updated on every successful
    /// `AppendEntries`. `None` on the leader (its NodeHandler rejects
    /// incoming AppendEntries and never reaches the success path).
    pub cluster_commit_index: Option<Arc<ClusterCommitIndex>>,
    /// ADR-0016 §7. Single per-node mutex serialising every Node
    /// service handler that mutates persistent state
    /// (`AppendEntries`, `RequestVote`, `InstallSnapshot`). tonic
    /// dispatches RPCs in parallel by default; without this lock the
    /// `(currentTerm, votedFor, log)` invariant could be violated by
    /// interleaved handlers. `Ping` is read-only and intentionally
    /// **not** gated by this lock.
    pub node_mutex: AsyncMutex<()>,
    /// ADR-0016 §9. Whenever `append_entries` detects log divergence
    /// (`prev_tx_id` exists locally but the term that covered it does
    /// not match `prev_term`), the rejecting request's
    /// `leader_commit_tx_id` is stashed here for the supervisor
    /// (Stage 3c) to consume as the recovery-mode reseed watermark.
    ///
    /// `0` means "no divergence observed yet". The supervisor reads
    /// and resets this from a single thread, so a plain `AtomicU64`
    /// is sufficient.
    pub last_divergence_watermark: AtomicU64,
    /// Shared election timer (ADR-0016 §3.8 / §5). The supervisor
    /// owns the role driver that awaits `await_expiry`; the handler
    /// resets the timer on every event that, per Raft, must keep
    /// the current leader in office: a valid `AppendEntries`
    /// (including a heartbeat) and a granted `RequestVote`.
    /// `None` only in unit tests that construct a `NodeHandlerCore`
    /// outside a real supervisor.
    pub election_timer: Option<Arc<ElectionTimer>>,
}

impl NodeHandlerCore {
    pub fn new(
        ledger: Arc<LedgerSlot>,
        node_id: u64,
        term: Arc<Term>,
        vote: Arc<Vote>,
        role: Arc<RoleFlag>,
        cluster_commit_index: Option<Arc<ClusterCommitIndex>>,
    ) -> Self {
        Self {
            ledger,
            node_id,
            term,
            vote,
            role,
            cluster_commit_index,
            node_mutex: AsyncMutex::new(()),
            last_divergence_watermark: AtomicU64::new(0),
            election_timer: None,
        }
    }

    /// Builder-style override used by the supervisor to inject the
    /// shared election timer. Tests that don't care about timer
    /// behaviour can omit this and the handler reduces to a no-op
    /// for `reset_election_timer`.
    pub fn with_election_timer(mut self, timer: Arc<ElectionTimer>) -> Self {
        self.election_timer = Some(timer);
        self
    }

    /// Reset the shared election timer if one is wired in. No-op
    /// when this `Core` was built without one (test contexts).
    #[inline]
    fn reset_election_timer(&self) {
        if let Some(t) = self.election_timer.as_ref() {
            t.reset();
        }
    }

    /// Transition `Initializing` or `Candidate` to `Follower`. Called
    /// after a successful term-observation on `AppendEntries`: per
    /// Raft, any valid leader RPC defeats an in-flight election and
    /// settles the recipient as a follower. Idempotent — repeated
    /// calls when already a `Follower` are a no-op.
    fn settle_as_follower(&self) {
        match self.role.get() {
            Role::Initializing | Role::Candidate => {
                self.role.set(Role::Follower);
            }
            Role::Follower | Role::Leader => {}
        }
    }

    #[inline]
    fn current_term(&self) -> u64 {
        self.term.get_current_term()
    }

    /// Read (and clear) the most recent divergence watermark stashed
    /// by `append_entries`. Returns `None` when no divergence has
    /// been observed since the last call. Used by the cluster
    /// supervisor (Stage 3c) to drive the recovery-mode reseed.
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

    /// ADR-0016 §8 prev-log-entry consistency check. See `NodeHandler`
    /// docs for full semantics.
    fn check_prev_log(
        &self,
        prev_tx_id: u64,
        prev_term: u64,
        leader_commit_tx_id: u64,
    ) -> Result<(), proto::RejectReason> {
        if prev_tx_id == 0 {
            return Ok(());
        }
        let our_last = self.ledger.ledger().last_commit_id();
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

/// Thin wrapper that owns an `Arc<NodeHandlerCore>` and implements
/// the tonic `Node` service. The supervisor builds **one** `core`
/// per process and constructs a `NodeHandler` over it; the gRPC
/// server then lives for the process lifetime.
pub struct NodeHandler {
    core: Arc<NodeHandlerCore>,
}

impl NodeHandler {
    pub fn new(core: Arc<NodeHandlerCore>) -> Self {
        Self { core }
    }

    /// Test/observability accessor — exposes the shared core so tests
    /// can drive role transitions or read the divergence watermark.
    pub fn core(&self) -> &Arc<NodeHandlerCore> {
        &self.core
    }

    /// Convenience accessor used by tests that previously called
    /// `handler.take_divergence_watermark()` directly.
    pub fn take_divergence_watermark(&self) -> Option<u64> {
        self.core.take_divergence_watermark()
    }

    /// Convenience accessor used by tests for non-destructive reads.
    pub fn divergence_watermark(&self) -> u64 {
        self.core.divergence_watermark()
    }
}

#[tonic::async_trait]
impl Node for NodeHandler {
    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let core = &self.core;

        // ADR-0016 §7: serialise every mutating Node-service handler.
        // Held for the entire body so `(currentTerm, votedFor, log)`
        // observations and writes appear atomic to peers.
        let _guard = core.node_mutex.lock().await;

        // Snapshot the live ledger once. If a supervisor reseed
        // races (ADR-0016 §9), this RPC finishes against the old
        // ledger; the next RPC sees the new one.
        let ledger = core.ledger.ledger();

        let req = request.into_inner();

        // Leader rejects incoming AppendEntries; only Follower /
        // Initializing are eligible to apply leader-shipped bytes.
        // Read role atomically so transitions made by the supervisor
        // since this RPC was queued take effect immediately.
        if matches!(core.role.get(), Role::Leader) {
            return Ok(Response::new(proto::AppendEntriesResponse {
                term: core.current_term(),
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
            && let Err(e) = core.term.observe(req.term, req.from_tx_id)
        {
            warn!(
                "append_entries: term observe failed on node {} (incoming={}): {}",
                core.node_id, req.term, e
            );
            return Ok(Response::new(proto::AppendEntriesResponse {
                term: core.current_term(),
                success: false,
                last_tx_id: ledger.last_commit_id(),
                reject_reason: proto::RejectReason::RejectTermStale as u32,
            }));
        }

        // Term observation succeeded. Per Raft (ADR-0016 §5), any
        // valid leader RPC defeats an in-flight election: settle as
        // Follower if we were Initializing or Candidate, and bump
        // the election deadline.
        core.settle_as_follower();
        core.reset_election_timer();

        // ADR-0016 §8 prev-log-entry consistency check. Runs after
        // term observation so a higher-term advance is still durably
        // recorded even on rejection — Raft's term invariant must
        // outlive the per-RPC outcome.
        if let Err(reason) =
            core.check_prev_log(req.prev_tx_id, req.prev_term, req.leader_commit_tx_id)
        {
            return Ok(Response::new(proto::AppendEntriesResponse {
                term: core.current_term(),
                success: false,
                last_tx_id: ledger.last_commit_id(),
                reject_reason: reason as u32,
            }));
        }

        let last = ledger.last_commit_id();

        let entries = decode_records(&req.wal_bytes);
        if entries.is_empty() {
            core.advance_cluster_commit(req.leader_commit_tx_id);
            return Ok(Response::new(proto::AppendEntriesResponse {
                term: core.current_term(),
                success: true,
                last_tx_id: last,
                reject_reason: proto::RejectReason::RejectNone as u32,
            }));
        }

        match ledger.append_wal_entries(entries) {
            Ok(()) => {
                core.advance_cluster_commit(req.leader_commit_tx_id);
                Ok(Response::new(proto::AppendEntriesResponse {
                    term: core.current_term(),
                    success: true,
                    last_tx_id: last,
                    reject_reason: proto::RejectReason::RejectNone as u32,
                }))
            }
            Err(e) => {
                warn!("append_entries failed on node {}: {}", core.node_id, e);
                Ok(Response::new(proto::AppendEntriesResponse {
                    term: core.current_term(),
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
        let core = &self.core;
        let req = request.into_inner();
        Ok(Response::new(proto::PingResponse {
            node_id: core.node_id,
            term: core.current_term(),
            last_tx_id: core.ledger.ledger().last_commit_id(),
            role: core.role.get().as_proto() as i32,
            nonce: req.nonce,
        }))
    }

    async fn request_vote(
        &self,
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        // ADR-0016 §6 — Raft §5.4.1 verbatim. The per-node mutex
        // serialises this against `AppendEntries` so a candidate
        // and an arriving leader cannot race against the same
        // `(currentTerm, votedFor, log)` snapshot.
        let core = &self.core;
        let _guard = core.node_mutex.lock().await;
        let req = request.into_inner();

        let our_term_before = core.term.get_current_term();

        // 1. Stale-term reject.
        if req.term < our_term_before {
            return Ok(Response::new(proto::RequestVoteResponse {
                term: our_term_before,
                vote_granted: false,
            }));
        }

        // 2. Higher-term observation: durably advance our term and
        // clear `voted_for` so we can grant in this new term. Both
        // the term log (`Term::observe`) and the vote log
        // (`Vote::observe_term`) are fdatasync'd before we proceed.
        // We use the candidate's `last_tx_id` as `start_tx_id`
        // because it's a tx the new term will logically span (the
        // candidate is asking us to assent to its leadership).
        if req.term > our_term_before {
            if let Err(e) = core.term.observe(req.term, req.last_tx_id) {
                warn!(
                    "request_vote: term observe failed on node {} (incoming={}): {}",
                    core.node_id, req.term, e
                );
                return Ok(Response::new(proto::RequestVoteResponse {
                    term: core.term.get_current_term(),
                    vote_granted: false,
                }));
            }
            if let Err(e) = core.vote.observe_term(req.term) {
                warn!(
                    "request_vote: vote observe_term failed on node {} (incoming={}): {}",
                    core.node_id, req.term, e
                );
                return Ok(Response::new(proto::RequestVoteResponse {
                    term: core.term.get_current_term(),
                    vote_granted: false,
                }));
            }
        }

        // 3. Already-voted check. If we voted for someone else in
        // this term, reject. If we voted for *this* candidate
        // (idempotent retry), the vote-grant path below will
        // observe that and return success.
        if let Some(prev) = core.vote.get_voted_for()
            && prev != req.candidate_id
        {
            return Ok(Response::new(proto::RequestVoteResponse {
                term: core.term.get_current_term(),
                vote_granted: false,
            }));
        }

        // 4. Up-to-date-log check (Raft §5.4.1). The candidate must
        // have a log at least as up-to-date as ours: their
        // `(last_term, last_tx_id)` must be ≥ ours by lex order.
        let our_last_tx_id = core.ledger.ledger().last_commit_id();
        let our_last_term = core
            .term
            .last_record()
            .map(|r| r.term)
            .unwrap_or(0);
        let candidate_more_up_to_date = (req.last_term, req.last_tx_id)
            >= (our_last_term, our_last_tx_id);
        if !candidate_more_up_to_date {
            return Ok(Response::new(proto::RequestVoteResponse {
                term: core.term.get_current_term(),
                vote_granted: false,
            }));
        }

        // 5. Grant — durably persist the vote *before* we reply.
        // On a clean grant the same call re-applies on retry
        // (idempotent for the same `candidate_id`).
        match core.vote.vote(req.term, req.candidate_id) {
            Ok(true) => {
                // Per Raft §5.2: a server that grants a vote in a
                // term defers to that election. Reset our timer so
                // we don't immediately become a Candidate ourselves.
                core.reset_election_timer();
                Ok(Response::new(proto::RequestVoteResponse {
                    term: core.term.get_current_term(),
                    vote_granted: true,
                }))
            }
            Ok(false) => {
                // Another vote landed in this term first; we cannot
                // double-vote.
                Ok(Response::new(proto::RequestVoteResponse {
                    term: core.term.get_current_term(),
                    vote_granted: false,
                }))
            }
            Err(e) => {
                warn!(
                    "request_vote: durable vote write failed on node {}: {}",
                    core.node_id, e
                );
                Ok(Response::new(proto::RequestVoteResponse {
                    term: core.term.get_current_term(),
                    vote_granted: false,
                }))
            }
        }
    }

    async fn install_snapshot(
        &self,
        _request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        let _guard = self.core.node_mutex.lock().await;
        Err(Status::unimplemented("InstallSnapshot deferred to ADR-016"))
    }
}
