//! `RaftNode<P>` — the pure-state-machine entry point. ADR-0017
//! §"Architectural Boundary": no internal threads, no async, no
//! timers, and the only I/O is the durable writes routed through
//! [`crate::Persistence`]. Every transition is driven through
//! `step(now, event) -> Vec<Action>`.
//!
//! # Persistence
//!
//! `RaftNode` is generic over `P: Persistence`. Production wiring
//! supplies a `DiskPersistence` (term/vote files); tests supply a
//! `MemPersistence` (in-memory). The trait's writes are synchronous
//! — when a method returns `Ok(_)`, the change is durable, and the
//! library can proceed to externalise its consequences.
//!
//! `RaftNode` does **not** cache any persistence state internally.
//! `current_term()` / `voted_for()` delegate straight to the trait.
//!
//! # Internal organisation
//!
//! - `state: NodeState` — role-specific state (Initializing /
//!   Follower / Candidate / Leader).
//! - `persistence: P` — the only durable state. Owned for the
//!   lifetime of the node; recovered via [`Self::into_persistence`]
//!   on graceful shutdown / simulator crash.
//! - `quorum: Quorum` — per-peer match-index tracker. Active under
//!   the leader.
//! - `local_commit_index` / `cluster_commit_index` — read API
//!   surface; the first is advanced by the driver-side
//!   `advance_commit_index` method, the second is updated in-place
//!   when a quorum advances or `leader_commit` propagates (drivers
//!   poll the getter — there is no dedicated action).
//! - `election_timer: ElectionTimer` — armed only when a non-leader
//!   role needs one.

use std::time::{Duration, Instant};

use spdlog::warn;

use crate::consensus::{CandidateState, Election};
use crate::follower::FollowerState;
use crate::leader::{InFlightAppend, LeaderState};
use crate::log_entry::LogEntryRange;
use crate::persistence::Persistence;
use crate::quorum::Quorum;
use crate::replication::{AppendEntriesRequest, AppendResult, Replication};
use crate::role::Role;
use crate::timer::{ElectionTimer, ElectionTimerConfig};
use crate::types::{NodeId, RejectReason, Term, TxId};

/// Tunable runtime knobs. Defaults match the existing
/// `cluster::raft::*` constants so behaviour transfers when this
/// library replaces the in-place implementation.
#[derive(Clone, Copy, Debug)]
pub struct RaftConfig {
    pub election_timer: ElectionTimerConfig,
    pub heartbeat_interval: Duration,
    pub rpc_timeout: Duration,
    /// Maximum number of entries packed into one `SendAppendEntries`.
    pub max_entries_per_append: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timer: ElectionTimerConfig::default(),
            heartbeat_interval: Duration::from_millis(50),
            rpc_timeout: Duration::from_millis(500),
            max_entries_per_append: 64,
        }
    }
}

impl RaftConfig {
    /// Validate that the configuration won't pathologically misbehave
    /// at runtime. Specifically:
    ///
    /// - The randomised election timeout range must be non-empty
    ///   (`max_ms > min_ms`) — otherwise the random spread degenerates
    ///   and split-vote storms become likely.
    /// - The heartbeat interval must be safely below the minimum
    ///   election timeout. We require `heartbeat_interval * 2 <
    ///   min_ms` so a single missed heartbeat doesn't trigger an
    ///   election; this is the standard Raft sizing rule.
    ///
    /// Returns the offending field name on failure. Called from
    /// `RaftNode::new` so misconfiguration fails loudly at boot.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.election_timer.max_ms <= self.election_timer.min_ms {
            return Err("RaftConfig: election_timer.max_ms must be > min_ms");
        }
        let hb_ms = self.heartbeat_interval.as_millis() as u64;
        if hb_ms.saturating_mul(2) >= self.election_timer.min_ms {
            return Err("RaftConfig: heartbeat_interval * 2 must be < election_timer.min_ms");
        }
        Ok(())
    }
}

/// Result of [`RaftNode::validate_handshake`]. The driver inspects
/// this to decide what wire response to send. The library has already
/// applied every state-machine effect (term observation, term-log
/// boundary record, follower transition, election-timer reset, §5.3
/// truncation + watermark clamping).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandshakeDecision {
    /// Reply false. `truncate_after` is `Some(t)` only when §5.3
    /// prev_log_term mismatched **and** the local log covered
    /// `prev_log_tx_id` — the driver must drop entry-log records with
    /// `tx_id > t`. Raft's term-log mirror is already truncated
    /// synchronously inside validate.
    Reject {
        reason: RejectReason,
        truncate_after: Option<TxId>,
    },
    /// Reply true. Stream proceeds with WAL updates and heartbeats;
    /// no further per-message validation on the raft side.
    Accept,
}

/// Role-specific state held inline so the type system enforces
/// "you can only access leader state if you are the leader".
pub(crate) enum NodeState {
    Initializing,
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}

impl NodeState {
    pub(crate) fn role(&self) -> Role {
        match self {
            NodeState::Initializing => Role::Initializing,
            NodeState::Follower(_) => Role::Follower,
            NodeState::Candidate(_) => Role::Candidate,
            NodeState::Leader(_) => Role::Leader,
        }
    }
}

pub struct RaftNode<P: Persistence> {
    pub(crate) self_id: NodeId,
    pub(crate) peers: Vec<NodeId>,

    pub(crate) state: NodeState,
    pub(crate) election_timer: ElectionTimer,

    pub(crate) persistence: P,

    pub(crate) quorum: Quorum,
    pub(crate) self_slot: usize,

    /// Highest tx_id durably committed to this node's ledger.
    /// Advanced by the driver via `advance_commit_index`. Acts as
    /// the single durability watermark: bounds the AE replication
    /// window, gates §5.4.1's up-to-date check, feeds the §5.3
    /// prev_log coverage check, and feeds the leader's quorum
    /// self-slot. Survives role transitions. Read by
    /// `commit_index()`.
    pub(crate) local_commit_index: TxId,
    pub(crate) cluster_commit_index: TxId,

    /// First `tx_id` of the leader's current term — set on
    /// `become_leader_after_win` to `local_commit_index + 1`
    /// (matching the `start_tx_id` passed to `commit_term`). Cleared
    /// on step-down so a stale value doesn't leak into a follower's
    /// `leader_commit` clamping.
    ///
    /// Raft §5.4.2 / Figure 8: a leader must not advance
    /// `cluster_commit` to a tx_id below this watermark. Doing so
    /// would commit prior-term entries by counting current-term
    /// replicas, and a future leader could overwrite them. The
    /// gate is inlined at every cluster_commit advance site.
    /// Followers bypass it (the field is reset to 0 on follower
    /// transition); they trust the leader's `leader_commit`, which
    /// the leader has
    /// already gated on its end.
    pub(crate) current_term_first_tx: TxId,

    pub(crate) cfg: RaftConfig,
}

impl<P: Persistence> RaftNode<P> {
    /// Construct a `RaftNode` from an already-opened `Persistence`.
    /// The driver is responsible for hydrating the persistence off
    /// disk (term log + vote log) before calling `new`. The library
    /// reads through the trait — no separate "rehydrate" event.
    ///
    /// `local_commit_index` starts at 0; the driver feeds
    /// `node.advance_commit_index(commit)` for any entries already
    /// on disk before exposing the node to RPCs.
    ///
    /// Starting role is always `Initializing`. ADR-0017 §"No
    /// boot-time term bumping": the library does not increment term
    /// on boot.
    pub fn new(
        self_id: NodeId,
        peers: Vec<NodeId>,
        persistence: P,
        cfg: RaftConfig,
        seed: u64,
    ) -> Self {
        assert!(self_id != 0, "raft: self_id must be non-zero");
        assert!(
            peers.contains(&self_id),
            "raft: peers list must contain self_id"
        );
        cfg.validate()
            .expect("raft: RaftConfig::validate failed at construction");

        let self_slot = peers
            .iter()
            .position(|p| *p == self_id)
            .expect("checked above");
        let quorum = Quorum::new(peers.len());

        Self {
            self_id,
            peers,
            state: NodeState::Initializing,
            election_timer: ElectionTimer::new(cfg.election_timer, seed),
            persistence,
            quorum,
            self_slot,
            local_commit_index: 0,
            cluster_commit_index: 0,
            current_term_first_tx: 0,
            cfg,
        }
    }

    /// Consume the node and return its `Persistence`. Used for
    /// graceful shutdown (driver-side: keep the term/vote logs open
    /// for the next start) and the simulator's crash/restart support
    /// (drop the volatile RaftNode, retain the persistence to model
    /// disk surviving the crash).
    pub fn into_persistence(self) -> P {
        self.persistence
    }

    // ─── Public read API (ADR §"Read-only query API") ────────────────────

    pub fn role(&self) -> Role {
        self.state.role()
    }

    /// "What term am I in?" — `max(persistence.current_term(),
    /// persistence.vote_term())`. The two can diverge while a
    /// candidate has self-voted for a term it hasn't yet won (the
    /// term log advances only on `commit_term`, the vote log
    /// advances on every `vote` / `observe_vote_term`). Delegates
    /// straight to the trait; not cached.
    pub fn current_term(&self) -> Term {
        self.persistence
            .current_term()
            .max(self.persistence.vote_term())
    }

    pub fn commit_index(&self) -> TxId {
        self.local_commit_index
    }

    pub fn cluster_commit_index(&self) -> TxId {
        self.cluster_commit_index
    }

    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    pub fn voted_for(&self) -> Option<NodeId> {
        self.persistence.voted_for()
    }

    /// Best-known current leader id while this node is a follower.
    /// `None` from any other role and from a Follower that has not
    /// yet received an `AppendEntries`.
    pub fn current_leader(&self) -> Option<NodeId> {
        match &self.state {
            NodeState::Follower(f) => f.leader_id,
            _ => None,
        }
    }

    // ─── advance_local_index / advance_cluster_index ─────────────────────

    /// Driver calls after a WAL fsync. The only path that mutates
    /// `local_commit_index` on a follower. On a leader, also feeds
    /// the quorum self-slot — may transitively advance
    /// `cluster_commit_index` via §5.4.2-gated quorum logic.
    pub fn advance_local_index(&mut self, new_local: TxId) -> bool {
        if new_local > self.local_commit_index {
            self.local_commit_index = new_local;
            if matches!(self.state, NodeState::Leader(_))
                && let Some(adv) = self.quorum.advance(self.self_slot, self.local_commit_index)
                && !(self.peers.len() > 1 && adv < self.current_term_first_tx)
                && adv > self.cluster_commit_index
            {
                self.cluster_commit_index = adv;
            }
            return true;
        }
        false
    }

    /// Driver calls when the leader's claimed cluster-commit arrives
    /// (handshake response, heartbeat, or WAL-update message on the
    /// follower). Clamped to `local_commit_index` — never acknowledges
    /// commit past what's durable here.
    ///
    /// No-op on the leader: a leader advances `cluster_commit_index`
    /// only through its own quorum (inside `advance_local_index`).
    /// Calling this on a leader is a driver-side bug, so warn.
    pub fn advance_cluster_index(&mut self, new_cluster: TxId) {
        if matches!(self.state, NodeState::Leader(_)) {
            warn!(
                "advance_cluster_index called on leader (self_id={}, new_cluster={}); ignoring — leaders advance cluster_commit via quorum only",
                self.self_id, new_cluster
            );
            return;
        }
        let new_cluster = new_cluster.min(self.local_commit_index);
        if !(self.peers.len() > 1 && new_cluster < self.current_term_first_tx)
            && new_cluster > self.cluster_commit_index
        {
            self.cluster_commit_index = new_cluster;
        }
    }

    /// Driver calls on every leader message a follower processes
    /// (heartbeat or WAL update) so the election timer does not fire
    /// while the leader is actively pushing. Handshake-only validation
    /// still applies; this just keeps the follower from triggering
    /// a spurious election under steady-state replication.
    pub fn note_leader_activity(&mut self, now: Instant) {
        if !self.role().is_leader() {
            self.election_timer.reset(now);
        }
    }

    /// Returns the first `tx_id` of the leader's current term, or `0`
    /// when not leader / no entries yet in this term. Used by the
    /// leader-side cluster driver to populate the handshake message.
    pub fn current_term_first_tx(&self) -> TxId {
        self.current_term_first_tx
    }

    /// Term of the entry at `tx_id` according to the durable term
    /// log, or `None` if no record covers that index. Used by the
    /// leader-side cluster driver to populate the handshake's §5.3
    /// anchor.
    pub fn term_at_tx(&self, tx_id: TxId) -> Option<Term> {
        self.persistence.term_at_tx(tx_id).map(|r| r.term)
    }

    /// Snapshot of the durable term log (ascending). Used by the
    /// leader-side cluster driver to ship the full term log on a
    /// handshake — the follower applies it so its term log mirrors
    /// the leader's view for every entry below `local_commit_index`.
    /// Stop-gap until `InstallSnapshot` covers cross-term catch-up.
    pub fn term_log_snapshot(&self) -> Vec<crate::TermRecord> {
        self.persistence.iter_term_records()
    }

    // ─── replication() ───────────────────────────────────────────────────

    /// Entry point for the cluster-driven leader-side replication
    /// API. Returns a [`Replication`] handle that exposes per-peer
    /// progress; see the module docs for the cluster's loop pattern.
    ///
    /// All counters live on this `RaftNode`; the returned handle is
    /// a borrow view that performs no allocation of its own.
    pub fn replication(&mut self) -> Replication<'_, P> {
        Replication::new(self)
    }

    // Internal helpers backing `Replication` / `PeerReplication`.
    // Defined here (rather than in `replication_driver`) so they can
    // touch private state directly without relaxing visibility.

    pub(crate) fn replication_peer_next_index(&self, peer_id: NodeId) -> TxId {
        match &self.state {
            NodeState::Leader(l) => l.peers.get(&peer_id).map(|p| p.next_index).unwrap_or(0),
            _ => 0,
        }
    }

    pub(crate) fn replication_peer_match_index(&self, peer_id: NodeId) -> TxId {
        match &self.state {
            NodeState::Leader(l) => l.peers.get(&peer_id).map(|p| p.match_index).unwrap_or(0),
            _ => 0,
        }
    }

    /// Build the next AE for `peer_id` and arm the in-flight slot.
    /// Returns `None` if not leader / peer unknown / heartbeat not
    /// due / RPC already in flight.
    pub(crate) fn replication_get_append_range(
        &mut self,
        peer_id: NodeId,
        now: Instant,
    ) -> Option<AppendEntriesRequest> {
        if !self.role().is_leader() {
            return None;
        }

        if let NodeState::Leader(l) = &mut self.state
            && let Some(p) = l.peers.get_mut(&peer_id)
            && let Some(infl) = p.in_flight
            && now >= infl.expires_at
        {
            p.in_flight = None;
        }

        let progress = match &self.state {
            NodeState::Leader(l) => l.peers.get(&peer_id).cloned()?,
            _ => return None,
        };
        if progress.in_flight.is_some() {
            return None;
        }
        if now < progress.next_heartbeat {
            return None;
        }

        let leader_commit = self.cluster_commit_index;
        let max_entries = self.cfg.max_entries_per_append as u64;
        let last_written = self.local_commit_index;
        let next_index = progress.next_index;

        let entries = if next_index > last_written {
            LogEntryRange::empty()
        } else {
            let count = (last_written - next_index + 1).min(max_entries);
            LogEntryRange::new(next_index, count)
        };

        let last_tx_in_batch = entries.last_tx_id().unwrap_or(next_index.saturating_sub(1));

        let interval = self.cfg.heartbeat_interval;
        let rpc_to = self.cfg.rpc_timeout;
        if let NodeState::Leader(l) = &mut self.state
            && let Some(p) = l.peers.get_mut(&peer_id)
        {
            p.in_flight = Some(InFlightAppend {
                last_tx_id_in_batch: last_tx_in_batch,
                expires_at: now + rpc_to,
            });
            p.next_heartbeat = now + interval;
        }

        Some(AppendEntriesRequest {
            to: peer_id,
            entries,
            leader_commit,
        })
    }

    /// Feed the cluster-driver's RPC outcome back into the state
    /// machine. `Success` / `Reject(LogMismatch)` advance / regress
    /// the peer's `next_index` and feed the quorum;
    /// `Reject(TermBehind)` triggers leader step-down;
    /// `Timeout` clears `in_flight` while leaving indexes alone.
    pub(crate) fn replication_append_result(
        &mut self,
        peer_id: NodeId,
        now: Instant,
        result: AppendResult,
    ) {
        match result {
            AppendResult::Success {
                term,
                last_commit_id,
            } => {
                self.on_append_entries_reply(now, peer_id, term, true, last_commit_id, None);
            }
            AppendResult::Reject {
                term,
                reason,
                last_commit_id,
            } => {
                self.on_append_entries_reply(
                    now,
                    peer_id,
                    term,
                    false,
                    last_commit_id,
                    Some(reason),
                );
            }
            AppendResult::Timeout => {
                let term = self.current_term();
                self.on_append_entries_reply(
                    now,
                    peer_id,
                    term,
                    false,
                    0,
                    Some(RejectReason::RpcTimeout),
                );
            }
        }
    }

    // ─── election() ──────────────────────────────────────────────────────

    /// Entry point for the consensus / election sub-primitive. Returns
    /// an [`Election`] borrow view; see the [`crate::consensus`]
    /// module for the cluster's loop pattern.
    ///
    /// All consensus state lives on this `RaftNode`; the returned
    /// handle is a borrow view that performs no allocation of its own.
    pub fn election(&mut self) -> Election<'_, P> {
        Election { node: self }
    }

    // ─── Handshake (follower path) ───────────────────────────────────────

    /// Direct-method follower-side handler for an inbound replication
    /// handshake. Runs the §5.1 / §5.3 protocol checks and returns a
    /// [`HandshakeDecision`] that tells the driver what wire response
    /// to send.
    ///
    /// Per the handshake-only validation model, this is the **only**
    /// raft-side validation point in a replication stream: subsequent
    /// WAL updates and heartbeats are trusted. Any leader/term/role
    /// change must close the stream and re-handshake.
    ///
    /// Internal state mutations performed before returning:
    ///
    /// - On `term > current_term`: `observe_higher_term`,
    ///   `observe_term(term, term_first_tx_id)` (records the new term
    ///   boundary in the durable term log so the next handshake's
    ///   §5.3 anchor works), follower transition, election-timer reset.
    /// - On §5.3 prev_log_term mismatch with a covering log: synchronous
    ///   `truncate_term_after` and a clamp of `local_commit_index` /
    ///   `cluster_commit_index` to the truncation point. The driver
    ///   must mirror the truncation in its entry log.
    ///
    /// The driver is responsible for advancing `cluster_commit_index`
    /// via [`advance_cluster_index`](Self::advance_cluster_index) after
    /// `Accept` — this method no longer mutates it.
    pub fn validate_handshake(
        &mut self,
        now: Instant,
        from: NodeId,
        term: Term,
        leader_term_records: &[crate::TermRecord],
        prev_log_tx_id: TxId,
        prev_log_term: Term,
    ) -> HandshakeDecision {
        let current_term = self.current_term();

        if term < current_term {
            return HandshakeDecision::Reject {
                reason: RejectReason::TermBehind,
                truncate_after: None,
            };
        }

        if term > current_term {
            self.observe_higher_term(term);
        }
        // Ingest the leader's full term log so the §5.3 anchor below
        // resolves for every entry the follower may have below
        // `local_commit_index`. `observe_term` is monotone, so we only
        // apply records strictly above the term log's current term —
        // earlier records were either already observed or are missing
        // because of the cross-term-catch-up gap that InstallSnapshot
        // will eventually close.
        for record in leader_term_records {
            if record.term > self.persistence.current_term() {
                self.persistence
                    .observe_term(record.term, record.start_tx_id);
            }
        }
        self.transition_to_follower(now, Some(from));
        self.election_timer.reset(now);

        // §5.3 anchor: the follower's term-log record at `prev_log_tx_id`
        // must match the leader's claim.
        if prev_log_tx_id != 0 {
            let our_term_at_prev = self
                .persistence
                .term_at_tx(prev_log_tx_id)
                .map(|r| r.term)
                .unwrap_or(0);
            let log_covers_prev = prev_log_tx_id <= self.local_commit_index;
            if !log_covers_prev || our_term_at_prev != prev_log_term {
                let truncate_after = if log_covers_prev {
                    let after = prev_log_tx_id.saturating_sub(1);
                    debug_assert!(
                        after >= self.cluster_commit_index,
                        "truncation below cluster_commit_index: after={} cluster={}",
                        after,
                        self.cluster_commit_index
                    );
                    self.persistence.truncate_term_after(after);
                    if self.local_commit_index > after {
                        self.local_commit_index = after;
                    }
                    if self.cluster_commit_index > after {
                        self.cluster_commit_index = after;
                    }
                    Some(after)
                } else {
                    None
                };
                return HandshakeDecision::Reject {
                    reason: RejectReason::LogMismatch,
                    truncate_after,
                };
            }
        }

        HandshakeDecision::Accept
    }

    // ─── AppendEntries reply handling (leader path) ──────────────────────

    fn on_append_entries_reply(
        &mut self,
        now: Instant,
        from: NodeId,
        term: Term,
        success: bool,
        last_commit_id: TxId,
        reject_reason: Option<RejectReason>,
    ) {
        let current_term = self.current_term();
        if term > current_term {
            self.observe_higher_term(term);
            self.transition_to_follower(now, None);
            return;
        }

        let outcome: Result<TxId, Option<TxId>> = match &mut self.state {
            NodeState::Leader(l) => {
                let progress = match l.peers.get_mut(&from) {
                    Some(p) => p,
                    None => return,
                };
                progress.in_flight = None;
                if success {
                    if last_commit_id > progress.match_index {
                        progress.match_index = last_commit_id;
                    }
                    progress.next_index = last_commit_id + 1;
                    Ok(last_commit_id)
                } else {
                    match reject_reason {
                        Some(RejectReason::TermBehind) | Some(RejectReason::RpcTimeout) => {
                            Err(None)
                        }
                        Some(RejectReason::LogMismatch) | None => {
                            progress.next_index = progress.next_index.saturating_sub(1).max(1);
                            Err(Some(last_commit_id))
                        }
                    }
                }
            }
            _ => return,
        };

        let peer_slot_index = match self.peers.iter().position(|p| *p == from) {
            Some(idx) => idx,
            None => return,
        };
        match outcome {
            Ok(advanced_to) => {
                if let Some(new_cluster) = self.quorum.advance(peer_slot_index, advanced_to)
                    && !(self.peers.len() > 1 && new_cluster < self.current_term_first_tx)
                    && new_cluster > self.cluster_commit_index
                {
                    self.cluster_commit_index = new_cluster;
                }
            }
            Err(Some(peer_last_commit)) => {
                self.quorum.regress(peer_slot_index, peer_last_commit);
            }
            Err(None) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request_vote::RequestVote;

    /// Tiny in-test in-memory persistence, mirroring
    /// `tests/common/mem_persistence.rs`. The trait abstraction lets
    /// us avoid pulling in tempdirs for unit tests.
    #[derive(Default)]
    struct TestPersistence {
        term_log: Vec<crate::TermRecord>,
        vote_term: Term,
        voted_for: NodeId,
    }

    impl Persistence for TestPersistence {
        fn current_term(&self) -> Term {
            self.term_log.last().map(|r| r.term).unwrap_or(0)
        }
        fn last_term_record(&self) -> Option<crate::TermRecord> {
            self.term_log.last().copied()
        }
        fn term_at_tx(&self, tx_id: TxId) -> Option<crate::TermRecord> {
            let mut best: Option<crate::TermRecord> = None;
            for rec in &self.term_log {
                if rec.start_tx_id > tx_id {
                    continue;
                }
                best = match best {
                    Some(b) if b.term >= rec.term => Some(b),
                    _ => Some(*rec),
                };
            }
            best
        }
        fn commit_term(&mut self, expected: Term, start_tx_id: TxId) -> bool {
            let cur = self.current_term();
            if cur >= expected {
                return false;
            }
            self.term_log.push(crate::TermRecord {
                term: expected,
                start_tx_id,
            });
            true
        }
        fn observe_term(&mut self, term: Term, start_tx_id: TxId) {
            let cur = self.current_term();
            if term == cur {
                return;
            }
            assert!(
                term > cur,
                "observe_term regression: incoming={} current={}",
                term,
                cur
            );
            self.term_log.push(crate::TermRecord { term, start_tx_id });
        }
        fn truncate_term_after(&mut self, tx_id: TxId) {
            self.term_log.retain(|r| r.start_tx_id <= tx_id);
        }
        fn iter_term_records(&self) -> Vec<crate::TermRecord> {
            self.term_log.clone()
        }
        fn vote_term(&self) -> Term {
            self.vote_term
        }
        fn voted_for(&self) -> Option<NodeId> {
            match self.voted_for {
                0 => None,
                n => Some(n),
            }
        }
        fn vote(&mut self, term: Term, candidate_id: NodeId) -> bool {
            assert!(candidate_id != 0, "candidate_id must be non-zero");
            if term < self.vote_term {
                return false;
            }
            if term == self.vote_term {
                if self.voted_for == candidate_id {
                    return true;
                }
                if self.voted_for != 0 {
                    return false;
                }
            }
            self.vote_term = term;
            self.voted_for = candidate_id;
            true
        }
        fn observe_vote_term(&mut self, term: Term) {
            if term == self.vote_term {
                return;
            }
            assert!(
                term > self.vote_term,
                "observe_vote_term regression: incoming={} current={}",
                term,
                self.vote_term
            );
            self.vote_term = term;
            self.voted_for = 0;
        }
    }

    fn fresh(self_id: NodeId, peers: Vec<NodeId>) -> RaftNode<TestPersistence> {
        RaftNode::new(
            self_id,
            peers,
            TestPersistence::default(),
            RaftConfig::default(),
            42,
        )
    }

    #[test]
    fn boot_starts_in_initializing_at_term_zero() {
        let node = fresh(1, vec![1, 2, 3]);
        assert_eq!(node.role(), Role::Initializing);
        assert_eq!(node.current_term(), 0);
        assert_eq!(node.commit_index(), 0);
        assert_eq!(node.cluster_commit_index(), 0);
        assert_eq!(node.voted_for(), None);
    }

    #[test]
    fn election_timeout_starts_election() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let t0 = Instant::now();
        // First tick lazy-arms; second tick after the timeout starts
        // the election round.
        let _ = node.election().tick(t0);
        let _ = node.election().tick(t0 + Duration::from_secs(60));
        assert!(node.election().start(t0 + Duration::from_secs(60)));
        assert_eq!(node.role(), Role::Candidate);
        // `current_term()` = max(term-log, vote-log). The term log
        // stays at 0 until election win, but the candidate's
        // self-vote bumps the vote log to 1, so the public read is 1.
        assert_eq!(node.current_term(), 1);
        assert_eq!(node.voted_for(), Some(1));
        // One outbound `RequestVote` per other peer.
        assert_eq!(node.election().get_requests().len(), 2);
    }

    #[test]
    fn single_node_election_immediately_becomes_leader() {
        let mut node = fresh(1, vec![1]);
        let t0 = Instant::now();
        let _ = node.election().tick(t0);
        let _ = node.election().tick(t0 + Duration::from_secs(60));
        // `start` short-circuits to Leader inside the same call when
        // the cluster has no other peers (self-vote = majority).
        assert!(node.election().start(t0 + Duration::from_secs(60)));
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.current_term(), 1);
    }

    #[test]
    fn request_vote_with_higher_term_grants_and_steps_down() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let reply = node.election().handle_request_vote(
            Instant::now(),
            RequestVote {
                from: 2,
                term: 5,
                last_tx_id: 0,
                last_term: 0,
            },
        );
        assert_eq!(node.current_term(), 5);
        assert_eq!(node.voted_for(), Some(2));
        assert!(reply.granted);
    }

    #[test]
    fn advance_on_single_node_leader_lifts_cluster_commit() {
        let mut node = fresh(1, vec![1]);
        let t0 = Instant::now();
        let _ = node.election().tick(t0);
        let _ = node.election().tick(t0 + Duration::from_secs(60));
        let _ = node.election().start(t0 + Duration::from_secs(60));
        assert!(node.role().is_leader());
        node.advance_local_index(7);
        // `advance` is silent; observe via getters per ADR-0017
        // §"Driver call pattern".
        assert_eq!(node.commit_index(), 7);
        assert_eq!(node.cluster_commit_index(), 7);
    }

    #[test]
    fn into_persistence_recovers_durable_state() {
        let mut node = fresh(1, vec![1]);
        let t0 = Instant::now();
        let _ = node.election().tick(t0);
        let _ = node.election().tick(t0 + Duration::from_secs(60));
        let _ = node.election().start(t0 + Duration::from_secs(60));
        assert!(node.role().is_leader());
        assert_eq!(node.current_term(), 1);

        let p = node.into_persistence();
        assert_eq!(p.current_term(), 1);
        assert_eq!(p.voted_for(), Some(1));

        // Restart with the same persistence: term/voted_for survive,
        // role and commit indexes start fresh.
        let restarted = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);
        assert_eq!(restarted.current_term(), 1);
        assert_eq!(restarted.voted_for(), Some(1));
        assert_eq!(restarted.role(), Role::Initializing);
        assert_eq!(restarted.commit_index(), 0);
    }

    // ── RaftConfig::validate ─────────────────────────────────────────────

    #[test]
    fn raftconfig_validate_accepts_default() {
        assert!(RaftConfig::default().validate().is_ok());
    }

    #[test]
    fn raftconfig_validate_rejects_heartbeat_too_close_to_election_timeout() {
        let cfg = RaftConfig {
            heartbeat_interval: Duration::from_millis(100),
            election_timer: ElectionTimerConfig {
                min_ms: 150,
                max_ms: 300,
            },
            rpc_timeout: Duration::from_millis(500),
            max_entries_per_append: 64,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn raftconfig_validate_rejects_inverted_election_timer_range() {
        let cfg = RaftConfig {
            election_timer: ElectionTimerConfig {
                min_ms: 300,
                max_ms: 150,
            },
            ..RaftConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn raftconfig_validate_rejects_equal_election_timer_range() {
        let cfg = RaftConfig {
            election_timer: ElectionTimerConfig {
                min_ms: 200,
                max_ms: 200,
            },
            ..RaftConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    // ── validate_handshake ───────────────────────────────────────────────

    #[test]
    fn validate_handshake_returns_reject_on_stale_term() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let _ = node.election().handle_request_vote(
            Instant::now(),
            RequestVote {
                from: 2,
                term: 5,
                last_tx_id: 0,
                last_term: 0,
            },
        );
        assert_eq!(node.current_term(), 5);

        let decision = node.validate_handshake(Instant::now(), 2, 3, &[], 0, 0);
        assert_eq!(
            decision,
            HandshakeDecision::Reject {
                reason: RejectReason::TermBehind,
                truncate_after: None,
            }
        );
    }

    #[test]
    fn validate_handshake_accepts_with_no_prior_log() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let records = [crate::TermRecord {
            term: 1,
            start_tx_id: 1,
        }];
        let decision = node.validate_handshake(Instant::now(), 2, 1, &records, 0, 0);
        assert_eq!(decision, HandshakeDecision::Accept);
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), 1);
    }

    #[test]
    fn validate_handshake_records_term_boundary() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let records = [crate::TermRecord {
            term: 5,
            start_tx_id: 42,
        }];
        let _ = node.validate_handshake(Instant::now(), 2, 5, &records, 0, 0);
        assert_eq!(node.term_at_tx(42), Some(5));
        assert_eq!(node.term_at_tx(100), Some(5));
    }

    #[test]
    fn validate_handshake_returns_reject_on_log_mismatch_with_truncate_after() {
        let mut node = fresh(1, vec![1, 2, 3]);
        node.persistence.commit_term(1, 0);
        node.local_commit_index = 5;

        let decision = node.validate_handshake(Instant::now(), 2, 2, &[], 5, 2);
        assert_eq!(
            decision,
            HandshakeDecision::Reject {
                reason: RejectReason::LogMismatch,
                truncate_after: Some(4),
            }
        );
    }

    #[test]
    fn validate_handshake_clamps_watermarks_on_log_mismatch() {
        let mut node = fresh(1, vec![1, 2, 3]);
        node.persistence.commit_term(1, 0);
        node.local_commit_index = 5;
        node.cluster_commit_index = 2;

        let _ = node.validate_handshake(Instant::now(), 2, 2, &[], 5, 2);
        assert_eq!(node.commit_index(), 4);
        assert_eq!(node.cluster_commit_index(), 2);
    }

    // ── advance_local_index / advance_cluster_index ─────────────────────

    #[test]
    fn advance_cluster_index_clamps_to_local_commit() {
        let mut node = fresh(1, vec![1, 2, 3]);
        node.local_commit_index = 4;
        node.advance_cluster_index(10);
        // Clamped to local (4), not the leader-supplied 10.
        assert_eq!(node.cluster_commit_index(), 4);
    }

    #[test]
    fn advance_cluster_index_is_monotonic() {
        let mut node = fresh(1, vec![1, 2, 3]);
        node.local_commit_index = 10;
        node.advance_cluster_index(5);
        assert_eq!(node.cluster_commit_index(), 5);
        node.advance_cluster_index(3);
        // Going backwards is a no-op.
        assert_eq!(node.cluster_commit_index(), 5);
    }

    #[test]
    fn advance_local_index_does_not_touch_local_commit_when_leader_commit_received() {
        let mut node = fresh(1, vec![1, 2, 3]);
        // advance_cluster_index alone never moves local_commit_index.
        node.advance_cluster_index(50);
        assert_eq!(node.commit_index(), 0);
    }

    // ── Replication / PeerReplication (cluster-pull leader path) ──────────

    /// Promote a 3-node `RaftNode` to leader by running an election
    /// manually: a tick lazy-arms, a second tick fires the timeout,
    /// `start` transitions to Candidate, and a `Granted` outcome from
    /// one peer is enough to make a 3-node majority (self-vote + 1).
    ///
    /// Returns `(node, leader_now)` so callers can issue subsequent
    /// calls with timestamps `>= leader_now` — `LeaderState::new`
    /// initialises every peer's `next_heartbeat` to that instant, so
    /// the in-place gate in `replication_get_append_range` rejects
    /// earlier `now` values.
    fn fresh_leader_3node(
        self_id: NodeId,
        peers: Vec<NodeId>,
    ) -> (RaftNode<TestPersistence>, Instant) {
        use crate::consensus::VoteOutcome;
        let mut node = fresh(self_id, peers.clone());
        let t0 = Instant::now();
        let leader_now = t0 + Duration::from_secs(60);
        let _ = node.election().tick(t0);
        let _ = node.election().tick(leader_now);
        let _ = node.election().start(leader_now);
        if node.role().is_leader() {
            return (node, leader_now);
        }
        let term = node.current_term();
        for peer in peers.iter().filter(|&&p| p != self_id) {
            node.election()
                .handle_votes(leader_now, vec![(*peer, VoteOutcome::Granted { term })]);
            if node.role().is_leader() {
                return (node, leader_now);
            }
        }
        panic!(
            "fresh_leader_3node: did not become leader (role={:?})",
            node.role()
        );
    }

    #[test]
    fn replication_get_append_range_returns_none_when_not_leader() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let req = node
            .replication()
            .peer(2)
            .and_then(|mut p| p.get_append_range(Instant::now()));
        assert!(req.is_none());
    }

    #[test]
    fn replication_peer_returns_none_for_unknown_or_self_id() {
        let mut node = fresh(1, vec![1, 2, 3]);
        // self_id is excluded
        assert!(node.replication().peer(1).is_none());
        // unknown peer is excluded
        assert!(node.replication().peer(99).is_none());
    }

    #[test]
    fn replication_peers_excludes_self_id() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let peers = node.replication().peers();
        assert_eq!(peers, vec![2, 3]);
    }

    #[test]
    fn replication_get_append_range_returns_heartbeat_when_caught_up() {
        let (mut node, leader_now) = fresh_leader_3node(1, vec![1, 2, 3]);
        let req = node
            .replication()
            .peer(2)
            .unwrap()
            .get_append_range(leader_now)
            .expect("leader should produce an AE");
        assert!(req.entries.is_empty());
        assert_eq!(req.to, 2);
    }

    #[test]
    fn replication_get_append_range_ships_new_entries_after_advance() {
        let (mut node, leader_now) = fresh_leader_3node(1, vec![1, 2, 3]);
        node.advance_local_index(3);

        let req = node
            .replication()
            .peer(2)
            .unwrap()
            .get_append_range(leader_now)
            .expect("leader with durable entries should produce an AE");
        assert_eq!(req.entries.start_tx_id, 1);
        assert_eq!(req.entries.count, 3);
    }

    #[test]
    fn replication_append_result_success_advances_match_and_next_index() {
        let (mut node, leader_now) = fresh_leader_3node(1, vec![1, 2, 3]);
        node.advance_local_index(3);
        let _ = node
            .replication()
            .peer(2)
            .unwrap()
            .get_append_range(leader_now);

        let term = node.current_term();
        node.replication().peer(2).unwrap().append_result(
            leader_now,
            AppendResult::Success {
                term,
                last_commit_id: 3,
            },
        );

        let pr = node.replication().peer(2).unwrap();
        assert_eq!(pr.match_index(), 3);
        assert_eq!(pr.next_index(), 4);
    }

    #[test]
    fn replication_append_result_log_mismatch_walks_back_next_index() {
        let (mut node, leader_now) = fresh_leader_3node(1, vec![1, 2, 3]);
        node.advance_local_index(5);
        if let NodeState::Leader(l) = &mut node.state {
            l.peers.get_mut(&2).unwrap().next_index = 4;
        }

        let _ = node
            .replication()
            .peer(2)
            .unwrap()
            .get_append_range(leader_now);

        let term = node.current_term();
        node.replication().peer(2).unwrap().append_result(
            leader_now,
            AppendResult::Reject {
                term,
                reason: RejectReason::LogMismatch,
                last_commit_id: 0,
            },
        );
        assert_eq!(node.replication().peer(2).unwrap().next_index(), 3);
    }

    #[test]
    fn replication_append_result_term_behind_steps_down() {
        let (mut node, leader_now) = fresh_leader_3node(1, vec![1, 2, 3]);
        let leader_term = node.current_term();
        let _ = node
            .replication()
            .peer(2)
            .unwrap()
            .get_append_range(leader_now);

        node.replication().peer(2).unwrap().append_result(
            leader_now,
            AppendResult::Reject {
                term: leader_term + 5,
                reason: RejectReason::TermBehind,
                last_commit_id: 0,
            },
        );
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), leader_term + 5);
    }

    #[test]
    fn replication_append_result_timeout_clears_in_flight_only() {
        let (mut node, leader_now) = fresh_leader_3node(1, vec![1, 2, 3]);
        node.advance_local_index(3);
        let _ = node
            .replication()
            .peer(2)
            .unwrap()
            .get_append_range(leader_now);

        let nx_before = node.replication().peer(2).unwrap().next_index();
        let mi_before = node.replication().peer(2).unwrap().match_index();

        node.replication()
            .peer(2)
            .unwrap()
            .append_result(leader_now, AppendResult::Timeout);

        assert_eq!(node.replication().peer(2).unwrap().next_index(), nx_before);
        assert_eq!(node.replication().peer(2).unwrap().match_index(), mi_before);

        let later = leader_now + Duration::from_millis(100);
        let req2 = node.replication().peer(2).unwrap().get_append_range(later);
        assert!(
            req2.is_some(),
            "timeout must clear in_flight so a fresh request can fire after next_heartbeat"
        );
    }
}
