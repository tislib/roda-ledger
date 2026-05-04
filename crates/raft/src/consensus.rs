//! Consensus / election sub-primitive — replaces the historic
//! `step / Event / Action` model. ADR-0017.
//!
//! ## API shape
//!
//! [`RaftNode::election`] returns an [`Election`] borrow view. The
//! cluster driver calls, in a tight loop:
//!
//! - [`Election::tick`] — drive election-side time forward; returns
//!   a [`Wakeup`] whose `deadline` is the soonest pending timer (the
//!   cluster sleeps until then or until an inbound command arrives).
//! - [`Election::start`] — if the election timer has expired (and we
//!   are not Leader), bump term, durably self-vote, transition to
//!   Candidate, return `true`.
//! - [`Election::get_requests`] — the `RequestVote`s to send. The
//!   cluster sends them concurrently and collects [`VoteOutcome`]s.
//! - [`Election::handle_votes`] — drain a batch of vote outcomes;
//!   higher-term observation steps down, majority wins the election.
//! - [`Election::handle_request_vote`] — voter-side handler for an
//!   inbound `RequestVote`.
//!
//! Role transitions are observed by polling [`RaftNode::role`] after
//! each call — there is no action stream.
//!
//! All consensus state lives on [`RaftNode`]: the election timer, the
//! [`CandidateState`] inside `NodeState::Candidate(_)`, and the
//! durable term / vote logs behind [`crate::Persistence`]. `Election`
//! holds a `&mut RaftNode<P>` and dispatches into `pub(crate)` methods
//! defined in this module's `impl` block.

use std::collections::HashSet;
use std::time::Instant;

use spdlog::{debug, info};

use crate::follower::FollowerState;
use crate::leader::LeaderState;
use crate::node::{NodeState, RaftNode};
use crate::persistence::Persistence;
use crate::request_vote::{RequestVoteReply, RequestVoteRequest};
use crate::types::{NodeId, Term};

// ─── Public types ────────────────────────────────────────────────────

/// Next-deadline hint returned from [`Election::tick`]. The cluster
/// driver sleeps until `deadline` (or an inbound command preempts the
/// sleep, whichever fires first), then re-enters the tight loop.
///
/// `deadline` is always populated. When the node has no real pending
/// wakeup (single-node leader, idle leader without peers), `deadline`
/// is `now + heartbeat_interval` so the loop polls again at a sensible
/// cadence rather than parking forever.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Wakeup {
    pub deadline: Instant,
}

/// Outcome of a single outbound `RequestVote` RPC, as observed by the
/// cluster driver. Passed back into [`Election::handle_votes`].
#[derive(Clone, Copy, Debug)]
pub enum VoteOutcome {
    /// Peer replied `granted=true`. The peer's reported `term` is
    /// included so a stale reply (from a superseded round) can be
    /// filtered against the current candidacy's `election_term`.
    Granted { term: Term },
    /// Peer replied `granted=false`. A `term > election_term` triggers
    /// step-down to Follower; otherwise the response is ignored.
    Denied { term: Term },
    /// RPC failed (timeout / connect error / decode error). The vote
    /// is not counted. The peer is not deemed to have observed any
    /// term — the election proceeds with the remaining responses.
    Failed,
}

/// Candidate role state — seeded with the candidate's self-vote and
/// updated per [`Election::handle_votes`] until either majority or
/// step-down. Stamped onto every outgoing `RequestVote` so a stale
/// reply from a previous round can be filtered against the current
/// `election_term`.
#[derive(Clone, Debug)]
pub struct CandidateState {
    pub election_term: Term,
    votes_received: HashSet<NodeId>,
}

impl CandidateState {
    pub fn new(election_term: Term, self_id: NodeId) -> Self {
        let mut votes_received = HashSet::new();
        votes_received.insert(self_id);
        Self {
            election_term,
            votes_received,
        }
    }

    /// Returns `true` if the grant pushed us over the majority
    /// threshold for the first time. Idempotent on repeated grants
    /// from the same peer.
    pub fn record_grant(&mut self, peer: NodeId, majority: usize) -> bool {
        let pre = self.votes_received.len();
        self.votes_received.insert(peer);
        let post = self.votes_received.len();
        post >= majority && pre < majority
    }

    pub fn votes_received(&self) -> usize {
        self.votes_received.len()
    }
}

/// Borrow view exposing the election API. See module docs.
pub struct Election<'a, P: Persistence> {
    pub(crate) node: &'a mut RaftNode<P>,
}

impl<'a, P: Persistence> Election<'a, P> {
    /// Drive election-side time forward.
    ///
    /// Lazy-arms the election timer on first call (the library
    /// constructor never reads a clock). Returns a [`Wakeup`] whose
    /// `deadline` is the soonest pending wakeup (election timer or
    /// leader heartbeat); for a single-node leader with no real
    /// pending wakeup, `deadline` falls back to
    /// `now + heartbeat_interval` so the cluster's tight loop polls
    /// again rather than parking forever.
    ///
    /// Does **not** transition state — call [`Self::start`] to react
    /// to an expired timer.
    pub fn tick(&mut self, now: Instant) -> Wakeup {
        self.node.election_tick(now)
    }

    /// If (a) we are not Leader and (b) the election timer has
    /// expired, bump term, durably self-vote, transition to
    /// Candidate, reset the election timer, and return `true`.
    /// Single-node clusters short-circuit to Leader inside the same
    /// call (still returns `true`). Returns `false` otherwise.
    pub fn start(&mut self, now: Instant) -> bool {
        self.node.election_start(now)
    }

    /// All outbound `RequestVote`s for the current candidacy. Empty
    /// when not Candidate. Pure read — caller may invoke many times.
    /// The cluster sends these concurrently and feeds the outcomes
    /// back via [`Self::handle_votes`].
    pub fn get_requests(&self) -> Vec<(NodeId, RequestVoteRequest)> {
        self.node.election_get_requests()
    }

    /// Drain a batch of vote outcomes. Per-response handling:
    ///
    /// - `Granted { term }` at `election_term`: counts toward
    ///   majority; on threshold-cross, becomes Leader.
    /// - `Granted { term > election_term }` and `Denied { term >
    ///   election_term }`: higher term observed → step down.
    /// - Replies at a term `< election_term`: stale, ignored.
    /// - `Denied { term == election_term }` and `Failed`: ignored.
    ///
    /// If we are not Candidate when called, every response is dropped
    /// silently. Cluster polls `node.role()` afterward to update its
    /// `ReplicationGate` / mirror.
    pub fn handle_votes(
        &mut self,
        now: Instant,
        responses: Vec<(NodeId, VoteOutcome)>,
    ) {
        self.node.election_handle_votes(now, responses);
    }

    /// Voter-side handler for an inbound `RequestVote`. §5.4.1
    /// up-to-date check, term observation, durable vote, election-
    /// timer reset on grant.
    pub fn handle_request_vote(
        &mut self,
        now: Instant,
        req: RequestVoteRequest,
    ) -> RequestVoteReply {
        self.node.election_handle_request_vote(now, req)
    }
}

// ─── Methods on RaftNode ────────────────────────────────────────────
//
// The `Election` borrow view delegates here. Defining these as
// `RaftNode` methods (rather than free functions taking
// `&mut RaftNode<P>`) lets them touch private fields directly without
// raising any field's visibility above what `node.rs` already grants
// to its sister modules in this crate.

impl<P: Persistence> RaftNode<P> {
    pub(crate) fn election_tick(&mut self, now: Instant) -> Wakeup {
        // First-call lazy-arm. The library never reads a clock at
        // construction (ADR-0017); the constructor leaves the
        // election timer disarmed and we prime it here.
        if !self.role().is_leader() && self.election_timer.deadline().is_none() {
            self.election_timer.arm(now);
        }
        Wakeup {
            deadline: self.next_pending_wakeup(now),
        }
    }

    pub(crate) fn election_start(&mut self, now: Instant) -> bool {
        if self.role().is_leader() {
            return false;
        }
        if !self.election_timer.is_expired(now) {
            return false;
        }
        self.start_election_round(now)
    }

    pub(crate) fn election_get_requests(&self) -> Vec<(NodeId, RequestVoteRequest)> {
        let term = match &self.state {
            NodeState::Candidate(c) => c.election_term,
            _ => return Vec::new(),
        };
        let last_tx_id = self.local_write_index;
        let last_term = self.local_last_term();
        self.peers
            .iter()
            .copied()
            .filter(|p| *p != self.self_id)
            .map(|peer| {
                (
                    peer,
                    RequestVoteRequest {
                        from: self.self_id,
                        term,
                        last_tx_id,
                        last_term,
                    },
                )
            })
            .collect()
    }

    pub(crate) fn election_handle_votes(
        &mut self,
        now: Instant,
        responses: Vec<(NodeId, VoteOutcome)>,
    ) {
        // Snapshot the candidacy term up-front; any per-response
        // step-down ends the loop early.
        let election_term = match &self.state {
            NodeState::Candidate(c) => c.election_term,
            _ => return,
        };

        for (from, outcome) in responses {
            if !matches!(self.state, NodeState::Candidate(_)) {
                return;
            }
            let (term, granted) = match outcome {
                VoteOutcome::Granted { term } => (term, true),
                VoteOutcome::Denied { term } => (term, false),
                VoteOutcome::Failed => continue,
            };

            if term > election_term {
                self.observe_higher_term(term);
                self.transition_to_follower(now, None);
                return;
            }
            if term < election_term || !granted {
                continue;
            }

            let majority = self.quorum.majority();
            let majority_reached = match &mut self.state {
                NodeState::Candidate(c) => c.record_grant(from, majority),
                _ => return,
            };
            if majority_reached {
                self.become_leader_after_win(election_term, now);
                return;
            }
        }
    }

    pub(crate) fn election_handle_request_vote(
        &mut self,
        now: Instant,
        req: RequestVoteRequest,
    ) -> RequestVoteReply {
        let RequestVoteRequest {
            from,
            term,
            last_tx_id,
            last_term,
        } = req;

        let current_term = self.current_term();

        if term < current_term {
            return RequestVoteReply {
                term: current_term,
                granted: false,
            };
        }

        if term > current_term {
            self.observe_higher_term(term);
            self.transition_to_follower(now, None);
        }

        // §5.4.1 up-to-date check. Compare against our durable write
        // extent (`local_write_index`), not the commit watermark — a
        // voter with durably-written-but-uncommitted entries still
        // votes against a candidate whose log is shorter.
        let our_last_tx = self.local_write_index;
        let our_last_term = self.local_last_term();
        let candidate_up_to_date = (last_term > our_last_term)
            || (last_term == our_last_term && last_tx_id >= our_last_tx);
        if !candidate_up_to_date {
            return RequestVoteReply {
                term: self.current_term(),
                granted: false,
            };
        }

        let granted = self.persistence.vote(term, from);
        if granted {
            self.election_timer.reset(now);
        }

        RequestVoteReply {
            term: self.current_term(),
            granted,
        }
    }

    // ── Helpers also called from node.rs (validate_append_entries,
    //    on_append_entries_reply) ──────────────────────────────────

    /// Term of the local log entry at `local_write_index`. `0` means
    /// the log is empty (required by §5.4.1's up-to-date check). When
    /// `local_write_index > 0` there is, by construction, a covering
    /// term-log record — a `None` here means the persistence layer
    /// is corrupted, so we `expect`.
    pub(crate) fn local_last_term(&self) -> Term {
        if self.local_write_index == 0 {
            return 0;
        }
        self.persistence
            .term_at_tx(self.local_write_index)
            .expect("term_at_tx must cover local_write_index when local_write_index > 0")
            .term
    }

    /// Soonest pending wakeup across the election timer and the
    /// leader's per-peer heartbeat / in-flight expirations. Falls
    /// back to `now + heartbeat_interval` when nothing is pending —
    /// the cluster's tight loop expects an `Instant`, not an
    /// `Option`.
    pub(crate) fn next_pending_wakeup(&self, now: Instant) -> Instant {
        let mut best: Option<Instant> = None;
        if let Some(d) = self.election_timer.deadline() {
            best = Some(best.map(|b| b.min(d)).unwrap_or(d));
        }
        if let NodeState::Leader(l) = &self.state
            && let Some(d) = l.next_wakeup()
        {
            best = Some(best.map(|b| b.min(d)).unwrap_or(d));
        }
        best.unwrap_or_else(|| now + self.cfg.heartbeat_interval)
    }

    /// Observe a higher term via inbound RPC (request OR reply).
    /// Only the vote log is updated — the term log records actual
    /// entry-term boundaries, and observing a higher term via an RPC
    /// without receiving entries from that term must not establish
    /// one (writing a phantom record would make `term_at_tx` lookups
    /// match against it during the next §5.3 prev-log-term check).
    /// `current_term()` is `max(term-log, vote-log)`, so the read API
    /// still reflects the higher term.
    pub(crate) fn observe_higher_term(&mut self, term: Term) {
        self.persistence.observe_vote_term(term);
    }

    /// Step down to Follower, optionally recording the leader id.
    /// Re-arms the election timer and clears `current_term_first_tx`
    /// (the §5.4.2 watermark is leader-only; a stale value would
    /// gate the new follower's `leader_commit` propagation).
    /// Role changes are observed by the cluster polling
    /// `RaftNode::role()` — no action stream.
    pub(crate) fn transition_to_follower(
        &mut self,
        now: Instant,
        leader_id: Option<NodeId>,
    ) {
        let mut state = match leader_id {
            Some(id) => NodeState::Follower(FollowerState::with_leader(id)),
            None => NodeState::Follower(FollowerState::new()),
        };
        std::mem::swap(&mut self.state, &mut state);
        self.election_timer.arm(now);
        self.current_term_first_tx = 0;
    }

    // ── Private internals of `election_start` / `handle_votes`. ─────

    fn start_election_round(&mut self, now: Instant) -> bool {
        // `current_term()` is `max(term-log, vote-log)` — a candidate
        // that lost round N still bumped its vote log to N, so N+1
        // is the right next round.
        let new_term = self.current_term().saturating_add(1);

        if !self.persistence.vote(new_term, self.self_id) {
            // Vote refused (already voted for a different candidate
            // at this term). Leave the timer armed for the next round.
            self.election_timer.reset(now);
            return false;
        }

        let majority = self.quorum.majority();

        let candidate = CandidateState::new(new_term, self.self_id);
        self.state = NodeState::Candidate(candidate);
        self.election_timer.reset(now);

        // Single-node cluster: self-vote already crosses majority.
        let other_peers_empty = !self
            .peers
            .iter()
            .copied()
            .any(|p| p != self.self_id);
        if other_peers_empty {
            self.become_leader_after_win(new_term, now);
        }

        true
    }

    fn become_leader_after_win(&mut self, new_term: Term, now: Instant) {
        // §5.4.2 / Figure 8: anchor the new leader's term-log
        // boundary at `local_write_index + 1` so existing entries
        // keep their original term in `term_at_tx` lookups.
        let start_tx_id = self.local_write_index + 1;

        if !self.persistence.commit_term(new_term, start_tx_id) {
            debug!(
                "raft: node_id={} commit_term({}) refused (current={}); stepping down",
                self.self_id,
                new_term,
                self.persistence.current_term()
            );
            self.transition_to_follower(now, None);
            return;
        }

        self.election_timer.disarm();
        self.quorum.reset_peers(self.self_slot);
        self.current_term_first_tx = start_tx_id;
        // Seed the quorum self-slot from the new leader's *commit*
        // watermark, not the write watermark. Written-but-uncommitted
        // entries must not contribute their replica count to
        // `cluster_commit`; the §5.4.2 gate below blocks any advance
        // below `current_term_first_tx`.
        if let Some(adv) = self.quorum.advance(self.self_slot, self.local_commit_index)
            && !(self.peers.len() > 1 && adv < self.current_term_first_tx)
            && adv > self.cluster_commit_index
        {
            self.cluster_commit_index = adv;
        }

        let other_peers: Vec<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|p| *p != self.self_id)
            .collect();
        let leader = LeaderState::new(
            &other_peers,
            self.local_write_index,
            now,
            self.cfg.heartbeat_interval,
            self.cfg.rpc_timeout,
        );

        self.state = NodeState::Leader(leader);
        // Replication picks up here. Each peer's `next_heartbeat` is
        // initialised to `now` in `LeaderState::new`, so the very
        // first `PeerReplication::get_append_range` returns Some — the
        // immediate first-round AE that suppresses peer election
        // timers.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_vote_seeds_vote_set() {
        let s = CandidateState::new(1, 7);
        assert_eq!(s.election_term, 1);
        assert_eq!(s.votes_received(), 1);
    }

    #[test]
    fn record_grant_reaches_majority_on_threshold_crossing() {
        // 5-node cluster, majority = 3; need 2 more grants beyond self.
        let mut s = CandidateState::new(1, 1);
        assert!(!s.record_grant(2, 3));
        assert!(s.record_grant(3, 3));
        assert!(!s.record_grant(4, 3));
    }

    #[test]
    fn record_grant_is_idempotent_on_repeated_peer() {
        let mut s = CandidateState::new(1, 1);
        assert!(!s.record_grant(2, 3));
        assert!(!s.record_grant(2, 3));
        assert_eq!(s.votes_received(), 2);
    }
}
