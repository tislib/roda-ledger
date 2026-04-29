//! `RaftNode` — the pure-state-machine entry point. ADR-0017
//! §"Architectural Boundary": no internal threads, no I/O at all,
//! no timers, no async. Every transition is driven through
//! `step(now, event) -> Vec<Action>`.
//!
//! The library does not write to disk. Durable persistence of the
//! term and vote logs is the driver's responsibility: raft emits
//! `Action::PersistTerm` / `Action::PersistVote` describing the
//! intent and the driver feeds back `Event::TermPersisted` /
//! `Event::VotePersisted` once the write lands. The library's
//! in-memory `Term` / `Vote` views are updated synchronously when the
//! action is emitted; the ack is the signal that the on-disk state
//! has caught up.
//!
//! Internal organisation:
//!
//! - `state: NodeState` — role-specific state (Initializing /
//!   Follower / Candidate / Leader). Constructed fresh on every
//!   transition.
//! - `term: Term`, `vote: Vote` — pure in-memory views (see
//!   `term.rs`, `vote.rs`).
//! - `durable_term`, `durable_voted_for` — last `Event::*Persisted`
//!   the library observed. The library exposes these so callers
//!   building tooling can tell "intent" apart from "what's on disk
//!   right now"; the state machine itself relies on the driver
//!   applying actions in the order emitted (Persist before
//!   Send/Append) to keep durability ahead of externalisation.
//! - `quorum: Quorum` — per-peer match-index tracker. Active under
//!   the leader.
//! - `local_log_index` / `cluster_commit_index` — read API surface.
//! - `election_timer: ElectionTimer` — armed only when a non-leader
//!   role needs one.

use std::time::{Duration, Instant};

use spdlog::{debug, info, trace, warn};

use crate::action::Action;
use crate::candidate::CandidateState;
use crate::event::Event;
use crate::follower::FollowerState;
use crate::leader::{InFlightAppend, LeaderState};
use crate::log_entry::LogEntryMeta;
use crate::quorum::Quorum;
use crate::role::Role;
use crate::term::{Term as TermView, TermRecord};
use crate::timer::{ElectionTimer, ElectionTimerConfig};
use crate::types::{NodeId, RejectReason, Term, TxId};
use crate::vote::Vote as VoteView;

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

/// Role-specific state. `RaftNode` swaps between these on transitions
/// (see [`RaftNode::transition_to_*`]). Keeping the per-role data in
/// the variant rather than as separate fields lets the type system
/// enforce "you can only access leader state if you are the leader".
enum NodeState {
    Initializing,
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}

impl NodeState {
    fn role(&self) -> Role {
        match self {
            NodeState::Initializing => Role::Initializing,
            NodeState::Follower(_) => Role::Follower,
            NodeState::Candidate(_) => Role::Candidate,
            NodeState::Leader(_) => Role::Leader,
        }
    }
}

/// Durable state hydrated from disk by the driver and handed to
/// `RaftNode::new`. ADR-0017 §"No boot-time term bumping": the
/// library trusts whatever the driver read off disk and does not
/// modify it on construction.
#[derive(Clone, Debug, Default)]
pub struct RaftInitialState {
    /// Current term as stored in the durable vote log. `0` on a
    /// fresh install.
    pub current_term: Term,
    /// Node we voted for in `current_term`. `None` (mapped to `0`
    /// internally) means no vote granted in this term yet.
    pub voted_for: Option<NodeId>,
    /// Term-log records in append order, oldest first. Empty on a
    /// fresh install.
    pub term_records: Vec<TermRecord>,
    /// Highest `tx_id` durably present in the local log. `0` on a
    /// fresh install.
    pub local_log_index: TxId,
}

pub struct RaftNode {
    self_id: NodeId,
    peers: Vec<NodeId>,

    state: NodeState,
    election_timer: ElectionTimer,

    // Pure in-memory views of the persistent state. The library
    // mutates these synchronously; the driver mirrors via Action::Persist*
    // and acks via Event::*Persisted.
    term: TermView,
    vote: VoteView,

    // Last `(term, start_tx_id)` we've been told is durable on disk.
    // ADR-0017 §"two-phase persistence": the in-memory `term` /
    // `vote` move first; these trail and reflect what the driver has
    // actually written.
    durable_term: Term,
    durable_voted_for: NodeId,

    // Per-peer match-index ⇒ cluster_commit_index. Indexing follows
    // `peers` order. Slot 0 in `peers` may be self or a peer; we
    // resolve `self_slot` once at construction so leader paths can
    // mirror the leader's own commit progress.
    quorum: Quorum,
    self_slot: usize,

    // Local view of the log: the highest `tx_id` known durably
    // present locally. Leader advances this via
    // `Event::LocalCommitAdvanced`; follower via
    // `Event::LogAppendComplete`.
    local_log_index: TxId,

    // Last cluster-commit watermark we emitted in `Action::AdvanceClusterCommit`.
    // Used to dedupe — only emit on real advances.
    cluster_commit_index: TxId,

    // Coalesce `SetWakeup` — re-emit only if the soonest pending
    // deadline genuinely changed.
    last_emitted_wakeup: Option<Instant>,

    cfg: RaftConfig,
}

impl RaftNode {
    /// Construct a `RaftNode` from durable state the driver hydrated
    /// off disk. The library does no I/O here — the driver is
    /// responsible for reading the term log and vote log before
    /// calling `new` and for routing every subsequent `Action::Persist*`
    /// back to disk.
    ///
    /// Starting role:
    /// - Single-node cluster → `Initializing` until the first tick
    ///   triggers an immediate self-elect.
    /// - Multi-node cluster → `Initializing` until a leader speaks
    ///   (transitions to Follower) or the election timer fires
    ///   (transitions to Candidate). ADR-0017 §"No boot-time term
    ///   bumping": we do NOT increment term on boot.
    pub fn new(
        self_id: NodeId,
        peers: Vec<NodeId>,
        seed: u64,
        cfg: RaftConfig,
        initial: RaftInitialState,
    ) -> Self {
        assert!(self_id != 0, "raft: self_id must be non-zero");
        assert!(
            peers.contains(&self_id),
            "raft: peers list must contain self_id"
        );

        let voted_for_raw = initial.voted_for.unwrap_or(0);
        let term = TermView::from_records(initial.term_records);
        let vote = VoteView::new(initial.current_term, voted_for_raw);

        let self_slot = peers
            .iter()
            .position(|p| *p == self_id)
            .expect("checked above");
        let quorum = Quorum::new(peers.len());

        let durable_term = vote.current_term().max(term.current_term());

        Self {
            self_id,
            peers,
            state: NodeState::Initializing,
            election_timer: ElectionTimer::new(cfg.election_timer, seed),
            term,
            vote,
            durable_term,
            durable_voted_for: voted_for_raw,
            quorum,
            self_slot,
            local_log_index: initial.local_log_index,
            cluster_commit_index: 0,
            last_emitted_wakeup: None,
            cfg,
        }
    }

    // ─── Public read API (ADR §"Read-only query API") ────────────────────

    pub fn role(&self) -> Role {
        self.state.role()
    }

    pub fn current_term(&self) -> Term {
        // Vote log is the source of truth for "what term am I in".
        // The term log only records boundaries for entries actually
        // present in the local log; observing a higher term via an
        // RPC bumps the vote log without producing entries, so the
        // two can diverge. The max yields a consistent answer for
        // every observable role.
        self.vote.current_term().max(self.term.current_term())
    }

    pub fn commit_index(&self) -> TxId {
        self.local_log_index
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
        self.vote.voted_for()
    }

    /// Highest term the driver has acknowledged as durable on disk
    /// via `Event::TermPersisted` / `Event::VotePersisted`. Tracks
    /// behind `current_term()` between an `Action::Persist*` and its
    /// matching ack — useful for observability and for the driver's
    /// own invariant checks.
    pub fn durable_term(&self) -> Term {
        self.durable_term
    }

    /// Last `voted_for` the driver has acknowledged as durable.
    /// `None` if the durable record has `voted_for == 0`.
    pub fn durable_voted_for(&self) -> Option<NodeId> {
        match self.durable_voted_for {
            0 => None,
            n => Some(n),
        }
    }

    /// Best-known current leader id while this node is a follower.
    /// `None` from any other role and from a Follower that has not
    /// yet received an `AppendEntries`. Not part of the ADR's public
    /// read surface — kept as a crate-local query for tests and the
    /// driver's redirect path.
    pub fn current_leader(&self) -> Option<NodeId> {
        match &self.state {
            NodeState::Follower(f) => f.leader_id,
            _ => None,
        }
    }

    // ─── step() ──────────────────────────────────────────────────────────

    pub fn step(&mut self, now: Instant, event: Event) -> Vec<Action> {
        let mut out = Vec::new();
        match event {
            Event::Tick => self.on_tick(now, &mut out),
            Event::AppendEntriesRequest {
                from,
                term,
                prev_log_tx_id,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.on_append_entries_request(
                    now,
                    from,
                    term,
                    prev_log_tx_id,
                    prev_log_term,
                    entries,
                    leader_commit,
                    &mut out,
                );
            }
            Event::AppendEntriesReply {
                from,
                term,
                success,
                last_tx_id,
                reject_reason,
            } => {
                self.on_append_entries_reply(
                    now,
                    from,
                    term,
                    success,
                    last_tx_id,
                    reject_reason,
                    &mut out,
                );
            }
            Event::RequestVoteRequest {
                from,
                term,
                last_tx_id,
                last_term,
            } => self.on_request_vote_request(now, from, term, last_tx_id, last_term, &mut out),
            Event::RequestVoteReply {
                from,
                term,
                granted,
            } => self.on_request_vote_reply(now, from, term, granted, &mut out),
            Event::LocalCommitAdvanced { tx_id } => {
                self.on_local_commit_advanced(tx_id, &mut out);
            }
            Event::LogAppendComplete { tx_id } => self.on_log_append_complete(tx_id),
            Event::LogTruncateComplete { up_to } => self.on_log_truncate_complete(up_to),
            Event::TermPersisted { term, start_tx_id } => {
                self.on_term_persisted(term, start_tx_id);
            }
            Event::VotePersisted { term, voted_for } => {
                self.on_vote_persisted(term, voted_for);
            }
        }
        self.emit_wakeup_if_changed(&mut out);
        out
    }

    fn on_term_persisted(&mut self, term: Term, _start_tx_id: TxId) {
        if term > self.durable_term {
            self.durable_term = term;
        }
    }

    fn on_vote_persisted(&mut self, term: Term, voted_for: NodeId) {
        if term > self.durable_term {
            self.durable_term = term;
        }
        self.durable_voted_for = voted_for;
    }

    // ─── Tick ────────────────────────────────────────────────────────────

    fn on_tick(&mut self, now: Instant, out: &mut Vec<Action>) {
        // First-Tick lazy arm. The library cannot call `Instant::now()`
        // (ADR-0017); the constructor leaves the election timer
        // disarmed and we prime it on the first time-bearing event.
        // Subsequent Ticks proceed to the expiry check normally.
        if !self.role().is_leader() && self.election_timer.deadline().is_none() {
            self.election_timer.arm(now);
        }

        // Election timer expiry on non-leader roles transitions to
        // Candidate and runs an election round.
        if self.election_timer.is_expired(now) && !self.role().is_leader() {
            self.start_election(now, out);
        }

        // Drain expired in-flight RequestVote RPCs (treat as no-vote).
        if let NodeState::Candidate(c) = &mut self.state {
            let _expired = c.drain_expired(now);
            // No reply was received; we simply don't count the vote.
            // No state change beyond the in-flight removal.
        }

        // Leader heartbeats and per-peer RPC-deadline expiry.
        if let NodeState::Leader(_) = &self.state {
            self.leader_drive(now, out);
        }
    }

    // ─── Election machinery ──────────────────────────────────────────────

    fn start_election(&mut self, now: Instant, out: &mut Vec<Action>) {
        let new_term = self
            .term
            .current_term()
            .max(self.vote.current_term())
            .saturating_add(1);

        // Self-vote — pure in-memory update. If `vote.vote` refuses
        // (someone already voted in `new_term` for somebody else from a
        // stale rejoin), bail out and try next round.
        match self.vote.vote(new_term, self.self_id) {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    "raft: node_id={} could not self-vote in term {}; arming next round",
                    self.self_id, new_term
                );
                self.election_timer.reset(now);
                return;
            }
            Err(e) => {
                warn!("raft: node_id={} self-vote rejected: {:?}", self.self_id, e);
                self.election_timer.reset(now);
                return;
            }
        }
        // Driver writes durably and acks via `Event::VotePersisted`.
        out.push(Action::PersistVote {
            term: new_term,
            voted_for: self.self_id,
        });

        let mut candidate = CandidateState::new(new_term, self.self_id);
        let majority = self.quorum.majority();

        info!(
            "raft: node_id={} starting election for term {} (peers={}, majority={})",
            self.self_id,
            new_term,
            self.peers.len(),
            majority
        );

        let other_peers: Vec<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|p| *p != self.self_id)
            .collect();

        // Single-node cluster: the self-vote is already a majority.
        if other_peers.is_empty() {
            self.become_leader_after_win(new_term, now, out);
            return;
        }

        // Snapshot the local log tail for the §5.4.1 up-to-date check
        // each peer will run.
        let last_tx_id = self.local_log_index;
        let last_term = self.local_last_term();

        let expires_at = now + self.cfg.rpc_timeout;
        for peer in other_peers {
            candidate.record_in_flight(peer, expires_at);
            out.push(Action::SendRequestVote {
                to: peer,
                term: new_term,
                last_tx_id,
                last_term,
            });
        }

        self.election_timer.reset(now);
        self.state = NodeState::Candidate(candidate);
        out.push(Action::BecomeRole {
            role: Role::Candidate,
            term: new_term,
        });
    }

    fn on_request_vote_request(
        &mut self,
        now: Instant,
        from: NodeId,
        term: Term,
        last_tx_id: TxId,
        last_term: Term,
        out: &mut Vec<Action>,
    ) {
        let current_term = self.current_term();

        if term < current_term {
            out.push(Action::SendRequestVoteReply {
                to: from,
                term: current_term,
                granted: false,
            });
            return;
        }

        // Higher term observed — step down and clear our vote so we
        // can grant a new one in this term.
        if term > current_term {
            self.observe_higher_term(term, out);
            self.transition_to_follower(now, None);
        }

        // Up-to-date check (§5.4.1).
        let our_last_tx = self.local_log_index;
        let our_last_term = self.local_last_term();
        let candidate_up_to_date = (last_term > our_last_term)
            || (last_term == our_last_term && last_tx_id >= our_last_tx);
        if !candidate_up_to_date {
            out.push(Action::SendRequestVoteReply {
                to: from,
                term: self.current_term(),
                granted: false,
            });
            return;
        }

        // Try to grant. `Vote::vote` enforces "one vote per term";
        // returns false if we already voted for somebody else.
        let granted = match self.vote.vote(term, from) {
            Ok(g) => g,
            Err(e) => {
                warn!("raft: node_id={} vote rejected: {:?}", self.self_id, e);
                false
            }
        };

        if granted {
            // Reset election timer on a successful vote grant — we
            // don't want to start a competing election while we've
            // committed to someone else's. Action emitted *before*
            // the SendRequestVoteReply below so the driver writes
            // durably before externalising granted=true.
            self.election_timer.reset(now);
            out.push(Action::PersistVote {
                term,
                voted_for: from,
            });
        }

        out.push(Action::SendRequestVoteReply {
            to: from,
            term: self.current_term(),
            granted,
        });
    }

    fn on_request_vote_reply(
        &mut self,
        now: Instant,
        from: NodeId,
        term: Term,
        granted: bool,
        out: &mut Vec<Action>,
    ) {
        // Replies only matter while we're a candidate at the term
        // they answer. Anything else is stale and dropped.
        let (election_term, majority_reached) = match &mut self.state {
            NodeState::Candidate(c) => {
                if term > c.election_term {
                    // Higher term: step down regardless of grant.
                    self.observe_higher_term(term, out);
                    self.transition_to_follower(now, None);
                    return;
                }
                if term < c.election_term {
                    return; // Stale
                }
                c.complete_in_flight(from);
                if !granted {
                    return;
                }
                let majority = self.quorum.majority();
                (c.election_term, c.record_grant(from, majority))
            }
            _ => return,
        };

        if majority_reached {
            self.become_leader_after_win(election_term, now, out);
        }
    }

    fn become_leader_after_win(&mut self, new_term: Term, now: Instant, out: &mut Vec<Action>) {
        // Atomic term commit (ADR-0017 §"Required Invariants" #5).
        // `commit_term` rejects unless `current + 1 == new_term`, so
        // catch up the term log if observe-via-RPC let the vote log
        // run ahead.
        //
        // The new term's first *entry* will be at
        // `local_log_index + 1` — existing entries (1..=local_log_index)
        // belong to whatever previous term they were committed under,
        // and we must NOT shadow them with a new boundary at
        // `local_log_index`. That would break `term_at_tx(local_log_index)`
        // for §5.3 prev_log_term matching: the leader would tell a
        // follower "tx N is at term T_old" while the follower's term
        // log returns T_new. Picking `local_log_index + 1` keeps the
        // boundary where the new entries actually start.
        let start_tx_id = self.local_log_index + 1;
        let term_current = self.term.current_term();
        if new_term > term_current + 1 {
            if let Err(e) = self.term.observe(new_term - 1, start_tx_id) {
                warn!(
                    "raft: node_id={} term.observe(catch-up to {}) refused: {:?}; stepping down",
                    self.self_id,
                    new_term - 1,
                    e
                );
                self.transition_to_follower(now, None);
                return;
            }
            out.push(Action::PersistTerm {
                term: new_term - 1,
                start_tx_id,
            });
        }
        match self.term.commit_term(new_term, start_tx_id) {
            Ok(true) => {
                out.push(Action::PersistTerm {
                    term: new_term,
                    start_tx_id,
                });
            }
            Ok(false) => {
                // Concurrent observer pushed past us; step down.
                debug!(
                    "raft: node_id={} commit_term({}) refused (current={}); stepping down",
                    self.self_id,
                    new_term,
                    self.term.current_term()
                );
                self.transition_to_follower(now, None);
                return;
            }
            Err(e) => {
                warn!(
                    "raft: node_id={} commit_term({}) errored: {:?}; stepping down",
                    self.self_id, new_term, e
                );
                self.transition_to_follower(now, None);
                return;
            }
        }

        info!(
            "raft: node_id={} won election at term {} (start_tx_id={})",
            self.self_id, new_term, start_tx_id
        );

        self.election_timer.disarm();
        self.quorum.reset_peers(self.self_slot);
        // The leader's own slot mirrors local commit progress.
        if let Some(adv) = self.quorum.advance(self.self_slot, self.local_log_index) {
            self.publish_cluster_commit(adv, out);
        }

        let other_peers: Vec<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|p| *p != self.self_id)
            .collect();
        let leader = LeaderState::new(
            &other_peers,
            self.local_log_index,
            now,
            self.cfg.heartbeat_interval,
            self.cfg.rpc_timeout,
        );

        self.state = NodeState::Leader(leader);
        out.push(Action::BecomeRole {
            role: Role::Leader,
            term: new_term,
        });

        // Immediate first-round AppendEntries (heartbeat) so peers
        // don't run their election timers out.
        self.leader_drive(now, out);
    }

    // ─── AppendEntries (follower path) ───────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries_request(
        &mut self,
        now: Instant,
        from: NodeId,
        term: Term,
        prev_log_tx_id: TxId,
        prev_log_term: Term,
        entries: Vec<LogEntryMeta>,
        leader_commit: TxId,
        out: &mut Vec<Action>,
    ) {
        let current_term = self.current_term();

        if term < current_term {
            // Stale leader.
            out.push(Action::SendAppendEntriesReply {
                to: from,
                term: current_term,
                success: false,
                last_tx_id: self.local_log_index,
            });
            return;
        }

        // Equal or higher term ⇒ this peer is the (current) leader.
        // Step down to Follower if we're not already.
        if term > current_term {
            self.observe_higher_term(term, out);
        }
        // Always settle into Follower with `from` recorded as leader
        // — including when we were already a follower of someone else
        // in the same term (defensive).
        self.transition_to_follower(now, Some(from));
        self.election_timer.reset(now);

        // §5.3 prev_log_term match. `prev_log_tx_id == 0` is the
        // "start of log" case — accept by convention.
        if prev_log_tx_id != 0 {
            let our_term_at_prev = self
                .term
                .term_at_tx(prev_log_tx_id)
                .map(|r| r.term)
                .unwrap_or(0);
            let log_covers_prev = prev_log_tx_id <= self.local_log_index;
            if !log_covers_prev || our_term_at_prev != prev_log_term {
                // Mismatch — truncate everything strictly past the
                // last point we agree on. A follower whose log
                // doesn't cover prev_log_tx_id at all simply rejects
                // and the leader will retry from earlier.
                if log_covers_prev {
                    let after = prev_log_tx_id.saturating_sub(1);
                    // Action::TruncateLog tells the driver to drop
                    // entry-log + term-log records past `after`; we
                    // mirror the term-log side immediately in memory.
                    out.push(Action::TruncateLog { after_tx_id: after });
                    self.term.truncate_after(after);
                    self.local_log_index = after;
                    if self.cluster_commit_index > self.local_log_index {
                        self.cluster_commit_index = self.local_log_index;
                    }
                }
                out.push(Action::SendAppendEntriesReply {
                    to: from,
                    term: self.current_term(),
                    success: false,
                    last_tx_id: self.local_log_index,
                });
                return;
            }
        }

        // Append entries. The driver will append payload + metadata
        // to its storage and ack via `Event::LogAppendComplete` —
        // which advances `local_log_index`. We optimistically
        // advance the term log here so subsequent RPCs in this batch
        // see consistent term-at-tx lookups.
        for e in &entries {
            if e.term > self.term.current_term() {
                if let Err(err) = self.term.observe(e.term, e.tx_id) {
                    warn!(
                        "raft: node_id={} term.observe({},{}) refused: {:?}",
                        self.self_id, e.term, e.tx_id, err
                    );
                } else {
                    out.push(Action::PersistTerm {
                        term: e.term,
                        start_tx_id: e.tx_id,
                    });
                }
            }
            out.push(Action::AppendLog {
                tx_id: e.tx_id,
                term: e.term,
            });
        }

        // Optimistic local-log advance: the driver's
        // `LogAppendComplete` will redundantly set the same value,
        // but the leader's reply needs to know our high-water mark
        // *now*.
        if let Some(last) = entries.last() {
            self.local_log_index = last.tx_id;
        }

        // Cluster-commit clamp.
        let new_cluster = leader_commit.min(self.local_log_index);
        if new_cluster > self.cluster_commit_index {
            self.publish_cluster_commit(new_cluster, out);
        }

        out.push(Action::SendAppendEntriesReply {
            to: from,
            term: self.current_term(),
            success: true,
            last_tx_id: self.local_log_index,
        });
    }

    // ─── AppendEntries (leader path) ─────────────────────────────────────

    fn leader_drive(&mut self, now: Instant, out: &mut Vec<Action>) {
        // First, expire stale in-flight RPCs so they don't block
        // re-sends.
        if let NodeState::Leader(l) = &mut self.state {
            for p in l.peers.values_mut() {
                if let Some(infl) = p.in_flight
                    && now >= infl.expires_at
                {
                    trace!("raft: in-flight AppendEntries timed out");
                    p.in_flight = None;
                }
            }
        }

        // Snapshot peer ids and their need-to-send status — we
        // can't borrow `self` mutably while iterating `l.peers`.
        let peers_to_send: Vec<NodeId> = match &self.state {
            NodeState::Leader(l) => l
                .peers
                .iter()
                .filter(|(_, p)| p.in_flight.is_none() && now >= p.next_heartbeat)
                .map(|(id, _)| *id)
                .collect(),
            _ => return,
        };

        for peer in peers_to_send {
            self.leader_send_to(peer, now, out);
        }
    }

    fn leader_send_to(&mut self, peer: NodeId, now: Instant, out: &mut Vec<Action>) {
        let current_term = self.current_term();
        let leader_commit = self.cluster_commit_index;
        let max_entries = self.cfg.max_entries_per_append;

        // Compute the entry batch from `next_index..=local_log_index`.
        let (next_index, prev_log_tx_id, prev_log_term, entries) = {
            let progress = match &self.state {
                NodeState::Leader(l) => l.peers.get(&peer).cloned(),
                _ => return,
            };
            let progress = match progress {
                Some(p) => p,
                None => return,
            };
            let next_index = progress.next_index;
            let prev_log_tx_id = next_index.saturating_sub(1);
            let prev_log_term = self
                .term
                .term_at_tx(prev_log_tx_id)
                .map(|r| r.term)
                .unwrap_or(0);

            // Build the entry batch — metadata only, the driver fills
            // in payloads from storage indexed by tx_id.
            let mut entries = Vec::new();
            let mut tx = next_index;
            while tx <= self.local_log_index && entries.len() < max_entries {
                let term_at = self
                    .term
                    .term_at_tx(tx)
                    .map(|r| r.term)
                    .unwrap_or(current_term);
                entries.push(LogEntryMeta::new(tx, term_at));
                tx += 1;
            }

            (next_index, prev_log_tx_id, prev_log_term, entries)
        };

        // Even with no entries (heartbeat case) we still send: the
        // RPC keeps the follower's election timer reset and pushes
        // the latest leader_commit.
        let last_tx_in_batch = entries.last().map(|e| e.tx_id).unwrap_or(prev_log_tx_id);

        out.push(Action::SendAppendEntries {
            to: peer,
            term: current_term,
            prev_log_tx_id,
            prev_log_term,
            entries,
            leader_commit,
        });

        // Re-arm in-flight + heartbeat for this peer.
        let interval = self.cfg.heartbeat_interval;
        let rpc_to = self.cfg.rpc_timeout;
        if let NodeState::Leader(l) = &mut self.state
            && let Some(p) = l.peers.get_mut(&peer)
        {
            p.in_flight = Some(InFlightAppend {
                last_tx_id_in_batch: last_tx_in_batch,
                expires_at: now + rpc_to,
            });
            p.next_heartbeat = now + interval;
        }
        let _ = next_index;
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries_reply(
        &mut self,
        now: Instant,
        from: NodeId,
        term: Term,
        success: bool,
        last_tx_id: TxId,
        reject_reason: Option<RejectReason>,
        out: &mut Vec<Action>,
    ) {
        let current_term = self.current_term();
        if term > current_term {
            self.observe_higher_term(term, out);
            self.transition_to_follower(now, None);
            return;
        }
        // Only the leader handles replies meaningfully. Stale
        // replies from a previous leadership window are dropped.
        let advanced = match &mut self.state {
            NodeState::Leader(l) => {
                let progress = match l.peers.get_mut(&from) {
                    Some(p) => p,
                    None => return,
                };
                progress.in_flight = None;
                if success {
                    if last_tx_id > progress.match_index {
                        progress.match_index = last_tx_id;
                    }
                    progress.next_index = last_tx_id + 1;
                    Some(last_tx_id)
                } else {
                    match reject_reason {
                        Some(RejectReason::TermBehind) => {
                            // The higher-term branch above already
                            // handled the step-down; nothing more to
                            // do here.
                        }
                        Some(RejectReason::RpcTimeout) => {
                            // Already cleared in_flight; next
                            // heartbeat retries.
                        }
                        // LogMismatch (explicit) and the unannotated
                        // case both mean §5.3: walk `next_index`
                        // back one entry so the next heartbeat
                        // tries from earlier. Clamp at 1 — tx_id 0
                        // is the "no entry" sentinel.
                        Some(RejectReason::LogMismatch) | None => {
                            progress.next_index = progress.next_index.saturating_sub(1).max(1);
                        }
                    }
                    None
                }
            }
            _ => return,
        };

        if let Some(advanced_to) = advanced {
            let self_slot_index = match self.peers.iter().position(|p| *p == from) {
                Some(idx) => idx,
                None => return,
            };
            if let Some(new_cluster) = self.quorum.advance(self_slot_index, advanced_to) {
                self.publish_cluster_commit(new_cluster, out);
            }
        }
    }

    // ─── LocalCommitAdvanced (leader) ────────────────────────────────────

    fn on_local_commit_advanced(&mut self, tx_id: TxId, out: &mut Vec<Action>) {
        if tx_id > self.local_log_index {
            self.local_log_index = tx_id;
        }
        // Leader: feeds into quorum.
        if matches!(self.state, NodeState::Leader(_))
            && let Some(new_cluster) = self.quorum.advance(self.self_slot, tx_id)
        {
            self.publish_cluster_commit(new_cluster, out);
        }
    }

    fn on_log_append_complete(&mut self, tx_id: TxId) {
        if tx_id > self.local_log_index {
            self.local_log_index = tx_id;
        }
    }

    fn on_log_truncate_complete(&mut self, up_to: TxId) {
        if self.local_log_index > up_to {
            self.local_log_index = up_to;
        }
        if self.cluster_commit_index > up_to {
            self.cluster_commit_index = up_to;
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────

    fn observe_higher_term(&mut self, term: Term, out: &mut Vec<Action>) {
        // Only the vote log records the term advance — the term log
        // captures *entry* boundaries (where a new leader's first
        // entry lands), and observing a higher term via an RPC
        // doesn't produce any entries. Writing a term-log record
        // here would create a phantom boundary at the wrong
        // start_tx_id and break `term_at_tx` lookups for the
        // §5.4.1 up-to-date check.
        match self.vote.observe_term(term) {
            Ok(()) => {
                out.push(Action::PersistVote { term, voted_for: 0 });
            }
            Err(e) => {
                warn!(
                    "raft: node_id={} vote.observe_term({}) refused: {:?}",
                    self.self_id, term, e
                );
            }
        }
    }

    /// Term of the local log entry at `local_log_index`. `0` means the
    /// log is empty — required by §5.4.1's up-to-date check, which
    /// treats the empty log as "term 0".
    fn local_last_term(&self) -> Term {
        if self.local_log_index == 0 {
            return 0;
        }
        self.term
            .term_at_tx(self.local_log_index)
            .map(|r| r.term)
            .unwrap_or(0)
    }

    fn transition_to_follower(&mut self, now: Instant, leader_id: Option<NodeId>) {
        // `transition_to_follower` is idempotent if we're already a
        // follower — still useful: it re-records the leader id.
        let mut state = match leader_id {
            Some(id) => NodeState::Follower(FollowerState::with_leader(id)),
            None => NodeState::Follower(FollowerState::new()),
        };
        std::mem::swap(&mut self.state, &mut state);
        self.election_timer.arm(now);
    }

    fn publish_cluster_commit(&mut self, new_value: TxId, out: &mut Vec<Action>) {
        if new_value > self.cluster_commit_index {
            self.cluster_commit_index = new_value;
            out.push(Action::AdvanceClusterCommit { tx_id: new_value });
        }
    }

    fn next_pending_wakeup(&self) -> Option<Instant> {
        let mut best: Option<Instant> = None;
        if let Some(d) = self.election_timer.deadline() {
            best = Some(best.map(|b| b.min(d)).unwrap_or(d));
        }
        if let NodeState::Candidate(c) = &self.state
            && let Some(d) = c.next_rpc_expiry()
        {
            best = Some(best.map(|b| b.min(d)).unwrap_or(d));
        }
        if let NodeState::Leader(l) = &self.state
            && let Some(d) = l.next_wakeup()
        {
            best = Some(best.map(|b| b.min(d)).unwrap_or(d));
        }
        best
    }

    fn emit_wakeup_if_changed(&mut self, out: &mut Vec<Action>) {
        let next = self.next_pending_wakeup();
        if next != self.last_emitted_wakeup {
            if let Some(at) = next {
                out.push(Action::SetWakeup { at });
            }
            self.last_emitted_wakeup = next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh(self_id: NodeId, peers: Vec<NodeId>) -> RaftNode {
        RaftNode::new(
            self_id,
            peers,
            42,
            RaftConfig::default(),
            RaftInitialState::default(),
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
        // Bootstrap tick — arms the election timer.
        let _ = node.step(t0, Event::Tick);
        // Far-future tick — election timer expired, runs the round.
        let actions = node.step(t0 + Duration::from_secs(60), Event::Tick);
        assert_eq!(node.role(), Role::Candidate);
        assert_eq!(node.current_term(), 1);
        assert_eq!(node.voted_for(), Some(1));
        let send_count = actions
            .iter()
            .filter(|a| matches!(a, Action::SendRequestVote { .. }))
            .count();
        assert_eq!(send_count, 2);
    }

    #[test]
    fn single_node_election_immediately_becomes_leader() {
        let mut node = fresh(1, vec![1]);
        let t0 = Instant::now();
        let _ = node.step(t0, Event::Tick);
        let actions = node.step(t0 + Duration::from_secs(60), Event::Tick);
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.current_term(), 1);
        let became = actions
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    Action::BecomeRole {
                        role: Role::Leader,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(became, 1);
    }

    #[test]
    fn request_vote_with_higher_term_grants_and_steps_down() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let actions = node.step(
            Instant::now(),
            Event::RequestVoteRequest {
                from: 2,
                term: 5,
                last_tx_id: 0,
                last_term: 0,
            },
        );
        assert_eq!(node.current_term(), 5);
        assert_eq!(node.voted_for(), Some(2));
        let granted = actions
            .iter()
            .any(|a| matches!(a, Action::SendRequestVoteReply { granted: true, .. }));
        assert!(granted);
    }

    #[test]
    fn request_vote_with_outdated_log_is_refused() {
        let mut node = fresh(1, vec![1, 2, 3]);
        node.term.commit_term(1, 0).unwrap();
        node.term.commit_term(2, 50).unwrap();
        node.term.commit_term(3, 100).unwrap();
        node.local_log_index = 100;

        let actions = node.step(
            Instant::now(),
            Event::RequestVoteRequest {
                from: 2,
                term: 4,
                last_tx_id: 50,
                last_term: 2,
            },
        );
        let granted_false = actions
            .iter()
            .any(|a| matches!(a, Action::SendRequestVoteReply { granted: false, .. }));
        assert!(granted_false);
    }

    #[test]
    fn append_entries_with_lower_term_is_rejected() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let mut bootstrap = Vec::new();
        node.observe_higher_term(5, &mut bootstrap);
        let actions = node.step(
            Instant::now(),
            Event::AppendEntriesRequest {
                from: 2,
                term: 3,
                prev_log_tx_id: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        );
        let success = actions.iter().any(|a| match a {
            Action::SendAppendEntriesReply { success, .. } => *success,
            _ => false,
        });
        assert!(!success);
        assert_eq!(node.current_term(), 5);
    }

    #[test]
    fn append_entries_first_batch_appends_and_replies_success() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let actions = node.step(
            Instant::now(),
            Event::AppendEntriesRequest {
                from: 2,
                term: 1,
                prev_log_tx_id: 0,
                prev_log_term: 0,
                entries: vec![LogEntryMeta::new(1, 1), LogEntryMeta::new(2, 1)],
                leader_commit: 0,
            },
        );
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), 1);
        let appends = actions
            .iter()
            .filter(|a| matches!(a, Action::AppendLog { .. }))
            .count();
        assert_eq!(appends, 2);
        let success = actions.iter().any(|a| match a {
            Action::SendAppendEntriesReply { success, .. } => *success,
            _ => false,
        });
        assert!(success);
    }

    #[test]
    fn append_entries_with_log_mismatch_truncates_and_rejects() {
        let mut node = fresh(1, vec![1, 2, 3]);
        node.term.commit_term(1, 0).unwrap();
        node.local_log_index = 5;

        let actions = node.step(
            Instant::now(),
            Event::AppendEntriesRequest {
                from: 2,
                term: 2,
                prev_log_tx_id: 5,
                prev_log_term: 2,
                entries: vec![],
                leader_commit: 0,
            },
        );
        let truncates = actions
            .iter()
            .filter(|a| matches!(a, Action::TruncateLog { .. }))
            .count();
        assert_eq!(truncates, 1);
        let success = actions.iter().any(|a| match a {
            Action::SendAppendEntriesReply { success, .. } => *success,
            _ => false,
        });
        assert!(!success);
    }

    #[test]
    fn local_commit_advanced_advances_local_log_only_off_leader() {
        let mut node = fresh(1, vec![1]);
        let t0 = Instant::now();
        let _ = node.step(t0, Event::Tick);
        let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
        assert!(node.role().is_leader());
        let actions = node.step(
            t0 + Duration::from_secs(61),
            Event::LocalCommitAdvanced { tx_id: 7 },
        );
        assert_eq!(node.commit_index(), 7);
        assert_eq!(node.cluster_commit_index(), 7);
        let advances = actions
            .iter()
            .filter(|a| matches!(a, Action::AdvanceClusterCommit { tx_id: 7 }))
            .count();
        assert_eq!(advances, 1);
    }
}
