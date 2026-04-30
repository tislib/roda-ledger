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
//! - `local_log_index` / `cluster_commit_index` — read API surface,
//!   advanced by `Event::*Complete` and `Event::LocalCommitAdvanced`.
//! - `election_timer: ElectionTimer` — armed only when a non-leader
//!   role needs one.

use std::time::{Duration, Instant};

use spdlog::{debug, info, trace, warn};

use crate::action::Action;
use crate::candidate::CandidateState;
use crate::event::Event;
use crate::follower::FollowerState;
use crate::leader::{InFlightAppend, LeaderState};
use crate::log_entry::LogEntryRange;
use crate::persistence::Persistence;
use crate::quorum::Quorum;
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
            return Err(
                "RaftConfig: heartbeat_interval * 2 must be < election_timer.min_ms",
            );
        }
        Ok(())
    }
}

/// Role-specific state held inline so the type system enforces
/// "you can only access leader state if you are the leader".
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

pub struct RaftNode<P: Persistence> {
    self_id: NodeId,
    peers: Vec<NodeId>,

    state: NodeState,
    election_timer: ElectionTimer,

    persistence: P,

    quorum: Quorum,
    self_slot: usize,

    /// Set once the library has emitted `Action::FatalError`. Every
    /// subsequent `step()` returns an empty action vec — the node is
    /// frozen and the driver must shut it down. We don't re-emit the
    /// fatal: the driver already saw it once.
    failed: bool,

    local_log_index: TxId,
    cluster_commit_index: TxId,

    /// First `tx_id` of the leader's current term — set on
    /// `become_leader_after_win` to `local_log_index + 1` (matching
    /// the `start_tx_id` passed to `commit_term`). Cleared on
    /// step-down so a stale value doesn't leak into a follower's
    /// `leader_commit` clamping.
    ///
    /// Raft §5.4.2 / Figure 8: a leader must not advance
    /// `cluster_commit` to a tx_id below this watermark. Doing so
    /// would commit prior-term entries by counting current-term
    /// replicas, and a future leader could overwrite them. The
    /// gate lives in `publish_cluster_commit`. Followers bypass it
    /// (the field is reset to 0 on follower transition); they
    /// trust the leader's `leader_commit`, which the leader has
    /// already gated on its end.
    current_term_first_tx: TxId,

    last_emitted_wakeup: Option<Instant>,

    cfg: RaftConfig,
}

impl<P: Persistence> RaftNode<P> {
    /// Construct a `RaftNode` from an already-opened `Persistence`.
    /// The driver is responsible for hydrating the persistence off
    /// disk (term log + vote log) before calling `new`. The library
    /// reads through the trait — no separate "rehydrate" event.
    ///
    /// `local_log_index` starts at 0; the driver feeds
    /// `Event::LogAppendComplete` for any entries already on disk
    /// before exposing the node to RPCs.
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
            failed: false,
            local_log_index: 0,
            cluster_commit_index: 0,
            current_term_first_tx: 0,
            last_emitted_wakeup: None,
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

    // ─── step() ──────────────────────────────────────────────────────────

    pub fn step(&mut self, now: Instant, event: Event) -> Vec<Action> {
        // Once the library has emitted a `FatalError`, every
        // subsequent step is a no-op. The driver already received the
        // signal and must shut the node down; continuing would risk
        // committing non-durable entries or violating §5.3 / §5.4
        // because in-memory state and on-disk state may have diverged.
        if self.failed {
            return Vec::new();
        }
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
            Event::LocalWriteAdvanced { tx_id } => {
                self.on_local_write_advanced(tx_id);
            }
            Event::LogAppendComplete { tx_id } => {
                self.on_log_append_complete(tx_id, &mut out);
            }
            Event::LogTruncateComplete { up_to } => self.on_log_truncate_complete(up_to),
        }
        // Skip wakeup emission if a fatal happened in this step —
        // there is no future work for the timer to wake up to.
        if !self.failed {
            self.emit_wakeup_if_changed(&mut out);
        }
        out
    }

    /// Mark the node fatally failed and emit `Action::FatalError`.
    /// Callers must `return` immediately after invoking this — once
    /// `failed` is set, the rest of the current handler should not
    /// queue further actions or mutate state.
    fn fatal(&mut self, reason: &'static str, out: &mut Vec<Action>) {
        self.failed = true;
        out.push(Action::FatalError { reason });
    }

    // ─── Tick ────────────────────────────────────────────────────────────

    fn on_tick(&mut self, now: Instant, out: &mut Vec<Action>) {
        // First-Tick lazy arm. The library cannot call `Instant::now()`
        // (ADR-0017); the constructor leaves the election timer
        // disarmed and we prime it on the first time-bearing event.
        if !self.role().is_leader() && self.election_timer.deadline().is_none() {
            self.election_timer.arm(now);
        }

        if self.election_timer.is_expired(now) && !self.role().is_leader() {
            self.start_election(now, out);
        }

        // Drain expired in-flight RequestVote RPCs (treat as no-vote).
        if let NodeState::Candidate(c) = &mut self.state {
            let _expired = c.drain_expired(now);
        }

        if let NodeState::Leader(_) = &self.state {
            self.leader_drive(now, out);
        }
    }

    // ─── Election machinery ──────────────────────────────────────────────

    fn start_election(&mut self, now: Instant, out: &mut Vec<Action>) {
        // Use max-of-(term-log, vote-log) so a candidate that fails
        // round N still bumps to N+1 next round — the term log
        // doesn't advance on a lost election, only the vote log does.
        let new_term = self.current_term().saturating_add(1);

        // Self-vote (durable, synchronous through the trait).
        match self.persistence.vote(new_term, self.self_id) {
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
                warn!(
                    "raft: node_id={} self-vote durable write failed: {}",
                    self.self_id, e
                );
                self.election_timer.reset(now);
                return;
            }
        }

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

        // Single-node cluster: self-vote is already a majority.
        if other_peers.is_empty() {
            self.become_leader_after_win(new_term, now, out);
            return;
        }

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

        // Higher term observed — persist FIRST, then step down. The
        // helper `observe_higher_term` uses `local_log_index + 1` as
        // the term-log boundary so we never shadow our own entries
        // (using the candidate's `last_tx_id` would record the new
        // term at a tx that already has a term assigned, breaking
        // §5.3 prev_log_term lookups for our existing entries).
        if term > current_term {
            self.observe_higher_term(term);
            self.transition_to_follower(now, None);
        }

        // §5.4.1 up-to-date check.
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

        // Try to grant — durable write through the trait.
        let granted = match self.persistence.vote(term, from) {
            Ok(g) => g,
            Err(e) => {
                warn!(
                    "raft: node_id={} vote durable write failed: {}",
                    self.self_id, e
                );
                false
            }
        };

        if granted {
            // Reset election timer on successful vote grant — don't
            // start a competing election while we've committed to
            // someone else's.
            self.election_timer.reset(now);
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
        let (election_term, majority_reached) = match &mut self.state {
            NodeState::Candidate(c) => {
                if term > c.election_term {
                    self.observe_higher_term(term);
                    self.transition_to_follower(now, None);
                    return;
                }
                if term < c.election_term {
                    return;
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
        // The new term's first entry will live at `local_log_index +
        // 1`; existing entries belong to whatever earlier term they
        // were committed under, so the new boundary must not be
        // recorded at or before `local_log_index` or it would
        // shadow them in `term_at_tx` lookups.
        //
        // No catch-up record. The vote log can validly race ahead of
        // the term log (observed-higher-term-via-RPC), and Raft does
        // not require term-log records to be contiguous. The
        // `Persistence::commit_term` contract permits any
        // `current < expected` jump.
        let start_tx_id = self.local_log_index + 1;

        // Atomic election-win commit. ADR-0017 §"Required Invariants" #5.
        match self.persistence.commit_term(new_term, start_tx_id) {
            Ok(true) => {}
            Ok(false) => {
                debug!(
                    "raft: node_id={} commit_term({}) refused (current={}); stepping down",
                    self.self_id,
                    new_term,
                    self.persistence.current_term()
                );
                self.transition_to_follower(now, None);
                return;
            }
            Err(e) => {
                warn!(
                    "raft: node_id={} commit_term({}) failed: {}; stepping down",
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
        // Record this term's first-entry watermark *before* the
        // first publish_cluster_commit can fire — Raft §5.4.2 /
        // Figure 8 gate (see `publish_cluster_commit`).
        self.current_term_first_tx = start_tx_id;
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
        entries: LogEntryRange,
        leader_commit: TxId,
        out: &mut Vec<Action>,
    ) {
        let current_term = self.current_term();

        if term < current_term {
            out.push(Action::SendAppendEntriesReply {
                to: from,
                term: current_term,
                success: false,
                last_tx_id: self.local_log_index,
            });
            return;
        }

        if term > current_term {
            self.observe_higher_term(term);
        }
        self.transition_to_follower(now, Some(from));
        self.election_timer.reset(now);

        // §5.3 prev_log_term match.
        if prev_log_tx_id != 0 {
            let our_term_at_prev = self
                .persistence
                .term_at_tx(prev_log_tx_id)
                .map(|r| r.term)
                .unwrap_or(0);
            let log_covers_prev = prev_log_tx_id <= self.local_log_index;
            if !log_covers_prev || our_term_at_prev != prev_log_term {
                if log_covers_prev {
                    let after = prev_log_tx_id.saturating_sub(1);
                    // Leader Completeness §5.4: a leader cannot ask
                    // a follower to truncate below the cluster commit
                    // index. If this fires, a Raft invariant has
                    // already been violated upstream — surface it
                    // loudly in debug builds rather than silently
                    // clamping over the bug. The release-build clamp
                    // below stays as belt-and-suspenders.
                    debug_assert!(
                        after >= self.cluster_commit_index,
                        "truncation below cluster_commit_index: after={} cluster={}",
                        after,
                        self.cluster_commit_index
                    );
                    // Library's term-log mirror: durable through the
                    // trait. If this write fails the term log is
                    // intact (rename-based replacement) but we cannot
                    // proceed — the driver's entry log truncation
                    // would otherwise leave the two stores diverged.
                    if let Err(e) = self.persistence.truncate_term_after(after) {
                        warn!(
                            "raft: node_id={} truncate_term_after({}) failed: {}",
                            self.self_id, after, e
                        );
                        self.fatal("truncate_term_after failed", out);
                        return;
                    }
                    out.push(Action::TruncateLog { after_tx_id: after });
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

        // Append entries. The range is same-term by construction —
        // observe once if the term is new. The driver's entry log is
        // updated via `Action::AppendLog`; the term-log mirror is
        // updated synchronously through the trait above.
        if !entries.is_empty() {
            if entries.term > self.persistence.current_term()
                && let Err(err) = self
                    .persistence
                    .observe_term(entries.term, entries.start_tx_id)
            {
                warn!(
                    "raft: node_id={} observe_term({},{}) failed: {}",
                    self.self_id, entries.term, entries.start_tx_id, err
                );
                self.fatal("observe_term failed", out);
                return;
            }
            out.push(Action::AppendLog { range: entries });

            // Park the success reply until the driver has durably
            // appended the entries (it acks via
            // `Event::LogAppendComplete`). Sending success here would
            // let the leader count an entry as replicated before it
            // is on disk on this follower — a write-ahead-invariant
            // violation, see ADR-0017 §"Required Invariants" #3.
            let parked_tx_id = entries
                .last_tx_id()
                .expect("non-empty range has a last_tx_id");
            let reply_term = self.current_term();
            if let NodeState::Follower(f) = &mut self.state {
                f.park_reply(crate::follower::PendingReply {
                    to: from,
                    term: reply_term,
                    last_tx_id: parked_tx_id,
                    leader_commit,
                });
            }

            // Note: `local_log_index` and `cluster_commit_index` are
            // NOT advanced here — they advance when
            // `on_log_append_complete` runs, which is also where the
            // parked reply is drained and `leader_commit` is applied.
            return;
        }

        // Heartbeat path (no entries): no durability needed, reply
        // immediately. `leader_commit` still propagates.
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

        let (prev_log_tx_id, prev_log_term, entries) = {
            let (progress, last_written) = match &self.state {
                NodeState::Leader(l) => (l.peers.get(&peer).cloned(), l.last_written),
                _ => return,
            };
            let progress = match progress {
                Some(p) => p,
                None => return,
            };
            let next_index = progress.next_index;
            let prev_log_tx_id = next_index.saturating_sub(1);
            let prev_log_term = self
                .persistence
                .term_at_tx(prev_log_tx_id)
                .map(|r| r.term)
                .unwrap_or(0);

            // Build a single same-term contiguous range starting at
            // next_index. The range stops at the next term boundary
            // or at `max_entries`, whichever comes first; multi-term
            // catch-up takes multiple AEs. Heartbeat (next_index >
            // last_written) → empty range.
            //
            // The upper bound is `last_written` — the leader's
            // raft-log durability watermark, advanced by
            // `Event::LocalWriteAdvanced`. Replication is gated on
            // raft-log durability, NOT on the ledger-commit signal
            // (`local_log_index` / `LocalCommitAdvanced`); shipping
            // an entry to followers as soon as it is on disk is
            // safe and correct.
            //
            // Inside the `else` branch `next_index <= last_written`,
            // so `next_index` indexes a real entry. Every in-log
            // entry has a covering term-log record (`observe_term` is
            // called for any new term, `commit_term` for a leader's
            // own first entry; truncation drops both the entry and
            // its term record together via `truncate_term_after`).
            // A `None` from `term_at_tx` therefore signals a
            // corrupted persistence — `expect` surfaces it instead
            // of silently downgrading the entry to `current_term`,
            // which would produce a malformed multi-term
            // `LogEntryRange`.
            let entries = if next_index > last_written {
                let _ = current_term; // not consulted in the heartbeat path
                LogEntryRange::empty()
            } else {
                let range_term = self
                    .persistence
                    .term_at_tx(next_index)
                    .expect("term_at_tx must cover next_index when next_index <= last_written")
                    .term;
                let mut count: u64 = 0;
                let mut tx = next_index;
                while tx <= last_written && count < max_entries as u64 {
                    let t = self
                        .persistence
                        .term_at_tx(tx)
                        .expect("term_at_tx must cover every in-log tx_id")
                        .term;
                    if t != range_term {
                        break;
                    }
                    count += 1;
                    tx += 1;
                }
                LogEntryRange::new(next_index, count, range_term)
            };

            (prev_log_tx_id, prev_log_term, entries)
        };

        let last_tx_in_batch = entries.last_tx_id().unwrap_or(prev_log_tx_id);

        out.push(Action::SendAppendEntries {
            to: peer,
            term: current_term,
            prev_log_tx_id,
            prev_log_term,
            entries,
            leader_commit,
        });

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
            self.observe_higher_term(term);
            self.transition_to_follower(now, None);
            return;
        }

        // `Ok(adv)` — peer acked, advance the quorum slot to `adv`.
        // `Err(Some(peer_last))` — `LogMismatch`; regress the
        // quorum slot defensively to the peer's reported
        // `last_tx_id`. `Err(None)` — non-mismatch reject (term
        // behind, rpc timeout); leave the quorum alone.
        let outcome: Result<TxId, Option<TxId>> = match &mut self.state {
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
                    Ok(last_tx_id)
                } else {
                    match reject_reason {
                        Some(RejectReason::TermBehind) | Some(RejectReason::RpcTimeout) => Err(None),
                        // LogMismatch (explicit) and the unannotated
                        // case both mean §5.3: walk `next_index` back
                        // one entry. Clamp at 1 — tx_id 0 is the "no
                        // entry" sentinel.
                        Some(RejectReason::LogMismatch) | None => {
                            progress.next_index = progress.next_index.saturating_sub(1).max(1);
                            Err(Some(last_tx_id))
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
                if let Some(new_cluster) = self.quorum.advance(peer_slot_index, advanced_to) {
                    self.publish_cluster_commit(new_cluster, out);
                }
            }
            Err(Some(peer_last_tx)) => {
                // Defense-in-depth: a partially-recovered peer can
                // resurface with a shorter durable log than its
                // earlier acks claimed; lower the slot to the
                // peer's freshly-reported `last_tx_id`. The
                // `cluster_commit_index` watermark stays put
                // (`Quorum::regress` does not republish it), so a
                // committed entry can never become uncommitted.
                self.quorum.regress(peer_slot_index, peer_last_tx);
            }
            Err(None) => {}
        }
    }

    // ─── LocalCommitAdvanced (leader) ────────────────────────────────────

    fn on_local_commit_advanced(&mut self, tx_id: TxId, out: &mut Vec<Action>) {
        if tx_id > self.local_log_index {
            self.local_log_index = tx_id;
        }
        if matches!(self.state, NodeState::Leader(_))
            && let Some(new_cluster) = self.quorum.advance(self.self_slot, tx_id)
        {
            self.publish_cluster_commit(new_cluster, out);
        }
    }

    // ─── LocalWriteAdvanced (leader) ─────────────────────────────────────

    /// Driver acks that the leader's raft log is durable up to
    /// `tx_id`. Bounds the AE replication window only — does not
    /// touch `local_log_index`, the quorum self-slot, or
    /// `cluster_commit_index`. No-op outside the leader role: the
    /// driver may legitimately fire this around a role transition.
    fn on_local_write_advanced(&mut self, tx_id: TxId) {
        if let NodeState::Leader(l) = &mut self.state
            && tx_id > l.last_written
        {
            l.last_written = tx_id;
        }
    }

    fn on_log_append_complete(&mut self, tx_id: TxId, out: &mut Vec<Action>) {
        if tx_id > self.local_log_index {
            self.local_log_index = tx_id;
        }
        // If a follower had a deferred AppendEntries success reply
        // waiting on this durability ack, drain it now and apply
        // `leader_commit` (Raft §5.3 commit advance). Heartbeats and
        // rejections never park anything here.
        let drained = if let NodeState::Follower(f) = &mut self.state {
            f.take_ready_reply(tx_id)
        } else {
            None
        };
        if let Some(pending) = drained {
            let new_cluster = pending.leader_commit.min(self.local_log_index);
            if new_cluster > self.cluster_commit_index {
                self.publish_cluster_commit(new_cluster, out);
            }
            out.push(Action::SendAppendEntriesReply {
                to: pending.to,
                term: pending.term,
                success: true,
                last_tx_id: pending.last_tx_id,
            });
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

    /// Observe a higher term via inbound RPC (request OR reply).
    /// **Only the vote log is updated.** The term log records actual
    /// entry-term boundaries — observing a higher term via an RPC
    /// without receiving entries from that term does not establish
    /// a boundary, and writing one would pollute `term_at_tx` for
    /// existing entries: the §5.3 prev-log-term check would later
    /// match against the phantom record, so a leader's AE could
    /// graft new entries onto a stale prefix without truncation.
    /// (`current_term()` is `max(term-log, vote-log)`, so the read
    /// API still reflects the higher term.)
    fn observe_higher_term(&mut self, term: Term) {
        if let Err(e) = self.persistence.observe_vote_term(term) {
            warn!(
                "raft: node_id={} observe_vote_term({}) failed: {}",
                self.self_id, term, e
            );
        }
    }

    /// Term of the local log entry at `local_log_index`. `0` means
    /// the log is empty — required by §5.4.1's up-to-date check,
    /// which treats the empty log as "term 0".
    ///
    /// When `local_log_index > 0` there is, by construction, a
    /// term-log record covering it (every entry's term is recorded
    /// via `observe_term` / `commit_term`, and truncation drops
    /// the entry and its term record together). A `None` from
    /// `term_at_tx` here therefore means the persistence layer is
    /// corrupted: returning a default term would silently break
    /// §5.4.1's election safety, so `expect` surfaces it loudly.
    fn local_last_term(&self) -> Term {
        if self.local_log_index == 0 {
            return 0;
        }
        self.persistence
            .term_at_tx(self.local_log_index)
            .expect("term_at_tx must cover local_log_index when local_log_index > 0")
            .term
    }

    fn transition_to_follower(&mut self, now: Instant, leader_id: Option<NodeId>) {
        let mut state = match leader_id {
            Some(id) => NodeState::Follower(FollowerState::with_leader(id)),
            None => NodeState::Follower(FollowerState::new()),
        };
        std::mem::swap(&mut self.state, &mut state);
        self.election_timer.arm(now);
        // The §5.4.2 watermark is leader-only — clear it so a stale
        // value does not block this node, now a follower, from
        // accepting the new leader's `leader_commit`.
        self.current_term_first_tx = 0;
    }

    fn publish_cluster_commit(&mut self, new_value: TxId, out: &mut Vec<Action>) {
        // Raft §5.4.2 / Figure 8: a leader must not commit
        // prior-term entries by counting replicas. Block any
        // advance below this term's first-entry watermark — once
        // an entry from the current term has been committed by
        // replica count, the Log Matching Property carries prior
        // entries along.
        //
        // Single-node clusters skip the gate: there are no peers
        // to disagree, so no future leader can overwrite. The
        // gate would otherwise leave a single-node leader unable
        // to commit recovered prior-term entries until the next
        // client write.
        //
        // Followers also bypass the gate via `current_term_first_tx
        // == 0` (cleared in `transition_to_follower`); they trust
        // the leader's `leader_commit`, which the leader has
        // already gated on its end.
        if self.peers.len() > 1 && new_value < self.current_term_first_tx {
            return;
        }
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
        fn commit_term(&mut self, expected: Term, start_tx_id: TxId) -> std::io::Result<bool> {
            let cur = self.current_term();
            if cur >= expected {
                return Ok(false);
            }
            self.term_log.push(crate::TermRecord {
                term: expected,
                start_tx_id,
            });
            Ok(true)
        }
        fn observe_term(&mut self, term: Term, start_tx_id: TxId) -> std::io::Result<()> {
            let cur = self.current_term();
            if term == cur {
                return Ok(());
            }
            if term < cur {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "regression",
                ));
            }
            self.term_log.push(crate::TermRecord { term, start_tx_id });
            Ok(())
        }
        fn truncate_term_after(&mut self, tx_id: TxId) -> std::io::Result<()> {
            self.term_log.retain(|r| r.start_tx_id <= tx_id);
            Ok(())
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
        fn vote(&mut self, term: Term, candidate_id: NodeId) -> std::io::Result<bool> {
            if candidate_id == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "zero",
                ));
            }
            if term < self.vote_term {
                return Ok(false);
            }
            if term == self.vote_term {
                if self.voted_for == candidate_id {
                    return Ok(true);
                }
                if self.voted_for != 0 {
                    return Ok(false);
                }
            }
            self.vote_term = term;
            self.voted_for = candidate_id;
            Ok(true)
        }
        fn observe_vote_term(&mut self, term: Term) -> std::io::Result<()> {
            if term == self.vote_term {
                return Ok(());
            }
            if term < self.vote_term {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "regression",
                ));
            }
            self.vote_term = term;
            self.voted_for = 0;
            Ok(())
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
        let _ = node.step(t0, Event::Tick);
        let actions = node.step(t0 + Duration::from_secs(60), Event::Tick);
        assert_eq!(node.role(), Role::Candidate);
        // `current_term()` = max(term-log, vote-log). The term log
        // stays at 0 until election win, but the candidate's
        // self-vote bumps the vote log to 1, so the public read is 1.
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
    fn append_entries_first_batch_appends_and_replies_success() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let actions = node.step(
            Instant::now(),
            Event::AppendEntriesRequest {
                from: 2,
                term: 1,
                prev_log_tx_id: 0,
                prev_log_term: 0,
                entries: LogEntryRange::new(1, 2, 1),
                leader_commit: 0,
            },
        );
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), 1);
        let appended_range: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::AppendLog { range } => Some(*range),
                _ => None,
            })
            .collect();
        assert_eq!(appended_range, vec![LogEntryRange::new(1, 2, 1)]);
        // Per the durability-before-reply rule the success reply is
        // parked until the driver acks via `LogAppendComplete`. The
        // first step must NOT contain a success reply.
        let early_success = actions.iter().any(|a| {
            matches!(
                a,
                Action::SendAppendEntriesReply { success: true, .. }
            )
        });
        assert!(!early_success);

        // After the driver confirms durability, the parked success
        // reply fires.
        let after_ack = node.step(
            Instant::now(),
            Event::LogAppendComplete { tx_id: 2 },
        );
        let success = after_ack.iter().any(|a| {
            matches!(
                a,
                Action::SendAppendEntriesReply { success: true, .. }
            )
        });
        assert!(success);
    }

    #[test]
    fn append_entries_with_log_mismatch_truncates_and_rejects() {
        let mut node = fresh(1, vec![1, 2, 3]);
        // Pretend we accepted entries 1-5 at term 1.
        node.persistence.commit_term(1, 0).unwrap();
        node.local_log_index = 5;

        let actions = node.step(
            Instant::now(),
            Event::AppendEntriesRequest {
                from: 2,
                term: 2,
                prev_log_tx_id: 5,
                prev_log_term: 2,
                entries: LogEntryRange::empty(),
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

    #[test]
    fn into_persistence_recovers_durable_state() {
        let mut node = fresh(1, vec![1]);
        let t0 = Instant::now();
        let _ = node.step(t0, Event::Tick);
        let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
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

    // ── RaftConfig::validate (audit finding #5) ──────────────────────────

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
}
