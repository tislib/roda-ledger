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
//! - `local_write_index` / `local_commit_index` /
//!   `cluster_commit_index` — read API surface; the first two are
//!   advanced by the driver-side `advance(write, commit)` method,
//!   the third is updated in-place when a quorum advances or
//!   `leader_commit` propagates (drivers poll the getter — there
//!   is no dedicated action).
//! - `election_timer: ElectionTimer` — armed only when a non-leader
//!   role needs one.

use std::time::{Duration, Instant};

use spdlog::{debug, error, info, warn};

use crate::action::Action;
use crate::candidate::CandidateState;
use crate::event::Event;
use crate::follower::FollowerState;
use crate::leader::{InFlightAppend, LeaderState};
use crate::log_entry::LogEntryRange;
use crate::persistence::Persistence;
use crate::quorum::Quorum;
use crate::replication::{AppendEntriesRequest, AppendResult, Replication};
use crate::request_vote::{RequestVoteReply, RequestVoteRequest};
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

/// Result of [`RaftNode::validate_append_entries_request`]. The
/// driver inspects this to decide what wire response to send and what
/// I/O to perform on its WAL. The library has already applied every
/// state-machine effect that does not depend on driver-side I/O
/// (term observation, follower transition, election-timer reset,
/// term-log truncation, watermark clamping, heartbeat-path
/// `cluster_commit` advance). On the entries path, the deferred
/// `leader_commit` is consumed inside the next [`RaftNode::advance`]
/// — the driver does not pass it back explicitly.
///
/// See ADR-0017 §"Driver call pattern".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendEntriesDecision {
    /// Reply false. The driver reads `term`, `last_commit_id`, and
    /// `last_write_id` from `current_term()` / `commit_index()` /
    /// `write_index()` for the wire response.
    ///
    /// `truncate_after` is `Some(t)` only when §5.3 prev_log_term
    /// mismatched **and** the local log covered `prev_log_tx_id` —
    /// the driver must drop entry-log records with `tx_id > t`.
    /// Raft's term-log mirror is already truncated synchronously
    /// inside validate.
    Reject {
        reason: RejectReason,
        truncate_after: Option<TxId>,
    },
    /// Reply true. `append: None` is a heartbeat — no I/O, reply
    /// immediately. `append: Some(range)` means the driver must
    /// durably append the entries (payload bytes already arrived on
    /// the inbound RPC), await fsync, then call
    /// `advance(new_write, new_commit)` before replying. The deferred
    /// `leader_commit` is propagated into `cluster_commit_index`
    /// inside that `advance` call.
    Accept {
        append: Option<LogEntryRange>,
    },
    /// A `Persistence` write failed mid-validate. The node has
    /// frozen itself (`step` and `advance` are now no-ops). The
    /// driver MUST NOT touch the WAL, MUST NOT call `advance`, and
    /// MUST NOT send a success reply. Tear the node down.
    Fatal {
        reason: &'static str,
    },
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

    /// Highest tx_id durably written to this node's raft log.
    /// Advanced by the driver via `advance(write, commit)`. Bounds
    /// the AE replication window in `leader_send_to`, gates §5.4.1's
    /// up-to-date check, and feeds the §5.3 prev_log coverage check.
    /// Survives role transitions. Invariant:
    /// `local_write_index >= local_commit_index >= 0`.
    local_write_index: TxId,
    /// Highest tx_id durably committed to this node's ledger.
    /// Advanced by the driver via `advance(write, commit)`. Feeds
    /// the leader's quorum self-slot (entries committed locally
    /// count toward cluster_commit). Read by `commit_index()`.
    local_commit_index: TxId,
    cluster_commit_index: TxId,

    /// First `tx_id` of the leader's current term — set on
    /// `become_leader_after_win` to `local_write_index + 1`
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
    /// `local_write_index` and `local_commit_index` start at 0; the
    /// driver feeds `node.advance(write, commit)` for any entries
    /// already on disk before exposing the node to RPCs.
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
            local_write_index: 0,
            local_commit_index: 0,
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
        self.local_commit_index
    }

    /// Highest tx_id durably written to this node's raft log. See the
    /// field doc on `local_write_index`. After step 2 of the split
    /// refactor this becomes the §5.4.1 / §5.3 reference and the
    /// replication-window upper bound.
    pub fn write_index(&self) -> TxId {
        self.local_write_index
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

    // ─── advance() ───────────────────────────────────────────────────────

    /// Driver-side update of the local watermarks. The driver calls
    /// this whenever it observes a change in raft-log durability
    /// (`local_write_index`) or ledger-apply (`local_commit_index`).
    /// On the leader, advancing `local_commit_index` feeds the
    /// quorum self-slot and may advance `cluster_commit_index`
    /// in-place — there is no dedicated `Action` for that signal;
    /// the driver polls `cluster_commit_index()` after each call
    /// (ADR-0017 §"Driver call pattern").
    ///
    /// On a follower, `advance` also drains any deferred
    /// `leader_commit` recorded by the most recent
    /// `validate_append_entries_request` into `cluster_commit_index`
    /// (clamped to the freshly-updated `local_commit_index`,
    /// satisfying Raft §5.3's follower commit rule).
    ///
    /// Returns `()` and queues no actions. Convention: the driver
    /// follows `advance` with `step(Event::Tick)` to flush deferred
    /// outputs (heartbeats, `SetWakeup` re-arming). After a fatal
    /// failure the call is a no-op, mirroring `step`.
    ///
    /// Invariant: `local_commit_index <= local_write_index`. Debug
    /// builds panic on violation; release builds clamp the commit
    /// argument down to the write argument (same posture as the
    /// AE-reply guard).
    pub fn advance(&mut self, local_write_index: TxId, local_commit_index: TxId) {
        if self.failed {
            return;
        }
        debug_assert!(
            local_commit_index <= local_write_index,
            "advance: commit_index={} > write_index={}",
            local_commit_index,
            local_write_index
        );
        let new_commit = local_commit_index.min(local_write_index);

        if local_write_index > self.local_write_index {
            self.local_write_index = local_write_index;
        }
        if new_commit > self.local_commit_index {
            self.local_commit_index = new_commit;
            // Leader-only: feed the new local commit into the quorum
            // self-slot. The Figure-8 §5.4.2 gate is enforced inside
            // the inlined cluster_commit update — single-node clusters
            // bypass it (no peer can overwrite), and followers bypass
            // it via `current_term_first_tx == 0`.
            if matches!(self.state, NodeState::Leader(_))
                && let Some(adv) = self
                    .quorum
                    .advance(self.self_slot, self.local_commit_index)
                && !(self.peers.len() > 1 && adv < self.current_term_first_tx)
                && adv > self.cluster_commit_index
            {
                self.cluster_commit_index = adv;
            }
        }

        // Follower-side: drain the deferred `leader_commit` observed
        // by the most recent `validate_append_entries_request` whose
        // entries the driver has now durably persisted (this `advance`
        // is the durability ack). Clamped to `local_commit_index`
        // (Raft §5.3 follower commit rule). The §5.4.2 gate is a
        // no-op on followers (`current_term_first_tx == 0` after
        // `transition_to_follower`). Take-once mirrors the previous
        // parked-reply drain; if the clamp here is bounded by a stale
        // `local_commit_index`, the next heartbeat will refresh
        // `cluster_commit_index` once the ledger catches up.
        if let NodeState::Follower(f) = &mut self.state
            && let Some(lc) = f.pending_leader_commit.take()
        {
            let new_cluster = lc.min(self.local_commit_index);
            if !(self.peers.len() > 1 && new_cluster < self.current_term_first_tx)
                && new_cluster > self.cluster_commit_index
            {
                self.cluster_commit_index = new_cluster;
            }
        }
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
    // Defined here (rather than in `replication.rs`) so they can
    // touch private state directly without relaxing visibility.

    pub(crate) fn replication_peer_next_index(&self, peer_id: NodeId) -> TxId {
        match &self.state {
            NodeState::Leader(l) => l
                .peers
                .get(&peer_id)
                .map(|p| p.next_index)
                .unwrap_or(0),
            _ => 0,
        }
    }

    pub(crate) fn replication_peer_match_index(&self, peer_id: NodeId) -> TxId {
        match &self.state {
            NodeState::Leader(l) => l
                .peers
                .get(&peer_id)
                .map(|p| p.match_index)
                .unwrap_or(0),
            _ => 0,
        }
    }

    /// Build the next AE for `peer_id` and arm the in-flight slot.
    /// Returns `None` if not leader / peer unknown / failed.
    ///
    /// Same source-of-truth as `leader_send_to`: both routes share
    /// the §5.3 prev-log header construction, the same-term range
    /// expansion (capped by `max_entries_per_append`), and the
    /// heartbeat-vs-entries decision (`next_index > last_written`).
    pub(crate) fn replication_get_append_range(
        &mut self,
        peer_id: NodeId,
        now: Instant,
    ) -> Option<AppendEntriesRequest> {
        if self.failed {
            return None;
        }
        if !self.role().is_leader() {
            return None;
        }

        // Sweep expired in-flight first so a peer with a stale RPC
        // can re-send on the same call.
        if let NodeState::Leader(l) = &mut self.state
            && let Some(p) = l.peers.get_mut(&peer_id)
            && let Some(infl) = p.in_flight
            && now >= infl.expires_at
        {
            p.in_flight = None;
        }

        // Gate (matches the legacy `leader_drive` filter): skip
        // when an RPC is already in flight or the heartbeat
        // deadline hasn't elapsed yet. Lets the cluster (or
        // simulator) call `get_append_range` freely without
        // spamming the wire — a `None` here means "not due, sleep
        // until next_heartbeat".
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

        let current_term = self.current_term();
        let leader_commit = self.cluster_commit_index;
        let max_entries = self.cfg.max_entries_per_append;
        let last_written = self.local_write_index;
        let next_index = progress.next_index;

        let prev_log_tx_id = next_index.saturating_sub(1);
        let prev_log_term = self
            .persistence
            .term_at_tx(prev_log_tx_id)
            .map(|r| r.term)
            .unwrap_or(0);

        let entries = if next_index > last_written {
            // Heartbeat: nothing to ship.
            LogEntryRange::empty()
        } else {
            // Same-term contiguous range. The expect()s match the
            // `leader_send_to` invariant: every in-log tx_id has a
            // covering term-log record (truncation drops both
            // together via `truncate_term_after`). A `None` here
            // signals corrupted persistence.
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

        let last_tx_in_batch = entries.last_tx_id().unwrap_or(prev_log_tx_id);

        // Arm the in-flight window and push the heartbeat deadline
        // out — same accounting as `leader_send_to`.
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
            term: current_term,
            prev_log_tx_id,
            prev_log_term,
            entries,
            leader_commit,
        })
    }

    /// Feed the cluster-driver's RPC outcome back into the state
    /// machine. `Success` / `Reject(LogMismatch)` advance / regress
    /// the peer's `next_index` and feed the quorum;
    /// `Reject(TermBehind)` triggers leader step-down via
    /// `transition_to_follower`; `Timeout` is mapped to
    /// `RejectReason::RpcTimeout` and clears `in_flight` while
    /// leaving indexes alone.
    pub(crate) fn replication_append_result(
        &mut self,
        peer_id: NodeId,
        now: Instant,
        result: AppendResult,
    ) {
        match result {
            AppendResult::Success {
                term,
                last_write_id,
                last_commit_id,
            } => {
                self.on_append_entries_reply(
                    now,
                    peer_id,
                    term,
                    true,
                    last_commit_id,
                    last_write_id,
                    None,
                );
            }
            AppendResult::Reject {
                term,
                reason,
                last_write_id,
                last_commit_id,
            } => {
                self.on_append_entries_reply(
                    now,
                    peer_id,
                    term,
                    false,
                    last_commit_id,
                    last_write_id,
                    Some(reason),
                );
            }
            AppendResult::Timeout => {
                // Synthesise a peer-less reply at the leader's own
                // term: the reply handler clears `in_flight` and
                // leaves `next_index` / `match_index` unchanged on
                // `RejectReason::RpcTimeout`. The watermark args
                // are unused on this path but must satisfy
                // `last_write_id >= last_commit_id` per the §"AE
                // reply" debug assert.
                let term = self.current_term();
                self.on_append_entries_reply(
                    now,
                    peer_id,
                    term,
                    false,
                    0,
                    0,
                    Some(RejectReason::RpcTimeout),
                );
            }
        }
    }

    // ─── request_vote (direct-method voter side) ─────────────────────────

    /// Voter-side handler for an inbound `RequestVote`. Runs the
    /// §5.1 / §5.4.1 protocol checks and returns a
    /// [`RequestVoteReply`] synchronously — there is no action-stream
    /// equivalent; this is the only voter-side entry-point.
    ///
    /// Internal state mutations performed before returning:
    ///
    /// - On `term > current_term`: `observe_higher_term` (vote-log
    ///   bump) and `transition_to_follower` (state reset, including
    ///   `pending_leader_commit` and `current_term_first_tx`).
    /// - On grant: durable `Persistence::vote(term, from)` and
    ///   `election_timer.reset(now)` — don't start a competing
    ///   election while we've committed to someone else's.
    ///
    /// Frozen-after-fatal: if the node has already emitted
    /// `Action::FatalError`, every call returns
    /// `RequestVoteReply { term: current_term(), granted: false }`
    /// without touching persistence — same posture as `step()`.
    pub fn request_vote(
        &mut self,
        now: Instant,
        req: RequestVoteRequest,
    ) -> RequestVoteReply {
        if self.failed {
            return RequestVoteReply {
                term: self.current_term(),
                granted: false,
            };
        }

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

        // Higher term observed — persist the vote-log bump first,
        // then step down. The helper `observe_higher_term` uses
        // `local_write_index + 1` as the term-log boundary so we
        // never shadow our own entries (using the candidate's
        // `last_tx_id` would record the new term at a tx that
        // already has a term assigned, breaking §5.3 prev_log_term
        // lookups for our existing entries).
        if term > current_term {
            self.observe_higher_term(term);
            self.transition_to_follower(now, None);
        }

        // §5.4.1 up-to-date check. The voter compares its own
        // durable log extent (`local_write_index`) — not its commit
        // watermark — to the candidate's claim. Using commit would
        // let a candidate with strictly older on-disk state win
        // votes from a node whose ledger had merely fallen behind
        // its own raft-log.
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
            self.election_timer.reset(now);
        }

        RequestVoteReply {
            term: self.current_term(),
            granted,
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
            error!("raft_loop: step() failed: node is in failed state");
            return Vec::new();
        }
        let mut out = Vec::new();
        match event {
            Event::Tick => self.on_tick(now, &mut out),
            Event::RequestVoteReply {
                from,
                term,
                granted,
            } => self.on_request_vote_reply(now, from, term, granted, &mut out),
        }
        // Skip wakeup emission if a fatal happened in this step —
        // there is no future work for the timer to wake up to.
        if !self.failed {
            self.emit_wakeup_if_changed(&mut out);
        }
        out
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

        let last_tx_id = self.local_write_index;
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
        // The new term's first entry will live at `local_write_index +
        // 1`; existing entries belong to whatever earlier term they
        // were committed under, so the new boundary must not be
        // recorded at or before `local_write_index` or it would
        // shadow them in `term_at_tx` lookups. We anchor on the
        // *write* extent (not the commit extent) because the new
        // leader's first entry sits after the last on-disk one,
        // regardless of which entries the local ledger has applied.
        //
        // No catch-up record. The vote log can validly race ahead of
        // the term log (observed-higher-term-via-RPC), and Raft does
        // not require term-log records to be contiguous. The
        // `Persistence::commit_term` contract permits any
        // `current < expected` jump.
        let start_tx_id = self.local_write_index + 1;

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
        // first cluster_commit advance can fire — the §5.4.2 / Figure
        // 8 gate is inlined at every advance site.
        self.current_term_first_tx = start_tx_id;
        // Quorum self-slot is seeded from the new leader's *commit*
        // watermark, not the write watermark. The cluster_commit
        // quorum advances on durably-committed-locally entries; a
        // written-but-uncommitted entry must not contribute its
        // replica count to cluster_commit (Figure-8 §5.4.2 gate
        // below blocks any advance below `current_term_first_tx`).
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
        // `last_local_tx` is the write extent — replication's initial
        // `next_index = last_local_tx + 1` must include any entries
        // that are written-but-not-yet-locally-committed so the leader
        // can ship them to followers without first re-fsyncing. The
        // replication-window upper bound itself is read directly from
        // `self.local_write_index` in `leader_send_to`.
        let leader = LeaderState::new(
            &other_peers,
            self.local_write_index,
            now,
            self.cfg.heartbeat_interval,
            self.cfg.rpc_timeout,
        );

        self.state = NodeState::Leader(leader);
        out.push(Action::BecomeRole {
            role: Role::Leader,
            term: new_term,
        });

        // The cluster's per-peer replication loop (driven via
        // `RaftNode::replication`) picks up from here. Each peer's
        // `next_heartbeat` is initialised to `now` in
        // `LeaderState::new`, so the very first
        // `PeerReplication::get_append_range` after this `BecomeRole`
        // returns Some — that's the immediate first-round AE that
        // suppresses peer election timers.
    }

    // ─── AppendEntries (follower path) ───────────────────────────────────

    /// Direct-method follower-side handler for an inbound
    /// `AppendEntries`. Runs the §5.1 / §5.3 / §5.4 protocol checks
    /// and returns an [`AppendEntriesDecision`] that tells the
    /// driver what to do with its WAL and how to construct the wire
    /// reply.
    ///
    /// Internal state mutations performed before returning:
    ///
    /// - Term observation (`observe_higher_term`) and follower
    ///   transition (election timer reset, `current_term_first_tx`
    ///   cleared, `pending_leader_commit` cleared) on `term >=
    ///   current_term`.
    /// - On §5.3 prev_log_term mismatch, a synchronous
    ///   `truncate_term_after` and a clamp of
    ///   `local_write_index` / `local_commit_index` /
    ///   `cluster_commit_index` to the truncation point. The driver
    ///   must mirror the truncation in its entry log.
    /// - On a non-empty entries range, a `observe_term` if the term
    ///   is new, plus storing `leader_commit` in
    ///   `FollowerState::pending_leader_commit` for the next
    ///   `advance` to consume (Raft §5.3 follower commit rule).
    /// - On a heartbeat (empty entries), the `cluster_commit_index =
    ///   min(leader_commit, local_commit_index)` advance is applied
    ///   inline — the driver has nothing further to do.
    ///
    /// See `AppendEntriesDecision` for the per-variant contract.
    /// See ADR-0017 §"Driver call pattern" for where this fits in
    /// the overall flow.
    #[allow(clippy::too_many_arguments)]
    pub fn validate_append_entries_request(
        &mut self,
        now: Instant,
        from: NodeId,
        term: Term,
        prev_log_tx_id: TxId,
        prev_log_term: Term,
        entries: LogEntryRange,
        leader_commit: TxId,
    ) -> AppendEntriesDecision {
        if self.failed {
            return AppendEntriesDecision::Fatal {
                reason: "node already failed",
            };
        }

        let current_term = self.current_term();

        if term < current_term {
            return AppendEntriesDecision::Reject {
                reason: RejectReason::TermBehind,
                truncate_after: None,
            };
        }

        if term > current_term {
            self.observe_higher_term(term);
        }
        self.transition_to_follower(now, Some(from));
        self.election_timer.reset(now);

        // §5.3 prev_log_term match. `local_write_index` is the §5.3
        // reference — entries up to that watermark exist on disk and
        // can be checked against the leader's prev_log_term.
        if prev_log_tx_id != 0 {
            let our_term_at_prev = self
                .persistence
                .term_at_tx(prev_log_tx_id)
                .map(|r| r.term)
                .unwrap_or(0);
            let log_covers_prev = prev_log_tx_id <= self.local_write_index;
            if !log_covers_prev || our_term_at_prev != prev_log_term {
                if log_covers_prev {
                    let after = prev_log_tx_id.saturating_sub(1);
                    // Leader Completeness §5.4: a leader cannot ask a
                    // follower to truncate below the cluster commit
                    // index. If this fires, a Raft invariant has
                    // already been violated upstream — surface it
                    // loudly in debug builds. Release-build clamp
                    // below is belt-and-suspenders. The local-commit
                    // guard catches the same invariant from the
                    // follower's side.
                    debug_assert!(
                        after >= self.cluster_commit_index,
                        "truncation below cluster_commit_index: after={} cluster={}",
                        after,
                        self.cluster_commit_index
                    );
                    debug_assert!(
                        after >= self.local_commit_index,
                        "truncation below local_commit_index: after={} local_commit={}",
                        after,
                        self.local_commit_index
                    );
                    if let Err(e) = self.persistence.truncate_term_after(after) {
                        warn!(
                            "raft: node_id={} truncate_term_after({}) failed: {}",
                            self.self_id, after, e
                        );
                        self.failed = true;
                        return AppendEntriesDecision::Fatal {
                            reason: "truncate_term_after failed",
                        };
                    }
                    self.local_write_index = after;
                    if self.local_commit_index > after {
                        self.local_commit_index = after;
                    }
                    if self.cluster_commit_index > after {
                        self.cluster_commit_index = after;
                    }
                    return AppendEntriesDecision::Reject {
                        reason: RejectReason::LogMismatch,
                        truncate_after: Some(after),
                    };
                }
                return AppendEntriesDecision::Reject {
                    reason: RejectReason::LogMismatch,
                    truncate_after: None,
                };
            }
        }

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
                self.failed = true;
                return AppendEntriesDecision::Fatal {
                    reason: "observe_term failed",
                };
            }
            // Defer `leader_commit` until the driver durably persists
            // the entries and calls `advance(write, …)`. The §5.3
            // follower commit rule (`min(leader_commit,
            // last_new_entry_index)`) is enforced there, with
            // `last_new_entry_index <= local_commit_index` once the
            // ledger has applied them.
            if let NodeState::Follower(f) = &mut self.state {
                f.pending_leader_commit = Some(leader_commit);
            }
            return AppendEntriesDecision::Accept {
                append: Some(entries),
            };
        }

        // Heartbeat path: no entries to durably persist. `leader_commit`
        // is clamped to the follower's `local_commit_index`
        // (ADR-0017 §"Two Commit Indexes": cluster_commit on a
        // follower is bounded by what the local ledger has applied)
        // and applied in-place. The §5.4.2 gate is a no-op on
        // followers (`current_term_first_tx == 0` after
        // `transition_to_follower`).
        let new_cluster = leader_commit.min(self.local_commit_index);
        if !(self.peers.len() > 1 && new_cluster < self.current_term_first_tx)
            && new_cluster > self.cluster_commit_index
        {
            self.cluster_commit_index = new_cluster;
        }
        AppendEntriesDecision::Accept { append: None }
    }

    // ─── AppendEntries (leader path) ─────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries_reply(
        &mut self,
        now: Instant,
        from: NodeId,
        term: Term,
        success: bool,
        last_commit_id: TxId,
        last_write_id: TxId,
        reject_reason: Option<RejectReason>,
    ) {
        let current_term = self.current_term();
        if term > current_term {
            self.observe_higher_term(term);
            self.transition_to_follower(now, None);
            return;
        }

        // The two reply watermarks drive different leader decisions
        // (ADR-0017 §"AE reply: write vs commit watermark"):
        //   `last_commit_id` — peer's durable end → match_index +
        //                      quorum advance/regress.
        //   `last_write_id`  — peer's accepted end → next_index
        //                      (replication window).
        // Invariant: `last_write_id >= last_commit_id`. A peer that
        // violates it is treated as misbehaving: panic in debug,
        // clamp in release. Same posture as the §5.4 truncation guard
        // earlier in this file.
        debug_assert!(
            last_write_id >= last_commit_id,
            "AE reply: last_commit_id={} > last_write_id={} from peer={}",
            last_commit_id,
            last_write_id,
            from
        );
        let last_commit_id = last_commit_id.min(last_write_id);

        // `Ok(adv)` — peer acked, advance the quorum slot to `adv`
        // (the durable watermark).
        // `Err(Some(peer_last))` — `LogMismatch`; regress the
        // quorum slot defensively to the peer's reported durable end.
        // `Err(None)` — non-mismatch reject (term behind, rpc
        // timeout); leave the quorum alone.
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
                    progress.next_index = last_write_id + 1;
                    Ok(last_commit_id)
                } else {
                    match reject_reason {
                        Some(RejectReason::TermBehind) | Some(RejectReason::RpcTimeout) => Err(None),
                        // LogMismatch (explicit) and the unannotated
                        // case both mean §5.3: walk `next_index` back
                        // one entry. Clamp at 1 — tx_id 0 is the "no
                        // entry" sentinel.
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
                // Defense-in-depth: a partially-recovered peer can
                // resurface with a shorter durable log than its
                // earlier acks claimed; lower the slot to the
                // peer's freshly-reported durable end. The
                // `cluster_commit_index` watermark stays put
                // (`Quorum::regress` does not republish it), so a
                // committed entry can never become uncommitted.
                self.quorum.regress(peer_slot_index, peer_last_commit);
            }
            Err(None) => {}
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

    /// Term of the local log entry at `local_write_index`. `0` means
    /// the log is empty — required by §5.4.1's up-to-date check,
    /// which treats the empty log as "term 0". The §5.4.1 candidate-
    /// vs-voter comparison is against the *write* extent, not the
    /// commit extent: a voter with durably-written-but-uncommitted
    /// entries still votes against a candidate whose log is shorter.
    ///
    /// When `local_write_index > 0` there is, by construction, a
    /// term-log record covering it (every entry's term is recorded
    /// via `observe_term` / `commit_term`, and truncation drops
    /// the entry and its term record together). A `None` from
    /// `term_at_tx` here therefore means the persistence layer is
    /// corrupted: returning a default term would silently break
    /// §5.4.1's election safety, so `expect` surfaces it loudly.
    fn local_last_term(&self) -> Term {
        if self.local_write_index == 0 {
            return 0;
        }
        self.persistence
            .term_at_tx(self.local_write_index)
            .expect("term_at_tx must cover local_write_index when local_write_index > 0")
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
        let reply = node.request_vote(
            Instant::now(),
            RequestVoteRequest {
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
        let _ = node.step(t0, Event::Tick);
        let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
        assert!(node.role().is_leader());
        node.advance(7, 7);
        // `advance` is silent; observe via getters per ADR-0017
        // §"Driver call pattern".
        assert_eq!(node.commit_index(), 7);
        assert_eq!(node.write_index(), 7);
        assert_eq!(node.cluster_commit_index(), 7);
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

    // ── validate_append_entries_request (direct-method follower path) ────

    #[test]
    fn validate_returns_reject_on_stale_term() {
        let mut node = fresh(1, vec![1, 2, 3]);
        // Bump the node into term 5 by routing an inbound vote at that
        // term (cheapest way to advance current_term in unit tests).
        let _ = node.request_vote(
            Instant::now(),
            RequestVoteRequest {
                from: 2,
                term: 5,
                last_tx_id: 0,
                last_term: 0,
            },
        );
        assert_eq!(node.current_term(), 5);

        let decision = node.validate_append_entries_request(
            Instant::now(),
            2,
            3, // stale term
            0,
            0,
            LogEntryRange::empty(),
            0,
        );
        assert_eq!(
            decision,
            AppendEntriesDecision::Reject {
                reason: RejectReason::TermBehind,
                truncate_after: None
            }
        );
    }

    #[test]
    fn validate_heartbeat_advances_cluster_commit_immediately() {
        let mut node = fresh(1, vec![1, 2, 3]);
        // Pretend the local ledger has applied up to tx 4 already.
        node.local_write_index = 4;
        node.local_commit_index = 4;

        let decision = node.validate_append_entries_request(
            Instant::now(),
            2,
            1,
            4,
            0, // prev_log_term: 0 because TestPersistence has no record at tx 4
            LogEntryRange::empty(),
            10, // leader_commit
        );
        // §5.3 mismatch path is bypassed here because prev_log_tx_id
        // covers an empty term log → our_term_at_prev = 0 == prev_log_term.
        assert_eq!(decision, AppendEntriesDecision::Accept { append: None });
        // Cluster commit clamps to local_commit_index (4), not the
        // leader-supplied 10.
        assert_eq!(node.cluster_commit_index(), 4);
        assert_eq!(node.role(), Role::Follower);
    }

    #[test]
    fn validate_returns_reject_on_log_mismatch_with_truncate_after() {
        let mut node = fresh(1, vec![1, 2, 3]);
        // Pretend we accepted entries 1..=5 at term 1 (durably written
        // but not yet locally committed; truncation invariant forbids
        // truncating below local_commit_index).
        node.persistence.commit_term(1, 0).unwrap();
        node.local_write_index = 5;

        let decision = node.validate_append_entries_request(
            Instant::now(),
            2,
            2,
            5,
            2, // claimed prev_log_term = 2; our term-log at tx 5 says 1
            LogEntryRange::empty(),
            0,
        );
        assert_eq!(
            decision,
            AppendEntriesDecision::Reject {
                reason: RejectReason::LogMismatch,
                truncate_after: Some(4),
            }
        );
    }

    #[test]
    fn validate_clamps_watermarks_synchronously_on_log_mismatch() {
        let mut node = fresh(1, vec![1, 2, 3]);
        // Set up: persistence has term 1 covering all entries; we
        // pretend the entry log is at tx 5 with cluster_commit at 2.
        node.persistence.commit_term(1, 0).unwrap();
        node.local_write_index = 5;
        node.local_commit_index = 2;
        node.cluster_commit_index = 2;

        let decision = node.validate_append_entries_request(
            Instant::now(),
            2,
            2,
            5,
            2, // mismatched prev_log_term
            LogEntryRange::empty(),
            0,
        );
        assert!(matches!(
            decision,
            AppendEntriesDecision::Reject {
                truncate_after: Some(4),
                ..
            }
        ));
        // All three watermarks clamp to the truncation point or stay
        // below it: write clamps to 4, commit/cluster_commit stay at 2
        // because they were already <= 4.
        assert_eq!(node.write_index(), 4);
        assert_eq!(node.commit_index(), 2);
        assert_eq!(node.cluster_commit_index(), 2);
    }

    #[test]
    fn validate_returns_accept_with_append_for_entries_path() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let decision = node.validate_append_entries_request(
            Instant::now(),
            2,
            1,
            0,
            0,
            LogEntryRange::new(1, 3, 1),
            0,
        );
        assert_eq!(
            decision,
            AppendEntriesDecision::Accept {
                append: Some(LogEntryRange::new(1, 3, 1)),
            }
        );
        // Term observed; node is now a follower at term 1.
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), 1);
    }

    #[test]
    fn advance_propagates_pending_leader_commit_after_entries() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let _ = node.validate_append_entries_request(
            Instant::now(),
            2,
            1,
            0,
            0,
            LogEntryRange::new(1, 3, 1),
            3, // leader_commit covering all three entries
        );
        // Cluster fsyncs entries and the ledger applies them.
        node.advance(3, 3);
        // cluster_commit advances to min(leader_commit=3, local_commit=3) = 3.
        assert_eq!(node.cluster_commit_index(), 3);
    }

    #[test]
    fn advance_clamps_pending_leader_commit_to_local_commit() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let _ = node.validate_append_entries_request(
            Instant::now(),
            2,
            1,
            0,
            0,
            LogEntryRange::new(1, 3, 1),
            10, // leader_commit ahead of what the follower has
        );
        // Cluster fsync covers the 3 entries but ledger has only
        // applied 2 of them.
        node.advance(3, 2);
        // cluster_commit clamps to local_commit (2), not leader_commit (10).
        assert_eq!(node.cluster_commit_index(), 2);
    }

    #[test]
    fn transition_to_follower_clears_pending_leader_commit() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let _ = node.validate_append_entries_request(
            Instant::now(),
            2,
            1,
            0,
            0,
            LogEntryRange::new(1, 3, 1),
            3,
        );
        // Election timeout drives transition to candidate (which
        // discards the FollowerState entirely, including
        // pending_leader_commit).
        let _ = node.step(Instant::now() + Duration::from_secs(60), Event::Tick);
        assert_eq!(node.role(), Role::Candidate);

        // Now drive the candidate back into a follower term — the new
        // FollowerState starts with no pending_leader_commit.
        node.advance(3, 3);
        // cluster_commit must NOT pick up the stale leader_commit=3
        // because pending_leader_commit was wiped on the transition.
        assert_eq!(node.cluster_commit_index(), 0);
    }

    #[test]
    fn validate_then_validate_overwrites_pending_leader_commit() {
        let mut node = fresh(1, vec![1, 2, 3]);
        let now = Instant::now();
        // First AE with leader_commit=2.
        let _ = node.validate_append_entries_request(
            now,
            2,
            1,
            0,
            0,
            LogEntryRange::new(1, 3, 1),
            2,
        );
        // Second AE before the cluster has called advance — same prev=0,
        // extended range, leader_commit bumped to 5.
        let _ = node.validate_append_entries_request(
            now,
            2,
            1,
            0,
            0,
            LogEntryRange::new(1, 5, 1),
            5,
        );
        // Cluster fsyncs durability up to 5 and the ledger applies up
        // to 5. Drain should use the second AE's leader_commit.
        node.advance(5, 5);
        assert_eq!(node.cluster_commit_index(), 5);
    }

    // ── Replication / PeerReplication (cluster-pull leader path) ──────────

    /// Promote a 3-node `RaftNode` to leader by running the
    /// election manually: a Tick lazy-arms, a second Tick fires
    /// the timeout, and a `RequestVoteReply { granted: true }`
    /// from one peer is enough to make a 3-node majority
    /// (self-vote + that one).
    ///
    /// Returns `(node, leader_now)` so callers can issue
    /// subsequent calls with timestamps `>= leader_now` —
    /// `LeaderState::new` initialises every peer's `next_heartbeat`
    /// to that instant, so the in-place gate in
    /// `replication_get_append_range` rejects earlier `now` values.
    fn fresh_leader_3node(
        self_id: NodeId,
        peers: Vec<NodeId>,
    ) -> (RaftNode<TestPersistence>, Instant) {
        let mut node = fresh(self_id, peers.clone());
        let t0 = Instant::now();
        let leader_now = t0 + Duration::from_secs(60);
        let _ = node.step(t0, Event::Tick);
        let _ = node.step(leader_now, Event::Tick);
        if node.role().is_leader() {
            return (node, leader_now);
        }
        let term = node.current_term();
        for peer in peers.iter().filter(|&&p| p != self_id) {
            let _ = node.step(
                leader_now,
                Event::RequestVoteReply {
                    from: *peer,
                    term,
                    granted: true,
                },
            );
            if node.role().is_leader() {
                return (node, leader_now);
            }
        }
        panic!("fresh_leader_3node: did not become leader (role={:?})", node.role());
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
        // No entries durably written → heartbeat (empty range).
        assert!(req.entries.is_empty());
        assert_eq!(req.to, 2);
        assert_eq!(req.term, node.current_term());
    }

    #[test]
    fn replication_get_append_range_ships_new_entries_after_advance() {
        let (mut node, leader_now) = fresh_leader_3node(1, vec![1, 2, 3]);
        // Leader-side ledger writes entries 1..=3 at the current term
        // (set by the election win as `current_term_first_tx`'s
        // start_tx_id) and acks durability via `advance`.
        node.advance(3, 3);

        let req = node
            .replication()
            .peer(2)
            .unwrap()
            .get_append_range(leader_now)
            .expect("leader with durable entries should produce an AE");
        // next_index started at local_write_index + 1 = 1 (peer's
        // initial estimate), and the leader's local_write is now 3,
        // so the range is [1..=3].
        assert_eq!(req.entries.start_tx_id, 1);
        assert_eq!(req.entries.count, 3);
        assert_eq!(req.prev_log_tx_id, 0);
    }

    #[test]
    fn replication_append_result_success_advances_match_and_next_index() {
        let (mut node, leader_now) = fresh_leader_3node(1, vec![1, 2, 3]);
        node.advance(3, 3);
        // Pull the request to arm in_flight.
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
                last_write_id: 3,
                last_commit_id: 3,
            },
        );

        let pr = node.replication().peer(2).unwrap();
        assert_eq!(pr.match_index(), 3);
        // next_index = last_write_id + 1.
        assert_eq!(pr.next_index(), 4);
    }

    #[test]
    fn replication_append_result_log_mismatch_walks_back_next_index() {
        let (mut node, leader_now) = fresh_leader_3node(1, vec![1, 2, 3]);
        node.advance(5, 5);
        // After election, next_index = local_write_index + 1 *at
        // election time*. We bumped local_write to 5 via `advance`
        // *after* the election, so peer next_index is still 1.
        // Set it explicitly to model a peer that has already
        // accepted a few entries.
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
                last_write_id: 0,
                last_commit_id: 0,
            },
        );
        // §5.3: walk back next_index by 1 (clamped at 1).
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
                last_write_id: 0,
                last_commit_id: 0,
            },
        );
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), leader_term + 5);
    }

    #[test]
    fn replication_append_result_timeout_clears_in_flight_only() {
        let (mut node, leader_now) = fresh_leader_3node(1, vec![1, 2, 3]);
        node.advance(3, 3);
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

        // Verify in_flight was cleared by pulling a fresh request
        // *after* the heartbeat deadline elapses. The gate respects
        // `next_heartbeat`; in-flight being None is necessary but not
        // sufficient.
        let later = leader_now + Duration::from_millis(100);
        let req2 = node
            .replication()
            .peer(2)
            .unwrap()
            .get_append_range(later);
        assert!(
            req2.is_some(),
            "timeout must clear in_flight so a fresh request can fire after next_heartbeat"
        );
    }
}
