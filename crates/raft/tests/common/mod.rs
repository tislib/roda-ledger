//! Deterministic in-process simulator for `RaftNode`.
//!
//! ADR-0017 §"Test Strategy" calls for a fake clock + fake message
//! bus driving N nodes through randomized event schedules with fault
//! injection. This file provides the harness; `tests/simulator.rs`
//! exercises end-to-end scenarios and `tests/safety_properties.rs`
//! drives proptest schedules.
//!
//! Concept of operation:
//! - The harness owns N `RaftNode<MemPersistence>` instances, each
//!   with its own in-memory persistence. No tempdirs.
//! - A virtual clock starts at `Instant::now()` and only advances
//!   when the harness explicitly advances it.
//! - A min-heap of `Scheduled` items combines (a) inbound RPC events
//!   queued at a future delivery time and (b) per-node wakeup
//!   deadlines requested via `Action::SetWakeup`.
//! - `run_until` repeatedly: pops the soonest `Scheduled`, sets the
//!   clock, dispatches the event to the target node via
//!   `step(now, event)`, and processes returned `Action`s back into
//!   `Scheduled`s.
//! - Fault injection: configurable network delay, message drop,
//!   crash via `into_persistence` (drops the volatile RaftNode while
//!   retaining the durable persistence), partition sets.

// Each integration test compiles `common/mod.rs` into its own test
// binary, so methods unused by *that* binary look dead even when
// other binaries call them. The harness is intentionally a complete
// API surface — silence the false positives.
#![allow(dead_code)]

pub mod mem_persistence;

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::time::{Duration, Instant};

use raft::{
    Action, AppendEntriesDecision, AppendResult, ElectionTimerConfig, Event, LogEntryRange, NodeId,
    RaftConfig, RaftNode, RejectReason, Role, Term, TxId,
};

/// Per-target delivery in the simulator queue. AE arrivals call the
/// `validate_append_entries_request` direct method (with the cluster
/// driver's responsibilities — WAL apply, `advance`, reply
/// synthesis — performed by `Sim::deliver_append_entries`); AE replies
/// route through the `Replication::append_result` direct method on the
/// leader; every other input runs through `node.step(now, event)`.
#[derive(Clone, Debug)]
enum Delivery {
    Step(Event),
    AppendEntries {
        from: NodeId,
        term: Term,
        prev_log_tx_id: TxId,
        prev_log_term: Term,
        entries: LogEntryRange,
        leader_commit: TxId,
    },
    /// Reply from `from` (the follower that processed the AE) back to
    /// the leader (this scheduled item's `target`). The leader feeds
    /// the result through
    /// `node.replication().peer(from).unwrap().append_result(now, result)`.
    AppendEntriesReply {
        from: NodeId,
        result: AppendResult,
    },
}

use mem_persistence::MemPersistence;

/// Private harness mirror of one log entry. The library only ever
/// talks about ranges; the harness expands ranges into per-entry
/// records so the §5.3 / Log Matching property checks (which
/// compare individual `(tx_id, term)` pairs across nodes) have
/// something concrete to look at.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MirrorEntry {
    pub tx_id: TxId,
    pub term: Term,
}

impl MirrorEntry {
    fn new(tx_id: TxId, term: Term) -> Self {
        Self { tx_id, term }
    }
}

#[derive(Clone, Debug)]
struct Scheduled {
    at: Instant,
    target: NodeId,
    delivery: Delivery,
    /// Tie-breaker: if two events sit at exactly the same `at`, lower
    /// seq fires first. The harness assigns this monotonically so
    /// schedules stay deterministic across runs with the same seed.
    seq: u64,
}

impl Eq for Scheduled {}
impl PartialEq for Scheduled {
    fn eq(&self, o: &Self) -> bool {
        self.at == o.at && self.seq == o.seq && self.target == o.target
    }
}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Scheduled {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.at
            .cmp(&o.at)
            .then(self.seq.cmp(&o.seq))
            .then(self.target.cmp(&o.target))
    }
}

/// Per-node simulator state.
///
/// `node` is `Some` while the node is running and `None` while
/// crashed; `saved_persistence` is the inverse — it holds the durable
/// state across a crash so a subsequent `restart` can rebuild a
/// `RaftNode` from it. This models a real crash dropping all
/// volatile state while leaving the on-disk term/vote logs intact.
struct NodeBox {
    node: Option<RaftNode<MemPersistence>>,
    saved_persistence: Option<MemPersistence>,
    /// Append-only mirror of the local entry log. `tx_id` indexes
    /// `entries[tx_id - 1]` (1-based). Truncation drops the suffix.
    /// Survives crash/restart (models on-disk WAL).
    entries: Vec<MirrorEntry>,
    /// Cluster commit watermark observed by polling
    /// `node.cluster_commit_index()` after each step (ADR-0017
    /// §"Driver call pattern" — there is no dedicated action).
    cluster_commit: TxId,
    /// (term, tx_id) pairs the node has been told to apply; used
    /// for state-machine safety checks.
    applied: Vec<MirrorEntry>,
    /// Most recent `SetWakeup` requested by this node.
    pending_wakeup: Option<Instant>,
    /// Stable seed used to recreate the node on restart.
    seed: u64,
}

impl NodeBox {
    fn is_crashed(&self) -> bool {
        self.node.is_none()
    }
}

pub struct Sim {
    base: Instant,
    clock: Instant,
    /// Keyed by `NodeId`, kept in a `BTreeMap` so iteration order is
    /// deterministic across runs. The default `HashMap`'s randomised
    /// hash makes bootstrap order vary, which under chaos
    /// (drop/duplicate) sometimes leaves the cluster mid-convergence
    /// when a test asserts safety properties.
    nodes: BTreeMap<NodeId, NodeBox>,
    peer_list: Vec<NodeId>,
    cfg: RaftConfig,
    queue: BinaryHeap<Reverse<Scheduled>>,
    seq: u64,
    network_delay: Duration,
    /// Pairs `(a, b)` such that any message a→b or b→a is dropped.
    /// Modelled as an undirected partition.
    partitioned: HashSet<(NodeId, NodeId)>,
    /// Per-message drop probability in `[0.0, 1.0]`. Each outbound
    /// message tosses a coin against this.
    drop_probability: f64,
    /// Per-message duplication probability — when set, each
    /// outbound message is delivered twice with the same delay.
    duplicate_probability: f64,
    /// Seeded RNG used for the probabilistic faults above. Stays
    /// deterministic across runs with the same `seed_base`.
    fault_rng: rand::rngs::StdRng,
    /// Leaders observed per term — used to verify Election Safety
    /// (§5.4 #1: at most one leader per term).
    leaders_per_term: HashMap<u64, HashSet<NodeId>>,
    /// Recorded so callers can inspect after the run.
    transitions: Vec<(NodeId, Role, u64)>,
    /// `Action::FatalError` emissions captured during the run. Test
    /// assertions can check this to spot a Raft node that froze
    /// itself due to a persistence I/O failure or invariant
    /// violation. Most simulator scenarios should expect this to be
    /// empty.
    fatal_errors: Vec<(NodeId, &'static str)>,
}

impl Sim {
    pub fn new(node_ids: &[NodeId], cfg: RaftConfig, seed_base: u64) -> Self {
        use rand::SeedableRng;
        let base = Instant::now();
        let mut nodes = BTreeMap::new();
        for (i, id) in node_ids.iter().enumerate() {
            let seed = seed_base.wrapping_add(*id).wrapping_add(i as u64);
            let node = RaftNode::new(*id, node_ids.to_vec(), MemPersistence::new(), cfg, seed);
            nodes.insert(
                *id,
                NodeBox {
                    node: Some(node),
                    saved_persistence: None,
                    entries: Vec::new(),
                    cluster_commit: 0,
                    applied: Vec::new(),
                    pending_wakeup: None,
                    seed,
                },
            );
        }
        let mut sim = Sim {
            base,
            clock: base,
            nodes,
            peer_list: node_ids.to_vec(),
            cfg,
            queue: BinaryHeap::new(),
            seq: 0,
            network_delay: Duration::from_millis(2),
            partitioned: HashSet::new(),
            drop_probability: 0.0,
            duplicate_probability: 0.0,
            fault_rng: rand::rngs::StdRng::seed_from_u64(seed_base ^ 0xDEAD_BEEF),
            leaders_per_term: HashMap::new(),
            transitions: Vec::new(),
            fatal_errors: Vec::new(),
        };
        // Bootstrap each node with an initial Tick — primes the
        // election timer per the lazy-arm convention in `node.rs`.
        let bootstrap: Vec<NodeId> = sim.nodes.keys().copied().collect();
        for id in bootstrap {
            sim.deliver_bootstrap_tick(id);
        }
        sim
    }

    fn deliver_bootstrap_tick(&mut self, id: NodeId) {
        let now = self.clock;
        let actions = match self.nodes.get_mut(&id).and_then(|b| b.node.as_mut()) {
            Some(n) => n.step(now, Event::Tick),
            None => return,
        };
        self.process_actions(id, now, actions);
        self.drive_replication(id, now);
        self.observe_cluster_commit(id);
    }

    /// Configure the (uniform) network delay applied to outbound RPCs.
    pub fn set_network_delay(&mut self, d: Duration) {
        self.network_delay = d;
    }

    /// Probability `[0.0, 1.0]` that any outbound message is silently
    /// dropped. The drop is tossed independently per message.
    pub fn set_drop_probability(&mut self, p: f64) {
        assert!((0.0..=1.0).contains(&p));
        self.drop_probability = p;
    }

    /// Probability `[0.0, 1.0]` that any outbound message is
    /// delivered twice (the duplicate copy ships with the same
    /// network delay). Models a flaky link that fans out retries.
    pub fn set_duplicate_probability(&mut self, p: f64) {
        assert!((0.0..=1.0).contains(&p));
        self.duplicate_probability = p;
    }

    /// Mark `(a, b)` as partitioned — every direction-symmetric
    /// message between them is dropped.
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitioned.insert(canon_pair(a, b));
    }

    pub fn heal_partition(&mut self, a: NodeId, b: NodeId) {
        self.partitioned.remove(&canon_pair(a, b));
    }

    /// Crash the node. Drops the volatile `RaftNode` (modelling the
    /// loss of in-memory state); preserves the durable `Persistence`
    /// state so a subsequent `restart` recovers from it.
    pub fn crash(&mut self, id: NodeId) {
        if let Some(b) = self.nodes.get_mut(&id)
            && let Some(node) = b.node.take()
        {
            b.saved_persistence = Some(node.into_persistence());
            b.pending_wakeup = None;
        }
    }

    /// Restart a previously-crashed node. Constructs a fresh
    /// `RaftNode` over the preserved `MemPersistence`, replays the
    /// durable entry-log high-water mark via `LogAppendComplete`
    /// (modelling a real driver's on-boot WAL recovery), and
    /// re-bootstraps with a Tick.
    ///
    /// Replaying `local_log_index` is **required** for §5.4.1
    /// correctness on restart — otherwise the node's `last_term` /
    /// `last_tx_id` advertise as 0, the up-to-date check passes
    /// for every candidate, and a candidate with strictly older log
    /// state can be granted votes it shouldn't get. That produces
    /// the kind of divergent log this simulator is meant to catch.
    pub fn restart(&mut self, id: NodeId) {
        let now = self.clock;
        // Snapshot the durable WAL extent (harness mirror models
        // disk; survives the crash). The trailing trim drops any
        // padding sentinels so we report the highest *real* tx_id.
        let last_tx = self
            .nodes
            .get(&id)
            .map(|b| {
                b.entries
                    .iter()
                    .rev()
                    .find(|e| e.tx_id != 0)
                    .map(|e| e.tx_id)
                    .unwrap_or(0)
            })
            .unwrap_or(0);

        let actions = {
            let b = match self.nodes.get_mut(&id) {
                Some(b) => b,
                None => return,
            };
            if b.node.is_some() {
                return;
            }
            let p = b.saved_persistence.take().expect("not crashed");
            let mut n = RaftNode::new(id, self.peer_list.clone(), p, self.cfg, b.seed);
            // Hydrate both watermarks from the durable WAL extent
            // before the first Tick. The harness pretends the ledger
            // tracks raft-log durability synchronously, so write and
            // commit collapse at restore time. (ADR-0017 §"Required
            // Invariants" notes the implicit recovery invariant
            // `ledger.last_commit_id <= raft_wal.last_tx_id`.)
            if last_tx > 0 {
                n.advance(last_tx, last_tx);
            }
            let acts = n.step(now, Event::Tick);
            b.node = Some(n);
            acts
        };
        self.process_actions(id, now, actions);
        self.drive_replication(id, now);
        self.observe_cluster_commit(id);
    }

    pub fn role_of(&self, id: NodeId) -> Role {
        self.nodes
            .get(&id)
            .and_then(|b| b.node.as_ref())
            .map(|n| n.role())
            .unwrap_or(Role::Initializing)
    }

    pub fn current_term_of(&self, id: NodeId) -> u64 {
        self.nodes
            .get(&id)
            .and_then(|b| b.node.as_ref())
            .map(|n| n.current_term())
            .unwrap_or(0)
    }

    pub fn cluster_commit_of(&self, id: NodeId) -> TxId {
        self.nodes
            .get(&id)
            .and_then(|b| b.node.as_ref())
            .map(|n| n.cluster_commit_index())
            .unwrap_or(0)
    }

    pub fn local_commit_of(&self, id: NodeId) -> TxId {
        self.nodes
            .get(&id)
            .and_then(|b| b.node.as_ref())
            .map(|n| n.commit_index())
            .unwrap_or(0)
    }

    pub fn entries_of(&self, id: NodeId) -> &[MirrorEntry] {
        &self.nodes[&id].entries
    }

    pub fn is_crashed(&self, id: NodeId) -> bool {
        self.nodes.get(&id).map(|b| b.is_crashed()).unwrap_or(true)
    }

    /// Inject a "client write" on `leader_id`: simulates the leader's
    /// ledger appending the next tx_id, notifying raft via the
    /// `advance(write, commit)` direct method, and flushing deferred
    /// outputs via a `Tick` (per ADR-0017 §"Driver call pattern":
    /// always Tick after advance). Skips silently if the named node
    /// is not (or no longer) a leader.
    pub fn client_write(&mut self, leader_id: NodeId, count: usize) {
        let term = self.current_term_of(leader_id);
        if !self.role_of(leader_id).is_leader() {
            return;
        }
        let now = self.clock;
        let mut actions_acc: Vec<(NodeId, Vec<Action>)> = Vec::new();
        if let Some(b) = self.nodes.get_mut(&leader_id)
            && let Some(node) = b.node.as_mut()
        {
            let start = node.commit_index();
            for i in 0..count {
                let tx = start + 1 + i as u64;
                b.entries.push(MirrorEntry::new(tx, term));
                // Single direct call advances both watermarks. Write
                // bounds the AE replication window, commit feeds the
                // leader's quorum self-slot.
                node.advance(tx, tx);
                // Tick flushes any deferred outputs (heartbeat fires,
                // SetWakeup re-arming) — `advance` itself is silent.
                let tick_acts = node.step(now, Event::Tick);
                actions_acc.push((leader_id, tick_acts));
            }
        }
        for (id, acts) in actions_acc {
            self.process_actions(id, now, acts);
        }
        self.drive_replication(leader_id, now);
        self.observe_cluster_commit(leader_id);
    }

    /// Drive the simulator until `deadline`. Returns the number of
    /// step invocations executed.
    pub fn run_until(&mut self, deadline: Instant) -> usize {
        self.requeue_pending_wakeups();
        let mut steps = 0usize;
        while let Some(Reverse(s)) = self.queue.peek() {
            if s.at > deadline {
                break;
            }
            let s = self.queue.pop().unwrap().0;
            self.clock = s.at;
            steps += 1;
            self.dispatch(s);
            if steps > 200_000 {
                panic!("simulator: runaway loop ({} steps)", steps);
            }
        }
        steps
    }

    /// Drive until either `deadline` or `predicate` returns true.
    pub fn run_until_predicate<F>(&mut self, deadline: Instant, mut predicate: F) -> bool
    where
        F: FnMut(&Sim) -> bool,
    {
        self.requeue_pending_wakeups();
        let mut steps = 0u32;
        while let Some(Reverse(s)) = self.queue.peek() {
            if s.at > deadline {
                break;
            }
            let s = self.queue.pop().unwrap().0;
            self.clock = s.at;
            self.dispatch(s);
            if predicate(self) {
                return true;
            }
            steps += 1;
            if steps > 200_000 {
                panic!("simulator: runaway loop");
            }
        }
        false
    }

    fn requeue_pending_wakeups(&mut self) {
        let pending: Vec<(NodeId, Instant)> = self
            .nodes
            .iter()
            .filter_map(|(id, b)| b.pending_wakeup.map(|w| (*id, w)))
            .collect();
        for (id, w) in pending {
            self.schedule_event(w, id, Event::Tick);
        }
    }

    fn dispatch(&mut self, s: Scheduled) {
        if !self.nodes.contains_key(&s.target) {
            return;
        }
        if self.nodes[&s.target].is_crashed() {
            return;
        }
        let now = s.at;
        match s.delivery {
            // ADR-0017 §"Driver call pattern" follower path: the
            // simulator plays the role of the cluster driver — calls
            // `validate_append_entries_request`, applies the WAL
            // changes to the harness mirror, calls `advance` on
            // durability, synthesizes the wire reply.
            Delivery::AppendEntries {
                from,
                term,
                prev_log_tx_id,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.deliver_append_entries(
                    s.target,
                    now,
                    from,
                    term,
                    prev_log_tx_id,
                    prev_log_term,
                    entries,
                    leader_commit,
                );
            }
            // Leader-side: feed an AE reply into the per-peer
            // `Replication` API. `peer(from)` returns `None` if the
            // sender is not a configured peer of this node — that
            // matches the legacy event path's "ignore unsolicited
            // replies" semantics. ADR-0017 §"Driver call pattern":
            // follow the direct-method call with `step(Tick)` so any
            // wakeup re-arming on the leader (peer's
            // `next_heartbeat`, in-flight expiry clearing) gets
            // emitted as `Action::SetWakeup` for the simulator to
            // schedule.
            Delivery::AppendEntriesReply { from, result } => {
                if let Some(node) =
                    self.nodes.get_mut(&s.target).and_then(|b| b.node.as_mut())
                    && let Some(mut peer) = node.replication().peer(from)
                {
                    peer.append_result(now, result);
                }
                let tick_actions = match self
                    .nodes
                    .get_mut(&s.target)
                    .and_then(|b| b.node.as_mut())
                {
                    Some(n) => n.step(now, Event::Tick),
                    None => return,
                };
                self.process_actions(s.target, now, tick_actions);
                self.drive_replication(s.target, now);
                self.observe_cluster_commit(s.target);
            }
            Delivery::Step(event) => {
                let actions = match self.nodes.get_mut(&s.target).and_then(|b| b.node.as_mut())
                {
                    Some(n) => n.step(now, event),
                    None => return,
                };
                self.process_actions(s.target, now, actions);
                self.drive_replication(s.target, now);
                self.observe_cluster_commit(s.target);
            }
        }
    }

    /// Cluster-driver-side handling of an inbound `AppendEntries` for
    /// `target`. Mirrors the new follower flow: validate → mirror
    /// truncation/append → `advance` on durability → `step(Tick)` to
    /// flush wakeups → synthesize and ship the wire reply.
    #[allow(clippy::too_many_arguments)]
    fn deliver_append_entries(
        &mut self,
        target: NodeId,
        now: Instant,
        from: NodeId,
        term: Term,
        prev_log_tx_id: TxId,
        prev_log_term: Term,
        entries: LogEntryRange,
        leader_commit: TxId,
    ) {
        let decision = match self.nodes.get_mut(&target).and_then(|b| b.node.as_mut()) {
            Some(n) => n.validate_append_entries_request(
                now,
                from,
                term,
                prev_log_tx_id,
                prev_log_term,
                entries,
                leader_commit,
            ),
            None => return,
        };

        let (success, reject_reason): (bool, Option<RejectReason>) = match decision {
            AppendEntriesDecision::Reject {
                reason,
                truncate_after,
            } => {
                if let Some(t) = truncate_after
                    && let Some(b) = self.nodes.get_mut(&target)
                {
                    b.entries.truncate(t as usize);
                }
                (false, Some(reason))
            }
            AppendEntriesDecision::Accept { append: None } => (true, None),
            AppendEntriesDecision::Accept {
                append: Some(range),
            } => {
                if !range.is_empty() {
                    if let Some(b) = self.nodes.get_mut(&target) {
                        for i in 0..range.count {
                            let tx = range.start_tx_id + i;
                            while b.entries.len() < (tx as usize).saturating_sub(1) {
                                b.entries.push(MirrorEntry::new(0, 0));
                            }
                            let entry = MirrorEntry::new(tx, range.term);
                            if (tx as usize) <= b.entries.len() {
                                b.entries[(tx - 1) as usize] = entry;
                            } else {
                                b.entries.push(entry);
                            }
                        }
                    }
                    let last_tx = range.last_tx_id().unwrap();
                    // Synchronous-apply harness model: ledger applies
                    // AE entries the moment they're durable, so write
                    // and commit watermarks move together.
                    if let Some(node) =
                        self.nodes.get_mut(&target).and_then(|b| b.node.as_mut())
                    {
                        node.advance(last_tx, last_tx);
                    }
                }
                (true, None)
            }
            AppendEntriesDecision::Fatal { reason } => {
                self.fatal_errors.push((target, reason));
                return;
            }
        };

        // Tick after validate (+ optional advance) so any wakeup
        // re-arming or election-timer changes get re-emitted as
        // `Action::SetWakeup`. ADR-0017 §"Driver call pattern": the
        // driver always follows `advance` with `step(Tick)`.
        let tick_actions = match self.nodes.get_mut(&target).and_then(|b| b.node.as_mut()) {
            Some(n) => n.step(now, Event::Tick),
            None => return,
        };
        self.process_actions(target, now, tick_actions);
        self.drive_replication(target, now);
        self.observe_cluster_commit(target);

        // Synthesize the reply from getters (ADR-0017 §"AE reply:
        // write vs commit watermark" — the reply ships
        // `last_commit_id` / `last_write_id` separately) and ship
        // it back through the simulated network, subject to the
        // drop / duplicate model. Translates the (success,
        // reject_reason) pair into the `AppendResult` enum the
        // leader's `Replication::append_result` expects.
        if self.is_dropped(target, from) || self.rolled_drop() {
            return;
        }
        let (reply_term, last_commit_id, last_write_id) = match self
            .nodes
            .get(&target)
            .and_then(|b| b.node.as_ref())
        {
            Some(n) => (n.current_term(), n.commit_index(), n.write_index()),
            None => return,
        };
        let result = if success {
            AppendResult::Success {
                term: reply_term,
                last_write_id,
                last_commit_id,
            }
        } else {
            AppendResult::Reject {
                term: reply_term,
                reason: reject_reason.unwrap_or(RejectReason::LogMismatch),
                last_write_id,
                last_commit_id,
            }
        };
        let when = now + self.network_delay;
        let delivery = Delivery::AppendEntriesReply {
            from: target,
            result,
        };
        let dup = self.rolled_duplicate();
        self.schedule(when, from, delivery.clone());
        if dup {
            self.schedule(when, from, delivery);
        }
    }

    /// Cluster-driver-side leader replication: walk every peer of
    /// `src` and pull the next `AppendEntries` via the
    /// `Replication` API, scheduling the result as a
    /// `Delivery::AppendEntries` (subject to the network drop /
    /// duplicate model). This replaces the legacy
    /// `Action::SendAppendEntries` push path.
    ///
    /// Called after every `step` on a node so that:
    ///
    /// - new entries shipped via `client_write` (advance + Tick)
    ///   get fanned out immediately;
    /// - heartbeat deadlines firing on `Tick` get serviced;
    /// - newly-elected leaders' first round of AEs goes out as
    ///   soon as `BecomeRole { Leader, .. }` is observed.
    ///
    /// `get_append_range` itself gates on `next_heartbeat` and
    /// in-flight, so calling it on every step is idempotent — a
    /// peer that's not due returns `None`.
    fn drive_replication(&mut self, src: NodeId, now: Instant) {
        let peers = match self.nodes.get_mut(&src).and_then(|b| b.node.as_mut()) {
            Some(n) => n.replication().peers(),
            None => return,
        };
        for peer in peers {
            let req = match self.nodes.get_mut(&src).and_then(|b| b.node.as_mut()) {
                Some(n) => n
                    .replication()
                    .peer(peer)
                    .and_then(|mut p| p.get_append_range(now)),
                None => return,
            };
            let Some(req) = req else { continue };

            if self.is_dropped(src, peer) || self.rolled_drop() {
                continue;
            }
            let when = now + self.network_delay;
            let dup = self.rolled_duplicate();
            let delivery = Delivery::AppendEntries {
                from: src,
                term: req.term,
                prev_log_tx_id: req.prev_log_tx_id,
                prev_log_term: req.prev_log_term,
                entries: req.entries,
                leader_commit: req.leader_commit,
            };
            self.schedule(when, peer, delivery.clone());
            if dup {
                self.schedule(when, peer, delivery);
            }
        }
    }

    /// Poll the node's `cluster_commit_index()` and apply any
    /// advance to the harness mirror. Replaces the former
    /// `Action::AdvanceClusterCommit` consumer (ADR-0017 §"Driver
    /// call pattern": drivers poll the getter; no dedicated
    /// action). Call after every `step` that may have changed
    /// state.
    fn observe_cluster_commit(&mut self, src: NodeId) {
        let new_value = self
            .nodes
            .get(&src)
            .and_then(|b| b.node.as_ref())
            .map(|n| n.cluster_commit_index())
            .unwrap_or(0);
        if let Some(b) = self.nodes.get_mut(&src) {
            if new_value > b.cluster_commit {
                b.cluster_commit = new_value;
            }
            // Apply (state-machine safety): the harness captures
            // the apply order to assert determinism across nodes.
            for i in 0..b.cluster_commit {
                let idx = i as usize;
                if idx < b.entries.len() && b.applied.len() < (i + 1) as usize {
                    b.applied.push(b.entries[idx]);
                }
            }
        }
    }

    fn process_actions(&mut self, src: NodeId, now: Instant, actions: Vec<Action>) {
        for a in actions {
            match a {
                Action::SendRequestVote {
                    to,
                    term,
                    last_tx_id,
                    last_term,
                } => {
                    if self.is_dropped(src, to) || self.rolled_drop() {
                        continue;
                    }
                    let when = now + self.network_delay;
                    let event = Event::RequestVoteRequest {
                        from: src,
                        term,
                        last_tx_id,
                        last_term,
                    };
                    let dup = self.rolled_duplicate();
                    self.schedule_event(when, to, event.clone());
                    if dup {
                        self.schedule_event(when, to, event);
                    }
                }
                Action::SendRequestVoteReply { to, term, granted } => {
                    if self.is_dropped(src, to) || self.rolled_drop() {
                        continue;
                    }
                    let when = now + self.network_delay;
                    let event = Event::RequestVoteReply {
                        from: src,
                        term,
                        granted,
                    };
                    let dup = self.rolled_duplicate();
                    self.schedule_event(when, to, event.clone());
                    if dup {
                        self.schedule_event(when, to, event);
                    }
                }
                Action::BecomeRole { role, term } => {
                    self.transitions.push((src, role, term));
                    if matches!(role, Role::Leader) {
                        self.leaders_per_term.entry(term).or_default().insert(src);
                    }
                }
                Action::SetWakeup { at } => {
                    if let Some(b) = self.nodes.get_mut(&src) {
                        b.pending_wakeup = Some(at);
                    }
                    self.schedule_event(at, src, Event::Tick);
                }
                Action::FatalError { reason } => {
                    // The library has frozen — record it for the
                    // assertion helpers. In simulator runs that don't
                    // expect a fatal, this surfaces via
                    // `Sim::fatal_errors()` (panic-on-unexpected is the
                    // caller's responsibility).
                    self.fatal_errors.push((src, reason));
                }
            }
        }
    }

    fn schedule(&mut self, at: Instant, target: NodeId, delivery: Delivery) {
        self.seq += 1;
        self.queue.push(Reverse(Scheduled {
            at,
            target,
            delivery,
            seq: self.seq,
        }));
    }

    fn schedule_event(&mut self, at: Instant, target: NodeId, event: Event) {
        self.schedule(at, target, Delivery::Step(event));
    }

    /// Probabilistic drop check. Tossed once per outbound message;
    /// hard-drops (partition / crashed target) take precedence and
    /// don't consume RNG state.
    fn rolled_drop(&mut self) -> bool {
        if self.drop_probability <= 0.0 {
            return false;
        }
        use rand::Rng;
        self.fault_rng.r#gen::<f64>() < self.drop_probability
    }

    fn rolled_duplicate(&mut self) -> bool {
        if self.duplicate_probability <= 0.0 {
            return false;
        }
        use rand::Rng;
        self.fault_rng.r#gen::<f64>() < self.duplicate_probability
    }

    fn is_dropped(&self, from: NodeId, to: NodeId) -> bool {
        self.partitioned.contains(&canon_pair(from, to))
            || self.nodes.get(&to).map(|b| b.is_crashed()).unwrap_or(true)
    }

    /// Election-safety check: at most one leader per term across all
    /// nodes that ever entered Leader for that term.
    pub fn assert_election_safety(&self) {
        for (term, leaders) in &self.leaders_per_term {
            assert!(
                leaders.len() <= 1,
                "election safety violated at term {}: leaders {:?}",
                term,
                leaders
            );
        }
    }

    /// State-machine safety: every applied prefix is a prefix of every
    /// other applied prefix (no two nodes apply different entries at
    /// the same index).
    pub fn assert_state_machine_safety(&self) {
        let mut nodes: Vec<NodeId> = self.nodes.keys().copied().collect();
        nodes.sort();
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                let a = &self.nodes[&nodes[i]].applied;
                let b = &self.nodes[&nodes[j]].applied;
                let n = a.len().min(b.len());
                for k in 0..n {
                    assert_eq!(
                        a[k], b[k],
                        "state machine safety violated at index {} between nodes {} and {}: {:?} vs {:?}",
                        k, nodes[i], nodes[j], a[k], b[k]
                    );
                }
            }
        }
    }

    /// Log-matching property: if two logs both contain an entry at
    /// `tx_id` with the same term, all preceding entries match.
    pub fn assert_log_matching(&self) {
        let mut nodes: Vec<NodeId> = self.nodes.keys().copied().collect();
        nodes.sort();
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                let a = &self.nodes[&nodes[i]].entries;
                let b = &self.nodes[&nodes[j]].entries;
                let n = a.len().min(b.len());
                for k in 0..n {
                    if a[k].tx_id == b[k].tx_id && a[k].term == b[k].term {
                        for p in 0..k {
                            if a[p] != b[p] {
                                self.dump_logs_for_diagnosis();
                                panic!(
                                    "log matching violated: nodes {}, {} agree at index {} (entry {:?}) but differ at {}: {:?} vs {:?}",
                                    nodes[i], nodes[j], k, a[k], p, a[p], b[p]
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// Dump every node's harness log + role/term/commit snapshot.
    /// Called by `assert_log_matching` on a failure so the test
    /// output explains the disagreement.
    fn dump_logs_for_diagnosis(&self) {
        let mut nodes: Vec<NodeId> = self.nodes.keys().copied().collect();
        nodes.sort();
        eprintln!(
            "\n=== Sim log dump (clock={}ms) ===",
            self.elapsed().as_millis()
        );
        for n in &nodes {
            let b = &self.nodes[n];
            let role = b
                .node
                .as_ref()
                .map(|x| format!("{:?}", x.role()))
                .unwrap_or_else(|| "Crashed".to_string());
            let term = b
                .node
                .as_ref()
                .map(|x| x.current_term().to_string())
                .unwrap_or_else(|| "?".to_string());
            let lci = b
                .node
                .as_ref()
                .map(|x| x.commit_index().to_string())
                .unwrap_or_else(|| "?".to_string());
            let cci = b
                .node
                .as_ref()
                .map(|x| x.cluster_commit_index().to_string())
                .unwrap_or_else(|| "?".to_string());
            eprintln!(
                "  node {}: role={} term={} local_commit={} cluster_commit={}",
                n, role, term, lci, cci
            );
            for (i, e) in b.entries.iter().enumerate() {
                eprintln!("    [{}] {:?}", i, e);
            }
        }
        eprintln!("=== end dump ===\n");
    }

    /// Time elapsed in virtual time since the simulator started.
    pub fn elapsed(&self) -> Duration {
        self.clock.duration_since(self.base)
    }

    /// Current virtual time. Tests use this to compose new `deadline`
    /// values after partial runs.
    pub fn clock(&self) -> Instant {
        self.clock
    }

    /// Standard config used across the integration tests. Aggressive
    /// election + heartbeat windows so the simulator finishes a
    /// realistic schedule in tens of milliseconds of virtual time.
    pub fn standard_cfg() -> RaftConfig {
        RaftConfig {
            election_timer: ElectionTimerConfig {
                min_ms: 80,
                max_ms: 160,
            },
            heartbeat_interval: Duration::from_millis(20),
            rpc_timeout: Duration::from_millis(80),
            max_entries_per_append: 16,
        }
    }
}

fn canon_pair(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    if a < b { (a, b) } else { (b, a) }
}
