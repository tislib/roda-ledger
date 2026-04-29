//! Deterministic in-process simulator for `RaftNode`.
//!
//! ADR-0017 §"Test Strategy" calls for a fake clock + fake message
//! bus driving N nodes through randomized event schedules with fault
//! injection. This file provides the harness; `tests/simulator.rs`
//! exercises end-to-end scenarios and `tests/safety_properties.rs`
//! drives proptest schedules.
//!
//! Concept of operation:
//! - The harness owns N `RaftNode`s, each with its own `data_dir`.
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
//!   per-node crash markers, partition sets.

// Each integration test compiles `common/mod.rs` into its own test
// binary, so methods unused by *that* binary look dead even when
// other binaries call them. The harness is intentionally a complete
// API surface — silence the false positives.
#![allow(dead_code)]

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::time::{Duration, Instant};

use raft::{
    Action, ElectionTimerConfig, Event, LogEntryMeta, NodeId, RaftConfig, RaftInitialState,
    RaftNode, Role, TxId,
};

#[derive(Clone, Debug)]
struct Scheduled {
    at: Instant,
    target: NodeId,
    event: Event,
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

struct NodeBox {
    node: RaftNode,
    /// Append-only mirror of the local log. `tx_id` indexes
    /// `entries[tx_id - 1]` (1-based). Truncation drops the suffix.
    entries: Vec<LogEntryMeta>,
    /// Cluster commit watermark observed via `Action::AdvanceClusterCommit`.
    cluster_commit: TxId,
    /// Set of (term, tx_id) the node has been told to apply; used
    /// for state-machine safety checks.
    applied: Vec<LogEntryMeta>,
    /// Crash flag — when true the harness drops every event for this
    /// node (used by fault injection scenarios).
    crashed: bool,
    /// Most recent `SetWakeup` requested by this node.
    pending_wakeup: Option<Instant>,
}

pub struct Sim {
    base: Instant,
    clock: Instant,
    nodes: HashMap<NodeId, NodeBox>,
    queue: BinaryHeap<Reverse<Scheduled>>,
    seq: u64,
    network_delay: Duration,
    /// Pairs `(a, b)` such that any message a→b or b→a is dropped.
    /// Modelled as an undirected partition.
    partitioned: HashSet<(NodeId, NodeId)>,
    /// All durations a node was observed in `Role::Leader`, by
    /// `(term, leader_id)`. Used to verify Election Safety
    /// (§5.4 #1: at most one leader per term).
    leaders_per_term: HashMap<u64, HashSet<NodeId>>,
    /// Recorded so callers can inspect after the run.
    transitions: Vec<(NodeId, Role, u64)>,
}

impl Sim {
    pub fn new(node_ids: &[NodeId], cfg: RaftConfig, seed_base: u64) -> Self {
        let base = Instant::now();
        let mut nodes = HashMap::new();
        for (i, id) in node_ids.iter().enumerate() {
            // No tempdir / data_dir: ADR-0017 keeps I/O out of the
            // library. The simulator round-trips
            // `Action::Persist*` → `Event::*Persisted` immediately,
            // emulating an instant-durable driver.
            let node = RaftNode::new(
                *id,
                node_ids.to_vec(),
                seed_base.wrapping_add(*id).wrapping_add(i as u64),
                cfg,
                RaftInitialState::default(),
            );
            nodes.insert(
                *id,
                NodeBox {
                    node,
                    entries: Vec::new(),
                    cluster_commit: 0,
                    applied: Vec::new(),
                    crashed: false,
                    pending_wakeup: None,
                },
            );
        }
        let mut sim = Sim {
            base,
            clock: base,
            nodes,
            queue: BinaryHeap::new(),
            seq: 0,
            network_delay: Duration::from_millis(2),
            partitioned: HashSet::new(),
            leaders_per_term: HashMap::new(),
            transitions: Vec::new(),
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
        let actions = match self.nodes.get_mut(&id) {
            Some(b) => b.node.step(now, Event::Tick),
            None => return,
        };
        self.process_actions(id, now, actions);
    }

    /// Configure the (uniform) network delay applied to outbound RPCs.
    pub fn set_network_delay(&mut self, d: Duration) {
        self.network_delay = d;
    }

    /// Mark `(a, b)` as partitioned — every direction-symmetric
    /// message between them is dropped.
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitioned.insert(canon_pair(a, b));
    }

    pub fn heal_partition(&mut self, a: NodeId, b: NodeId) {
        self.partitioned.remove(&canon_pair(a, b));
    }

    pub fn crash(&mut self, id: NodeId) {
        if let Some(b) = self.nodes.get_mut(&id) {
            b.crashed = true;
        }
    }

    pub fn restart(&mut self, id: NodeId) {
        if let Some(b) = self.nodes.get_mut(&id) {
            b.crashed = false;
            // Re-prime the election timer post-restart.
            let now = self.clock;
            let actions = b.node.step(now, Event::Tick);
            self.process_actions(id, now, actions);
        }
    }

    pub fn role_of(&self, id: NodeId) -> Role {
        self.nodes[&id].node.role()
    }

    pub fn current_term_of(&self, id: NodeId) -> u64 {
        self.nodes[&id].node.current_term()
    }

    pub fn cluster_commit_of(&self, id: NodeId) -> TxId {
        self.nodes[&id].node.cluster_commit_index()
    }

    pub fn local_commit_of(&self, id: NodeId) -> TxId {
        self.nodes[&id].node.commit_index()
    }

    pub fn entries_of(&self, id: NodeId) -> &[LogEntryMeta] {
        &self.nodes[&id].entries
    }

    /// Inject a "client write" on `leader_id`: simulates the leader's
    /// ledger appending the next tx_id and notifying raft via
    /// `LocalCommitAdvanced`. Skips silently if the named node is not
    /// (or no longer) a leader.
    pub fn client_write(&mut self, leader_id: NodeId, count: usize) {
        let term = self.current_term_of(leader_id);
        if !self.role_of(leader_id).is_leader() {
            return;
        }
        let now = self.clock;
        let mut actions_acc: Vec<(NodeId, Vec<Action>)> = Vec::new();
        if let Some(b) = self.nodes.get_mut(&leader_id) {
            let start = b.node.commit_index();
            for i in 0..count {
                let tx = start + 1 + i as u64;
                b.entries.push(LogEntryMeta::new(tx, term));
                let acts = b.node.step(now, Event::LocalCommitAdvanced { tx_id: tx });
                actions_acc.push((leader_id, acts));
            }
        }
        for (id, acts) in actions_acc {
            self.process_actions(id, now, acts);
        }
    }

    /// Drive the simulator until `deadline`. Returns the number of
    /// step invocations executed.
    pub fn run_until(&mut self, deadline: Instant) -> usize {
        let mut steps = 0usize;
        // Re-arm wakeups in the queue from any pending_wakeup the
        // bootstrap left behind.
        let pending: Vec<(NodeId, Instant)> = self
            .nodes
            .iter()
            .filter_map(|(id, b)| b.pending_wakeup.map(|w| (*id, w)))
            .collect();
        for (id, w) in pending {
            self.schedule(w, id, Event::Tick);
        }
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
        // Same loop body as `run_until` but with an early-exit hook.
        let pending: Vec<(NodeId, Instant)> = self
            .nodes
            .iter()
            .filter_map(|(id, b)| b.pending_wakeup.map(|w| (*id, w)))
            .collect();
        for (id, w) in pending {
            self.schedule(w, id, Event::Tick);
        }
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

    fn dispatch(&mut self, s: Scheduled) {
        if !self.nodes.contains_key(&s.target) {
            return;
        }
        if self.nodes[&s.target].crashed {
            return;
        }
        let now = s.at;
        let actions = match self.nodes.get_mut(&s.target) {
            Some(b) => b.node.step(now, s.event),
            None => return,
        };
        self.process_actions(s.target, now, actions);
    }

    fn process_actions(&mut self, src: NodeId, now: Instant, actions: Vec<Action>) {
        for a in actions {
            match a {
                Action::SendAppendEntries {
                    to,
                    term,
                    prev_log_tx_id,
                    prev_log_term,
                    entries,
                    leader_commit,
                } => {
                    if self.is_dropped(src, to) {
                        continue;
                    }
                    let when = now + self.network_delay;
                    self.schedule(
                        when,
                        to,
                        Event::AppendEntriesRequest {
                            from: src,
                            term,
                            prev_log_tx_id,
                            prev_log_term,
                            entries,
                            leader_commit,
                        },
                    );
                }
                Action::SendAppendEntriesReply {
                    to,
                    term,
                    success,
                    last_tx_id,
                } => {
                    if self.is_dropped(src, to) {
                        continue;
                    }
                    let when = now + self.network_delay;
                    self.schedule(
                        when,
                        to,
                        Event::AppendEntriesReply {
                            from: src,
                            term,
                            success,
                            last_tx_id,
                            reject_reason: None,
                        },
                    );
                }
                Action::SendRequestVote {
                    to,
                    term,
                    last_tx_id,
                    last_term,
                } => {
                    if self.is_dropped(src, to) {
                        continue;
                    }
                    let when = now + self.network_delay;
                    self.schedule(
                        when,
                        to,
                        Event::RequestVoteRequest {
                            from: src,
                            term,
                            last_tx_id,
                            last_term,
                        },
                    );
                }
                Action::SendRequestVoteReply { to, term, granted } => {
                    if self.is_dropped(src, to) {
                        continue;
                    }
                    let when = now + self.network_delay;
                    self.schedule(
                        when,
                        to,
                        Event::RequestVoteReply {
                            from: src,
                            term,
                            granted,
                        },
                    );
                }
                Action::AppendLog { tx_id, term } => {
                    if let Some(b) = self.nodes.get_mut(&src) {
                        // Pad the log if needed (the harness's mirror
                        // is dense from tx_id 1..N — gaps shouldn't
                        // happen with correct §5.3 prev-log matching).
                        while b.entries.len() < (tx_id as usize).saturating_sub(1) {
                            b.entries.push(LogEntryMeta::new(0, 0));
                        }
                        if (tx_id as usize) <= b.entries.len() {
                            b.entries[(tx_id - 1) as usize] = LogEntryMeta::new(tx_id, term);
                        } else {
                            b.entries.push(LogEntryMeta::new(tx_id, term));
                        }
                    }
                    let actions = self
                        .nodes
                        .get_mut(&src)
                        .map(|b| b.node.step(now, Event::LogAppendComplete { tx_id }));
                    if let Some(a) = actions {
                        self.process_actions(src, now, a);
                    }
                }
                Action::TruncateLog { after_tx_id } => {
                    if let Some(b) = self.nodes.get_mut(&src) {
                        b.entries.truncate(after_tx_id as usize);
                    }
                    let actions = self.nodes.get_mut(&src).map(|b| {
                        b.node
                            .step(now, Event::LogTruncateComplete { up_to: after_tx_id })
                    });
                    if let Some(a) = actions {
                        self.process_actions(src, now, a);
                    }
                }
                Action::AdvanceClusterCommit { tx_id } => {
                    if let Some(b) = self.nodes.get_mut(&src) {
                        if tx_id > b.cluster_commit {
                            b.cluster_commit = tx_id;
                        }
                        // Apply (state-machine safety): the harness
                        // captures the apply order to assert
                        // determinism across nodes.
                        for i in 0..tx_id {
                            let idx = i as usize;
                            if idx < b.entries.len() && b.applied.len() < (i + 1) as usize {
                                b.applied.push(b.entries[idx]);
                            }
                        }
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
                    self.schedule(at, src, Event::Tick);
                }
                Action::PersistTerm { term, start_tx_id } => {
                    // Emulate an instant-durable driver: feed the ack
                    // back into the same node so the library can
                    // advance `durable_term`.
                    let actions = self
                        .nodes
                        .get_mut(&src)
                        .map(|b| b.node.step(now, Event::TermPersisted { term, start_tx_id }));
                    if let Some(a) = actions {
                        self.process_actions(src, now, a);
                    }
                }
                Action::PersistVote { term, voted_for } => {
                    let actions = self
                        .nodes
                        .get_mut(&src)
                        .map(|b| b.node.step(now, Event::VotePersisted { term, voted_for }));
                    if let Some(a) = actions {
                        self.process_actions(src, now, a);
                    }
                }
            }
        }
    }

    fn schedule(&mut self, at: Instant, target: NodeId, event: Event) {
        self.seq += 1;
        self.queue.push(Reverse(Scheduled {
            at,
            target,
            event,
            seq: self.seq,
        }));
    }

    fn is_dropped(&self, from: NodeId, to: NodeId) -> bool {
        self.partitioned.contains(&canon_pair(from, to))
            || self.nodes.get(&to).map(|b| b.crashed).unwrap_or(true)
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
                        // All preceding entries must match.
                        for p in 0..k {
                            assert_eq!(
                                a[p], b[p],
                                "log matching violated: nodes {}, {} agree at index {} but differ at {}",
                                nodes[i], nodes[j], k, p
                            );
                        }
                    }
                }
            }
        }
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
