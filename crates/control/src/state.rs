//! Central in-memory state for the control mock.
//!
//! All mutations and reads go through the same `parking_lot::RwLock`. The
//! `service` module takes write or read locks per RPC; the `background`
//! task takes a write lock once per tick. Lock granularity is intentionally
//! coarse — for a CRUD mock the simplicity is worth more than the throughput.
//!
//! Ledger data-plane state (transactions, balances, function registry)
//! lives on the real ledger nodes the control plane proxies to. The
//! state here is **only** the operational/orchestration shape:
//! membership, partitions, election + fault history, scenario runs,
//! and per-node mocked Raft observations.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use proto::control::{
    ClusterConfig, ElectionEvent, ElectionReason, FaultEvent, FaultKind, NodeRole, Scenario,
    ScenarioState as PbScenarioState,
};

/// Process-level health. Network state (Partitioned/Isolated) is derived at
/// snapshot time from the partition matrix and the node's reachable peers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessHealth {
    Up,
    Stopped,
}

#[derive(Clone, Debug)]
pub struct NodeRecord {
    pub node_id: u64,
    pub address: String,
    pub role: NodeRole,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub health: ProcessHealth,
    pub last_heartbeat_at_ms: i64,
    pub compute_index: u64,
    pub commit_index: u64,
    pub snapshot_index: u64,
    pub cluster_commit_index: u64,
}

impl NodeRecord {
    pub fn fresh(node_id: u64, role: NodeRole, term: u64) -> Self {
        Self {
            node_id,
            address: format!("127.0.0.1:5005{node_id}"),
            role,
            current_term: term,
            voted_for: if matches!(role, NodeRole::Leader) {
                Some(node_id)
            } else {
                None
            },
            health: ProcessHealth::Up,
            last_heartbeat_at_ms: epoch_ms_now(),
            compute_index: 0,
            commit_index: 0,
            snapshot_index: 0,
            cluster_commit_index: 0,
        }
    }

    /// Build a `NodeRecord` for a peer the control plane was provisioned
    /// with. The peer's gRPC address is recorded verbatim so the UI can
    /// display the connection target.
    pub fn from_peer(node_id: u64, address: String, role: NodeRole, term: u64) -> Self {
        let mut n = Self::fresh(node_id, role, term);
        n.address = address;
        n
    }
}

#[derive(Clone, Debug)]
pub struct RunRecord {
    pub run_id: String,
    pub scenario: Scenario,
    pub state: PbScenarioState,
    pub started_at: Instant,
    pub started_at_ms: i64,
    pub ended_at: Option<Instant>,
    pub ended_at_ms: i64,
    pub progress_pct: u32,
    pub ops_submitted: u64,
    pub ops_succeeded: u64,
    pub ops_failed: u64,
    pub step_index: usize,
    pub step_started_at: Instant,
    pub step_ops_emitted: u64,
    pub cancel_requested: bool,
    pub error: String,
    pub recent_steps: VecDeque<String>,
}

pub struct InMemoryState {
    pub nodes: BTreeMap<u64, NodeRecord>,
    pub target_node_count: u32,
    pub cluster_config: ClusterConfig,
    pub partitions: BTreeSet<(u64, u64)>,
    pub elections: VecDeque<ElectionEvent>,
    pub faults: VecDeque<FaultEvent>,
    pub scenario_runs: BTreeMap<String, RunRecord>,
    pub next_run_seq: u64,
    pub started_at: Instant,
}

impl InMemoryState {
    pub fn new(seed_nodes: u32) -> Self {
        let mut nodes = BTreeMap::new();
        let count = seed_nodes.max(1);
        for i in 1..=count as u64 {
            let role = if i == 1 {
                NodeRole::Leader
            } else {
                NodeRole::Follower
            };
            nodes.insert(i, NodeRecord::fresh(i, role, 1));
        }

        let mut elections = VecDeque::with_capacity(64);
        elections.push_back(ElectionEvent {
            at_ms: epoch_ms_now(),
            term: 1,
            winner_node_id: 1,
            reason: ElectionReason::Bootstrap as i32,
        });

        Self {
            nodes,
            target_node_count: count,
            cluster_config: default_cluster_config(),
            partitions: BTreeSet::new(),
            elections,
            faults: VecDeque::with_capacity(256),
            scenario_runs: BTreeMap::new(),
            next_run_seq: 1,
            started_at: Instant::now(),
        }
    }

    /// Build an `InMemoryState` whose membership tracks the explicit
    /// peer set the control plane was provisioned with. The first peer
    /// (lowest-id) is seeded as Leader so writes have somewhere to go;
    /// the proxy's leader-rotation logic learns the real leader as
    /// soon as the first write attempt completes.
    pub fn from_peers(peers: &[(u64, String)]) -> Self {
        if peers.is_empty() {
            return Self::new(1);
        }
        let mut nodes = BTreeMap::new();
        let mut sorted: Vec<&(u64, String)> = peers.iter().collect();
        sorted.sort_by_key(|(id, _)| *id);
        let leader_id = sorted[0].0;
        for (id, addr) in &sorted {
            let role = if *id == leader_id {
                NodeRole::Leader
            } else {
                NodeRole::Follower
            };
            nodes.insert(*id, NodeRecord::from_peer(*id, addr.clone(), role, 1));
        }
        let mut elections = VecDeque::with_capacity(64);
        elections.push_back(ElectionEvent {
            at_ms: epoch_ms_now(),
            term: 1,
            winner_node_id: leader_id,
            reason: ElectionReason::Bootstrap as i32,
        });
        Self {
            nodes,
            target_node_count: peers.len() as u32,
            cluster_config: default_cluster_config(),
            partitions: BTreeSet::new(),
            elections,
            faults: VecDeque::with_capacity(256),
            scenario_runs: BTreeMap::new(),
            next_run_seq: 1,
            started_at: Instant::now(),
        }
    }

    pub fn current_leader(&self) -> Option<&NodeRecord> {
        self.nodes
            .values()
            .find(|n| matches!(n.role, NodeRole::Leader) && n.health == ProcessHealth::Up)
    }

    pub fn current_leader_mut(&mut self) -> Option<&mut NodeRecord> {
        self.nodes
            .values_mut()
            .find(|n| matches!(n.role, NodeRole::Leader) && n.health == ProcessHealth::Up)
    }

    /// True iff `from` and `to` are not on opposite ends of an active partition.
    pub fn reachable(&self, from: u64, to: u64) -> bool {
        if from == to {
            return true;
        }
        let pair = canonical_pair(from, to);
        !self.partitions.contains(&pair)
    }

    pub fn derive_node_health(&self, node: &NodeRecord) -> (proto::control::NodeHealth, Vec<u64>) {
        use proto::control::NodeHealth as PbNodeHealth;
        if node.health == ProcessHealth::Stopped {
            return (PbNodeHealth::Stopped, Vec::new());
        }
        let peers: Vec<u64> = self
            .nodes
            .keys()
            .copied()
            .filter(|id| *id != node.node_id)
            .collect();
        let lost: Vec<u64> = peers
            .iter()
            .copied()
            .filter(|p| !self.reachable(node.node_id, *p))
            .collect();
        if lost.is_empty() {
            (PbNodeHealth::Up, Vec::new())
        } else if lost.len() == peers.len() {
            (PbNodeHealth::Isolated, lost)
        } else {
            (PbNodeHealth::Partitioned, lost)
        }
    }

    pub fn cluster_health_unhealthy(&self) -> bool {
        self.current_leader().is_none()
    }

    pub fn record_fault(
        &mut self,
        kind: FaultKind,
        node_id: u64,
        peer_node_id: u64,
        description: impl Into<String>,
    ) {
        self.faults.push_front(FaultEvent {
            at_ms: epoch_ms_now(),
            kind: kind as i32,
            node_id,
            peer_node_id,
            description: description.into(),
        });
        if self.faults.len() > 256 {
            self.faults.pop_back();
        }
    }

    pub fn record_election(&mut self, term: u64, winner: u64, reason: ElectionReason) {
        self.elections.push_front(ElectionEvent {
            at_ms: epoch_ms_now(),
            term,
            winner_node_id: winner,
            reason: reason as i32,
        });
        if self.elections.len() > 64 {
            self.elections.pop_back();
        }
    }

    pub fn allocate_run_id(&mut self) -> String {
        let seq = self.next_run_seq;
        self.next_run_seq += 1;
        format!("run_{seq}_{}", epoch_ms_now())
    }
}

pub fn default_cluster_config() -> ClusterConfig {
    ClusterConfig {
        max_accounts: 1_000_000,
        queue_size: 16_384,
        transaction_count_per_segment: 10_000_000,
        snapshot_frequency: 4,
        replication_poll_ms: 5,
        append_entries_max_bytes: 4 * 1024 * 1024,
    }
}

pub fn canonical_pair(a: u64, b: u64) -> (u64, u64) {
    if a <= b { (a, b) } else { (b, a) }
}

pub fn epoch_ms_now() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    dur.as_millis() as i64
}
