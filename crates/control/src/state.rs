//! Central in-memory state for the control mock.
//!
//! All mutations and reads go through the same `parking_lot::RwLock`. The
//! `service` module takes write or read locks per RPC; the `background`
//! task takes a write lock once per tick. Lock granularity is intentionally
//! coarse — for a CRUD mock the simplicity is worth more than the throughput.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use proto::control::{
    ClusterConfig, Deposit, ElectionEvent, ElectionReason, FaultEvent, FaultKind, Function,
    NodeHealth as PbNodeHealth, NodeRole, Scenario, ScenarioState as PbScenarioState,
    TransactionStatus as PbTransactionStatus, Transfer, Withdrawal, submit_operation_request,
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
}

#[derive(Clone, Debug)]
pub enum SubmittedOp {
    Deposit {
        account: u64,
        amount: u64,
        user_ref: u64,
    },
    Withdrawal {
        account: u64,
        amount: u64,
        user_ref: u64,
    },
    Transfer {
        from: u64,
        to: u64,
        amount: u64,
        user_ref: u64,
    },
    Function {
        name: String,
        params: [i64; 8],
        user_ref: u64,
    },
}

#[derive(Clone, Debug)]
pub struct TxRecord {
    pub tx_id: u64,
    pub op: SubmittedOp,
    pub status: PbTransactionStatus,
    pub fail_reason: u32,
    pub submitted_at: Instant,
    pub computed_at: Option<Instant>,
    pub committed_at: Option<Instant>,
    pub snapshot_at: Option<Instant>,
}

#[derive(Clone, Debug)]
pub struct FunctionRecord {
    pub name: String,
    pub binary: Vec<u8>,
    pub deployed_at: Instant,
    pub version: u32,
    pub crc32c: u32,
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
    pub transactions: BTreeMap<u64, TxRecord>,
    pub accounts: HashMap<u64, i64>,
    pub functions: BTreeMap<String, FunctionRecord>,
    pub scenario_runs: BTreeMap<String, RunRecord>,
    pub next_tx_id: u64,
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

        // Seed accounts that the demo defaults to.
        let mut accounts = HashMap::new();
        accounts.insert(1, 1_000_000);
        accounts.insert(2, 500_000);
        accounts.insert(3, 250_000);
        accounts.insert(99, 0);

        Self {
            nodes,
            target_node_count: count,
            cluster_config: default_cluster_config(),
            partitions: BTreeSet::new(),
            elections,
            faults: VecDeque::with_capacity(256),
            transactions: BTreeMap::new(),
            accounts,
            functions: BTreeMap::new(),
            scenario_runs: BTreeMap::new(),
            next_tx_id: 1,
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

    pub fn derive_node_health(&self, node: &NodeRecord) -> (PbNodeHealth, Vec<u64>) {
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

    pub fn allocate_tx_id(&mut self) -> u64 {
        let id = self.next_tx_id;
        self.next_tx_id += 1;
        id
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

/// Convert a proto `submit_operation_request::Operation` into the internal
/// `SubmittedOp`. Used by both the service (for direct submissions) and the
/// background scenario runner.
pub fn parse_op(o: submit_operation_request::Operation) -> SubmittedOp {
    match o {
        submit_operation_request::Operation::Deposit(Deposit {
            account,
            amount,
            user_ref,
        }) => SubmittedOp::Deposit {
            account,
            amount,
            user_ref,
        },
        submit_operation_request::Operation::Withdrawal(Withdrawal {
            account,
            amount,
            user_ref,
        }) => SubmittedOp::Withdrawal {
            account,
            amount,
            user_ref,
        },
        submit_operation_request::Operation::Transfer(Transfer {
            from,
            to,
            amount,
            user_ref,
        }) => SubmittedOp::Transfer {
            from,
            to,
            amount,
            user_ref,
        },
        submit_operation_request::Operation::Function(Function {
            name,
            params,
            user_ref,
        }) => {
            let mut p = [0i64; 8];
            for (i, v) in params.into_iter().take(8).enumerate() {
                p[i] = v;
            }
            SubmittedOp::Function {
                name,
                params: p,
                user_ref,
            }
        }
    }
}
