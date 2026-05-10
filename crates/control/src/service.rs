//! `roda.control.v1.Control` service implementation backed by `InMemoryState`.

use std::sync::Arc;

use parking_lot::RwLock;
use proto::control::control_server::Control;
use proto::control::{
    CancelScenarioRequest, CancelScenarioResponse, Capability, ClusterHealth, ClusterMembership,
    ElectionEvent, FaultKind, GetClusterConfigRequest, GetClusterConfigResponse,
    GetClusterSnapshotRequest, GetClusterSnapshotResponse, GetFaultHistoryRequest,
    GetFaultHistoryResponse, GetNodeLogRequest, GetNodeLogResponse, GetRecentElectionsRequest,
    GetRecentElectionsResponse, GetScenarioStatusRequest, GetScenarioStatusResponse,
    GetServerInfoRequest, GetServerInfoResponse, HealPartitionRequest, HealPartitionResponse,
    KillNodeRequest, KillNodeResponse, ListScenarioRunsRequest, ListScenarioRunsResponse,
    NodeInfo, NodeRole, NodeStatus, PartitionPair, PartitionPairRequest, PartitionPairResponse,
    RestartNodeRequest, RestartNodeResponse, RunScenarioRequest, RunScenarioResponse,
    ScenarioRunSummary, ScenarioState, SetNodeCountRequest, SetNodeCountResponse,
    StartNodeRequest, StartNodeResponse, StopNodeRequest, StopNodeResponse,
    UpdateClusterConfigRequest, UpdateClusterConfigResponse,
};
use tonic::{Request, Response, Status};

use crate::state::{InMemoryState, NodeRecord, ProcessHealth, canonical_pair, epoch_ms_now};

#[derive(Clone)]
pub struct ControlService {
    state: Arc<RwLock<InMemoryState>>,
}

impl ControlService {
    pub fn new(state: Arc<RwLock<InMemoryState>>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Control for ControlService {
    async fn get_server_info(
        &self,
        _req: Request<GetServerInfoRequest>,
    ) -> Result<Response<GetServerInfoResponse>, Status> {
        Ok(Response::new(GetServerInfoResponse {
            version: format!("control-{}", env!("CARGO_PKG_VERSION")),
            api_version: 1,
            // Mock supports both abrupt KillNode and pairwise partitions, so
            // both capabilities are advertised.
            capabilities: vec![Capability::Kill as i32, Capability::NetworkPartition as i32],
        }))
    }

    async fn get_cluster_snapshot(
        &self,
        _req: Request<GetClusterSnapshotRequest>,
    ) -> Result<Response<GetClusterSnapshotResponse>, Status> {
        let state = self.state.read();
        let now_ms = epoch_ms_now();
        let leader = state.current_leader().cloned();
        let leader_cluster_commit = leader.as_ref().map(|n| n.cluster_commit_index).unwrap_or(0);

        let nodes: Vec<NodeStatus> = state
            .nodes
            .values()
            .map(|n| {
                let (health, partitioned_peers) = state.derive_node_health(n);
                let is_leader = matches!(n.role, NodeRole::Leader);
                let lag_entries = if is_leader || n.health == ProcessHealth::Stopped {
                    0
                } else {
                    leader_cluster_commit.saturating_sub(n.commit_index)
                };
                let lag_ms = if is_leader || n.health == ProcessHealth::Stopped {
                    0
                } else {
                    (now_ms - n.last_heartbeat_at_ms).max(0)
                };
                NodeStatus {
                    node_id: n.node_id,
                    address: n.address.clone(),
                    role: n.role as i32,
                    current_term: n.current_term,
                    voted_for: n.voted_for.unwrap_or(0),
                    health: health as i32,
                    partitioned_peers,
                    last_heartbeat_at_ms: n.last_heartbeat_at_ms,
                    compute_index: n.compute_index,
                    commit_index: n.commit_index,
                    snapshot_index: n.snapshot_index,
                    cluster_commit_index: n.cluster_commit_index,
                    lag_entries,
                    lag_ms,
                }
            })
            .collect();

        let cluster_health = if state.cluster_health_unhealthy() {
            ClusterHealth::Unhealthy
        } else {
            ClusterHealth::Healthy
        };

        let partitions: Vec<PartitionPair> = state
            .partitions
            .iter()
            .map(|(a, b)| PartitionPair {
                node_a: *a,
                node_b: *b,
            })
            .collect();

        Ok(Response::new(GetClusterSnapshotResponse {
            taken_at_ms: now_ms,
            cluster_health: cluster_health as i32,
            leader_node_id: leader.as_ref().map(|n| n.node_id).unwrap_or(0),
            current_term: leader.map(|n| n.current_term).unwrap_or(0),
            nodes,
            partitions,
        }))
    }

    async fn get_recent_elections(
        &self,
        req: Request<GetRecentElectionsRequest>,
    ) -> Result<Response<GetRecentElectionsResponse>, Status> {
        let limit = req.into_inner().limit.clamp(1, 256) as usize;
        let state = self.state.read();
        let events: Vec<ElectionEvent> = state.elections.iter().take(limit).cloned().collect();
        Ok(Response::new(GetRecentElectionsResponse { events }))
    }

    async fn get_node_log(
        &self,
        req: Request<GetNodeLogRequest>,
    ) -> Result<Response<GetNodeLogResponse>, Status> {
        let r = req.into_inner();
        let state = self.state.read();
        if !state.nodes.contains_key(&r.node_id) {
            return Err(Status::not_found(format!("unknown node {}", r.node_id)));
        }
        // Ledger data lives on the cluster nodes; the control plane's
        // mocked log surface is empty. The UI's node-log panel uses
        // this only for cluster monitoring, not for ledger replay.
        let _ = r.from_index;
        let _ = r.limit;
        Ok(Response::new(GetNodeLogResponse {
            entries: Vec::new(),
            total_count: 0,
            next_from_index: 0,
            oldest_retained_index: 0,
        }))
    }

    async fn get_cluster_config(
        &self,
        _req: Request<GetClusterConfigRequest>,
    ) -> Result<Response<GetClusterConfigResponse>, Status> {
        let state = self.state.read();
        let nodes: Vec<NodeInfo> = state
            .nodes
            .values()
            .map(|n| NodeInfo {
                node_id: n.node_id,
                address: n.address.clone(),
            })
            .collect();
        Ok(Response::new(GetClusterConfigResponse {
            config: Some(state.cluster_config),
            membership: Some(ClusterMembership {
                nodes,
                target_count: state.target_node_count,
            }),
        }))
    }

    async fn update_cluster_config(
        &self,
        req: Request<UpdateClusterConfigRequest>,
    ) -> Result<Response<UpdateClusterConfigResponse>, Status> {
        let cfg = match req.into_inner().config {
            Some(c) => c,
            None => {
                return Ok(Response::new(UpdateClusterConfigResponse {
                    accepted: false,
                    error: "config field is required".into(),
                }));
            }
        };
        if cfg.max_accounts == 0 {
            return Ok(Response::new(UpdateClusterConfigResponse {
                accepted: false,
                error: "max_accounts must be > 0".into(),
            }));
        }
        if cfg.queue_size == 0 {
            return Ok(Response::new(UpdateClusterConfigResponse {
                accepted: false,
                error: "queue_size must be > 0".into(),
            }));
        }
        if cfg.transaction_count_per_segment == 0 {
            return Ok(Response::new(UpdateClusterConfigResponse {
                accepted: false,
                error: "transaction_count_per_segment must be > 0".into(),
            }));
        }
        if cfg.replication_poll_ms == 0 {
            return Ok(Response::new(UpdateClusterConfigResponse {
                accepted: false,
                error: "replication_poll_ms must be > 0".into(),
            }));
        }
        if cfg.append_entries_max_bytes == 0 {
            return Ok(Response::new(UpdateClusterConfigResponse {
                accepted: false,
                error: "append_entries_max_bytes must be > 0".into(),
            }));
        }
        let mut state = self.state.write();
        state.cluster_config = cfg;
        Ok(Response::new(UpdateClusterConfigResponse {
            accepted: true,
            error: String::new(),
        }))
    }

    async fn set_node_count(
        &self,
        req: Request<SetNodeCountRequest>,
    ) -> Result<Response<SetNodeCountResponse>, Status> {
        let target = req.into_inner().target_count;
        if target == 0 {
            return Ok(Response::new(SetNodeCountResponse {
                accepted: false,
                error: "target_count must be >= 1".into(),
                target_count: 0,
                current_count: 0,
            }));
        }
        let mut state = self.state.write();
        state.target_node_count = target;
        // Grow.
        while state.nodes.len() < target as usize {
            let next_id = state
                .nodes
                .keys()
                .copied()
                .max()
                .unwrap_or(0)
                .saturating_add(1);
            state
                .nodes
                .insert(next_id, NodeRecord::fresh(next_id, NodeRole::Follower, 1));
        }
        // Shrink — drop highest non-leader id.
        while state.nodes.len() > target as usize {
            let candidate = state
                .nodes
                .iter()
                .rev()
                .find(|(_, n)| !matches!(n.role, NodeRole::Leader))
                .map(|(id, _)| *id)
                .or_else(|| state.nodes.keys().copied().max());
            if let Some(id) = candidate {
                state.nodes.remove(&id);
                let pairs: Vec<(u64, u64)> = state
                    .partitions
                    .iter()
                    .copied()
                    .filter(|(a, b)| *a == id || *b == id)
                    .collect();
                for p in pairs {
                    state.partitions.remove(&p);
                }
            } else {
                break;
            }
        }
        if target < 3 {
            state.record_fault(
                FaultKind::Unspecified,
                0,
                0,
                format!("target_count={target}: consensus may not work as expected"),
            );
        }
        let current = state.nodes.len() as u32;
        Ok(Response::new(SetNodeCountResponse {
            accepted: true,
            error: String::new(),
            target_count: target,
            current_count: current,
        }))
    }

    async fn stop_node(
        &self,
        req: Request<StopNodeRequest>,
    ) -> Result<Response<StopNodeResponse>, Status> {
        let id = req.into_inner().node_id;
        let mut state = self.state.write();
        let res = match state.nodes.get_mut(&id) {
            None => StopNodeResponse {
                accepted: false,
                error: format!("unknown node {id}"),
            },
            Some(n) if n.health == ProcessHealth::Stopped => StopNodeResponse {
                accepted: false,
                error: "already stopped".into(),
            },
            Some(n) => {
                n.health = ProcessHealth::Stopped;
                n.role = NodeRole::Follower;
                n.voted_for = None;
                StopNodeResponse {
                    accepted: true,
                    error: String::new(),
                }
            }
        };
        if res.accepted {
            state.record_fault(FaultKind::Stop, id, 0, format!("Stopped node {id}"));
        }
        Ok(Response::new(res))
    }

    async fn start_node(
        &self,
        req: Request<StartNodeRequest>,
    ) -> Result<Response<StartNodeResponse>, Status> {
        let id = req.into_inner().node_id;
        let mut state = self.state.write();
        let res = match state.nodes.get_mut(&id) {
            None => StartNodeResponse {
                accepted: false,
                error: format!("unknown node {id}"),
            },
            Some(n) if n.health == ProcessHealth::Up => StartNodeResponse {
                accepted: false,
                error: "node is not stopped".into(),
            },
            Some(n) => {
                n.health = ProcessHealth::Up;
                n.role = NodeRole::Follower;
                n.voted_for = None;
                n.last_heartbeat_at_ms = epoch_ms_now();
                StartNodeResponse {
                    accepted: true,
                    error: String::new(),
                }
            }
        };
        if res.accepted {
            state.record_fault(FaultKind::Start, id, 0, format!("Started node {id}"));
        }
        Ok(Response::new(res))
    }

    async fn kill_node(
        &self,
        req: Request<KillNodeRequest>,
    ) -> Result<Response<KillNodeResponse>, Status> {
        // Mock conflates Kill with Stop (no real process to abruptly terminate).
        // Recorded in fault history as Kill so the UI can distinguish.
        let id = req.into_inner().node_id;
        let mut state = self.state.write();
        let res = match state.nodes.get_mut(&id) {
            None => KillNodeResponse {
                accepted: false,
                error: format!("unknown node {id}"),
            },
            Some(n) => {
                n.health = ProcessHealth::Stopped;
                n.role = NodeRole::Follower;
                n.voted_for = None;
                KillNodeResponse {
                    accepted: true,
                    error: String::new(),
                }
            }
        };
        if res.accepted {
            state.record_fault(FaultKind::Kill, id, 0, format!("Killed node {id}"));
        }
        Ok(Response::new(res))
    }

    async fn restart_node(
        &self,
        req: Request<RestartNodeRequest>,
    ) -> Result<Response<RestartNodeResponse>, Status> {
        let id = req.into_inner().node_id;
        let mut state = self.state.write();
        let res = match state.nodes.get_mut(&id) {
            None => RestartNodeResponse {
                accepted: false,
                error: format!("unknown node {id}"),
            },
            Some(n) => {
                n.health = ProcessHealth::Up;
                n.role = NodeRole::Follower;
                n.voted_for = None;
                n.last_heartbeat_at_ms = epoch_ms_now();
                RestartNodeResponse {
                    accepted: true,
                    error: String::new(),
                }
            }
        };
        if res.accepted {
            state.record_fault(FaultKind::Restart, id, 0, format!("Restarted node {id}"));
        }
        Ok(Response::new(res))
    }

    async fn partition_pair(
        &self,
        req: Request<PartitionPairRequest>,
    ) -> Result<Response<PartitionPairResponse>, Status> {
        let r = req.into_inner();
        if r.node_a == r.node_b {
            return Ok(Response::new(PartitionPairResponse {
                accepted: false,
                error: "cannot partition a node from itself".into(),
            }));
        }
        let mut state = self.state.write();
        if !state.nodes.contains_key(&r.node_a) || !state.nodes.contains_key(&r.node_b) {
            return Ok(Response::new(PartitionPairResponse {
                accepted: false,
                error: "unknown node".into(),
            }));
        }
        let pair = canonical_pair(r.node_a, r.node_b);
        state.partitions.insert(pair);
        state.record_fault(
            FaultKind::Partition,
            r.node_a,
            r.node_b,
            format!("Partitioned n{} ⇎ n{}", pair.0, pair.1),
        );
        Ok(Response::new(PartitionPairResponse {
            accepted: true,
            error: String::new(),
        }))
    }

    async fn heal_partition(
        &self,
        req: Request<HealPartitionRequest>,
    ) -> Result<Response<HealPartitionResponse>, Status> {
        let r = req.into_inner();
        let mut state = self.state.write();
        let pair = canonical_pair(r.node_a, r.node_b);
        let removed = state.partitions.remove(&pair);
        if removed {
            state.record_fault(
                FaultKind::Heal,
                r.node_a,
                r.node_b,
                format!("Healed n{} ↔ n{}", pair.0, pair.1),
            );
        }
        Ok(Response::new(HealPartitionResponse {
            accepted: true,
            error: String::new(),
        }))
    }

    async fn get_fault_history(
        &self,
        req: Request<GetFaultHistoryRequest>,
    ) -> Result<Response<GetFaultHistoryResponse>, Status> {
        let limit = req.into_inner().limit;
        let limit = if limit == 0 { 64 } else { limit.min(1000) } as usize;
        let state = self.state.read();
        let events = state.faults.iter().take(limit).cloned().collect();
        Ok(Response::new(GetFaultHistoryResponse { events }))
    }

    async fn run_scenario(
        &self,
        req: Request<RunScenarioRequest>,
    ) -> Result<Response<RunScenarioResponse>, Status> {
        let scenario = req
            .into_inner()
            .scenario
            .ok_or_else(|| Status::invalid_argument("scenario is required"))?;
        let mut state = self.state.write();
        let run_id = state.allocate_run_id();
        let now = std::time::Instant::now();
        let started_at_ms = epoch_ms_now();
        state.scenario_runs.insert(
            run_id.clone(),
            crate::state::RunRecord {
                run_id: run_id.clone(),
                scenario,
                state: ScenarioState::Running,
                started_at: now,
                started_at_ms,
                ended_at: None,
                ended_at_ms: 0,
                progress_pct: 0,
                ops_submitted: 0,
                ops_succeeded: 0,
                ops_failed: 0,
                step_index: 0,
                step_started_at: now,
                step_ops_emitted: 0,
                cancel_requested: false,
                error: String::new(),
                recent_steps: Default::default(),
            },
        );
        Ok(Response::new(RunScenarioResponse {
            run_id,
            started_at_ms,
        }))
    }

    async fn get_scenario_status(
        &self,
        req: Request<GetScenarioStatusRequest>,
    ) -> Result<Response<GetScenarioStatusResponse>, Status> {
        let run_id = req.into_inner().run_id;
        let state = self.state.read();
        match state.scenario_runs.get(&run_id) {
            None => Err(Status::not_found(format!("unknown run_id {run_id}"))),
            Some(r) => Ok(Response::new(GetScenarioStatusResponse {
                run_id: r.run_id.clone(),
                state: r.state as i32,
                progress_pct: r.progress_pct,
                ops_submitted: r.ops_submitted,
                ops_succeeded: r.ops_succeeded,
                ops_failed: r.ops_failed,
                latency_p50_ms: 0,
                latency_p99_ms: 0,
                started_at_ms: r.started_at_ms,
                ended_at_ms: r.ended_at_ms,
                error: r.error.clone(),
                recent_steps: r.recent_steps.iter().cloned().collect(),
            })),
        }
    }

    async fn cancel_scenario(
        &self,
        req: Request<CancelScenarioRequest>,
    ) -> Result<Response<CancelScenarioResponse>, Status> {
        let run_id = req.into_inner().run_id;
        let mut state = self.state.write();
        match state.scenario_runs.get_mut(&run_id) {
            None => Ok(Response::new(CancelScenarioResponse {
                accepted: false,
                error: format!("unknown run_id {run_id}"),
            })),
            Some(r) if r.state != ScenarioState::Running => {
                Ok(Response::new(CancelScenarioResponse {
                    accepted: false,
                    error: format!("scenario is {:?}", r.state),
                }))
            }
            Some(r) => {
                r.cancel_requested = true;
                Ok(Response::new(CancelScenarioResponse {
                    accepted: true,
                    error: String::new(),
                }))
            }
        }
    }

    async fn list_scenario_runs(
        &self,
        req: Request<ListScenarioRunsRequest>,
    ) -> Result<Response<ListScenarioRunsResponse>, Status> {
        let limit = req.into_inner().limit;
        let limit = if limit == 0 { 32 } else { limit.min(256) } as usize;
        let state = self.state.read();
        let mut runs: Vec<_> = state.scenario_runs.values().collect();
        runs.sort_by_key(|r| std::cmp::Reverse(r.started_at_ms));
        let runs = runs
            .into_iter()
            .take(limit)
            .map(|r| ScenarioRunSummary {
                run_id: r.run_id.clone(),
                scenario_name: r.scenario.name.clone(),
                state: r.state as i32,
                started_at_ms: r.started_at_ms,
                ended_at_ms: r.ended_at_ms,
                ops_submitted: r.ops_submitted,
                ops_succeeded: r.ops_succeeded,
                ops_failed: r.ops_failed,
            })
            .collect();
        Ok(Response::new(ListScenarioRunsResponse { runs }))
    }
}
