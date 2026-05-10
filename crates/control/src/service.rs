//! `roda.control.v1.Control` service implementation backed by a real
//! cluster.
//!
//! Each RPC routes to one of three places:
//! - [`ClusterHandle`] for live cluster reads (snapshot, pipeline,
//!   balances, txs) and provisioner-driven mutations (config update,
//!   set node count, fault injection).
//! - [`EventStore`] for ephemeral history (faults, scenario runs,
//!   recent submissions) — none of this is produced by the cluster
//!   itself.
//! - The seed scenario catalogue in `testing::scenarios` for
//!   `RunScenario`, looked up by name.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;

use proto::control::control_server::Control;
use proto::control::{
    AvailableScenario, CancelScenarioRequest, CancelScenarioResponse, Capability, ClusterHealth,
    ClusterMembership, ElectionEvent, FaultEvent, FaultKind, FunctionEntry, FunctionListUpdate,
    GetClusterConfigRequest, GetClusterConfigResponse, GetClusterSnapshotRequest,
    GetClusterSnapshotResponse, GetFaultHistoryRequest, GetFaultHistoryResponse, GetNodeLogRequest,
    GetNodeLogResponse, GetNodeWalLogRequest, GetNodeWalLogResponse, GetRecentElectionsRequest,
    GetRecentElectionsResponse, GetScenarioStatusRequest, GetScenarioStatusResponse,
    GetServerInfoRequest, GetServerInfoResponse, HealPartitionRequest, HealPartitionResponse,
    KillNodeRequest, KillNodeResponse, ListAvailableScenariosRequest,
    ListAvailableScenariosResponse, ListScenarioRunsRequest, ListScenarioRunsResponse, NodeInfo,
    NodeRole, NodeStatus, PartitionPair as PbPartitionPair, PartitionPairRequest,
    PartitionPairResponse, ResetClusterRequest, ResetClusterResponse, RestartNodeRequest,
    RestartNodeResponse, RunScenarioRequest, RunScenarioResponse, ScenarioCategory,
    ScenarioRunSummary, ScenarioState, SetNodeCountRequest, SetNodeCountResponse, StartNodeRequest,
    StartNodeResponse, StopNodeRequest, StopNodeResponse, UpdateClusterConfigRequest,
    UpdateClusterConfigResponse, WatchClusterSnapshotRequest, WatchFunctionsRequest,
};
use proto::ledger as proto_ledger;
use tonic::{Request, Response, Status};
use tracing::warn;

use crate::cluster_handle::ClusterHandle;
use crate::event_store::{EventStore, ScenarioRunRecord, epoch_ms_now};
use crate::provisioner::Provisioner;
use crate::runner::{MetricsCollector, ScenarioRunner};

#[derive(Clone)]
pub struct ControlService {
    handle: Arc<ClusterHandle>,
    events: Arc<EventStore>,
}

impl ControlService {
    pub fn new(handle: Arc<ClusterHandle>, events: Arc<EventStore>) -> Self {
        Self { handle, events }
    }
}

/// Helper: build a snapshot from the current cluster state.
/// Extracted so the streaming RPCs don't have to re-derive the same logic.
async fn build_snapshot(svc: &ControlService) -> GetClusterSnapshotResponse {
    // Reuse the unary handler so streaming and one-shot stay aligned.
    // Wrapping in a fake Request is cheap and keeps logic in one place.
    let req = Request::new(GetClusterSnapshotRequest {});
    match svc.get_cluster_snapshot(req).await {
        Ok(resp) => resp.into_inner(),
        Err(_) => GetClusterSnapshotResponse {
            taken_at_ms: 0,
            cluster_health: ClusterHealth::Unhealthy as i32,
            leader_node_id: 0,
            current_term: 0,
            nodes: Vec::new(),
            partitions: Vec::new(),
        },
    }
}

async fn build_function_list(svc: &ControlService) -> FunctionListUpdate {
    match svc.handle.client().list_functions().await {
        Ok(infos) => FunctionListUpdate {
            functions: infos
                .into_iter()
                .map(|f| FunctionEntry {
                    name: f.name,
                    version: f.version as u32,
                    crc32c: f.crc32c,
                })
                .collect(),
        },
        Err(_) => FunctionListUpdate {
            functions: Vec::new(),
        },
    }
}

#[tonic::async_trait]
impl Control for ControlService {
    type WatchClusterSnapshotStream =
        Pin<Box<dyn Stream<Item = Result<GetClusterSnapshotResponse, Status>> + Send>>;
    type WatchFunctionsStream =
        Pin<Box<dyn Stream<Item = Result<FunctionListUpdate, Status>> + Send>>;
    async fn get_server_info(
        &self,
        _req: Request<GetServerInfoRequest>,
    ) -> Result<Response<GetServerInfoResponse>, Status> {
        // ProcessProvisioner has Kill but not network partition (no
        // OS-level network control today).
        Ok(Response::new(GetServerInfoResponse {
            version: format!("control-real-{}", env!("CARGO_PKG_VERSION")),
            api_version: 1,
            capabilities: vec![Capability::Kill as i32],
        }))
    }

    async fn get_cluster_snapshot(
        &self,
        _req: Request<GetClusterSnapshotRequest>,
    ) -> Result<Response<GetClusterSnapshotResponse>, Status> {
        let now_ms = epoch_ms_now();
        let client = self.handle.client();
        let n = client.node_count();
        let addrs = self.handle.node_addrs();

        let mut nodes: Vec<NodeStatus> = Vec::with_capacity(n);
        let mut leader_id: u64 = 0;
        let mut leader_term: u64 = 0;
        let mut max_term: u64 = 0;
        let mut max_cluster_commit: u64 = 0;

        for i in 0..n {
            let node_id = (i as u64) + 1;
            let address = addrs.get(i).cloned().unwrap_or_default();
            match client.node(i).get_pipeline_index().await {
                Ok(pi) => {
                    if pi.is_leader {
                        leader_id = node_id;
                        leader_term = pi.term;
                    }
                    if pi.term > max_term {
                        max_term = pi.term;
                    }
                    if pi.cluster_commit > max_cluster_commit {
                        max_cluster_commit = pi.cluster_commit;
                    }
                    nodes.push(NodeStatus {
                        node_id,
                        address,
                        role: if pi.is_leader {
                            NodeRole::Leader as i32
                        } else {
                            NodeRole::Follower as i32
                        },
                        current_term: pi.term,
                        voted_for: 0,
                        health: proto::control::NodeHealth::Up as i32,
                        partitioned_peers: Vec::new(),
                        last_heartbeat_at_ms: now_ms,
                        compute_index: pi.compute,
                        commit_index: pi.commit,
                        snapshot_index: pi.snapshot,
                        cluster_commit_index: pi.cluster_commit,
                        // lag fields filled in below once we know the
                        // cluster watermark.
                        lag_entries: 0,
                        lag_ms: 0,
                    });
                }
                Err(_) => {
                    nodes.push(NodeStatus {
                        node_id,
                        address,
                        role: NodeRole::Follower as i32,
                        current_term: 0,
                        voted_for: 0,
                        health: proto::control::NodeHealth::Stopped as i32,
                        partitioned_peers: Vec::new(),
                        last_heartbeat_at_ms: now_ms,
                        compute_index: 0,
                        commit_index: 0,
                        snapshot_index: 0,
                        cluster_commit_index: 0,
                        lag_entries: 0,
                        lag_ms: 0,
                    });
                }
            }
        }

        // Fill in lag entries against the cluster watermark.
        for ns in &mut nodes {
            if ns.health == proto::control::NodeHealth::Up as i32
                && ns.role != NodeRole::Leader as i32
            {
                ns.lag_entries = max_cluster_commit.saturating_sub(ns.commit_index);
            }
        }

        let alive = nodes
            .iter()
            .filter(|n| n.health == proto::control::NodeHealth::Up as i32)
            .count();
        let cluster_health = derive_cluster_health(leader_id, alive, nodes.len());

        // Cluster term = leader's term when there is one, else the
        // highest term any node reported (covers mid-election states).
        let current_term = if leader_term > 0 {
            leader_term
        } else {
            max_term
        };
        Ok(Response::new(GetClusterSnapshotResponse {
            taken_at_ms: now_ms,
            cluster_health: cluster_health as i32,
            leader_node_id: leader_id,
            current_term,
            nodes,
            partitions: Vec::new(),
        }))
    }

    async fn watch_cluster_snapshot(
        &self,
        req: Request<WatchClusterSnapshotRequest>,
    ) -> Result<Response<Self::WatchClusterSnapshotStream>, Status> {
        let interval_ms = match req.into_inner().interval_ms {
            0 => 250,
            n => n.clamp(50, 5_000),
        };
        let (tx, rx) = mpsc::channel::<Result<GetClusterSnapshotResponse, Status>>(8);
        let svc = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms as u64));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let snapshot = build_snapshot(&svc).await;
                if tx.send(Ok(snapshot)).await.is_err() {
                    break; // client dropped the stream
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn watch_functions(
        &self,
        req: Request<WatchFunctionsRequest>,
    ) -> Result<Response<Self::WatchFunctionsStream>, Status> {
        let interval_ms = match req.into_inner().interval_ms {
            0 => 1_000,
            n => n.clamp(200, 10_000),
        };
        let (tx, rx) = mpsc::channel::<Result<FunctionListUpdate, Status>>(4);
        let svc = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms as u64));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Track previous emission so we only push on change after the
            // initial frame. This avoids the polling-style flicker while
            // still serving as a heartbeat.
            let mut last_signature: Option<Vec<(String, u32, u32)>> = None;
            loop {
                ticker.tick().await;
                let resp = build_function_list(&svc).await;
                let sig: Vec<(String, u32, u32)> = resp
                    .functions
                    .iter()
                    .map(|f| (f.name.clone(), f.version, f.crc32c))
                    .collect();
                // Suppress transient empties. When `list_functions`
                // momentarily returns an empty list during normal
                // cluster activity (e.g. a leader hop mid-registration),
                // don't propagate that — the operator would see deployed
                // entries flicker out and back. Only emit empty if the
                // previous emission was also empty (or this is the
                // first frame against a genuinely empty cluster).
                if sig.is_empty() && last_signature.as_ref().is_some_and(|prev| !prev.is_empty()) {
                    continue;
                }
                let changed = last_signature.as_ref() != Some(&sig);
                if changed || last_signature.is_none() {
                    last_signature = Some(sig);
                    if tx.send(Ok(resp)).await.is_err() {
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_recent_elections(
        &self,
        req: Request<GetRecentElectionsRequest>,
    ) -> Result<Response<GetRecentElectionsResponse>, Status> {
        const DEFAULT_LIMIT: u32 = 16;
        const MAX_LIMIT: u32 = 256;
        let r = req.into_inner();
        let limit = if r.limit == 0 {
            DEFAULT_LIMIT
        } else {
            r.limit.min(MAX_LIMIT)
        };

        // Pick a node to ask: prefer the leader (its vote.log records
        // identify each prior term's winner from its own POV), fall back
        // to any reachable node so the timeline isn't blank during an
        // ongoing election.
        let client = self.handle.client();
        let n = client.node_count();
        let mut leader_idx: Option<usize> = None;
        let mut fallback_idx: Option<usize> = None;
        for i in 0..n {
            match client.node(i).get_pipeline_index().await {
                Ok(pi) => {
                    if pi.is_leader && leader_idx.is_none() {
                        leader_idx = Some(i);
                    }
                    if fallback_idx.is_none() {
                        fallback_idx = Some(i);
                    }
                }
                Err(_) => continue,
            }
        }
        let target = match leader_idx.or(fallback_idx) {
            Some(i) => i,
            None => {
                return Ok(Response::new(GetRecentElectionsResponse {
                    events: Vec::new(),
                }));
            }
        };

        // Pull all terms (paginate forward from the most-recent boundary
        // when the cluster has more than `limit` records).
        let mut all: Vec<proto_ledger::TermInfo> = Vec::new();
        let mut from_term: u64 = 0;
        while let Ok(page) = client.node(target).get_terms(from_term, MAX_LIMIT).await {
            all.extend(page.terms);
            if page.next_term == 0 {
                break;
            }
            from_term = page.next_term;
            if all.len() >= MAX_LIMIT as usize {
                break;
            }
        }

        // Newest first, capped to caller's limit.
        all.sort_by_key(|b| std::cmp::Reverse(b.term));
        all.truncate(limit as usize);

        let events: Vec<ElectionEvent> = all
            .into_iter()
            .map(|ti| ElectionEvent {
                at_ms: 0,
                term: ti.term,
                winner_node_id: ti.voted_for,
                reason: proto::control::ElectionReason::Unspecified as i32,
                voted_for: ti.voted_for,
                start_tx_id: ti.start_tx_id,
                has_term_record: ti.has_term_record,
                has_vote_record: ti.has_vote_record,
            })
            .collect();
        Ok(Response::new(GetRecentElectionsResponse { events }))
    }

    async fn get_node_log(
        &self,
        _req: Request<GetNodeLogRequest>,
    ) -> Result<Response<GetNodeLogResponse>, Status> {
        // Cluster doesn't expose raft-log fetch over the wire yet.
        Ok(Response::new(GetNodeLogResponse {
            entries: Vec::new(),
            total_count: 0,
            next_from_index: 0,
            oldest_retained_index: 0,
        }))
    }

    async fn get_node_wal_log(
        &self,
        req: Request<GetNodeWalLogRequest>,
    ) -> Result<Response<GetNodeWalLogResponse>, Status> {
        let r = req.into_inner();
        let idx = self
            .handle
            .idx_for_node_id(r.node_id)
            .ok_or_else(|| Status::not_found(format!("unknown node_id={}", r.node_id)))?;
        let addrs = self.handle.node_addrs();
        let url = addrs
            .get(idx)
            .ok_or_else(|| Status::internal("node index out of range"))?
            .clone();
        let url = if url.starts_with("http://") || url.starts_with("https://") {
            url
        } else {
            format!("http://{}", url)
        };
        let client = client::NodeClient::connect_url(&url)
            .await
            .map_err(|e| Status::unavailable(format!("connect {}: {}", url, e)))?;
        let page = client.get_log(r.from_tx_id, r.to_tx_id, r.limit).await?;
        Ok(Response::new(GetNodeWalLogResponse {
            records: page
                .records
                .into_iter()
                .map(ledger_record_to_control)
                .collect(),
            next_tx_id: page.next_tx_id,
            last_commit_tx_id: page.last_commit_tx_id,
        }))
    }

    async fn get_cluster_config(
        &self,
        _req: Request<GetClusterConfigRequest>,
    ) -> Result<Response<GetClusterConfigResponse>, Status> {
        let cfg = *self.handle.config();
        let count = self.handle.node_count();
        let addrs = self.handle.node_addrs();
        let nodes: Vec<NodeInfo> = addrs
            .iter()
            .enumerate()
            .map(|(i, addr)| NodeInfo {
                node_id: (i as u64) + 1,
                address: addr.clone(),
            })
            .collect();
        Ok(Response::new(GetClusterConfigResponse {
            config: Some(cfg),
            membership: Some(ClusterMembership {
                nodes,
                target_count: count,
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
        if let Some(err) = validate_cluster_config(&cfg) {
            return Ok(Response::new(UpdateClusterConfigResponse {
                accepted: false,
                error: err,
            }));
        }
        match self.handle.reprovision(Some(cfg), None).await {
            Ok(()) => Ok(Response::new(UpdateClusterConfigResponse {
                accepted: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(UpdateClusterConfigResponse {
                accepted: false,
                error: format!("reprovision failed: {e}"),
            })),
        }
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
                current_count: self.handle.node_count(),
            }));
        }
        match self.handle.reprovision(None, Some(target)).await {
            Ok(()) => Ok(Response::new(SetNodeCountResponse {
                accepted: true,
                error: String::new(),
                target_count: target,
                current_count: self.handle.node_count(),
            })),
            Err(e) => Ok(Response::new(SetNodeCountResponse {
                accepted: false,
                error: format!("reprovision failed: {e}"),
                target_count: target,
                current_count: self.handle.node_count(),
            })),
        }
    }

    async fn reset_cluster(
        &self,
        _req: Request<ResetClusterRequest>,
    ) -> Result<Response<ResetClusterResponse>, Status> {
        // Force the provisioner past its fast path: kill children, wipe
        // the cluster temp dir, drop cached config. After this, calling
        // `reprovision` with the current (unchanged) config goes through
        // the cold-provision path and brings up a fresh cluster with new
        // data dirs. Then clear the control plane's session state.
        self.handle
            .process_provisioner()
            .reset_for_full_reprovision();
        match self.handle.reprovision(None, None).await {
            Ok(()) => {
                self.events.reset();
                Ok(Response::new(ResetClusterResponse {
                    accepted: true,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(ResetClusterResponse {
                accepted: false,
                error: format!("reset failed: {e}"),
            })),
        }
    }

    async fn stop_node(
        &self,
        req: Request<StopNodeRequest>,
    ) -> Result<Response<StopNodeResponse>, Status> {
        let id = req.into_inner().node_id;
        match self.handle.idx_for_node_id(id) {
            None => Ok(Response::new(StopNodeResponse {
                accepted: false,
                error: format!("unknown node {id}"),
            })),
            Some(idx) => {
                let provisioner = self.handle.process_provisioner();
                match provisioner.stop_node(idx).await {
                    Ok(()) => {
                        self.events.record_fault(FaultEvent {
                            at_ms: epoch_ms_now(),
                            kind: FaultKind::Stop as i32,
                            node_id: id,
                            peer_node_id: 0,
                            description: format!("Stopped node {id}"),
                        });
                        Ok(Response::new(StopNodeResponse {
                            accepted: true,
                            error: String::new(),
                        }))
                    }
                    Err(e) => Ok(Response::new(StopNodeResponse {
                        accepted: false,
                        error: e.to_string(),
                    })),
                }
            }
        }
    }

    async fn start_node(
        &self,
        req: Request<StartNodeRequest>,
    ) -> Result<Response<StartNodeResponse>, Status> {
        let id = req.into_inner().node_id;
        match self.handle.idx_for_node_id(id) {
            None => Ok(Response::new(StartNodeResponse {
                accepted: false,
                error: format!("unknown node {id}"),
            })),
            Some(idx) => match self.handle.process_provisioner().start_node(idx).await {
                Ok(()) => {
                    self.events.record_fault(FaultEvent {
                        at_ms: epoch_ms_now(),
                        kind: FaultKind::Start as i32,
                        node_id: id,
                        peer_node_id: 0,
                        description: format!("Started node {id}"),
                    });
                    Ok(Response::new(StartNodeResponse {
                        accepted: true,
                        error: String::new(),
                    }))
                }
                Err(e) => Ok(Response::new(StartNodeResponse {
                    accepted: false,
                    error: e.to_string(),
                })),
            },
        }
    }

    async fn kill_node(
        &self,
        req: Request<KillNodeRequest>,
    ) -> Result<Response<KillNodeResponse>, Status> {
        let id = req.into_inner().node_id;
        match self.handle.idx_for_node_id(id) {
            None => Ok(Response::new(KillNodeResponse {
                accepted: false,
                error: format!("unknown node {id}"),
            })),
            Some(idx) => match self.handle.process_provisioner().kill_node(idx).await {
                Ok(()) => {
                    self.events.record_fault(FaultEvent {
                        at_ms: epoch_ms_now(),
                        kind: FaultKind::Kill as i32,
                        node_id: id,
                        peer_node_id: 0,
                        description: format!("Killed node {id}"),
                    });
                    Ok(Response::new(KillNodeResponse {
                        accepted: true,
                        error: String::new(),
                    }))
                }
                Err(e) => Ok(Response::new(KillNodeResponse {
                    accepted: false,
                    error: e.to_string(),
                })),
            },
        }
    }

    async fn restart_node(
        &self,
        req: Request<RestartNodeRequest>,
    ) -> Result<Response<RestartNodeResponse>, Status> {
        let id = req.into_inner().node_id;
        match self.handle.idx_for_node_id(id) {
            None => Ok(Response::new(RestartNodeResponse {
                accepted: false,
                error: format!("unknown node {id}"),
            })),
            Some(idx) => match self.handle.process_provisioner().restart_node(idx).await {
                Ok(()) => {
                    self.events.record_fault(FaultEvent {
                        at_ms: epoch_ms_now(),
                        kind: FaultKind::Restart as i32,
                        node_id: id,
                        peer_node_id: 0,
                        description: format!("Restarted node {id}"),
                    });
                    Ok(Response::new(RestartNodeResponse {
                        accepted: true,
                        error: String::new(),
                    }))
                }
                Err(e) => Ok(Response::new(RestartNodeResponse {
                    accepted: false,
                    error: e.to_string(),
                })),
            },
        }
    }

    async fn partition_pair(
        &self,
        _req: Request<PartitionPairRequest>,
    ) -> Result<Response<PartitionPairResponse>, Status> {
        // ProcessProvisioner can't partition a real network today.
        Err(Status::unimplemented(
            "network partition is not supported by the process provisioner",
        ))
    }

    async fn heal_partition(
        &self,
        _req: Request<HealPartitionRequest>,
    ) -> Result<Response<HealPartitionResponse>, Status> {
        Err(Status::unimplemented(
            "network partition is not supported by the process provisioner",
        ))
    }

    async fn get_fault_history(
        &self,
        req: Request<GetFaultHistoryRequest>,
    ) -> Result<Response<GetFaultHistoryResponse>, Status> {
        let limit = req.into_inner().limit;
        let limit = if limit == 0 { 64 } else { limit.min(1000) } as usize;
        let events = self.events.fault_history(limit);
        Ok(Response::new(GetFaultHistoryResponse { events }))
    }

    async fn list_available_scenarios(
        &self,
        _req: Request<ListAvailableScenariosRequest>,
    ) -> Result<Response<ListAvailableScenariosResponse>, Status> {
        // Hand back the server's built-in catalogue grouped by category.
        // E2E first, then Load — same order as `testing::scenarios::list`.
        let mut scenarios: Vec<AvailableScenario> = Vec::new();
        for s in testing::scenarios::e2e::all() {
            scenarios.push(AvailableScenario {
                name: s.name.clone(),
                description: s.description.clone(),
                category: ScenarioCategory::E2e as i32,
                step_count: s.steps.len() as u32,
            });
        }
        for s in testing::scenarios::load::all() {
            scenarios.push(AvailableScenario {
                name: s.name.clone(),
                description: s.description.clone(),
                category: ScenarioCategory::Load as i32,
                step_count: s.steps.len() as u32,
            });
        }
        Ok(Response::new(ListAvailableScenariosResponse { scenarios }))
    }

    async fn run_scenario(
        &self,
        req: Request<RunScenarioRequest>,
    ) -> Result<Response<RunScenarioResponse>, Status> {
        let scenario_proto = req
            .into_inner()
            .scenario
            .ok_or_else(|| Status::invalid_argument("scenario is required"))?;

        // v1: look up by name from the seed catalogue. Custom step
        // lists from the UI's editor are not yet translated.
        let scenario = lookup_scenario(&scenario_proto.name).ok_or_else(|| {
            Status::not_found(format!(
                "scenario `{}` not found in the seed catalogue",
                scenario_proto.name
            ))
        })?;

        let run_id = self.events.next_run_id();
        let mut record = ScenarioRunRecord::new(run_id.clone(), scenario_proto.name.clone());
        let started_at_ms = record.started_at_epoch_ms;
        let events_handle = self.events.clone();
        let handle = self.handle.clone();
        let runner_run_id = run_id.clone();
        let runner_scenario = scenario.clone();

        // Spawn the run on a background task so the RPC returns the
        // run_id immediately. The task updates the event store with
        // the final state when it finishes.
        let join = tokio::spawn(async move {
            let runner = ScenarioRunner::new(handle.provisioner());
            let metrics = Arc::new(MetricsCollector::new());
            let report = runner
                .run_against_existing(&runner_scenario, (*handle.client()).clone(), metrics)
                .await;
            let elapsed_ms = report.elapsed.as_millis() as i64;
            let throughput = report.metrics.throughput_stats();
            let (state, err) = match report.result {
                Ok(()) => (ScenarioState::Completed, None),
                Err(e) => (ScenarioState::Failed, Some(e.to_string())),
            };
            events_handle.update_run(&runner_run_id, |r| {
                r.state = state;
                r.elapsed_ms = Some(elapsed_ms);
                r.finished_at_epoch_ms = Some(epoch_ms_now());
                r.error_message = err;
                r.ops_total = throughput.ops_total;
                r.handle = None;
            });
        });
        record.handle = Some(join);
        self.events.insert_run(record);

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
        let resp = self.events.get_run(&run_id, |r| GetScenarioStatusResponse {
            run_id: r.run_id.clone(),
            state: r.state as i32,
            progress_pct: 0,
            ops_submitted: r.ops_total,
            ops_succeeded: r.ops_total,
            ops_failed: 0,
            latency_p50_ms: 0,
            latency_p99_ms: 0,
            started_at_ms: r.started_at_epoch_ms,
            ended_at_ms: r.finished_at_epoch_ms.unwrap_or(0),
            error: r.error_message.clone().unwrap_or_default(),
            recent_steps: Vec::new(),
        });
        match resp {
            Some(r) => Ok(Response::new(r)),
            None => Err(Status::not_found(format!("unknown run_id {run_id}"))),
        }
    }

    async fn cancel_scenario(
        &self,
        req: Request<CancelScenarioRequest>,
    ) -> Result<Response<CancelScenarioResponse>, Status> {
        let run_id = req.into_inner().run_id;
        if self.events.cancel_run(&run_id) {
            Ok(Response::new(CancelScenarioResponse {
                accepted: true,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(CancelScenarioResponse {
                accepted: false,
                error: format!("run {run_id} is not running"),
            }))
        }
    }

    async fn list_scenario_runs(
        &self,
        req: Request<ListScenarioRunsRequest>,
    ) -> Result<Response<ListScenarioRunsResponse>, Status> {
        let limit = req.into_inner().limit;
        let limit = if limit == 0 { 32 } else { limit.min(256) } as usize;
        let runs: Vec<ScenarioRunSummary> = self
            .events
            .list_runs()
            .into_iter()
            .take(limit)
            .map(|r| ScenarioRunSummary {
                run_id: r.run_id,
                scenario_name: r.scenario_name,
                state: r.state as i32,
                started_at_ms: r.started_at_epoch_ms,
                ended_at_ms: r.finished_at_epoch_ms.unwrap_or(0),
                ops_submitted: r.ops_total,
                ops_succeeded: r.ops_total,
                ops_failed: 0,
            })
            .collect();
        Ok(Response::new(ListScenarioRunsResponse { runs }))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn validate_cluster_config(cfg: &proto::control::ClusterConfig) -> Option<String> {
    if cfg.max_accounts == 0 {
        return Some("max_accounts must be > 0".into());
    }
    if cfg.transaction_count_per_segment == 0 {
        return Some("transaction_count_per_segment must be > 0".into());
    }
    if cfg.replication_poll_ms == 0 {
        return Some("replication_poll_ms must be > 0".into());
    }
    if cfg.append_entries_max_bytes == 0 {
        return Some("append_entries_max_bytes must be > 0".into());
    }
    None
}

fn ledger_record_to_control(r: proto_ledger::WalLogRecord) -> proto::control::WalLogRecordC {
    use proto::control as c;
    use proto_ledger::wal_log_record::Entry as L;
    let entry = match r.entry {
        Some(L::Metadata(m)) => Some(c::wal_log_record_c::Entry::Metadata(c::WalTxMetadataC {
            tx_id: m.tx_id,
            fail_reason: m.fail_reason,
            sub_item_count: m.sub_item_count,
            crc32c: m.crc32c,
            timestamp: m.timestamp,
            user_ref: m.user_ref,
            tag: m.tag,
        })),
        Some(L::TxEntry(e)) => Some(c::wal_log_record_c::Entry::TxEntry(c::WalTxEntryC {
            tx_id: e.tx_id,
            account_id: e.account_id,
            amount: e.amount,
            kind: e.kind as u32,
            computed_balance: e.computed_balance,
        })),
        Some(L::Link(l)) => Some(c::wal_log_record_c::Entry::Link(c::WalTxLinkC {
            tx_id: l.tx_id,
            to_tx_id: l.to_tx_id,
            kind: l.kind as u32,
        })),
        Some(L::Term(t)) => Some(c::wal_log_record_c::Entry::Term(c::WalTxTermC {
            term: t.term,
            node_id: t.node_id,
            node_count: t.node_count,
            node_voted: t.node_voted,
        })),
        Some(L::FunctionRegistered(f)) => Some(c::wal_log_record_c::Entry::FunctionRegistered(
            c::WalFunctionRegisteredC {
                name: f.name,
                version: f.version,
                crc32c: f.crc32c,
            },
        )),
        Some(L::SegmentHeader(h)) => Some(c::wal_log_record_c::Entry::SegmentHeader(
            c::WalSegmentHeaderC {
                segment_id: h.segment_id,
            },
        )),
        Some(L::SegmentSealed(s)) => Some(c::wal_log_record_c::Entry::SegmentSealed(
            c::WalSegmentSealedC {
                segment_id: s.segment_id,
                last_tx_id: s.last_tx_id,
                record_count: s.record_count,
            },
        )),
        None => None,
    };
    c::WalLogRecordC { entry }
}

/// Look up a scenario by name from the seed catalogues. Mirrors the
/// CLI's `find_scenario` so the server and the CLI surface the same
/// catalogue.
fn lookup_scenario(name: &str) -> Option<testing::scenario::Scenario> {
    let mut all: Vec<testing::scenario::Scenario> = Vec::new();
    all.extend(testing::scenarios::e2e::all());
    all.extend(testing::scenarios::load::all());
    all.into_iter().find(|s| s.name == name)
}

// Silence unused-warning helpers brought in conditionally.
#[allow(dead_code)]
fn _unused_marker(_: PbPartitionPair) {
    warn!("partition pair type referenced for trait bounds only");
}

/// Cluster health rule: stable leader + quorum reachable.
/// `total` is the configured cluster size; quorum is `total/2 + 1`.
/// `leader_id == 0` means no leader observed.
fn derive_cluster_health(leader_id: u64, alive: usize, total: usize) -> ClusterHealth {
    let quorum = total / 2 + 1;
    if leader_id == 0 || alive < quorum {
        ClusterHealth::Unhealthy
    } else {
        ClusterHealth::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn four_node_one_follower_down_is_healthy() {
        // The reported bug: 4-node cluster, 1 follower killed → 3 alive,
        // quorum is 3, leader still present → must be Healthy.
        assert_eq!(derive_cluster_health(2, 3, 4), ClusterHealth::Healthy);
    }

    #[test]
    fn quorum_lost_is_unhealthy() {
        // 4 nodes, only 2 alive (quorum needs 3), even with a leader.
        assert_eq!(derive_cluster_health(2, 2, 4), ClusterHealth::Unhealthy);
    }

    #[test]
    fn no_leader_is_unhealthy_even_with_full_cluster() {
        assert_eq!(derive_cluster_health(0, 4, 4), ClusterHealth::Unhealthy);
    }

    #[test]
    fn three_node_one_down_is_healthy() {
        assert_eq!(derive_cluster_health(1, 2, 3), ClusterHealth::Healthy);
    }

    #[test]
    fn five_node_two_down_is_healthy() {
        assert_eq!(derive_cluster_health(1, 3, 5), ClusterHealth::Healthy);
    }

    #[test]
    fn five_node_three_down_is_unhealthy() {
        assert_eq!(derive_cluster_health(1, 2, 5), ClusterHealth::Unhealthy);
    }

    #[test]
    fn all_healthy_is_healthy() {
        assert_eq!(derive_cluster_health(1, 4, 4), ClusterHealth::Healthy);
    }
}
