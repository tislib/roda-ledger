//! Periodic background tick.
//!
//! Drives:
//!   1. Election fallback: if the cluster has no leader (e.g. operator
//!      stopped the leader), the lowest-id healthy non-isolated node
//!      becomes leader and an `ElectionEvent` is recorded.
//!   2. Scenario runner: for each RUNNING scenario, advances the current
//!      step by elapsed time, accumulating workload counters and
//!      applying fault steps.
//!
//! Ledger data-plane state (transactions, balances, function registry)
//! is **not** maintained here — those operations go through the
//! `LedgerProxy` to real ledger nodes. The scenario runner therefore
//! tracks workload submission as a synthetic counter rather than
//! actually issuing ops; its purpose is to drive the UI's run-history
//! panel and to inject faults at scripted times.
//!
//! Cooperative shutdown via `tokio_util::sync::CancellationToken`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use proto::control::{ElectionReason, FaultKind, NodeRole, ScenarioState};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::state::{InMemoryState, ProcessHealth, canonical_pair, epoch_ms_now};

const TICK_MS: u64 = 250;

pub fn spawn(
    state: Arc<RwLock<InMemoryState>>,
    token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_millis(TICK_MS));
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("background task cancelled, exiting");
                    return;
                }
                _ = tick.tick() => {
                    let mut state = state.write();
                    elect_if_needed(&mut state);
                    drive_scenarios(&mut state);
                }
            }
        }
    })
}

fn elect_if_needed(state: &mut InMemoryState) {
    if state.current_leader().is_some() {
        return;
    }
    // Find lowest-id node that is Up and reachable to a quorum (mock: just Up + not isolated).
    let candidate = state
        .nodes
        .iter()
        .filter(|(_, n)| n.health == ProcessHealth::Up)
        .map(|(id, _)| *id)
        .find(|id| {
            let peers: Vec<u64> = state.nodes.keys().copied().filter(|p| p != id).collect();
            // Reachable to at least one peer (mock relaxation; in real Raft would be quorum).
            peers.iter().any(|p| state.reachable(*id, *p)) || peers.is_empty()
        });
    let new_leader = match candidate {
        Some(id) => id,
        None => return,
    };
    let new_term = state
        .nodes
        .values()
        .map(|n| n.current_term)
        .max()
        .unwrap_or(0)
        + 1;
    if let Some(node) = state.nodes.get_mut(&new_leader) {
        node.role = NodeRole::Leader;
        node.current_term = new_term;
        node.voted_for = Some(new_leader);
        node.last_heartbeat_at_ms = epoch_ms_now();
    }
    for (id, node) in state.nodes.iter_mut() {
        if *id != new_leader && node.health == ProcessHealth::Up {
            node.role = NodeRole::Follower;
            node.current_term = new_term;
            node.voted_for = None;
        }
    }
    state.record_election(new_term, new_leader, ElectionReason::LeaderCrash);
}

fn drive_scenarios(state: &mut InMemoryState) {
    let now = Instant::now();
    let now_ms = epoch_ms_now();
    let run_ids: Vec<String> = state.scenario_runs.keys().cloned().collect();
    for run_id in run_ids {
        let action = {
            let run = match state.scenario_runs.get_mut(&run_id) {
                Some(r) => r,
                None => continue,
            };
            if run.state != ScenarioState::Running {
                continue;
            }
            if run.cancel_requested {
                run.state = ScenarioState::Canceled;
                run.ended_at = Some(now);
                run.ended_at_ms = now_ms;
                continue;
            }
            if run.step_index >= run.scenario.steps.len() {
                run.state = ScenarioState::Completed;
                run.ended_at = Some(now);
                run.ended_at_ms = now_ms;
                run.progress_pct = 100;
                continue;
            }
            decide_scenario_action(run, now)
        };
        apply_scenario_action(state, &run_id, action, now, now_ms);
    }
}

enum ScenarioAction {
    None,
    AdvanceStep(String),
    /// Bump the run's submitted/succeeded counters by `count`. The
    /// proxy is responsible for actual op delivery; the scenario
    /// runner is purely a metrics + fault-step driver.
    EmitOps {
        count: u64,
    },
    Fault {
        kind: FaultKind,
        node_id: u64,
        peer_node_id: u64,
    },
}

fn decide_scenario_action(run: &mut crate::state::RunRecord, now: Instant) -> ScenarioAction {
    let step = match run.scenario.steps.get(run.step_index) {
        Some(s) => s.clone(),
        None => return ScenarioAction::None,
    };
    let elapsed_ms = now.duration_since(run.step_started_at).as_millis() as u64;

    match step.step {
        Some(proto::control::scenario_step::Step::Wait(w)) => {
            if elapsed_ms >= w.duration_ms as u64 {
                ScenarioAction::AdvanceStep(format!("wait {}ms", w.duration_ms))
            } else {
                ScenarioAction::None
            }
        }
        Some(proto::control::scenario_step::Step::Fault(f)) => ScenarioAction::Fault {
            kind: FaultKind::try_from(f.kind).unwrap_or(FaultKind::Unspecified),
            node_id: f.node_id,
            peer_node_id: f.peer_node_id,
        },
        Some(proto::control::scenario_step::Step::SubmitOps(s)) => {
            let target_total = if s.total_ops > 0 {
                let by_rate = if s.rate_ops_s > 0 {
                    (elapsed_ms as f64 / 1000.0 * s.rate_ops_s as f64).floor() as u64
                } else {
                    s.total_ops
                };
                by_rate.min(s.total_ops)
            } else if s.rate_ops_s > 0 {
                (elapsed_ms as f64 / 1000.0 * s.rate_ops_s as f64).floor() as u64
            } else {
                run.step_ops_emitted + 1
            };
            let reached_total = s.total_ops > 0 && run.step_ops_emitted >= s.total_ops;
            let reached_duration = s.duration_ms > 0 && elapsed_ms >= s.duration_ms as u64;
            if reached_total || reached_duration {
                return ScenarioAction::AdvanceStep(format!(
                    "submit {:?} {} ops over {}ms",
                    s.kind(),
                    run.step_ops_emitted,
                    elapsed_ms
                ));
            }
            if run.step_ops_emitted >= target_total {
                return ScenarioAction::None;
            }
            ScenarioAction::EmitOps {
                count: (target_total - run.step_ops_emitted).min(64),
            }
        }
        None => ScenarioAction::AdvanceStep("noop".into()),
    }
}

fn apply_scenario_action(
    state: &mut InMemoryState,
    run_id: &str,
    action: ScenarioAction,
    now: Instant,
    now_ms: i64,
) {
    match action {
        ScenarioAction::None => {}
        ScenarioAction::AdvanceStep(desc) => {
            if let Some(run) = state.scenario_runs.get_mut(run_id) {
                run.recent_steps.push_back(desc);
                while run.recent_steps.len() > 12 {
                    run.recent_steps.pop_front();
                }
                run.step_index += 1;
                run.step_started_at = now;
                run.step_ops_emitted = 0;
                let total = run.scenario.steps.len().max(1) as u64;
                run.progress_pct = ((run.step_index as u64 * 100) / total).min(99) as u32;
            }
        }
        ScenarioAction::EmitOps { count } => {
            let unhealthy = state.cluster_health_unhealthy();
            if let Some(r) = state.scenario_runs.get_mut(run_id) {
                if unhealthy {
                    r.ops_submitted += count;
                    r.ops_failed += count;
                } else {
                    r.ops_submitted += count;
                    r.ops_succeeded += count;
                }
                r.step_ops_emitted += count;
            }
        }
        ScenarioAction::Fault {
            kind,
            node_id,
            peer_node_id,
        } => {
            apply_fault(state, kind, node_id, peer_node_id);
            if let Some(run) = state.scenario_runs.get_mut(run_id) {
                run.recent_steps
                    .push_back(format!("{kind:?} n{node_id} peer={peer_node_id}"));
                while run.recent_steps.len() > 12 {
                    run.recent_steps.pop_front();
                }
                run.step_index += 1;
                run.step_started_at = now;
                run.step_ops_emitted = 0;
                let _ = now_ms;
            }
        }
    }
}

fn apply_fault(state: &mut InMemoryState, kind: FaultKind, node_id: u64, peer_node_id: u64) {
    match kind {
        FaultKind::Stop => {
            if let Some(n) = state.nodes.get_mut(&node_id) {
                n.health = ProcessHealth::Stopped;
                n.role = NodeRole::Follower;
                n.voted_for = None;
            }
            state.record_fault(
                kind,
                node_id,
                0,
                format!("Stopped node {node_id} (scenario)"),
            );
        }
        FaultKind::Start => {
            if let Some(n) = state.nodes.get_mut(&node_id) {
                n.health = ProcessHealth::Up;
                n.role = NodeRole::Follower;
                n.voted_for = None;
                n.last_heartbeat_at_ms = epoch_ms_now();
            }
            state.record_fault(
                kind,
                node_id,
                0,
                format!("Started node {node_id} (scenario)"),
            );
        }
        FaultKind::Restart => {
            if let Some(n) = state.nodes.get_mut(&node_id) {
                n.health = ProcessHealth::Up;
                n.role = NodeRole::Follower;
                n.voted_for = None;
                n.last_heartbeat_at_ms = epoch_ms_now();
            }
            state.record_fault(
                kind,
                node_id,
                0,
                format!("Restarted node {node_id} (scenario)"),
            );
        }
        FaultKind::Partition => {
            if peer_node_id != 0 && peer_node_id != node_id {
                state
                    .partitions
                    .insert(canonical_pair(node_id, peer_node_id));
                state.record_fault(
                    kind,
                    node_id,
                    peer_node_id,
                    format!("Partition n{node_id} ⇎ n{peer_node_id} (scenario)"),
                );
            }
        }
        FaultKind::Heal => {
            state
                .partitions
                .remove(&canonical_pair(node_id, peer_node_id));
            state.record_fault(
                kind,
                node_id,
                peer_node_id,
                format!("Heal n{node_id} ↔ n{peer_node_id} (scenario)"),
            );
        }
        FaultKind::Kill => {
            // Mock conflates Kill with Stop; recorded as Kill so the UI can
            // distinguish the two in fault history.
            if let Some(n) = state.nodes.get_mut(&node_id) {
                n.health = ProcessHealth::Stopped;
                n.role = NodeRole::Follower;
                n.voted_for = None;
            }
            state.record_fault(
                kind,
                node_id,
                0,
                format!("Killed node {node_id} (scenario)"),
            );
        }
        FaultKind::Unspecified => {
            warn!("ScenarioStep emitted an Unspecified fault — ignored");
        }
    }
}
