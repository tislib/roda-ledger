//! Periodic background tick.
//!
//! Drives:
//!   1. Pipeline watermarks: each transaction transitions Pending → Computed → Committed → OnSnapshot at staggered timing so the three pipeline indices on the Dashboard advance independently.
//!   2. Election fallback: if the cluster has no leader (e.g. operator stopped the leader), the lowest-id healthy non-isolated node becomes leader and an `ElectionEvent` is recorded.
//!   3. Scenario runner: for each RUNNING scenario, advances the current step by elapsed time, emitting ops / faults / waits.
//!
//! Cooperative shutdown via `tokio_util::sync::CancellationToken`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use proto::control::{
    ElectionReason, FaultKind, NodeRole, ScenarioState, TransactionStatus, submit_operation_request,
};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::state::{
    FunctionRecord, InMemoryState, ProcessHealth, SubmittedOp, canonical_pair, epoch_ms_now,
};

const TICK_MS: u64 = 250;
const COMPUTE_AFTER_MS: u128 = 100;
const COMMIT_AFTER_MS: u128 = 250;
const SNAPSHOT_AFTER_MS: u128 = 500;

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
                    advance_pipeline(&mut state);
                    elect_if_needed(&mut state);
                    drive_scenarios(&mut state);
                }
            }
        }
    })
}

fn advance_pipeline(state: &mut InMemoryState) {
    let now = Instant::now();
    let leader_id = state.current_leader().map(|n| n.node_id);

    // Transition statuses on tx records.
    let tx_ids: Vec<u64> = state.transactions.keys().copied().collect();
    for tx_id in tx_ids {
        let tx = match state.transactions.get_mut(&tx_id) {
            Some(t) => t,
            None => continue,
        };
        let elapsed_ms = now.duration_since(tx.submitted_at).as_millis();
        match tx.status {
            TransactionStatus::Pending if elapsed_ms >= COMPUTE_AFTER_MS => {
                // Validate + apply.
                let fail_reason = execute_op(
                    &tx.op,
                    state.cluster_config.max_accounts,
                    &mut state.accounts,
                    &mut state.functions,
                );
                if fail_reason != 0 {
                    tx.status = TransactionStatus::Error;
                    tx.fail_reason = fail_reason;
                } else {
                    tx.status = TransactionStatus::Computed;
                    tx.computed_at = Some(now);
                }
            }
            TransactionStatus::Computed if elapsed_ms >= COMMIT_AFTER_MS => {
                tx.status = TransactionStatus::Committed;
                tx.committed_at = Some(now);
            }
            TransactionStatus::Committed if elapsed_ms >= SNAPSHOT_AFTER_MS => {
                tx.status = TransactionStatus::OnSnapshot;
                tx.snapshot_at = Some(now);
            }
            _ => {}
        }
    }

    // Compute the watermarks each node should advertise. The leader's compute_index is the count of all txs at COMPUTED or beyond; commit_index = COMMITTED or beyond; snapshot_index = ON_SNAPSHOT.
    let computed = count_at_or_beyond(state, |s| {
        matches!(
            s,
            TransactionStatus::Computed
                | TransactionStatus::Committed
                | TransactionStatus::OnSnapshot
        )
    });
    let committed = count_at_or_beyond(state, |s| {
        matches!(
            s,
            TransactionStatus::Committed | TransactionStatus::OnSnapshot
        )
    });
    let on_snapshot = count_at_or_beyond(state, |s| matches!(s, TransactionStatus::OnSnapshot));

    // Apply per-node, respecting health and reachability to the leader.
    let leader_id = leader_id.unwrap_or(0);
    let nodes_snapshot: Vec<u64> = state.nodes.keys().copied().collect();
    for id in nodes_snapshot {
        let reachable = leader_id != 0 && state.reachable(id, leader_id);
        if let Some(n) = state.nodes.get_mut(&id) {
            if n.health == ProcessHealth::Stopped {
                continue;
            }
            if id == leader_id || reachable {
                n.compute_index = computed;
                n.commit_index = committed;
                n.snapshot_index = on_snapshot;
                n.cluster_commit_index = committed;
                n.last_heartbeat_at_ms = epoch_ms_now();
            }
            // Followers that lost reachability to the leader stay at their old indices,
            // making lag visible on the Dashboard.
        }
    }
}

fn count_at_or_beyond(state: &InMemoryState, pred: impl Fn(TransactionStatus) -> bool) -> u64 {
    state
        .transactions
        .values()
        .filter(|t| pred(t.status))
        .count() as u64
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
    Submit {
        op: submit_operation_request::Operation,
        until_op_count: u64,
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
            let op = synthesize_workload_op(&s);
            ScenarioAction::Submit {
                op,
                until_op_count: target_total,
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
        ScenarioAction::Submit { op, until_op_count } => {
            // Submit one op per tick at most; multiple if the rate demands.
            // (A real runner would run a separate task; the mock keeps it on the tick for simplicity.)
            let needed = match state.scenario_runs.get(run_id) {
                Some(r) => until_op_count.saturating_sub(r.step_ops_emitted),
                None => 0,
            };
            for _ in 0..needed.min(64) {
                if state.cluster_health_unhealthy() {
                    if let Some(r) = state.scenario_runs.get_mut(run_id) {
                        r.ops_failed += 1;
                        r.ops_submitted += 1;
                        r.step_ops_emitted += 1;
                    }
                    continue;
                }
                let parsed = crate::state::parse_op(op.clone());
                let tx_id = state.allocate_tx_id();
                state.transactions.insert(
                    tx_id,
                    crate::state::TxRecord {
                        tx_id,
                        op: parsed,
                        status: TransactionStatus::Pending,
                        fail_reason: 0,
                        submitted_at: Instant::now(),
                        computed_at: None,
                        committed_at: None,
                        snapshot_at: None,
                    },
                );
                if let Some(r) = state.scenario_runs.get_mut(run_id) {
                    r.ops_submitted += 1;
                    r.ops_succeeded += 1;
                    r.step_ops_emitted += 1;
                }
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

fn synthesize_workload_op(
    s: &proto::control::SubmitOpsStep,
) -> submit_operation_request::Operation {
    use proto::control::{Deposit, Function, Transfer};
    let lookup = |k: &str, dflt: &str| -> String {
        s.params
            .iter()
            .find(|kv| kv.key == k)
            .map(|kv| kv.value.clone())
            .unwrap_or_else(|| dflt.to_string())
    };
    let parse_u64 = |v: &str| v.parse::<u64>().unwrap_or(0);
    match s.kind() {
        proto::control::WorkloadKind::DepositBurst => {
            submit_operation_request::Operation::Deposit(Deposit {
                account: parse_u64(&lookup("account", "1")),
                amount: parse_u64(&lookup("amount", "100")),
                user_ref: epoch_ms_now() as u64,
            })
        }
        proto::control::WorkloadKind::TransferPair => {
            submit_operation_request::Operation::Transfer(Transfer {
                from: parse_u64(&lookup("from", "1")),
                to: parse_u64(&lookup("to", "2")),
                amount: parse_u64(&lookup("amount", "10")),
                user_ref: epoch_ms_now() as u64,
            })
        }
        proto::control::WorkloadKind::TransferRandom => {
            let count = parse_u64(&lookup("account_count", "5")).max(2);
            let min = parse_u64(&lookup("amount_min", "1"));
            let max = parse_u64(&lookup("amount_max", "100")).max(min);
            let now = epoch_ms_now() as u64;
            let a = (now % count) + 1;
            let b = ((now / count) % count) + 1;
            let amount = min + (now % (max - min + 1));
            submit_operation_request::Operation::Transfer(Transfer {
                from: a,
                to: if b == a { (a % count) + 1 } else { b },
                amount,
                user_ref: now,
            })
        }
        proto::control::WorkloadKind::FunctionInvocation => {
            let name = lookup("name", "escrow");
            let mut params = Vec::with_capacity(8);
            for i in 0..8 {
                let key = format!("p{i}");
                let v = lookup(&key, "0").parse::<i64>().unwrap_or(0);
                params.push(v);
            }
            submit_operation_request::Operation::Function(Function {
                name,
                params,
                user_ref: epoch_ms_now() as u64,
            })
        }
        _ => submit_operation_request::Operation::Deposit(Deposit {
            account: 1,
            amount: 1,
            user_ref: epoch_ms_now() as u64,
        }),
    }
}

fn execute_op(
    op: &SubmittedOp,
    max_accounts: u64,
    accounts: &mut std::collections::HashMap<u64, i64>,
    functions: &mut std::collections::BTreeMap<String, FunctionRecord>,
) -> u32 {
    match op {
        SubmittedOp::Deposit {
            account, amount, ..
        } => {
            if *account >= max_accounts {
                return 6;
            }
            let cur = accounts.entry(*account).or_insert(0);
            *cur += *amount as i64;
            0
        }
        SubmittedOp::Withdrawal {
            account, amount, ..
        } => {
            if *account >= max_accounts {
                return 6;
            }
            let cur = accounts.entry(*account).or_insert(0);
            if *cur < *amount as i64 {
                return 1;
            }
            *cur -= *amount as i64;
            0
        }
        SubmittedOp::Transfer {
            from, to, amount, ..
        } => {
            if *from >= max_accounts || *to >= max_accounts {
                return 6;
            }
            let fb = accounts.entry(*from).or_insert(0);
            if *fb < *amount as i64 {
                return 1;
            }
            *fb -= *amount as i64;
            let tb = accounts.entry(*to).or_insert(0);
            *tb += *amount as i64;
            0
        }
        SubmittedOp::Function { name, .. } => {
            if functions.get(name).is_none() {
                return 5;
            }
            0
        }
    }
}
