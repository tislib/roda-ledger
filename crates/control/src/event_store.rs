//! In-memory ephemeral history for the control plane.
//!
//! These records are derived from operator actions and scenario runs;
//! they aren't produced by the cluster itself, so they live here, not
//! in the ledger or Raft. All bounded; oldest entries fall off when
//! the cap is hit. Lost on server restart by design (demo).
//!
//! Backed by a single `parking_lot::RwLock` — none of these are hot
//! paths.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use proto::control::{FaultEvent, ScenarioState as PbScenarioState};
use tokio::task::JoinHandle;

const FAULT_HISTORY_CAP: usize = 256;
const SUBMISSION_HISTORY_CAP: usize = 512;

/// One past operator-driven cluster modification or scenario-driven
/// fault. Mirrors the proto `FaultEvent`; we just store its proto-ready
/// shape directly.
pub type Fault = FaultEvent;

/// Thin record kept so the UI's "recent transactions" panel has
/// something to render — the cluster doesn't expose a tx index. Every
/// `SubmitOperation` RPC adds one of these; status itself is fetched
/// live via `client.get_transaction_status`.
#[derive(Clone, Debug)]
pub struct SubmissionRecord {
    pub tx_id: u64,
    pub user_ref: u64,
    pub kind: SubmittedOpKind,
    pub at_epoch_ms: i64,
}

#[derive(Clone, Debug)]
pub enum SubmittedOpKind {
    Deposit { account: u64, amount: u64 },
    Withdrawal { account: u64, amount: u64 },
    Transfer { from: u64, to: u64, amount: u64 },
    Function { name: String },
}

/// State for one in-progress or finished `RunScenario`.
pub struct ScenarioRunRecord {
    pub run_id: String,
    pub scenario_name: String,
    pub state: PbScenarioState,
    pub started_at_epoch_ms: i64,
    pub started_at: Instant,
    pub finished_at_epoch_ms: Option<i64>,
    pub elapsed_ms: Option<i64>,
    pub error_message: Option<String>,
    /// Ops committed by the cluster between `started_at` and
    /// `finished_at` (or current time, if still running). Filled in
    /// when the run finishes.
    pub ops_total: u64,
    /// JoinHandle so `CancelScenario` can abort the run.
    pub handle: Option<JoinHandle<()>>,
}

impl ScenarioRunRecord {
    pub fn new(run_id: String, scenario_name: String) -> Self {
        Self {
            run_id,
            scenario_name,
            state: PbScenarioState::Running,
            started_at_epoch_ms: epoch_ms_now(),
            started_at: Instant::now(),
            finished_at_epoch_ms: None,
            elapsed_ms: None,
            error_message: None,
            ops_total: 0,
            handle: None,
        }
    }
}

#[derive(Default)]
struct Inner {
    fault_history: VecDeque<Fault>,
    submissions: VecDeque<SubmissionRecord>,
    scenario_runs: BTreeMap<String, ScenarioRunRecord>,
    next_run_seq: u64,
}

#[derive(Default)]
pub struct EventStore {
    inner: RwLock<Inner>,
}

impl EventStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drop everything: fault history, submissions, scenario runs. Used by
    /// `ResetCluster` so the new cluster doesn't inherit old session state.
    /// Aborts any in-flight run handles before clearing.
    pub fn reset(&self) {
        let mut inner = self.inner.write();
        for (_, rec) in inner.scenario_runs.iter_mut() {
            if let Some(h) = rec.handle.take() {
                h.abort();
            }
        }
        inner.fault_history.clear();
        inner.submissions.clear();
        inner.scenario_runs.clear();
        inner.next_run_seq = 0;
    }

    pub fn record_fault(&self, fault: Fault) {
        let mut inner = self.inner.write();
        if inner.fault_history.len() >= FAULT_HISTORY_CAP {
            inner.fault_history.pop_front();
        }
        inner.fault_history.push_back(fault);
    }

    /// Newest first.
    pub fn fault_history(&self, limit: usize) -> Vec<Fault> {
        let inner = self.inner.read();
        inner
            .fault_history
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn record_submission(&self, rec: SubmissionRecord) {
        let mut inner = self.inner.write();
        if inner.submissions.len() >= SUBMISSION_HISTORY_CAP {
            inner.submissions.pop_front();
        }
        inner.submissions.push_back(rec);
    }

    /// Generate a fresh run id like "run-001". Caller stores the record.
    pub fn next_run_id(&self) -> String {
        let mut inner = self.inner.write();
        inner.next_run_seq += 1;
        format!("run-{:03}", inner.next_run_seq)
    }

    pub fn insert_run(&self, rec: ScenarioRunRecord) {
        let mut inner = self.inner.write();
        inner.scenario_runs.insert(rec.run_id.clone(), rec);
    }

    pub fn update_run<F: FnOnce(&mut ScenarioRunRecord)>(&self, run_id: &str, f: F) {
        let mut inner = self.inner.write();
        if let Some(rec) = inner.scenario_runs.get_mut(run_id) {
            f(rec);
        }
    }

    pub fn get_run<R, F>(&self, run_id: &str, f: F) -> Option<R>
    where
        F: FnOnce(&ScenarioRunRecord) -> R,
    {
        let inner = self.inner.read();
        inner.scenario_runs.get(run_id).map(f)
    }

    /// Newest first.
    pub fn list_runs(&self) -> Vec<RunSummary> {
        let inner = self.inner.read();
        let mut runs: Vec<_> = inner
            .scenario_runs
            .values()
            .map(|r| RunSummary {
                run_id: r.run_id.clone(),
                scenario_name: r.scenario_name.clone(),
                state: r.state,
                started_at_epoch_ms: r.started_at_epoch_ms,
                finished_at_epoch_ms: r.finished_at_epoch_ms,
                elapsed_ms: r.elapsed_ms,
                ops_total: r.ops_total,
                error_message: r.error_message.clone(),
            })
            .collect();
        runs.sort_by_key(|r| std::cmp::Reverse(r.started_at_epoch_ms));
        runs
    }

    /// Abort an in-flight run if it's still running. Returns true if
    /// the run was found and an abort signal was sent.
    pub fn cancel_run(&self, run_id: &str) -> bool {
        let mut inner = self.inner.write();
        match inner.scenario_runs.get_mut(run_id) {
            Some(rec) if rec.state == PbScenarioState::Running => {
                if let Some(h) = rec.handle.take() {
                    h.abort();
                }
                rec.state = PbScenarioState::Canceled;
                rec.finished_at_epoch_ms = Some(epoch_ms_now());
                rec.elapsed_ms = Some(rec.started_at.elapsed().as_millis() as i64);
                true
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RunSummary {
    pub run_id: String,
    pub scenario_name: String,
    pub state: PbScenarioState,
    pub started_at_epoch_ms: i64,
    pub finished_at_epoch_ms: Option<i64>,
    pub elapsed_ms: Option<i64>,
    pub ops_total: u64,
    pub error_message: Option<String>,
}

pub fn epoch_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
