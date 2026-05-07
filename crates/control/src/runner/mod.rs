//! Scenario runner — drives a `Scenario` against a real cluster.
//!
//! Two collaborators:
//! - `Provisioner` (in [`provisioner`]) owns cluster lifecycle and
//!   fault injection. The runner asks it for node addresses up front
//!   and tears the cluster down on the way out.
//! - [`client::ClusterClient`] handles every ledger-side call —
//!   submits, reads, waits, function registration. The runner builds
//!   one over the addresses returned by `Provisioner::provision`.
//!
//! The runner glues the two together: walks the scenario's steps,
//! dispatches each to the right collaborator, tracks `user_ref →
//! tx_id` bindings so later `TxRef::UserRef` references resolve, and
//! awaits async-branch tasks at end-of-scenario.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use client::ClusterClient;
use proto::ledger as proto_ledger;
use tokio::task::JoinHandle;

use testing::scenario::{
    Action, AssertBalance, AssertBalanceSum, AssertLeader, AssertPipelineCaughtUp,
    AssertTxStatus, AsyncBranch, BatchKind, GetBalance, GetPipelineIndex, HealPartition,
    NodeSelector, PartitionPair, PipelineLevel, RetryConfig, Scenario, Step, Submit,
    SubmitBatch, SubmitOp, TxRef, TxStatus, WaitForLevel, WaitLevel,
};

pub use crate::provisioner::{Capabilities, ProvisionConfig, Provisioner, ProvisionerError};

/// Per-`run` state. Each top-level `run()` and each spawned branch
/// gets its own — bindings are scoped to the task that produced them.
#[derive(Default)]
struct RunCtx {
    /// `user_ref → tx_id`, populated by `Submit` / `SubmitBatch`.
    bindings: HashMap<u64, u64>,
    /// Pending branch tasks, awaited at end-of-scenario.
    branches: Vec<JoinHandle<Result<(), RunError>>>,
}

impl RunCtx {
    /// Drain spawned branches. Reports the first error encountered;
    /// remaining branches still complete.
    async fn join_all(self) -> Result<(), RunError> {
        let mut first_err: Option<RunError> = None;
        for handle in self.branches {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(je) => {
                    if first_err.is_none() {
                        first_err = Some(RunError::BranchPanic(je.to_string()));
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// Categorical run failures. Provisioner / client errors bubble up;
/// everything else is the runner's own bookkeeping.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("provisioner: {0}")]
    Provisioner(#[from] ProvisionerError),

    #[error("client: {0}")]
    Client(String),

    #[error("connect to cluster failed: {0}")]
    Connect(String),

    #[error("assertion failed: {0}")]
    AssertionFailed(String),

    #[error("user_ref {0} has no bound tx_id (no prior Submit / SubmitBatch with this user_ref)")]
    UnknownUserRef(u64),

    #[error("node selector {0:?} cannot be resolved against the current cluster")]
    UnresolvableNode(NodeSelector),

    #[error("operation timed out after {0:?}")]
    Timeout(Duration),

    #[error("async branch panicked or was canceled: {0}")]
    BranchPanic(String),

    #[error("scenario uses {requirement} but provisioner does not support it (capabilities: {capabilities:?})")]
    UnsupportedCapability {
        requirement: &'static str,
        capabilities: Capabilities,
    },
}

/// Drives a `Scenario` against a `Provisioner` + a freshly-built
/// `ClusterClient`. One `run` per scenario; each call owns the
/// cluster's lifecycle.
pub struct ScenarioRunner {
    provisioner: Arc<dyn Provisioner>,
}

impl ScenarioRunner {
    pub fn new(provisioner: Arc<dyn Provisioner>) -> Self {
        Self { provisioner }
    }

    /// Reconcile the cluster against `config` via the provisioner,
    /// then run the scenario against it.
    ///
    /// Pre-flight checks the scenario against the provisioner's
    /// declared capabilities and refuses scenarios that would hit
    /// unsupported faults — this avoids wasting a provision call on
    /// a doomed run. Cluster teardown is the provisioner's `Drop`
    /// responsibility, fired when the runner (and any cloned `Arc`s)
    /// goes out of scope; there is no explicit destroy step here.
    pub async fn run(
        &self,
        scenario: &Scenario,
        config: &ProvisionConfig,
    ) -> Result<(), RunError> {
        let caps = self.provisioner.capabilities();
        check_capabilities(scenario, caps)?;

        let urls = self.provisioner.provision(config).await?;
        let client = ClusterClient::connect(&urls)
            .await
            .map_err(|e| RunError::Connect(e.to_string()))?;

        self.execute(&client, scenario).await
    }

    async fn execute(
        &self,
        client: &ClusterClient,
        scenario: &Scenario,
    ) -> Result<(), RunError> {
        let mut ctx = RunCtx::default();
        for step in &scenario.steps {
            self.dispatch(client, &mut ctx, step).await?;
        }
        ctx.join_all().await
    }

    async fn dispatch(
        &self,
        client: &ClusterClient,
        ctx: &mut RunCtx,
        step: &Step,
    ) -> Result<(), RunError> {
        match &step.action {
            Action::Submit(s) => self.run_submit(client, ctx, s).await,
            Action::SubmitBatch(s) => self.run_submit_batch(client, ctx, s).await,
            Action::AsyncBranch(b) => {
                let handle = self.spawn_branch(client, b);
                ctx.branches.push(handle);
                Ok(())
            }
            Action::Wait(w) => {
                tokio::time::sleep(w.duration).await;
                Ok(())
            }
            Action::WaitForLevel(w) => self.run_wait_for_level(client, ctx, w).await,
            Action::GetBalance(g) => self.run_get_balance(client, g).await,
            Action::GetPipelineIndex(g) => self.run_get_pipeline_index(client, g).await,
            Action::AssertBalance(a) => self.run_assert_balance(client, a).await,
            Action::AssertBalanceSum(a) => self.run_assert_balance_sum(client, a).await,
            Action::AssertPipelineCaughtUp(a) => {
                self.run_assert_pipeline_caught_up(client, a).await
            }
            Action::AssertTxStatus(a) => self.run_assert_tx_status(client, ctx, a).await,
            Action::AssertLeader(a) => self.run_assert_leader(client, a).await,
            Action::StopNode(s) => self.run_provisioner_node(client, &s.node, ProvFault::Stop).await,
            Action::KillNode(s) => self.run_provisioner_node(client, &s.node, ProvFault::Kill).await,
            Action::StartNode(s) => self.run_provisioner_node(client, &s.node, ProvFault::Start).await,
            Action::RestartNode(s) => {
                self.run_provisioner_node(client, &s.node, ProvFault::Restart).await
            }
            Action::PartitionPair(p) => self.run_partition_pair(client, p).await,
            Action::HealPartition(h) => self.run_heal_partition(client, h).await,
            Action::RegisterFunction(r) => {
                client
                    .register_function(&r.name, &r.binary, r.override_existing)
                    .await
                    .map_err(client_err)?;
                Ok(())
            }
            Action::UnregisterFunction(u) => {
                client.unregister_function(&u.name).await.map_err(client_err)?;
                Ok(())
            }
            // Action is `#[non_exhaustive]`. Adding a variant upstream
            // without updating this dispatch lands here at runtime.
            _ => Err(RunError::Client(
                "scenario step variant not handled by ScenarioRunner".into(),
            )),
        }
    }

    // ============================================================
    // Submission
    // ============================================================

    async fn run_submit(
        &self,
        client: &ClusterClient,
        ctx: &mut RunCtx,
        s: &Submit,
    ) -> Result<(), RunError> {
        let user_ref = user_ref_of(&s.op);
        let tx_id = self.do_submit(client, &s.op, s.wait, s.retry).await?;
        ctx.bindings.insert(user_ref, tx_id);
        Ok(())
    }

    async fn run_submit_batch(
        &self,
        client: &ClusterClient,
        ctx: &mut RunCtx,
        s: &SubmitBatch,
    ) -> Result<(), RunError> {
        // Submit each expanded op individually. The cluster client's
        // homogeneous batch APIs (`deposit_batch_and_wait` etc.) are
        // a worthwhile optimization later — for now correctness over
        // throughput, especially since the runner does not yet need
        // to drive multi-op heterogeneous batches with different
        // wait semantics per op.
        for op in expand_batch(&s.kind) {
            let tx_id = self.do_submit(client, &op, s.wait, s.retry).await?;
            ctx.bindings.insert(user_ref_of(&op), tx_id);
        }
        Ok(())
    }

    async fn do_submit(
        &self,
        client: &ClusterClient,
        op: &SubmitOp,
        wait: WaitLevel,
        retry: Option<RetryConfig>,
    ) -> Result<u64, RunError> {
        let (max_attempts, backoff_ms) = retry_params(retry);
        let mut last_err: Option<RunError> = None;
        for attempt in 0..max_attempts {
            match submit_once(client, op, wait).await {
                Ok(tx_id) => return Ok(tx_id),
                Err(e) => {
                    last_err = Some(e);
                    if attempt + 1 < max_attempts && backoff_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(backoff_ms as u64)).await;
                    }
                }
            }
        }
        Err(last_err.expect("retry loop ran at least once"))
    }

    // ============================================================
    // Concurrency
    // ============================================================

    fn spawn_branch(
        &self,
        client: &ClusterClient,
        branch: &AsyncBranch,
    ) -> JoinHandle<Result<(), RunError>> {
        let provisioner = self.provisioner.clone();
        let client = client.clone();
        let steps = branch.steps.clone();
        tokio::spawn(async move {
            let runner = ScenarioRunner::new(provisioner);
            let mut ctx = RunCtx::default();
            for step in &steps {
                runner.dispatch(&client, &mut ctx, step).await?;
            }
            ctx.join_all().await
        })
    }

    // ============================================================
    // Synchronization
    // ============================================================

    async fn run_wait_for_level(
        &self,
        client: &ClusterClient,
        ctx: &RunCtx,
        w: &WaitForLevel,
    ) -> Result<(), RunError> {
        let tx = resolve_tx(ctx, &w.tx)?;
        let target = pipeline_to_status_threshold(w.level);
        let timeout = Duration::from_secs(30);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let (status, _) = client
                .get_transaction_status(tx)
                .await
                .map_err(client_err)?;
            if status >= target {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RunError::Timeout(timeout));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // ============================================================
    // Reads (telemetry — invoke and discard for now)
    // ============================================================

    async fn run_get_balance(
        &self,
        client: &ClusterClient,
        g: &GetBalance,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, &g.node)?;
        client
            .node(idx)
            .get_balance(g.account)
            .await
            .map_err(client_err)?;
        Ok(())
    }

    async fn run_get_pipeline_index(
        &self,
        client: &ClusterClient,
        g: &GetPipelineIndex,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, &g.node)?;
        client
            .node(idx)
            .get_pipeline_index()
            .await
            .map_err(client_err)?;
        Ok(())
    }

    // ============================================================
    // Assertions
    // ============================================================

    async fn run_assert_balance(
        &self,
        client: &ClusterClient,
        a: &AssertBalance,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, &a.node)?;
        let bal = client
            .node(idx)
            .get_balance(a.account)
            .await
            .map_err(client_err)?;
        if bal.balance != a.expected {
            return Err(RunError::AssertionFailed(format!(
                "AssertBalance: account {} on node[{idx}] = {}, expected {}",
                a.account, bal.balance, a.expected
            )));
        }
        Ok(())
    }

    async fn run_assert_balance_sum(
        &self,
        client: &ClusterClient,
        a: &AssertBalanceSum,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, &a.node)?;
        let account_ids: Vec<u64> = (0..=a.max_account).collect();
        let balances = client
            .node(idx)
            .get_balances(&account_ids)
            .await
            .map_err(client_err)?;
        let total: i64 = balances.iter().sum();
        if total != 0 {
            return Err(RunError::AssertionFailed(format!(
                "AssertBalanceSum: sum on node[{idx}] over [0,{}] = {total}, expected 0",
                a.max_account
            )));
        }
        Ok(())
    }

    async fn run_assert_pipeline_caught_up(
        &self,
        client: &ClusterClient,
        a: &AssertPipelineCaughtUp,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, &a.node)?;
        let pi = client
            .node(idx)
            .get_pipeline_index()
            .await
            .map_err(client_err)?;
        if pi.compute < a.target || pi.commit < a.target || pi.snapshot < a.target {
            return Err(RunError::AssertionFailed(format!(
                "AssertPipelineCaughtUp: node[{idx}] compute={} commit={} snapshot={}, expected all >= {}",
                pi.compute, pi.commit, pi.snapshot, a.target
            )));
        }
        Ok(())
    }

    async fn run_assert_tx_status(
        &self,
        client: &ClusterClient,
        ctx: &RunCtx,
        a: &AssertTxStatus,
    ) -> Result<(), RunError> {
        let tx = resolve_tx(ctx, &a.tx)?;
        let (status, _) = client
            .get_transaction_status(tx)
            .await
            .map_err(client_err)?;
        let expected = tx_status_to_i32(a.expected);
        if status != expected {
            return Err(RunError::AssertionFailed(format!(
                "AssertTxStatus: tx {tx} status = {status}, expected {expected} ({:?})",
                a.expected
            )));
        }
        Ok(())
    }

    async fn run_assert_leader(
        &self,
        client: &ClusterClient,
        a: &AssertLeader,
    ) -> Result<(), RunError> {
        match &a.expected {
            Some(sel) => {
                let idx = self.resolve_node_idx(client, sel)?;
                let pi = client
                    .node(idx)
                    .get_pipeline_index()
                    .await
                    .map_err(client_err)?;
                if pi.is_leader {
                    Ok(())
                } else {
                    Err(RunError::AssertionFailed(format!(
                        "AssertLeader: node[{idx}] is not leader"
                    )))
                }
            }
            None => {
                for i in 0..client.node_count() {
                    if let Ok(pi) = client.node(i).get_pipeline_index().await {
                        if pi.is_leader {
                            return Ok(());
                        }
                    }
                }
                Err(RunError::AssertionFailed(
                    "AssertLeader: no node reports as leader".into(),
                ))
            }
        }
    }

    // ============================================================
    // Fault injection — delegate to provisioner
    // ============================================================

    async fn run_provisioner_node(
        &self,
        client: &ClusterClient,
        sel: &NodeSelector,
        fault: ProvFault,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, sel)?;
        match fault {
            ProvFault::Stop => self.provisioner.stop_node(idx).await?,
            ProvFault::Kill => self.provisioner.kill_node(idx).await?,
            ProvFault::Start => self.provisioner.start_node(idx).await?,
            ProvFault::Restart => self.provisioner.restart_node(idx).await?,
        }
        Ok(())
    }

    async fn run_partition_pair(
        &self,
        client: &ClusterClient,
        p: &PartitionPair,
    ) -> Result<(), RunError> {
        let a = self.resolve_node_idx(client, &p.a)?;
        let b = self.resolve_node_idx(client, &p.b)?;
        self.provisioner.partition_pair(a, b).await?;
        Ok(())
    }

    async fn run_heal_partition(
        &self,
        client: &ClusterClient,
        h: &HealPartition,
    ) -> Result<(), RunError> {
        let a = self.resolve_node_idx(client, &h.a)?;
        let b = self.resolve_node_idx(client, &h.b)?;
        self.provisioner.heal_partition(a, b).await?;
        Ok(())
    }

    // ============================================================
    // Resolution helpers
    // ============================================================

    /// Map a `NodeSelector` to a 0-based index into the
    /// `ClusterClient`'s node list. `Id(_)` is rejected — it's not
    /// addressable in the current happy-path scenarios and a real
    /// node-id-to-index mapping needs more topology metadata than the
    /// runner currently has.
    fn resolve_node_idx(
        &self,
        client: &ClusterClient,
        sel: &NodeSelector,
    ) -> Result<usize, RunError> {
        match sel {
            NodeSelector::Index(idx) => {
                if *idx < client.node_count() {
                    Ok(*idx)
                } else {
                    Err(RunError::UnresolvableNode(sel.clone()))
                }
            }
            NodeSelector::Leader => {
                let idx = client.current_leader_index();
                if idx < client.node_count() {
                    Ok(idx)
                } else {
                    Err(RunError::UnresolvableNode(sel.clone()))
                }
            }
            NodeSelector::Any => {
                if client.node_count() > 0 {
                    Ok(0)
                } else {
                    Err(RunError::UnresolvableNode(sel.clone()))
                }
            }
            NodeSelector::Id(_) => Err(RunError::UnresolvableNode(sel.clone())),
        }
    }
}

// ============================================================
// Free helpers
// ============================================================

#[derive(Clone, Copy)]
enum ProvFault {
    Stop,
    Kill,
    Start,
    Restart,
}

fn user_ref_of(op: &SubmitOp) -> u64 {
    match op {
        SubmitOp::Deposit { user_ref, .. }
        | SubmitOp::Withdraw { user_ref, .. }
        | SubmitOp::Transfer { user_ref, .. }
        | SubmitOp::Function { user_ref, .. } => *user_ref,
    }
}

fn with_user_ref_offset(op: SubmitOp, offset: u64) -> SubmitOp {
    match op {
        SubmitOp::Deposit {
            account,
            amount,
            user_ref,
        } => SubmitOp::Deposit {
            account,
            amount,
            user_ref: user_ref + offset,
        },
        SubmitOp::Withdraw {
            account,
            amount,
            user_ref,
        } => SubmitOp::Withdraw {
            account,
            amount,
            user_ref: user_ref + offset,
        },
        SubmitOp::Transfer {
            from,
            to,
            amount,
            user_ref,
        } => SubmitOp::Transfer {
            from,
            to,
            amount,
            user_ref: user_ref + offset,
        },
        SubmitOp::Function {
            name,
            params,
            user_ref,
        } => SubmitOp::Function {
            name,
            params,
            user_ref: user_ref + offset,
        },
    }
}

/// Expand `BatchKind` into a flat `Vec<SubmitOp>`. For `Dynamic`,
/// each iteration's user_refs are offset by `iter * base.len()`.
fn expand_batch(kind: &BatchKind) -> Vec<SubmitOp> {
    match kind {
        BatchKind::Static(ops) => ops.clone(),
        BatchKind::Dynamic { base, repeat } => {
            let stride = base.len() as u64;
            let mut out = Vec::with_capacity(base.len() * (*repeat as usize));
            for iter in 0..*repeat as u64 {
                let offset = iter * stride;
                for op in base {
                    out.push(with_user_ref_offset(op.clone(), offset));
                }
            }
            out
        }
    }
}

fn resolve_tx(ctx: &RunCtx, tx: &TxRef) -> Result<u64, RunError> {
    match tx {
        TxRef::UserRef(ur) => ctx
            .bindings
            .get(ur)
            .copied()
            .ok_or(RunError::UnknownUserRef(*ur)),
    }
}

fn wait_to_proto(w: WaitLevel) -> Option<proto_ledger::WaitLevel> {
    match w {
        WaitLevel::None => None,
        WaitLevel::Computed => Some(proto_ledger::WaitLevel::Computed),
        WaitLevel::Committed => Some(proto_ledger::WaitLevel::Committed),
        WaitLevel::OnSnapshot => Some(proto_ledger::WaitLevel::Snapshot),
    }
}

fn pipeline_to_status_threshold(level: PipelineLevel) -> i32 {
    // Mirrors `proto::ledger::TransactionStatus`:
    // PENDING = 0, COMPUTED = 1, COMMITTED = 2, ON_SNAPSHOT = 3.
    match level {
        PipelineLevel::Computed => 1,
        PipelineLevel::Committed => 2,
        PipelineLevel::OnSnapshot => 3,
    }
}

fn tx_status_to_i32(s: TxStatus) -> i32 {
    // Mirrors `proto::ledger::TransactionStatus`.
    match s {
        TxStatus::Pending => 0,
        TxStatus::Computed => 1,
        TxStatus::Committed => 2,
        TxStatus::OnSnapshot => 3,
        TxStatus::Error => 4,
        TxStatus::NotFound => 5,
    }
}

fn retry_params(retry: Option<RetryConfig>) -> (u32, u32) {
    match retry {
        Some(r) => (r.max_retries.saturating_add(1), r.backoff_ms),
        None => (1, 0),
    }
}

fn client_err(s: tonic::Status) -> RunError {
    RunError::Client(format!("{s}"))
}

fn params_to_arr(params: &[i64]) -> [i64; 8] {
    let mut arr = [0i64; 8];
    for (i, &p) in params.iter().take(8).enumerate() {
        arr[i] = p;
    }
    arr
}

/// Walk every step in the scenario (recursing into `AsyncBranch`
/// sub-steps) and reject any that requires a capability the
/// provisioner has not declared. Surface-level only — does not
/// attempt to predict whether a *supported* operation will succeed
/// against the live cluster.
fn check_capabilities(scenario: &Scenario, caps: Capabilities) -> Result<(), RunError> {
    fn walk(step: &Step, caps: Capabilities) -> Result<(), RunError> {
        match &step.action {
            Action::KillNode(_) if !caps.kill => Err(RunError::UnsupportedCapability {
                requirement: "KillNode (capability: kill)",
                capabilities: caps,
            }),
            Action::PartitionPair(_) | Action::HealPartition(_) if !caps.network_partition => {
                Err(RunError::UnsupportedCapability {
                    requirement: "PartitionPair / HealPartition (capability: network_partition)",
                    capabilities: caps,
                })
            }
            Action::AsyncBranch(b) => {
                for nested in &b.steps {
                    walk(nested, caps)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    for step in &scenario.steps {
        walk(step, caps)?;
    }
    Ok(())
}

// ============================================================
// Tests — pure functions only (no real cluster, no real backend)
// ============================================================

#[cfg(test)]
mod tests {
    use testing::scenario::{
        Action, AsyncBranch, HealPartition, KillNode, NodeSelector as Sel, PartitionPair,
        Scenario, Step, StopNode,
    };

    use super::*;

    fn step(a: Action) -> Step {
        Step::new(a)
    }

    #[test]
    fn capability_check_passes_for_caps_only_using_baseline_faults() {
        // StopNode is always supported — no capability required.
        let s = Scenario::new("ok").with_steps(vec![step(Action::StopNode(StopNode {
            node: Sel::Index(0),
        }))]);
        check_capabilities(&s, Capabilities::none()).expect("baseline fault must be allowed");
    }

    #[test]
    fn capability_check_rejects_kill_when_unsupported() {
        let s = Scenario::new("nope").with_steps(vec![step(Action::KillNode(KillNode {
            node: Sel::Index(0),
        }))]);
        let err = check_capabilities(&s, Capabilities::none()).expect_err("should refuse");
        assert!(matches!(err, RunError::UnsupportedCapability { .. }), "got {err:?}");
    }

    #[test]
    fn capability_check_rejects_partition_when_unsupported() {
        let s = Scenario::new("nope").with_steps(vec![
            step(Action::PartitionPair(PartitionPair {
                a: Sel::Index(0),
                b: Sel::Index(1),
            })),
            step(Action::HealPartition(HealPartition {
                a: Sel::Index(0),
                b: Sel::Index(1),
            })),
        ]);
        let err = check_capabilities(&s, Capabilities::none()).expect_err("should refuse");
        assert!(matches!(err, RunError::UnsupportedCapability { .. }), "got {err:?}");
    }

    #[test]
    fn capability_check_descends_into_async_branches() {
        // Kill nested inside an AsyncBranch must still be flagged.
        let s = Scenario::new("nested").with_steps(vec![step(Action::AsyncBranch(AsyncBranch {
            name: None,
            steps: vec![step(Action::KillNode(KillNode {
                node: Sel::Index(0),
            }))],
        }))]);
        let err = check_capabilities(&s, Capabilities::none()).expect_err("should refuse");
        assert!(matches!(err, RunError::UnsupportedCapability { .. }), "got {err:?}");
    }

    #[test]
    fn capability_check_passes_when_caps_advertised() {
        let s = Scenario::new("ok").with_steps(vec![
            step(Action::KillNode(KillNode {
                node: Sel::Index(0),
            })),
            step(Action::PartitionPair(PartitionPair {
                a: Sel::Index(0),
                b: Sel::Index(1),
            })),
        ]);
        check_capabilities(&s, Capabilities::all()).expect("all caps satisfies");
    }
}

async fn submit_once(
    client: &ClusterClient,
    op: &SubmitOp,
    wait: WaitLevel,
) -> Result<u64, RunError> {
    let user_ref = user_ref_of(op);
    if let Some(wl) = wait_to_proto(wait) {
        match op {
            SubmitOp::Deposit {
                account, amount, ..
            } => Ok(client
                .deposit_and_wait(*account, *amount, user_ref, wl)
                .await
                .map_err(client_err)?
                .tx_id),
            SubmitOp::Withdraw {
                account, amount, ..
            } => Ok(client
                .withdraw_and_wait(*account, *amount, user_ref, wl)
                .await
                .map_err(client_err)?
                .tx_id),
            SubmitOp::Transfer {
                from, to, amount, ..
            } => Ok(client
                .transfer_and_wait(*from, *to, *amount, user_ref, wl)
                .await
                .map_err(client_err)?
                .tx_id),
            SubmitOp::Function { name, params, .. } => Ok(client
                .submit_function_and_wait(name, params_to_arr(params), user_ref, wl)
                .await
                .map_err(client_err)?
                .tx_id),
        }
    } else {
        match op {
            SubmitOp::Deposit {
                account, amount, ..
            } => client
                .deposit(*account, *amount, user_ref)
                .await
                .map_err(client_err),
            SubmitOp::Withdraw {
                account, amount, ..
            } => client
                .withdraw(*account, *amount, user_ref)
                .await
                .map_err(client_err),
            SubmitOp::Transfer {
                from, to, amount, ..
            } => client
                .transfer(*from, *to, *amount, user_ref)
                .await
                .map_err(client_err),
            SubmitOp::Function { .. } => Err(RunError::Client(
                "Function submit with WaitLevel::None is not supported by ClusterClient".into(),
            )),
        }
    }
}
