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
//!
//! While the scenario runs, a [`MetricsCollector`] gathers per-node
//! pipeline samples (every 100 ms) and per-submit latencies (only
//! for waiting submits). The collector's snapshot is returned inside
//! [`RunReport`] regardless of pass/fail so callers always see what
//! landed before a failure.

pub mod metrics;

use client::ClusterClient;
use proto::ledger as proto_ledger;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

use testing::scenario::{
    Action, AssertBalance, AssertBalanceSum, AssertLeader, AssertPipelineCaughtUp, AssertTxStatus,
    AsyncBranch, BatchKind, Concurrent, GetBalance, GetPipelineIndex, HealPartition, NodeSelector,
    PartitionPair, PipelineLevel, RetryConfig, Scenario, Step, Submit, SubmitBatch, SubmitOp,
    TxRef, TxStatus, WaitForLevel, WaitLevel,
};

pub use crate::provisioner::{Capabilities, ProvisionConfig, Provisioner, ProvisionerError};
pub use metrics::{
    LatencyPoint, MetricsCollector, NodePipelineSnap, PerSecondStats, Sample, Snapshot,
};

/// Per-`run` state. Each top-level `run()` and each spawned branch
/// gets its own — bindings are scoped to the task that produced them.
#[derive(Default)]
struct RunCtx {
    /// `user_ref → tx_id`, populated by `Submit` / `SubmitBatch`.
    bindings: HashMap<u64, u64>,
    /// Cluster index of the most recent `KillNode` / `StopNode`.
    /// Resolves `NodeSelector::LastKilled`. Reset per task; siblings
    /// in concurrent branches don't share kill state.
    last_killed: Option<usize>,
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

    #[error(
        "scenario uses {requirement} but provisioner does not support it (capabilities: {capabilities:?})"
    )]
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

/// Aggregate result of a single `run()` call. Carries the scenario's
/// pass/fail outcome plus everything the metrics collector gathered
/// during execution. The `result` field is `Err` when the scenario
/// hit any [`RunError`] — partial metrics from the work that
/// happened before the failure are still in `metrics`, useful for
/// post-mortems.
#[derive(Debug)]
pub struct RunReport {
    pub elapsed: Duration,
    pub result: Result<(), RunError>,
    pub metrics: Snapshot,
}

/// How often the background poller probes every node's pipeline
/// indices. Small enough to capture short scenarios; loose enough
/// not to congest the cluster on long ones.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

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
    ///
    /// Always returns a [`RunReport`]. Inspect `report.result` for
    /// pass/fail; `report.metrics` holds whatever the background
    /// poller gathered before the run ended.
    pub async fn run(
        &self,
        scenario: &Scenario,
        config: &ProvisionConfig,
        metrics: Arc<MetricsCollector>,
    ) -> RunReport {
        let caps = self.provisioner.capabilities();
        if let Err(e) = check_capabilities(scenario, caps) {
            return RunReport {
                elapsed: Duration::ZERO,
                result: Err(e),
                metrics: metrics.snapshot(),
            };
        }

        let provision_started = Instant::now();
        let urls = match self.provisioner.provision(config).await {
            Ok(u) => u,
            Err(e) => {
                return RunReport {
                    elapsed: provision_started.elapsed(),
                    result: Err(RunError::Provisioner(e)),
                    metrics: metrics.snapshot(),
                };
            }
        };
        let client = match ClusterClient::connect(&urls).await {
            Ok(c) => c,
            Err(e) => {
                return RunReport {
                    elapsed: provision_started.elapsed(),
                    result: Err(RunError::Connect(e.to_string())),
                    metrics: metrics.snapshot(),
                };
            }
        };

        self.run_against_existing(scenario, client, metrics).await
    }

    /// Run against a cluster the caller already owns. Skips provision
    /// and connect — the gRPC server uses this so its single long-lived
    /// `ClusterHandle` carries the cluster across many runs. The
    /// provisioner is still used for fault-injection steps.
    pub async fn run_against_existing(
        &self,
        scenario: &Scenario,
        client: ClusterClient,
        metrics: Arc<MetricsCollector>,
    ) -> RunReport {
        let caps = self.provisioner.capabilities();
        if let Err(e) = check_capabilities(scenario, caps) {
            return RunReport {
                elapsed: Duration::ZERO,
                result: Err(e),
                metrics: metrics.snapshot(),
            };
        }

        // Bookend with explicit samples so the throughput delta
        // covers the full run — short bursts can complete entirely
        // between two periodic samples otherwise.
        metrics.snapshot_now(&client).await;
        let poller = metrics::spawn_poller(client.clone(), metrics.clone(), POLL_INTERVAL);

        let started = metrics.start();
        let exec_result = self.execute(&client, scenario, &metrics).await;
        let elapsed = started.elapsed();

        poller.abort();
        metrics.snapshot_now(&client).await;

        RunReport {
            elapsed,
            result: exec_result,
            metrics: metrics.snapshot(),
        }
    }

    async fn execute(
        &self,
        client: &ClusterClient,
        scenario: &Scenario,
        metrics: &Arc<MetricsCollector>,
    ) -> Result<(), RunError> {
        let mut ctx = RunCtx::default();
        for step in &scenario.steps {
            self.dispatch(client, metrics, &mut ctx, step).await?;
        }
        ctx.join_all().await
    }

    async fn dispatch(
        &self,
        client: &ClusterClient,
        metrics: &Arc<MetricsCollector>,
        ctx: &mut RunCtx,
        step: &Step,
    ) -> Result<(), RunError> {
        match &step.action {
            Action::Submit(s) => self.run_submit(client, metrics, ctx, s).await,
            Action::SubmitBatch(s) => self.run_submit_batch(client, metrics, ctx, s).await,
            Action::AsyncBranch(b) => {
                let handle = self.spawn_branch(client, metrics, b);
                ctx.branches.push(handle);
                Ok(())
            }
            Action::Concurrent(c) => self.run_concurrent(client, metrics, ctx, c).await,
            Action::Wait(w) => {
                tokio::time::sleep(w.duration).await;
                Ok(())
            }
            Action::WaitForLevel(w) => self.run_wait_for_level(client, ctx, w).await,
            Action::GetBalance(g) => self.run_get_balance(client, ctx.last_killed, g).await,
            Action::GetPipelineIndex(g) => {
                self.run_get_pipeline_index(client, ctx.last_killed, g)
                    .await
            }
            Action::AssertBalance(a) => self.run_assert_balance(client, ctx.last_killed, a).await,
            Action::AssertBalanceSum(a) => {
                self.run_assert_balance_sum(client, ctx.last_killed, a)
                    .await
            }
            Action::AssertPipelineCaughtUp(a) => {
                self.run_assert_pipeline_caught_up(client, ctx.last_killed, a)
                    .await
            }
            Action::AssertTxStatus(a) => self.run_assert_tx_status(client, ctx, a).await,
            Action::AssertLeader(a) => self.run_assert_leader(client, ctx.last_killed, a).await,
            Action::StopNode(s) => {
                self.run_provisioner_node(client, ctx, &s.node, ProvFault::Stop)
                    .await
            }
            Action::KillNode(s) => {
                self.run_provisioner_node(client, ctx, &s.node, ProvFault::Kill)
                    .await
            }
            Action::StartNode(s) => {
                self.run_provisioner_node(client, ctx, &s.node, ProvFault::Start)
                    .await
            }
            Action::RestartNode(s) => {
                self.run_provisioner_node(client, ctx, &s.node, ProvFault::Restart)
                    .await
            }
            Action::PartitionPair(p) => self.run_partition_pair(client, ctx.last_killed, p).await,
            Action::HealPartition(h) => self.run_heal_partition(client, ctx.last_killed, h).await,
            Action::RegisterFunction(r) => {
                client
                    .register_function(&r.name, &r.binary, r.override_existing)
                    .await
                    .map_err(client_err)?;
                Ok(())
            }
            Action::UnregisterFunction(u) => {
                client
                    .unregister_function(&u.name)
                    .await
                    .map_err(client_err)?;
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
        metrics: &Arc<MetricsCollector>,
        ctx: &mut RunCtx,
        s: &Submit,
    ) -> Result<(), RunError> {
        let user_ref = user_ref_of(&s.op);
        let started = Instant::now();
        let tx_id = self.do_submit(client, &s.op, s.wait, s.retry).await?;
        metrics.record_submit_latency(started.elapsed());
        ctx.bindings.insert(user_ref, tx_id);
        Ok(())
    }

    async fn run_submit_batch(
        &self,
        client: &ClusterClient,
        metrics: &Arc<MetricsCollector>,
        ctx: &mut RunCtx,
        s: &SubmitBatch,
    ) -> Result<(), RunError> {
        match s.kind.clone() {
            BatchKind::Dynamic {
                base,
                repeat,
                mut batch_size,
            } => {
                if batch_size == 0 {
                    batch_size = base.len() as u32;
                }
                self.run_submit_batched(client, metrics, ctx, s, &base, repeat, batch_size)
                    .await
            }
            BatchKind::Static(base) => {
                self.run_submit_batched(client, metrics, ctx, s, &base, 1, base.len() as u32)
                    .await
            }
        }
    }

    /// Dispatch `base.len() * repeat` ops in `submit_batch` RPCs of at
    /// most `batch_size` ops each. `batch_size` is a *chunk size*, not a
    /// multiplier: changing it changes how many RPCs go out and how big
    /// each one is, but never the total op count. Chunk boundaries do
    /// not have to align with base boundaries.
    #[allow(clippy::too_many_arguments)]
    async fn run_submit_batched(
        &self,
        client: &ClusterClient,
        metrics: &Arc<MetricsCollector>,
        ctx: &mut RunCtx,
        s: &SubmitBatch,
        base: &[SubmitOp],
        repeat: u32,
        batch_size: u32,
    ) -> Result<(), RunError> {
        if base.is_empty() || repeat == 0 {
            return Ok(());
        }
        let chunk_cap = batch_size.max(1) as usize;
        let stride = base.len() as u64;
        let total_ops: u64 = stride * repeat as u64;
        let total_chunks: u64 = total_ops.div_ceil(chunk_cap as u64);
        let rate_start = if s.rate > 0 {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let mut chunk: Vec<SubmitOp> = Vec::with_capacity(chunk_cap);

        for chunk_idx in 0..total_chunks {
            let start_op = chunk_idx * chunk_cap as u64;
            let end_op = (start_op + chunk_cap as u64).min(total_ops);

            // Rate throttle: chunk `i` starts no earlier than
            // `(i * chunk_cap) / rate` seconds after the first.
            if let (Some(start), Some(target)) =
                (rate_start, rate_target_delay(start_op as usize, s.rate))
            {
                let elapsed = start.elapsed();
                if elapsed < target {
                    tokio::time::sleep(target - elapsed).await;
                }
            }

            chunk.clear();
            for global_op_idx in start_op..end_op {
                let iter = global_op_idx / stride;
                let base_idx = (global_op_idx % stride) as usize;
                let offset = iter * stride;
                chunk.push(with_user_ref_offset(base[base_idx].clone(), offset));
            }

            let started = Instant::now();
            let tx_ids = submit_batch_once(client, &chunk, s.wait).await?;
            if s.wait != WaitLevel::None {
                metrics.record_submit_latency(started.elapsed());
            }
            for (op, tx_id) in chunk.iter().zip(tx_ids.iter()) {
                ctx.bindings.insert(user_ref_of(op), *tx_id);
            }
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
        metrics: &Arc<MetricsCollector>,
        branch: &AsyncBranch,
    ) -> JoinHandle<Result<(), RunError>> {
        let provisioner = self.provisioner.clone();
        let client = client.clone();
        let metrics = metrics.clone();
        let steps = branch.steps.clone();
        tokio::spawn(async move {
            let runner = ScenarioRunner::new(provisioner);
            let mut ctx = RunCtx::default();
            for step in &steps {
                runner.dispatch(&client, &metrics, &mut ctx, step).await?;
            }
            ctx.join_all().await
        })
    }

    /// Fork-join concurrent step block. Each branch runs in its own
    /// task with a private `RunCtx`. After every branch has finished,
    /// their bindings are merged into the parent `ctx` (later-finishing
    /// branches win on conflicting `user_ref`). The first error
    /// encountered is propagated; remaining branches are awaited first
    /// so spawned work doesn't leak past the step boundary.
    ///
    /// Returns a `Pin<Box<...>>` rather than `impl Future` to break the
    /// recursive type cycle through `dispatch` — without it the
    /// compiler can't prove the spawned futures are `Send`.
    fn run_concurrent<'a>(
        &'a self,
        client: &'a ClusterClient,
        metrics: &'a Arc<MetricsCollector>,
        ctx: &'a mut RunCtx,
        c: &'a Concurrent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), RunError>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut handles: Vec<JoinHandle<Result<RunCtx, RunError>>> =
                Vec::with_capacity(c.branches.len());
            for branch_steps in &c.branches {
                let provisioner = self.provisioner.clone();
                let client = client.clone();
                let metrics = metrics.clone();
                let steps = branch_steps.clone();
                handles.push(tokio::spawn(async move {
                    let runner = ScenarioRunner::new(provisioner);
                    let mut local = RunCtx::default();
                    for step in &steps {
                        runner.dispatch(&client, &metrics, &mut local, step).await?;
                    }
                    // Drain any AsyncBranches spawned inside this branch so
                    // nothing escapes the `Concurrent` step boundary.
                    let branches = std::mem::take(&mut local.branches);
                    let mut first_err: Option<RunError> = None;
                    for h in branches {
                        match h.await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                if first_err.is_none() {
                                    first_err = Some(e);
                                }
                            }
                            Err(join_err) => {
                                if first_err.is_none() {
                                    first_err = Some(RunError::Client(format!(
                                        "branch panicked: {join_err}"
                                    )));
                                }
                            }
                        }
                    }
                    if let Some(e) = first_err {
                        return Err(e);
                    }
                    Ok(local)
                }));
            }

            let mut first_error: Option<RunError> = None;
            let mut joined: Vec<RunCtx> = Vec::with_capacity(handles.len());
            for h in handles {
                match h.await {
                    Ok(Ok(local)) => joined.push(local),
                    Ok(Err(e)) => {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                    Err(join_err) => {
                        if first_error.is_none() {
                            first_error =
                                Some(RunError::Client(format!("branch panicked: {join_err}")));
                        }
                    }
                }
            }
            for local in joined {
                for (user_ref, tx_id) in local.bindings {
                    ctx.bindings.insert(user_ref, tx_id);
                }
                if let Some(idx) = local.last_killed {
                    ctx.last_killed = Some(idx);
                }
            }
            match first_error {
                Some(e) => Err(e),
                None => Ok(()),
            }
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
        let _node = self.resolve_node_idx(client, &w.node, ctx.last_killed)?;
        let tx = resolve_tx(ctx, &w.tx)?;
        let target = pipeline_to_status_threshold(w.level);
        let timeout = Duration::from_secs(30);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Proto `TransactionStatus`: PENDING=0, COMPUTED=1,
            // COMMITTED=2, ON_SNAPSHOT=3, ERROR=4, TX_NOT_FOUND=5.
            // Only 1..=3 are pipeline-stage progress; 4 and 5 are
            // terminal tags, NOT higher levels — treating the field
            // as an ordering would let `TX_NOT_FOUND=5 >= ON_SNAPSHOT=3`
            // silently satisfy the wait when the queried node has no
            // record of the tx (e.g. post-failover loss).
            let (status, fail_reason) = client
                .get_transaction_status(tx)
                .await
                .map_err(client_err)?;
            match status {
                s if (1..=3).contains(&s) && s >= target => return Ok(()),
                4 => {
                    return Err(RunError::AssertionFailed(format!(
                        "tx {tx} reached ERROR status (fail_reason={fail_reason}) while \
                         waiting for level >= {target}"
                    )));
                }
                // 0 (PENDING) or 5 (TX_NOT_FOUND): keep polling. A
                // freshly-elected leader may briefly not know the tx;
                // the outer deadline caps the wait.
                _ => {}
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
        last_killed: Option<usize>,
        g: &GetBalance,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, &g.node, last_killed)?;
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
        last_killed: Option<usize>,
        g: &GetPipelineIndex,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, &g.node, last_killed)?;
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
        last_killed: Option<usize>,
        a: &AssertBalance,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, &a.node, last_killed)?;
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
        last_killed: Option<usize>,
        a: &AssertBalanceSum,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, &a.node, last_killed)?;
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
        last_killed: Option<usize>,
        a: &AssertPipelineCaughtUp,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, &a.node, last_killed)?;
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
        last_killed: Option<usize>,
        a: &AssertLeader,
    ) -> Result<(), RunError> {
        match &a.expected {
            Some(sel) => {
                let idx = self.resolve_node_idx(client, sel, last_killed)?;
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
                    if let Ok(pi) = client.node(i).get_pipeline_index().await
                        && pi.is_leader
                    {
                        return Ok(());
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
        ctx: &mut RunCtx,
        sel: &NodeSelector,
        fault: ProvFault,
    ) -> Result<(), RunError> {
        let idx = self.resolve_node_idx(client, sel, ctx.last_killed)?;
        match fault {
            ProvFault::Stop => {
                self.provisioner.stop_node(idx).await?;
                ctx.last_killed = Some(idx);
            }
            ProvFault::Kill => {
                self.provisioner.kill_node(idx).await?;
                ctx.last_killed = Some(idx);
            }
            ProvFault::Start => self.provisioner.start_node(idx).await?,
            ProvFault::Restart => self.provisioner.restart_node(idx).await?,
        }
        Ok(())
    }

    async fn run_partition_pair(
        &self,
        client: &ClusterClient,
        last_killed: Option<usize>,
        p: &PartitionPair,
    ) -> Result<(), RunError> {
        let a = self.resolve_node_idx(client, &p.a, last_killed)?;
        let b = self.resolve_node_idx(client, &p.b, last_killed)?;
        self.provisioner.partition_pair(a, b).await?;
        Ok(())
    }

    async fn run_heal_partition(
        &self,
        client: &ClusterClient,
        last_killed: Option<usize>,
        h: &HealPartition,
    ) -> Result<(), RunError> {
        let a = self.resolve_node_idx(client, &h.a, last_killed)?;
        let b = self.resolve_node_idx(client, &h.b, last_killed)?;
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
    /// runner currently has. `LastKilled` reads from `last_killed`,
    /// which is per-task state populated by `KillNode` / `StopNode`.
    fn resolve_node_idx(
        &self,
        client: &ClusterClient,
        sel: &NodeSelector,
        last_killed: Option<usize>,
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
            NodeSelector::LastKilled => {
                last_killed.ok_or_else(|| RunError::UnresolvableNode(sel.clone()))
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

// `user_ref == 0` is the dedup opt-out sentinel; preserve it through offset.
fn with_user_ref_offset(op: SubmitOp, offset: u64) -> SubmitOp {
    let shift = |user_ref: u64| if user_ref == 0 { 0 } else { user_ref + offset };
    match op {
        SubmitOp::Deposit {
            account,
            amount,
            user_ref,
        } => SubmitOp::Deposit {
            account,
            amount,
            user_ref: shift(user_ref),
        },
        SubmitOp::Withdraw {
            account,
            amount,
            user_ref,
        } => SubmitOp::Withdraw {
            account,
            amount,
            user_ref: shift(user_ref),
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
            user_ref: shift(user_ref),
        },
        SubmitOp::Function {
            name,
            params,
            user_ref,
        } => SubmitOp::Function {
            name,
            params,
            user_ref: shift(user_ref),
        },
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
        WaitLevel::ClusterCommit => Some(proto_ledger::WaitLevel::ClusterCommit),
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

/// Target delay for the `i`-th op in a `SubmitBatch` with the given
/// rate. `rate == 0` disables throttling (returns `None`); otherwise
/// returns `i / rate` seconds.
fn rate_target_delay(op_index: usize, rate: u32) -> Option<Duration> {
    if rate == 0 {
        None
    } else {
        Some(Duration::from_secs_f64(op_index as f64 / rate as f64))
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
            Action::Concurrent(c) => {
                for branch in &c.branches {
                    for nested in branch {
                        walk(nested, caps)?;
                    }
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

/// Send a homogeneous batch as a single `submit_batch` RPC. Returns one
/// transaction id per op, in input order.
///
/// Today this only supports all-Deposit batches: the cluster client's
/// other batch APIs either don't exist (Withdraw, Function) or drop
/// the per-op `user_ref` on the wire (`transfer_batch_and_wait`
/// hardcodes `user_ref = 0`), which would break `user_ref → tx_id`
/// bindings.
async fn submit_batch_once(
    client: &ClusterClient,
    ops: &[SubmitOp],
    wait: WaitLevel,
) -> Result<Vec<u64>, RunError> {
    if ops.is_empty() {
        return Ok(Vec::new());
    }
    let mut deposits: Vec<(u64, u64, u64)> = Vec::with_capacity(ops.len());
    for op in ops {
        match op {
            SubmitOp::Deposit {
                account,
                amount,
                user_ref,
            } => deposits.push((*account, *amount, *user_ref)),
            _ => {
                return Err(RunError::Client(
                    "submit_batch only supports Deposit ops today \
                     (Transfer, Withdraw, Function batch APIs not yet wired)"
                        .into(),
                ));
            }
        }
    }
    if let Some(wl) = wait_to_proto(wait) {
        let results = client
            .deposit_batch_and_wait(&deposits, wl)
            .await
            .map_err(client_err)?;
        Ok(results.into_iter().map(|r| r.tx_id).collect())
    } else {
        client.deposit_batch(&deposits).await.map_err(client_err)
    }
}

// ============================================================
// Tests — pure functions only (no real cluster, no real backend)
// ============================================================

#[cfg(test)]
mod tests {
    use testing::scenario::{
        Action, AsyncBranch, HealPartition, KillNode, NodeSelector as Sel, PartitionPair, Scenario,
        Step, StopNode,
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
        assert!(
            matches!(err, RunError::UnsupportedCapability { .. }),
            "got {err:?}"
        );
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
        assert!(
            matches!(err, RunError::UnsupportedCapability { .. }),
            "got {err:?}"
        );
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
        assert!(
            matches!(err, RunError::UnsupportedCapability { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn rate_target_delay_zero_disables_throttle() {
        assert_eq!(super::rate_target_delay(0, 0), None);
        assert_eq!(super::rate_target_delay(999, 0), None);
    }

    #[test]
    fn rate_target_delay_at_10_ops_per_sec() {
        assert_eq!(super::rate_target_delay(0, 10), Some(Duration::ZERO));
        assert_eq!(
            super::rate_target_delay(5, 10),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            super::rate_target_delay(10, 10),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn rate_target_delay_at_1000_ops_per_sec() {
        // 1ms per op, 100 ops → 100ms
        assert_eq!(
            super::rate_target_delay(100, 1000),
            Some(Duration::from_millis(100))
        );
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

    #[test]
    fn with_user_ref_offset_preserves_zero_for_all_variants() {
        let offset = 1_000;
        let variants = [
            SubmitOp::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            },
            SubmitOp::Withdraw {
                account: 1,
                amount: 1,
                user_ref: 0,
            },
            SubmitOp::Transfer {
                from: 1,
                to: 2,
                amount: 1,
                user_ref: 0,
            },
            SubmitOp::Function {
                name: "noop".into(),
                params: vec![],
                user_ref: 0,
            },
        ];
        for op in variants {
            let shifted = with_user_ref_offset(op.clone(), offset);
            assert_eq!(
                user_ref_of(&shifted),
                0,
                "user_ref == 0 must be preserved through offset for {op:?}"
            );
        }
    }

    #[test]
    fn with_user_ref_offset_adds_offset_for_nonzero() {
        let shifted = with_user_ref_offset(
            SubmitOp::Deposit {
                account: 1,
                amount: 1,
                user_ref: 1,
            },
            100,
        );
        assert_eq!(user_ref_of(&shifted), 101);
    }
}
