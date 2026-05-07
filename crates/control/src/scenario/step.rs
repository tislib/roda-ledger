//! Step primitives.
//!
//! `Step` is the unit of scenario execution. It is an enum with one
//! variant per kind of action; every variant carries a dedicated struct
//! holding its fields. Each struct shares the same skeleton — a
//! `label: Option<String>` for log/progress lines — plus the
//! variant-specific schema.
//!
//! Steps are pure data. They describe *what* should happen; the runner
//! (built separately) decides *how*.

use std::time::Duration;

use super::types::{NodeSelector, SubmitOp, TxRef, TxStatus, WaitLevel};

/// A single step in a scenario. The variant determines the action; the
/// inner struct carries its parameters.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Step {
    // ---- Submission ----
    Submit(SubmitStep),

    // ---- Concurrency ----
    AsyncBranch(AsyncBranchStep),

    // ---- Synchronization ----
    Wait(WaitStep),
    WaitForLevel(WaitForLevelStep),

    // ---- Reads (capture values for later steps) ----
    GetBalance(GetBalanceStep),
    GetPipelineIndex(GetPipelineIndexStep),

    // ---- Assertions ----
    AssertBalance(AssertBalanceStep),
    AssertBalanceSum(AssertBalanceSumStep),
    AssertPipelineCaughtUp(AssertPipelineCaughtUpStep),
    AssertTxStatus(AssertTxStatusStep),
    AssertLeader(AssertLeaderStep),

    // ---- Fault injection ----
    StopNode(StopNodeStep),
    KillNode(KillNodeStep),
    StartNode(StartNodeStep),
    RestartNode(RestartNodeStep),
    PartitionPair(PartitionPairStep),
    HealPartition(HealPartitionStep),

    // ---- WASM extensions ----
    RegisterFunction(RegisterFunctionStep),
    UnregisterFunction(UnregisterFunctionStep),
}

impl Step {
    /// The optional label carried by the inner step struct, if any.
    pub fn label(&self) -> Option<&str> {
        match self {
            Step::Submit(s) => s.label.as_deref(),
            Step::AsyncBranch(s) => s.label.as_deref(),
            Step::Wait(s) => s.label.as_deref(),
            Step::WaitForLevel(s) => s.label.as_deref(),
            Step::GetBalance(s) => s.label.as_deref(),
            Step::GetPipelineIndex(s) => s.label.as_deref(),
            Step::AssertBalance(s) => s.label.as_deref(),
            Step::AssertBalanceSum(s) => s.label.as_deref(),
            Step::AssertPipelineCaughtUp(s) => s.label.as_deref(),
            Step::AssertTxStatus(s) => s.label.as_deref(),
            Step::AssertLeader(s) => s.label.as_deref(),
            Step::StopNode(s) => s.label.as_deref(),
            Step::KillNode(s) => s.label.as_deref(),
            Step::StartNode(s) => s.label.as_deref(),
            Step::RestartNode(s) => s.label.as_deref(),
            Step::PartitionPair(s) => s.label.as_deref(),
            Step::HealPartition(s) => s.label.as_deref(),
            Step::RegisterFunction(s) => s.label.as_deref(),
            Step::UnregisterFunction(s) => s.label.as_deref(),
        }
    }

    /// Static name of the step variant, useful for telemetry and
    /// fall-back log lines when no label is set.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Step::Submit(_) => "submit",
            Step::AsyncBranch(_) => "async_branch",
            Step::Wait(_) => "wait",
            Step::WaitForLevel(_) => "wait_for_level",
            Step::GetBalance(_) => "get_balance",
            Step::GetPipelineIndex(_) => "get_pipeline_index",
            Step::AssertBalance(_) => "assert_balance",
            Step::AssertBalanceSum(_) => "assert_balance_sum",
            Step::AssertPipelineCaughtUp(_) => "assert_pipeline_caught_up",
            Step::AssertTxStatus(_) => "assert_tx_status",
            Step::AssertLeader(_) => "assert_leader",
            Step::StopNode(_) => "stop_node",
            Step::KillNode(_) => "kill_node",
            Step::StartNode(_) => "start_node",
            Step::RestartNode(_) => "restart_node",
            Step::PartitionPair(_) => "partition_pair",
            Step::HealPartition(_) => "heal_partition",
            Step::RegisterFunction(_) => "register_function",
            Step::UnregisterFunction(_) => "unregister_function",
        }
    }
}

// ============================================================
// Submission
// ============================================================

/// Submit a single operation. If `wait` is anything other than `None`,
/// the runner waits for the tx to reach that pipeline level before
/// advancing. `bind_tx_id` captures the resulting tx_id under a name
/// that later steps can reference via `TxRef::Bound`.
#[derive(Clone, Debug)]
pub struct SubmitStep {
    pub label: Option<String>,
    pub node: NodeSelector,
    pub op: SubmitOp,
    pub wait: WaitLevel,
    pub bind_tx_id: Option<String>,
}

// ============================================================
// Concurrency
// ============================================================

/// Fork off a sub-flow that runs concurrently with the main scenario.
/// The branch starts when this step is reached; the main scenario
/// continues immediately. Branches are awaited at end-of-scenario by
/// default; named branches can be joined explicitly by future
/// `JoinBranch` steps.
///
/// Bindings produced inside a branch are scoped to that branch unless
/// explicitly published — same scoping the runner enforces for its
/// per-branch context.
#[derive(Clone, Debug)]
pub struct AsyncBranchStep {
    pub label: Option<String>,
    /// Optional handle for explicit joining. If unset, the branch is
    /// only awaitable implicitly at end-of-scenario.
    pub name: Option<String>,
    pub steps: Vec<Step>,
}

// ============================================================
// Synchronization
// ============================================================

/// Sleep for a fixed duration. Pure wall-clock wait.
#[derive(Clone, Debug)]
pub struct WaitStep {
    pub label: Option<String>,
    pub duration: Duration,
}

/// Wait until a specific transaction reaches `level` on `node`.
/// Times out per the runner's configured per-step timeout.
#[derive(Clone, Debug)]
pub struct WaitForLevelStep {
    pub label: Option<String>,
    pub node: NodeSelector,
    pub tx: TxRef,
    pub level: WaitLevel,
}

// ============================================================
// Reads — capture values for later steps
// ============================================================

#[derive(Clone, Debug)]
pub struct GetBalanceStep {
    pub label: Option<String>,
    pub node: NodeSelector,
    pub account: u64,
    /// Bind the captured balance to this name. Other steps can
    /// reference it (e.g. an assertion templated on a relative value).
    pub bind: Option<String>,
}

#[derive(Clone, Debug)]
pub struct GetPipelineIndexStep {
    pub label: Option<String>,
    pub node: NodeSelector,
    /// Bind the captured `(compute, commit, snapshot)` tuple under this name.
    pub bind: Option<String>,
}

// ============================================================
// Assertions
// ============================================================

#[derive(Clone, Debug)]
pub struct AssertBalanceStep {
    pub label: Option<String>,
    pub node: NodeSelector,
    pub account: u64,
    pub expected: i64,
}

/// Sum of all account balances on `node` across [0, max_account].
/// Useful for the zero-sum invariant check.
#[derive(Clone, Debug)]
pub struct AssertBalanceSumStep {
    pub label: Option<String>,
    pub node: NodeSelector,
    pub max_account: u64,
    pub expected: i64,
}

/// All three pipeline indices on `node` are >= `target`.
#[derive(Clone, Debug)]
pub struct AssertPipelineCaughtUpStep {
    pub label: Option<String>,
    pub node: NodeSelector,
    pub target: u64,
}

#[derive(Clone, Debug)]
pub struct AssertTxStatusStep {
    pub label: Option<String>,
    pub tx: TxRef,
    pub expected: TxStatus,
}

/// Assert leadership state. If `expected` is set, the leader must be
/// exactly that node. If `None`, just assert *some* leader exists.
#[derive(Clone, Debug)]
pub struct AssertLeaderStep {
    pub label: Option<String>,
    pub expected: Option<NodeSelector>,
}

// ============================================================
// Fault injection
// ============================================================

#[derive(Clone, Debug)]
pub struct StopNodeStep {
    pub label: Option<String>,
    pub node: NodeSelector,
}

#[derive(Clone, Debug)]
pub struct KillNodeStep {
    pub label: Option<String>,
    pub node: NodeSelector,
}

#[derive(Clone, Debug)]
pub struct StartNodeStep {
    pub label: Option<String>,
    pub node: NodeSelector,
}

#[derive(Clone, Debug)]
pub struct RestartNodeStep {
    pub label: Option<String>,
    pub node: NodeSelector,
}

#[derive(Clone, Debug)]
pub struct PartitionPairStep {
    pub label: Option<String>,
    pub a: NodeSelector,
    pub b: NodeSelector,
}

#[derive(Clone, Debug)]
pub struct HealPartitionStep {
    pub label: Option<String>,
    pub a: NodeSelector,
    pub b: NodeSelector,
}

// ============================================================
// WASM extensions
// ============================================================

#[derive(Clone, Debug)]
pub struct RegisterFunctionStep {
    pub label: Option<String>,
    pub node: NodeSelector,
    pub name: String,
    pub binary: Vec<u8>,
    pub override_existing: bool,
}

#[derive(Clone, Debug)]
pub struct UnregisterFunctionStep {
    pub label: Option<String>,
    pub node: NodeSelector,
    pub name: String,
}
