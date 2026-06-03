//! Step primitives.
//!
//! `Step` is the unit of scenario execution. It pairs a `StepMeta` —
//! the shared schema across every variant — with an `Action` — the
//! variant payload describing what the step does. Steps are pure data;
//! the runner consumes them but doesn't build them here.

use std::time::Duration;

use super::types::{
    NodeSelector, PipelineLevel, RetryConfig, SubmitOp, TxRef, TxStatus, WaitLevel,
};

/// One step in a scenario. Common fields live in `meta`; the variant
/// payload lives in `action`.
#[derive(Clone, Debug)]
pub struct Step {
    pub meta: StepMeta,
    pub action: Action,
}

impl Step {
    pub fn new(action: Action) -> Self {
        Self {
            meta: StepMeta::default(),
            action,
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.meta.label = Some(label.into());
        self
    }

    pub fn label(&self) -> Option<&str> {
        self.meta.label.as_deref()
    }

    pub fn kind_name(&self) -> &'static str {
        self.action.kind_name()
    }
}

/// Standard schema carried by every step. Currently just a label;
/// designed to grow (timeouts, telemetry tags, etc.) without churning
/// the per-variant payload structs.
#[derive(Clone, Debug, Default)]
pub struct StepMeta {
    /// Optional human-readable label. Surfaced in progress reports and
    /// failure messages. If absent, runners derive a label from the
    /// action shape (see `Action::kind_name`).
    pub label: Option<String>,
}

/// The verb of a step. One variant per kind of action; each carries its
/// own payload struct so adding fields is local to that variant.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Action {
    // ---- Submission ----
    Submit(Submit),
    SubmitBatch(SubmitBatch),

    // ---- Concurrency ----
    AsyncBranch(AsyncBranch),
    Concurrent(Concurrent),

    // ---- Synchronization ----
    Wait(Wait),
    WaitForLevel(WaitForLevel),

    // ---- Reads (telemetry) ----
    GetBalance(GetBalance),
    GetPipelineIndex(GetPipelineIndex),

    // ---- Assertions ----
    AssertBalance(AssertBalance),
    AssertBalanceSum(AssertBalanceSum),
    AssertPipelineCaughtUp(AssertPipelineCaughtUp),
    AssertTxStatus(AssertTxStatus),
    AssertLeader(AssertLeader),

    // ---- Fault injection ----
    StopNode(StopNode),
    KillNode(KillNode),
    StartNode(StartNode),
    RestartNode(RestartNode),
    PartitionPair(PartitionPair),
    HealPartition(HealPartition),

    // ---- WASM extensions ----
    RegisterFunction(RegisterFunction),
    UnregisterFunction(UnregisterFunction),
}

impl Action {
    /// Static name of the variant, useful for telemetry and fall-back
    /// log lines when no label is set on `StepMeta`.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Action::Submit(_) => "submit",
            Action::SubmitBatch(_) => "submit_batch",
            Action::AsyncBranch(_) => "async_branch",
            Action::Concurrent(_) => "concurrent",
            Action::Wait(_) => "wait",
            Action::WaitForLevel(_) => "wait_for_level",
            Action::GetBalance(_) => "get_balance",
            Action::GetPipelineIndex(_) => "get_pipeline_index",
            Action::AssertBalance(_) => "assert_balance",
            Action::AssertBalanceSum(_) => "assert_balance_sum",
            Action::AssertPipelineCaughtUp(_) => "assert_pipeline_caught_up",
            Action::AssertTxStatus(_) => "assert_tx_status",
            Action::AssertLeader(_) => "assert_leader",
            Action::StopNode(_) => "stop_node",
            Action::KillNode(_) => "kill_node",
            Action::StartNode(_) => "start_node",
            Action::RestartNode(_) => "restart_node",
            Action::PartitionPair(_) => "partition_pair",
            Action::HealPartition(_) => "heal_partition",
            Action::RegisterFunction(_) => "register_function",
            Action::UnregisterFunction(_) => "unregister_function",
        }
    }
}

// ============================================================
// Submission
// ============================================================

/// Submit a single operation. Always targets the current leader —
/// followers reject writes — so there is no node selector. If
/// `wait != WaitLevel::None`, the runner waits for the tx to reach
/// that pipeline level before advancing. References to this submission
/// later in the scenario use the `user_ref` carried by `op` via
/// `TxRef::UserRef(...)`.
#[derive(Clone, Debug)]
pub struct Submit {
    pub op: SubmitOp,
    pub wait: WaitLevel,
    /// If set, the runner re-issues the submit up to
    /// `retry.max_retries` additional times on failure. `None` means
    /// one attempt only.
    pub retry: Option<RetryConfig>,
}

/// Submit many operations as a single step. Like `Submit`, always
/// targets the leader. Per-submission `wait` level and `retry` policy
/// live at the top level so they apply uniformly regardless of how the
/// op list was produced. The op list itself is described by `kind`.
#[derive(Clone, Debug)]
pub struct SubmitBatch {
    pub wait: WaitLevel,
    pub retry: Option<RetryConfig>,
    /// Throttle dispatch to at most this many ops per second. `0`
    /// disables throttling — submit as fast as the cluster accepts.
    /// When `wait != WaitLevel::None`, the actual achieved rate is
    /// bounded by the cluster's response time as well as this cap.
    pub rate: u32,
    pub kind: BatchKind,
}

/// How the batch's operation list is produced.
#[derive(Clone, Debug)]
pub enum BatchKind {
    /// A literal list. Submitted as one `submit_batch` RPC.
    Static(Vec<SubmitOp>),
    /// Repeat `base` `repeat` times for a total of `base.len() * repeat`
    /// ops, dispatched in `submit_batch` RPCs of at most `batch_size`
    /// ops each. `batch_size` is a *chunk size*, not a multiplier: it
    /// changes how many RPCs go out and how big each one is, never the
    /// total op count. `batch_size: 0` is treated as `base.len()` —
    /// one RPC per base copy.
    ///
    /// Each emitted op's `user_ref` is offset by `iter * base.len()` so
    /// every op has a unique `user_ref` across the whole batch. Chunk
    /// boundaries do not have to align with base boundaries.
    Dynamic {
        base: Vec<SubmitOp>,
        repeat: u32,
        batch_size: u32,
    },
}

// ============================================================
// Concurrency
// ============================================================

/// Fork off a sub-flow that runs concurrently with the main scenario.
/// The branch starts when this step is reached; the main flow continues
/// immediately. Branches are awaited at end-of-scenario by default; a
/// later `JoinBranch` step kind can explicitly synchronize earlier.
#[derive(Clone, Debug)]
pub struct AsyncBranch {
    /// Optional handle for explicit joining. If unset, the branch is
    /// only awaitable implicitly at end-of-scenario.
    pub name: Option<String>,
    pub steps: Vec<Step>,
}

/// Fork N step-lists, run them in parallel, join all before the step
/// returns. Each branch runs its steps sequentially; branches do not
/// share a `RunCtx` while in flight — they build private bindings
/// that the runner merges into the parent context after every branch
/// has finished. Any branch returning an error fails the whole step;
/// the other branches are still awaited before the error propagates.
///
/// Contrast with [`AsyncBranch`], which is fire-and-forget and joined
/// only at end-of-scenario. Use `Concurrent` when the *correctness*
/// of subsequent steps depends on every branch having completed —
/// e.g. parallel submits racing a kill/restart cycle, where the
/// scenario's drain and assertions must run only after both have
/// settled.
#[derive(Clone, Debug)]
pub struct Concurrent {
    /// Each inner `Vec<Step>` is one branch. Steps within a branch
    /// are sequential; branches are concurrent.
    pub branches: Vec<Vec<Step>>,
    /// Optional per-branch labels for trace output. Length must
    /// equal `branches.len()` if set.
    pub labels: Option<Vec<String>>,
}

// ============================================================
// Synchronization
// ============================================================

/// Sleep for a fixed duration. Pure wall-clock wait.
#[derive(Clone, Debug)]
pub struct Wait {
    pub duration: Duration,
}

/// Wait until a transaction reaches `level` on `node`. Times out per
/// the runner's configured per-step timeout.
#[derive(Clone, Debug)]
pub struct WaitForLevel {
    pub node: NodeSelector,
    pub tx: TxRef,
    pub level: PipelineLevel,
}

// ============================================================
// Reads — emit values into the run telemetry
// ============================================================

/// Read a balance and surface it in the run record. Does not feed back
/// into later steps — assertions take literal expected values.
#[derive(Clone, Debug)]
pub struct GetBalance {
    pub node: NodeSelector,
    pub account: u64,
}

/// Read pipeline indices and surface them in the run record. Same
/// telemetry-only semantics as `GetBalance`.
#[derive(Clone, Debug)]
pub struct GetPipelineIndex {
    pub node: NodeSelector,
}

// ============================================================
// Assertions
// ============================================================

#[derive(Clone, Debug)]
pub struct AssertBalance {
    pub node: NodeSelector,
    pub account: u64,
    pub expected: i64,
}

/// The zero-sum invariant: total of all account balances on `node`
/// across `[0, max_account]` equals zero. No `expected` parameter — the
/// only valid sum is zero.
#[derive(Clone, Debug)]
pub struct AssertBalanceSum {
    pub node: NodeSelector,
    pub max_account: u64,
}

/// All three pipeline indices on `node` are >= `target`.
#[derive(Clone, Debug)]
pub struct AssertPipelineCaughtUp {
    pub node: NodeSelector,
    pub target: u64,
}

#[derive(Clone, Debug)]
pub struct AssertTxStatus {
    pub tx: TxRef,
    pub expected: TxStatus,
}

/// Assert leadership state. If `expected` is set, the leader must be
/// exactly that node. If `None`, just assert *some* leader exists.
#[derive(Clone, Debug)]
pub struct AssertLeader {
    pub expected: Option<NodeSelector>,
}

// ============================================================
// Fault injection
// ============================================================

#[derive(Clone, Debug)]
pub struct StopNode {
    pub node: NodeSelector,
}

#[derive(Clone, Debug)]
pub struct KillNode {
    pub node: NodeSelector,
}

#[derive(Clone, Debug)]
pub struct StartNode {
    pub node: NodeSelector,
}

#[derive(Clone, Debug)]
pub struct RestartNode {
    pub node: NodeSelector,
}

#[derive(Clone, Debug)]
pub struct PartitionPair {
    pub a: NodeSelector,
    pub b: NodeSelector,
}

#[derive(Clone, Debug)]
pub struct HealPartition {
    pub a: NodeSelector,
    pub b: NodeSelector,
}

// ============================================================
// WASM extensions
// ============================================================

/// Always targets the leader — function registration is a write.
#[derive(Clone, Debug)]
pub struct RegisterFunction {
    pub name: String,
    pub binary: Vec<u8>,
    pub override_existing: bool,
}

/// Always targets the leader — function unregistration is a write.
#[derive(Clone, Debug)]
pub struct UnregisterFunction {
    pub name: String,
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_action_has_kind_name() {
        // Construct one of every variant. The exhaustive match in
        // `Action::kind_name` already prevents missed variants at
        // compile time; this test additionally guards against someone
        // adding a `_ =>` arm and a variant returning empty.
        let actions: Vec<Action> = vec![
            Action::Submit(Submit {
                op: SubmitOp::Deposit {
                    account: 1,
                    amount: 1,
                    user_ref: 1,
                },
                wait: WaitLevel::None,
                retry: None,
            }),
            Action::SubmitBatch(SubmitBatch {
                wait: WaitLevel::None,
                retry: None,
                rate: 0,
                kind: BatchKind::Dynamic {
                    base: vec![SubmitOp::Deposit {
                        account: 1,
                        amount: 1,
                        user_ref: 1,
                    }],
                    repeat: 10,
                    batch_size: 0,
                },
            }),
            Action::AsyncBranch(AsyncBranch {
                name: None,
                steps: vec![],
            }),
            Action::Concurrent(Concurrent {
                branches: vec![],
                labels: None,
            }),
            Action::Wait(Wait {
                duration: Duration::from_millis(0),
            }),
            Action::WaitForLevel(WaitForLevel {
                node: NodeSelector::Leader,
                tx: TxRef::UserRef(1),
                level: PipelineLevel::Committed,
            }),
            Action::GetBalance(GetBalance {
                node: NodeSelector::Leader,
                account: 1,
            }),
            Action::GetPipelineIndex(GetPipelineIndex {
                node: NodeSelector::Leader,
            }),
            Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: 1,
                expected: 0,
            }),
            Action::AssertBalanceSum(AssertBalanceSum {
                node: NodeSelector::Leader,
                max_account: 100,
            }),
            Action::AssertPipelineCaughtUp(AssertPipelineCaughtUp {
                node: NodeSelector::Leader,
                target: 0,
            }),
            Action::AssertTxStatus(AssertTxStatus {
                tx: TxRef::UserRef(1),
                expected: TxStatus::Committed,
            }),
            Action::AssertLeader(AssertLeader { expected: None }),
            Action::StopNode(StopNode {
                node: NodeSelector::Leader,
            }),
            Action::KillNode(KillNode {
                node: NodeSelector::Leader,
            }),
            Action::StartNode(StartNode {
                node: NodeSelector::Leader,
            }),
            Action::RestartNode(RestartNode {
                node: NodeSelector::Leader,
            }),
            Action::PartitionPair(PartitionPair {
                a: NodeSelector::Index(0),
                b: NodeSelector::Index(1),
            }),
            Action::HealPartition(HealPartition {
                a: NodeSelector::Index(0),
                b: NodeSelector::Index(1),
            }),
            Action::RegisterFunction(RegisterFunction {
                name: "f".into(),
                binary: vec![],
                override_existing: false,
            }),
            Action::UnregisterFunction(UnregisterFunction { name: "f".into() }),
        ];
        for a in &actions {
            assert!(!a.kind_name().is_empty(), "kind_name returned empty");
        }
    }

    #[test]
    fn step_label_round_trip() {
        let s = Step::new(Action::Wait(Wait {
            duration: Duration::from_millis(5),
        }))
        .with_label("warm up");
        assert_eq!(s.label(), Some("warm up"));
        assert_eq!(s.kind_name(), "wait");
    }
}
