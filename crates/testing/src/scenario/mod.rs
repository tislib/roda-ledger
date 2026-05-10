//! Scenario primitives — pure-Rust types describing an executable
//! sequence of cluster interactions.
//!
//! This module is the *vocabulary* the scenario runner speaks. It owns:
//!   - the top-level `Scenario` container
//!   - the `Step` wrapper (carrying shared `StepMeta`) and the `Action`
//!     enum with one variant per action kind (`step`)
//!   - shared building blocks: node selectors, wait/pipeline levels,
//!     tx refs, submit ops, tx statuses, retry config (`types`)
//!
//! Concrete scenarios built on top of these primitives live in
//! [`crate::scenarios`]. The runner (in the `control` crate) imports
//! these types and consumes scenarios produced here. The module
//! intentionally has no dependency on `proto` so callers — control or
//! otherwise — can build scenarios without dragging in wire
//! definitions; a translation layer to/from `proto::control` belongs
//! in the runner, not here.

pub mod sim;
pub mod step;
pub mod types;

pub use step::{
    Action, AssertBalance, AssertBalanceSum, AssertLeader, AssertPipelineCaughtUp, AssertTxStatus,
    AsyncBranch, BatchKind, GetBalance, GetPipelineIndex, HealPartition, KillNode, PartitionPair,
    RegisterFunction, RestartNode, StartNode, Step, StepMeta, StopNode, Submit, SubmitBatch,
    UnregisterFunction, Wait, WaitForLevel,
};
pub use types::{NodeSelector, PipelineLevel, RetryConfig, SubmitOp, TxRef, TxStatus, WaitLevel};

/// A named, ordered sequence of steps the runner executes top-to-bottom.
/// Branches inside steps may run concurrently (see `AsyncBranch`).
#[derive(Clone, Debug)]
pub struct Scenario {
    /// Stable identifier used in the catalogue and run records.
    pub name: String,
    /// Free-form description for the UI scenario library.
    pub description: String,
    /// Steps in declared order. Empty scenarios are valid (and trivial).
    pub steps: Vec<Step>,
}

impl Scenario {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            steps: Vec::new(),
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn with_steps(mut self, steps: Vec<Step>) -> Self {
        self.steps = steps;
        self
    }

    pub fn push(&mut self, step: Step) {
        self.steps.push(step);
    }
}
