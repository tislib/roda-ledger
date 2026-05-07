//! Scenario primitives — pure-Rust types describing an executable
//! sequence of cluster interactions.
//!
//! This module is the *vocabulary* the scenario runner speaks. It owns:
//!   - the `Step` enum and its per-variant payload structs (`step`)
//!   - shared building blocks: node selectors, wait levels, tx refs,
//!     submit ops, tx statuses (`types`)
//!   - the top-level `Scenario` container
//!
//! High-level builders (in the `testing` crate) construct values of
//! these types; the runner (added separately) consumes them. The module
//! intentionally has no dependency on `proto` so callers can build
//! scenarios without dragging in wire definitions; a translation layer
//! to/from `proto::control` belongs in the runner, not here.
//!
//! Kept distinct from the in-memory mock state in [`crate::state`] —
//! that code uses `proto::control::Scenario` directly and is on its
//! way out as the real runner lands.

pub mod step;
pub mod types;

pub use step::{
    AssertBalanceStep, AssertBalanceSumStep, AssertLeaderStep, AssertPipelineCaughtUpStep,
    AssertTxStatusStep, AsyncBranchStep, GetBalanceStep, GetPipelineIndexStep, HealPartitionStep,
    KillNodeStep, PartitionPairStep, RegisterFunctionStep, RestartNodeStep, StartNodeStep, Step,
    StopNodeStep, SubmitStep, UnregisterFunctionStep, WaitForLevelStep, WaitStep,
};
pub use types::{NodeSelector, SubmitOp, TxRef, TxStatus, WaitLevel};

/// A named, ordered sequence of steps the runner executes top-to-bottom.
/// Branches inside steps may run concurrently (see `AsyncBranchStep`).
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
