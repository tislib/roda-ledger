//! Shared building blocks used by every step variant.
//!
//! These are deliberately plain-Rust types (no `proto`, no `serde`) so
//! the high-level scenario builder in `testing` can construct them
//! without taking a transitive dependency on the wire format. A future
//! translation layer can bridge to/from `proto::control` when scenarios
//! cross the gRPC boundary.

/// Where a step targets the cluster. Resolution happens at execution
/// time against the runner's current view; `Leader` re-resolves on
/// every step so a scenario keeps working through re-elections.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum NodeSelector {
    /// 0-based index into the cluster, mirroring `node: N` in the
    /// e2e macro DSL. Convenient for fixed-shape scenarios.
    Index(usize),
    /// Explicit `node_id`.
    Id(u64),
    /// Whichever node is leader at the moment the step runs.
    #[default]
    Leader,
    /// Any reachable node — the runner picks.
    Any,
}

/// Pipeline level a submission should wait for before the step returns.
/// Mirrors `proto::ledger::WaitLevel`, kept as a plain Rust enum to keep
/// the scenario API proto-free.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WaitLevel {
    #[default]
    None,
    Computed,
    Committed,
    OnSnapshot,
}

/// Refer to a transaction by absolute id or by a binding name set
/// earlier in the scenario (e.g. by a `SubmitStep` with `bind_tx_id`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxRef {
    Id(u64),
    Bound(String),
}

/// One submittable operation. Mirrors the `roda.ledger.v1` operation
/// shapes; intentionally not the proto type so the scenario module
/// stays decoupled from wire generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmitOp {
    Deposit {
        account: u64,
        amount: u64,
        user_ref: u64,
    },
    Withdraw {
        account: u64,
        amount: u64,
        user_ref: u64,
    },
    Transfer {
        from: u64,
        to: u64,
        amount: u64,
        user_ref: u64,
    },
    Function {
        name: String,
        params: Vec<i64>,
        user_ref: u64,
    },
}

/// Terminal-or-pipeline transaction status. Mirrors
/// `proto::control::TransactionStatus`; same proto-decoupling rationale
/// as the other types here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxStatus {
    Pending,
    Computed,
    Committed,
    OnSnapshot,
    Error,
    NotFound,
}
