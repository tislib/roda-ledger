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
    /// The node most recently killed (or stopped) by a `KillNode` /
    /// `StopNode` step in the current run-context. Lets a follow-up
    /// `StartNode` bring the same node back without the scenario
    /// knowing the cluster index ahead of time. Scoped per-task —
    /// each `AsyncBranch` has its own kill history.
    LastKilled,
}

/// Pipeline level a submission should wait for before the step returns.
/// `None` means fire-and-forget. `ClusterCommit` waits for majority
/// replication on the cluster (proto `WAIT_LEVEL_CLUSTER_COMMIT`) and
/// is what surfaces leader-failover as an observable RPC error so
/// `cluster_client::with_leader_retry` can rotate and resubmit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WaitLevel {
    #[default]
    None,
    Computed,
    Committed,
    OnSnapshot,
    ClusterCommit,
}

/// Pipeline level used by *waits* on already-submitted transactions.
/// Distinct from `WaitLevel` because there is no "wait for nothing" —
/// waits always have a concrete target level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipelineLevel {
    Computed,
    Committed,
    OnSnapshot,
}

/// Refer to a transaction by the `user_ref` an earlier `Submit` step
/// carried. The runner maps `user_ref → tx_id` internally on submission
/// completion. Single-variant enum so additional reference forms can
/// be added later (e.g. `Latest`) without breaking call sites.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxRef {
    UserRef(u64),
    TxId(u64),
    LastTx,
}

/// One submittable operation. Mirrors the `roda.ledger.v1` operation
/// shapes; intentionally not the proto type so the scenario module
/// stays decoupled from wire generation.
///
/// `user_ref` is both the idempotency key (cluster-side dedup) and the
/// scenario-side identity used by `TxRef::UserRef` references.
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
/// `proto::control::TransactionStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxStatus {
    Pending,
    Computed,
    Committed,
    OnSnapshot,
    Error,
    NotFound,
}

/// Per-`Submit` retry configuration. The runner re-issues the submit up
/// to `max_retries` additional times after the first failure, with a
/// constant `backoff_ms` delay between attempts. Kept deliberately
/// minimal — extra knobs (jitter, exponential backoff, retry-on
/// classification) get added when a real scenario needs them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub backoff_ms: u32,
}
