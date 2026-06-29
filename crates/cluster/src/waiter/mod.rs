//! Shared pipeline-wait primitive. A single [`Waiter`] per node, held
//! behind `Arc`, blocks until a transaction reaches a wait level. Both
//! the ledger handler (for `*_and_wait` RPCs) and the latency-probe
//! handler share the one instance.

pub mod index_watch;
pub mod wait_for_transaction_level;

use index_watch::IndexWatch;
use ledger::ledger::PipelineIndexKind;

/// Holds a reactive **watcher** ([`IndexWatch`]) per progress index the cluster
/// waits on — not the indexes themselves. The source-of-truth indexes live in
/// the ledger pipeline (compute/commit/snapshot) and the raft quorum
/// (cluster_commit); these watches are mirrors those sources feed. Pure data
/// behind `Arc` — shared into `Consensus` (replication sender + cluster-commit
/// driver) and the gRPC handlers (client `*_and_wait`). The ledger index hook
/// feeds compute/commit/snapshot via [`Self::record`]; the raft layer feeds
/// cluster_commit via [`Self::record_cluster_commit`] at each advance site.
pub struct Waiter {
    compute: IndexWatch,
    commit: IndexWatch,
    snapshot: IndexWatch,
    cluster_commit: IndexWatch,
}

impl Waiter {
    pub fn new(compute: u64, commit: u64, snapshot: u64, cluster_commit: u64) -> Self {
        Self {
            compute: IndexWatch::new(compute),
            commit: IndexWatch::new(commit),
            snapshot: IndexWatch::new(snapshot),
            cluster_commit: IndexWatch::new(cluster_commit),
        }
    }

    /// Feed a ledger index advance (wire as the ledger index hook). Non-blocking.
    pub fn record(&self, kind: PipelineIndexKind, value: u64) {
        match kind {
            PipelineIndexKind::Compute => self.compute.advance(value),
            PipelineIndexKind::Commit => self.commit.advance(value),
            PipelineIndexKind::Snapshot => self.snapshot.advance(value),
        }
    }

    /// Feed a raft cluster-commit advance. Called at every deterministic site
    /// the RaftNode advances `cluster_commit_index`. Non-blocking.
    pub fn record_cluster_commit(&self, value: u64) {
        self.cluster_commit.advance(value);
    }

    pub fn compute(&self) -> &IndexWatch {
        &self.compute
    }
    pub fn commit(&self) -> &IndexWatch {
        &self.commit
    }
    pub fn snapshot(&self) -> &IndexWatch {
        &self.snapshot
    }
    pub fn cluster_commit(&self) -> &IndexWatch {
        &self.cluster_commit
    }
}
