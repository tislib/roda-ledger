//! Shared pipeline-wait primitive. A single [`Waiter`] per node, held
//! behind `Arc`, blocks until a transaction reaches a wait level. Both
//! the ledger handler (for `*_and_wait` RPCs) and the latency-probe
//! handler share the one instance.

pub mod index_watch;
pub mod wait_for_transaction_level;

use crate::consensus::state::Consensus;
use crate::ledger_slot::LedgerSlot;
use index_watch::IndexWatch;
use ledger::ledger::PipelineIndexKind;
use std::sync::Arc;

pub struct Waiter {
    ledger: Arc<LedgerSlot>,
    consensus: Arc<Consensus>,
    // Reactive mirrors of the ledger pipeline indexes, fed by `record` (wired as
    // the ledger index hook). ClusterCommit is not here — it advances via raft
    // quorum, not the ledger hook (see `wait_for_transaction_level`).
    compute: IndexWatch,
    commit: IndexWatch,
    snapshot: IndexWatch,
}

impl Waiter {
    pub fn new(ledger: Arc<LedgerSlot>, consensus: Arc<Consensus>) -> Self {
        // Seed from the live ledger so we don't miss the gap before the hook is
        // registered; `record` advances them from there.
        let l = ledger.current();
        let (compute, commit, snapshot) = (
            IndexWatch::new(l.last_compute_id()),
            IndexWatch::new(l.last_commit_id()),
            IndexWatch::new(l.last_snapshot_id()),
        );
        Self {
            ledger,
            consensus,
            compute,
            commit,
            snapshot,
        }
    }

    /// Feed a ledger index advance into the matching watch. Wire as the ledger
    /// index hook (`Ledger::set_index_hook`); must stay non-blocking.
    pub fn record(&self, kind: PipelineIndexKind, value: u64) {
        match kind {
            PipelineIndexKind::Compute => self.compute.advance(value),
            PipelineIndexKind::Commit => self.commit.advance(value),
            PipelineIndexKind::Snapshot => self.snapshot.advance(value),
        }
    }
}
