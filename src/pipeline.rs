//! Pipeline: centralized ownership of inter-stage queues and global progress
//! indexes.
//!
//! Every stage in the ledger (sequencer → transactor → wal → snapshot → seal)
//! reads its inputs and publishes its progress through state that lives here.
//! Stages borrow their slice of that state via the `*_context()` accessors,
//! which return a `*Context` holding an `Arc<Pipeline>`.
//!
//! All atomic indexes are wrapped in `CachePadded` so progress updates from
//! one stage do not cause false sharing with adjacent indexes.

use crate::entities::WalEntry;
use crate::snapshot::SnapshotMessage;
use crate::transaction::Transaction;
use crate::wait_strategy::WaitStrategy;
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

/// Owns every inter-stage queue and every global progress index in the ledger.
///
/// Stages never own queues or atomics directly: they receive a typed
/// `*Context` (a thin Arc-wrapper) from `Pipeline::*_context()` and read /
/// publish through it.
pub struct Pipeline {
    // ---- inter-stage queues ----
    sequencer_to_transactor: ArrayQueue<Transaction>,
    transactor_to_wal: ArrayQueue<WalEntry>,
    wal_to_snapshot: ArrayQueue<SnapshotMessage>,

    // ---- global progress indexes (cache-padded to avoid false sharing) ----
    /// Next transaction id to be handed out by the sequencer (initialized to 1).
    sequencer_index: CachePadded<AtomicU64>,
    /// Last transaction id executed by the transactor (compute index).
    compute_index: CachePadded<AtomicU64>,
    /// Last transaction id durably written by the WAL (commit index).
    commit_index: CachePadded<AtomicU64>,
    /// Last transaction id reflected in the snapshot/indexer.
    snapshot_index: CachePadded<AtomicU64>,
    /// Last segment id sealed by the seal stage.
    seal_index: CachePadded<AtomicU32>,

    /// Global shutdown flag. Cleared by `shutdown()` to stop every stage.
    running: CachePadded<AtomicBool>,

    /// Shared wait strategy used by every stage's idle/backpressure loops.
    wait_strategy: WaitStrategy,
}

impl Pipeline {
    /// Construct a pipeline with empty queues sized from the ledger config.
    ///
    /// `small_queue_size` is used for the sequencer→transactor and
    /// wal→snapshot hops, while `wal_queue_size` is used for the
    /// transactor→wal hop which is historically sized much larger.
    pub fn new(
        small_queue_size: usize,
        wal_queue_size: usize,
        wait_strategy: WaitStrategy,
    ) -> Arc<Self> {
        Arc::new(Self {
            sequencer_to_transactor: ArrayQueue::new(small_queue_size),
            transactor_to_wal: ArrayQueue::new(wal_queue_size),
            wal_to_snapshot: ArrayQueue::new(small_queue_size),

            sequencer_index: CachePadded::new(AtomicU64::new(1)),
            compute_index: CachePadded::new(AtomicU64::new(0)),
            commit_index: CachePadded::new(AtomicU64::new(0)),
            snapshot_index: CachePadded::new(AtomicU64::new(0)),
            seal_index: CachePadded::new(AtomicU32::new(0)),

            running: CachePadded::new(AtomicBool::new(true)),
            wait_strategy,
        })
    }

    #[inline(always)]
    pub fn wait_strategy(&self) -> WaitStrategy {
        self.wait_strategy
    }

    // ─── context accessors ──────────────────────────────────────────────────

    pub fn sequencer_context(self: &Arc<Self>) -> SequencerContext {
        SequencerContext {
            pipeline: Arc::clone(self),
        }
    }

    pub fn transactor_context(self: &Arc<Self>) -> TransactorContext {
        TransactorContext {
            pipeline: Arc::clone(self),
        }
    }

    pub fn wal_context(self: &Arc<Self>) -> WalContext {
        WalContext {
            pipeline: Arc::clone(self),
        }
    }

    pub fn snapshot_context(self: &Arc<Self>) -> SnapshotContext {
        SnapshotContext {
            pipeline: Arc::clone(self),
        }
    }

    pub fn seal_context(self: &Arc<Self>) -> SealContext {
        SealContext {
            pipeline: Arc::clone(self),
        }
    }

    // ─── ledger-side direct readers / submit ────────────────────────────────

    /// Stop every stage. Idempotent.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn last_sequenced_id(&self) -> u64 {
        self.sequencer_index
            .load(Ordering::Acquire)
            .saturating_sub(1)
    }

    pub fn last_computed_id(&self) -> u64 {
        self.compute_index.load(Ordering::Acquire)
    }

    pub fn last_committed_id(&self) -> u64 {
        self.commit_index.load(Ordering::Acquire)
    }

    pub fn last_snapshot_id(&self) -> u64 {
        self.snapshot_index.load(Ordering::Acquire)
    }

    pub fn last_sealed_id(&self) -> u32 {
        self.seal_index.load(Ordering::Acquire)
    }

    /// Try to push a query onto the WAL→Snapshot queue without blocking.
    /// Used by `Ledger::query` which manages its own backpressure.
    pub fn try_push_query(&self, msg: SnapshotMessage) -> Result<(), SnapshotMessage> {
        self.wal_to_snapshot.push(msg)
    }

    // ─── recovery setters (crate-internal) ──────────────────────────────────

    pub(crate) fn set_compute_index(&self, id: u64) {
        self.compute_index.store(id, Ordering::Release);
    }

    pub(crate) fn set_snapshot_index(&self, id: u64) {
        self.snapshot_index.store(id, Ordering::Release);
    }

    pub(crate) fn set_sequencer_next_id(&self, next_id: u64) {
        self.sequencer_index.store(next_id, Ordering::Release);
    }

    pub(crate) fn set_seal_index(&self, id: u32) {
        self.seal_index.store(id, Ordering::Release);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage contexts: thin Arc<Pipeline> wrappers exposing only the slice of state
// each stage may touch. All accessors go through methods so that the storage
// layout (CachePadded, ordering, etc.) stays an internal detail.
// ─────────────────────────────────────────────────────────────────────────────

/// Slice of the pipeline visible to the sequencer (synchronous submit path).
pub struct SequencerContext {
    pipeline: Arc<Pipeline>,
}

impl SequencerContext {
    #[inline(always)]
    pub fn output(&self) -> &ArrayQueue<Transaction> {
        &self.pipeline.sequencer_to_transactor
    }

    /// Atomically fetch and increment the sequencer index. Returns the id
    /// the caller should stamp on its transaction.
    #[inline(always)]
    pub fn fetch_next_id(&self) -> u64 {
        self.pipeline
            .sequencer_index
            .fetch_add(1, Ordering::Acquire)
    }

    #[inline(always)]
    pub fn is_running(&self) -> bool {
        self.pipeline.running.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn wait_strategy(&self) -> WaitStrategy {
        self.pipeline.wait_strategy
    }

    #[inline(always)]
    pub fn last_id(&self) -> u64 {
        self.pipeline.last_sequenced_id()
    }

    pub fn set_next_id(&self, next_id: u64) {
        self.pipeline.set_sequencer_next_id(next_id);
    }
}

/// Slice of the pipeline visible to the transactor stage.
pub struct TransactorContext {
    pipeline: Arc<Pipeline>,
}

impl TransactorContext {
    #[inline(always)]
    pub fn input(&self) -> &ArrayQueue<Transaction> {
        &self.pipeline.sequencer_to_transactor
    }

    #[inline(always)]
    pub fn output(&self) -> &ArrayQueue<WalEntry> {
        &self.pipeline.transactor_to_wal
    }

    #[inline(always)]
    pub fn input_capacity(&self) -> usize {
        self.pipeline.sequencer_to_transactor.capacity()
    }

    #[inline(always)]
    pub fn get_processed_index(&self) -> u64 {
        self.pipeline.compute_index.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn set_processed_index(&self, id: u64) {
        self.pipeline.compute_index.store(id, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn is_running(&self) -> bool {
        self.pipeline.running.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn wait_strategy(&self) -> WaitStrategy {
        self.pipeline.wait_strategy
    }
}

/// Slice of the pipeline visible to the WAL stage.
pub struct WalContext {
    pipeline: Arc<Pipeline>,
}

impl WalContext {
    #[inline(always)]
    pub fn input(&self) -> &ArrayQueue<WalEntry> {
        &self.pipeline.transactor_to_wal
    }

    #[inline(always)]
    pub fn output(&self) -> &ArrayQueue<SnapshotMessage> {
        &self.pipeline.wal_to_snapshot
    }

    #[inline(always)]
    pub fn input_capacity(&self) -> usize {
        self.pipeline.transactor_to_wal.capacity()
    }

    #[inline(always)]
    pub fn get_processed_index(&self) -> u64 {
        self.pipeline.commit_index.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn set_processed_index(&self, id: u64) {
        self.pipeline.commit_index.store(id, Ordering::Release);
    }

    #[inline(always)]
    pub fn is_running(&self) -> bool {
        self.pipeline.running.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn wait_strategy(&self) -> WaitStrategy {
        self.pipeline.wait_strategy
    }
}

/// Slice of the pipeline visible to the snapshot stage.
pub struct SnapshotContext {
    pipeline: Arc<Pipeline>,
}

impl SnapshotContext {
    #[inline(always)]
    pub fn input(&self) -> &ArrayQueue<SnapshotMessage> {
        &self.pipeline.wal_to_snapshot
    }

    #[inline(always)]
    pub fn get_processed_index(&self) -> u64 {
        self.pipeline.snapshot_index.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn set_processed_index(&self, id: u64) {
        self.pipeline.snapshot_index.store(id, Ordering::Release);
    }

    #[inline(always)]
    pub fn is_running(&self) -> bool {
        self.pipeline.running.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn wait_strategy(&self) -> WaitStrategy {
        self.pipeline.wait_strategy
    }
}

/// Slice of the pipeline visible to the seal stage.
pub struct SealContext {
    pipeline: Arc<Pipeline>,
}

impl SealContext {
    #[inline(always)]
    pub fn get_processed_index(&self) -> u32 {
        self.pipeline.seal_index.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn set_processed_index(&self, id: u32) {
        self.pipeline.seal_index.store(id, Ordering::Release);
    }

    #[inline(always)]
    pub fn is_running(&self) -> bool {
        self.pipeline.running.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn wait_strategy(&self) -> WaitStrategy {
        self.pipeline.wait_strategy
    }
}
