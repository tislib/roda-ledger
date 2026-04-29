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

use crate::config::LedgerConfig;
use storage::entities::{WalEntry, WalInput};
use crate::snapshot::SnapshotMessage;
use crate::transaction::TransactionInput;
use crate::wait_strategy::WaitStrategy;
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// Callback fired by the WAL stage whenever `commit_index` advances.
pub type CommitHandler = Arc<dyn Fn(u64) + Send + Sync + 'static>;

/// Owns every inter-stage queue and every global progress index in the ledger.
///
/// Stages never own queues or atomics directly: they receive a typed
/// `*Context` (a thin Arc-wrapper) from `Pipeline::*_context()` and read /
/// publish through it.
pub struct Pipeline {
    // ---- inter-stage queues ----
    sequencer_to_transactor: ArrayQueue<TransactionInput>,
    wal_input: ArrayQueue<WalInput>,
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

    /// Cluster-commit gate consulted by the seal stage (ADR-0016 §10).
    /// A segment may only be sealed once **every** transaction it
    /// contains has `tx_id <= seal_watermark`. The cluster supervisor
    /// drives this atomic from `Quorum::get()` advances on the leader
    /// and from `leader_commit_tx_id` arriving in `AppendEntries` on
    /// followers. Default `u64::MAX` means "no cluster gate" — the
    /// behaviour every standalone (non-cluster) `Ledger` user gets.
    ///
    /// The invariant this enforces: sealed segments contain only
    /// cluster-committed transactions, so `start_with_recovery_until`
    /// never has to unseal anything during a recovery-mode reseed
    /// (sealed segments are by construction always at or below the
    /// reseed watermark, since the cluster watermark is monotonic).
    seal_watermark: CachePadded<AtomicU64>,

    /// Global shutdown flag. Cleared by `shutdown()` to stop every stage.
    running: CachePadded<AtomicBool>,

    /// Shared wait strategy used by every stage's idle/backpressure loops.
    wait_strategy: WaitStrategy,

    /// Optional hook fired by the WAL stage every time `commit_index`
    commit_handler: OnceLock<CommitHandler>,
}

impl Pipeline {
    /// Construct a pipeline with empty queues sized from the ledger config.
    pub fn new(config: &LedgerConfig) -> Arc<Self> {
        Self::with_sizes(config.queue_size, config.queue_size, config.wait_strategy)
    }

    /// Low-level constructor exposed for benches/tests that need to control
    /// the individual queue sizes without building a full `LedgerConfig`.
    pub fn with_sizes(
        small_queue_size: usize,
        wal_queue_size: usize,
        wait_strategy: WaitStrategy,
    ) -> Arc<Self> {
        Arc::new(Self {
            sequencer_to_transactor: ArrayQueue::new(small_queue_size),
            wal_input: ArrayQueue::new(wal_queue_size),
            wal_to_snapshot: ArrayQueue::new(small_queue_size),

            sequencer_index: CachePadded::new(AtomicU64::new(1)),
            compute_index: CachePadded::new(AtomicU64::new(0)),
            commit_index: CachePadded::new(AtomicU64::new(0)),
            snapshot_index: CachePadded::new(AtomicU64::new(0)),
            seal_index: CachePadded::new(AtomicU32::new(0)),
            seal_watermark: CachePadded::new(AtomicU64::new(u64::MAX)),

            running: CachePadded::new(AtomicBool::new(true)),
            wait_strategy,
            commit_handler: OnceLock::new(),
        })
    }

    /// Install the commit-index callback. Returns `Err` if already set —
    /// the hook is intentionally one-shot so the WAL stage can rely on
    /// it being stable for the lifetime of the pipeline.
    pub fn set_commit_handler(&self, handler: CommitHandler) -> Result<(), CommitHandler> {
        self.commit_handler.set(handler)
    }

    #[inline]
    fn commit_handler(&self) -> Option<&CommitHandler> {
        self.commit_handler.get()
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

    pub fn ledger_context(self: &Arc<Self>) -> LedgerContext {
        LedgerContext {
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

    pub fn last_compute_id(&self) -> u64 {
        self.compute_index.load(Ordering::Acquire)
    }

    pub fn last_commit_id(&self) -> u64 {
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

    pub(crate) fn set_commit_index(&self, next_id: u64) {
        self.commit_index.store(next_id, Ordering::Release);
    }

    pub(crate) fn set_seal_index(&self, id: u32) {
        self.seal_index.store(id, Ordering::Release);
    }

    // ─── cluster-driven seal gate (ADR-0016 §10) ────────────────────────────

    /// Public so the cluster supervisor can drive the seal watermark
    /// from `Quorum::get()` advances (leader) or `leader_commit_tx_id`
    /// from incoming `AppendEntries` (follower). Standalone Ledger
    /// users never call this — the default `u64::MAX` keeps the
    /// pre-cluster sealing cadence intact.
    ///
    /// Plain store, not `fetch_max`: the default value is `u64::MAX`
    /// (the "no gate" sentinel), so a `fetch_max(N)` against it would
    /// never lower the gate. The cluster supervisor is responsible for
    /// driving this value monotonically within its own lifecycle —
    /// `Quorum::get()` and `leader_commit_tx_id` are both
    /// monotonically non-decreasing per Raft, so the supervisor can
    /// just thread their values straight through.
    pub fn set_seal_watermark(&self, tx_id: u64) {
        self.seal_watermark.store(tx_id, Ordering::Release);
    }

    #[inline(always)]
    pub fn get_seal_watermark(&self) -> u64 {
        self.seal_watermark.load(Ordering::Acquire)
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
    pub fn output(&self) -> &ArrayQueue<TransactionInput> {
        &self.pipeline.sequencer_to_transactor
    }

    /// Atomically fetch and increment the sequencer index. Returns the id
    /// the caller should stamp on its transaction.
    #[inline(always)]
    pub fn fetch_next_id(&self, count: u64) -> u64 {
        self.pipeline
            .sequencer_index
            .fetch_add(count, Ordering::Acquire)
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
    pub fn input(&self) -> &ArrayQueue<TransactionInput> {
        &self.pipeline.sequencer_to_transactor
    }

    #[inline(always)]
    pub fn output(&self) -> &ArrayQueue<WalInput> {
        &self.pipeline.wal_input
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

    /// Follower path: when a replicated batch with `max_tx_id_seen` is
    /// applied, raise the sequencer's next-id high-water mark to at
    /// least `max_tx_id_seen + 1`. On a future role transition to
    /// Leader, the Sequencer's next `fetch_add(1)` returns an id that
    /// does NOT collide with replicated tx_ids already in the WAL.
    /// `fetch_max` semantics are safe even if a leader-side sequencer
    /// has independently advanced the index past this point.
    #[inline]
    pub fn bump_sequencer_to_at_least(&self, max_tx_id_seen: u64) {
        let next = max_tx_id_seen.saturating_add(1);
        self.pipeline
            .sequencer_index
            .fetch_max(next, Ordering::AcqRel);
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
#[derive(Clone)]
pub struct WalContext {
    pipeline: Arc<Pipeline>,
}

impl WalContext {
    #[inline(always)]
    pub fn input(&self) -> &ArrayQueue<WalInput> {
        &self.pipeline.wal_input
    }

    #[inline(always)]
    pub fn output(&self) -> &ArrayQueue<SnapshotMessage> {
        &self.pipeline.wal_to_snapshot
    }

    #[inline(always)]
    pub fn input_capacity(&self) -> usize {
        self.pipeline.wal_input.capacity()
    }

    #[inline(always)]
    pub fn get_processed_index(&self) -> u64 {
        self.pipeline.commit_index.load(Ordering::Acquire)
    }

    /// Publish a new commit-index and fire the registered `on_commit`
    /// handler (if any). The handler runs synchronously on the WAL
    /// commit thread — keep it fast and non-blocking.
    #[inline]
    pub fn set_commit_index(&self, id: u64) {
        self.pipeline.commit_index.store(id, Ordering::Release);
        if let Some(handler) = self.pipeline.commit_handler() {
            handler(id);
        }
    }

    #[inline]
    pub fn commit_index(&self) -> u64 {
        self.pipeline.commit_index.load(Ordering::Acquire)
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

    /// Cluster-commit gate (ADR-0016 §10). The seal stage refuses to
    /// seal a segment whose last `tx_id` is above this value, so
    /// sealed segments only ever contain cluster-committed
    /// transactions.
    #[inline(always)]
    pub fn seal_watermark(&self) -> u64 {
        self.pipeline.get_seal_watermark()
    }
}

#[derive(Clone)]
pub struct LedgerContext {
    pipeline: Arc<Pipeline>,
}

impl LedgerContext {
    /// Non-blocking single-entry push; returns the entry back on a full queue.
    #[inline]
    pub fn push_wal_entry(&self, entry: WalEntry) -> Result<(), WalEntry> {
        self.pipeline
            .wal_input
            .push(WalInput::Single(entry))
            .map_err(|wi| wi.single())
    }

    /// Non-blocking push of follower-replicated WAL entries through the
    /// Transactor (NOT directly to the WAL stage). The Transactor mirrors
    /// the entries' effects onto its `balances` and `dedup` state, then
    /// forwards them to the WAL stage as `WalInput::Multi`. Returns the
    /// entries back on a full queue so the caller can retry.
    ///
    /// Routing through the Transactor is what keeps a follower's
    /// in-memory state in sync with the WAL — without it, a promotion
    /// to leader would start from stale state and silently double-apply
    /// user retries (`dedup` gap) or mis-validate ops (`balances` gap).
    #[inline]
    pub fn push_replicated_entries(&self, entries: Vec<WalEntry>) -> Result<(), Vec<WalEntry>> {
        match self
            .pipeline
            .sequencer_to_transactor
            .push(crate::transaction::TransactionInput::Replicated(entries))
        {
            Ok(()) => Ok(()),
            Err(crate::transaction::TransactionInput::Replicated(returned)) => Err(returned),
            Err(_) => unreachable!(
                "push_replicated_entries pushed Replicated; only Replicated can come back"
            ),
        }
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
