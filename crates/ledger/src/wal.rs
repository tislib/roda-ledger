use crate::pipeline::WalContext;
use crate::recover::ActiveSnapshot;
use crate::tx_ring::reader::TxRingReader;
use crate::wait_strategy::WaitStrategy;
use arc_swap::ArcSwap;
use std::sync::Arc;
use std::thread::JoinHandle;
use storage::entities::WalEntry;
use storage::{Segment, Storage, Syncer};

/// One-shot launcher for the WAL threads. Holds exactly what `start` needs;
/// `start` consumes `self`, restores resume state from `active_snapshot`, spawns
/// the writer + committer, and drops `self` while they keep running.
pub struct Wal<'a> {
    pub ctx: WalContext,
    pub active_snapshot: &'a ActiveSnapshot,
    pub storage: Arc<Storage>,
    pub ring_reader: TxRingReader,
}

impl Wal<'_> {
    pub fn start(self) -> std::io::Result<Vec<JoinHandle<()>>> {
        let Wal {
            ctx,
            active_snapshot,
            storage,
            ring_reader,
        } = self;

        // The WAL owns these indices: everything ≤ last_tx_id is already durable.
        ctx.set_write_index(active_snapshot.last_tx_id);
        ctx.set_commit_index(active_snapshot.last_tx_id);

        let active_segment_sync: Arc<ArcSwap<Option<Syncer>>> = Arc::new(ArcSwap::new(None.into()));

        let mut wal_runner = WalRunner::new(
            storage,
            active_segment_sync.clone(),
            ring_reader,
            &ctx,
            active_snapshot.last_tx_id,
        );
        let wal_ctx_clone = ctx.clone();
        let wal_th = std::thread::Builder::new()
            .name("wal_write".to_string())
            .spawn(move || {
                wal_runner.run(wal_ctx_clone);
            })?;

        let mut wal_committer = WalCommitter {
            active_segment: active_segment_sync,
        };
        let wal_commit_th = std::thread::Builder::new()
            .name("wal_commit".to_string())
            .spawn(move || {
                wal_committer.run(ctx);
            })?;

        Ok(vec![wal_th, wal_commit_th])
    }
}

pub struct WalRunner {
    storage: Arc<Storage>,
    active_segment_sync: Arc<ArcSwap<Option<Syncer>>>,
    retry_count: u64,
    active_segment: Segment,
    last_received_tx_id: u64,
    segment_start_tx_id: u64,
    wait_strategy: WaitStrategy,
    /// Reads the ring and frees slots (on write). Wrapped in `Option` so `run`
    /// can move it to a local — the walk closure borrows the rest of `self`.
    reader: Option<TxRingReader>,
}

impl WalRunner {
    pub fn new(
        storage: Arc<Storage>,
        active_segment_sync: Arc<ArcSwap<Option<Syncer>>>,
        reader: TxRingReader,
        ctx: &WalContext,
        last_tx_id: u64,
    ) -> Self {
        let active_segment = storage.active_segment().unwrap();
        let syncer = active_segment.syncer().expect("Failed to get syncer");
        active_segment_sync.store(Arc::new(Some(syncer)));
        let wait_strategy = ctx.wait_strategy();
        // Resume bookkeeping so rotation accounts for txs already on disk.
        let segment_start_tx_id = active_segment.first_tx_id_in_wal_data().unwrap_or(0);

        Self {
            storage,
            active_segment_sync,
            retry_count: 0,
            active_segment,
            last_received_tx_id: last_tx_id,
            segment_start_tx_id,
            wait_strategy,
            reader: Some(reader),
        }
    }

    pub fn run(&mut self, ctx: WalContext) {
        // Move the reader to a local so the walk closure can borrow the rest of self.
        let mut reader = self.reader.take().expect("WalRunner::run called twice");
        let mut last_ring_index = 0;
        while ctx.is_running() {
            let mut entries_ingested = 0;
            reader.walk(last_ring_index, |entry| {
                self.ingest_entry(entry);
                entries_ingested += 1;
                last_ring_index += 1;

                // if the segment is full, do not push any more entries.
                if self.is_rotation_needed() {
                    return false;
                }

                true
            });

            // if new entries are available, write them to the segment.
            if entries_ingested > 0 {
                self.active_segment.write_pending_entries();
                // Publish after the page-cache write (not the in-memory copy) so a
                // stalled writer back-pressures the transactor; fdatasync/commit stays decoupled.
                ctx.set_write_index(self.last_received_tx_id);
                reader.release_to(last_ring_index);
            }

            // if there are no movements, skip the rest of the loop.
            if entries_ingested == 0 {
                self.retry_count += 1;
                self.wait_strategy.retry(self.retry_count);
                continue;
            }
            self.retry_count = 0;

            // Rotate the segment when the transaction count threshold is exceeded.
            let is_rotation_needed = self.is_rotation_needed();

            if is_rotation_needed {
                self.rotate(&ctx);
            }
        }
    }

    fn is_rotation_needed(&self) -> bool {
        let tx_per_seg = self.storage.config().transaction_count_per_segment;
        self.last_received_tx_id > 0
            && self.segment_start_tx_id > 0
            && tx_per_seg > 0
            && self.last_received_tx_id - self.segment_start_tx_id >= tx_per_seg - 1
    }

    /// Append one entry to the active segment and advance tx bookkeeping. Trailer
    /// layout: a `TxMetadata` closes its tx, so tracking advances only there.
    fn ingest_entry(&mut self, entry: WalEntry) {
        self.active_segment.append_pending_entry(&entry);
        if let WalEntry::Metadata(m) = &entry {
            self.last_received_tx_id = m.tx_id;
            if self.segment_start_tx_id == 0 {
                self.segment_start_tx_id = m.tx_id;
            }
        }
    }

    fn rotate(&mut self, ctx: &WalContext) {
        self.commit_sync(ctx);

        self.active_segment
            .close()
            .expect("Failed to close active segment");

        self.storage.next_segment();
        self.active_segment = self.storage.active_segment().unwrap();
        let syncer = self.active_segment.syncer().expect("Failed to get syncer");
        self.active_segment_sync.store(Arc::new(Some(syncer)));
        self.segment_start_tx_id = 0; // will be set on next tx arrival
    }

    pub fn commit_sync(&mut self, ctx: &WalContext) {
        let mut syncer = self.active_segment.syncer().expect("Failed to get syncer");
        syncer.sync().expect("Failed to sync segment");
        ctx.set_commit_index(ctx.write_index());
    }
}

struct WalCommitter {
    active_segment: Arc<ArcSwap<Option<Syncer>>>,
}

impl WalCommitter {
    fn run(&mut self, ctx: WalContext) {
        let mut retry_count = 0;
        let wait_strategy = ctx.wait_strategy();
        let mut sync: Option<Syncer> = None;
        let mut sync_id = 0;
        while ctx.is_running() {
            if ctx.write_index() <= ctx.commit_index() {
                retry_count += 1;
                wait_strategy.retry(retry_count);
                continue;
            }
            retry_count = 0;

            // loading syncer
            let active_segment = self.active_segment.load();
            if let Some(syncer) = active_segment.as_ref()
                && sync_id != syncer.id()
            {
                sync_id = syncer.id();
                sync = Some(syncer.clone());
            }

            if let Some(syncer) = sync.as_mut() {
                self.commit_sync(&ctx, syncer);
            }
        }
    }

    pub fn commit_sync(&mut self, ctx: &WalContext, syncer: &mut Syncer) {
        let commit_tx_id = ctx.write_index();

        syncer.sync().expect("Failed to sync segment");

        ctx.set_commit_index(commit_tx_id);
    }
}
