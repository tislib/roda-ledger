use crate::pipeline::WalContext;
use crate::tx_ring::reader::TxRingReader;
use crate::wait_strategy::WaitStrategy;
use Ordering::Release;
use arc_swap::ArcSwap;
use std::sync::Arc;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use storage::{Segment, Storage, Syncer};

pub struct Wal {
    storage: Arc<Storage>,
    /// Ring reader, moved into the WalRunner on `start`. The WAL is the sole ring
    /// consumer: it reads each entry and frees its slot (on write) via this reader.
    reader: Option<TxRingReader>,
}

impl Wal {
    pub fn new(storage: Arc<Storage>, reader: TxRingReader) -> Self {
        Self {
            storage,
            reader: Some(reader),
        }
    }

    pub fn start(&mut self, ctx: WalContext) -> std::io::Result<[JoinHandle<()>; 2]> {
        let reader = self
            .reader
            .take()
            .expect("Wal::start called twice (reader already taken)");
        let last_written_tx_id = Arc::new(AtomicU64::new(0));
        let active_segment_sync: Arc<ArcSwap<Option<Syncer>>> = Arc::new(ArcSwap::new(None.into()));

        let mut wal_runner = WalRunner::new(
            self.storage.clone(),
            last_written_tx_id.clone(),
            active_segment_sync.clone(),
            reader,
            &ctx,
        );
        let wal_ctx_clone = ctx.clone();
        let wal_th = std::thread::Builder::new()
            .name("wal_write".to_string())
            .spawn(move || {
                wal_runner.run(wal_ctx_clone);
            })?;

        let mut wal_committer = WalCommitter {
            active_segment: active_segment_sync,
            last_written_tx_id,
        };
        let wal_commit_th = std::thread::Builder::new()
            .name("wal_commit".to_string())
            .spawn(move || {
                wal_committer.run(ctx);
            })?;

        Ok([wal_th, wal_commit_th])
    }
}

pub struct WalRunner {
    storage: Arc<Storage>,
    last_written_tx_id: Arc<AtomicU64>,
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
        last_written_tx_id: Arc<AtomicU64>,
        active_segment_sync: Arc<ArcSwap<Option<Syncer>>>,
        reader: TxRingReader,
        ctx: &WalContext,
    ) -> Self {
        let active_segment = storage.active_segment().unwrap();
        let syncer = active_segment.syncer().expect("Failed to get syncer");
        active_segment_sync.store(Arc::new(Some(syncer)));
        let wait_strategy = ctx.wait_strategy();

        Self {
            storage,
            last_written_tx_id,
            active_segment_sync,
            retry_count: 0,
            active_segment,
            last_received_tx_id: 0,
            segment_start_tx_id: 0,
            wait_strategy,
            reader: Some(reader),
        }
    }

    pub fn run(&mut self, ctx: WalContext) {
        // Move the reader to a local; the write closure borrows the rest of self.
        let mut reader = self.reader.take().expect("WalRunner::run called twice");
        let tx_per_seg = self.storage.config().transaction_count_per_segment;
        while ctx.is_running() {
            // Cap the batch at the active segment's remaining tx room so a segment holds
            // exactly `tx_per_seg` txs; the ring cuts on a tx boundary (never splits one).
            let max_tx_count = if tx_per_seg == 0 {
                usize::MAX // unlimited — never rotate
            } else {
                (tx_per_seg - self.segment_tx_count()) as usize
            };

            // The ring hands us the next whole-tx byte run and which txs it covers; we
            // just write it to the segment, then release happens inside `read_next_batch`.
            let batch = reader.read_next_batch(max_tx_count, |bytes| {
                self.active_segment.write_bytes(bytes)
            });

            // No complete tx ready yet — back off and retry.
            let Some(last_tx) = batch.last_tx else {
                self.retry_count += 1;
                self.wait_strategy.retry(self.retry_count);
                continue;
            };
            self.retry_count = 0;

            if self.segment_start_tx_id == 0 {
                self.segment_start_tx_id =
                    batch.first_tx.expect("first_tx is set whenever last_tx is");
            }
            self.last_received_tx_id = last_tx;
            // Release happens on the disk write (inside read_next_batch), so a stalled
            // writer back-pressures the transactor; fdatasync/commit stays decoupled.
            self.last_written_tx_id.store(last_tx, Release);

            // The segment is full once it holds `tx_per_seg` txs.
            if tx_per_seg > 0 && self.segment_tx_count() >= tx_per_seg {
                self.rotate(&ctx);
            }
        }
    }

    // Txs already written to the active segment (0 before its first tx lands).
    fn segment_tx_count(&self) -> u64 {
        if self.segment_start_tx_id == 0 {
            0
        } else {
            self.last_received_tx_id - self.segment_start_tx_id + 1
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
        ctx.set_commit_index(self.last_written_tx_id.load(Acquire));
    }
}

struct WalCommitter {
    last_written_tx_id: Arc<AtomicU64>,
    active_segment: Arc<ArcSwap<Option<Syncer>>>,
}

impl WalCommitter {
    fn run(&mut self, ctx: WalContext) {
        let mut retry_count = 0;
        let wait_strategy = ctx.wait_strategy();
        let mut sync: Option<Syncer> = None;
        let mut sync_id = 0;
        while ctx.is_running() {
            if self.last_written_tx_id.load(Acquire) <= ctx.commit_index() {
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
        let commit_tx_id = self.last_written_tx_id.load(Acquire);

        syncer.sync().expect("Failed to sync segment");

        ctx.set_commit_index(commit_tx_id);
    }
}
