use crate::entities::{WalEntry, WalInput};
use crate::entries::{wal_segment_header_entry, wal_segment_sealed_entry};
use crate::pipeline::WalContext;
use crate::snapshot::SnapshotMessage;
use crate::storage::{Segment, Storage, Syncer};
use crate::wait_strategy::WaitStrategy;
use Ordering::Release;
use arc_swap::ArcSwap;
use std::collections::VecDeque;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{JoinHandle, yield_now};

pub struct Wal {
    storage: Arc<Storage>,
}

impl Wal {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    pub fn start(&self, ctx: WalContext) -> std::io::Result<[JoinHandle<()>; 2]> {
        let last_written_tx_id = Arc::new(AtomicU64::new(0));
        let active_segment_sync: Arc<ArcSwap<Option<Syncer>>> = Arc::new(ArcSwap::new(None.into()));

        let mut wal_runner = WalRunner::new(
            self.storage.clone(),
            last_written_tx_id.clone(),
            active_segment_sync.clone(),
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
    pending_records: u8,
    last_written_tx_id: Arc<AtomicU64>,
    active_segment_sync: Arc<ArcSwap<Option<Syncer>>>,
    retry_count: u64,
    active_segment: Segment,
    buffer: VecDeque<WalEntry>,
    last_received_tx_id: u64,
    segment_start_tx_id: u64,
    wait_strategy: WaitStrategy,
}

impl WalRunner {
    pub fn new(
        storage: Arc<Storage>,
        last_written_tx_id: Arc<AtomicU64>,
        active_segment_sync: Arc<ArcSwap<Option<Syncer>>>,
        ctx: &WalContext,
    ) -> Self {
        let mut active_segment = storage.active_segment().unwrap();
        let syncer = active_segment.syncer().expect("Failed to get syncer");
        active_segment_sync.store(Arc::new(Some(syncer)));
        let buffer = VecDeque::with_capacity(ctx.input_capacity() * 16);
        let wait_strategy = ctx.wait_strategy();

        if active_segment.current_wal_offset() == 0 {
            active_segment.write_entries(&[WalEntry::SegmentHeader(wal_segment_header_entry(
                active_segment.id(),
            ))])
        }

        Self {
            storage,
            pending_records: 0,
            last_written_tx_id,
            active_segment_sync,
            retry_count: 0,
            active_segment,
            buffer,
            last_received_tx_id: 0,
            segment_start_tx_id: 0,
            wait_strategy,
        }
    }

    pub fn run(&mut self, ctx: WalContext) {
        while ctx.is_running() {
            // Each slot is a WalInput::{Single,Multi}; Multi carries a follower batch.
            let inbound = ctx.input();
            let available = inbound
                .len()
                .min(self.buffer.capacity() - self.buffer.len());

            let mut entries_ingested: u32 = 0;
            let mut k = available;
            while k > 0 || self.pending_records > 0 {
                if let Some(input) = inbound.pop() {
                    match input {
                        WalInput::Single(entry) => {
                            self.ingest_entry(entry);
                            entries_ingested += 1;
                        }
                        WalInput::Multi(entries) => {
                            for entry in entries {
                                self.ingest_entry(entry);
                                entries_ingested += 1;
                            }
                        }
                    }
                } else {
                    spin_loop();
                }
                k = k.saturating_sub(1);
            }

            // if new entries are available, write them to the segment.
            if entries_ingested > 0 {
                self.active_segment.write_pending_entries();
                self.last_written_tx_id
                    .store(self.last_received_tx_id, Release);
            }

            // if there are no movements, skip the rest of the loop.
            if entries_ingested == 0 && self.buffer.is_empty() {
                self.retry_count += 1;
                self.wait_strategy.retry(self.retry_count);
                continue;
            }
            self.retry_count = 0;

            // Rotate the segment when the transaction count threshold is exceeded.
            let tx_per_seg = self.storage.config().transaction_count_per_segment;
            if self.last_received_tx_id > 0
                && self.segment_start_tx_id > 0
                && tx_per_seg > 0
                && self.last_received_tx_id - self.segment_start_tx_id >= tx_per_seg
            {
                self.rotate(&ctx);
            }

            // Push committed entries to the outbound queue.
            while let Some(entry) = self.buffer.pop_front() {
                let tx_id = entry.tx_id();
                if tx_id > ctx.commit_index() {
                    // return back
                    self.buffer.push_front(entry);
                    break;
                }
                if !self.push_outbound(&ctx, entry) {
                    break;
                }
            }
        }
    }

    /// Append one entry to the active segment buffer and update tx_id bookkeeping.
    fn ingest_entry(&mut self, entry: WalEntry) {
        self.active_segment.append_pending_entry(&entry);
        self.buffer.push_back(entry);
        if self.move_pending_entry(&entry) {
            self.last_received_tx_id = entry.tx_id();
            if self.segment_start_tx_id == 0 {
                self.segment_start_tx_id = entry.tx_id();
            }
        }
    }

    fn rotate(&mut self, ctx: &WalContext) {
        let last_written_tx_id = self.last_written_tx_id.load(Acquire);

        self.active_segment
            .write_entries(&[WalEntry::SegmentSealed(wal_segment_sealed_entry(
                self.active_segment.id(),
                last_written_tx_id,
                self.active_segment.record_count(),
            ))]);

        self.commit_sync(ctx);

        self.active_segment
            .close()
            .expect("Failed to close active segment");

        self.storage.next_segment();
        self.active_segment = self.storage.active_segment().unwrap();
        self.active_segment
            .write_entries(&[WalEntry::SegmentHeader(wal_segment_header_entry(
                self.active_segment.id(),
            ))]);
        let syncer = self.active_segment.syncer().expect("Failed to get syncer");
        self.active_segment_sync.store(Arc::new(Some(syncer)));
        self.segment_start_tx_id = 0; // will be set on next tx arrival
    }

    // Update `pending_records` based on the given entry. 0 means the entry is
    // complete.
    fn move_pending_entry(&mut self, entry: &WalEntry) -> bool {
        match entry {
            WalEntry::Metadata(m) => {
                self.pending_records = m.entry_count.saturating_add(m.link_count);
            }
            WalEntry::Entry(_) | WalEntry::Link(_) => {
                self.pending_records = self.pending_records.saturating_sub(1);
            }
            WalEntry::SegmentHeader(_) | WalEntry::SegmentSealed(_) => {}
            // FunctionRegistered is a standalone (non-transactional) WAL
            // record. It carries no tx_id and does not participate in the
            // entry-count book-keeping.
            WalEntry::FunctionRegistered(_) => {}
        }
        self.pending_records == 0
    }

    /// Push `entry` to the outbound queue, yielding until space is available.
    /// Returns `false` if a shutdown was requested while waiting.
    fn push_outbound(&self, ctx: &WalContext, entry: WalEntry) -> bool {
        let outbound = ctx.output();
        let mut msg = SnapshotMessage::Entry(entry);
        loop {
            match outbound.push(msg) {
                Ok(_) => return true,
                Err(returned) => {
                    msg = returned;
                    if !ctx.is_running() {
                        return false;
                    }
                    yield_now();
                }
            }
        }
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
