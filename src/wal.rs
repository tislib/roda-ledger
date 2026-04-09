use crate::entities::WalEntry;
use crate::entries::{wal_segment_header_entry, wal_segment_sealed_entry};
use crate::pipeline::WalContext;
use crate::snapshot::SnapshotMessage;
use crate::storage::{Segment, Storage, Syncer};
use Ordering::Release;
use arc_swap::ArcSwap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;

pub struct Wal {
    storage: Arc<Storage>,
}

impl Wal {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    pub fn start(&self, ctx: WalContext) -> std::io::Result<[JoinHandle<()>; 2]> {
        let last_written_tx_id = Arc::new(AtomicU64::new(0));
        let last_committed_tx_id = Arc::new(AtomicU64::new(0));
        let active_segment: Arc<ArcSwap<Option<Syncer>>> = Arc::new(ArcSwap::new(None.into()));

        let mut wal_runner = WalRunner {
            storage: self.storage.clone(),
            pending_records: 0,
            active_segment_sync: active_segment.clone(),
            last_written_tx_id: last_written_tx_id.clone(),
            last_committed_tx_id: last_committed_tx_id.clone(),
        };
        let wal_ctx_clone = ctx.clone();
        let wal_th = std::thread::Builder::new()
            .name("wal_write".to_string())
            .spawn(move || {
                wal_runner.run(wal_ctx_clone);
            })?;

        let mut wal_committer = WalCommitter {
            active_segment,
            last_written_tx_id,
            last_committed_tx_id,
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
    last_committed_tx_id: Arc<AtomicU64>,
    active_segment_sync: Arc<ArcSwap<Option<Syncer>>>,
}

impl WalRunner {
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
                    std::thread::yield_now();
                }
            }
        }
    }

    pub fn run(&mut self, ctx: WalContext) {
        let mut retry_count = 0;
        let mut active_segment = self.storage.active_segment().unwrap();
        let syncer = active_segment.syncer().expect("Failed to get syncer");
        self.active_segment_sync.store(Arc::new(Some(syncer)));
        let mut buffer = VecDeque::with_capacity(ctx.input_capacity());
        let mut last_received_tx_id = 0;
        let wait_strategy = ctx.wait_strategy();

        while ctx.is_running() {
            // init
            if active_segment.current_wal_offset() == 0 {
                let first_tx_id = 0; // fixme remove it from segment header
                active_segment.write_entries(&[WalEntry::SegmentHeader(wal_segment_header_entry(
                    active_segment.id(),
                    first_tx_id,
                ))])
            }

            // Receive entries from the inbound queue.
            let inbound = ctx.input();
            let available = inbound.len().min(buffer.capacity() - buffer.len());

            let mut k = available;
            while k > 0 || self.pending_records > 0 {
                if let Some(entry) = inbound.pop() {
                    active_segment.append_pending_entry(&entry);
                    buffer.push_back(entry);
                    if self.move_pending_entry(&entry) {
                        last_received_tx_id = entry.tx_id();
                    }
                }
                k = k.saturating_sub(1);
            }

            if available > 0 {
                active_segment.write_pending_entries();
                self.last_written_tx_id.store(last_received_tx_id, Release);
            }
            let last_written_tx_id = self.last_written_tx_id.load(Acquire);

            if available == 0 && buffer.is_empty() {
                retry_count += 1;
                wait_strategy.retry(retry_count);
                continue;
            }
            retry_count = 0;

            // Must Wait until commit is done
            // Rotate the segment when the size threshold is exceeded.
            // No pending-entries guard needed here: the buffer contract already
            // guarantees we only ever write complete transactions.
            if active_segment.current_wal_offset() >= (self.storage.wal_size()) {
                active_segment.write_entries(&[WalEntry::SegmentSealed(wal_segment_sealed_entry(
                    active_segment.id(),
                    last_written_tx_id,
                    active_segment.record_count(),
                ))]);
                self.commit_sync(&mut active_segment);
                active_segment
                    .close()
                    .expect("Failed to close active segment"); // fixme implement retry logic
                self.storage.next_segment();
                active_segment = self.storage.active_segment().unwrap();
                let syncer = active_segment.syncer().expect("Failed to get syncer");
                self.active_segment_sync.store(Arc::new(Some(syncer)));
            }

            while let Some(entry) = buffer.pop_front() {
                let tx_id = entry.tx_id();
                if tx_id > self.last_committed_tx_id.load(Acquire) {
                    // return back
                    buffer.push_front(entry);
                    break;
                }
                if !self.push_outbound(&ctx, entry) {
                    break;
                }
                ctx.set_processed_index(tx_id);
            }
        }
    }

    pub fn commit_sync(&mut self, active_segment: &mut Segment) {
        let mut syncer = active_segment.syncer().expect("Failed to get syncer");
        syncer.sync().expect("Failed to sync segment");
        self.last_committed_tx_id
            .store(self.last_written_tx_id.load(Acquire), Release);
    }
}

struct WalCommitter {
    last_written_tx_id: Arc<AtomicU64>,
    last_committed_tx_id: Arc<AtomicU64>,
    active_segment: Arc<ArcSwap<Option<Syncer>>>,
}

impl WalCommitter {
    fn run(&mut self, ctx: WalContext) {
        let mut retry_count = 0;
        let wait_strategy = ctx.wait_strategy();
        while ctx.is_running() {
            if self.last_written_tx_id.load(Acquire) <= self.last_committed_tx_id.load(Acquire) {
                retry_count += 1;
                wait_strategy.retry(retry_count);
                continue;
            }
            retry_count = 0;
            self.commit_sync();
        }
    }

    pub fn commit_sync(&mut self) {
        let active_segment = self.active_segment.load();
        if let Some(syncer) = active_segment.as_ref() {
            let mut syncer = syncer.clone();
            let commit_tx_id = self.last_written_tx_id.load(Acquire);

            syncer.sync().expect("Failed to sync segment");

            self.last_committed_tx_id.store(commit_tx_id, Release);
        }
    }
}
