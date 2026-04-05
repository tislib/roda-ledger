use crate::entities::WalEntry;
use crate::entries::{wal_segment_header_entry, wal_segment_sealed_entry};
use crate::pipeline_mode::PipelineMode;
use crate::snapshot::SnapshotMessage;
use crate::storage::Storage;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;

pub struct Wal {
    inbound: Arc<ArrayQueue<WalEntry>>,
    outbound: Arc<ArrayQueue<SnapshotMessage>>,
    storage: Arc<Storage>,
    last_processed_transaction_id: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    pipeline_mode: PipelineMode,
}

pub struct WalRunner {
    inbound: Arc<ArrayQueue<WalEntry>>,
    outbound: Arc<ArrayQueue<SnapshotMessage>>,
    storage: Arc<Storage>,
    buffer: Vec<WalEntry>,
    last_processed_transaction_id: Arc<AtomicU64>,
    /// Number of entries at the front of `buffer` that form one or more complete
    /// transactions.  Everything in `buffer[..committed_len]` is safe to write/forward.
    committed_len: usize,
    running: Arc<AtomicBool>,
    pipeline_mode: PipelineMode,
    // Transaction state (across fill_buffer calls)
    /// Total remaining records (entries + links) for the current transaction.
    pending_records: u8,
    pending_tx_id: u64,
    // Segmentation state
    last_committed_tx_id: u64,
}

impl Wal {
    pub fn new(
        inbound: Arc<ArrayQueue<WalEntry>>,
        outbound: Arc<ArrayQueue<SnapshotMessage>>,
        storage: Arc<Storage>,
        running: Arc<AtomicBool>,
        pipeline_mode: PipelineMode,
    ) -> Self {
        Self {
            inbound,
            outbound,
            storage,
            last_processed_transaction_id: Arc::new(Default::default()),
            running,
            pipeline_mode,
        }
    }

    pub fn last_processed_transaction_id(&self) -> u64 {
        self.last_processed_transaction_id.load(Ordering::Acquire)
    }

    pub fn move_forward(&self, last_id: u64) {
        self.last_processed_transaction_id
            .store(last_id, Ordering::Release);
    }

    pub fn start(&self) -> std::io::Result<JoinHandle<()>> {
        let mut runner = self.create_runner();
        std::thread::Builder::new()
            .name("wal".to_string())
            .spawn(move || {
                runner.run();
            })
    }

    fn create_runner(&self) -> WalRunner {
        WalRunner {
            inbound: self.inbound.clone(),
            outbound: self.outbound.clone(),
            storage: self.storage.clone(),
            buffer: Vec::with_capacity(self.inbound.capacity()),
            last_processed_transaction_id: self.last_processed_transaction_id.clone(),
            committed_len: 0,
            running: self.running.clone(),
            pipeline_mode: self.pipeline_mode,
            pending_records: 0,
            pending_tx_id: 0,
            last_committed_tx_id: 0,
        }
    }
}

impl WalRunner {
    /// Drain up to `capacity - buffer.len()` items from inbound into `buffer`,
    /// tracking transaction boundaries.  This is the **only** place
    /// `pending_entries` / `pending_tx_id` / `committed_len` are updated.
    ///
    /// After the call, `buffer[..committed_len]` contains one or more complete
    /// transactions ready to be written/forwarded.  Any remaining entries in
    /// `buffer[committed_len..]` are a partial transaction that will be completed
    /// in a future call.
    ///
    /// Returns `true` when there is at least one complete transaction to process.
    fn fill_buffer(&mut self) -> bool {
        let available = self
            .inbound
            .len()
            .min(self.buffer.capacity() - self.buffer.len());
        for _ in 0..available {
            if let Some(entry) = self.inbound.pop() {
                match &entry {
                    WalEntry::Metadata(m) => {
                        self.pending_records = m.entry_count.saturating_add(m.link_count);
                        self.pending_tx_id = m.tx_id;
                    }
                    WalEntry::Entry(_) | WalEntry::Link(_) => {
                        self.pending_records = self.pending_records.saturating_sub(1);
                    }
                    WalEntry::SegmentHeader(_) | WalEntry::SegmentSealed(_) => {}
                }
                self.buffer.push(entry);

                if self.pending_records == 0 && self.pending_tx_id != 0 {
                    // A full transaction just landed in the buffer.
                    self.committed_len = self.buffer.len();
                    self.last_committed_tx_id = self.pending_tx_id;
                    self.pending_tx_id = 0; // reset for next tx
                }
            }
        }
        self.committed_len > 0
    }

    /// Push `entry` to the outbound queue, yielding until space is available.
    /// Returns `false` if a shutdown was requested while waiting.
    fn push_outbound(&mut self, entry: WalEntry) -> bool {
        let mut msg = SnapshotMessage::Entry(entry);
        loop {
            match self.outbound.push(msg) {
                Ok(_) => return true,
                Err(returned) => {
                    msg = returned;
                    if !self.running.load(Ordering::Relaxed) {
                        return false;
                    }
                    std::thread::yield_now();
                }
            }
        }
    }

    pub fn run(&mut self) {
        let mut retry_count = 0;
        let mut active_segment = self.storage.active_segment().unwrap();

        while self.running.load(Ordering::Relaxed) {
            // Fill the buffer; skip this iteration if no complete transaction is ready yet.
            if !self.fill_buffer() {
                self.pipeline_mode.wait_strategy(retry_count);
                retry_count += 1;
                continue;
            }
            retry_count = 0;

            if active_segment.current_wal_offset() == 0 {
                let first_tx_id = self.buffer[0].tx_id();
                active_segment.write_entries(&[WalEntry::SegmentHeader(wal_segment_header_entry(
                    active_segment.id(),
                    first_tx_id,
                ))])
            }

            active_segment.write_entries(&self.buffer[..self.committed_len]); //fixme move retry logic to here

            // Rotate the segment when the size threshold is exceeded.
            // No pending-entries guard needed here: the buffer contract already
            // guarantees we only ever write complete transactions.
            if active_segment.current_wal_offset()
                >= (self.storage.config().wal_segment_size_mb * 1024 * 1024) as usize
            {
                active_segment.write_entries(&[WalEntry::SegmentSealed(wal_segment_sealed_entry(
                    active_segment.id(),
                    self.last_committed_tx_id,
                    active_segment.record_count(),
                ))]);
                active_segment
                    .close()
                    .expect("Failed to close active segment"); // fixme implement retry logic
                self.storage.next_segment();
                drop(active_segment);
                active_segment = self.storage.active_segment().unwrap();
            }

            // Forward committed entries downstream and publish the committed tx id.
            let committed_tx_id = self.last_committed_tx_id;
            let committed: Vec<WalEntry> = self.buffer.drain(..self.committed_len).collect();
            self.committed_len = 0;
            for entry in committed {
                if !self.push_outbound(entry) {
                    break;
                }
            }
            self.last_processed_transaction_id
                .store(committed_tx_id, Ordering::Release);
        }
    }
}
