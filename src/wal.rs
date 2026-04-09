use crate::entities::WalEntry;
use crate::entries::{wal_segment_header_entry, wal_segment_sealed_entry};
use crate::pipeline::WalContext;
use crate::snapshot::SnapshotMessage;
use crate::storage::{Segment, Storage};
use std::sync::Arc;
use std::thread::JoinHandle;

pub struct Wal {
    storage: Arc<Storage>,
}

impl Wal {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    pub fn start(&self, ctx: WalContext) -> std::io::Result<JoinHandle<()>> {
        let mut runner = WalRunner {
            storage: self.storage.clone(),
            buffer: Vec::with_capacity(ctx.input_capacity()),
            committed_len: 0,
            pending_records: 0,
            pending_tx_id: 0,
            last_committed_tx_id: 0,
        };
        std::thread::Builder::new()
            .name("wal".to_string())
            .spawn(move || {
                runner.run(ctx);
            })
    }
}

pub struct WalRunner {
    storage: Arc<Storage>,
    buffer: Vec<WalEntry>,
    /// Number of entries at the front of `buffer` that form one or more complete
    /// transactions.  Everything in `buffer[..committed_len]` is safe to write/forward.
    committed_len: usize,
    // Transaction state (across fill_buffer calls)
    /// Total remaining records (entries + links) for the current transaction.
    pending_records: u8,
    pending_tx_id: u64,
    // Segmentation state
    last_committed_tx_id: u64,
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
    fn fill_buffer(&mut self, ctx: &WalContext) -> bool {
        let inbound = ctx.input();
        let available = inbound
            .len()
            .min(self.buffer.capacity() - self.buffer.len());
        for _ in 0..available {
            if let Some(entry) = inbound.pop() {
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
    fn push_outbound(&mut self, ctx: &WalContext, entry: WalEntry) -> bool {
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

        while ctx.is_running() {
            // Fill the buffer; skip this iteration if no complete transaction is ready yet.
            if !self.fill_buffer(&ctx) {
                ctx.wait_strategy().wait_strategy(retry_count);
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

            self.commit(&mut active_segment);

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
                if !self.push_outbound(&ctx, entry) {
                    break;
                }
            }
            ctx.set_processed_index(committed_tx_id);
        }
    }

    pub fn commit(&mut self, active_segment: &mut Segment) {
        let mut syncer = active_segment.syncer().expect("Failed to get syncer");
        syncer.sync().expect("Failed to sync segment");
    }
}
