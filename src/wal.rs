use crate::entities::WalEntry;
use crate::entries::{wal_segment_header_entry, wal_segment_sealed_entry};
use crate::pipeline::WalContext;
use crate::snapshot::SnapshotMessage;
use crate::storage::{Segment, Storage};
use std::collections::VecDeque;
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
            pending_records: 0,
            last_written_tx_id: 0,
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
    pending_records: u8,
    last_written_tx_id: u64,
    last_committed_tx_id: u64,
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

            retry_count = 0;

            for _ in 0..available {
                if let Some(entry) = inbound.pop() {
                    active_segment.append_pending_entry(&entry);
                    buffer.push_back(entry);
                    if self.move_pending_entry(&entry) {
                        last_received_tx_id = entry.tx_id();
                    }
                }
            }

            if available > 0 {
                // Write pending entries to the active segment disk.
                // It is okay to write stale entries to disk because atomicity is guaranteed during commit time, not during write time.
                // And the recovery function will handle partial fails
                active_segment.write_pending_entries();
                self.last_written_tx_id = last_received_tx_id;
            }

            if available == 0 && self.last_written_tx_id == self.last_committed_tx_id {
                retry_count += 1;
                wait_strategy.retry(retry_count);
                continue;
            }
            retry_count = 0;

            // Commit
            self.commit(&mut active_segment);

            // Must Wait until commit is done
            // Rotate the segment when the size threshold is exceeded.
            // No pending-entries guard needed here: the buffer contract already
            // guarantees we only ever write complete transactions.
            if active_segment.current_wal_offset()
                >= (self.storage.config().wal_segment_size_mb * 1024 * 1024) as usize
            {
                active_segment.write_entries(&[WalEntry::SegmentSealed(wal_segment_sealed_entry(
                    active_segment.id(),
                    self.last_written_tx_id,
                    active_segment.record_count(),
                ))]);
                active_segment
                    .close()
                    .expect("Failed to close active segment"); // fixme implement retry logic
                self.storage.next_segment();
                drop(active_segment);
                active_segment = self.storage.active_segment().unwrap();
            }

            let mut last_sent_tx_id = 0;
            loop {
                if let Some(entry) = buffer.pop_front() {
                    if !self.push_outbound(&ctx, entry) {
                        break;
                    }
                    last_sent_tx_id = entry.tx_id();
                } else {
                    break;
                }
            }
            if last_sent_tx_id > 0 {
                ctx.set_processed_index(last_sent_tx_id);
            }
        }
    }

    pub fn commit(&mut self, active_segment: &mut Segment) {
        let mut syncer = active_segment.syncer().expect("Failed to get syncer");
        syncer.sync().expect("Failed to sync segment");
        self.last_committed_tx_id = self.last_written_tx_id;
    }
}
