//! Raw WAL byte streaming for ADR-015 Cluster Mode leader shipping.

use crate::entities::WalEntryKind;
use crate::storage::Storage;
use std::sync::Arc;

/// Every WAL record is `#[repr(C)]` and exactly 40 bytes (entities.rs asserts).
pub const WAL_RECORD_SIZE: usize = 40;
/// `tx_id` offset shared by TxMetadata, TxEntry, TxLink.
const TX_ID_OFFSET: usize = 8;

/// Stateful WAL byte tailer. Each `tail()` call resumes from where the
/// previous call stopped when the same `from_tx_id` is passed, so the
/// caller does not rescan already-streamed records across segments.
pub struct WalTailer {
    storage: Arc<Storage>,
    cursor: Option<Cursor>,
}

#[derive(Clone, Copy, Debug)]
struct Cursor {
    /// `from_tx_id` this cursor is valid for; a different value re-seeks.
    from_tx_id: u64,
    /// Segment id the cursor points into.
    segment_id: u32,
    /// Byte offset inside that segment; next record to consider.
    offset: usize,
}

impl WalTailer {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage, cursor: None }
    }

    /// Reset the cursor; the next `tail()` re-seeks from segment 0.
    pub fn reset(&mut self) {
        self.cursor = None;
    }

    /// Stream WAL bytes into `buffer` starting at `from_tx_id`; returns bytes written.
    /// Resumes from the cached cursor when `from_tx_id` matches the previous call.
    pub fn tail(&mut self, from_tx_id: u64, buffer: &mut [u8]) -> u32 {
        let capacity = buffer.len() - (buffer.len() % WAL_RECORD_SIZE);
        if capacity == 0 {
            return 0;
        }

        // Resume only when the caller is still streaming the same range.
        let (start_seg, start_off) = match self.cursor {
            Some(c) if c.from_tx_id == from_tx_id => (c.segment_id, c.offset),
            _ => (0u32, 0usize),
        };

        let mut segments = match self.storage.list_all_segments() {
            Ok(s) => s,
            Err(_) => return 0,
        };
        if let Ok(active) = self.storage.active_segment() {
            segments.push(active);
        }
        segments.sort_by_key(|s| s.id());

        let mut written: usize = 0;
        let mut last_seg = start_seg;
        let mut last_off = start_off;

        for mut segment in segments {
            if segment.id() < start_seg {
                continue;
            }
            if segment.load().is_err() {
                continue;
            }

            let data = segment.wal_data_copy();
            if !data.len().is_multiple_of(WAL_RECORD_SIZE) {
                continue;
            }

            let mut offset = if segment.id() == start_seg { start_off } else { 0 };
            while offset + WAL_RECORD_SIZE <= data.len() {
                let record = &data[offset..offset + WAL_RECORD_SIZE];
                offset += WAL_RECORD_SIZE;

                if !record_matches(record, from_tx_id) {
                    last_seg = segment.id();
                    last_off = offset;
                    continue;
                }

                if written + WAL_RECORD_SIZE > capacity {
                    // Park cursor at this unwritten record so the next call re-emits it.
                    self.cursor = Some(Cursor {
                        from_tx_id,
                        segment_id: segment.id(),
                        offset: offset - WAL_RECORD_SIZE,
                    });
                    return written as u32;
                }
                buffer[written..written + WAL_RECORD_SIZE].copy_from_slice(record);
                written += WAL_RECORD_SIZE;
                last_seg = segment.id();
                last_off = offset;
            }
        }

        // Fully drained; parking at end-of-data lets actively-written records
        // surface on the next call without rescanning earlier segments.
        self.cursor = Some(Cursor { from_tx_id, segment_id: last_seg, offset: last_off });
        written as u32
    }
}

#[inline]
fn record_matches(record: &[u8], from_tx_id: u64) -> bool {
    debug_assert_eq!(record.len(), WAL_RECORD_SIZE);
    let kind = record[0];
    let has_tx_id = kind == WalEntryKind::TxMetadata as u8
        || kind == WalEntryKind::TxEntry as u8
        || kind == WalEntryKind::Link as u8;

    if !has_tx_id {
        return from_tx_id == 0;
    }

    let tx_id = u64::from_le_bytes(
        record[TX_ID_OFFSET..TX_ID_OFFSET + 8]
            .try_into()
            .expect("WAL record is exactly 40 bytes"),
    );
    tx_id >= from_tx_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::{
        EntryKind, FailReason, TxEntry, TxLink, TxLinkKind, TxMetadata, WalEntryKind,
    };
    use bytemuck::bytes_of;

    fn meta_bytes(tx_id: u64) -> [u8; 40] {
        let m = TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            entry_count: 1,
            link_count: 0,
            fail_reason: FailReason::NONE,
            crc32c: 0,
            tx_id,
            timestamp: 0,
            user_ref: 0,
            tag: [0; 8],
        };
        let mut out = [0u8; 40];
        out.copy_from_slice(bytes_of(&m));
        out
    }

    fn entry_bytes(tx_id: u64) -> [u8; 40] {
        let e = TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind: EntryKind::Credit,
            _pad0: [0; 6],
            tx_id,
            account_id: 1,
            amount: 1,
            computed_balance: 1,
        };
        let mut out = [0u8; 40];
        out.copy_from_slice(bytes_of(&e));
        out
    }

    fn link_bytes(tx_id: u64) -> [u8; 40] {
        let l = TxLink {
            entry_type: WalEntryKind::Link as u8,
            link_kind: TxLinkKind::Duplicate as u8,
            _pad: [0; 6],
            tx_id,
            to_tx_id: 0,
            _pad2: [0; 16],
        };
        let mut out = [0u8; 40];
        out.copy_from_slice(bytes_of(&l));
        out
    }

    #[test]
    fn record_matches_filters_structural_unless_from_zero() {
        let mut header = [0u8; 40];
        header[0] = WalEntryKind::SegmentHeader as u8;
        assert!(record_matches(&header, 0));
        assert!(!record_matches(&header, 1));
    }

    #[test]
    fn record_matches_filters_by_tx_id_for_tx_records() {
        assert!(record_matches(&meta_bytes(5), 5));
        assert!(record_matches(&entry_bytes(5), 5));
        assert!(record_matches(&link_bytes(5), 5));
        assert!(!record_matches(&meta_bytes(5), 6));
    }
}
