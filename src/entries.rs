use crate::entities::{SegmentHeader, SegmentSealed, WalEntryKind};
use crate::storage::{WAL_MAGIC, WAL_VERSION};

pub(crate) fn wal_segment_header_entry(segment_id: u32, first_tx_id: u64) -> SegmentHeader {
    SegmentHeader {
        entry_type: WalEntryKind::SegmentHeader as u8,
        version: WAL_VERSION,
        _pad0: [0; 2],
        magic: WAL_MAGIC,
        segment_id,
        _pad1: [0; 4],
        first_tx_id,
        _pad2: [0; 16],
    }
}

pub(crate) fn wal_segment_sealed_entry(
    segment_id: u32,
    last_tx_id: u64,
    record_count: u64,
) -> SegmentSealed {
    SegmentSealed {
        entry_type: WalEntryKind::SegmentSealed as u8,
        _pad0: [0; 3],
        segment_id,
        last_tx_id,
        record_count: record_count + 1, // +1 for the sealed record itself
        _pad1: [0; 16],
    }
}
