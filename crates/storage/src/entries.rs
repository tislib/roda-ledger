use crate::entities::{SegmentHeader, SegmentSealed, TxTerm, WalEntryKind};
use crate::{WAL_MAGIC, WAL_VERSION};

pub fn wal_segment_header_entry(segment_id: u32) -> SegmentHeader {
    SegmentHeader {
        entry_type: WalEntryKind::SegmentHeader as u8,
        version: WAL_VERSION,
        _pad0: [0; 2],
        magic: WAL_MAGIC,
        segment_id,
        _pad1: [0; 4],
        _pad2: [0; 24],
    }
}

pub fn wal_segment_sealed_entry(
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

pub fn wal_tx_term_entry(term: u64, node_id: u64, node_count: u16, node_voted: u16) -> TxTerm {
    TxTerm {
        entry_type: WalEntryKind::TxTerm as u8,
        _pad0: [0; 7],
        term,
        node_id,
        node_count,
        node_voted,
        _pad1: [0; 12],
    }
}
