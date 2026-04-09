use crate::entities::{
    SegmentHeader, SegmentSealed, TxEntry, TxLink, TxMetadata, WalEntry, WalEntryKind,
};

pub fn serialize_wal_records(entry: &WalEntry) -> &[u8] {
    match entry {
        WalEntry::Metadata(m) => bytemuck::bytes_of(m),
        WalEntry::Entry(e) => bytemuck::bytes_of(e),
        WalEntry::SegmentHeader(h) => bytemuck::bytes_of(h),
        WalEntry::SegmentSealed(s) => bytemuck::bytes_of(s),
        WalEntry::Link(l) => bytemuck::bytes_of(l),
    }
}

pub fn parse_wal_record(data: &[u8]) -> Result<WalEntry, std::io::Error> {
    if data.len() < 40 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("WAL record too short: {} bytes (expected 40)", data.len()),
        ));
    }

    let kind = data[0];
    let record_data = &data[0..40];

    match kind {
        k if k == WalEntryKind::TxMetadata as u8 => {
            let meta: TxMetadata = bytemuck::pod_read_unaligned(record_data);
            Ok(WalEntry::Metadata(meta))
        }
        k if k == WalEntryKind::TxEntry as u8 => {
            let entry: TxEntry = bytemuck::pod_read_unaligned(record_data);
            Ok(WalEntry::Entry(entry))
        }
        k if k == WalEntryKind::SegmentSealed as u8 => {
            let sealed_rec: SegmentSealed = bytemuck::pod_read_unaligned(record_data);
            Ok(WalEntry::SegmentSealed(sealed_rec))
        }
        k if k == WalEntryKind::SegmentHeader as u8 => {
            let header: SegmentHeader = bytemuck::pod_read_unaligned(record_data);
            Ok(WalEntry::SegmentHeader(header))
        }
        k if k == WalEntryKind::Link as u8 => {
            let link: TxLink = bytemuck::pod_read_unaligned(record_data);
            Ok(WalEntry::Link(link))
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown WAL record kind={}", kind),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::{
        EntryKind, FailReason, SegmentHeader, SegmentSealed, TxEntry, TxMetadata, WalEntry,
        WalEntryKind,
    };

    fn make_tx_metadata() -> TxMetadata {
        TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            entry_count: 2,
            fail_reason: FailReason::NONE,
            link_count: 0,
            crc32c: 0xDEADBEEF,
            tx_id: 42,
            timestamp: 1_700_000_000,
            user_ref: 99,
            tag: [1, 2, 3, 4, 5, 6, 7, 8],
        }
    }

    fn make_tx_entry() -> TxEntry {
        TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind: EntryKind::Credit,
            _pad0: [0; 6],
            tx_id: 42,
            account_id: 7,
            amount: 1000,
            computed_balance: 5000,
        }
    }

    fn make_segment_header() -> SegmentHeader {
        SegmentHeader {
            entry_type: WalEntryKind::SegmentHeader as u8,
            version: 1,
            _pad0: [0; 2],
            magic: 0x524F4441,
            segment_id: 3,
            _pad1: [0; 4],
            _pad2: [0; 24],
        }
    }

    fn make_segment_sealed() -> SegmentSealed {
        SegmentSealed {
            entry_type: WalEntryKind::SegmentSealed as u8,
            _pad0: [0; 3],
            segment_id: 3,
            last_tx_id: 200,
            record_count: 50,
            _pad1: [0; 16],
        }
    }

    // --- serialize tests ---

    #[test]
    fn serialize_metadata_produces_40_bytes() {
        let entry = WalEntry::Metadata(make_tx_metadata());
        let buf = serialize_wal_records(&entry);
        assert_eq!(buf.len(), 40);
        assert_eq!(buf[0], WalEntryKind::TxMetadata as u8);
    }

    #[test]
    fn serialize_tx_entry_produces_40_bytes() {
        let entry = WalEntry::Entry(make_tx_entry());
        let buf = serialize_wal_records(&entry);
        assert_eq!(buf.len(), 40);
        assert_eq!(buf[0], WalEntryKind::TxEntry as u8);
    }

    #[test]
    fn serialize_segment_header_produces_40_bytes() {
        let entry = WalEntry::SegmentHeader(make_segment_header());
        let buf = serialize_wal_records(&entry);
        assert_eq!(buf.len(), 40);
        assert_eq!(buf[0], WalEntryKind::SegmentHeader as u8);
    }

    #[test]
    fn serialize_segment_sealed_produces_40_bytes() {
        let entry = WalEntry::SegmentSealed(make_segment_sealed());
        let buf = serialize_wal_records(&entry);
        assert_eq!(buf.len(), 40);
        assert_eq!(buf[0], WalEntryKind::SegmentSealed as u8);
    }

    // --- parse tests ---

    #[test]
    fn parse_too_short_returns_error() {
        let data = vec![0u8; 39];
        let result = parse_wal_record(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn parse_unknown_kind_returns_error() {
        let mut data = vec![0u8; 40];
        data[0] = 0xFF; // unknown kind
        let result = parse_wal_record(&data);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn parse_metadata_round_trip() {
        let original = WalEntry::Metadata(make_tx_metadata());
        let buf = serialize_wal_records(&original);
        let parsed = parse_wal_record(buf).expect("should parse metadata");
        assert_eq!(parsed, original);
    }

    #[test]
    fn parse_tx_entry_round_trip() {
        let original = WalEntry::Entry(make_tx_entry());
        let buf = serialize_wal_records(&original);
        let parsed = parse_wal_record(buf).expect("should parse tx entry");
        assert_eq!(parsed, original);
    }

    #[test]
    fn parse_segment_header_round_trip() {
        let original = WalEntry::SegmentHeader(make_segment_header());
        let buf = serialize_wal_records(&original);
        let parsed = parse_wal_record(buf).expect("should parse segment header");
        assert_eq!(parsed, original);
    }

    #[test]
    fn parse_segment_sealed_round_trip() {
        let original = WalEntry::SegmentSealed(make_segment_sealed());
        let buf = serialize_wal_records(&original);
        let parsed = parse_wal_record(buf).expect("should parse segment sealed");
        assert_eq!(parsed, original);
    }

    #[test]
    fn parse_uses_only_first_40_bytes_of_larger_slice() {
        let original = WalEntry::Metadata(make_tx_metadata());
        let serialized = serialize_wal_records(&original);
        let mut buf = serialized.to_vec();
        buf.extend_from_slice(&[0xFF; 20]); // trailing garbage
        let parsed = parse_wal_record(&buf).expect("should parse with extra bytes");
        assert_eq!(parsed, original);
    }

    #[test]
    fn serialize_then_parse_all_variants_in_sequence() {
        let entries = vec![
            WalEntry::SegmentHeader(make_segment_header()),
            WalEntry::Metadata(make_tx_metadata()),
            WalEntry::Entry(make_tx_entry()),
            WalEntry::SegmentSealed(make_segment_sealed()),
        ];

        for expected in &entries {
            let buf = serialize_wal_records(expected);
            assert_eq!(buf.len(), 40);
            let parsed = parse_wal_record(buf).expect("should parse each entry");
            assert_eq!(&parsed, expected);
        }
    }
}
