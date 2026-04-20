use super::common::{build_range, build_tx, build_tx_with_balance, TEST_TERM};
use roda_ledger::entities::{SegmentHeader, WalEntry, WalEntryKind};
use roda_ledger::replication::{validate_wal_bytes, RejectReason, ENTRY_SIZE};

#[test]
fn accepts_single_transaction_range() {
    let bytes = build_tx(1, 42, 500);
    let entries = validate_wal_bytes(&bytes, 1, 1, TEST_TERM).expect("valid");
    assert_eq!(entries.len(), 2);
    assert!(matches!(entries[0], WalEntry::Metadata(_)));
    assert!(matches!(entries[1], WalEntry::Entry(_)));
}

#[test]
fn accepts_contiguous_multi_tx_range() {
    let bytes = build_range(10, 25, 1, 1);
    let entries = validate_wal_bytes(&bytes, 10, 34, TEST_TERM).expect("valid");
    assert_eq!(entries.len(), 50);
}

#[test]
fn rejects_mid_stream_crc_corruption() {
    let mut bytes = build_range(1, 3, 1, 100);
    bytes[120 + 24] ^= 0x10;
    let err = validate_wal_bytes(&bytes, 1, 3, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::CrcFailed);
    assert!(err.detail.contains("tx 2"), "detail was: {}", err.detail);
}

#[test]
fn rejects_range_window_mismatch_too_wide() {
    let bytes = build_range(1, 3, 1, 100);
    let err = validate_wal_bytes(&bytes, 1, 5, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::SequenceInvalid);
}

#[test]
fn rejects_range_window_mismatch_too_narrow() {
    let bytes = build_range(1, 3, 1, 100);
    let err = validate_wal_bytes(&bytes, 1, 2, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::SequenceInvalid);
}

#[test]
fn rejects_duplicate_tx_id_in_stream() {
    let mut bytes = build_tx(5, 1, 100);
    bytes.extend(build_tx(5, 1, 100));
    let err = validate_wal_bytes(&bytes, 5, 5, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::SequenceInvalid);
}

#[test]
fn rejects_gap_in_tx_id_sequence() {
    let mut bytes = build_tx(1, 1, 100);
    bytes.extend(build_tx(3, 1, 100));
    let err = validate_wal_bytes(&bytes, 1, 3, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::SequenceInvalid);
}

#[test]
fn rejects_misaligned_buffer() {
    let mut bytes = build_tx(1, 1, 100);
    bytes.push(0xAA);
    let err = validate_wal_bytes(&bytes, 1, 1, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::SequenceInvalid);
    assert!(err.detail.contains("not a multiple"));
}

#[test]
fn rejects_empty_buffer() {
    let err = validate_wal_bytes(&[], 1, 1, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::SequenceInvalid);
}

#[test]
fn rejects_buffer_with_only_non_tx_records() {
    let header = SegmentHeader {
        entry_type: WalEntryKind::SegmentHeader as u8,
        version: 1,
        _pad0: [0; 2],
        magic: 0x524F4441,
        segment_id: 1,
        _pad1: [0; 4],
        _pad2: [0; 24],
    };
    let bytes = bytemuck::bytes_of(&header).to_vec();
    let err = validate_wal_bytes(&bytes, 1, 1, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::SequenceInvalid);
    assert!(err.detail.contains("no transactional"));
}

#[test]
fn rejects_truncated_follower_at_tail() {
    let mut bytes = build_tx(1, 1, 100);
    bytes.truncate(ENTRY_SIZE);
    let err = validate_wal_bytes(&bytes, 1, 1, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::SequenceInvalid);
}

#[test]
fn rejects_unknown_record_kind() {
    let mut bytes = build_tx(1, 1, 100);
    let mut garbage = vec![0u8; ENTRY_SIZE];
    garbage[0] = 0xAB;
    bytes.extend(garbage);
    let err = validate_wal_bytes(&bytes, 1, 1, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::SequenceInvalid);
}

#[test]
fn accepts_tx_with_explicit_balance() {
    let bytes = build_tx_with_balance(7, 1, 100, 12_345);
    let entries = validate_wal_bytes(&bytes, 7, 7, TEST_TERM).expect("valid");
    if let WalEntry::Entry(e) = entries[1] {
        assert_eq!(e.computed_balance, 12_345);
    } else {
        panic!("expected TxEntry second");
    }
}

#[test]
fn single_bit_flip_in_timestamp_caught_by_crc() {
    let mut bytes = build_tx(1, 1, 100);
    bytes[16] ^= 0x01;
    let err = validate_wal_bytes(&bytes, 1, 1, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::CrcFailed);
}

#[test]
fn tx_id_bit_flip_caught_by_sequence_check() {
    let mut bytes = build_tx(1, 1, 100);
    bytes[8] ^= 0x02;
    let err = validate_wal_bytes(&bytes, 1, 1, TEST_TERM).unwrap_err();
    assert_eq!(err.reason, RejectReason::SequenceInvalid);
}
