//! Zero-copy WAL record decoding.
//!
//! Mirrors [`crate::wal_serializer::parse_wal_record`] but returns
//! references into the caller's byte buffer instead of owned values.
//! Used on hot read paths (`WalTailer::tail` trimming, future scan
//! optimisations) where allocating an owned `WalEntry` per record
//! would dominate the cost. The error surface matches
//! `parse_wal_record` — same `std::io::ErrorKind::InvalidData` for
//! too-short slices and unknown kind bytes, with the same message
//! shape so callers can treat the two parsers interchangeably.

use crate::constants::WAL_RECORD_SIZE;
use crate::entities::{
    AccountFlagsUpdated, AccountLinked, AccountOpened, FunctionRegistered, KvConstant, KvEntry,
    TxEntry, TxLink, TxMetadata, TxTerm, WalEntryKind,
};

/// Borrowed view of a single WAL record. Lifetime is tied to the
/// backing byte slice; nothing here owns its data.
#[derive(Debug)]
pub enum WalEntryRef<'a> {
    Metadata(&'a TxMetadata),
    Entry(&'a TxEntry),
    Link(&'a TxLink),
    FunctionRegistered(&'a FunctionRegistered),
    Term(&'a TxTerm),
    AccountOpened(&'a AccountOpened),
    AccountLinked(&'a AccountLinked),
    AccountFlagsUpdated(&'a AccountFlagsUpdated),
    Kv(&'a KvEntry),
    KvConstant(&'a KvConstant),
}

/// Decode a single 40-byte WAL record without copying. The returned
/// reference borrows from `data`. Error semantics match
/// [`crate::wal_serializer::parse_wal_record`]:
/// - `data.len() < WAL_RECORD_SIZE` → `InvalidData` "WAL record too short"
/// - first byte is not a known [`WalEntryKind`] → `InvalidData`
///   "unknown WAL record kind=N"
pub fn read_entry(data: &[u8]) -> Result<WalEntryRef<'_>, std::io::Error> {
    if data.len() < WAL_RECORD_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "WAL record too short: {} bytes (expected {})",
                data.len(),
                WAL_RECORD_SIZE
            ),
        ));
    }
    let rec: &[u8; WAL_RECORD_SIZE] = data[..WAL_RECORD_SIZE].try_into().expect("len-checked");
    let kind = rec[0];
    match kind {
        k if k == WalEntryKind::TxMetadata as u8 => {
            Ok(WalEntryRef::Metadata(bytemuck::from_bytes(rec)))
        }
        k if k == WalEntryKind::TxEntry as u8 => Ok(WalEntryRef::Entry(bytemuck::from_bytes(rec))),
        k if k == WalEntryKind::Link as u8 => Ok(WalEntryRef::Link(bytemuck::from_bytes(rec))),
        k if k == WalEntryKind::FunctionRegistered as u8 => {
            Ok(WalEntryRef::FunctionRegistered(bytemuck::from_bytes(rec)))
        }
        k if k == WalEntryKind::TxTerm as u8 => Ok(WalEntryRef::Term(bytemuck::from_bytes(rec))),
        k if k == WalEntryKind::AccountOpened as u8 => {
            Ok(WalEntryRef::AccountOpened(bytemuck::from_bytes(rec)))
        }
        k if k == WalEntryKind::AccountLinked as u8 => {
            Ok(WalEntryRef::AccountLinked(bytemuck::from_bytes(rec)))
        }
        k if k == WalEntryKind::AccountFlagsUpdated as u8 => {
            Ok(WalEntryRef::AccountFlagsUpdated(bytemuck::from_bytes(rec)))
        }
        k if k == WalEntryKind::Kv as u8 => Ok(WalEntryRef::Kv(bytemuck::from_bytes(rec))),
        k if k == WalEntryKind::KvConstant as u8 => {
            Ok(WalEntryRef::KvConstant(bytemuck::from_bytes(rec)))
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown WAL record kind={}", kind),
        )),
    }
}

/// Iterate `bytes` as a sequence of 40-byte WAL records, stopping
/// **at the first unknown / malformed record**. Trailing bytes shorter
/// than `WAL_RECORD_SIZE` are silently dropped (a stronger guarantee
/// — the iterator never observes a torn record).
///
/// Iteration stops on error rather than skipping past it so that
/// position-keeping callers (e.g. `WalTailer::tail`'s trim loop) can
/// rely on `enumerate()` matching byte offsets exactly. If you need
/// to surface decode errors, call [`read_entry`] directly.
pub fn iter_records(bytes: &[u8]) -> impl Iterator<Item = WalEntryRef<'_>> {
    bytes
        .chunks_exact(WAL_RECORD_SIZE)
        .map_while(|rec| read_entry(rec).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::{
        EntryKind, FailReason, TxEntry, TxLink, TxLinkKind, TxMetadata, TxTerm, WalEntry,
    };
    use crate::wal_serializer::serialize_wal_records;

    fn make_tx_metadata() -> TxMetadata {
        TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            fail_reason: FailReason::NONE,
            sub_item_count: 2,
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
            kind: EntryKind::CREDIT,
            _pad0: [0; 6],
            _pad1: [0; 8],
            account_id: 7,
            amount: 1000,
            computed_balance: 5000,
        }
    }

    fn make_tx_link() -> TxLink {
        TxLink {
            entry_type: WalEntryKind::Link as u8,
            link_kind: TxLinkKind::Duplicate as u8,
            _pad: [0; 6],
            _pad1: [0; 8],
            to_tx_id: 17,
            _pad2: [0; 16],
        }
    }

    fn make_tx_term() -> TxTerm {
        TxTerm {
            entry_type: WalEntryKind::TxTerm as u8,
            _pad0: [0; 7],
            term: 7,
            node_id: 3,
            node_count: 5,
            node_voted: 4,
            _pad1: [0; 12],
        }
    }

    // ── read_entry: error surface mirroring parse_wal_record ──

    #[test]
    fn read_too_short_returns_error() {
        let data = vec![0u8; WAL_RECORD_SIZE - 1];
        let err = read_entry(&data).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn read_unknown_kind_returns_error() {
        let mut data = vec![0u8; WAL_RECORD_SIZE];
        data[0] = 0xFF;
        let err = read_entry(&data).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn read_retired_segment_header_discriminant_errors() {
        let mut data = vec![0u8; WAL_RECORD_SIZE];
        data[0] = 2;
        assert!(read_entry(&data).is_err());
    }

    #[test]
    fn read_retired_segment_sealed_discriminant_errors() {
        let mut data = vec![0u8; WAL_RECORD_SIZE];
        data[0] = 3;
        assert!(read_entry(&data).is_err());
    }

    // ── read_entry: round-trip parity with parse_wal_record ──

    #[test]
    fn read_metadata_round_trip() {
        let owned = WalEntry::Metadata(make_tx_metadata());
        let buf = serialize_wal_records(&owned);
        let view = read_entry(buf).expect("parse metadata");
        match view {
            WalEntryRef::Metadata(m) => assert_eq!(*m, make_tx_metadata()),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn read_tx_entry_round_trip() {
        let owned = WalEntry::Entry(make_tx_entry());
        let buf = serialize_wal_records(&owned);
        let view = read_entry(buf).expect("parse entry");
        match view {
            WalEntryRef::Entry(e) => assert_eq!(*e, make_tx_entry()),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn read_tx_link_round_trip() {
        let owned = WalEntry::Link(make_tx_link());
        let buf = serialize_wal_records(&owned);
        let view = read_entry(buf).expect("parse link");
        match view {
            WalEntryRef::Link(l) => {
                assert_eq!(l.to_tx_id, 17);
                assert_eq!(l.link_kind, TxLinkKind::Duplicate as u8);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn read_tx_term_round_trip() {
        let owned = WalEntry::Term(make_tx_term());
        let buf = serialize_wal_records(&owned);
        let view = read_entry(buf).expect("parse term");
        match view {
            WalEntryRef::Term(t) => {
                assert_eq!(t.term, 7);
                assert_eq!(t.node_id, 3);
                assert_eq!(t.node_count, 5);
                assert_eq!(t.node_voted, 4);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn read_function_registered_round_trip() {
        let reg = FunctionRegistered::new("fee_calc", 3, 0xDEAD_BEEF);
        let owned = WalEntry::FunctionRegistered(reg);
        let buf = serialize_wal_records(&owned);
        let view = read_entry(buf).expect("parse function registered");
        match view {
            WalEntryRef::FunctionRegistered(f) => {
                assert_eq!(f.name_str(), "fee_calc");
                assert_eq!(f.version, 3);
                assert_eq!(f.crc32c, 0xDEAD_BEEF);
                assert!(!f.is_unregister());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn read_function_registered_unregister_carries_zero_crc() {
        let reg = FunctionRegistered::new("fee_calc", 4, 0);
        let owned = WalEntry::FunctionRegistered(reg);
        let buf = serialize_wal_records(&owned);
        let view = read_entry(buf).expect("parse");
        match view {
            WalEntryRef::FunctionRegistered(f) => assert!(f.is_unregister()),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn read_uses_only_first_40_bytes_of_larger_slice() {
        let owned = WalEntry::Metadata(make_tx_metadata());
        let mut buf = serialize_wal_records(&owned).to_vec();
        buf.extend_from_slice(&[0xFF; 20]);
        let view = read_entry(&buf).expect("parse with extra bytes");
        match view {
            WalEntryRef::Metadata(m) => assert_eq!(*m, make_tx_metadata()),
            _ => panic!("wrong variant"),
        }
    }

    // ── iter_records: structural guarantees ──

    /// Build a contiguous record buffer by concatenating
    /// `serialize_wal_records` outputs — the same shape `Wal::run`
    /// produces on disk.
    fn pack(entries: &[WalEntry]) -> Vec<u8> {
        let mut out = Vec::with_capacity(entries.len() * WAL_RECORD_SIZE);
        for e in entries {
            out.extend_from_slice(serialize_wal_records(e));
        }
        out
    }

    #[test]
    fn iter_records_yields_all_known_kinds_in_order() {
        let entries = vec![
            WalEntry::Metadata(make_tx_metadata()),
            WalEntry::Entry(make_tx_entry()),
            WalEntry::Link(make_tx_link()),
            WalEntry::Term(make_tx_term()),
            WalEntry::FunctionRegistered(FunctionRegistered::new("f", 1, 1)),
        ];
        let buf = pack(&entries);
        let kinds: Vec<u8> = iter_records(&buf)
            .map(|r| match r {
                WalEntryRef::Metadata(_) => WalEntryKind::TxMetadata as u8,
                WalEntryRef::Entry(_) => WalEntryKind::TxEntry as u8,
                WalEntryRef::Link(_) => WalEntryKind::Link as u8,
                WalEntryRef::FunctionRegistered(_) => WalEntryKind::FunctionRegistered as u8,
                WalEntryRef::Term(_) => WalEntryKind::TxTerm as u8,
                WalEntryRef::AccountOpened(_) => WalEntryKind::AccountOpened as u8,
                WalEntryRef::AccountLinked(_) => WalEntryKind::AccountLinked as u8,
                WalEntryRef::AccountFlagsUpdated(_) => WalEntryKind::AccountFlagsUpdated as u8,
                WalEntryRef::Kv(_) => WalEntryKind::Kv as u8,
                WalEntryRef::KvConstant(_) => WalEntryKind::KvConstant as u8,
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                WalEntryKind::TxMetadata as u8,
                WalEntryKind::TxEntry as u8,
                WalEntryKind::Link as u8,
                WalEntryKind::TxTerm as u8,
                WalEntryKind::FunctionRegistered as u8,
            ]
        );
    }

    #[test]
    fn iter_records_drops_trailing_partial_chunk() {
        // 40 valid bytes + 7 trailing — chunks_exact discards the 7.
        let mut buf = serialize_wal_records(&WalEntry::Term(make_tx_term())).to_vec();
        buf.extend_from_slice(&[0u8; 7]);
        let count = iter_records(&buf).count();
        assert_eq!(count, 1);
    }

    #[test]
    fn iter_records_stops_at_unknown_kind() {
        // [Term] [unknown=0xFF] [Entry] — the iterator should yield
        // only the leading Term and stop; the trailing Entry must NOT
        // appear because skipping past the unknown record would
        // desync byte offsets in `enumerate()`-driven consumers.
        let mut buf = serialize_wal_records(&WalEntry::Term(make_tx_term())).to_vec();
        let mut unknown = vec![0u8; WAL_RECORD_SIZE];
        unknown[0] = 0xFF;
        buf.extend_from_slice(&unknown);
        buf.extend_from_slice(serialize_wal_records(&WalEntry::Entry(make_tx_entry())));

        let count = iter_records(&buf).count();
        assert_eq!(count, 1);
    }

    #[test]
    fn iter_records_empty_input_yields_nothing() {
        assert_eq!(iter_records(&[]).count(), 0);
    }

    #[test]
    fn iter_records_offsets_match_chunks_exact_when_all_known() {
        // For a buffer of N well-formed records, the iterator yields
        // exactly N items — so `enumerate()`-derived offsets line up
        // with the original byte positions. Asserts the invariant
        // `last_complete_tx_end` in wal_tail.rs depends on.
        let entries: Vec<WalEntry> = (0..16)
            .map(|i| {
                WalEntry::Metadata(TxMetadata {
                    sub_item_count: 0,
                    tx_id: i + 1,
                    ..make_tx_metadata()
                })
            })
            .collect();
        let buf = pack(&entries);
        let yielded: Vec<u64> = iter_records(&buf)
            .filter_map(|r| match r {
                WalEntryRef::Metadata(m) => Some(m.tx_id),
                _ => None,
            })
            .collect();
        assert_eq!(yielded.len(), entries.len());
        for (i, tx_id) in yielded.iter().enumerate() {
            assert_eq!(*tx_id, (i + 1) as u64);
        }
    }
}
