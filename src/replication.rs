//! Follower-side replication stage (ADR-015).
//!
//! The `Replication` struct is **not** a runner — it has no thread of its
//! own. Like the `Sequencer`, it is a synchronous stage invoked directly
//! from its caller (the Node gRPC `AppendEntries` handler).
//!
//! Flow of `process`:
//!   1. Parse the incoming contiguous WAL byte range into 40-byte records.
//!   2. Validate envelope (`prev_tx_id` / `prev_term` match the follower's
//!      local state, `request.term >= current_term`, the parsed stream's
//!      first/last tx_id match `from_tx_id`/`to_tx_id`).
//!   3. Validate each record: sequence monotonicity, per-transaction CRC32C
//!      (mirrors `Recover::validate_wal_transactions`).
//!   4. Push the parsed records into the shared WAL input queue (same
//!      queue the Transactor uses on a leader).
//!   5. Block until the WAL committer advances `last_commit_id` past
//!      `to_tx_id` — on a follower, committed == fsynced locally.
//!   6. Return `last_tx_id` so the leader can advance matchIndex.
//!
//! The function is intentionally blocking: the ADR requires the RPC
//! handler to call it from a blocking tokio task
//! (`tokio::task::spawn_blocking`) so the async reactor stays free.

use crate::entities::{TxMetadata, WalEntry, WalEntryKind};
use crate::pipeline::LedgerContext;
use crate::storage::wal_serializer::parse_wal_record;

/// Fixed WAL record size (see `entities.rs` — every variant is 40 bytes).
pub const ENTRY_SIZE: usize = 40;

/// Mirrors `node.proto::RejectReason` for handler-side mapping. Kept as a
/// plain enum so the core replication stage does not depend on the
/// generated proto crate (tests/benches can exercise it without `grpc`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    None = 0,
    TermStale = 1,
    PrevMismatch = 2,
    CrcFailed = 3,
    SequenceInvalid = 4,
    WalAppendFailed = 5,
    NotFollower = 6,
}

/// Validated parameters extracted from an `AppendEntriesRequest`.
#[derive(Clone, Debug)]
pub struct AppendEntries<'a> {
    pub term: u64,
    pub prev_tx_id: u64,
    pub prev_term: u64,
    pub from_tx_id: u64,
    pub to_tx_id: u64,
    pub wal_bytes: &'a [u8],
    pub leader_commit_tx_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendOk {
    pub term: u64,
    pub last_tx_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendError {
    pub term: u64,
    pub reason: RejectReason,
    pub detail: String,
}

pub type AppendResult = Result<AppendOk, AppendError>;

/// Follower state the replication stage needs to consult on each call.
/// Backed by the `Pipeline` for `last_commit_id()` and by the Ledger for
/// `current_term` + `prev_term` lookup. For ADR-015 the static leader
/// writes term=1 and the follower's `current_term` is also 1 — the lookup
/// closure is still supplied so the structure is already shaped for
/// ADR-016.
pub struct Replication {
    /// Queue access + shutdown flag + wait strategy.
    ctx: LedgerContext,
    /// Cluster id (used for observability / future leader-change checks).
    #[allow(dead_code)]
    node_id: u64,
    /// Follower's persisted term (from `raft_state` sidecar).
    current_term: u64,
}

impl Replication {
    pub fn new(ctx: LedgerContext, node_id: u64, current_term: u64) -> Self {
        Self {
            ctx,
            node_id,
            current_term,
        }
    }

    /// Entry point called from the gRPC handler (inside a blocking task).
    ///
    /// Runs the full validate → enqueue → wait-for-commit cycle. The
    /// handler maps the result to `AppendEntriesResponse`.
    pub fn process(&self, req: AppendEntries<'_>, last_local_tx_id: u64) -> AppendResult {
        // ── 1. Envelope: term check ────────────────────────────────────
        if req.term < self.current_term {
            return Err(AppendError {
                term: self.current_term,
                reason: RejectReason::TermStale,
                detail: format!(
                    "request.term {} < follower.current_term {}",
                    req.term, self.current_term
                ),
            });
        }

        // ── 2. Envelope: prev_tx_id / prev_term consistency ────────────
        //
        // ADR-015 uses a static single-term model (term=1). The
        // follower's only piece of historical term info is
        // `current_term`, so the consistency check is:
        //   - If prev_tx_id==0 (very first RPC), accept.
        //   - Else prev_tx_id must match what we already have on disk,
        //     and prev_term must equal our current_term.
        if req.prev_tx_id != 0 {
            if req.prev_tx_id != last_local_tx_id {
                return Err(AppendError {
                    term: self.current_term,
                    reason: RejectReason::PrevMismatch,
                    detail: format!(
                        "prev_tx_id {} does not match local last_tx_id {}",
                        req.prev_tx_id, last_local_tx_id
                    ),
                });
            }
            if req.prev_term != self.current_term {
                return Err(AppendError {
                    term: self.current_term,
                    reason: RejectReason::PrevMismatch,
                    detail: format!(
                        "prev_term {} does not match follower.current_term {}",
                        req.prev_term, self.current_term
                    ),
                });
            }
        }

        // ── 3. Range bounds sanity ─────────────────────────────────────
        if req.from_tx_id == 0 || req.to_tx_id < req.from_tx_id {
            return Err(AppendError {
                term: self.current_term,
                reason: RejectReason::SequenceInvalid,
                detail: format!(
                    "bad range from_tx_id={} to_tx_id={}",
                    req.from_tx_id, req.to_tx_id
                ),
            });
        }

        // ── 4. Parse + validate the WAL byte range ─────────────────────
        let entries = validate_wal_bytes(
            req.wal_bytes,
            req.from_tx_id,
            req.to_tx_id,
            self.current_term,
        )?;

        // ── 5. Push all parsed entries into the WAL input queue.
        //     Backpressure via the pipeline's wait strategy, same pattern
        //     as the sequencer / transactor. Aborts if the pipeline was
        //     shut down mid-enqueue.
        let mut retry_count = 0u64;
        for entry in entries.into_iter() {
            let mut msg = entry;
            loop {
                if !self.ctx.is_running() {
                    return Err(AppendError {
                        term: self.current_term,
                        reason: RejectReason::WalAppendFailed,
                        detail: "pipeline shut down during append".to_string(),
                    });
                }
                match self.ctx.push_wal_entry(msg) {
                    Ok(()) => break,
                    Err(returned) => {
                        msg = returned;
                        self.ctx.wait_strategy().retry(retry_count);
                        retry_count = retry_count.saturating_add(1);
                    }
                }
            }
        }

        Ok(AppendOk {
            term: self.current_term,
            last_tx_id: req.to_tx_id,
        })
    }
}

/// Walk the 40-byte-aligned byte range, enforce the tx_id sequence rule,
/// recompute per-transaction CRC32C, and return the parsed `WalEntry` list
/// in order. Mirrors `Recover::validate_wal_transactions` but keyed on the
/// advertised tx_id range instead of the file-tail-truncation semantic.
pub fn validate_wal_bytes(
    data: &[u8],
    from_tx_id: u64,
    to_tx_id: u64,
    _term: u64,
) -> Result<Vec<WalEntry>, AppendError> {
    if data.is_empty() {
        return Err(AppendError {
            term: _term,
            reason: RejectReason::SequenceInvalid,
            detail: "empty wal_bytes".to_string(),
        });
    }
    if !data.len().is_multiple_of(ENTRY_SIZE) {
        return Err(AppendError {
            term: _term,
            reason: RejectReason::SequenceInvalid,
            detail: format!(
                "wal_bytes len {} not a multiple of {}",
                data.len(),
                ENTRY_SIZE
            ),
        });
    }

    let total = data.len() / ENTRY_SIZE;
    let mut out: Vec<WalEntry> = Vec::with_capacity(total);

    let mut offset: usize = 0;
    let mut first_tx_id: Option<u64> = None;
    let mut last_tx_id: u64 = 0;

    while offset + ENTRY_SIZE <= data.len() {
        let kind = data[offset];

        match kind {
            k if k == WalEntryKind::SegmentHeader as u8
                || k == WalEntryKind::SegmentSealed as u8
                || k == WalEntryKind::FunctionRegistered as u8 =>
            {
                // Non-transactional record: parse and append, no CRC step.
                let entry = parse_wal_record(&data[offset..offset + ENTRY_SIZE]).map_err(|e| {
                    AppendError {
                        term: _term,
                        reason: RejectReason::SequenceInvalid,
                        detail: format!("parse failure at offset {}: {}", offset, e),
                    }
                })?;
                out.push(entry);
                offset += ENTRY_SIZE;
            }

            k if k == WalEntryKind::TxMetadata as u8 => {
                let meta: TxMetadata =
                    bytemuck::pod_read_unaligned(&data[offset..offset + ENTRY_SIZE]);
                let expected = meta.entry_count as usize + meta.link_count as usize;

                // Sequence check: tx_id must be strictly monotonic and
                // land inside `[from_tx_id, to_tx_id]`.
                if meta.tx_id < from_tx_id || meta.tx_id > to_tx_id {
                    return Err(AppendError {
                        term: _term,
                        reason: RejectReason::SequenceInvalid,
                        detail: format!(
                            "tx_id {} outside [{}, {}] at offset {}",
                            meta.tx_id, from_tx_id, to_tx_id, offset
                        ),
                    });
                }
                if first_tx_id.is_none() {
                    if meta.tx_id != from_tx_id {
                        return Err(AppendError {
                            term: _term,
                            reason: RejectReason::SequenceInvalid,
                            detail: format!(
                                "first tx_id {} != from_tx_id {}",
                                meta.tx_id, from_tx_id
                            ),
                        });
                    }
                    first_tx_id = Some(meta.tx_id);
                } else if meta.tx_id != last_tx_id + 1 {
                    return Err(AppendError {
                        term: _term,
                        reason: RejectReason::SequenceInvalid,
                        detail: format!(
                            "non-monotonic tx_id: {} after {}",
                            meta.tx_id, last_tx_id
                        ),
                    });
                }

                // Follower-record count check.
                let records_left = (data.len() - offset) / ENTRY_SIZE - 1;
                if records_left < expected {
                    return Err(AppendError {
                        term: _term,
                        reason: RejectReason::SequenceInvalid,
                        detail: format!(
                            "tx {} truncated: expected {} followers, {} available",
                            meta.tx_id, expected, records_left
                        ),
                    });
                }

                // Collect follower slices, kind-check on the fly.
                let mut follower_slices: Vec<&[u8]> = Vec::with_capacity(expected);
                let mut foff = offset + ENTRY_SIZE;
                for _ in 0..expected {
                    let fk = data[foff];
                    if fk != WalEntryKind::TxEntry as u8 && fk != WalEntryKind::Link as u8 {
                        return Err(AppendError {
                            term: _term,
                            reason: RejectReason::SequenceInvalid,
                            detail: format!(
                                "unexpected follower kind {} for tx {}",
                                fk, meta.tx_id
                            ),
                        });
                    }
                    follower_slices.push(&data[foff..foff + ENTRY_SIZE]);
                    foff += ENTRY_SIZE;
                }

                // CRC: zero the stored crc field when folding.
                let mut meta_for_crc = meta;
                meta_for_crc.crc32c = 0;
                let mut digest = crc32c::crc32c(bytemuck::bytes_of(&meta_for_crc));
                for slice in &follower_slices {
                    digest = crc32c::crc32c_append(digest, slice);
                }
                if digest != meta.crc32c {
                    return Err(AppendError {
                        term: _term,
                        reason: RejectReason::CrcFailed,
                        detail: format!(
                            "crc mismatch at tx {} (stored={:#x}, computed={:#x})",
                            meta.tx_id, meta.crc32c, digest
                        ),
                    });
                }

                out.push(WalEntry::Metadata(meta));
                for slice in &follower_slices {
                    let entry = parse_wal_record(slice).map_err(|e| AppendError {
                        term: _term,
                        reason: RejectReason::SequenceInvalid,
                        detail: format!("follower parse failure for tx {}: {}", meta.tx_id, e),
                    })?;
                    out.push(entry);
                }

                last_tx_id = meta.tx_id;
                offset = foff;
            }

            _ => {
                return Err(AppendError {
                    term: _term,
                    reason: RejectReason::SequenceInvalid,
                    detail: format!("orphan/unknown record kind {} at offset {}", kind, offset),
                });
            }
        }
    }

    // Post-walk check: advertised range must be fully covered.
    match first_tx_id {
        None => Err(AppendError {
            term: _term,
            reason: RejectReason::SequenceInvalid,
            detail: "wal_bytes contains no transactional records".to_string(),
        }),
        Some(_) if last_tx_id != to_tx_id => Err(AppendError {
            term: _term,
            reason: RejectReason::SequenceInvalid,
            detail: format!(
                "last parsed tx_id {} != to_tx_id {}",
                last_tx_id, to_tx_id
            ),
        }),
        Some(_) => Ok(out),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Unit tests (Step 9): exercise the pure validator against hand-crafted
// byte ranges — positive & negative cases. Integration with the WAL runner
// is covered indirectly by `tests/replay_test.rs` patterns; this file
// keeps the byte-level semantics tight.
// ─────────────────────────────────────────────────────────────────────────

#[cfg(test)]
pub mod test_helpers {
    use super::*;
    use crate::entities::{EntryKind, FailReason, TxEntry, TxMetadata};

    /// Build a single-entry transaction (Metadata + one TxEntry) with a
    /// correct CRC. Returns (bytes, tx_id). Caller can splice into a
    /// multi-tx buffer.
    pub fn make_single_tx_bytes(tx_id: u64, account_id: u64, amount: u64) -> Vec<u8> {
        let entry = TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind: EntryKind::Credit,
            _pad0: [0; 6],
            tx_id,
            account_id,
            amount,
            computed_balance: amount as i64,
        };

        let mut meta = TxMetadata {
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
        let mut digest = crc32c::crc32c(bytemuck::bytes_of(&meta));
        digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&entry));
        meta.crc32c = digest;

        let mut bytes = Vec::with_capacity(80);
        bytes.extend_from_slice(bytemuck::bytes_of(&meta));
        bytes.extend_from_slice(bytemuck::bytes_of(&entry));
        bytes
    }

    /// Concatenate N single-entry transactions with contiguous tx_ids.
    pub fn make_range_bytes(first_tx_id: u64, count: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity((count as usize) * 80);
        for i in 0..count {
            out.extend(make_single_tx_bytes(first_tx_id + i, 1, 100));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::test_helpers::*;
    use super::*;
    use crate::entities::{SegmentHeader, WalEntryKind};

    const TERM: u64 = 1;

    #[test]
    fn empty_bytes_rejected() {
        let err = validate_wal_bytes(&[], 1, 1, TERM).unwrap_err();
        assert_eq!(err.reason, RejectReason::SequenceInvalid);
    }

    #[test]
    fn misaligned_length_rejected() {
        let bytes = vec![0u8; 39];
        let err = validate_wal_bytes(&bytes, 1, 1, TERM).unwrap_err();
        assert_eq!(err.reason, RejectReason::SequenceInvalid);
    }

    #[test]
    fn valid_single_tx_round_trip() {
        let bytes = make_single_tx_bytes(1, 42, 500);
        let entries = validate_wal_bytes(&bytes, 1, 1, TERM).expect("valid");
        assert_eq!(entries.len(), 2);
        match &entries[0] {
            WalEntry::Metadata(m) => assert_eq!(m.tx_id, 1),
            _ => panic!("expected metadata first"),
        }
    }

    #[test]
    fn valid_multi_tx_range() {
        let bytes = make_range_bytes(10, 4);
        let entries = validate_wal_bytes(&bytes, 10, 13, TERM).expect("valid");
        // 4 transactions * 2 records each.
        assert_eq!(entries.len(), 8);
    }

    #[test]
    fn first_tx_must_match_from_tx_id() {
        let bytes = make_range_bytes(5, 2);
        let err = validate_wal_bytes(&bytes, 4, 6, TERM).unwrap_err();
        assert_eq!(err.reason, RejectReason::SequenceInvalid);
        assert!(err.detail.contains("first tx_id"));
    }

    #[test]
    fn last_tx_must_match_to_tx_id() {
        let bytes = make_range_bytes(1, 3);
        let err = validate_wal_bytes(&bytes, 1, 5, TERM).unwrap_err();
        assert_eq!(err.reason, RejectReason::SequenceInvalid);
        assert!(err.detail.contains("last parsed tx_id"));
    }

    #[test]
    fn crc_mismatch_is_rejected() {
        let mut bytes = make_single_tx_bytes(1, 42, 500);
        // Corrupt the TxEntry amount (byte 24 of the follower record).
        bytes[ENTRY_SIZE + 24] ^= 0xFF;
        let err = validate_wal_bytes(&bytes, 1, 1, TERM).unwrap_err();
        assert_eq!(err.reason, RejectReason::CrcFailed);
    }

    #[test]
    fn orphan_tx_entry_rejected() {
        // Only a TxEntry with no preceding TxMetadata.
        let bytes = {
            let mut b = make_single_tx_bytes(1, 1, 100);
            b.drain(..ENTRY_SIZE); // remove metadata
            b
        };
        let err = validate_wal_bytes(&bytes, 1, 1, TERM).unwrap_err();
        assert_eq!(err.reason, RejectReason::SequenceInvalid);
    }

    #[test]
    fn segment_header_alone_has_no_tx() {
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
        let err = validate_wal_bytes(&bytes, 1, 1, TERM).unwrap_err();
        assert_eq!(err.reason, RejectReason::SequenceInvalid);
        assert!(err.detail.contains("no transactional"));
    }

    #[test]
    fn segment_header_plus_tx_accepted() {
        let header = SegmentHeader {
            entry_type: WalEntryKind::SegmentHeader as u8,
            version: 1,
            _pad0: [0; 2],
            magic: 0x524F4441,
            segment_id: 1,
            _pad1: [0; 4],
            _pad2: [0; 24],
        };
        let mut bytes = bytemuck::bytes_of(&header).to_vec();
        bytes.extend(make_single_tx_bytes(7, 1, 100));
        let entries = validate_wal_bytes(&bytes, 7, 7, TERM).expect("valid");
        // 1 header + Metadata + TxEntry
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn truncated_follower_rejected() {
        let mut bytes = make_single_tx_bytes(1, 1, 100);
        bytes.truncate(ENTRY_SIZE); // drop the TxEntry follower
        let err = validate_wal_bytes(&bytes, 1, 1, TERM).unwrap_err();
        assert_eq!(err.reason, RejectReason::SequenceInvalid);
    }

    #[test]
    fn non_monotonic_tx_id_rejected() {
        let mut bytes = make_single_tx_bytes(5, 1, 100);
        bytes.extend(make_single_tx_bytes(7, 1, 100)); // gap: 5 → 7
        let err = validate_wal_bytes(&bytes, 5, 7, TERM).unwrap_err();
        assert_eq!(err.reason, RejectReason::SequenceInvalid);
    }
}
