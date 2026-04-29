use std::path::Path;

use storage::entities::*;
use storage::{SegmentStaus, WAL_MAGIC, WAL_VERSION};

use super::json::{compute_tx_crc_with_links, verify_tx_crc_with_links};
use super::{CtlError, SegmentReport, SnapshotReport, VerifyReport, make_storage};

pub fn run(
    dir: &Path,
    segment_filter: Option<u32>,
    range: Option<(u32, u32)>,
) -> Result<VerifyReport, CtlError> {
    if !dir.is_dir() {
        return Err(CtlError::new(format!("{:?} is not a directory", dir)));
    }

    let filter = match (segment_filter, range) {
        (Some(id), _) => Some((id, id)),
        (_, Some(r)) => Some(r),
        _ => None,
    };

    let storage = make_storage(&dir.to_string_lossy())?;

    let all_segments = storage.list_all_segments()?;
    let filtered_ids: Vec<u32> = if let Some((from, to)) = filter {
        all_segments
            .iter()
            .map(|s| s.id())
            .filter(|id| *id >= from && *id <= to)
            .collect()
    } else {
        all_segments.iter().map(|s| s.id()).collect()
    };

    let mut segments: Vec<SegmentReport> = Vec::new();
    for &id in &filtered_ids {
        segments.push(verify_segment(&storage, id));
    }

    let active = if dir.join("wal.bin").exists() && filter.is_none() {
        Some(verify_active_wal(&storage))
    } else {
        None
    };

    let cross_errors = if filter.is_none() && segments.len() > 1 {
        verify_cross_segment(&segments)
    } else {
        vec![]
    };

    Ok(VerifyReport {
        segments,
        active,
        cross_errors,
    })
}

fn verify_segment(storage: &storage::Storage, segment_id: u32) -> SegmentReport {
    let filename = format!("wal_{:06}.bin", segment_id);
    let mut errors = Vec::new();
    let mut ok = true;

    let mut segment = match storage.segment(segment_id) {
        Ok(s) => s,
        Err(e) => {
            return SegmentReport {
                filename,
                status: "UNKNOWN".into(),
                record_count: 0,
                first_tx_id: 0,
                last_tx_id: 0,
                ok: false,
                errors: vec![format!("Cannot open: {}", e)],
                snapshot: None,
            };
        }
    };

    let status = match segment.status() {
        SegmentStaus::SEALED => "SEALED",
        SegmentStaus::CLOSED => "CLOSED",
        SegmentStaus::ACTIVE => "ACTIVE",
    };

    // load() verifies file-level CRC for SEALED segments
    if let Err(e) = segment.load() {
        return SegmentReport {
            filename,
            status: status.into(),
            record_count: 0,
            first_tx_id: 0,
            last_tx_id: 0,
            ok: false,
            errors: vec![format!("Cannot load: {}", e)],
            snapshot: None,
        };
    }

    // Record-level verification via visit_wal_records
    let mut record_count = 0u64;
    let mut first_tx_id = 0u64;
    let mut last_tx_id = 0u64;
    let mut last_meta_tx: Option<u64> = None;
    let mut pending_meta: Option<TxMetadata> = None;
    let mut pending_entries: Vec<TxEntry> = Vec::new();
    let mut pending_links: Vec<TxLink> = Vec::new();
    let mut header_checked = false;
    // Per-account last known computed_balance for continuity checks.
    let mut account_balances: std::collections::HashMap<u64, i64> =
        std::collections::HashMap::new();

    let visit_result = segment.visit_wal_records(|entry| {
        record_count += 1;
        match entry {
            WalEntry::SegmentHeader(h) => {
                if !header_checked {
                    header_checked = true;
                    if h.magic != WAL_MAGIC {
                        errors.push(format!("SegmentHeader: wrong magic {:#010x}", h.magic));
                        ok = false;
                    }
                    if h.version != WAL_VERSION {
                        errors.push(format!("SegmentHeader: unknown version {}", h.version));
                        ok = false;
                    }
                    if h.segment_id != segment_id {
                        errors.push(format!(
                            "SegmentHeader: segment_id {} != filename {}",
                            h.segment_id, segment_id
                        ));
                        ok = false;
                    }
                }
            }
            WalEntry::Metadata(m) => {
                flush_pending(
                    &mut pending_meta,
                    &pending_entries,
                    &pending_links,
                    &mut errors,
                    &mut ok,
                );
                pending_entries.clear();
                pending_links.clear();

                if first_tx_id == 0 {
                    first_tx_id = m.tx_id;
                }

                if let Some(prev) = last_meta_tx
                    && m.tx_id <= prev
                {
                    errors.push(format!("tx_id not monotonic: {} after {}", m.tx_id, prev));
                    ok = false;
                }
                last_meta_tx = Some(m.tx_id);
                last_tx_id = m.tx_id;

                if m.entry_count == 0 && m.link_count == 0 {
                    if !verify_tx_crc_with_links(m, &[], &[]) {
                        errors.push(format!("Record CRC mismatch (tx_id {})", m.tx_id));
                        ok = false;
                    }
                } else {
                    pending_meta = Some(*m);
                }
            }
            WalEntry::Entry(e) => {
                if pending_meta.is_some() {
                    // Balance continuity: if we have seen this account before, its new
                    // computed_balance must equal prev_balance ± amount.
                    // Convention (from transactor.rs): Debit → balance += amount,
                    //                                  Credit → balance -= amount.
                    let delta: i64 = if e.kind == EntryKind::Debit {
                        e.amount as i64
                    } else {
                        -(e.amount as i64)
                    };
                    if let Some(&prev) = account_balances.get(&e.account_id) {
                        let expected = prev + delta;
                        if e.computed_balance != expected {
                            errors.push(format!(
                                "tx_id {}: account {} balance continuity violation \
                                 (prev={}, delta={}, expected={}, got={})",
                                e.tx_id, e.account_id, prev, delta, expected, e.computed_balance
                            ));
                            ok = false;
                        }
                    }
                    account_balances.insert(e.account_id, e.computed_balance);
                    pending_entries.push(*e);
                }
            }
            WalEntry::Link(l) => {
                if pending_meta.is_some() {
                    pending_links.push(*l);
                }
            }
            WalEntry::SegmentSealed(s) => {
                flush_pending(
                    &mut pending_meta,
                    &pending_entries,
                    &pending_links,
                    &mut errors,
                    &mut ok,
                );
                pending_entries.clear();
                pending_links.clear();

                if s.segment_id != segment_id {
                    errors.push(format!(
                        "SegmentSealed: segment_id {} != filename {}",
                        s.segment_id, segment_id
                    ));
                    ok = false;
                }
                if s.record_count != record_count {
                    errors.push(format!(
                        "SegmentSealed: record_count {} != actual {}",
                        s.record_count, record_count
                    ));
                    ok = false;
                }
                last_tx_id = s.last_tx_id;
            }
            // Function-registry events are validated independently: the
            // CRC32C is embedded in the record, no cross-record invariants.
            WalEntry::FunctionRegistered(_) => {}
        }
    });

    if let Err(e) = visit_result {
        errors.push(format!("Record parse error: {}", e));
        ok = false;
    }

    // Snapshot verification
    let snapshot = if segment.has_snapshot() {
        let snap_file = format!("snapshot_{:06}.bin", segment_id);
        match segment.load_snapshot() {
            Ok(Some(data)) => Some(SnapshotReport {
                filename: snap_file,
                account_count: data.balances.len() as u64,
                ok: true,
                errors: vec![],
            }),
            Ok(None) => {
                ok = false;
                Some(SnapshotReport {
                    filename: snap_file,
                    account_count: 0,
                    ok: false,
                    errors: vec!["Snapshot exists but failed verification".into()],
                })
            }
            Err(e) => {
                ok = false;
                Some(SnapshotReport {
                    filename: snap_file,
                    account_count: 0,
                    ok: false,
                    errors: vec![format!("Snapshot error: {}", e)],
                })
            }
        }
    } else {
        None
    };

    SegmentReport {
        filename,
        status: status.into(),
        record_count,
        first_tx_id,
        last_tx_id,
        ok,
        errors,
        snapshot,
    }
}

fn verify_active_wal(storage: &storage::Storage) -> SegmentReport {
    let segment = match storage.active_segment() {
        Ok(s) => s,
        Err(e) => {
            return SegmentReport {
                filename: "wal.bin".into(),
                status: "ACTIVE".into(),
                record_count: 0,
                first_tx_id: 0,
                last_tx_id: 0,
                ok: false,
                errors: vec![format!("Cannot open active WAL: {}", e)],
                snapshot: None,
            };
        }
    };

    // active_segment opens and loads wal.bin already
    let mut record_count = 0u64;
    let first_tx_id = 0u64;
    let mut last_tx_id = 0u64;
    let mut errors = Vec::new();
    let mut ok = true;

    let visit_result = segment.visit_wal_records(|entry| {
        record_count += 1;
        match entry {
            WalEntry::SegmentHeader(_) => {}
            WalEntry::Metadata(m) => last_tx_id = m.tx_id,
            _ => {}
        }
    });

    if let Err(e) = visit_result {
        errors.push(format!("Parse error: {}", e));
        ok = false;
    }

    SegmentReport {
        filename: "wal.bin".into(),
        status: "ACTIVE".into(),
        record_count,
        first_tx_id,
        last_tx_id,
        ok,
        errors,
        snapshot: None,
    }
}

fn flush_pending(
    pending_meta: &mut Option<TxMetadata>,
    entries: &[TxEntry],
    links: &[TxLink],
    errors: &mut Vec<String>,
    ok: &mut bool,
) {
    if let Some(meta) = pending_meta.take() {
        // CRC check (covers metadata + entries + links).
        if !verify_tx_crc_with_links(&meta, entries, links) {
            errors.push(format!(
                "Record CRC mismatch (tx_id {}): expected {:#010x}, actual {:#010x}",
                meta.tx_id,
                meta.crc32c,
                compute_tx_crc_with_links(&meta, entries, links)
            ));
            *ok = false;
        }

        // Zero-sum (double-entry): total debits must equal total credits.
        if !entries.is_empty() {
            let debit_total: u64 = entries
                .iter()
                .filter(|e| e.kind == EntryKind::Debit)
                .map(|e| e.amount)
                .sum();
            let credit_total: u64 = entries
                .iter()
                .filter(|e| e.kind == EntryKind::Credit)
                .map(|e| e.amount)
                .sum();
            if debit_total != credit_total {
                errors.push(format!(
                    "tx_id {}: zero-sum violation (debit_total={}, credit_total={})",
                    meta.tx_id, debit_total, credit_total
                ));
                *ok = false;
            }
        }

        // Entry count declared in metadata must match actual entries.
        if entries.len() != meta.entry_count as usize {
            errors.push(format!(
                "tx_id {}: entry_count mismatch (declared={}, actual={})",
                meta.tx_id,
                meta.entry_count,
                entries.len()
            ));
            *ok = false;
        }

        // Link count declared in metadata must match actual links.
        if links.len() != meta.link_count as usize {
            errors.push(format!(
                "tx_id {}: link_count mismatch (declared={}, actual={})",
                meta.tx_id,
                meta.link_count,
                links.len()
            ));
            *ok = false;
        }
    }
}

fn verify_cross_segment(results: &[SegmentReport]) -> Vec<String> {
    let mut errors = Vec::new();
    for w in results.windows(2) {
        let prev_id = w[0]
            .filename
            .strip_prefix("wal_")
            .and_then(|s| s.strip_suffix(".bin"))
            .and_then(|s| s.parse::<u32>().ok());
        let next_id = w[1]
            .filename
            .strip_prefix("wal_")
            .and_then(|s| s.strip_suffix(".bin"))
            .and_then(|s| s.parse::<u32>().ok());
        if let (Some(p), Some(n)) = (prev_id, next_id)
            && n != p + 1
        {
            errors.push(format!(
                "Cross-segment: gap between segment {} and {}",
                p, n
            ));
        }
        if w[0].last_tx_id > 0 && w[1].first_tx_id > 0 && w[1].first_tx_id != w[0].last_tx_id + 1 {
            errors.push(format!(
                "Cross-segment: tx_id gap between {} (last_tx_id={}) and {} (first_tx_id={})",
                w[0].filename, w[0].last_tx_id, w[1].filename, w[1].first_tx_id
            ));
        }
    }
    errors
}
