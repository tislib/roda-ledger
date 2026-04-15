use crate::entities::{FailReason, TxMetadata, WalEntry, WalEntryKind};
use crate::pipeline::Pipeline;
use crate::seal::Seal;
use crate::snapshot::Snapshot;
use crate::storage::SegmentStaus::SEALED;
use crate::storage::{Segment, Storage};
use crate::transactor::Transactor;
use spdlog::{info, warn};
use std::collections::HashMap;
use std::sync::Arc;

const ENTRY_SIZE: usize = 40;

pub struct Recover<'r> {
    transactor: &'r mut Transactor,
    snapshot: &'r mut Snapshot,
    seal: &'r mut Seal,
    pipeline: &'r Arc<Pipeline>,
    storage: &'r Storage,
    segments: Vec<Segment>,
}

impl<'r> Recover<'r> {
    pub fn new(
        transactor: &'r mut Transactor,
        snapshot: &'r mut Snapshot,
        seal: &'r mut Seal,
        pipeline: &'r Arc<Pipeline>,
        storage: &'r Storage,
    ) -> Self {
        Self {
            transactor,
            snapshot,
            seal,
            pipeline,
            storage,
            segments: vec![],
        }
    }

    /// Runs crash recovery on the active WAL segment if needed.
    ///
    /// Detects a crash when `wal.bin` exists but `wal.stop` does not.
    /// Reads the raw WAL data, walks it transaction-by-transaction validating
    /// CRC, and truncates the file if a partially written transaction is found
    /// at the tail.
    ///
    /// If a broken transaction is found in the **middle** (i.e. there are valid
    /// transactions after it), recovery refuses to cut and returns an error —
    /// this indicates non-recoverable corruption.
    ///
    /// After the check, `wal.stop` is always removed so that a crash during
    /// *this* run will be detectable on the next start.
    pub fn crash_recover_if_needed(storage: &Storage) -> Result<(), std::io::Error> {
        let data_dir = &storage.config().data_dir;

        if Segment::has_active_wal(data_dir) && !Segment::has_wal_stop(data_dir) {
            warn!("========================================================");
            warn!("  WAL CRASH RECOVERY: wal.bin exists without wal.stop");
            warn!("  The previous run did not shut down cleanly.");
            warn!("  Running crash recovery on the active segment...");
            warn!("========================================================");

            let mut active = storage.active_segment().map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("crash recovery: failed to open active segment: {}", e),
                )
            })?;

            let total_len = active.wal_data_len();
            let data = active.wal_data_copy();

            let valid_end = Self::validate_wal_transactions(&data)?;

            if valid_end < total_len {
                warn!(
                    "crash recovery: truncating wal.bin from {} to {} bytes \
                     (removing {} bytes of partial/corrupt data at tail)",
                    total_len,
                    valid_end,
                    total_len - valid_end
                );
                active.truncate_wal(valid_end as u64)?;
            } else {
                info!("crash recovery: active segment is consistent, no truncation needed");
            }
        }

        // Always remove wal.stop so a crash during this run is detectable.
        Segment::delete_wal_stop(data_dir)?;

        Ok(())
    }

    /// Walks the raw WAL data transaction-by-transaction, verifying CRC for
    /// each one.  Returns the byte offset of the last fully validated record.
    ///
    /// A broken transaction at the tail is tolerated (the offset before it is
    /// returned).  A broken transaction in the middle (with valid transactions
    /// after it) is non-recoverable and returns an error.
    fn validate_wal_transactions(data: &[u8]) -> Result<usize, std::io::Error> {
        let aligned_len = (data.len() / ENTRY_SIZE) * ENTRY_SIZE;
        let data = &data[..aligned_len];

        // Reusable buffer — collect follower slices for CRC computation,
        // reuse capacity across transactions.
        let mut follower_slices: Vec<&[u8]> = Vec::with_capacity(256);

        let mut offset: usize = 0;
        let mut last_good: usize = 0;

        while offset + ENTRY_SIZE <= data.len() {
            let kind = data[offset];

            match kind {
                // ── Structural records (SegmentHeader / SegmentSealed) ──
                k if k == WalEntryKind::SegmentHeader as u8
                    || k == WalEntryKind::SegmentSealed as u8 =>
                {
                    offset += ENTRY_SIZE;
                    last_good = offset;
                }

                // ── Transaction: TxMetadata + entries + links ───────────
                k if k == WalEntryKind::TxMetadata as u8 => {
                    let meta: TxMetadata =
                        bytemuck::pod_read_unaligned(&data[offset..offset + ENTRY_SIZE]);
                    let expected = meta.entry_count as usize + meta.link_count as usize;
                    let tx_start = offset;

                    // Check whether enough records remain for this tx.
                    let records_left = (data.len() - offset) / ENTRY_SIZE - 1;
                    if records_left < expected {
                        return Self::handle_broken_tx(
                            data,
                            tx_start,
                            last_good,
                            "not enough follower records",
                        );
                    }

                    // Validate follower kinds and collect slices for CRC.
                    follower_slices.clear();
                    let mut foff = offset + ENTRY_SIZE;
                    let mut followers_ok = true;
                    for _ in 0..expected {
                        let fk = data[foff];
                        if fk != WalEntryKind::TxEntry as u8 && fk != WalEntryKind::Link as u8 {
                            followers_ok = false;
                            break;
                        }
                        follower_slices.push(&data[foff..foff + ENTRY_SIZE]);
                        foff += ENTRY_SIZE;
                    }

                    if !followers_ok {
                        return Self::handle_broken_tx(
                            data,
                            tx_start,
                            last_good,
                            "unexpected follower record kind",
                        );
                    }

                    // ── CRC verification ─────────────────────────────
                    let mut meta_for_crc = meta;
                    meta_for_crc.crc32c = 0;
                    let mut digest = crc32c::crc32c(bytemuck::bytes_of(&meta_for_crc));
                    for slice in &follower_slices {
                        digest = crc32c::crc32c_append(digest, slice);
                    }

                    if digest != meta.crc32c {
                        return Self::handle_broken_tx(
                            data,
                            tx_start,
                            last_good,
                            &format!(
                                "CRC mismatch (stored={:#010x}, computed={:#010x})",
                                meta.crc32c, digest
                            ),
                        );
                    }

                    // Transaction is valid.
                    offset = foff;
                    last_good = offset;
                }

                // ── Orphan entry/link or unknown kind ───────────────────
                _ => {
                    return Self::handle_broken_tx(
                        data,
                        offset,
                        last_good,
                        &format!("unexpected record kind {}", kind),
                    );
                }
            }
        }

        Ok(last_good)
    }

    /// Decides whether a broken transaction can be truncated.
    ///
    /// If the broken tx is at the tail (no valid complete transaction follows),
    /// returns `Ok(last_good)` so the caller can truncate there.
    ///
    /// If there is a valid transaction *after* the broken one, the damage is
    /// in the middle and we refuse to cut — returns an error.
    fn handle_broken_tx(
        data: &[u8],
        broken_offset: usize,
        last_good: usize,
        reason: &str,
    ) -> Result<usize, std::io::Error> {
        if Self::has_valid_tx_after(data, broken_offset) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "crash recovery FAILED: broken transaction at offset {} ({}) \
                     is NOT at the tail — valid transactions exist after it. \
                     Non-recoverable WAL corruption.",
                    broken_offset, reason
                ),
            ));
        }

        warn!(
            "crash recovery: broken transaction at offset {} ({}). \
             This is the last element — safe to truncate to offset {}.",
            broken_offset, reason, last_good
        );
        Ok(last_good)
    }

    /// Scans forward from `from_offset` to check if any complete, CRC-valid
    /// transaction exists after this point.
    fn has_valid_tx_after(data: &[u8], from_offset: usize) -> bool {
        let mut off = from_offset;

        while off + ENTRY_SIZE <= data.len() {
            let kind = data[off];

            if kind == WalEntryKind::TxMetadata as u8 {
                if off + ENTRY_SIZE > data.len() {
                    return false;
                }
                let meta: TxMetadata = bytemuck::pod_read_unaligned(&data[off..off + ENTRY_SIZE]);
                let expected = meta.entry_count as usize + meta.link_count as usize;
                let records_available = (data.len() - off) / ENTRY_SIZE - 1;

                if records_available < expected {
                    off += ENTRY_SIZE;
                    continue;
                }

                // Check follower kinds.
                let mut foff = off + ENTRY_SIZE;
                let mut followers_ok = true;
                let mut follower_slices: Vec<&[u8]> = Vec::with_capacity(expected);
                for _ in 0..expected {
                    let fk = data[foff];
                    if fk != WalEntryKind::TxEntry as u8 && fk != WalEntryKind::Link as u8 {
                        followers_ok = false;
                        break;
                    }
                    follower_slices.push(&data[foff..foff + ENTRY_SIZE]);
                    foff += ENTRY_SIZE;
                }

                if !followers_ok {
                    off += ENTRY_SIZE;
                    continue;
                }

                // CRC check.
                let mut meta_for_crc = meta;
                meta_for_crc.crc32c = 0;
                let mut digest = crc32c::crc32c(bytemuck::bytes_of(&meta_for_crc));
                for slice in &follower_slices {
                    digest = crc32c::crc32c_append(digest, slice);
                }

                if digest == meta.crc32c {
                    // Found a valid transaction after the broken one.
                    return true;
                }

                off += ENTRY_SIZE;
                continue;
            }

            off += ENTRY_SIZE;
        }

        false
    }

    pub fn recover(&mut self) -> Result<(), std::io::Error> {
        info!("Starting recovery...");

        // locate segments
        self.segments = self.storage.list_all_segments().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to list segments during recovery: {}", e),
            )
        })?;

        // find the latest snapshot
        let latest_snapshot_segment_id = self.locate_latest_snapshot_segment_id().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to locate latest snapshot segment: {}", e),
            )
        })?;
        let mut last_tx_id = 0;
        let mut recover_balances = HashMap::new();

        // restore the latest snapshot
        for segment in self.segments.iter_mut() {
            // ignore segments before snapshot
            if segment.id() < latest_snapshot_segment_id {
                continue;
            }

            // restore the snapshot
            if segment.id() == latest_snapshot_segment_id {
                let data = segment
                    .load_snapshot()
                    .map_err(|e| {
                        std::io::Error::new(
                            e.kind(),
                            format!(
                                "failed to load snapshot for segment {}: {}",
                                segment.id(),
                                e
                            ),
                        )
                    })?
                    .unwrap();

                for (account_id, balance) in data.balances {
                    recover_balances.insert(account_id, balance);
                    self.seal
                        .recover_balance(account_id as usize, balance)
                        .map_err(|e| {
                            std::io::Error::new(
                                e.kind(),
                                format!(
                                    "failed to recover balance for account {} from snapshot: {}",
                                    account_id, e
                                ),
                            )
                        })?;
                }

                last_tx_id = data.last_tx_id;

                continue;
            }

            // process the segments after snapshot
            if segment.status() != SEALED {
                let sealed_id = self.seal.recover_pre_seal(segment).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!(
                            "failed to recover pre-seal for segment {}: {}",
                            segment.id(),
                            e
                        ),
                    )
                })?;
                self.pipeline.set_seal_index(sealed_id);
            }

            segment.load().map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to load segment {}: {}", segment.id(), e),
                )
            })?;

            let mut segment_recover_tx_id = 0u64;
            segment
                .visit_wal_records(|record| match record {
                    WalEntry::Metadata(metadata) => {
                        last_tx_id = metadata.tx_id;
                        segment_recover_tx_id = metadata.tx_id;
                        self.snapshot.recover_index_tx_metadata(metadata);
                    }
                    WalEntry::Entry(entry) => {
                        recover_balances.insert(entry.account_id, entry.computed_balance);
                        self.snapshot.recover_index_tx_entry(entry);
                    }
                    WalEntry::Link(link) => {
                        self.snapshot
                            .recover_index_tx_link(segment_recover_tx_id, link);
                    }
                    _ => {}
                })
                .map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!(
                            "failed to visit wal records for segment {}: {}",
                            segment.id(),
                            e
                        ),
                    )
                })?;
        }

        // process active WAL records
        let active_segment = self.storage.active_segment().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to get active segment: {}", e))
        })?;
        let mut current_recover_tx_id = 0u64;
        active_segment
            .visit_wal_records(|record| match record {
                WalEntry::Metadata(metadata) => {
                    last_tx_id = metadata.tx_id;
                    current_recover_tx_id = metadata.tx_id;
                    self.snapshot.recover_index_tx_metadata(metadata);

                    // Only non-duplicate committed transactions should be in the dedup cache
                    if metadata.user_ref != 0 && metadata.fail_reason != FailReason::DUPLICATE {
                        self.transactor.dedup_cache_mut().recover_entry(
                            metadata.user_ref,
                            metadata.tx_id,
                            last_tx_id,
                        );
                    }
                }
                WalEntry::Entry(entry) => {
                    recover_balances.insert(entry.account_id, entry.computed_balance);
                    self.snapshot.recover_index_tx_entry(entry);
                }
                WalEntry::Link(link) => {
                    self.snapshot
                        .recover_index_tx_link(current_recover_tx_id, link);
                }
                _ => {}
            })
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to visit active wal records: {}", e),
                )
            })?;

        self.transactor.recover_balances(&recover_balances);
        self.snapshot.recover_balances(&recover_balances);

        // Restore last tx ids in the pipeline indexes.
        self.pipeline.set_compute_index(last_tx_id);
        self.pipeline.set_snapshot_index(last_tx_id);
        self.pipeline.set_commit_index(last_tx_id);
        self.pipeline.set_sequencer_next_id(last_tx_id + 1);

        info!("Recovery completed successfully.");

        Ok(())
    }

    fn locate_latest_snapshot_segment_id(&self) -> Result<u32, std::io::Error> {
        let mut last_snapshot_segment_id = 0;

        for segment in self.segments.iter() {
            if segment.has_snapshot() {
                last_snapshot_segment_id = segment.id();
            }
        }

        Ok(last_snapshot_segment_id)
    }
}
