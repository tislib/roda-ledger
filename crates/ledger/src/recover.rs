use crate::pipeline::Pipeline;
use crate::seal::Seal;
use crate::snapshot::Snapshot;
use crate::transactor::Transactor;
use spdlog::{debug, warn};
use std::collections::HashMap;
use std::sync::Arc;
use storage::SegmentStaus::SEALED;
use storage::entities::{FailReason, FunctionRegistered, TxMetadata, WalEntry, WalEntryKind};
use storage::{Segment, Storage};

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
                debug!("crash recovery: active segment is consistent, no truncation needed");
            }
        }

        // Always remove wal.stop so a crash during this run is detectable.
        Segment::delete_wal_stop(data_dir)?;

        Ok(())
    }

    /// Walks the raw WAL data transaction-by-transaction (trailer layout:
    /// followers precede the closing `TxMetadata`), verifying each group's
    /// declared count and CRC. Returns the byte offset of the last fully
    /// validated record — the end of the last complete transaction.
    ///
    /// A partial tail (followers with no closing metadata, or a torn record) is
    /// truncated to that offset. A broken transaction in the middle (with valid
    /// transactions after it) is non-recoverable and returns an error.
    fn validate_wal_transactions(data: &[u8]) -> Result<usize, std::io::Error> {
        let aligned_len = (data.len() / ENTRY_SIZE) * ENTRY_SIZE;
        let data = &data[..aligned_len];

        // Reusable buffer — collect a transaction's follower slices until its
        // closing metadata, which carries the count and the CRC over
        // (followers ++ zeroed-crc metadata).
        let mut follower_slices: Vec<&[u8]> = Vec::with_capacity(256);
        let mut group_start: usize = 0;

        let mut offset: usize = 0;
        let mut last_good: usize = 0;

        while offset + ENTRY_SIZE <= data.len() {
            let kind = data[offset];

            if kind == WalEntryKind::TxMetadata as u8 {
                // ── Closing metadata: validate the buffered group ───────
                let meta: TxMetadata =
                    bytemuck::pod_read_unaligned(&data[offset..offset + ENTRY_SIZE]);
                let broken_at = if follower_slices.is_empty() {
                    offset
                } else {
                    group_start
                };

                if follower_slices.len() != meta.sub_item_count as usize {
                    return Self::handle_broken_tx(
                        data,
                        broken_at,
                        last_good,
                        "follower count mismatch",
                    );
                }

                // CRC over the followers (push order) then the zeroed-crc metadata.
                let mut digest = 0u32;
                for slice in &follower_slices {
                    digest = crc32c::crc32c_append(digest, slice);
                }
                let mut meta_for_crc = meta;
                meta_for_crc.crc32c = 0;
                digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&meta_for_crc));

                if digest != meta.crc32c {
                    return Self::handle_broken_tx(
                        data,
                        broken_at,
                        last_good,
                        &format!(
                            "CRC mismatch (stored={:#010x}, computed={:#010x})",
                            meta.crc32c, digest
                        ),
                    );
                }

                // Group complete — it ends exactly at this metadata.
                offset += ENTRY_SIZE;
                last_good = offset;
                follower_slices.clear();
            } else if kind == WalEntryKind::TxEntry as u8
                || kind == WalEntryKind::Link as u8
                || kind == WalEntryKind::TxTerm as u8
                || kind == WalEntryKind::FunctionRegistered as u8
            {
                // ── Follower: buffer it until the closing metadata ──────
                if follower_slices.is_empty() {
                    group_start = offset;
                }
                follower_slices.push(&data[offset..offset + ENTRY_SIZE]);
                offset += ENTRY_SIZE;
            } else {
                return Self::handle_broken_tx(
                    data,
                    offset,
                    last_good,
                    &format!("unexpected record kind {}", kind),
                );
            }
        }

        // The valid region must end at a TxMetadata. Followers still buffered are a
        // partial tail with no commit record — truncating to `last_good` drops them.
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

    /// Scans forward from `from_offset` to check whether any complete, CRC-valid
    /// transaction exists after this point (trailer layout: a group is complete
    /// when a `TxMetadata` validates the followers buffered before it).
    fn has_valid_tx_after(data: &[u8], from_offset: usize) -> bool {
        let mut off = from_offset;
        let mut follower_slices: Vec<&[u8]> = Vec::with_capacity(256);

        while off + ENTRY_SIZE <= data.len() {
            let kind = data[off];

            if kind == WalEntryKind::TxMetadata as u8 {
                let meta: TxMetadata = bytemuck::pod_read_unaligned(&data[off..off + ENTRY_SIZE]);
                if follower_slices.len() == meta.sub_item_count as usize {
                    let mut digest = 0u32;
                    for slice in &follower_slices {
                        digest = crc32c::crc32c_append(digest, slice);
                    }
                    let mut meta_for_crc = meta;
                    meta_for_crc.crc32c = 0;
                    digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&meta_for_crc));
                    if digest == meta.crc32c {
                        // A complete, CRC-valid transaction exists after the break.
                        return true;
                    }
                }
                // This metadata did not close a valid group — restart buffering.
                follower_slices.clear();
                off += ENTRY_SIZE;
            } else if kind == WalEntryKind::TxEntry as u8
                || kind == WalEntryKind::Link as u8
                || kind == WalEntryKind::TxTerm as u8
                || kind == WalEntryKind::FunctionRegistered as u8
            {
                follower_slices.push(&data[off..off + ENTRY_SIZE]);
                off += ENTRY_SIZE;
            } else {
                follower_slices.clear();
                off += ENTRY_SIZE;
            }
        }

        false
    }

    /// Watermark-bounded recovery. The unbounded path is recovered by
    /// passing `watermark = u64::MAX` (what `Ledger::start` does);
    /// `Ledger::start_with_recovery_until` (ADR-0016 §9) passes a
    /// finite watermark. Replays snapshot + WAL up to and including
    /// `watermark` only; records whose `tx_id > watermark` are visited
    /// but not applied.
    ///
    /// Snapshot selection picks the latest sealed snapshot whose
    /// `last_tx_id <= watermark`, falling back to genesis (no
    /// snapshot) when none qualify. Pipeline indices are clamped to
    /// `min(replayed_last_tx, watermark)`.
    ///
    /// The watermark applies only to **transactional** records
    /// (`Metadata`, `Entry`, `Link`). `FunctionRegistered` records are
    /// applied or skipped based on whether the most recently observed
    /// `Metadata` had `tx_id > watermark` — i.e. function registrations
    /// that occurred *after* the last accepted transaction are dropped
    /// alongside the diverged tail.
    ///
    /// Caller (`Ledger::start_with_recovery_until`) is expected to have
    /// already invoked `Storage::truncate_wal_above(watermark)`. Even
    /// without that, `recover_until` is correct on its own — it simply
    /// won't physically reclaim the disk space of the rejected tail.
    pub fn recover_until(&mut self, watermark: u64) -> Result<(), std::io::Error> {
        debug!("Starting recovery (watermark={})...", watermark);

        // locate segments
        self.segments = self.storage.list_all_segments().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to list segments during recovery: {}", e),
            )
        })?;

        // find the latest snapshot whose covered_up_to_tx ≤ watermark
        let latest_snapshot_segment_id = self
            .locate_latest_snapshot_segment_id_for_watermark(watermark)
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to locate latest snapshot segment: {}", e),
                )
            })?;
        let mut last_tx_id = 0;
        let mut recover_balances = HashMap::new();
        // Trailer layout: a transaction's followers precede its `TxMetadata`, which
        // supplies the tx_id, the watermark decision, and the index ordering. Hold
        // the in-flight followers until the metadata; the buffer carries across
        // segment seams since a transaction may span a rotation boundary.
        let mut group: Vec<WalEntry> = Vec::new();

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

                // Pair: load the function snapshot for the same segment
                // and seed both Seal's function map and the WASM runtime
                // before any FunctionRegistered records are replayed
                // forward (internal.md §12.3 / §12.4).
                if let Some(fdata) = segment.load_function_snapshot().map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!(
                            "failed to load function snapshot for segment {}: {}",
                            segment.id(),
                            e
                        ),
                    )
                })? {
                    for (name, version, crc) in fdata.entries {
                        self.seal
                            .recover_function_record(&name, version, crc)
                            .map_err(|e| {
                                std::io::Error::new(
                                    e.kind(),
                                    format!(
                                        "failed to seed seal function map for {} v{}: {}",
                                        name, version, e
                                    ),
                                )
                            })?;
                        let record = FunctionRegistered::new(&name, version, crc);
                        self.transactor
                            .recover_function_registered(&record)
                            .map_err(|e| {
                                std::io::Error::new(
                                    e.kind(),
                                    format!(
                                        "recover: function snapshot apply for {} v{} failed: {}",
                                        name, version, e
                                    ),
                                )
                            })?;
                    }
                }

                continue;
            }

            // process the segments after snapshot
            if segment.status() != SEALED {
                // Pass the seal-watermark through so `recover_pre_seal`
                // can sanity-check it: if a closed segment's last tx
                // is somehow still above the watermark at recovery
                // time, truncation failed to remove the diverged tail
                // and we must abort rather than seal unsafe content
                // (ADR-0016 §10). On healthy data this is always a
                // pass-through.
                let sw = self.pipeline.get_seal_watermark();
                let sealed_id = self.seal.recover_pre_seal(segment, sw).map_err(|e| {
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

            let snapshot = &mut self.snapshot;
            let transactor = &*self.transactor;
            let seal = &mut *self.seal;
            let mut function_apply_err: Option<std::io::Error> = None;
            segment
                .visit_wal_records(|record| match record {
                    // Trailer layout: the metadata closes the transaction. Decide
                    // skip/apply here with the authoritative tx_id, then flush the
                    // followers buffered before it.
                    WalEntry::Metadata(metadata) => {
                        if metadata.tx_id > watermark {
                            group.clear();
                            return;
                        }
                        last_tx_id = metadata.tx_id;
                        snapshot.recover_index_tx_metadata(metadata);
                        for follower in group.drain(..) {
                            match follower {
                                WalEntry::Entry(entry) => {
                                    recover_balances
                                        .insert(entry.account_id, entry.computed_balance);
                                    snapshot.recover_index_tx_entry(metadata.tx_id, &entry);
                                }
                                WalEntry::Link(link) => {
                                    snapshot.recover_index_tx_link(metadata.tx_id, &link);
                                }
                                WalEntry::FunctionRegistered(f) if function_apply_err.is_none() => {
                                    if let Err(e) = transactor.recover_function_registered(&f) {
                                        function_apply_err = Some(std::io::Error::new(
                                            e.kind(),
                                            format!(
                                                "recover: replay FunctionRegistered({} v{}) failed: {}",
                                                f.name_str(),
                                                f.version,
                                                e
                                            ),
                                        ));
                                    } else if let Err(e) = seal.recover_function_record(
                                        f.name_str(),
                                        f.version,
                                        f.crc32c,
                                    ) {
                                        function_apply_err = Some(std::io::Error::new(
                                            e.kind(),
                                            format!(
                                                "recover: seal map update for {} v{} failed: {}",
                                                f.name_str(),
                                                f.version,
                                                e
                                            ),
                                        ));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    other => group.push(*other),
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
            if let Some(e) = function_apply_err {
                return Err(e);
            }
        }

        // process active WAL records
        let active_segment = self.storage.active_segment().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to get active segment: {}", e))
        })?;
        let snapshot = &mut self.snapshot;
        let transactor = &mut self.transactor;
        let seal = &mut *self.seal;
        let mut function_apply_err: Option<std::io::Error> = None;
        active_segment
            .visit_wal_records(|record| match record {
                WalEntry::Metadata(metadata) => {
                    if metadata.tx_id > watermark {
                        group.clear();
                        return;
                    }
                    last_tx_id = metadata.tx_id;
                    snapshot.recover_index_tx_metadata(metadata);

                    // Only non-duplicate committed transactions belong in the dedup cache.
                    if metadata.user_ref != 0 && metadata.fail_reason != FailReason::DUPLICATE {
                        transactor.dedup_cache_mut().recover_entry(
                            metadata.user_ref,
                            metadata.tx_id,
                            last_tx_id,
                        );
                    }
                    for follower in group.drain(..) {
                        match follower {
                            WalEntry::Entry(entry) => {
                                recover_balances.insert(entry.account_id, entry.computed_balance);
                                snapshot.recover_index_tx_entry(metadata.tx_id, &entry);
                            }
                            WalEntry::Link(link) => {
                                snapshot.recover_index_tx_link(metadata.tx_id, &link);
                            }
                            WalEntry::FunctionRegistered(f) if function_apply_err.is_none() => {
                                if let Err(e) = transactor.recover_function_registered(&f) {
                                    function_apply_err = Some(std::io::Error::new(
                                        e.kind(),
                                        format!(
                                            "recover: replay FunctionRegistered({} v{}) failed: {}",
                                            f.name_str(),
                                            f.version,
                                            e
                                        ),
                                    ));
                                } else if let Err(e) =
                                    seal.recover_function_record(f.name_str(), f.version, f.crc32c)
                                {
                                    function_apply_err = Some(std::io::Error::new(
                                        e.kind(),
                                        format!(
                                            "recover: seal map update for {} v{} failed: {}",
                                            f.name_str(),
                                            f.version,
                                            e
                                        ),
                                    ));
                                }
                            }
                            _ => {}
                        }
                    }
                }
                other => group.push(*other),
            })
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to visit active wal records: {}", e),
                )
            })?;
        if let Some(e) = function_apply_err {
            return Err(e);
        }

        self.transactor.recover_balances(&recover_balances);
        self.snapshot.recover_balances(&recover_balances);

        // Clamp pipeline indices to min(replayed, watermark). When the
        // watermark is u64::MAX (the unbounded default used by `recover`),
        // this is a no-op.
        let effective_last = last_tx_id.min(watermark);
        self.pipeline.set_compute_index(effective_last);
        self.pipeline.set_snapshot_index(effective_last);
        self.pipeline.set_commit_index(effective_last);
        self.pipeline
            .set_sequencer_next_id(effective_last.saturating_add(1));

        debug!(
            "Recovery completed successfully (last_tx_id={}, watermark={}).",
            effective_last, watermark
        );

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

    /// Watermark-aware variant of `locate_latest_snapshot_segment_id`
    /// (ADR-0016 §10). Returns the highest `segment.id()` with a
    /// snapshot whose `SnapshotData::last_tx_id <= watermark`. Falls
    /// back to `0` (no snapshot, replay from genesis) when none qualify.
    ///
    /// `watermark == u64::MAX` reduces to the unbounded selector and
    /// preserves the original `recover()` behaviour.
    fn locate_latest_snapshot_segment_id_for_watermark(
        &self,
        watermark: u64,
    ) -> Result<u32, std::io::Error> {
        if watermark == u64::MAX {
            return self.locate_latest_snapshot_segment_id();
        }
        let mut best: u32 = 0;
        for segment in self.segments.iter() {
            if !segment.has_snapshot() {
                continue;
            }
            // load_snapshot is cheap (small file); errors are tolerated
            // — a corrupt snapshot just gets skipped, falling back to
            // an earlier one or genesis.
            match segment.load_snapshot() {
                Ok(Some(data)) if data.last_tx_id <= watermark && segment.id() >= best => {
                    best = segment.id();
                }
                _ => {}
            }
        }
        Ok(best)
    }
}
