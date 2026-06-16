use crate::balance::Balance;
use spdlog::{debug, warn};
use std::collections::HashMap;
use storage::entities::{
    CommittedTransaction, FailReason, FunctionRegistered, TxMetadata, WalEntry, WalEntryKind,
};
use storage::{KeyPath, Segment, Storage, Value};

const ENTRY_SIZE: usize = 40;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoverAccount {
    pub balance: Balance,
    pub flags: u64,
}

pub struct ActiveSnapshot {
    pub last_tx_id: u64,
    pub next_account_id: u64,
    pub accounts: HashMap<u64, RecoverAccount>,
    pub functions: Vec<FunctionRegistered>,
    pub links: Vec<(u64, u16, u64)>,
    pub kv: HashMap<KeyPath, Value>,
    /// Interned constants (ADR-023): `key → null-terminated value`, checkpointed
    /// in the same KV snapshot file as `kv`.
    pub constants: HashMap<u32, [u8; 32]>,
    pub last_segment_user_ref_tx_id_map: HashMap<u64, u64>,
    pub active_segment_user_ref_tx_id_map: HashMap<u64, u64>,
    pub active_segment_transactions: Vec<CommittedTransaction>,
}

impl ActiveSnapshot {
    /// Genesis seed: no accounts/functions/links, allocator at 1 (account 0 is
    /// SYSTEM). Used as the recovery starting point and by isolated load tests.
    pub fn empty() -> Self {
        Self {
            last_tx_id: 0,
            next_account_id: 1,
            accounts: HashMap::new(),
            functions: Vec::new(),
            links: Vec::new(),
            kv: HashMap::new(),
            constants: HashMap::new(),
            last_segment_user_ref_tx_id_map: HashMap::new(),
            active_segment_user_ref_tx_id_map: HashMap::new(),
            active_segment_transactions: Vec::new(),
        }
    }
}

pub struct Recover<'r> {
    storage: &'r Storage,
    segments: Vec<Segment>,
}

impl<'r> Recover<'r> {
    pub fn new(storage: &'r Storage) -> Self {
        Self {
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
                || kind == WalEntryKind::AccountOpened as u8
                || kind == WalEntryKind::AccountLinked as u8
                || kind == WalEntryKind::AccountFlagsUpdated as u8
                || kind == WalEntryKind::Kv as u8
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
                || kind == WalEntryKind::AccountOpened as u8
                || kind == WalEntryKind::AccountLinked as u8
                || kind == WalEntryKind::AccountFlagsUpdated as u8
                || kind == WalEntryKind::Kv as u8
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

    /// Build the `ActiveSnapshot` from the latest snapshot (`last_tx_id <=
    /// watermark_tx_id`) plus the post-snapshot and active WAL. Records whose
    /// `tx_id > watermark_tx_id` are visited but skipped.
    pub fn recover_until(
        &mut self,
        watermark_tx_id: u64,
    ) -> Result<ActiveSnapshot, std::io::Error> {
        debug!("Starting recovery (watermark_tx_id={})...", watermark_tx_id);

        self.segments = self.storage.list_all_segments().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to list segments during recovery: {}", e),
            )
        })?;

        let latest_snapshot_segment_id = self
            .locate_latest_snapshot_segment_id_for_watermark(watermark_tx_id)
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to locate latest snapshot segment: {}", e),
                )
            })?;

        let mut snapshot = ActiveSnapshot::empty();

        // Followers precede their closing metadata and may span a segment seam,
        // so the buffer carries across segments.
        let mut group: Vec<WalEntry> = Vec::new();
        // Reset per closed segment; ends holding the last closed segment's map.
        let mut closed_user_refs: HashMap<u64, u64> = HashMap::new();

        for segment in self.segments.iter_mut() {
            if segment.id() < latest_snapshot_segment_id {
                continue;
            }
            // The snapshot (balances/accounts/functions/KV) covers this segment and
            // everything before it; only post-snapshot segments are replayed.
            if segment.id() == latest_snapshot_segment_id {
                Self::restore_snapshot(segment, &mut snapshot)?;
                continue;
            }
            segment.load().map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to load segment {}: {}", segment.id(), e),
                )
            })?;
            closed_user_refs.clear();
            Self::fold_segment(
                segment,
                watermark_tx_id,
                &mut group,
                &mut snapshot,
                &mut closed_user_refs,
                None,
            )?;
        }
        snapshot.last_segment_user_ref_tx_id_map = closed_user_refs;

        let active_segment = self.storage.active_segment().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to get active segment: {}", e))
        })?;
        let mut active_user_refs: HashMap<u64, u64> = HashMap::new();
        let mut active_txs: Vec<CommittedTransaction> = Vec::new();
        Self::fold_segment(
            &active_segment,
            watermark_tx_id,
            &mut group,
            &mut snapshot,
            &mut active_user_refs,
            Some(&mut active_txs),
        )?;
        snapshot.active_segment_user_ref_tx_id_map = active_user_refs;
        snapshot.active_segment_transactions = active_txs;

        // Keep the allocator high-water ahead of every recovered account id.
        let recovered_high_water = snapshot
            .accounts
            .keys()
            .copied()
            .max()
            .map(|id| id.saturating_add(1))
            .unwrap_or(0);
        snapshot.next_account_id = snapshot.next_account_id.max(recovered_high_water);

        debug!(
            "Recovery completed (last_tx_id={}, watermark_tx_id={}).",
            snapshot.last_tx_id, watermark_tx_id
        );

        Ok(snapshot)
    }

    /// Seed accounts and links from a segment's snapshot file. No-op when the
    /// segment has no (or a corrupt) snapshot.
    fn restore_snapshot(
        segment: &Segment,
        snapshot: &mut ActiveSnapshot,
    ) -> Result<(), std::io::Error> {
        let data = match segment.load_snapshot().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to load snapshot for segment {}: {}",
                    segment.id(),
                    e
                ),
            )
        })? {
            Some(data) => data,
            None => return Ok(()),
        };

        for (account_id, balance, flags) in data.accounts {
            let account = snapshot.accounts.entry(account_id).or_default();
            account.balance = balance;
            account.flags = flags;
        }
        for (parent_id, type_id, child_id) in data.links {
            snapshot.links.push((parent_id, type_id, child_id));
        }
        snapshot.last_tx_id = data.last_tx_id;
        snapshot.next_account_id = snapshot.next_account_id.max(data.next_account_id);

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
                snapshot
                    .functions
                    .push(FunctionRegistered::new(&name, version, crc));
            }
        }

        // KV snapshot (ADR-023): load the checkpointed KV state for this segment.
        if let Some(kvdata) = segment.load_kv_snapshot().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to load kv snapshot for segment {}: {}",
                    segment.id(),
                    e
                ),
            )
        })? {
            for (key, value) in kvdata.entries {
                let entry = storage::entities::KvEntry::new(key, value);
                if let Ok((kp, Some(v))) = entry.decode() {
                    snapshot.kv.insert(kp, v);
                }
            }
            for (key, value) in kvdata.constants {
                snapshot.constants.insert(key, value);
            }
        }
        Ok(())
    }

    /// Fold a segment's committed transactions (`tx_id <= watermark_tx_id`) into
    /// the snapshot's accounts/links and record each `user_ref -> tx_id` in
    /// `user_ref_map` (dedup set: skips `user_ref == 0` and DUPLICATE txs).
    fn fold_segment(
        segment: &Segment,
        watermark_tx_id: u64,
        group: &mut Vec<WalEntry>,
        snapshot: &mut ActiveSnapshot,
        user_ref_map: &mut HashMap<u64, u64>,
        mut transactions: Option<&mut Vec<CommittedTransaction>>,
    ) -> Result<(), std::io::Error> {
        segment
            .visit_wal_records(|record| match record {
                WalEntry::Metadata(metadata) => {
                    if metadata.tx_id > watermark_tx_id {
                        group.clear();
                        return;
                    }
                    snapshot.last_tx_id = metadata.tx_id;
                    // Only committed, non-duplicate user transactions feed dedup.
                    if metadata.user_ref != 0 && metadata.fail_reason != FailReason::DUPLICATE {
                        user_ref_map.insert(metadata.user_ref, metadata.tx_id);
                    }
                    // Retain the committed tx so the snapshot can warm its index.
                    if let Some(txs) = transactions.as_mut() {
                        txs.push(CommittedTransaction {
                            meta: *metadata,
                            entries: group.clone(),
                        });
                    }
                    for follower in group.drain(..) {
                        match follower {
                            // KV (ADR-023): apply to the folded map (post-snapshot tail).
                            WalEntry::Kv(kv) => {
                                if let Ok((key, value)) = kv.decode() {
                                    match value {
                                        Some(v) => {
                                            snapshot.kv.insert(key, v);
                                        }
                                        None => {
                                            snapshot.kv.remove(&key);
                                        }
                                    }
                                }
                            }
                            WalEntry::KvConstant(c) => {
                                snapshot.constants.insert(c.key, c.value);
                            }
                            WalEntry::Entry(entry) => {
                                snapshot
                                    .accounts
                                    .entry(entry.account_id)
                                    .or_default()
                                    .balance = entry.computed_balance;
                            }
                            WalEntry::AccountOpened(a) => {
                                let end = a.begin_account_id + a.count as u64;
                                snapshot.next_account_id = snapshot.next_account_id.max(end);
                                for id in a.begin_account_id..end {
                                    snapshot.accounts.entry(id).or_default().flags = a.flags;
                                }
                            }
                            WalEntry::AccountLinked(a) => {
                                snapshot.links.push((a.parent_id, a.type_id, a.child_id));
                            }
                            WalEntry::AccountFlagsUpdated(a) => {
                                snapshot.accounts.entry(a.account_id).or_default().flags =
                                    a.new_flags;
                            }
                            WalEntry::FunctionRegistered(f) => {
                                snapshot.functions.push(f);
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
            })
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

#[cfg(test)]
mod validator_tests {
    use super::*;
    use storage::entities::{AccountOpened, EntryKind, FailReason, TxEntry};

    fn follower_bytes(f: &WalEntry) -> Vec<u8> {
        match f {
            WalEntry::AccountOpened(a) => bytemuck::bytes_of(a).to_vec(),
            WalEntry::Entry(e) => bytemuck::bytes_of(e).to_vec(),
            _ => unreachable!("test only emits AccountOpened/Entry followers"),
        }
    }

    /// On-disk bytes for one committed tx: followers then the closing
    /// `TxMetadata`, carrying the trailer CRC the validator recomputes.
    fn tx_bytes(tx_id: u64, followers: &[WalEntry]) -> Vec<u8> {
        let mut body = Vec::new();
        let mut digest = 0u32;
        for f in followers {
            let bytes = follower_bytes(f);
            digest = crc32c::crc32c_append(digest, &bytes);
            body.extend_from_slice(&bytes);
        }
        let mut meta = TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            fail_reason: FailReason::NONE,
            sub_item_count: followers.len() as u16,
            crc32c: 0,
            tx_id,
            timestamp: 0,
            user_ref: 0,
            tag: [0; 8],
        };
        digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&meta));
        meta.crc32c = digest;
        body.extend_from_slice(bytemuck::bytes_of(&meta));
        body
    }

    fn entry(account_id: u64, amount: u64, kind: u8, computed_balance: i64) -> WalEntry {
        WalEntry::Entry(TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind,
            _pad0: [0; 6],
            _pad1: [0; 8],
            account_id,
            amount,
            computed_balance,
        })
    }

    // Regression: the crash-scan path must treat AccountOpened (kind 7) as a
    // valid follower, not "unexpected record kind" → non-recoverable.
    #[test]
    fn validate_accepts_open_account_tx() {
        let buf = tx_bytes(
            1,
            &[WalEntry::AccountOpened(AccountOpened::new(1, 5, 0, 0))],
        );
        let n = Recover::validate_wal_transactions(&buf).expect("OpenAccount tx must validate");
        assert_eq!(n, buf.len());
    }

    #[test]
    fn has_valid_tx_after_sees_open_account_tx() {
        let buf = tx_bytes(
            7,
            &[WalEntry::AccountOpened(AccountOpened::new(1, 5, 0, 0))],
        );
        assert!(Recover::has_valid_tx_after(&buf, 0));
    }

    // An OpenAccount tx mid-stream (followed by a deposit) must not break the
    // scan — exercises the follower-buffer reset across an AccountOpened group.
    #[test]
    fn validate_accepts_open_account_then_deposit() {
        let mut buf = tx_bytes(
            1,
            &[WalEntry::AccountOpened(AccountOpened::new(1, 5, 0, 0))],
        );
        buf.extend_from_slice(&tx_bytes(
            2,
            &[
                entry(0, 100, EntryKind::CREDIT, -100),
                entry(1, 100, EntryKind::DEBIT, 100),
            ],
        ));
        let n = Recover::validate_wal_transactions(&buf).expect("both txs valid");
        assert_eq!(n, buf.len());
    }
}
