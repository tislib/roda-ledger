//! WAL truncation primitives backing ADR-0016 §9 (recovery-mode reseed)
//! and ADR-006 crash-recovery's tail-trim. All methods extend
//! [`storage::Segment`] via an `impl Segment` block in this
//! file — the data structure is defined in `segment.rs`; truncation
//! lives here so the boot-time-only WAL-rewind surface is contained.
//!
//! Visibility note: this module reaches into `Segment`'s `pub`
//! fields directly (`wal_data`, `wal_position`, `status`, `loaded`,
//! `data_dir`, `segment_id`). That stays inside the `storage` module.

use crate::layout::{
    active_wal_path, segment_account_index_path, segment_crc_path, segment_seal_path,
    segment_tx_index_path, segment_wal_path, snapshot_bin_path, snapshot_crc_path,
};
use crate::wal_reader::read_wal_data;
use crate::{Segment, SegmentStaus};
use std::fs::OpenOptions;
use std::io::Error;
use std::path::Path;

impl Segment {
    /// Truncates this segment's WAL file to `new_len` bytes.
    ///
    /// Picks the appropriate on-disk path based on the segment's
    /// status:
    /// - `ACTIVE` → `wal.bin` (the path used by ADR-006 crash-recovery
    ///   tail-trim, the only pre-ADR-0016 caller).
    /// - `CLOSED` → `wal_{id:06}.bin`.
    ///
    /// Sealed segments are immutable — see ADR-0016 §10's
    /// `seal_watermark` gate, which guarantees a sealed segment's
    /// content is always at or below any future recovery watermark.
    /// This method panics if asked to truncate a sealed segment;
    /// `Storage::truncate_wal_above` returns `InvalidData` before
    /// reaching this point.
    pub fn truncate_wal(&mut self, new_len: u64) -> Result<(), Error> {
        assert_ne!(
            self.status,
            SegmentStaus::SEALED,
            "Cannot truncate a sealed segment — sealed segments are immutable per ADR-0016 §10",
        );

        let data_dir_path = Path::new(&self.data_dir);
        let wal_file_path = if self.status == SegmentStaus::ACTIVE {
            active_wal_path(data_dir_path)
        } else {
            segment_wal_path(data_dir_path, self.segment_id)
        };
        let file = OpenOptions::new().write(true).open(&wal_file_path)?;
        file.set_len(new_len)?;
        file.sync_all()?;

        // Re-read the truncated data so visit_wal_records sees the correct state.
        if new_len > 0 {
            self.wal_data = read_wal_data(&wal_file_path)?;
        } else {
            self.wal_data.clear();
        }
        self.wal_position = new_len as usize;
        Ok(())
    }

    /// Removes every on-disk artifact for this segment id:
    /// `wal_{id}.bin`, `wal_{id}.crc`, `wal_{id}.seal`,
    /// `wal_index_{id}.bin`, `account_index_{id}.bin`,
    /// `snapshot_{id}.bin`, `snapshot_{id}.crc`. Also clears any
    /// `function_snapshot_{id}.{bin,crc}` paired to this segment.
    ///
    /// Used by [`storage::Storage::truncate_wal_above`] when an
    /// entire segment falls past the recovery watermark (ADR-0016 §9).
    /// Missing files are tolerated — every removal is idempotent.
    pub fn delete_all_files_for_segment(
        data_dir: &str,
        segment_id: u32,
    ) -> Result<(), Error> {
        let dir = Path::new(data_dir);
        let candidates = [
            segment_wal_path(dir, segment_id),
            segment_crc_path(dir, segment_id),
            segment_seal_path(dir, segment_id),
            segment_tx_index_path(dir, segment_id),
            segment_account_index_path(dir, segment_id),
            snapshot_bin_path(dir, segment_id),
            snapshot_crc_path(dir, segment_id),
            dir.join(format!("function_snapshot_{:06}.bin", segment_id)),
            dir.join(format!("function_snapshot_{:06}.crc", segment_id)),
        ];
        for path in &candidates {
            if path.exists() {
                std::fs::remove_file(path).map_err(|e| {
                    Error::new(e.kind(), format!("failed to remove {:?}: {}", path, e))
                })?;
            }
        }
        Ok(())
    }

    /// Removes only the snapshot pair (`snapshot_{id}.bin` +
    /// `snapshot_{id}.crc`, plus any `function_snapshot_{id}.*`)
    /// for this segment id. Used after truncating a *straddling*
    /// segment — its existing balance snapshot captured state past the
    /// watermark and is no longer trustworthy. Missing files are
    /// tolerated.
    pub fn delete_snapshot_files_for_segment(
        data_dir: &str,
        segment_id: u32,
    ) -> Result<(), Error> {
        let dir = Path::new(data_dir);
        let candidates = [
            snapshot_bin_path(dir, segment_id),
            snapshot_crc_path(dir, segment_id),
            dir.join(format!("function_snapshot_{:06}.bin", segment_id)),
            dir.join(format!("function_snapshot_{:06}.crc", segment_id)),
        ];
        for path in &candidates {
            if path.exists() {
                std::fs::remove_file(path).map_err(|e| {
                    Error::new(e.kind(), format!("failed to remove {:?}: {}", path, e))
                })?;
            }
        }
        Ok(())
    }
}
