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
    active_wal_path, function_snapshot_bin_path, function_snapshot_crc_path, kv_snapshot_bin_path,
    kv_snapshot_crc_path, segment_account_index_path, segment_crc_path, segment_seal_path,
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
    /// `snapshot_{id}.bin`, `snapshot_{id}.crc`,
    /// `function_snapshot_{id}.bin`, `function_snapshot_{id}.crc`,
    /// `kv_snapshot_{id}.bin`, `kv_snapshot_{id}.crc`
    /// (internal.md §23.5).
    ///
    /// Used by [`storage::Storage::truncate_wal_above`] when an
    /// entire segment falls past the recovery watermark (ADR-0016 §9).
    /// Missing files are tolerated — every removal is idempotent.
    pub fn delete_all_files_for_segment(data_dir: &str, segment_id: u32) -> Result<(), Error> {
        let dir = Path::new(data_dir);
        let candidates = [
            segment_wal_path(dir, segment_id),
            segment_crc_path(dir, segment_id),
            segment_seal_path(dir, segment_id),
            segment_tx_index_path(dir, segment_id),
            segment_account_index_path(dir, segment_id),
            snapshot_bin_path(dir, segment_id),
            snapshot_crc_path(dir, segment_id),
            function_snapshot_bin_path(dir, segment_id),
            function_snapshot_crc_path(dir, segment_id),
            // KV snapshot (ADR-023) captured state past the watermark too.
            kv_snapshot_bin_path(dir, segment_id),
            kv_snapshot_crc_path(dir, segment_id),
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

    /// Removes the balance, function, and KV snapshot triples
    /// (`snapshot_{id}.{bin,crc}`, `function_snapshot_{id}.{bin,crc}`,
    /// `kv_snapshot_{id}.{bin,crc}`) for this segment id. Used after
    /// truncating a *straddling* segment — its existing snapshots
    /// captured state past the watermark and are no longer
    /// trustworthy. Missing files are tolerated.
    pub fn delete_snapshot_files_for_segment(data_dir: &str, segment_id: u32) -> Result<(), Error> {
        let dir = Path::new(data_dir);
        let candidates = [
            snapshot_bin_path(dir, segment_id),
            snapshot_crc_path(dir, segment_id),
            function_snapshot_bin_path(dir, segment_id),
            function_snapshot_crc_path(dir, segment_id),
            // KV snapshot (ADR-023) captured state past the watermark too.
            kv_snapshot_bin_path(dir, segment_id),
            kv_snapshot_crc_path(dir, segment_id),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use crate::engine::Storage;

    // A closed segment with a balance, function, and KV snapshot on disk.
    fn temp_segment_with_snapshots() -> (Segment, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = StorageConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            ..StorageConfig::default()
        };
        let storage = Storage::new(cfg).expect("storage");
        let mut seg = storage.active_segment().expect("active segment");
        seg.close().expect("close segment");
        seg.save_snapshot(0, &[], &[])
            .expect("save balance snapshot");
        seg.save_function_snapshot(&[]).expect("save fn snapshot");
        seg.save_kv_snapshot(&[], &[]).expect("save kv snapshot");
        (seg, dir)
    }

    // Both delete helpers must drop the KV snapshot pair (ADR-023) so a
    // reseed-truncated segment can't seed KV state past the watermark.
    #[test]
    fn delete_snapshot_files_removes_kv_snapshot() {
        let (seg, dir) = temp_segment_with_snapshots();
        let id = seg.id();
        assert!(seg.has_kv_snapshot());
        Segment::delete_snapshot_files_for_segment(&dir.path().to_string_lossy(), id)
            .expect("delete snapshots");
        assert!(!kv_snapshot_bin_path(dir.path(), id).exists());
        assert!(!kv_snapshot_crc_path(dir.path(), id).exists());
    }

    #[test]
    fn delete_all_files_removes_kv_snapshot() {
        let (seg, dir) = temp_segment_with_snapshots();
        let id = seg.id();
        assert!(seg.has_kv_snapshot());
        Segment::delete_all_files_for_segment(&dir.path().to_string_lossy(), id)
            .expect("delete all");
        assert!(!kv_snapshot_bin_path(dir.path(), id).exists());
        assert!(!kv_snapshot_crc_path(dir.path(), id).exists());
    }
}
