use crate::entities::WalEntry;
use crate::storage::layout::{
    active_wal_path, segment_crc_path, segment_seal_path, segment_wal_path, wal_stop_path,
};
use crate::storage::syncer::Syncer;
use crate::storage::wal_reader::{read_wal_data, verify_wal_data};
use crate::storage::wal_serializer::{parse_wal_record, serialize_wal_records};
use spdlog::warn;
use std::fs::{File, OpenOptions};
use std::io::{Error, Write};
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

pub const WAL_MAGIC: u32 = 0x524F4441; // "RODA"
pub const WAL_VERSION: u8 = 1;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SegmentStaus {
    ACTIVE,
    CLOSED,
    SEALED,
}

pub struct Segment {
    // init parameters
    pub(super) data_dir: String,
    pub(super) segment_id: u32,
    pub(super) loaded: bool,

    // wal
    wal_file: Option<File>,       // if unloaded, wal_file is None
    pub(super) wal_data: Vec<u8>, // in case of closed or sealed mode
    pub(super) status: SegmentStaus,

    // active segment only
    wal_buffer: Vec<u8>,
    pub(super) wal_position: usize,
    record_count: u64,
}

impl Segment {
    pub(super) fn open(data_dir: String, segment_id: u32) -> Result<Self, Error> {
        let data_dir_path = Path::new(&data_dir);
        let wal_sealed_file_path = segment_seal_path(data_dir_path, segment_id);
        let status = if wal_sealed_file_path.exists() {
            SegmentStaus::SEALED
        } else {
            SegmentStaus::CLOSED
        };

        Ok(Self {
            data_dir,
            segment_id,
            wal_file: None,
            wal_buffer: vec![],
            wal_data: vec![],
            status,
            loaded: false,
            wal_position: 0,
            record_count: 0,
        })
    }

    pub(super) fn open_active(data_dir: String, segment_id: u32) -> Result<Self, Error> {
        let data_dir_path = Path::new(&data_dir);
        let wal_file_path = active_wal_path(data_dir_path);
        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_file_path)
            .map_err(|e| {
                Error::new(
                    e.kind(),
                    format!(
                        "failed to open active wal file at {:?}: {}",
                        wal_file_path, e
                    ),
                )
            })?;

        let metadata = wal_file.metadata().map_err(|e| {
            Error::new(
                e.kind(),
                format!(
                    "failed to get metadata for active wal file at {:?}: {}",
                    wal_file_path, e
                ),
            )
        })?;

        let wal_data = if metadata.len() > 0 {
            read_wal_data(&wal_file_path).map_err(|e| {
                Error::new(
                    e.kind(),
                    format!(
                        "failed to read active wal data at {:?}: {}",
                        wal_file_path, e
                    ),
                )
            })?
        } else {
            vec![]
        };

        let wal_position = metadata.len() as usize;

        Ok(Self {
            data_dir,
            segment_id,
            wal_file: Some(wal_file),
            status: SegmentStaus::ACTIVE,
            wal_buffer: Vec::with_capacity(16 * 1024 * 1024),
            wal_data,
            wal_position,
            record_count: 0,
            loaded: true,
        })
    }

    pub fn load(&mut self) -> Result<(), Error> {
        if self.loaded {
            return Ok(());
        }
        let data_dir_path = Path::new(&self.data_dir);
        let wal_file_path = segment_wal_path(data_dir_path, self.segment_id);
        let wal_crc_file_path = segment_crc_path(data_dir_path, self.segment_id);
        let wal_file = OpenOptions::new()
            .append(true)
            .open(&wal_file_path)
            .map_err(|e| {
                Error::new(
                    e.kind(),
                    format!(
                        "failed to open wal file for segment {} at {:?}: {}",
                        self.segment_id, wal_file_path, e
                    ),
                )
            })?;

        let wal_data = read_wal_data(&wal_file_path).map_err(|e| {
            Error::new(
                e.kind(),
                format!(
                    "failed to read wal data for segment {} at {:?}: {}",
                    self.segment_id, wal_file_path, e
                ),
            )
        })?;

        if self.status == SegmentStaus::SEALED {
            verify_wal_data(&wal_data[..], &wal_crc_file_path, self.segment_id).map_err(|e| {
                Error::new(
                    e.kind(),
                    format!(
                        "failed to verify wal data for segment {} at {:?}: {}",
                        self.segment_id, wal_file_path, e
                    ),
                )
            })?;
        }

        self.wal_file = Some(wal_file);
        self.wal_data = wal_data;
        self.loaded = true;

        Ok(())
    }

    pub(crate) fn id(&self) -> u32 {
        self.segment_id
    }

    pub(super) fn data_dir(&self) -> &str {
        &self.data_dir
    }

    pub(super) fn wal_data(&self) -> &[u8] {
        &self.wal_data
    }

    pub(crate) fn status(&self) -> SegmentStaus {
        self.status
    }

    pub(crate) fn append_pending_entry(&mut self, entry: &WalEntry) {
        assert_eq!(
            self.status,
            SegmentStaus::ACTIVE,
            "Segment is not active, cannot append"
        );

        self.wal_buffer
            .extend_from_slice(serialize_wal_records(entry));
        self.record_count += 1;
    }

    pub(crate) fn write_pending_entries(&mut self) {
        assert_eq!(
            self.status,
            SegmentStaus::ACTIVE,
            "Segment is not active, cannot append"
        );

        let mut file = self.wal_file.as_ref().unwrap();

        loop {
            if let Err(e) = file.write_all(&self.wal_buffer) {
                warn!("Failed to write to WAL: {}", e);
                sleep(Duration::from_secs(1));
                continue;
            }
            break;
        }

        self.wal_position += self.wal_buffer.len();
        self.wal_buffer.clear();
    }

    pub(crate) fn write_entries(&mut self, entries: &[WalEntry]) {
        assert_eq!(
            self.status,
            SegmentStaus::ACTIVE,
            "Segment is not active, cannot append"
        );

        for entry in entries {
            self.wal_buffer
                .extend_from_slice(serialize_wal_records(entry));
        }
        self.record_count += entries.len() as u64;

        self.write_pending_entries();
    }

    pub(crate) fn syncer(&self) -> Result<Syncer, Error> {
        if let Some(wal_file) = &self.wal_file {
            let wal_file_clone = wal_file
                .try_clone()
                .map_err(|e| Error::other(format!("Failed to clone WAL file: {}", e)))?;

            Ok(Syncer::new(wal_file_clone))
        } else {
            Err(Error::other("Segment is not loaded, cannot sync"))
        }
    }

    pub(crate) fn record_count(&self) -> u64 {
        self.record_count
    }

    pub(crate) fn close(&mut self) -> Result<(), Error> {
        assert_eq!(
            self.status,
            SegmentStaus::ACTIVE,
            "Segment is not active, cannot close"
        );

        let file = self
            .wal_file
            .as_ref()
            .expect("active segment should have wal_file");

        self.status = SegmentStaus::CLOSED;
        file.sync_all().map_err(|e| {
            Error::new(
                e.kind(),
                format!("failed to sync wal file before closing: {}", e),
            )
        })?;

        // rename wal file to wal.bin
        let data_dir_path = Path::new(&self.data_dir);
        let active_wal_file_path = active_wal_path(data_dir_path);
        let closed_wal_file_path = segment_wal_path(data_dir_path, self.segment_id);
        std::fs::rename(&active_wal_file_path, &closed_wal_file_path).map_err(|e| {
            Error::new(
                e.kind(),
                format!(
                    "failed to rename active wal file {:?} to {:?}: {}",
                    active_wal_file_path, closed_wal_file_path, e
                ),
            )
        })?;

        Ok(())
    }

    pub(crate) fn seal(&mut self) -> Result<(), Error> {
        assert_eq!(
            self.status,
            SegmentStaus::CLOSED,
            "Segment is not closed, cannot seal"
        );

        // calculate checksum
        let crc32 = crc32c::crc32c(&self.wal_data);
        let size = self.wal_data.len() as u64;

        // build 16-byte sidecar: [crc32 LE 4B][size LE 8B][magic LE 4B]
        let mut crc_data = [0u8; 16];
        crc_data[0..4].copy_from_slice(&crc32.to_le_bytes());
        crc_data[4..12].copy_from_slice(&size.to_le_bytes());
        crc_data[12..16].copy_from_slice(&WAL_MAGIC.to_le_bytes());

        let data_dir_path = Path::new(&self.data_dir);
        let crc_file_path = segment_crc_path(data_dir_path, self.segment_id);
        let mut crc_file = File::create(&crc_file_path).map_err(|e| {
            Error::new(
                e.kind(),
                format!("failed to create crc file at {:?}: {}", crc_file_path, e),
            )
        })?;
        crc_file.write_all(&crc_data).map_err(|e| {
            Error::new(
                e.kind(),
                format!("failed to write crc data to {:?}: {}", crc_file_path, e),
            )
        })?;
        crc_file.sync_all().map_err(|e| {
            Error::new(
                e.kind(),
                format!("failed to sync crc file at {:?}: {}", crc_file_path, e),
            )
        })?;

        let seal_file_path = segment_seal_path(data_dir_path, self.segment_id);
        let seal_file = File::create(&seal_file_path).map_err(|e| {
            Error::new(
                e.kind(),
                format!("failed to create seal file at {:?}: {}", seal_file_path, e),
            )
        })?;
        seal_file.sync_all().map_err(|e| {
            Error::new(
                e.kind(),
                format!("failed to sync seal file at {:?}: {}", seal_file_path, e),
            )
        })?;

        self.status = SegmentStaus::SEALED;

        Ok(())
    }

    pub(crate) fn visit_wal_records(
        &self,
        mut handler: impl FnMut(&WalEntry),
    ) -> Result<(), Error> {
        assert!(self.loaded, "Segment is not loaded, cannot visit");

        if !self.wal_data.len().is_multiple_of(40) {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL data is corrupted, cannot read records (is not aligned to 40 bytes)",
            ));
        }

        let mut offset = 0;
        while offset < self.wal_data.len() {
            let entry = parse_wal_record(&self.wal_data[offset..])?;
            handler(&entry);
            offset += 40;
        }

        Ok(())
    }

    /// Lowest `tx_id` present in this segment's WAL records, or `0`
    /// if the segment contains no transactional records.
    ///
    /// Walks **forwards** record-by-record and returns the first
    /// non-zero `tx_id` encountered. Typically O(1) — the very first
    /// record after `SegmentHeader` is a transaction's `Metadata`.
    /// Used by `Storage::truncate_wal_above` to distinguish "segment
    /// fully past watermark" from "segment straddles watermark"
    /// (ADR-0016 §9).
    pub(crate) fn first_tx_id_in_wal_data(&self) -> Result<u64, Error> {
        assert!(
            self.loaded,
            "Segment is not loaded, cannot inspect first tx_id",
        );
        if !self.wal_data.len().is_multiple_of(40) {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL data is corrupted, cannot read records (is not aligned to 40 bytes)",
            ));
        }
        let mut offset = 0usize;
        while offset + 40 <= self.wal_data.len() {
            let entry = parse_wal_record(&self.wal_data[offset..offset + 40])?;
            let tx = entry.tx_id();
            if tx > 0 {
                return Ok(tx);
            }
            offset += 40;
        }
        Ok(0)
    }

    /// Highest `tx_id` present in this segment's WAL records, or `0`
    /// if the segment contains no transactional records (an empty
    /// active, or a segment with only `SegmentHeader` /
    /// `SegmentSealed` / `FunctionRegistered`).
    ///
    /// Walks **backwards** record-by-record (each record is 40 bytes)
    /// and returns the first non-zero `tx_id` encountered. O(1) in
    /// the typical case where the segment ends with an `Entry` /
    /// `Link` of the most recent transaction. Used by the seal stage's
    /// cluster gate (ADR-0016 §10) to decide whether a CLOSED segment
    /// is yet eligible for sealing without paying the cost of a full
    /// forward scan.
    pub(crate) fn last_tx_id_in_wal_data(&self) -> Result<u64, Error> {
        assert!(
            self.loaded,
            "Segment is not loaded, cannot inspect last tx_id",
        );
        if !self.wal_data.len().is_multiple_of(40) {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL data is corrupted, cannot read records (is not aligned to 40 bytes)",
            ));
        }
        let mut offset = self.wal_data.len();
        while offset >= 40 {
            offset -= 40;
            let entry = parse_wal_record(&self.wal_data[offset..offset + 40])?;
            let tx = entry.tx_id();
            if tx > 0 {
                return Ok(tx);
            }
        }
        Ok(0)
    }

    /// Locate the byte offset of the `Metadata` record carrying a
    /// specific `tx_id` in this segment's WAL data, or `None` if no
    /// such record is present.
    ///
    /// Forward scan, record-by-record. The returned offset points at
    /// the start of the `Metadata`; the caller can read it back with
    /// `parse_wal_record` to inspect (e.g. to verify the term that
    /// covered `prev_tx_id` per ADR-0016 §8), find its followers
    /// immediately after, or treat the offset as the truncation point
    /// that drops this transaction and everything after it.
    ///
    /// This is an **exact-match** probe. For "first tx beyond a
    /// threshold" semantics (the truncation use case) use
    /// [`Self::locate_tx_watermark`] instead — that handles the case
    /// where every tx in the segment is already above the threshold,
    /// which exact-match cannot.
    #[allow(dead_code)] // first user lands in Phase 2b (prev_tx_id check)
    pub(crate) fn locate_tx_position(&self, target_tx_id: u64) -> Result<Option<usize>, Error> {
        assert!(
            self.loaded,
            "Segment is not loaded, cannot locate tx position",
        );
        if !self.wal_data.len().is_multiple_of(40) {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL data is corrupted, cannot read records (is not aligned to 40 bytes)",
            ));
        }
        let mut offset = 0usize;
        while offset + 40 <= self.wal_data.len() {
            let entry = parse_wal_record(&self.wal_data[offset..offset + 40])?;
            if let WalEntry::Metadata(meta) = &entry
                && meta.tx_id == target_tx_id
            {
                return Ok(Some(offset));
            }
            offset += 40;
        }
        Ok(None)
    }

    /// Locate the byte offset of the **first** `Metadata` record
    /// whose `tx_id > watermark`, or `None` if every `Metadata` in
    /// this segment is already at or below the watermark.
    ///
    /// This is the truncation primitive (ADR-0016 §9): slicing the
    /// WAL at the returned offset preserves all earlier complete
    /// transactions intact (their followers live between the previous
    /// metadata and this one). The caller treats `None` as "segment
    /// fully ≤ watermark — nothing to truncate".
    ///
    /// Distinct from [`Self::locate_tx_position`]: that takes an
    /// exact `tx_id` and cannot represent "this segment is entirely
    /// above the threshold" because the specific `watermark + 1` that
    /// the caller would probe with may not exist (segments can start
    /// well above the watermark when the diverged tail spans the
    /// whole segment).
    ///
    /// Non-transactional records (`SegmentHeader`, `SegmentSealed`,
    /// `FunctionRegistered`) all carry `tx_id = 0` per
    /// `WalEntry::tx_id`, so they never trigger truncation on their
    /// own — they are kept alongside the transactions that bound them.
    pub(crate) fn locate_tx_watermark(&self, watermark: u64) -> Result<Option<usize>, Error> {
        assert!(
            self.loaded,
            "Segment is not loaded, cannot locate tx watermark",
        );
        if !self.wal_data.len().is_multiple_of(40) {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL data is corrupted, cannot read records (is not aligned to 40 bytes)",
            ));
        }
        let mut offset = 0usize;
        while offset + 40 <= self.wal_data.len() {
            let entry = parse_wal_record(&self.wal_data[offset..offset + 40])?;
            if let WalEntry::Metadata(meta) = &entry
                && meta.tx_id > watermark
            {
                return Ok(Some(offset));
            }
            offset += 40;
        }
        Ok(None)
    }

    pub(crate) fn current_wal_offset(&self) -> usize {
        self.wal_position
    }

    pub(crate) fn wal_data_len(&self) -> usize {
        self.wal_data.len()
    }

    /// Returns a copy of the raw WAL data for external processing.
    pub(crate) fn wal_data_copy(&self) -> Vec<u8> {
        self.wal_data.clone()
    }

    // ── wal.stop marker (physical abstraction) ──────────────────────────────

    /// Returns `true` if the `wal.stop` marker exists in the data directory.
    pub(crate) fn has_wal_stop(data_dir: &str) -> bool {
        wal_stop_path(Path::new(data_dir)).exists()
    }

    /// Returns `true` if the active WAL file (`wal.bin`) exists in the data directory.
    pub(crate) fn has_active_wal(data_dir: &str) -> bool {
        active_wal_path(Path::new(data_dir)).exists()
    }

    /// Creates the `wal.stop` marker to signal a clean shutdown.
    /// Silently succeeds if the data directory no longer exists.
    pub(crate) fn create_wal_stop(data_dir: &str) -> Result<(), Error> {
        let dir = Path::new(data_dir);
        if !dir.exists() {
            return Ok(());
        }
        let path = wal_stop_path(dir);
        File::create(&path)?.sync_all()?;
        Ok(())
    }

    /// Removes the `wal.stop` marker.
    pub(crate) fn delete_wal_stop(data_dir: &str) -> Result<(), Error> {
        let path = wal_stop_path(Path::new(data_dir));
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    // ── Public helpers for CLI tooling (ADR-007) ─────────────────────────────

    /// Removes `.crc` and `.seal` files so the segment can be re-sealed.
    /// Call before `open()` so the segment opens as CLOSED.
    /// Removes `.crc` and `.seal` files, resetting this segment to CLOSED state
    /// so it can be re-sealed.
    pub(crate) fn force_unseal(&mut self) -> Result<(), Error> {
        let dir = Path::new(&self.data_dir);
        let crc = segment_crc_path(dir, self.segment_id);
        let seal = segment_seal_path(dir, self.segment_id);
        if crc.exists() {
            std::fs::remove_file(&crc)?;
        }
        if seal.exists() {
            std::fs::remove_file(&seal)?;
        }
        self.status = SegmentStaus::CLOSED;
        Ok(())
    }
}
