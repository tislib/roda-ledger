use crate::entities::WalEntry;
use crate::storage::layout::{
    active_wal_path, segment_crc_path, segment_seal_path, segment_wal_path,
};
use crate::storage::wal_reader::{read_wal_data, verify_wal_data};
use crate::storage::wal_serializer::{parse_wal_record, serialize_wal_records};
use spdlog::warn;
use std::fs::{File, OpenOptions};
use std::io::Write;
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
    data_dir: String,
    segment_id: u32,
    loaded: bool,

    // wal
    wal_file: Option<File>, // if unloaded, wal_file is None
    wal_data: Vec<u8>,      // in case of closed or sealed mode
    status: SegmentStaus,

    // active segment only
    wal_buffer: Vec<u8>,
    wal_position: usize,
    record_count: u64,
}

impl Segment {
    pub(super) fn open(data_dir: String, segment_id: u32) -> Result<Self, std::io::Error> {
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

    pub(super) fn open_active(data_dir: String, segment_id: u32) -> Result<Self, std::io::Error> {
        let data_dir_path = Path::new(&data_dir);
        let wal_file_path = active_wal_path(data_dir_path);
        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_file_path)
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to open active wal file at {:?}: {}", wal_file_path, e),
                )
            })?;

        let metadata = wal_file.metadata().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to get metadata for active wal file at {:?}: {}",
                    wal_file_path, e
                ),
            )
        })?;
        // fix me, verify wal data and find if any transaction is broken or partially written in tail

        let wal_data = if metadata.len() > 0 {
            read_wal_data(&wal_file_path).map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to read active wal data at {:?}: {}", wal_file_path, e),
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
            wal_buffer: vec![0; 16 * 1024 * 1024], // 16MB
            wal_data,
            wal_position,
            record_count: 0,
            loaded: true,
        })
    }

    pub fn load(&mut self) -> Result<(), std::io::Error> {
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
                std::io::Error::new(
                    e.kind(),
                    format!(
                        "failed to open wal file for segment {} at {:?}: {}",
                        self.segment_id, wal_file_path, e
                    ),
                )
            })?;

        let wal_data = read_wal_data(&wal_file_path).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to read wal data for segment {} at {:?}: {}",
                    self.segment_id, wal_file_path, e
                ),
            )
        })?;

        if self.status == SegmentStaus::SEALED {
            verify_wal_data(&wal_data[..], &wal_crc_file_path, self.segment_id).map_err(|e| {
                std::io::Error::new(
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

    pub(crate) fn write_entries(&mut self, entries: &[WalEntry]) {
        assert_eq!(
            self.status,
            SegmentStaus::ACTIVE,
            "Segment is not active, cannot append"
        );
        let mut file = self.wal_file.as_ref().unwrap();
        self.wal_buffer.clear();

        for entry in entries {
            self.wal_buffer
                .extend_from_slice(serialize_wal_records(entry));
        }

        loop {
            if let Err(e) = file.write_all(&self.wal_buffer) {
                warn!("Failed to write to WAL: {}", e);
                sleep(Duration::from_secs(1));
                continue;
            }
            if let Err(e) = file.sync_data() {
                warn!("Failed to fsync WAL: {}", e);
                sleep(Duration::from_secs(1));
                continue;
            }

            break;
        }

        self.wal_position += self.wal_buffer.len();
        self.record_count += entries.len() as u64;
    }
    pub(crate) fn record_count(&self) -> u64 {
        self.record_count
    }

    pub(crate) fn close(&mut self) -> Result<(), std::io::Error> {
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
            std::io::Error::new(
                e.kind(),
                format!("failed to sync wal file before closing: {}", e),
            )
        })?;

        // rename wal file to wal.bin
        let data_dir_path = Path::new(&self.data_dir);
        let active_wal_file_path = active_wal_path(data_dir_path);
        let closed_wal_file_path = segment_wal_path(data_dir_path, self.segment_id);
        std::fs::rename(&active_wal_file_path, &closed_wal_file_path).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to rename active wal file {:?} to {:?}: {}",
                    active_wal_file_path, closed_wal_file_path, e
                ),
            )
        })?;

        Ok(())
    }

    pub(crate) fn seal(&mut self) -> Result<(), std::io::Error> {
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
            std::io::Error::new(
                e.kind(),
                format!("failed to create crc file at {:?}: {}", crc_file_path, e),
            )
        })?;
        crc_file.write_all(&crc_data).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to write crc data to {:?}: {}", crc_file_path, e),
            )
        })?;
        crc_file.sync_all().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to sync crc file at {:?}: {}", crc_file_path, e),
            )
        })?;

        let seal_file_path = segment_seal_path(data_dir_path, self.segment_id);
        let seal_file = File::create(&seal_file_path).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to create seal file at {:?}: {}", seal_file_path, e),
            )
        })?;
        seal_file.sync_all().map_err(|e| {
            std::io::Error::new(
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
    ) -> Result<(), std::io::Error> {
        assert!(self.loaded, "Segment is not loaded, cannot visit");

        if !self.wal_data.len().is_multiple_of(40) {
            return Err(std::io::Error::new(
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

    pub(crate) fn current_wal_offset(&self) -> usize {
        self.wal_position
    }

    // ── Public helpers for CLI tooling (ADR-007) ─────────────────────────────

    /// Removes `.crc` and `.seal` files so the segment can be re-sealed.
    /// Call before `open()` so the segment opens as CLOSED.
    /// Removes `.crc` and `.seal` files, resetting this segment to CLOSED state
    /// so it can be re-sealed.
    pub(crate) fn force_unseal(&mut self) -> Result<(), std::io::Error> {
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
