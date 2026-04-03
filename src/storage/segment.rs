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
            .open(&wal_file_path)?;

        let metadata = wal_file.metadata()?;
        // fix me, verify wal data and find if any transaction is broken or partially written in tail

        let wal_data = if metadata.len() > 0 {
            read_wal_data(&wal_file_path)?
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
        let wal_file = OpenOptions::new().append(true).open(&wal_file_path)?;

        let wal_data = read_wal_data(&wal_file_path)?;

        if self.status == SegmentStaus::SEALED {
            verify_wal_data(&wal_data[..], &wal_crc_file_path, self.segment_id)?;
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

    pub(crate) fn status(&self) -> SegmentStaus {
        self.status
    }

    pub(crate) fn append_entries(&mut self, entries: &[WalEntry]) {
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

        let mut file = self.wal_file.as_ref().unwrap();

        self.status = SegmentStaus::CLOSED;
        file.sync_all()?;

        // rename wal file to wal.bin
        let data_dir_path = Path::new(&self.data_dir);
        let active_wal_file_path = active_wal_path(data_dir_path);
        let closed_wal_file_path = segment_wal_path(data_dir_path, self.segment_id);
        std::fs::rename(&active_wal_file_path, &closed_wal_file_path)?;

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
        let mut crc_file = File::create(&crc_file_path)?;
        crc_file.write_all(&crc_data)?;
        crc_file.sync_all()?;

        let seal_file_path = segment_seal_path(data_dir_path, self.segment_id);
        let seal_file = File::create(&seal_file_path)?;
        seal_file.sync_all()?;

        self.status = SegmentStaus::SEALED;

        Ok(())
    }

    pub(crate) fn visit_wal_records(
        &self,
        mut handler: impl FnMut(&WalEntry),
    ) -> Result<(), std::io::Error> {
        assert_eq!(self.loaded, true, "Segment is not loaded, cannot visit");

        if self.wal_data.len() % 40 != 0 {
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
}
