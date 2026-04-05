use crate::storage::layout::{
    snapshot_bin_path, snapshot_bin_tmp_path, snapshot_crc_path, snapshot_crc_tmp_path,
};
use crate::storage::segment::Segment;
use spdlog::warn;
use std::io::Write;
use std::path::Path;

pub struct SnapshotData {
    pub segment_id: u32,
    pub last_tx_id: u64,
    pub balances: Vec<(u64, i64)>,
}

const SNAPSHOT_MAGIC: u32 = 0x534E4150; // "SNAP"
const SNAPSHOT_VERSION: u8 = 1;
const SNAPSHOT_HEADER_SIZE: usize = 36;

impl Segment {
    /// Writes a compressed snapshot for this segment to disk.
    /// Uses temp files + atomic rename for crash safety.
    /// Format (ADR-006): magic(4) + version(1) + segment_id(4) + checkpoint_id(8)
    ///                   + account_count(8) + compressed(1) + pad(6) + data_crc32c(4)
    ///                   + lz4_body
    pub fn save_snapshot(&self, records: &[(u64, i64)]) -> std::io::Result<()> {
        let segment_id = self.id();
        let data_dir = Path::new(self.data_dir());
        let bin_tmp = snapshot_bin_tmp_path(data_dir, segment_id);
        let crc_tmp = snapshot_crc_tmp_path(data_dir, segment_id);
        let bin_final = snapshot_bin_path(data_dir, segment_id);
        let crc_final = snapshot_crc_path(data_dir, segment_id);

        let account_count = records.len() as u64;
        let checkpoint_id = segment_id as u64;

        // 1. Serialize records to raw bytes (account_id: u64 LE + balance: i64 LE)
        let mut raw_data: Vec<u8> = Vec::with_capacity(records.len() * 16);
        for (account_id, balance) in records {
            raw_data.extend_from_slice(&account_id.to_le_bytes());
            raw_data.extend_from_slice(&balance.to_le_bytes());
        }

        // 2. Compute data CRC over raw (uncompressed) data
        let data_crc32c = crc32c::crc32c(&raw_data);

        // 3. LZ4 compress (always mandatory)
        let body_data = lz4_flex::compress_prepend_size(&raw_data);

        // 4. Build 36-byte header
        let mut header = Vec::with_capacity(SNAPSHOT_HEADER_SIZE);
        header.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes()); // 4
        header.push(SNAPSHOT_VERSION); // 1
        header.extend_from_slice(&segment_id.to_le_bytes()); // 4
        header.extend_from_slice(&checkpoint_id.to_le_bytes()); // 8
        header.extend_from_slice(&account_count.to_le_bytes()); // 8
        header.push(1u8); // 1  compressed=true
        header.extend_from_slice(&[0u8; 6]); // 6  pad
        header.extend_from_slice(&data_crc32c.to_le_bytes()); // 4
        debug_assert_eq!(header.len(), SNAPSHOT_HEADER_SIZE);

        // 5. Write header + body to tmp file
        {
            let mut file = std::fs::File::create(&bin_tmp)?;
            file.write_all(&header)?;
            file.write_all(&body_data)?;
            file.sync_all()?;
        }

        // 6. Compute file-level CRC for sidecar
        let file_data = std::fs::read(&bin_tmp)?;
        let file_crc = crc32c::crc32c(&file_data);
        let file_size = file_data.len() as u64;

        // 7. Write sidecar .crc.tmp: [crc32c:4][size:8][magic:4] = 16 bytes
        {
            let mut sidecar = Vec::with_capacity(16);
            sidecar.extend_from_slice(&file_crc.to_le_bytes());
            sidecar.extend_from_slice(&file_size.to_le_bytes());
            sidecar.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
            let mut crc_file = std::fs::File::create(&crc_tmp)?;
            crc_file.write_all(&sidecar)?;
            crc_file.sync_all()?;
        }

        // 8. Atomic rename both files
        std::fs::rename(&bin_tmp, &bin_final)?;
        std::fs::rename(&crc_tmp, &crc_final)?;
        Ok(())
    }

    /// Loads the snapshot for this specific segment, if it exists and is valid.
    pub fn load_snapshot(&self) -> std::io::Result<Option<SnapshotData>> {
        let data_dir = Path::new(self.data_dir());
        load_snapshot_for_segment(data_dir, self.id())
    }

    pub fn has_snapshot(&self) -> bool {
        let data_dir = Path::new(self.data_dir());
        let bin_path = snapshot_bin_path(data_dir, self.id());
        bin_path.exists()
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn load_snapshot_for_segment(
    data_dir: &Path,
    segment_id: u32,
) -> std::io::Result<Option<SnapshotData>> {
    let bin_path = snapshot_bin_path(data_dir, segment_id);
    let crc_path = snapshot_crc_path(data_dir, segment_id);
    let bin_name = format!("snapshot_{:06}.bin", segment_id);
    let crc_name = format!("snapshot_{:06}.crc", segment_id);

    let bin_data = std::fs::read(&bin_path)?;

    if crc_path.exists() {
        let crc_data = std::fs::read(&crc_path)?;
        if crc_data.len() != 16 {
            let msg = format!(
                "{}: .crc sidecar has wrong size ({})",
                crc_name,
                crc_data.len()
            );
            warn!("{}", msg);
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
        }
        let stored_crc = u32::from_le_bytes(crc_data[0..4].try_into().unwrap());
        let stored_size = u64::from_le_bytes(crc_data[4..12].try_into().unwrap());
        let sidecar_magic = u32::from_le_bytes(crc_data[12..16].try_into().unwrap());

        if sidecar_magic != SNAPSHOT_MAGIC {
            let msg = format!("{}: .crc sidecar has wrong magic", crc_name);
            warn!("{}", msg);
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
        }
        if stored_size != bin_data.len() as u64 {
            let msg = format!(
                "{}: size mismatch (stored={}, actual={})",
                crc_name,
                stored_size,
                bin_data.len()
            );
            warn!("{}", msg);
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
        }
        let actual_crc = crc32c::crc32c(&bin_data);
        if actual_crc != stored_crc {
            let msg = format!("{}: file CRC mismatch", bin_name);
            warn!("{}", msg);
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
        }
    } else {
        warn!(
            "{}: no .crc sidecar found, skipping file-level verification",
            bin_name
        );
        return Ok(None);
    }

    // Parse 36-byte header
    if bin_data.len() < SNAPSHOT_HEADER_SIZE {
        warn!("{}: file too small for header", bin_name);
        return Ok(None);
    }

    let mut offset = 0usize;

    let magic = u32::from_le_bytes(bin_data[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if magic != SNAPSHOT_MAGIC {
        warn!("{}: wrong magic {:#010x}", bin_name, magic);
        return Ok(None);
    }

    let version = bin_data[offset];
    offset += 1;
    if version != SNAPSHOT_VERSION {
        warn!("{}: unknown version {}", bin_name, version);
        return Ok(None);
    }

    let file_segment_id = u32::from_le_bytes(bin_data[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if file_segment_id != segment_id {
        warn!(
            "{}: segment_id mismatch (header={}, expected={})",
            bin_name, file_segment_id, segment_id
        );
        return Ok(None);
    }

    let last_tx_id = u64::from_le_bytes(bin_data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let account_count = u64::from_le_bytes(bin_data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    // compressed flag — always 1 for newly written snapshots; handle 0 for backward compat
    let compressed = bin_data[offset] != 0;
    offset += 1;

    offset += 6; // skip pad

    let data_crc32c = u32::from_le_bytes(bin_data[offset..offset + 4].try_into().unwrap());
    offset += 4;

    let compressed_data = &bin_data[offset..];

    let record_data = if compressed {
        match lz4_flex::decompress_size_prepended(compressed_data) {
            Ok(d) => d,
            Err(e) => {
                warn!("{}: LZ4 decompression failed: {}", bin_name, e);
                return Ok(None);
            }
        }
    } else {
        compressed_data.to_vec()
    };

    let actual_data_crc = crc32c::crc32c(&record_data);
    if actual_data_crc != data_crc32c {
        warn!("{}: data CRC mismatch", bin_name);
        return Ok(None);
    }

    let expected_data_len = account_count as usize * 16;
    if record_data.len() != expected_data_len {
        warn!(
            "{}: data length mismatch (expected={}, got={})",
            bin_name,
            expected_data_len,
            record_data.len()
        );
        return Ok(None);
    }

    let mut balances = Vec::with_capacity(account_count as usize);
    let mut rec_offset = 0;
    for _ in 0..account_count {
        let account_id =
            u64::from_le_bytes(record_data[rec_offset..rec_offset + 8].try_into().unwrap());
        rec_offset += 8;
        let balance =
            i64::from_le_bytes(record_data[rec_offset..rec_offset + 8].try_into().unwrap());
        rec_offset += 8;
        balances.push((account_id, balance));
    }

    Ok(Some(SnapshotData {
        segment_id,
        last_tx_id,
        balances,
    }))
}
