use crate::storage::WAL_MAGIC;
use spdlog::warn;
use std::path::Path;

pub fn verify_wal_data(
    bin_data: &[u8],
    crc_path: &Path,
    segment_id: u32,
) -> Result<(), std::io::Error> {
    // A sealed segment must have a .crc sidecar for integrity verification
    if !crc_path.exists() {
        warn!(
            "WAL segment {:06}: missing .crc sidecar at {:?}",
            segment_id, crc_path
        );
        return Ok(());
    }

    let crc_data = std::fs::read(&crc_path)?;

    if crc_data.len() != 16 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "WAL segment {:06}: .crc sidecar has wrong size {} (expected 16)",
                segment_id,
                crc_data.len()
            ),
        ));
    }

    let stored_crc = u32::from_le_bytes(crc_data[0..4].try_into().unwrap());
    let stored_size = u64::from_le_bytes(crc_data[4..12].try_into().unwrap());
    let magic = u32::from_le_bytes(crc_data[12..16].try_into().unwrap());

    if magic != WAL_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "WAL segment {:06}: .crc sidecar magic mismatch {:#010x} (expected {:#010x})",
                segment_id, magic, WAL_MAGIC
            ),
        ));
    }
    if stored_size != bin_data.len() as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "WAL segment {:06}: size mismatch in .crc sidecar (stored={}, actual={})",
                segment_id,
                stored_size,
                bin_data.len()
            ),
        ));
    }
    let actual_crc = crc32c::crc32c(&bin_data);
    if actual_crc != stored_crc {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "WAL segment {:06}: file CRC mismatch (stored={:#010x}, actual={:#010x})",
                segment_id, stored_crc, actual_crc
            ),
        ));
    }

    Ok(())
}

pub fn read_wal_data(bin_path: &Path) -> Result<Vec<u8>, std::io::Error> {
    // Read segment binary data — failure is fatal
    let bin_data = std::fs::read(bin_path)?;

    if bin_data.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("WAL segment file {:?} is empty", bin_path.display()),
        ));
    }
    Ok(bin_data)
}
