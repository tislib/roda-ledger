//! Function-registry snapshot file (ADR-014).
//!
//! Mirrors the balance snapshot ([`crate::storage::snapshot`]) but persists the
//! current function registry — `(name, version, crc32c)` triples — instead of
//! account balances. The Seal stage emits one `function_snapshot_{N}.bin` per
//! sealed segment that meets `snapshot_frequency`. Recovery reads the most
//! recent snapshot back, reloads the binaries from `functions/` into the
//! `WasmRuntime`, then replays any `FunctionRegistered` WAL records appearing
//! after the snapshot.
//!
//! All public I/O on this format goes through [`crate::storage::Storage`]
//! (`save_function_snapshot` / `load_function_snapshot`) — this module's
//! `save` / `load` helpers are `pub(super)` so no caller outside the
//! storage layer sees raw paths.
//!
//! Layout:
//! ```text
//! Header (36 bytes):
//!   magic(4)          0x46554E43 "FUNC"
//!   version(1)        1
//!   pad(3)
//!   segment_id(4)
//!   checkpoint_id(8)  last_tx_id at snapshot time
//!   function_count(8)
//!   compressed(1)     1 = LZ4 body
//!   pad(3)
//!   data_crc32c(4)    CRC32C of uncompressed body
//!
//! Body (40 bytes per record, LZ4-compressed when compressed=1):
//!   name(32)          snake_case ASCII, null-padded
//!   version(2)        u16 little-endian
//!   crc32c(4)         CRC32C of WASM binary; 0 = unregistered
//!   _pad(2)
//!
//! Sidecar `.crc` (16 bytes):
//!   file_crc32c(4) | file_size(8) | magic(4)
//! ```

use bytemuck::{Pod, Zeroable};
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Magic identifying a function snapshot file ("FUNC" little-endian).
pub const FUNCTION_SNAPSHOT_MAGIC: u32 = 0x46554E43;
/// Format version of the function snapshot.
pub const FUNCTION_SNAPSHOT_VERSION: u8 = 1;
/// Header size in bytes (matches the balance snapshot header exactly).
pub const FUNCTION_SNAPSHOT_HEADER_SIZE: usize = 36;
/// Size of one function record on disk.
pub const FUNCTION_SNAPSHOT_RECORD_SIZE: usize = 40;
/// Sidecar `.crc` file size.
pub const FUNCTION_SNAPSHOT_SIDECAR_SIZE: usize = 16;

/// One function record on disk. Exactly 40 bytes, naturally aligned for `Pod`.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct FunctionSnapshotRecord {
    pub name: [u8; 32], // 32 @ 0
    pub crc32c: u32,    // 4  @ 32
    pub version: u16,   // 2  @ 36
    pub _pad: [u8; 2],  // 2  @ 38
}
const _: () =
    assert!(std::mem::size_of::<FunctionSnapshotRecord>() == FUNCTION_SNAPSHOT_RECORD_SIZE);

impl FunctionSnapshotRecord {
    /// Build a record from logical fields. `name` is null-padded into the
    /// 32-byte field; longer names are rejected at the call site by
    /// `wasm_runtime::validate_name`.
    pub fn new(name: &str, version: u16, crc32c: u32) -> Self {
        let mut name_buf = [0u8; 32];
        let bytes = name.as_bytes();
        let copy_len = bytes.len().min(32);
        name_buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
        Self {
            name: name_buf,
            version,
            crc32c,
            _pad: [0; 2],
        }
    }

    /// Borrow the snake-case name with trailing nulls trimmed.
    pub fn name_str(&self) -> &str {
        let end = self.name.iter().position(|b| *b == 0).unwrap_or(32);
        std::str::from_utf8(&self.name[..end]).unwrap_or("")
    }
}

/// In-memory result of loading a function snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionSnapshotData {
    pub segment_id: u32,
    pub last_tx_id: u64,
    pub records: Vec<FunctionSnapshotRecord>,
}

fn bin_path(data_dir: &Path, segment_id: u32) -> PathBuf {
    data_dir.join(format!("function_snapshot_{:06}.bin", segment_id))
}

fn crc_path(data_dir: &Path, segment_id: u32) -> PathBuf {
    data_dir.join(format!("function_snapshot_{:06}.crc", segment_id))
}

fn bin_tmp_path(data_dir: &Path, segment_id: u32) -> PathBuf {
    data_dir.join(format!("function_snapshot_{:06}.bin.tmp", segment_id))
}

fn crc_tmp_path(data_dir: &Path, segment_id: u32) -> PathBuf {
    data_dir.join(format!("function_snapshot_{:06}.crc.tmp", segment_id))
}

/// Atomically write a function snapshot under `data_dir`.
///
/// The sequence is:
/// 1. Write the `.bin.tmp` (header + LZ4 compressed body) and the `.crc.tmp`
///    sidecar (file_crc32c + file_size + magic).
/// 2. Rename `.bin.tmp` → `.bin` and `.crc.tmp` → `.crc`.
///
/// Exposed only inside the storage layer; external callers go through
/// [`crate::storage::Storage::save_function_snapshot`].
pub(super) fn save(
    data_dir: &Path,
    segment_id: u32,
    last_tx_id: u64,
    records: &[FunctionSnapshotRecord],
) -> io::Result<()> {
    // Defensive — `Storage::new` already created `data_dir`.
    fs::create_dir_all(data_dir)?;

    let raw_body = bytemuck::cast_slice::<FunctionSnapshotRecord, u8>(records).to_vec();
    let data_crc32c = crc32c::crc32c(&raw_body);
    let body_compressed = lz4_flex::compress_prepend_size(&raw_body);

    // Build the 36-byte header by hand to side-step bytemuck alignment rules,
    // matching the layout used by the balance snapshot.
    let mut header = Vec::with_capacity(FUNCTION_SNAPSHOT_HEADER_SIZE);
    header.extend_from_slice(&FUNCTION_SNAPSHOT_MAGIC.to_le_bytes()); // 4
    header.push(FUNCTION_SNAPSHOT_VERSION); // 1
    header.extend_from_slice(&segment_id.to_le_bytes()); // 4
    header.extend_from_slice(&last_tx_id.to_le_bytes()); // 8
    header.extend_from_slice(&(records.len() as u64).to_le_bytes()); // 8
    header.push(1u8); // 1  compressed=true
    header.extend_from_slice(&[0u8; 6]); // 6  pad
    header.extend_from_slice(&data_crc32c.to_le_bytes()); // 4
    debug_assert_eq!(header.len(), FUNCTION_SNAPSHOT_HEADER_SIZE);

    let bin_tmp = bin_tmp_path(data_dir, segment_id);
    let crc_tmp = crc_tmp_path(data_dir, segment_id);
    let bin = bin_path(data_dir, segment_id);
    let crc = crc_path(data_dir, segment_id);

    {
        let mut f = fs::File::create(&bin_tmp)?;
        f.write_all(&header)?;
        f.write_all(&body_compressed)?;
        f.sync_all()?;
    }

    let file_bytes = fs::read(&bin_tmp)?;
    let file_size = file_bytes.len() as u64;
    let file_crc32c = crc32c::crc32c(&file_bytes);

    {
        let mut f = fs::File::create(&crc_tmp)?;
        f.write_all(&file_crc32c.to_le_bytes())?;
        f.write_all(&file_size.to_le_bytes())?;
        f.write_all(&FUNCTION_SNAPSHOT_MAGIC.to_le_bytes())?;
        f.sync_all()?;
    }

    fs::rename(&bin_tmp, &bin)?;
    fs::rename(&crc_tmp, &crc)?;
    Ok(())
}

/// Load and validate a function snapshot from `data_dir`.
///
/// Verifies:
/// - sidecar size, magic, and `file_crc32c` matches the actual `.bin` content
/// - header magic + version
/// - body `data_crc32c` after decompression
///
/// Exposed only inside the storage layer; external callers go through
/// [`crate::storage::Storage::load_function_snapshot`].
pub(super) fn load(data_dir: &Path, segment_id: u32) -> io::Result<FunctionSnapshotData> {
    let bin = bin_path(data_dir, segment_id);
    let crc = crc_path(data_dir, segment_id);

    let sidecar_bytes = fs::read(&crc)?;
    if sidecar_bytes.len() != FUNCTION_SNAPSHOT_SIDECAR_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "function snapshot sidecar wrong size: {} (expected {})",
                sidecar_bytes.len(),
                FUNCTION_SNAPSHOT_SIDECAR_SIZE
            ),
        ));
    }
    let expected_crc = u32::from_le_bytes(sidecar_bytes[0..4].try_into().unwrap());
    let expected_size = u64::from_le_bytes(sidecar_bytes[4..12].try_into().unwrap());
    let sidecar_magic = u32::from_le_bytes(sidecar_bytes[12..16].try_into().unwrap());
    if sidecar_magic != FUNCTION_SNAPSHOT_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "function snapshot sidecar magic mismatch",
        ));
    }

    let mut bin_file = fs::File::open(&bin)?;
    let bin_meta = bin_file.metadata()?;
    if bin_meta.len() != expected_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "function snapshot size mismatch: {} on disk vs {} in sidecar",
                bin_meta.len(),
                expected_size
            ),
        ));
    }

    let mut bin_bytes = Vec::with_capacity(bin_meta.len() as usize);
    bin_file.read_to_end(&mut bin_bytes)?;
    let actual_file_crc = crc32c::crc32c(&bin_bytes);
    if actual_file_crc != expected_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "function snapshot file CRC mismatch: got {:08x}, expected {:08x}",
                actual_file_crc, expected_crc
            ),
        ));
    }

    if bin_bytes.len() < FUNCTION_SNAPSHOT_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "function snapshot file too small for header",
        ));
    }
    // Manually parse the 36-byte header (mirror of `save`).
    let h = &bin_bytes[..FUNCTION_SNAPSHOT_HEADER_SIZE];
    let header_magic = u32::from_le_bytes(h[0..4].try_into().unwrap());
    let header_version = h[4];
    let header_segment_id = u32::from_le_bytes(h[5..9].try_into().unwrap());
    let header_checkpoint_id = u64::from_le_bytes(h[9..17].try_into().unwrap());
    let header_function_count = u64::from_le_bytes(h[17..25].try_into().unwrap());
    let header_compressed = h[25];
    // h[26..32] = pad
    let header_data_crc32c = u32::from_le_bytes(h[32..36].try_into().unwrap());

    if header_magic != FUNCTION_SNAPSHOT_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "function snapshot header magic mismatch",
        ));
    }
    if header_version != FUNCTION_SNAPSHOT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported function snapshot version {}", header_version),
        ));
    }

    let body_bytes = &bin_bytes[FUNCTION_SNAPSHOT_HEADER_SIZE..];
    let raw = if header_compressed == 1 {
        lz4_flex::decompress_size_prepended(body_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
    } else {
        body_bytes.to_vec()
    };

    let observed_crc = crc32c::crc32c(&raw);
    if observed_crc != header_data_crc32c {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "function snapshot body CRC mismatch: got {:08x}, expected {:08x}",
                observed_crc, header_data_crc32c
            ),
        ));
    }

    let expected_body_len = header_function_count as usize * FUNCTION_SNAPSHOT_RECORD_SIZE;
    if raw.len() != expected_body_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "function snapshot body length mismatch: {} vs {} expected",
                raw.len(),
                expected_body_len
            ),
        ));
    }

    // `raw` may not be aligned to FunctionSnapshotRecord's 4-byte alignment
    // (it came straight from a `Vec<u8>` decompressed body). Use
    // `pod_read_unaligned` per chunk to side-step the alignment check.
    let records: Vec<FunctionSnapshotRecord> = raw
        .chunks_exact(FUNCTION_SNAPSHOT_RECORD_SIZE)
        .map(bytemuck::pod_read_unaligned::<FunctionSnapshotRecord>)
        .collect();

    Ok(FunctionSnapshotData {
        segment_id: header_segment_id,
        last_tx_id: header_checkpoint_id,
        records,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use crate::storage::Storage;
    use tempfile::tempdir;

    fn temp_storage() -> (Storage, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let cfg = StorageConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            ..StorageConfig::default()
        };
        let storage = Storage::new(cfg).unwrap();
        (storage, dir)
    }

    fn data_dir_of(s: &Storage) -> PathBuf {
        PathBuf::from(&s.config().data_dir)
    }

    #[test]
    fn save_and_load_empty() {
        let (storage, _td) = temp_storage();
        storage.save_function_snapshot(4, 1234, &[]).unwrap();
        let data = storage.load_function_snapshot(4).unwrap();
        assert_eq!(data.segment_id, 4);
        assert_eq!(data.last_tx_id, 1234);
        assert!(data.records.is_empty());
    }

    #[test]
    fn save_and_load_many_records() {
        let (storage, _td) = temp_storage();
        let mut records = Vec::new();
        for i in 0u32..50 {
            records.push(FunctionSnapshotRecord::new(
                &format!("fn_{}", i),
                (i + 1) as u16,
                0xDEADBEEF ^ i,
            ));
        }
        storage
            .save_function_snapshot(12, 9_999_999, &records)
            .unwrap();

        let data = storage.load_function_snapshot(12).unwrap();
        assert_eq!(data.segment_id, 12);
        assert_eq!(data.last_tx_id, 9_999_999);
        assert_eq!(data.records.len(), 50);
        assert_eq!(data.records, records);
        assert_eq!(data.records[3].name_str(), "fn_3");
    }

    #[test]
    fn unregistered_records_have_zero_crc() {
        let (storage, _td) = temp_storage();
        let records = vec![
            FunctionSnapshotRecord::new("registered", 2, 0x1234_5678),
            FunctionSnapshotRecord::new("removed", 5, 0),
        ];
        storage.save_function_snapshot(1, 0, &records).unwrap();
        let data = storage.load_function_snapshot(1).unwrap();
        assert_eq!(data.records[0].crc32c, 0x1234_5678);
        assert_eq!(data.records[1].crc32c, 0);
        assert_eq!(data.records[1].name_str(), "removed");
    }

    #[test]
    fn load_rejects_corrupt_sidecar() {
        let (storage, _td) = temp_storage();
        let records = vec![FunctionSnapshotRecord::new("foo", 1, 0xAA55_AA55)];
        storage.save_function_snapshot(7, 0, &records).unwrap();
        let dir = data_dir_of(&storage);
        let crc = crc_path(&dir, 7);
        let mut buf = fs::read(&crc).unwrap();
        buf[12] ^= 0xFF; // corrupt sidecar magic
        fs::write(&crc, &buf).unwrap();
        assert!(storage.load_function_snapshot(7).is_err());
    }

    #[test]
    fn load_detects_body_corruption() {
        let (storage, _td) = temp_storage();
        let records = vec![FunctionSnapshotRecord::new("foo", 1, 0xAA55_AA55)];
        storage.save_function_snapshot(8, 0, &records).unwrap();
        let dir = data_dir_of(&storage);
        let path = bin_path(&dir, 8);
        let mut buf = fs::read(&path).unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0x01; // flip a byte well past the header
        fs::write(&path, &buf).unwrap();
        assert!(storage.load_function_snapshot(8).is_err());
    }

    #[test]
    fn name_str_trims_nulls() {
        let r = FunctionSnapshotRecord::new("ab", 1, 0);
        assert_eq!(r.name_str(), "ab");
        assert_eq!(r.name[2], 0);
    }
}
