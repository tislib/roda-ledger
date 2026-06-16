use crate::layout::{
    function_snapshot_bin_path, function_snapshot_bin_tmp_path, function_snapshot_crc_path,
    function_snapshot_crc_tmp_path, kv_snapshot_bin_path, kv_snapshot_bin_tmp_path,
    kv_snapshot_crc_path, kv_snapshot_crc_tmp_path, snapshot_bin_path, snapshot_bin_tmp_path,
    snapshot_crc_path, snapshot_crc_tmp_path,
};
use crate::segment::Segment;
use spdlog::warn;
use std::io::Write;
use std::path::Path;

pub struct SnapshotData {
    pub segment_id: u32,
    pub last_tx_id: u64,
    /// Account allocator high-water at the snapshot boundary (ADR-022): the
    /// next id to allocate. Reconstructs OPEN status for opened-but-unfunded
    /// accounts that the balance set alone can't (zero-balance accounts are
    /// not persisted). Version-1 snapshots default this to 1.
    pub next_account_id: u64,
    /// Existent accounts at the boundary: `(id, balance, flags)` (ADR-022 §5).
    /// Persisting flags keeps PROGRAMMED buckets from recovering as OPEN.
    pub accounts: Vec<(u64, i64, u64)>,
    /// Parent→bucket links: `(parent_id, type_id, child_id)` (ADR-022 §3).
    pub links: Vec<(u64, u16, u64)>,
}

/// In-memory representation of a function snapshot (internal.md §20.6).
/// Each entry is `(name, version, crc32c)` — `crc32c == 0` means the name
/// was unregistered at the snapshot boundary. Entries are sorted by name
/// for deterministic on-disk layout.
pub struct FunctionSnapshotData {
    pub segment_id: u32,
    pub entries: Vec<(String, u16, u32)>,
}

const SNAPSHOT_MAGIC: u32 = 0x534E4150; // "SNAP"
const SNAPSHOT_VERSION: u8 = 3; // v3 adds per-account flags + the link table (ADR-022)
const SNAPSHOT_HEADER_SIZE: usize = 50; // v2 was 44; +8 link_count, pad 6→4

const FUNCTION_SNAPSHOT_MAGIC: u32 = 0x46554E43; // "FUNC"
const FUNCTION_SNAPSHOT_VERSION: u8 = 1;
const FUNCTION_SNAPSHOT_HEADER_SIZE: usize = 28;
const FUNCTION_RECORD_NAME_LEN: usize = 32;
const FUNCTION_RECORD_SIZE: usize = FUNCTION_RECORD_NAME_LEN + 2 + 4; // 38 bytes

/// In-memory representation of a KV snapshot (ADR-023): the full programmable
/// KV state at the snapshot boundary. Each entry is the packed `(key, value)`
/// pair of a `KvEntry` (30-byte `KeyPath` + 9-byte `Value` slot); `constants`
/// are the interned `(key, null-terminated value)` definitions. Both sorted on
/// disk for determinism, stored in the one file.
pub struct KvSnapshotData {
    pub segment_id: u32,
    pub entries: Vec<([u8; 30], [u8; 9])>,
    pub constants: Vec<(u32, [u8; 32])>,
}

const KV_SNAPSHOT_MAGIC: u32 = 0x4B565354; // "KVST"
const KV_SNAPSHOT_VERSION: u8 = 2; // v2 appends constant records after entries
const KV_SNAPSHOT_HEADER_SIZE: usize = 28;
const KV_RECORD_SIZE: usize = 30 + 9; // packed KeyPath + Value slot = 39
const KV_CONSTANT_RECORD_SIZE: usize = 4 + 32; // key + value[32] = 36

impl Segment {
    /// Writes a compressed snapshot for this segment to disk (temp files plus
    /// atomic rename for crash safety). Format (ADR-006/ADR-022 v3): a header of
    /// magic(4), version(1), segment_id(4), checkpoint_id(8), next_account_id(8),
    /// account_count(8), link_count(8), compressed(1), pad(4), data_crc32c(4),
    /// then an lz4 body of account_count×(id:8, balance:8, flags:8) followed by
    /// link_count×(parent:8, type_id:2, child:8).
    pub fn save_snapshot(
        &self,
        next_account_id: u64,
        accounts: &[(u64, i64, u64)],
        links: &[(u64, u16, u64)],
    ) -> std::io::Result<()> {
        let segment_id = self.id();
        let data_dir = Path::new(self.data_dir());
        let bin_tmp = snapshot_bin_tmp_path(data_dir, segment_id);
        let crc_tmp = snapshot_crc_tmp_path(data_dir, segment_id);
        let bin_final = snapshot_bin_path(data_dir, segment_id);
        let crc_final = snapshot_crc_path(data_dir, segment_id);

        let account_count = accounts.len() as u64;
        let link_count = links.len() as u64;
        let checkpoint_id = segment_id as u64;

        // 1. Serialize: accounts (id:u64 LE, balance:i64 LE, flags:u64 LE) then
        //    links (parent:u64 LE, type_id:u16 LE, child:u64 LE).
        let mut raw_data: Vec<u8> = Vec::with_capacity(accounts.len() * 24 + links.len() * 18);
        for (account_id, balance, flags) in accounts {
            raw_data.extend_from_slice(&account_id.to_le_bytes());
            raw_data.extend_from_slice(&balance.to_le_bytes());
            raw_data.extend_from_slice(&flags.to_le_bytes());
        }
        for (parent_id, type_id, child_id) in links {
            raw_data.extend_from_slice(&parent_id.to_le_bytes());
            raw_data.extend_from_slice(&type_id.to_le_bytes());
            raw_data.extend_from_slice(&child_id.to_le_bytes());
        }

        // 2. Compute data CRC over raw (uncompressed) data
        let data_crc32c = crc32c::crc32c(&raw_data);

        // 3. LZ4 compress (always mandatory)
        let body_data = lz4_flex::compress_prepend_size(&raw_data);

        // 4. Build header
        let mut header = Vec::with_capacity(SNAPSHOT_HEADER_SIZE);
        header.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes()); // 4
        header.push(SNAPSHOT_VERSION); // 1
        header.extend_from_slice(&segment_id.to_le_bytes()); // 4
        header.extend_from_slice(&checkpoint_id.to_le_bytes()); // 8
        header.extend_from_slice(&next_account_id.to_le_bytes()); // 8
        header.extend_from_slice(&account_count.to_le_bytes()); // 8
        header.extend_from_slice(&link_count.to_le_bytes()); // 8
        header.push(1u8); // 1  compressed=true
        header.extend_from_slice(&[0u8; 4]); // 4  pad
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

    /// Write the function-registry snapshot for this segment (internal.md §20.6).
    /// Mirrors `save_snapshot`: 28-byte header (magic + version + segment_id +
    /// entry_count + data_crc32c) + LZ4-compressed body of 38-byte
    /// `(name[32], version, crc32c)` records, written via temp files +
    /// atomic rename. Empty entry lists are still emitted (internal.md §20.7).
    pub fn save_function_snapshot(&self, records: &[(String, u16, u32)]) -> std::io::Result<()> {
        let segment_id = self.id();
        let data_dir = Path::new(self.data_dir());
        let bin_tmp = function_snapshot_bin_tmp_path(data_dir, segment_id);
        let crc_tmp = function_snapshot_crc_tmp_path(data_dir, segment_id);
        let bin_final = function_snapshot_bin_path(data_dir, segment_id);
        let crc_final = function_snapshot_crc_path(data_dir, segment_id);

        let entry_count = records.len() as u32;

        let mut raw_data: Vec<u8> = Vec::with_capacity(records.len() * FUNCTION_RECORD_SIZE);
        for (name, version, crc) in records {
            let bytes = name.as_bytes();
            if bytes.len() > FUNCTION_RECORD_NAME_LEN {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "function name `{}` exceeds {} bytes",
                        name, FUNCTION_RECORD_NAME_LEN
                    ),
                ));
            }
            let mut name_buf = [0u8; FUNCTION_RECORD_NAME_LEN];
            name_buf[..bytes.len()].copy_from_slice(bytes);
            raw_data.extend_from_slice(&name_buf);
            raw_data.extend_from_slice(&version.to_le_bytes());
            raw_data.extend_from_slice(&crc.to_le_bytes());
        }

        let data_crc32c = crc32c::crc32c(&raw_data);
        let body_data = lz4_flex::compress_prepend_size(&raw_data);

        let mut header = Vec::with_capacity(FUNCTION_SNAPSHOT_HEADER_SIZE);
        header.extend_from_slice(&FUNCTION_SNAPSHOT_MAGIC.to_le_bytes()); // 4
        header.push(FUNCTION_SNAPSHOT_VERSION); // 1
        header.extend_from_slice(&[0u8; 3]); // 3 pad
        header.extend_from_slice(&segment_id.to_le_bytes()); // 4
        header.extend_from_slice(&entry_count.to_le_bytes()); // 4
        header.push(1u8); // 1 compressed=true
        header.extend_from_slice(&[0u8; 7]); // 7 pad
        header.extend_from_slice(&data_crc32c.to_le_bytes()); // 4
        debug_assert_eq!(header.len(), FUNCTION_SNAPSHOT_HEADER_SIZE);

        {
            let mut file = std::fs::File::create(&bin_tmp)?;
            file.write_all(&header)?;
            file.write_all(&body_data)?;
            file.sync_all()?;
        }

        let file_data = std::fs::read(&bin_tmp)?;
        let file_crc = crc32c::crc32c(&file_data);
        let file_size = file_data.len() as u64;

        {
            let mut sidecar = Vec::with_capacity(16);
            sidecar.extend_from_slice(&file_crc.to_le_bytes());
            sidecar.extend_from_slice(&file_size.to_le_bytes());
            sidecar.extend_from_slice(&FUNCTION_SNAPSHOT_MAGIC.to_le_bytes());
            let mut crc_file = std::fs::File::create(&crc_tmp)?;
            crc_file.write_all(&sidecar)?;
            crc_file.sync_all()?;
        }

        std::fs::rename(&bin_tmp, &bin_final)?;
        std::fs::rename(&crc_tmp, &crc_final)?;
        Ok(())
    }

    pub fn load_function_snapshot(&self) -> std::io::Result<Option<FunctionSnapshotData>> {
        let data_dir = Path::new(self.data_dir());
        load_function_snapshot_for_segment(data_dir, self.id())
    }

    pub fn has_function_snapshot(&self) -> bool {
        let data_dir = Path::new(self.data_dir());
        function_snapshot_bin_path(data_dir, self.id()).exists()
    }

    /// Write the programmable-KV snapshot for this segment (ADR-023). Mirrors
    /// `save_function_snapshot`: 28-byte header + LZ4-compressed body of 34-byte
    /// KV-entry records followed by 36-byte constant records, via temp + atomic
    /// rename. Entries and constants share the one file, compression, and CRC.
    pub fn save_kv_snapshot(
        &self,
        records: &[([u8; 30], [u8; 9])],
        constants: &[(u32, [u8; 32])],
    ) -> std::io::Result<()> {
        let segment_id = self.id();
        let data_dir = Path::new(self.data_dir());
        let bin_tmp = kv_snapshot_bin_tmp_path(data_dir, segment_id);
        let crc_tmp = kv_snapshot_crc_tmp_path(data_dir, segment_id);
        let bin_final = kv_snapshot_bin_path(data_dir, segment_id);
        let crc_final = kv_snapshot_crc_path(data_dir, segment_id);

        let entry_count = records.len() as u32;
        let constant_count = constants.len() as u32;
        let mut raw_data: Vec<u8> = Vec::with_capacity(
            records.len() * KV_RECORD_SIZE + constants.len() * KV_CONSTANT_RECORD_SIZE,
        );
        for (key, value) in records {
            raw_data.extend_from_slice(key);
            raw_data.extend_from_slice(value);
        }
        for (key, value) in constants {
            raw_data.extend_from_slice(&key.to_le_bytes());
            raw_data.extend_from_slice(value);
        }

        let data_crc32c = crc32c::crc32c(&raw_data);
        let body_data = lz4_flex::compress_prepend_size(&raw_data);

        let mut header = Vec::with_capacity(KV_SNAPSHOT_HEADER_SIZE);
        header.extend_from_slice(&KV_SNAPSHOT_MAGIC.to_le_bytes()); // 4
        header.push(KV_SNAPSHOT_VERSION); // 1
        header.extend_from_slice(&[0u8; 3]); // 3 pad
        header.extend_from_slice(&segment_id.to_le_bytes()); // 4
        header.extend_from_slice(&entry_count.to_le_bytes()); // 4
        header.push(1u8); // 1 compressed=true
        header.extend_from_slice(&constant_count.to_le_bytes()); // 4
        header.extend_from_slice(&[0u8; 3]); // 3 pad
        header.extend_from_slice(&data_crc32c.to_le_bytes()); // 4
        debug_assert_eq!(header.len(), KV_SNAPSHOT_HEADER_SIZE);

        {
            let mut file = std::fs::File::create(&bin_tmp)?;
            file.write_all(&header)?;
            file.write_all(&body_data)?;
            file.sync_all()?;
        }

        let file_data = std::fs::read(&bin_tmp)?;
        let file_crc = crc32c::crc32c(&file_data);
        let file_size = file_data.len() as u64;

        {
            let mut sidecar = Vec::with_capacity(16);
            sidecar.extend_from_slice(&file_crc.to_le_bytes());
            sidecar.extend_from_slice(&file_size.to_le_bytes());
            sidecar.extend_from_slice(&KV_SNAPSHOT_MAGIC.to_le_bytes());
            let mut crc_file = std::fs::File::create(&crc_tmp)?;
            crc_file.write_all(&sidecar)?;
            crc_file.sync_all()?;
        }

        std::fs::rename(&bin_tmp, &bin_final)?;
        std::fs::rename(&crc_tmp, &crc_final)?;
        Ok(())
    }

    pub fn load_kv_snapshot(&self) -> std::io::Result<Option<KvSnapshotData>> {
        let data_dir = Path::new(self.data_dir());
        load_kv_snapshot_for_segment(data_dir, self.id())
    }

    pub fn has_kv_snapshot(&self) -> bool {
        let data_dir = Path::new(self.data_dir());
        kv_snapshot_bin_path(data_dir, self.id()).exists()
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

    let next_account_id = u64::from_le_bytes(bin_data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let account_count = u64::from_le_bytes(bin_data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let link_count = u64::from_le_bytes(bin_data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    // compressed flag — always 1 for newly written snapshots; handle 0 for backward compat
    let compressed = bin_data[offset] != 0;
    offset += 1;

    offset += 4; // skip pad

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

    let expected_data_len = account_count as usize * 24 + link_count as usize * 18;
    if record_data.len() != expected_data_len {
        warn!(
            "{}: data length mismatch (expected={}, got={})",
            bin_name,
            expected_data_len,
            record_data.len()
        );
        return Ok(None);
    }

    let mut rec_offset = 0;
    let mut accounts = Vec::with_capacity(account_count as usize);
    for _ in 0..account_count {
        let account_id =
            u64::from_le_bytes(record_data[rec_offset..rec_offset + 8].try_into().unwrap());
        let balance = i64::from_le_bytes(
            record_data[rec_offset + 8..rec_offset + 16]
                .try_into()
                .unwrap(),
        );
        let flags = u64::from_le_bytes(
            record_data[rec_offset + 16..rec_offset + 24]
                .try_into()
                .unwrap(),
        );
        rec_offset += 24;
        accounts.push((account_id, balance, flags));
    }

    let mut links = Vec::with_capacity(link_count as usize);
    for _ in 0..link_count {
        let parent_id =
            u64::from_le_bytes(record_data[rec_offset..rec_offset + 8].try_into().unwrap());
        let type_id = u16::from_le_bytes(
            record_data[rec_offset + 8..rec_offset + 10]
                .try_into()
                .unwrap(),
        );
        let child_id = u64::from_le_bytes(
            record_data[rec_offset + 10..rec_offset + 18]
                .try_into()
                .unwrap(),
        );
        rec_offset += 18;
        links.push((parent_id, type_id, child_id));
    }

    Ok(Some(SnapshotData {
        segment_id,
        last_tx_id,
        next_account_id,
        accounts,
        links,
    }))
}

fn load_function_snapshot_for_segment(
    data_dir: &Path,
    segment_id: u32,
) -> std::io::Result<Option<FunctionSnapshotData>> {
    let bin_path = function_snapshot_bin_path(data_dir, segment_id);
    let crc_path = function_snapshot_crc_path(data_dir, segment_id);
    let bin_name = format!("function_snapshot_{:06}.bin", segment_id);
    let crc_name = format!("function_snapshot_{:06}.crc", segment_id);

    let bin_data = std::fs::read(&bin_path)?;

    if !crc_path.exists() {
        warn!(
            "{}: no .crc sidecar found, skipping file-level verification",
            bin_name
        );
        return Ok(None);
    }
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
    if sidecar_magic != FUNCTION_SNAPSHOT_MAGIC {
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

    if bin_data.len() < FUNCTION_SNAPSHOT_HEADER_SIZE {
        warn!("{}: file too small for header", bin_name);
        return Ok(None);
    }

    let mut offset = 0usize;
    let magic = u32::from_le_bytes(bin_data[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if magic != FUNCTION_SNAPSHOT_MAGIC {
        warn!("{}: wrong magic {:#010x}", bin_name, magic);
        return Ok(None);
    }
    let version = bin_data[offset];
    offset += 1;
    if version != FUNCTION_SNAPSHOT_VERSION {
        warn!("{}: unknown version {}", bin_name, version);
        return Ok(None);
    }
    offset += 3; // pad

    let file_segment_id = u32::from_le_bytes(bin_data[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if file_segment_id != segment_id {
        warn!(
            "{}: segment_id mismatch (header={}, expected={})",
            bin_name, file_segment_id, segment_id
        );
        return Ok(None);
    }

    let entry_count = u32::from_le_bytes(bin_data[offset..offset + 4].try_into().unwrap());
    offset += 4;

    let compressed = bin_data[offset] != 0;
    offset += 1;
    offset += 7; // pad

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

    let expected_data_len = entry_count as usize * FUNCTION_RECORD_SIZE;
    if record_data.len() != expected_data_len {
        warn!(
            "{}: data length mismatch (expected={}, got={})",
            bin_name,
            expected_data_len,
            record_data.len()
        );
        return Ok(None);
    }

    let mut entries = Vec::with_capacity(entry_count as usize);
    let mut rec_offset = 0;
    for _ in 0..entry_count {
        let name_bytes = &record_data[rec_offset..rec_offset + FUNCTION_RECORD_NAME_LEN];
        rec_offset += FUNCTION_RECORD_NAME_LEN;
        let end = name_bytes
            .iter()
            .position(|b| *b == 0)
            .unwrap_or(FUNCTION_RECORD_NAME_LEN);
        let name = match std::str::from_utf8(&name_bytes[..end]) {
            Ok(s) => s.to_string(),
            Err(_) => {
                warn!("{}: non-UTF-8 function name", bin_name);
                return Ok(None);
            }
        };
        let version =
            u16::from_le_bytes(record_data[rec_offset..rec_offset + 2].try_into().unwrap());
        rec_offset += 2;
        let crc = u32::from_le_bytes(record_data[rec_offset..rec_offset + 4].try_into().unwrap());
        rec_offset += 4;
        entries.push((name, version, crc));
    }

    Ok(Some(FunctionSnapshotData {
        segment_id,
        entries,
    }))
}

fn load_kv_snapshot_for_segment(
    data_dir: &Path,
    segment_id: u32,
) -> std::io::Result<Option<KvSnapshotData>> {
    let bin_path = kv_snapshot_bin_path(data_dir, segment_id);
    let crc_path = kv_snapshot_crc_path(data_dir, segment_id);
    let bin_name = format!("kv_snapshot_{:06}.bin", segment_id);
    let crc_name = format!("kv_snapshot_{:06}.crc", segment_id);

    let bin_data = std::fs::read(&bin_path)?;

    if !crc_path.exists() {
        warn!(
            "{}: no .crc sidecar found, skipping file-level verification",
            bin_name
        );
        return Ok(None);
    }
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
    if sidecar_magic != KV_SNAPSHOT_MAGIC {
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

    if bin_data.len() < KV_SNAPSHOT_HEADER_SIZE {
        warn!("{}: file too small for header", bin_name);
        return Ok(None);
    }

    let mut offset = 0usize;
    let magic = u32::from_le_bytes(bin_data[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if magic != KV_SNAPSHOT_MAGIC {
        warn!("{}: wrong magic {:#010x}", bin_name, magic);
        return Ok(None);
    }
    let version = bin_data[offset];
    offset += 1;
    if version != KV_SNAPSHOT_VERSION {
        warn!("{}: unknown version {}", bin_name, version);
        return Ok(None);
    }
    offset += 3; // pad

    let file_segment_id = u32::from_le_bytes(bin_data[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if file_segment_id != segment_id {
        warn!(
            "{}: segment_id mismatch (header={}, expected={})",
            bin_name, file_segment_id, segment_id
        );
        return Ok(None);
    }

    let entry_count = u32::from_le_bytes(bin_data[offset..offset + 4].try_into().unwrap());
    offset += 4;

    let compressed = bin_data[offset] != 0;
    offset += 1;
    let constant_count = u32::from_le_bytes(bin_data[offset..offset + 4].try_into().unwrap());
    offset += 4;
    offset += 3; // pad

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

    let expected_data_len =
        entry_count as usize * KV_RECORD_SIZE + constant_count as usize * KV_CONSTANT_RECORD_SIZE;
    if record_data.len() != expected_data_len {
        warn!(
            "{}: data length mismatch (expected={}, got={})",
            bin_name,
            expected_data_len,
            record_data.len()
        );
        return Ok(None);
    }

    let mut entries = Vec::with_capacity(entry_count as usize);
    let mut rec_offset = 0;
    for _ in 0..entry_count {
        let mut key = [0u8; 30];
        key.copy_from_slice(&record_data[rec_offset..rec_offset + 30]);
        rec_offset += 30;
        let mut value = [0u8; 9];
        value.copy_from_slice(&record_data[rec_offset..rec_offset + 9]);
        rec_offset += 9;
        entries.push((key, value));
    }

    let mut constants = Vec::with_capacity(constant_count as usize);
    for _ in 0..constant_count {
        let key = u32::from_le_bytes(record_data[rec_offset..rec_offset + 4].try_into().unwrap());
        rec_offset += 4;
        let mut value = [0u8; 32];
        value.copy_from_slice(&record_data[rec_offset..rec_offset + 32]);
        rec_offset += 32;
        constants.push((key, value));
    }

    Ok(Some(KvSnapshotData {
        segment_id,
        entries,
        constants,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use crate::engine::Storage;

    fn temp_segment() -> (Segment, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = StorageConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            ..StorageConfig::default()
        };
        let storage = Storage::new(cfg).expect("storage");
        let mut seg = storage.active_segment().expect("active segment");
        seg.close().expect("close segment");
        (seg, dir)
    }

    #[test]
    fn function_snapshot_roundtrip() {
        let (seg, _td) = temp_segment();
        let records = vec![
            ("alpha".to_string(), 1u16, 0xDEADBEEFu32),
            ("beta".to_string(), 7u16, 0u32), // unregister
            ("gamma".to_string(), 3u16, 0xCAFEBABEu32),
        ];
        seg.save_function_snapshot(&records).expect("save");
        assert!(seg.has_function_snapshot());

        let loaded = seg
            .load_function_snapshot()
            .expect("load")
            .expect("data present");
        assert_eq!(loaded.segment_id, seg.id());
        assert_eq!(loaded.entries, records);
    }

    #[test]
    fn empty_function_snapshot_roundtrip() {
        let (seg, _td) = temp_segment();
        seg.save_function_snapshot(&[]).expect("save empty");
        assert!(seg.has_function_snapshot());
        let loaded = seg
            .load_function_snapshot()
            .expect("load")
            .expect("data present");
        assert_eq!(loaded.segment_id, seg.id());
        assert!(loaded.entries.is_empty());
    }

    #[test]
    fn function_snapshot_rejects_long_name() {
        let (seg, _td) = temp_segment();
        let bad_name = "x".repeat(33);
        let err = seg
            .save_function_snapshot(&[(bad_name, 1, 1)])
            .expect_err("should reject long name");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn kv_snapshot_roundtrip() {
        let (seg, _td) = temp_segment();
        use crate::kv::{KeyPath, Value};
        let pack = |k: i64, v: i64| {
            (
                KeyPath::new([Value::Int(k)]).pack().unwrap(),
                Value::pack_slot(Some(&Value::Int(v))).unwrap(),
            )
        };
        let records = vec![pack(1, 42), pack(7, 100), pack(3, -9)];
        let mut pending = [0u8; 32];
        pending[..7].copy_from_slice(b"PENDING");
        let constants = vec![(1u32, pending), (2u32, [0u8; 32])];
        seg.save_kv_snapshot(&records, &constants).expect("save");
        assert!(seg.has_kv_snapshot());
        let loaded = seg.load_kv_snapshot().expect("load").expect("present");
        assert_eq!(loaded.segment_id, seg.id());
        assert_eq!(loaded.entries, records);
        assert_eq!(loaded.constants, constants);
    }

    #[test]
    fn empty_kv_snapshot_roundtrip() {
        let (seg, _td) = temp_segment();
        seg.save_kv_snapshot(&[], &[]).expect("save empty");
        assert!(seg.has_kv_snapshot());
        let loaded = seg.load_kv_snapshot().expect("load").expect("present");
        assert!(loaded.entries.is_empty());
        assert!(loaded.constants.is_empty());
    }

    #[test]
    fn snapshot_roundtrip_preserves_accounts_flags_and_links() {
        let (seg, _td) = temp_segment();
        // (id, balance, flags) — flags lane 0: 1 = OPEN, 2 = PROGRAMMED.
        let accounts = vec![(1u64, 100i64, 1u64), (3u64, -50i64, 2u64)];
        let links = vec![(1u64, 7u16, 3u64)];
        seg.save_snapshot(777, &accounts, &links).expect("save");
        assert!(seg.has_snapshot());

        let loaded = seg.load_snapshot().expect("load").expect("data present");
        assert_eq!(loaded.segment_id, seg.id());
        assert_eq!(
            loaded.next_account_id, 777,
            "v3 round-trips next_account_id"
        );
        assert_eq!(loaded.accounts, accounts, "balances + flags round-trip");
        assert_eq!(loaded.links, links, "links round-trip");
    }
}
