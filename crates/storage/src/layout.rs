use std::path::{Path, PathBuf};
// ── WAL file paths ────────────────────────────────────────────────────────────

/// Returns the path of the active WAL file: `{data_dir}/wal.bin`.
pub fn active_wal_path(data_dir: &Path) -> PathBuf {
    data_dir.join("wal.bin")
}

/// Returns the path of the WAL stop marker: `{data_dir}/wal.stop`.
/// Presence indicates a clean shutdown; absence with an existing `wal.bin`
/// signals a crash.
pub fn wal_stop_path(data_dir: &Path) -> PathBuf {
    data_dir.join("wal.stop")
}

/// Returns the path of a sealed WAL segment binary: `{data_dir}/wal_{id:06}.bin`.
pub fn segment_wal_path(data_dir: &Path, id: u32) -> PathBuf {
    data_dir.join(format!("wal_{:06}.bin", id))
}

/// Returns the path of a WAL segment CRC sidecar: `{data_dir}/wal_{id:06}.crc`.
pub fn segment_crc_path(data_dir: &Path, id: u32) -> PathBuf {
    data_dir.join(format!("wal_{:06}.crc", id))
}

/// Returns the path of a WAL segment seal marker: `{data_dir}/wal_{id:06}.seal`.
pub fn segment_seal_path(data_dir: &Path, id: u32) -> PathBuf {
    data_dir.join(format!("wal_{:06}.seal", id))
}

// ── Index file paths (ADR-008) ───────────────────────────────────────────

/// Returns the path of a transaction index: `{data_dir}/wal_index_{id:06}.bin`.
pub fn segment_tx_index_path(data_dir: &Path, id: u32) -> PathBuf {
    data_dir.join(format!("wal_index_{:06}.bin", id))
}

/// Returns the path of an account index: `{data_dir}/account_index_{id:06}.bin`.
pub fn segment_account_index_path(data_dir: &Path, id: u32) -> PathBuf {
    data_dir.join(format!("account_index_{:06}.bin", id))
}

// ── Snapshot file paths ───────────────────────────────────────────────────────

/// Returns the path of a snapshot binary: `{data_dir}/snapshot_{id:06}.bin`.
pub fn snapshot_bin_path(data_dir: &Path, id: u32) -> PathBuf {
    data_dir.join(format!("snapshot_{:06}.bin", id))
}

/// Returns the path of a snapshot CRC sidecar: `{data_dir}/snapshot_{id:06}.crc`.
pub fn snapshot_crc_path(data_dir: &Path, id: u32) -> PathBuf {
    data_dir.join(format!("snapshot_{:06}.crc", id))
}

/// Returns the path of a snapshot binary temp file: `{data_dir}/snapshot_{id:06}.bin.tmp`.
pub fn snapshot_bin_tmp_path(data_dir: &Path, id: u32) -> PathBuf {
    data_dir.join(format!("snapshot_{:06}.bin.tmp", id))
}

/// Returns the path of a snapshot CRC sidecar temp file: `{data_dir}/snapshot_{id:06}.crc.tmp`.
pub fn snapshot_crc_tmp_path(data_dir: &Path, id: u32) -> PathBuf {
    data_dir.join(format!("snapshot_{:06}.crc.tmp", id))
}

// ── File name parsers ─────────────────────────────────────────────────────────

/// Parses `wal_NNNNNN.bin` → `Some(NNNNNN)`, anything else → `None`.
pub fn parse_segment_id(name: &str) -> Option<u32> {
    let name = name.strip_prefix("wal_")?.strip_suffix(".bin")?;
    if name.len() == 6 && name.chars().all(|c| c.is_ascii_digit()) {
        name.parse().ok()
    } else {
        None
    }
}
