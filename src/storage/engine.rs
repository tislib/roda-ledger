use crate::config::StorageConfig;
use crate::storage::Segment;
use crate::storage::function_snapshot::{
    self, FunctionSnapshotData, FunctionSnapshotRecord,
};
use crate::storage::layout::parse_segment_id;
use spdlog::info;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

/// Snapshot file magic: "SNAP" = 0x534E4150
pub const SNAPSHOT_MAGIC: u32 = 0x534E4150;

pub struct Storage {
    config: StorageConfig,
    last_segment_id: AtomicU32,
}

impl Storage {
    pub fn new(config: StorageConfig) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to create data directory at {}: {}",
                    config.data_dir, e
                ),
            )
        })?;

        // Ensure {data_dir}/functions exists at startup so subsequent
        // write_function / read_function calls can assume it is present.
        let functions_dir = Path::new(&config.data_dir).join("functions");
        std::fs::create_dir_all(&functions_dir).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to create functions directory at {}: {}",
                    functions_dir.display(),
                    e
                ),
            )
        })?;

        let mut segment_ids: Vec<u32> = std::fs::read_dir(&config.data_dir)
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!(
                        "failed to read data directory at {}: {}",
                        config.data_dir, e
                    ),
                )
            })?
            .filter_map(|e| e.ok())
            .filter_map(|e| parse_segment_id(&e.file_name().to_string_lossy()))
            .collect();
        segment_ids.sort();

        let last_segment_id = AtomicU32::new(segment_ids.last().copied().unwrap_or(0) + 1);

        Ok(Self {
            config,
            last_segment_id,
        })
    }

    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    pub fn active_segment(&self) -> Result<Segment, std::io::Error> {
        let last_segment_id = self.last_segment_id();

        Segment::open_active(self.config.data_dir.clone(), last_segment_id)
    }

    pub fn segment(&self, segment_id: u32) -> Result<Segment, std::io::Error> {
        Segment::open(self.config.data_dir.to_string(), segment_id)
    }

    pub fn list_all_segments(&self) -> Result<Vec<Segment>, std::io::Error> {
        let mut segments: Vec<Segment> = std::fs::read_dir(&self.config.data_dir)
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!(
                        "failed to read data directory at {}: {}",
                        self.config.data_dir, e
                    ),
                )
            })?
            .filter_map(|e| e.ok())
            .filter_map(|e| parse_segment_id(&e.file_name().to_string_lossy()))
            .map(|id| self.segment(id))
            .filter_map(|r| r.ok())
            .collect();

        segments.sort_by_key(|s| s.id());

        Ok(segments)
    }

    pub fn last_segment_id(&self) -> u32 {
        self.last_segment_id.load(Ordering::Acquire)
    }

    pub fn next_segment(&self) {
        self.last_segment_id.fetch_add(1, Ordering::AcqRel);
    }

    // ─── WASM function binaries (ADR-014) ──────────────────────────────────
    //
    // All `{data_dir}/functions/*.wasm` I/O is hosted here so callers go
    // through `&Storage` and never see raw paths. The directory is
    // created at `Storage::new` time.

    /// `{data_dir}/functions/{name}_v{version}.wasm`.
    fn function_binary_path(&self, name: &str, version: u16) -> PathBuf {
        Path::new(&self.config.data_dir)
            .join("functions")
            .join(format!("{name}_v{version}.wasm"))
    }

    /// Atomically write a WASM binary under the versioned path (write to
    /// a temp file in the same directory, then rename). Returns the
    /// CRC32C of the bytes written.
    pub fn write_function(
        &self,
        name: &str,
        version: u16,
        binary: &[u8],
    ) -> std::io::Result<u32> {
        // Defensive create — the constructor already made this, but tests
        // that bypass Storage::new rely on it.
        let dir = Path::new(&self.config.data_dir).join("functions");
        std::fs::create_dir_all(&dir)?;
        let path = self.function_binary_path(name, version);
        let tmp = path.with_extension("wasm.tmp");
        std::fs::write(&tmp, binary)?;
        std::fs::rename(&tmp, &path)?;
        Ok(crc32c::crc32c(binary))
    }

    /// Read a WASM binary by `(name, version)`.
    pub fn read_function(&self, name: &str, version: u16) -> std::io::Result<Vec<u8>> {
        std::fs::read(self.function_binary_path(name, version))
    }

    /// Truncate the on-disk binary to 0 bytes — do not delete, the
    /// audit trail is preserved. `Ok(())` if the file did not exist.
    pub fn truncate_function(&self, name: &str, version: u16) -> std::io::Result<()> {
        let path = self.function_binary_path(name, version);
        if !path.exists() {
            return Ok(());
        }
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)?;
        Ok(())
    }

    // ─── Function-registry snapshot (ADR-014) ──────────────────────────────

    /// Atomically write a function-registry snapshot paired to
    /// `segment_id` (same trigger as the balance snapshot).
    pub fn save_function_snapshot(
        &self,
        segment_id: u32,
        last_tx_id: u64,
        records: &[FunctionSnapshotRecord],
    ) -> std::io::Result<()> {
        function_snapshot::save(
            Path::new(&self.config.data_dir),
            segment_id,
            last_tx_id,
            records,
        )
    }

    /// Load and validate the function snapshot written at `segment_id`.
    pub fn load_function_snapshot(
        &self,
        segment_id: u32,
    ) -> std::io::Result<FunctionSnapshotData> {
        function_snapshot::load(Path::new(&self.config.data_dir), segment_id)
    }

    /// List segment ids that have a `function_snapshot_{N}.bin` file on
    /// disk, ascending. Recovery uses the last entry to locate the most
    /// recent function snapshot; everything after it is reconstructed
    /// by WAL replay of `FunctionRegistered` records.
    pub fn list_function_snapshot_ids(&self) -> Result<Vec<u32>, std::io::Error> {
        let dir = &self.config.data_dir;
        if !std::path::Path::new(dir).exists() {
            return Ok(Vec::new());
        }
        let mut ids: Vec<u32> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name();
                let name = name.to_str()?;
                let stripped = name.strip_prefix("function_snapshot_")?;
                let stripped = stripped.strip_suffix(".bin")?;
                if stripped.len() == 6 && stripped.chars().all(|c| c.is_ascii_digit()) {
                    stripped.parse::<u32>().ok()
                } else {
                    None
                }
            })
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        if self.config.temporary {
            info!("Cleaning up temporary storage...");
            std::fs::remove_dir_all(&self.config.data_dir).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
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

    #[test]
    fn write_read_function_roundtrip() {
        let (storage, _td) = temp_storage();
        let bytes = b"\x00asm\x01\x00\x00\x00".to_vec();
        let crc = storage.write_function("foo", 1, &bytes).unwrap();
        assert_eq!(crc, crc32c::crc32c(&bytes));
        let read = storage.read_function("foo", 1).unwrap();
        assert_eq!(read, bytes);
    }

    #[test]
    fn truncate_function_preserves_file_at_zero_bytes() {
        let (storage, _td) = temp_storage();
        storage.write_function("bar", 3, b"hello").unwrap();
        storage.truncate_function("bar", 3).unwrap();
        let path = storage.function_binary_path("bar", 3);
        let meta = std::fs::metadata(path).unwrap();
        assert_eq!(meta.len(), 0);
    }

    #[test]
    fn truncate_missing_function_is_ok() {
        let (storage, _td) = temp_storage();
        storage.truncate_function("none", 1).unwrap();
    }

    #[test]
    fn read_specific_function_version() {
        // Read API only needs (name, version) — no on-disk discovery;
        // callers always know which version they want from the WAL or
        // function snapshot.
        let (storage, _td) = temp_storage();
        storage.write_function("fee", 1, b"v1").unwrap();
        storage.write_function("fee", 2, b"v2").unwrap();
        assert_eq!(storage.read_function("fee", 1).unwrap(), b"v1");
        assert_eq!(storage.read_function("fee", 2).unwrap(), b"v2");
    }

    #[test]
    fn function_binary_path_is_under_functions_subdir() {
        let (storage, _td) = temp_storage();
        let p = storage.function_binary_path("my_fn", 7);
        assert!(p.ends_with("functions/my_fn_v7.wasm"));
        assert!(p.starts_with(&storage.config.data_dir));
    }

    #[test]
    fn list_function_snapshot_ids_is_sorted_and_filtered() {
        let (storage, _td) = temp_storage();
        assert!(storage.list_function_snapshot_ids().unwrap().is_empty());

        storage.save_function_snapshot(3, 0, &[]).unwrap();
        storage.save_function_snapshot(1, 0, &[]).unwrap();
        storage.save_function_snapshot(2, 0, &[]).unwrap();

        assert_eq!(storage.list_function_snapshot_ids().unwrap(), vec![1, 2, 3]);
    }
}
