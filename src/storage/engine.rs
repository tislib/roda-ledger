use crate::config::StorageConfig;
use crate::storage::Segment;
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

        // ADR-014: ensure {data_dir}/functions exists at startup so that the
        // `storage::functions` and `storage::function_snapshot` modules can
        // assume the directory is present.
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

    /// Root data directory for this storage engine. Every other on-disk
    /// path (segments, snapshots, function binaries, function snapshots) is
    /// resolved underneath this directory.
    pub fn data_dir(&self) -> &Path {
        Path::new(&self.config.data_dir)
    }

    /// ADR-014: directory holding `{name}_v{N}.wasm` binaries. Always
    /// `{data_dir}/functions`. Callers in `storage::functions` and
    /// `storage::function_snapshot` go through this accessor instead of
    /// constructing the path themselves.
    pub fn functions_dir(&self) -> PathBuf {
        self.data_dir().join("functions")
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
}

impl Drop for Storage {
    fn drop(&mut self) {
        if self.config.temporary {
            info!("Cleaning up temporary storage...");
            std::fs::remove_dir_all(&self.config.data_dir).ok();
        }
    }
}
