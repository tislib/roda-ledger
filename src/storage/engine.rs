use crate::config::StorageConfig;
use crate::storage::function_snapshot::{self, FunctionSnapshotData, FunctionSnapshotRecord};
use crate::storage::layout::{active_wal_path, parse_segment_id};
use crate::storage::wal_tail::WalTailer;
use crate::storage::{Segment, SegmentStaus};
use spdlog::info;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

    /// Build a fresh [`WalTailer`] bound to this storage. Each call yields
    /// an independent cursor.
    pub fn wal_tailer(self: &Arc<Self>) -> WalTailer {
        WalTailer::new(self.clone())
    }

    /// Physically remove every WAL byte (and dependent snapshot file)
    /// whose `tx_id > watermark`. Boot-time mechanism behind
    /// [`crate::ledger::Ledger::start_with_recovery_until`] (ADR-0016 §9).
    ///
    /// **Iteration order: newest segment first.** Truncation only ever
    /// affects the *tail* of the on-disk log, so working from the
    /// active segment down through the closed segments lets us
    /// short-circuit as soon as we hit a segment whose every record
    /// is at or below `watermark` — earlier segments are too.
    ///
    /// Each segment is classified into one of three buckets via
    /// [`Segment::first_tx_id_in_wal_data`] / [`Segment::last_tx_id_in_wal_data`]:
    /// - **Fully ≤ `watermark`** (`last_tx ≤ watermark`, including
    ///   header-only segments where `last_tx = 0`): leave alone, walk
    ///   terminates.
    /// - **Fully > `watermark`** (`first_tx > watermark`): delete
    ///   every artefact (`wal_{id}.{bin,crc,seal}`,
    ///   `wal_index_{id}.bin`, `account_index_{id}.bin`,
    ///   `snapshot_{id}.{bin,crc}`,
    ///   `function_snapshot_{id}.{bin,crc}`). The walk continues —
    ///   older segments may also be fully past or may straddle.
    /// - **Straddles `watermark`** (`first_tx ≤ watermark < last_tx`):
    ///   byte-truncate the file at the first `Metadata > watermark`
    ///   offset and discard the snapshot pair (it captured state past
    ///   the watermark and is no longer trustworthy). Walk
    ///   terminates — older segments are entirely below by
    ///   construction.
    ///
    /// **Sealed segments are immutable.** ADR-0016 §10's seal-watermark
    /// gate (`Ledger::set_seal_watermark`) guarantees a sealed segment
    /// only ever contains cluster-committed transactions, so its last
    /// `tx_id` is always at or below any future recovery watermark
    /// (the cluster watermark is monotonically non-decreasing). If
    /// the truncation logic ever needs to mutate a sealed segment
    /// (delete or byte-truncate), the cluster invariant is broken
    /// and we return `InvalidData` rather than silently rewriting
    /// durable committed history.
    ///
    /// After all file ops, `last_segment_id` is recomputed from the
    /// surviving `wal_NNNNNN.bin` files so a follow-up `recover_until`
    /// or `recover` opens the correct active segment.
    ///
    /// Calling this **on a running ledger is undefined**: the active
    /// segment's mutable state inside the WAL stage is not
    /// coordinated with these file-system mutations. Callers must
    /// ensure no pipeline thread is touching the segment when this
    /// runs — which is exactly the situation in
    /// `start_with_recovery_until`, pre-`start`.
    pub fn truncate_wal_above(&self, watermark: u64) -> Result<(), std::io::Error> {
        let data_dir = self.config.data_dir.clone();
        let data_dir_path = Path::new(&data_dir);

        // ── Pass 1: classify, no mutation. ────────────────────────────
        //
        // Walk newest-first to build a plan (`Action` per segment) and
        // bail out *before* any disk mutation if a sealed segment
        // would need to be touched. This makes `truncate_wal_above`
        // atomic w.r.t. the cluster invariant (ADR-0016 §10): on a
        // broken-invariant input, the data dir is left exactly as we
        // found it.
        enum Action {
            DeleteActive { active_id: u32 },
            TruncateActive { active_id: u32, offset: usize },
            DeleteClosed { seg_id: u32 },
            TruncateClosed { idx: usize, seg_id: u32, offset: usize },
        }

        let mut closed_segs = self.list_all_segments()?;
        // list_all_segments returns ascending; we walk them tail-first.
        closed_segs.reverse();

        let active_path = active_wal_path(data_dir_path);
        let mut active: Option<Segment> = if active_path.exists() {
            Some(self.active_segment().map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("truncate_wal_above: failed to open active segment: {}", e),
                )
            })?)
        } else {
            None
        };

        // Iterate (active first, then closed segments newest → oldest)
        // and decide what to do with each.
        let mut plan: Vec<Action> = Vec::new();
        let mut continue_after_active = true;

        // Closure-style "classify one segment" used twice.
        let classify =
            |seg: &mut Segment| -> Result<(u64, u64), std::io::Error> {
                seg.load().map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!(
                            "truncate_wal_above: failed to load segment {}: {}",
                            seg.id(),
                            e
                        ),
                    )
                })?;
                let last_tx = seg.last_tx_id_in_wal_data()?;
                let first_tx = seg.first_tx_id_in_wal_data()?;
                Ok((first_tx, last_tx))
            };

        if let Some(active) = active.as_mut() {
            let active_id = active.id();
            let (first_tx, last_tx) = classify(active)?;

            if last_tx == 0 || last_tx <= watermark {
                // Active is fully ≤ watermark. If it has any tx of its
                // own (last_tx > 0), older closed are too → don't
                // continue. If it's empty/header-only (last_tx == 0),
                // we can't conclude — keep walking closed.
                if last_tx > 0 {
                    continue_after_active = false;
                }
            } else if first_tx == 0 || first_tx > watermark {
                // Fully past — delete. Older segments may also need
                // work, so don't short-circuit.
                plan.push(Action::DeleteActive { active_id });
            } else {
                // Straddles — truncate, then short-circuit.
                let off = active
                    .locate_tx_watermark(watermark)?
                    .expect("locate_tx_watermark returns Some when last_tx > watermark");
                plan.push(Action::TruncateActive {
                    active_id,
                    offset: off,
                });
                continue_after_active = false;
            }
        }

        if continue_after_active {
            for (idx, seg) in closed_segs.iter_mut().enumerate() {
                let seg_id = seg.id();
                let (first_tx, last_tx) = classify(seg)?;

                if last_tx == 0 || last_tx <= watermark {
                    // Fully ≤ watermark — older are too, stop.
                    break;
                }

                let is_fully_past = first_tx == 0 || first_tx > watermark;

                // Invariant check — same wording as the runtime gate
                // so the operator log makes ADR-0016 §10 explicit.
                if seg.status() == SegmentStaus::SEALED {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "truncate_wal_above: sealed segment {} would need {} \
                             at watermark={} (first_tx={}, last_tx={}). \
                             Sealed segments must hold only cluster-committed transactions \
                             (see seal_watermark / ADR-0016 §10); reaching this state \
                             indicates a broken cluster invariant.",
                            seg_id,
                            if is_fully_past { "deletion" } else { "byte truncation" },
                            watermark,
                            first_tx,
                            last_tx
                        ),
                    ));
                }

                if is_fully_past {
                    let _ = idx; // not needed for Delete
                    plan.push(Action::DeleteClosed { seg_id });
                } else {
                    let off = seg
                        .locate_tx_watermark(watermark)?
                        .expect("locate_tx_watermark returns Some when last_tx > watermark");
                    plan.push(Action::TruncateClosed {
                        idx,
                        seg_id,
                        offset: off,
                    });
                    break; // older segments are below by construction
                }
            }
        }

        // ── Pass 2: apply the plan. ────────────────────────────────────
        for action in plan {
            match action {
                Action::DeleteActive { active_id } => {
                    info!(
                        "truncate_wal_above: removing active wal.bin (segment {}, entirely past watermark={})",
                        active_id, watermark
                    );
                    // Drop the open file handle before unlinking on
                    // hosts where that matters (Windows-friendly).
                    let _ = active.take();
                    std::fs::remove_file(&active_path).map_err(|e| {
                        std::io::Error::new(
                            e.kind(),
                            format!("truncate_wal_above: failed to remove wal.bin: {}", e),
                        )
                    })?;
                    Segment::delete_snapshot_files_for_segment(&data_dir, active_id)?;
                }
                Action::TruncateActive { active_id, offset } => {
                    let active = active
                        .as_mut()
                        .expect("Active segment must still be in scope for truncation");
                    info!(
                        "truncate_wal_above: truncating active wal.bin from {} to {} bytes (watermark={})",
                        active.wal_data_len(),
                        offset,
                        watermark
                    );
                    active.truncate_wal(offset as u64)?;
                    Segment::delete_snapshot_files_for_segment(&data_dir, active_id)?;
                }
                Action::DeleteClosed { seg_id } => {
                    info!(
                        "truncate_wal_above: deleting segment {} (entirely past watermark={})",
                        seg_id, watermark
                    );
                    Segment::delete_all_files_for_segment(&data_dir, seg_id)?;
                }
                Action::TruncateClosed { idx, seg_id, offset } => {
                    let seg = &mut closed_segs[idx];
                    info!(
                        "truncate_wal_above: truncating segment {} from {} to {} bytes (watermark={})",
                        seg_id,
                        seg.wal_data_len(),
                        offset,
                        watermark
                    );
                    seg.truncate_wal(offset as u64)?;
                    Segment::delete_snapshot_files_for_segment(&data_dir, seg_id)?;
                }
            }
        }

        // ── Recompute last_segment_id from surviving sealed/closed files ──
        let mut surviving_ids: Vec<u32> = std::fs::read_dir(&data_dir)
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!(
                        "truncate_wal_above: failed to re-read data dir at {}: {}",
                        data_dir, e
                    ),
                )
            })?
            .filter_map(|e| e.ok())
            .filter_map(|e| parse_segment_id(&e.file_name().to_string_lossy()))
            .collect();
        surviving_ids.sort();
        let new_last = surviving_ids.last().copied().unwrap_or(0) + 1;
        self.last_segment_id.store(new_last, Ordering::Release);

        info!(
            "truncate_wal_above: complete (watermark={}, last_segment_id now={})",
            watermark, new_last
        );
        Ok(())
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
    pub fn write_function(&self, name: &str, version: u16, binary: &[u8]) -> std::io::Result<u32> {
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
    pub fn load_function_snapshot(&self, segment_id: u32) -> std::io::Result<FunctionSnapshotData> {
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
