//! Raw WAL byte streaming for ADR-015 Cluster Mode leader shipping.
//!
//! The tailer is a cursor — it holds one open `File` handle and a byte
//! position. Each `tail()` call does a positional read (`pread`) of only the
//! new bytes and filters/compacts them in place. It never parses records
//! beyond checking the 1-byte kind tag and the 8-byte `tx_id` field.
//!
//! Rotation detection is inode-based: when the file we opened as `wal.bin`
//! is renamed to `wal_{id:06}.bin` by a segment rotation, our `File` still
//! points at the same inode. We detect this by comparing our stashed inode
//! with `stat(wal.bin)` and advance to the next segment file.

use crate::entities::{WalEntry, WalEntryKind};
use crate::storage::engine::Storage;
use crate::storage::layout::{active_wal_path, segment_wal_path};
use crate::storage::wal_serializer::parse_wal_record;
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Every WAL record is `#[repr(C)]` and exactly 40 bytes (entities.rs asserts).
pub const WAL_RECORD_SIZE: usize = 40;
/// `tx_id` offset shared by TxMetadata, TxEntry, TxLink.
const TX_ID_OFFSET: usize = 8;

/// Decode a buffer returned by [`WalTailer::tail`] into `WalEntry` values.
/// Malformed 40-byte records are skipped; extra trailing bytes are ignored.
pub fn decode_records(bytes: &[u8]) -> Vec<WalEntry> {
    let mut out = Vec::with_capacity(bytes.len() / WAL_RECORD_SIZE);
    let mut off = 0;
    while off + WAL_RECORD_SIZE <= bytes.len() {
        if let Ok(e) = parse_wal_record(&bytes[off..off + WAL_RECORD_SIZE]) {
            out.push(e);
        }
        off += WAL_RECORD_SIZE;
    }
    out
}

/// Stateful raw-byte WAL tailer. Holds one open `File` + byte position so
/// successive `tail()` calls pay only for the new bytes, not for re-scanning
/// the whole segment.
pub struct WalTailer {
    storage: Arc<Storage>,
    cursor: Option<Cursor>,
}

struct Cursor {
    /// The `from_tx_id` the caller is currently streaming for. A strictly
    /// smaller value on the next call triggers a re-seek.
    from_tx_id: u64,
    /// Open file handle to either `wal.bin` (when `is_active`) or
    /// `wal_{segment_id:06}.bin` (when sealed/closed).
    file: File,
    /// Inode of `file` at open time. Used to detect rotation of an active
    /// segment: if this differs from the current `stat(wal.bin).ino()`, our
    /// file has already been renamed to `wal_{segment_id:06}.bin` and we
    /// should advance once drained.
    inode: u64,
    /// Next byte offset to read from `file`.
    position: u64,
    /// Segment id of the currently-open file. For `is_active` cursors this
    /// is the `Storage::last_segment_id()` value at open time.
    segment_id: u32,
    /// True when the file was opened as `wal.bin`. Becomes irrelevant
    /// after we advance to the next segment.
    is_active: bool,
}

impl WalTailer {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage, cursor: None }
    }

    /// Reset the cursor; the next `tail()` re-seeks.
    pub fn reset(&mut self) {
        self.cursor = None;
    }

    /// Stream WAL bytes from `from_tx_id` into `buffer`; returns bytes written.
    /// Resumes from the cached cursor on a monotonic non-decreasing `from_tx_id`.
    pub fn tail(&mut self, from_tx_id: u64, buffer: &mut [u8]) -> u32 {
        let capacity = buffer.len() - (buffer.len() % WAL_RECORD_SIZE);
        if capacity == 0 {
            return 0;
        }

        // Seek on first call or regression; otherwise keep the existing
        // cursor. Either way, stamp the caller's `from_tx_id` onto the
        // cursor so the filter below uses the correct value — a freshly
        // seeded cursor starts at 0 and would otherwise leak structural
        // records when `from_tx_id > 0`.
        let regressed = self.cursor.as_ref().is_some_and(|c| from_tx_id < c.from_tx_id);
        if self.cursor.is_none() || regressed {
            if self.seek(from_tx_id).is_err() {
                return 0;
            }
        }
        if let Some(c) = self.cursor.as_mut() {
            c.from_tx_id = from_tx_id;
        }

        let mut written = 0usize;
        while written + WAL_RECORD_SIZE <= capacity {
            let Some(cursor) = self.cursor.as_mut() else { break };

            let file_len = match cursor.file.metadata() {
                Ok(m) => m.len(),
                Err(_) => break,
            };
            let available = file_len.saturating_sub(cursor.position);
            if available < WAL_RECORD_SIZE as u64 {
                if !self.advance_segment() {
                    break;
                }
                continue;
            }

            // Read as much as fits, rounded to whole records.
            let want = ((capacity - written) as u64).min(available);
            let want = (want / WAL_RECORD_SIZE as u64) * WAL_RECORD_SIZE as u64;
            if want == 0 {
                break;
            }
            let want = want as usize;

            let cursor = self.cursor.as_mut().unwrap();
            let dst = &mut buffer[written..written + want];
            let n = match cursor.file.read_at(dst, cursor.position) {
                Ok(n) => n,
                Err(_) => break,
            };
            let n_aligned = (n / WAL_RECORD_SIZE) * WAL_RECORD_SIZE;
            if n_aligned == 0 {
                break;
            }
            cursor.position += n_aligned as u64;

            // In-place filter: keep records whose kind has a tx_id and
            // tx_id >= from_tx_id, or structural records when from_tx_id==0.
            let from_tx_id = cursor.from_tx_id;
            let records = n_aligned / WAL_RECORD_SIZE;
            let mut w = 0usize;
            for r in 0..records {
                let src = r * WAL_RECORD_SIZE;
                let keep = record_matches(&dst[src..src + WAL_RECORD_SIZE], from_tx_id);
                if keep {
                    if w != r {
                        dst.copy_within(src..src + WAL_RECORD_SIZE, w * WAL_RECORD_SIZE);
                    }
                    w += 1;
                }
            }
            written += w * WAL_RECORD_SIZE;
        }

        written as u32
    }

    /// Position the cursor at the start of the segment most likely to contain
    /// `from_tx_id`. Starts at the active segment and walks backward through
    /// sealed segments until the first transactional record's tx_id is
    /// `<= from_tx_id` (or we run out of segments).
    fn seek(&mut self, from_tx_id: u64) -> std::io::Result<()> {
        self.open_active()?;
        if from_tx_id == 0 {
            return Ok(());
        }
        let data_dir = self.storage.config().data_dir.clone();
        let data_dir = Path::new(&data_dir);

        loop {
            let Some(cursor) = self.cursor.as_ref() else { return Ok(()); };
            match first_tx_id_in_file(&cursor.file) {
                Some(first) if first > from_tx_id => {
                    if cursor.segment_id <= 1 {
                        return Ok(());
                    }
                    let prev = cursor.segment_id - 1;
                    let prev_path = segment_wal_path(data_dir, prev);
                    if !prev_path.exists() {
                        return Ok(());
                    }
                    self.open_sealed(prev)?;
                }
                // `None` (no tx records in this file yet) — caller is asking
                // for tx_id in the future; nothing to do.
                _ => return Ok(()),
            }
        }
    }

    fn open_active(&mut self) -> std::io::Result<()> {
        let data_dir = self.storage.config().data_dir.clone();
        let path = active_wal_path(Path::new(&data_dir));
        self.open_file(&path, self.storage.last_segment_id(), true)
    }

    fn open_sealed(&mut self, id: u32) -> std::io::Result<()> {
        let data_dir = self.storage.config().data_dir.clone();
        let path = segment_wal_path(Path::new(&data_dir), id);
        self.open_file(&path, id, false)
    }

    fn open_file(&mut self, path: &PathBuf, segment_id: u32, is_active: bool) -> std::io::Result<()> {
        let file = File::open(path)?;
        let inode = file.metadata()?.ino();
        let carried_from_tx = self.cursor.as_ref().map_or(0, |c| c.from_tx_id);
        self.cursor = Some(Cursor {
            from_tx_id: carried_from_tx,
            file,
            inode,
            position: 0,
            segment_id,
            is_active,
        });
        Ok(())
    }

    /// Advance from the current segment to the next one. Returns `false` when
    /// there is no next segment yet (no new writes or no rotation).
    fn advance_segment(&mut self) -> bool {
        let Some(cursor) = self.cursor.as_ref() else { return false };
        let data_dir = self.storage.config().data_dir.clone();
        let data_dir = Path::new(&data_dir);
        let active_path = active_wal_path(data_dir);
        let current_id = cursor.segment_id;

        if cursor.is_active {
            // Still on `wal.bin`? Check if rotation stole our inode from under us.
            let on_disk_ino = std::fs::metadata(&active_path).ok().map(|m| m.ino());
            if on_disk_ino == Some(cursor.inode) {
                // No rotation; the writer simply has nothing new.
                return false;
            }
            // Our file is now a sealed segment with id == current_id.
            // Next segment: sealed(current_id+1) if it exists, else new active.
            let next = current_id + 1;
            let next_sealed = segment_wal_path(data_dir, next);
            if next_sealed.exists() {
                return self.open_sealed(next).is_ok();
            }
            if active_path.exists() {
                return self.open_active().is_ok();
            }
            false
        } else {
            // Reading a sealed segment; EOF means finished.
            let next = current_id + 1;
            let next_sealed = segment_wal_path(data_dir, next);
            if next_sealed.exists() {
                return self.open_sealed(next).is_ok();
            }
            if active_path.exists() {
                return self.open_active().is_ok();
            }
            false
        }
    }
}

/// First transactional record's `tx_id` in `file`, or `None` if there are
/// no tx records yet (e.g. a fresh segment with only a `SegmentHeader`).
fn first_tx_id_in_file(file: &File) -> Option<u64> {
    let mut buf = [0u8; WAL_RECORD_SIZE];
    let mut off = 0u64;
    loop {
        match file.read_at(&mut buf, off) {
            Ok(n) if n == WAL_RECORD_SIZE => {}
            _ => return None,
        }
        off += WAL_RECORD_SIZE as u64;
        let kind = buf[0];
        let has_tx = kind == WalEntryKind::TxMetadata as u8
            || kind == WalEntryKind::TxEntry as u8
            || kind == WalEntryKind::Link as u8;
        if has_tx {
            return Some(u64::from_le_bytes(buf[TX_ID_OFFSET..TX_ID_OFFSET + 8].try_into().ok()?));
        }
    }
}

#[inline]
fn record_matches(record: &[u8], from_tx_id: u64) -> bool {
    debug_assert_eq!(record.len(), WAL_RECORD_SIZE);
    let kind = record[0];
    let has_tx_id = kind == WalEntryKind::TxMetadata as u8
        || kind == WalEntryKind::TxEntry as u8
        || kind == WalEntryKind::Link as u8;

    if !has_tx_id {
        return from_tx_id == 0;
    }

    let tx_id = u64::from_le_bytes(
        record[TX_ID_OFFSET..TX_ID_OFFSET + 8]
            .try_into()
            .expect("WAL record is exactly 40 bytes"),
    );
    tx_id >= from_tx_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_matches_filters_structural_unless_from_zero() {
        let mut header = [0u8; WAL_RECORD_SIZE];
        header[0] = WalEntryKind::SegmentHeader as u8;
        assert!(record_matches(&header, 0));
        assert!(!record_matches(&header, 1));
    }

    #[test]
    fn record_matches_filters_by_tx_id() {
        let mut rec = [0u8; WAL_RECORD_SIZE];
        rec[0] = WalEntryKind::TxMetadata as u8;
        rec[TX_ID_OFFSET..TX_ID_OFFSET + 8].copy_from_slice(&5u64.to_le_bytes());
        assert!(record_matches(&rec, 0));
        assert!(record_matches(&rec, 5));
        assert!(!record_matches(&rec, 6));
    }
}
