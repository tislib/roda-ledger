//! Raw WAL byte streaming for ADR-015 Cluster Mode leader shipping.
//!
//! The tailer is a cursor — it holds one open `File` handle and a byte
//! position. The starting `from_tx_id` is set once at construction (via
//! [`Storage::wal_tailer`] / [`crate::WalTailer::new`]); the constructor
//! walks back through sealed segments to the one containing `from_tx_id`
//! and scans forward inside it to position the cursor at the first
//! `TxMetadata` whose `tx_id >= from_tx_id`. Subsequent [`tail`](WalTailer::tail)
//! calls do a positional read (`pread`) of new bytes from the current
//! cursor and advance the cursor — there is no re-filtering and the
//! caller does not pass `from_tx_id` again.
//!
//! Rotation detection is inode-based: when the file we opened as `wal.bin`
//! is renamed to `wal_{id:06}.bin` by a segment rotation, our `File` still
//! points at the same inode. We detect this by comparing our stashed inode
//! with `stat(wal.bin)` and advance to the next segment file.

use crate::engine::Storage;
use crate::entities::{WalEntry, WalEntryKind};
use crate::layout::{active_wal_path, segment_wal_path};
use crate::wal_serializer::parse_wal_record;
use crate::wal_zero_copy::{WalEntryRef, iter_records};
use spdlog::{debug, trace};
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::constants::{TX_ID_OFFSET, WAL_RECORD_SIZE};

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

/// Stateful raw-byte WAL tailer. Holds one open `File` + byte position
/// so successive [`tail`](WalTailer::tail) calls only pay for new
/// bytes. The starting `from_tx_id` is bound at construction and the
/// cursor is pre-positioned at the first `TxMetadata` with `tx_id >=
/// from_tx_id`; the tail loop is purely "read-from-cursor, advance".
pub struct WalTailer {
    storage: Arc<Storage>,
    /// The `from_tx_id` this tailer was built for — kept so
    /// [`reset`](WalTailer::reset) can re-locate without the caller
    /// having to pass it again.
    from_tx_id: u64,
    cursor: Option<Cursor>,
}

struct Cursor {
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
    /// Build a tailer positioned at the first `TxMetadata` whose
    /// `tx_id >= from_tx_id`. After construction, [`tail`](Self::tail)
    /// just reads forward from the cursor.
    ///
    /// Locate semantics:
    /// - `from_tx_id == 0` → open the active segment, position 0 (no
    ///   walk-back through sealed segments — preserves the "from current
    ///   active" sentinel the cluster heartbeat path relied on).
    /// - `from_tx_id > 0` → walk back through sealed segments until we
    ///   reach the one whose first `TxMetadata` is `<= from_tx_id`,
    ///   then scan forward inside it to land on the first record with
    ///   `tx_id >= from_tx_id`.
    ///
    /// Failures during locate (missing file, IO error) leave the cursor
    /// `None`; subsequent tails return 0.
    pub fn new(storage: Arc<Storage>, from_tx_id: u64) -> Self {
        let mut t = Self {
            storage,
            from_tx_id,
            cursor: None,
        };
        let _ = t.locate();
        t
    }

    /// Re-run the initial locate. Useful for tests that want to rewind a
    /// tailer back to its construction-time position without rebuilding it.
    pub fn reset(&mut self) {
        self.cursor = None;
        let _ = self.locate();
    }

    /// Stream WAL bytes into `buffer` starting from the cursor; returns
    /// the number of bytes written. The cursor advances by that amount,
    /// so the next call resumes immediately after.
    ///
    /// The returned slice is always trimmed to a whole-transaction
    /// boundary. If the read ends inside a `TxMetadata + followers`
    /// group whose final follower hasn't been written yet, that
    /// partial group is excluded from the count and the cursor is
    /// rewound to its start — the next `tail()` will pick it up once
    /// the writer has appended the missing followers. This prevents
    /// shipping torn groups to a peer (the bug class that produced
    /// `[meta_X, follower_X1, meta_X, …]` corruption on followers
    /// when the leader's tailer split a tx).
    ///
    /// **Buffer-size contract.** `buffer` must be large enough to
    /// hold at least one complete tx group `(meta + sub_item_count
    /// followers)` end-to-end. If it isn't, the trim path will
    /// reject every read as partial and `tail()` will return `0` in
    /// a loop without making progress. Production callers
    /// (replication driver) size `buffer` from
    /// `append_entries_max_bytes` (4 MiB default) which dwarfs any
    /// real tx group. Tests can drop as low as `40 * (1 +
    /// max_sub_item_count)` bytes.
    pub fn tail(&mut self, buffer: &mut [u8]) -> u32 {
        let capacity = buffer.len() - (buffer.len() % WAL_RECORD_SIZE);
        if capacity == 0 || self.cursor.is_none() {
            return 0;
        }

        let mut written = 0usize;
        while written + WAL_RECORD_SIZE <= capacity {
            let Some(cursor) = self.cursor.as_mut() else {
                break;
            };

            let file_len = match cursor.file.metadata() {
                Ok(m) => m.len(),
                Err(_) => break,
            };
            let available = file_len.saturating_sub(cursor.position);
            if available < WAL_RECORD_SIZE as u64 {
                // DIAG-flake-replication: snapshot the cursor before the
                // advance attempt so we can attribute the result.
                let pre_segment_id = cursor.segment_id;
                let pre_is_active = cursor.is_active;
                let pre_position = cursor.position;
                let pre_file_len = file_len;
                if !self.advance_segment() {
                    trace!(
                        "DIAG-flake-replication: tailer.tail advance_segment=false \
                         from_tx_id={} segment_id={} is_active={} position={} file_len={}",
                        self.from_tx_id, pre_segment_id, pre_is_active, pre_position, pre_file_len,
                    );
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
            written += n_aligned;
        }

        // Trim a trailing partial tx group, if any. The bytes after
        // `complete_end` are a `TxMetadata` whose followers haven't all
        // arrived yet (or, defensively, a stray follower without a
        // preceding metadata — also discarded). Rewind the cursor so
        // those bytes are re-read on the next call.
        let complete_end = last_complete_tx_end(&buffer[..written]);
        if complete_end < written {
            let partial = (written - complete_end) as u64;
            if let Some(c) = self.cursor.as_mut() {
                c.position -= partial;
            }
            written = complete_end;
        }

        written as u32
    }

    /// Open the segment that holds `self.from_tx_id` (walking back
    /// through sealed segments if needed) and advance the cursor's
    /// `position` to the first `TxMetadata` with `tx_id >= from_tx_id`.
    ///
    /// Idempotent — safe to call from [`reset`](Self::reset).
    fn locate(&mut self) -> std::io::Result<()> {
        // DIAG-flake-replication: capture pre-seek state so we can see
        // both the path being opened and the storage's view of the
        // current segment id.
        let data_dir_for_diag = self.storage.config().data_dir.clone();
        let active_path_for_diag = active_wal_path(Path::new(&data_dir_for_diag));
        let last_segment_id_for_diag = self.storage.last_segment_id();
        debug!(
            "DIAG-flake-replication: tailer.locate BEGIN from_tx_id={} \
             active_path={:?} last_segment_id={}",
            self.from_tx_id, active_path_for_diag, last_segment_id_for_diag
        );

        self.open_active()?;
        if self.from_tx_id == 0 {
            debug!(
                "DIAG-flake-replication: tailer.locate END from_tx_id=0 (no walk-back) \
                 segment_id={} is_active={}",
                self.cursor.as_ref().map_or(0, |c| c.segment_id),
                self.cursor.as_ref().is_some_and(|c| c.is_active),
            );
            return Ok(());
        }
        let data_dir = self.storage.config().data_dir.clone();
        let data_dir = Path::new(&data_dir);

        loop {
            let Some(cursor) = self.cursor.as_ref() else {
                debug!(
                    "DIAG-flake-replication: tailer.locate END cursor=None from_tx_id={}",
                    self.from_tx_id
                );
                return Ok(());
            };
            let first_tx_in_active = first_tx_id_in_file(&cursor.file);
            match first_tx_in_active {
                Some(first) if first > self.from_tx_id => {
                    if cursor.segment_id <= 1 {
                        debug!(
                            "DIAG-flake-replication: tailer.locate END walk-back stopped \
                             at segment_id<=1 (segment_id={}, first_tx_in_segment={}, \
                             from_tx_id={})",
                            cursor.segment_id, first, self.from_tx_id
                        );
                        break;
                    }
                    let prev = cursor.segment_id - 1;
                    let prev_path = segment_wal_path(data_dir, prev);
                    if !prev_path.exists() {
                        debug!(
                            "DIAG-flake-replication: tailer.locate END walk-back missing \
                             prev segment file {:?} (from_tx_id={})",
                            prev_path, self.from_tx_id
                        );
                        break;
                    }
                    debug!(
                        "DIAG-flake-replication: tailer.locate walking back to sealed \
                         segment {} (current first={}, from_tx_id={})",
                        prev, first, self.from_tx_id
                    );
                    self.open_sealed(prev)?;
                }
                // `None` (no tx records in this file yet) — caller is asking
                // for tx_id in the future; or `Some(first) <= from_tx_id` —
                // this is the segment we want.
                other => {
                    debug!(
                        "DIAG-flake-replication: tailer.locate END parking on segment \
                         from_tx_id={} segment_id={} is_active={} \
                         first_tx_in_segment={:?}",
                        self.from_tx_id, cursor.segment_id, cursor.is_active, other,
                    );
                    break;
                }
            }
        }

        // Scan the located segment forward to find the byte offset of
        // the first `TxMetadata` with `tx_id >= from_tx_id`; that's our
        // tail start point. If no such record exists yet, we park at
        // end-of-file and `tail()` will return 0 until the writer
        // appends one.
        let Some(cursor) = self.cursor.as_mut() else {
            return Ok(());
        };
        cursor.position = first_meta_offset_at_or_after(&cursor.file, self.from_tx_id);
        Ok(())
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

    fn open_file(
        &mut self,
        path: &PathBuf,
        segment_id: u32,
        is_active: bool,
    ) -> std::io::Result<()> {
        let file = File::open(path)?;
        let inode = file.metadata()?.ino();
        self.cursor = Some(Cursor {
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
        let Some(cursor) = self.cursor.as_ref() else {
            return false;
        };
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

/// Walks `bytes` as a sequence of 40-byte WAL records and returns the
/// byte offset immediately after the last complete `TxMetadata +
/// followers` group. Bytes from this offset to `bytes.len()` are a
/// trailing partial group — either a `TxMetadata` whose followers are
/// still pending on disk, or (defensively) a stray non-metadata record
/// with no preceding metadata.
///
/// Uses [`iter_records`] for record decoding so the structural rules
/// (metadata starts a group, `sub_item_count` followers complete it)
/// live in one place. Returns `0` if no complete group is present.
fn last_complete_tx_end(bytes: &[u8]) -> usize {
    let mut last_complete_end: usize = 0;
    let mut in_group = false;
    let mut pending_followers: u16 = 0;

    for (idx, rec) in iter_records(bytes).enumerate() {
        let end_off = (idx + 1) * WAL_RECORD_SIZE;
        match rec {
            WalEntryRef::Metadata(m) => {
                in_group = true;
                pending_followers = m.sub_item_count;
                if pending_followers == 0 {
                    last_complete_end = end_off;
                    in_group = false;
                }
            }
            WalEntryRef::Entry(_)
            | WalEntryRef::Link(_)
            | WalEntryRef::FunctionRegistered(_)
            | WalEntryRef::Term(_) => {
                if !in_group {
                    // Orphan follower — the cursor invariant says this
                    // shouldn't happen, but if it does we refuse to ship
                    // it and stop trimming here.
                    break;
                }
                pending_followers = pending_followers.saturating_sub(1);
                if pending_followers == 0 {
                    last_complete_end = end_off;
                    in_group = false;
                }
            }
        }
    }

    last_complete_end
}

/// First `TxMetadata.tx_id` in `file`, or `None` if there are no
/// metadata records yet (an empty active segment, or one populated
/// only with structural / follower records — pre-refactor this also
/// honoured `TxEntry` / `TxLink` but those no longer carry tx_id).
fn first_tx_id_in_file(file: &File) -> Option<u64> {
    let mut buf = [0u8; WAL_RECORD_SIZE];
    let mut off = 0u64;
    loop {
        match file.read_at(&mut buf, off) {
            Ok(n) if n == WAL_RECORD_SIZE => {}
            _ => return None,
        }
        off += WAL_RECORD_SIZE as u64;
        if buf[0] == WalEntryKind::TxMetadata as u8 {
            return Some(u64::from_le_bytes(
                buf[TX_ID_OFFSET..TX_ID_OFFSET + 8].try_into().ok()?,
            ));
        }
    }
}

/// Byte offset within `file` of the first `TxMetadata` whose
/// `tx_id >= from_tx_id`. Used by [`WalTailer::locate`] to position
/// the cursor so subsequent `tail()` calls return only records the
/// caller is interested in — no in-line filtering required.
///
/// Returns the file length when no such metadata exists (yet); the
/// cursor parks at EOF and reads zero bytes until the writer appends
/// a new transaction.
fn first_meta_offset_at_or_after(file: &File, from_tx_id: u64) -> u64 {
    const CHUNK: usize = 1 << 22;       // 4 MiB = 100k records / read
    let mut buf = vec![0u8; CHUNK];
    let mut off: u64 = 0;
    loop {
        let n = match file.read_at(&mut buf, off) {
            Ok(0) | Err(_) => return off,
            Ok(n) => n - (n % WAL_RECORD_SIZE),
        };
        if n == 0 { return off; }
        // walk the in-memory chunk using iter_records (zero-copy)
        for (i, rec) in iter_records(&buf[..n]).enumerate() {
            if let WalEntryRef::Metadata(m) = rec
                && m.tx_id >= from_tx_id
            {
                return off + (i * WAL_RECORD_SIZE) as u64;
            }
        }
        off += n as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::StorageConfig;
    use crate::entities::{
        EntryKind, FailReason, TxEntry, TxLink, TxLinkKind, TxMetadata, TxTerm, WalEntry,
    };
    use crate::wal_serializer::serialize_wal_records;
    use std::io::Write;

    /// Build a `TxMetadata` whose `sub_item_count` matches the number
    /// of follower records the test writes immediately after it.
    /// `tail()` trims trailing partial groups (meta whose declared
    /// followers haven't all landed), so each test fixture must be
    /// structurally consistent on this count.
    fn meta(tx_id: u64, sub_item_count: u16) -> WalEntry {
        WalEntry::Metadata(TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            fail_reason: FailReason::NONE,
            sub_item_count,
            crc32c: 0,
            tx_id,
            timestamp: 0,
            user_ref: 0,
            tag: [0; 8],
        })
    }
    fn entry(_tx_id: u64) -> WalEntry {
        // The `_tx_id` arg is kept for callsite readability; the field
        // is no longer stored on `TxEntry` (it lives only on the
        // preceding `TxMetadata`).
        WalEntry::Entry(TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind: EntryKind::Credit,
            _pad0: [0; 6],
            _pad1: [0; 8],
            account_id: 1,
            amount: 1,
            computed_balance: 1,
        })
    }
    fn link(_tx_id: u64) -> WalEntry {
        WalEntry::Link(TxLink {
            entry_type: WalEntryKind::Link as u8,
            link_kind: TxLinkKind::Duplicate as u8,
            _pad: [0; 6],
            _pad1: [0; 8],
            to_tx_id: 0,
            _pad2: [0; 16],
        })
    }
    fn term() -> WalEntry {
        WalEntry::Term(TxTerm {
            entry_type: WalEntryKind::TxTerm as u8,
            _pad0: [0; 7],
            term: 1,
            node_id: 1,
            node_count: 1,
            node_voted: 1,
            _pad1: [0; 12],
        })
    }

    fn write_segment(path: &std::path::Path, entries: &[WalEntry]) {
        let mut f = std::fs::File::create(path).unwrap();
        for e in entries {
            f.write_all(serialize_wal_records(e)).unwrap();
        }
        f.sync_all().unwrap();
    }

    fn open_storage(dir: &tempfile::TempDir) -> Arc<crate::engine::Storage> {
        let cfg = StorageConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            ..StorageConfig::default()
        };
        Arc::new(crate::engine::Storage::new(cfg).unwrap())
    }

    #[test]
    fn tail_bridges_sealed_segment_to_active() {
        // segment 1 (sealed): tx=1 = meta(sub=3) + 3 followers — a
        // complete group.
        // active (wal.bin): tx=2 = meta(sub=2) + 2 followers — also
        // complete. The cursor must position past sealed and emit
        // exactly tx=2's three records without leaking any tx=1.
        let dir = tempfile::tempdir().unwrap();
        write_segment(
            &segment_wal_path(dir.path(), 1),
            &[meta(1, 3), entry(1), link(1), term()],
        );
        write_segment(
            &active_wal_path(dir.path()),
            &[meta(2, 2), entry(2), link(2)],
        );

        let storage = open_storage(&dir);
        let mut tailer = storage.wal_tailer(2);
        let mut buf = vec![0u8; 64 * WAL_RECORD_SIZE];

        // from_tx_id=2 should yield exactly the three tx=2 records.
        let n = tailer.tail(&mut buf) as usize;
        assert_eq!(n, 3 * WAL_RECORD_SIZE, "expected 3 records for tx=2");
        let decoded = decode_records(&buf[..n]);
        assert_eq!(decoded.len(), 3);
        // First record must be the TxMetadata carrying tx_id=2; the
        // follower records belong to it implicitly.
        assert!(
            matches!(decoded[0], WalEntry::Metadata(m) if m.tx_id == 2),
            "expected first record to be TxMetadata(tx_id=2), got {:?}",
            decoded[0]
        );
    }

    #[test]
    fn tail_returns_records_spanning_segments() {
        // segment 1 (sealed): tx=1 (meta + entry) + tx=2 (meta + entry
        // + term). Two complete groups.
        // active (wal.bin): tx=3 (meta + entry + link). Complete group.
        // from_tx_id=2 must cross the boundary and emit tx=2 (sealed)
        // + tx=3 (active).
        let dir = tempfile::tempdir().unwrap();
        write_segment(
            &segment_wal_path(dir.path(), 1),
            &[meta(1, 1), entry(1), meta(2, 2), entry(2), term()],
        );
        write_segment(
            &active_wal_path(dir.path()),
            &[meta(3, 2), entry(3), link(3)],
        );

        let storage = open_storage(&dir);
        let mut tailer = storage.wal_tailer(2);
        let mut buf = vec![0u8; 64 * WAL_RECORD_SIZE];

        let n = tailer.tail(&mut buf) as usize;
        let decoded = decode_records(&buf[..n]);
        // Expected: TxMetadata(2), TxEntry(2), TxTerm (inherits 2),
        // TxMetadata(3), TxEntry(3), TxLink(3) = 6 records.
        assert_eq!(decoded.len(), 6, "got {:?}", decoded);
        // First three are tx=2 (TxEntry/TxTerm inherit tx_id from the
        // preceding TxMetadata).
        assert!(matches!(decoded[0], WalEntry::Metadata(m) if m.tx_id == 2));
        assert!(matches!(decoded[1], WalEntry::Entry(_)));
        assert!(matches!(decoded[2], WalEntry::Term(_)));
        // Then tx=3.
        assert!(matches!(decoded[3], WalEntry::Metadata(m) if m.tx_id == 3));
    }

    #[test]
    fn tail_trims_partial_trailing_group() {
        // active (wal.bin): tx=1 complete + tx=2 partial (meta
        // declares 3 followers, only 1 has been written so far).
        // tail() must return only tx=1's records and rewind the
        // cursor to the start of tx=2's metadata; the next call
        // (after the writer appends the missing followers) sees the
        // complete group.
        let dir = tempfile::tempdir().unwrap();
        write_segment(
            &active_wal_path(dir.path()),
            &[meta(1, 1), entry(1), meta(2, 3), entry(2)],
        );

        let storage = open_storage(&dir);
        let mut tailer = storage.wal_tailer(1);
        let mut buf = vec![0u8; 64 * WAL_RECORD_SIZE];

        let n = tailer.tail(&mut buf) as usize;
        assert_eq!(
            n,
            2 * WAL_RECORD_SIZE,
            "expected only tx=1's 2 records; partial tx=2 must be trimmed",
        );
        let decoded = decode_records(&buf[..n]);
        assert!(matches!(decoded[0], WalEntry::Metadata(m) if m.tx_id == 1));
        assert!(matches!(decoded[1], WalEntry::Entry(_)));

        // A subsequent call with nothing new appended returns 0 (the
        // partial is still partial). The cursor sits at the partial
        // metadata's offset, ready to re-read once it's complete.
        let again = tailer.tail(&mut buf) as usize;
        assert_eq!(again, 0);
    }
}
