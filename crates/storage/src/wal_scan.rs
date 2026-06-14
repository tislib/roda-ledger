//! Backward WAL scanning — the newest→oldest twin of [`crate::WalTailer`].
//!
//! Reads each transaction's trailing `TxMetadata`, jumps back over its
//! `sub_item_count` followers, and crosses segments backward. A [`ChunkReader`]
//! buffers a 4 MiB window per `pread` (like the tailer) and decodes records
//! zero-copy via [`crate::wal_zero_copy`]; an owned `WalEntry` is built only for
//! the followers of a matching transaction.

use crate::constants::WAL_RECORD_SIZE;
use crate::engine::Storage;
use crate::entities::{CommittedTransaction, TxMetadata, WalEntry};
use crate::layout::{active_wal_path, segment_wal_path};
use crate::wal_tail::decode_records;
use crate::wal_zero_copy::{WalEntryRef, iter_records, read_entry};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

const REC: u64 = WAL_RECORD_SIZE as u64;
/// Backward read window: 4 MiB ≈ 100k records per `pread`, matching the
/// tailer's `TAIL_ENTRIES_BUF_BYTES` so the two paths amortize I/O alike.
const CHUNK: u64 = 1 << 22;

/// Backward WAL scanner. Cheap to construct; each `scan*` call runs an
/// independent newest→oldest sweep bounded by its own `from_tx_id` + `max_scan`.
pub struct WalScanner {
    storage: Arc<Storage>,
}

impl WalScanner {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    /// Scan transactions newest→oldest from `from_tx_id` (`0` = newest),
    /// examining at most `max_scan` records (`0` = to the WAL start). For every
    /// transaction whose followers contain an entry satisfying `matcher`, invoke
    /// `handler` with the whole [`CommittedTransaction`]; `handler` returns
    /// `false` to stop early. Returns `scan_last_tx_id` — the oldest tx_id
    /// examined (`0` if none); pass it back as `from_tx_id` to continue.
    pub fn scan<M, H>(&self, from_tx_id: u64, max_scan: u64, matcher: M, mut handler: H) -> u64
    where
        M: Fn(WalEntryRef) -> bool,
        H: FnMut(CommittedTransaction) -> bool,
    {
        let data_dir = self.storage.config().data_dir.clone();
        let data_dir = Path::new(&data_dir);
        let mut scanned: u64 = 0;
        let mut scan_last: u64 = 0;

        let (mut segment_id, mut is_active) = self.locate_start(data_dir, from_tx_id);
        loop {
            let path = if is_active {
                active_wal_path(data_dir)
            } else {
                segment_wal_path(data_dir, segment_id)
            };
            if let Some(mut reader) = File::open(&path).ok().and_then(ChunkReader::new) {
                let mut cursor = reader.file_len;
                while cursor >= REC {
                    if max_scan != 0 && scanned >= max_scan {
                        return scan_last;
                    }
                    let meta = match reader.meta_at(cursor) {
                        Some(m) => m,
                        // Stray follower or in-flight partial tail — step back one record.
                        None => {
                            cursor -= REC;
                            scanned += 1;
                            continue;
                        }
                    };
                    let n = meta.sub_item_count as u64;
                    let tx_bytes = (n + 1) * REC;
                    if tx_bytes > cursor {
                        break; // malformed: claims more followers than precede it.
                    }
                    let tx_start = cursor - tx_bytes;
                    cursor = tx_start;
                    scanned += n + 1;

                    if from_tx_id != 0 && meta.tx_id > from_tx_id {
                        continue; // newer than the window start.
                    }
                    scan_last = meta.tx_id;
                    // Materialize the owned entries only on a match.
                    if reader.followers_match(tx_start, n, &matcher) {
                        let entries = reader.materialize_followers(tx_start, n);
                        if !handler(CommittedTransaction { meta, entries }) {
                            return scan_last;
                        }
                    }
                }
            }
            if segment_id <= 1 {
                break;
            }
            segment_id -= 1;
            is_active = false;
        }
        scan_last
    }

    /// Newest matching transaction at/below `from_tx_id` (stops at the first).
    pub fn scan_single<M>(
        &self,
        from_tx_id: u64,
        max_scan: u64,
        matcher: M,
    ) -> Option<CommittedTransaction>
    where
        M: Fn(WalEntryRef) -> bool,
    {
        let mut out = None;
        self.scan(from_tx_id, max_scan, matcher, |tx| {
            out = Some(tx);
            false
        });
        out
    }

    /// All matching transactions, newest→oldest (bounded by `max_scan`).
    pub fn scan_all<M>(
        &self,
        from_tx_id: u64,
        max_scan: u64,
        matcher: M,
    ) -> Vec<CommittedTransaction>
    where
        M: Fn(WalEntryRef) -> bool,
    {
        let mut out = Vec::new();
        self.scan(from_tx_id, max_scan, matcher, |tx| {
            out.push(tx);
            true
        });
        out
    }

    /// The newest segment that can hold `from_tx_id`: walk back from the active
    /// segment while its first `tx_id` exceeds `from_tx_id`. `from_tx_id == 0`
    /// starts at the active segment.
    fn locate_start(&self, data_dir: &Path, from_tx_id: u64) -> (u32, bool) {
        let active_id = self.storage.last_segment_id();
        if from_tx_id == 0 {
            return (active_id, true);
        }
        let mut segment_id = active_id;
        let mut is_active = true;
        loop {
            let path = if is_active {
                active_wal_path(data_dir)
            } else {
                segment_wal_path(data_dir, segment_id)
            };
            let first = File::open(&path).ok().and_then(|f| first_tx_id(&f));
            match first {
                Some(first) if first > from_tx_id && segment_id > 1 => {
                    segment_id -= 1;
                    is_active = false;
                }
                _ => return (segment_id, is_active),
            }
        }
    }
}

/// Buffered backward reader over one segment file: a sliding window that
/// refills only when a request falls outside it — one `pread` per [`CHUNK`].
struct ChunkReader {
    file: File,
    /// File length rounded down to a whole number of records.
    file_len: u64,
    buf: Vec<u8>,
    /// File offset of `buf[0]`.
    buf_start: u64,
}

impl ChunkReader {
    fn new(file: File) -> Option<Self> {
        let len = file.metadata().ok()?.len();
        Some(Self {
            file,
            file_len: len - (len % REC),
            buf: Vec::new(),
            buf_start: 0,
        })
    }

    /// Slice for `[lo, hi)`, refilling a ≥[`CHUNK`] window ending at `hi` if it
    /// isn't buffered. Callers guarantee `lo < hi <= file_len`.
    fn read(&mut self, lo: u64, hi: u64) -> Option<&[u8]> {
        let buf_end = self.buf_start + self.buf.len() as u64;
        if self.buf.is_empty() || lo < self.buf_start || hi > buf_end {
            let want = (hi - lo).max(CHUNK);
            let start = hi.saturating_sub(want);
            self.buf.resize((hi - start) as usize, 0);
            self.file.read_exact_at(&mut self.buf, start).ok()?;
            self.buf_start = start;
        }
        let a = (lo - self.buf_start) as usize;
        let b = (hi - self.buf_start) as usize;
        Some(&self.buf[a..b])
    }

    /// Owned copy of the metadata record ending at `end`, or `None` if that
    /// record isn't a `TxMetadata` (a stray follower / in-flight tail).
    fn meta_at(&mut self, end: u64) -> Option<TxMetadata> {
        match read_entry(self.read(end - REC, end)?) {
            Ok(WalEntryRef::Metadata(m)) => Some(*m),
            _ => None,
        }
    }

    /// Whether any of the `n` followers beginning at `tx_start` satisfies
    /// `matcher` — decoded zero-copy, no allocation.
    fn followers_match<M: Fn(WalEntryRef) -> bool>(
        &mut self,
        tx_start: u64,
        n: u64,
        matcher: &M,
    ) -> bool {
        if n == 0 {
            return false;
        }
        match self.read(tx_start, tx_start + n * REC) {
            Some(buf) => iter_records(buf).any(matcher),
            None => false,
        }
    }

    /// Owned `WalEntry` list for the `n` followers beginning at `tx_start`.
    fn materialize_followers(&mut self, tx_start: u64, n: u64) -> Vec<WalEntry> {
        match self.read(tx_start, tx_start + n * REC) {
            Some(buf) => decode_records(buf),
            None => Vec::new(),
        }
    }
}

/// First `TxMetadata.tx_id` in `file` (chunked forward scan), or `None` if it
/// holds no metadata yet. Used only by `locate_start`, once per segment.
fn first_tx_id(file: &File) -> Option<u64> {
    let len = file.metadata().ok()?.len();
    let file_len = len - (len % REC);
    let mut buf = vec![0u8; CHUNK.min(file_len.max(REC)) as usize];
    let mut off = 0u64;
    while off < file_len {
        let want = (file_len - off).min(CHUNK) as usize;
        buf.resize(want, 0);
        file.read_exact_at(&mut buf, off).ok()?;
        for rec in iter_records(&buf) {
            if let WalEntryRef::Metadata(m) = rec {
                return Some(m.tx_id);
            }
        }
        off += want as u64;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use crate::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntryKind};
    use crate::wal_serializer::serialize_wal_records;
    use std::io::Write;

    fn meta(tx_id: u64, sub: u16) -> WalEntry {
        WalEntry::Metadata(TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            fail_reason: FailReason::NONE,
            sub_item_count: sub,
            crc32c: 0,
            tx_id,
            timestamp: 0,
            user_ref: 0,
            tag: [0; 8],
        })
    }
    fn entry(account_id: u64) -> WalEntry {
        WalEntry::Entry(TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind: EntryKind::CREDIT,
            _pad0: [0; 6],
            _pad1: [0; 8],
            account_id,
            amount: 1,
            computed_balance: 1,
        })
    }
    fn write_segment(path: &std::path::Path, entries: &[WalEntry]) {
        let mut f = std::fs::File::create(path).unwrap();
        for e in entries {
            f.write_all(serialize_wal_records(e)).unwrap();
        }
        f.sync_all().unwrap();
    }
    fn open_storage(dir: &tempfile::TempDir) -> Arc<Storage> {
        let cfg = StorageConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            ..StorageConfig::default()
        };
        Arc::new(Storage::new(cfg).unwrap())
    }
    fn on_account(account_id: u64) -> impl Fn(WalEntryRef) -> bool {
        move |e| matches!(e, WalEntryRef::Entry(te) if te.account_id == account_id)
    }
    fn ids(txs: &[CommittedTransaction]) -> Vec<u64> {
        txs.iter().map(|t| t.meta.tx_id).collect()
    }

    #[test]
    fn scans_newest_to_oldest_filtered_by_matcher() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(
            &active_wal_path(dir.path()),
            &[
                entry(5),
                meta(1, 1), // tx1 touches acct 5
                entry(7),
                meta(2, 1), // tx2 touches acct 7
                entry(5),
                meta(3, 1), // tx3 touches acct 5
            ],
        );
        let storage = open_storage(&dir);

        assert_eq!(
            ids(&storage.wal_scanner().scan_all(0, 0, on_account(5))),
            vec![3, 1]
        );
        assert_eq!(
            storage
                .wal_scanner()
                .scan_single(0, 0, on_account(5))
                .unwrap()
                .meta
                .tx_id,
            3
        );
        assert_eq!(
            ids(&storage.wal_scanner().scan_all(0, 0, on_account(7))),
            vec![2]
        );
        let tx3 = storage
            .wal_scanner()
            .scan_single(0, 0, on_account(5))
            .unwrap();
        assert_eq!(tx3.entries.len(), 1);
    }

    #[test]
    fn from_tx_id_skips_newer_transactions() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(
            &active_wal_path(dir.path()),
            &[
                entry(5),
                meta(1, 1),
                entry(5),
                meta(2, 1),
                entry(5),
                meta(3, 1),
            ],
        );
        let storage = open_storage(&dir);
        assert_eq!(
            ids(&storage.wal_scanner().scan_all(2, 0, on_account(5))),
            vec![2, 1]
        );
    }

    #[test]
    fn scans_backward_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(&segment_wal_path(dir.path(), 1), &[entry(5), meta(1, 1)]);
        write_segment(&active_wal_path(dir.path()), &[entry(5), meta(2, 1)]);
        let storage = open_storage(&dir);
        assert_eq!(
            ids(&storage.wal_scanner().scan_all(0, 0, on_account(5))),
            vec![2, 1]
        );
    }

    #[test]
    fn scan_returns_oldest_tx_examined_and_max_scan_bounds() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(
            &active_wal_path(dir.path()),
            &[
                entry(5),
                meta(1, 1),
                entry(5),
                meta(2, 1),
                entry(5),
                meta(3, 1),
            ],
        );
        let storage = open_storage(&dir);
        // Unbounded: scan_last is the oldest tx (1).
        let last = storage.wal_scanner().scan(0, 0, on_account(5), |_| true);
        assert_eq!(last, 1);
        // max_scan = 2 records → only tx3 (2 records) examined; scan_last = 3.
        let last2 = storage.wal_scanner().scan(0, 2, on_account(5), |_| true);
        assert_eq!(last2, 3);
    }

    #[test]
    fn no_match_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(&active_wal_path(dir.path()), &[entry(5), meta(1, 1)]);
        let storage = open_storage(&dir);
        assert!(
            storage
                .wal_scanner()
                .scan_all(0, 0, on_account(99))
                .is_empty()
        );
    }

    #[test]
    fn multi_follower_transactions_decode_whole_groups() {
        // Two followers per tx (e.g. debit + credit). The scanner must jump back
        // exactly sub_item_count followers and return them all.
        let dir = tempfile::tempdir().unwrap();
        write_segment(
            &active_wal_path(dir.path()),
            &[
                entry(5),
                entry(0),
                meta(1, 2), // tx1: debit acct5, credit SYSTEM
                entry(7),
                entry(0),
                meta(2, 2), // tx2: acct7
            ],
        );
        let storage = open_storage(&dir);
        let txs = storage.wal_scanner().scan_all(0, 0, on_account(5));
        assert_eq!(ids(&txs), vec![1]);
        assert_eq!(txs[0].entries.len(), 2, "both followers returned");
    }

    #[test]
    fn scans_many_transactions_across_chunk_window() {
        // Enough transactions that the backward sweep refills its buffered
        // window at least once, exercising the cross-CHUNK boundary path.
        let dir = tempfile::tempdir().unwrap();
        let count = 50_000u64; // 100k records ≈ 4 MB > CHUNK boundary effects
        let mut records = Vec::with_capacity((count * 2) as usize);
        for tx_id in 1..=count {
            records.push(entry(if tx_id == 1 { 5 } else { 9 }));
            records.push(meta(tx_id, 1));
        }
        write_segment(&active_wal_path(dir.path()), &records);
        let storage = open_storage(&dir);

        // Only tx1 touches account 5; it sits at the very oldest end.
        let txs = storage.wal_scanner().scan_all(0, 0, on_account(5));
        assert_eq!(ids(&txs), vec![1]);
        // The sweep reached the oldest tx examined.
        let last = storage.wal_scanner().scan(0, 0, on_account(9), |_| true);
        assert_eq!(last, 1);
    }
}
