//! Durable monotonic term log (ADR-016 scaffolding).
//!
//! `Term` tracks the cluster's current Raft-style term plus the `start_tx_id`
//! that opened each term. The current term lives in an `AtomicU64` for
//! lock-free reads; durable updates go through a `Mutex`-guarded append to
//! `{data_dir}/term.log` followed by `fdatasync`.
//!
//! Record layout — fixed 40 bytes, matching the WAL record size so tooling
//! can scan both with the same cadence:
//!
//! ```text
//!  offset  size  field
//!  0       8     u64 term              (LE)
//!  8       8     u64 start_tx_id       (LE)
//!  16      4     u32 magic             (0x4D524554 = b"TERM" LE)
//!  20      4     u32 crc32c of bytes[0..20]
//!  24      16    zero pad
//! ```
//!
//! The last 10 000 records are held in memory as a circular ring so
//! `get_term_at_tx` can answer without re-reading the file.

use crate::storage::Storage;
use spdlog::info;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub const TERM_RECORD_SIZE: usize = 40;
pub const TERM_MAGIC: u32 = u32::from_le_bytes(*b"TERM");
const RING_CAP: usize = 10_000;

/// One durable term record, as carried in the in-memory ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TermRecord {
    pub term: u64,
    pub start_tx_id: u64,
}

/// Lock-free reader + mutex-guarded writer for the durable term log.
pub struct Term {
    /// Current term. `Release`-stored after every durable append so readers
    /// observing a value are guaranteed that the corresponding record is on
    /// disk. Writers read this under the mutex to decide `current + 1`.
    current: AtomicU64,
    /// Serialises `fdatasync`-guarded appends. Holds the open file and the
    /// in-memory ring together so the ring only grows after a successful sync.
    writer: Mutex<Writer>,
}

struct Writer {
    file: File,
    ring: TermRing,
    path: PathBuf,
}

/// Fixed-capacity circular buffer of the last `RING_CAP` term records.
/// Older entries are silently dropped; `get_term_at_tx` then returns
/// `None` for transactions predating the ring.
struct TermRing {
    buf: Vec<TermRecord>,
    len: usize,
    head: usize,
}

impl TermRing {
    fn new() -> Self {
        Self {
            buf: vec![
                TermRecord {
                    term: 0,
                    start_tx_id: 0,
                };
                RING_CAP
            ],
            len: 0,
            head: 0,
        }
    }

    fn push(&mut self, rec: TermRecord) {
        let idx = (self.head + self.len) % RING_CAP;
        self.buf[idx] = rec;
        if self.len < RING_CAP {
            self.len += 1;
        } else {
            self.head = (self.head + 1) % RING_CAP;
        }
    }

    fn last(&self) -> Option<TermRecord> {
        if self.len == 0 {
            None
        } else {
            Some(self.buf[(self.head + self.len - 1) % RING_CAP])
        }
    }

    /// Largest record with `start_tx_id <= tx_id`. Returns `None` if
    /// every record in the ring started after `tx_id` or the ring is
    /// empty.
    fn find_covering(&self, tx_id: u64) -> Option<TermRecord> {
        if self.len == 0 {
            return None;
        }
        let mut best: Option<TermRecord> = None;
        for i in 0..self.len {
            let rec = self.buf[(self.head + i) % RING_CAP];
            if rec.start_tx_id <= tx_id && best.is_none_or(|b| rec.term >= b.term) {
                best = Some(rec);
            }
        }
        best
    }
}

impl Term {
    /// Open (or create) `{data_dir}/term.log` and load the tail of its
    /// records into memory. On first boot the file is empty and both
    /// `current` and the ring start at 0.
    pub fn open(storage: &Arc<Storage>) -> io::Result<Self> {
        Self::open_in_dir(&storage.config().data_dir)
    }

    /// Lower-level constructor that takes a raw directory path so tests
    /// and tooling can open a term log without a full `Storage`.
    pub fn open_in_dir(data_dir: &str) -> io::Result<Self> {
        let path = Path::new(data_dir).join("term.log");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let (ring, current) = load_existing(&mut file)?;
        info!(
            "term: opened {} (current={}, records_loaded={})",
            path.display(),
            current,
            ring.len
        );
        Ok(Self {
            current: AtomicU64::new(current),
            writer: Mutex::new(Writer { file, ring, path }),
        })
    }

    /// Current durable term. Lock-free, `Acquire`-ordered.
    #[inline]
    pub fn get_current_term(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    /// Largest durable term covering `tx_id` — returns
    /// `Some((term, start_tx_id))` where `start_tx_id <= tx_id` and
    /// `term` is the maximum in the ring satisfying that. `None` when
    /// `tx_id` predates the ring or the log is empty.
    pub fn get_term_at_tx(&self, tx_id: u64) -> Option<TermRecord> {
        let guard = self.writer.lock().expect("term: writer mutex poisoned");
        guard.ring.find_covering(tx_id)
    }

    /// Leader path: open a new term starting at `start_tx_id`.
    /// Blocking — takes the writer mutex, appends + fdatasyncs the new
    /// record, then publishes the new `current`. Returns the new term.
    pub fn new_term(&self, start_tx_id: u64) -> io::Result<u64> {
        let mut w = self.writer.lock().expect("term: writer mutex poisoned");
        let next = self
            .current
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| io::Error::other("term: u64 overflow"))?;
        let rec = TermRecord {
            term: next,
            start_tx_id,
        };
        durable_append(&mut w.file, rec)?;
        w.ring.push(rec);
        self.current.store(next, Ordering::Release);
        info!(
            "term: opened term {} at start_tx_id={} ({})",
            next,
            start_tx_id,
            w.path.display()
        );
        Ok(next)
    }

    /// Follower path: record `term` observed from an incoming
    /// AppendEntries. Idempotent — no-op when `term == current`.
    /// Blocking when the term is genuinely new (same code path as
    /// `new_term`). A `term` older than current is rejected with
    /// `InvalidInput` since terms must be monotonic.
    pub fn observe(&self, term: u64, start_tx_id: u64) -> io::Result<()> {
        let current = self.current.load(Ordering::Acquire);
        if term == current {
            return Ok(());
        }
        if term < current {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("term regression: incoming={} current={}", term, current),
            ));
        }

        let mut w = self.writer.lock().expect("term: writer mutex poisoned");
        // Re-read under the mutex in case another observer raced us to
        // the same term — idempotency at the durable layer.
        let current = self.current.load(Ordering::Acquire);
        if term <= current {
            return Ok(());
        }

        let rec = TermRecord { term, start_tx_id };
        durable_append(&mut w.file, rec)?;
        w.ring.push(rec);
        self.current.store(term, Ordering::Release);
        info!(
            "term: observed term {} at start_tx_id={} ({})",
            term,
            start_tx_id,
            w.path.display()
        );
        Ok(())
    }

    /// Inspect the last persisted record. Useful for recovery /
    /// observability; holds the writer mutex briefly.
    pub fn last_record(&self) -> Option<TermRecord> {
        self.writer
            .lock()
            .expect("term: writer mutex poisoned")
            .ring
            .last()
    }
}

// ── file helpers ───────────────────────────────────────────────────────────

fn encode_record(rec: TermRecord) -> [u8; TERM_RECORD_SIZE] {
    let mut buf = [0u8; TERM_RECORD_SIZE];
    buf[0..8].copy_from_slice(&rec.term.to_le_bytes());
    buf[8..16].copy_from_slice(&rec.start_tx_id.to_le_bytes());
    buf[16..20].copy_from_slice(&TERM_MAGIC.to_le_bytes());
    let crc = crc32c::crc32c(&buf[0..20]);
    buf[20..24].copy_from_slice(&crc.to_le_bytes());
    // Bytes 24..40 stay zero as pad.
    buf
}

fn decode_record(bytes: &[u8]) -> io::Result<TermRecord> {
    if bytes.len() != TERM_RECORD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "term: record is not 40 bytes",
        ));
    }
    let magic = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    if magic != TERM_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("term: magic mismatch (got {:#010x})", magic),
        ));
    }
    let stored_crc = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    let actual_crc = crc32c::crc32c(&bytes[0..20]);
    if stored_crc != actual_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "term: crc mismatch (stored={:#010x} actual={:#010x})",
                stored_crc, actual_crc
            ),
        ));
    }
    Ok(TermRecord {
        term: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        start_tx_id: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
    })
}

/// Append `rec` to `file` and fdatasync. Caller must already hold the
/// writer mutex so appends are serialised.
fn durable_append(file: &mut File, rec: TermRecord) -> io::Result<()> {
    file.seek(SeekFrom::End(0))?;
    let bytes = encode_record(rec);
    file.write_all(&bytes)?;
    file.sync_data()?;
    Ok(())
}

/// Read every record from `file` into a fresh ring, dropping older
/// entries once the ring reaches `RING_CAP`. Returns the tail term so
/// callers can seed the atomic. A corrupt or partial trailing record
/// stops the scan gracefully (file truncated or crash mid-append).
fn load_existing(file: &mut File) -> io::Result<(TermRing, u64)> {
    file.seek(SeekFrom::Start(0))?;
    let mut ring = TermRing::new();
    let mut buf = [0u8; TERM_RECORD_SIZE];
    let mut current = 0u64;
    loop {
        match file.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        match decode_record(&buf) {
            Ok(rec) => {
                current = rec.term.max(current);
                ring.push(rec);
            }
            Err(_) => break,
        }
    }
    file.seek(SeekFrom::End(0))?;
    Ok((ring, current))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_in_tmp() -> (TempDir, Term) {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        let term = Term::open_in_dir(&dir).unwrap();
        (td, term)
    }

    #[test]
    fn fresh_log_starts_at_zero() {
        let (_td, term) = open_in_tmp();
        assert_eq!(term.get_current_term(), 0);
        assert_eq!(term.last_record(), None);
        assert_eq!(term.get_term_at_tx(100), None);
    }

    #[test]
    fn new_term_increments_and_persists() {
        let (_td, term) = open_in_tmp();
        assert_eq!(term.new_term(0).unwrap(), 1);
        assert_eq!(term.new_term(10).unwrap(), 2);
        assert_eq!(term.new_term(25).unwrap(), 3);
        assert_eq!(term.get_current_term(), 3);
        assert_eq!(
            term.last_record(),
            Some(TermRecord {
                term: 3,
                start_tx_id: 25
            })
        );
    }

    #[test]
    fn get_term_at_tx_returns_covering_record() {
        let (_td, term) = open_in_tmp();
        term.new_term(0).unwrap(); // term 1 starts at 0
        term.new_term(50).unwrap(); // term 2 starts at 50
        term.new_term(100).unwrap(); // term 3 starts at 100

        assert_eq!(
            term.get_term_at_tx(5),
            Some(TermRecord {
                term: 1,
                start_tx_id: 0
            })
        );
        assert_eq!(
            term.get_term_at_tx(49),
            Some(TermRecord {
                term: 1,
                start_tx_id: 0
            })
        );
        assert_eq!(
            term.get_term_at_tx(75),
            Some(TermRecord {
                term: 2,
                start_tx_id: 50
            })
        );
        assert_eq!(
            term.get_term_at_tx(999),
            Some(TermRecord {
                term: 3,
                start_tx_id: 100
            })
        );
    }

    #[test]
    fn observe_is_idempotent_and_monotonic() {
        let (_td, term) = open_in_tmp();
        term.observe(5, 0).unwrap();
        assert_eq!(term.get_current_term(), 5);
        // Same term → no-op, no error.
        term.observe(5, 0).unwrap();
        // Larger term → advances.
        term.observe(7, 100).unwrap();
        assert_eq!(term.get_current_term(), 7);
        // Older term → InvalidInput.
        let err = term.observe(6, 200).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn reopen_recovers_current_term() {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        {
            let term = Term::open_in_dir(&dir).unwrap();
            term.new_term(0).unwrap();
            term.new_term(10).unwrap();
            term.observe(5, 20).unwrap(); // should no-op (5 < 2? 5 > 2 so it advances)
            assert_eq!(term.get_current_term(), 5);
        }
        let reopened = Term::open_in_dir(&dir).unwrap();
        assert_eq!(reopened.get_current_term(), 5);
        assert_eq!(
            reopened.last_record(),
            Some(TermRecord {
                term: 5,
                start_tx_id: 20
            })
        );
    }

    #[test]
    fn ring_caps_at_capacity() {
        // Direct ring exercise — avoids 10_000+ real fdatasyncs.
        let mut ring = TermRing::new();
        let over = RING_CAP + 500;
        for i in 1..=(over as u64) {
            ring.push(TermRecord {
                term: i,
                start_tx_id: i,
            });
        }
        assert_eq!(ring.len, RING_CAP);
        assert_eq!(
            ring.buf[ring.head].term,
            (over - RING_CAP + 1) as u64,
            "oldest slot should hold term = over - cap + 1"
        );
        assert_eq!(ring.last().unwrap().term, over as u64);
    }
}
