//! Durable term log (file layer).
//!
//! `TermStorage` owns `{data_dir}/term.log`: fixed 40-byte records laid
//! out to mirror the WAL record size so tooling can scan both with the
//! same cadence. Callers (typically `cluster::Term`) handle the
//! in-memory semantics — this file is just "open, scan, append, sync".
//! `append` and `sync` are deliberately separate so test seeds can
//! batch a single fdatasync after a bulk write while production paths
//! call `sync` after every record.
//!
//! Record layout:
//!
//! ```text
//!  offset  size  field
//!  0       8     u64 term              (LE)
//!  8       8     u64 start_tx_id       (LE)
//!  16      4     u32 magic             (b"TERM" LE)
//!  20      4     u32 crc32c of bytes[0..20]
//!  24      16    zero pad
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const TERM_RECORD_SIZE: usize = 40;
pub const TERM_MAGIC: u32 = u32::from_le_bytes(*b"TERM");

/// One durable term record, shared between the storage layer and the
/// cluster-level semantic wrapper.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TermRecord {
    pub term: u64,
    pub start_tx_id: u64,
}

/// File-backed term log. Holds an append-open `File` and the file path.
/// Writers `append` records and call `sync` to fdatasync; every read
/// is a positional scan.
pub struct TermStorage {
    file: File,
    path: PathBuf,
}

impl TermStorage {
    /// Open (or create) `{data_dir}/term.log`.
    pub fn open(data_dir: &str) -> io::Result<Self> {
        let path = Path::new(data_dir).join("term.log");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        Ok(Self { file, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Scan every record currently on disk from the head. Stops cleanly
    /// on a short tail or a malformed record (crash mid-append).
    pub fn scan<F>(&mut self, mut visit: F) -> io::Result<()>
    where
        F: FnMut(TermRecord),
    {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; TERM_RECORD_SIZE];
        loop {
            match self.file.read_exact(&mut buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            match decode_record(&buf) {
                Ok(rec) => visit(rec),
                Err(_) => break,
            }
        }
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    /// Cold-path lookup: scan the whole file and return the record whose
    /// `start_tx_id` is the greatest value `<= tx_id`. Used by
    /// `cluster::Term` when the in-memory ring has already rotated past
    /// the requested transaction.
    pub fn cold_lookup(&mut self, tx_id: u64) -> io::Result<Option<TermRecord>> {
        let mut best: Option<TermRecord> = None;
        self.scan(|rec| {
            if rec.start_tx_id <= tx_id {
                best = match best {
                    Some(b) if b.term >= rec.term => Some(b),
                    _ => Some(rec),
                };
            }
        })?;
        Ok(best)
    }

    /// Append `rec` to the file (no fdatasync — call `sync` for
    /// durability). Caller must serialise concurrent appenders
    /// externally (the cluster layer uses a mutex).
    pub fn append(&mut self, rec: TermRecord) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        let bytes = encode_record(rec);
        self.file.write_all(&bytes)?;
        Ok(())
    }

    /// `fdatasync` previously appended records. Production callers
    /// invoke this after every `append` to guarantee durability before
    /// returning to the client; bulk seeds in tests call it once after
    /// the loop.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Drop every record whose `start_tx_id > tx_id`, preserving the rest.
    ///
    /// Atomicity contract (ADR-0017 §"Persistence trait contract"): the
    /// rewrite uses rename-based replacement so a crash mid-truncate
    /// leaves the original file intact. Steps:
    ///
    /// 1. Scan `term.log` and stage kept records into `term.log.tmp`.
    /// 2. `fsync` the tmp file.
    /// 3. `rename(tmp, term.log)` — POSIX-atomic on the same filesystem.
    /// 4. `fsync` the parent directory (best-effort on macOS) so the
    ///    rename itself is durable.
    /// 5. Reopen `self.file` against the new inode.
    pub fn truncate_after(&mut self, tx_id: u64) -> io::Result<()> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| io::Error::other("term: term.log has no parent directory"))?
            .to_path_buf();
        let tmp_path = parent.join("term.log.tmp");

        let mut kept: Vec<TermRecord> = Vec::new();
        self.scan(|rec| {
            if rec.start_tx_id <= tx_id {
                kept.push(rec);
            }
        })?;

        {
            let mut tmp = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            for rec in &kept {
                tmp.write_all(&encode_record(*rec))?;
            }
            tmp.sync_data()?;
        }

        std::fs::rename(&tmp_path, &self.path)?;

        // Best-effort parent-dir fsync so the rename is durable. macOS does
        // not strictly require this for crash safety on APFS, but Linux
        // ext4/xfs do — and it's cheap.
        if let Ok(dir) = File::open(&parent) {
            let _ = dir.sync_all();
        }

        // Reopen against the new inode so subsequent appends/scans see the
        // truncated file rather than the renamed-out one.
        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .truncate(false)
            .open(&self.path)?;
        Ok(())
    }
}

// ── encoding / decoding ────────────────────────────────────────────────────

pub fn encode_record(rec: TermRecord) -> [u8; TERM_RECORD_SIZE] {
    let mut buf = [0u8; TERM_RECORD_SIZE];
    buf[0..8].copy_from_slice(&rec.term.to_le_bytes());
    buf[8..16].copy_from_slice(&rec.start_tx_id.to_le_bytes());
    buf[16..20].copy_from_slice(&TERM_MAGIC.to_le_bytes());
    let crc = crc32c::crc32c(&buf[0..20]);
    buf[20..24].copy_from_slice(&crc.to_le_bytes());
    buf
}

pub fn decode_record(bytes: &[u8]) -> io::Result<TermRecord> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_fresh() -> (TempDir, TermStorage) {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        let storage = TermStorage::open(&dir).unwrap();
        (td, storage)
    }

    #[test]
    fn append_and_scan_round_trip() {
        let (_td, mut storage) = open_fresh();
        storage
            .append(TermRecord {
                term: 1,
                start_tx_id: 0,
            })
            .unwrap();
        storage
            .append(TermRecord {
                term: 2,
                start_tx_id: 100,
            })
            .unwrap();
        storage.sync().unwrap();

        let mut collected = Vec::new();
        storage.scan(|r| collected.push(r)).unwrap();
        assert_eq!(
            collected,
            vec![
                TermRecord {
                    term: 1,
                    start_tx_id: 0,
                },
                TermRecord {
                    term: 2,
                    start_tx_id: 100,
                },
            ]
        );
    }

    #[test]
    fn cold_lookup_finds_covering_record() {
        let (_td, mut storage) = open_fresh();
        storage
            .append(TermRecord {
                term: 1,
                start_tx_id: 0,
            })
            .unwrap();
        storage
            .append(TermRecord {
                term: 2,
                start_tx_id: 50,
            })
            .unwrap();
        storage
            .append(TermRecord {
                term: 3,
                start_tx_id: 200,
            })
            .unwrap();
        storage.sync().unwrap();

        assert_eq!(
            storage.cold_lookup(5).unwrap(),
            Some(TermRecord {
                term: 1,
                start_tx_id: 0,
            })
        );
        assert_eq!(
            storage.cold_lookup(75).unwrap(),
            Some(TermRecord {
                term: 2,
                start_tx_id: 50,
            })
        );
        assert_eq!(
            storage.cold_lookup(9_999).unwrap(),
            Some(TermRecord {
                term: 3,
                start_tx_id: 200,
            })
        );
    }

    #[test]
    fn truncate_after_drops_records_past_threshold() {
        let (_td, mut storage) = open_fresh();
        for (term, start) in [(1, 0), (2, 50), (3, 100), (4, 200), (5, 300)] {
            storage
                .append(TermRecord {
                    term,
                    start_tx_id: start,
                })
                .unwrap();
        }
        storage.sync().unwrap();

        storage.truncate_after(150).unwrap();

        let mut collected = Vec::new();
        storage.scan(|r| collected.push(r)).unwrap();
        assert_eq!(
            collected,
            vec![
                TermRecord {
                    term: 1,
                    start_tx_id: 0,
                },
                TermRecord {
                    term: 2,
                    start_tx_id: 50,
                },
                TermRecord {
                    term: 3,
                    start_tx_id: 100,
                },
            ]
        );

        // Subsequent appends land at the new end of file.
        storage
            .append(TermRecord {
                term: 6,
                start_tx_id: 160,
            })
            .unwrap();
        storage.sync().unwrap();
        let mut after_append = Vec::new();
        storage.scan(|r| after_append.push(r)).unwrap();
        assert_eq!(after_append.len(), 4);
        assert_eq!(after_append.last().unwrap().term, 6);
    }

    #[test]
    fn truncate_after_keeps_all_when_threshold_above_max() {
        let (_td, mut storage) = open_fresh();
        storage
            .append(TermRecord {
                term: 1,
                start_tx_id: 0,
            })
            .unwrap();
        storage
            .append(TermRecord {
                term: 2,
                start_tx_id: 50,
            })
            .unwrap();
        storage.sync().unwrap();

        storage.truncate_after(9_999).unwrap();
        let mut collected = Vec::new();
        storage.scan(|r| collected.push(r)).unwrap();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn truncate_after_drops_all_when_threshold_below_first() {
        let (_td, mut storage) = open_fresh();
        storage
            .append(TermRecord {
                term: 1,
                start_tx_id: 50,
            })
            .unwrap();
        storage
            .append(TermRecord {
                term: 2,
                start_tx_id: 100,
            })
            .unwrap();
        storage.sync().unwrap();

        storage.truncate_after(10).unwrap();
        let mut collected = Vec::new();
        storage.scan(|r| collected.push(r)).unwrap();
        assert!(collected.is_empty());
    }

    #[test]
    fn corrupt_trailing_record_stops_scan_gracefully() {
        let (td, mut storage) = open_fresh();
        storage
            .append(TermRecord {
                term: 1,
                start_tx_id: 0,
            })
            .unwrap();
        storage.sync().unwrap();
        // Append garbage bytes directly to simulate a torn write.
        let path = td.path().join("term.log");
        let mut raw = OpenOptions::new().append(true).open(&path).unwrap();
        raw.write_all(&[0xAB; 40]).unwrap();
        raw.sync_data().unwrap();

        // Re-open so the in-process file handle is re-seeking from a clean state.
        let mut storage = TermStorage::open(&td.path().to_string_lossy()).unwrap();
        let mut collected = Vec::new();
        storage.scan(|r| collected.push(r)).unwrap();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].term, 1);
    }
}
