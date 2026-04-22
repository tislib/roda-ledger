//! Durable term log (file layer).
//!
//! `TermStorage` owns `{data_dir}/term.log`: fixed 40-byte records laid
//! out to mirror the WAL record size so tooling can scan both with the
//! same cadence. Callers (typically `cluster::Term`) handle the
//! in-memory semantics — this file is just "open, scan, append+fsync".
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
/// All writes are `fdatasync`-guarded; every read is a positional scan.
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

    /// Append `rec` and `fdatasync`. Caller must serialise concurrent
    /// appenders externally (the cluster layer uses a mutex).
    pub fn append(&mut self, rec: TermRecord) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        let bytes = encode_record(rec);
        self.file.write_all(&bytes)?;
        self.file.sync_data()?;
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
    fn corrupt_trailing_record_stops_scan_gracefully() {
        let (td, mut storage) = open_fresh();
        storage
            .append(TermRecord {
                term: 1,
                start_tx_id: 0,
            })
            .unwrap();
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
