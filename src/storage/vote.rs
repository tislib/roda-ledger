//! Durable vote log (file layer).
//!
//! `VoteStorage` owns `{data_dir}/vote.log`: fixed 40-byte records laid
//! out to mirror `term.log` (and the WAL record size) so tooling can
//! scan all three with the same cadence. Callers (typically
//! `cluster::Vote`) handle the in-memory semantics — this file is just
//! "open, scan, append, sync". `append` and `sync` are deliberately
//! separate so test seeds can batch a single fdatasync after a bulk
//! write while production paths call `sync` after every record.
//!
//! Each record captures the durable Raft state required by §5.4.1:
//! the `term` for which the vote applies, and the `voted_for` node id
//! within that term (`0` means "no vote granted"). On reopen, the
//! caller hydrates state from the **last** record on disk; older
//! records are kept for forensics but are not semantically required.
//!
//! Record layout:
//!
//! ```text
//!  offset  size  field
//!  0       8     u64 term              (LE)
//!  8       8     u64 voted_for         (LE; 0 = none)
//!  16      4     u32 magic             (b"VOTE" LE)
//!  20      4     u32 crc32c of bytes[0..20]
//!  24      16    zero pad
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const VOTE_RECORD_SIZE: usize = 40;
pub const VOTE_MAGIC: u32 = u32::from_le_bytes(*b"VOTE");

/// One durable vote record, shared between the storage layer and the
/// cluster-level semantic wrapper. `voted_for == 0` encodes "no vote
/// granted in this term" (config validation guarantees real node ids
/// are non-zero).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VoteRecord {
    pub term: u64,
    pub voted_for: u64,
}

/// File-backed vote log. Holds an append-open `File` and the file path.
/// Writers `append` records and call `sync` to fdatasync; every read
/// is a positional scan.
pub struct VoteStorage {
    file: File,
    path: PathBuf,
}

impl VoteStorage {
    /// Open (or create) `{data_dir}/vote.log`.
    pub fn open(data_dir: &str) -> io::Result<Self> {
        let path = Path::new(data_dir).join("vote.log");
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
        F: FnMut(VoteRecord),
    {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; VOTE_RECORD_SIZE];
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

    /// Return the last well-formed record on disk, or `None` if the
    /// file is empty or fully corrupt. Used by `cluster::Vote::open`
    /// to hydrate `(current_term, voted_for)` on boot.
    pub fn last_record(&mut self) -> io::Result<Option<VoteRecord>> {
        let mut latest: Option<VoteRecord> = None;
        self.scan(|rec| {
            latest = Some(rec);
        })?;
        Ok(latest)
    }

    /// Append `rec` to the file (no fdatasync — call `sync` for
    /// durability). Caller must serialise concurrent appenders
    /// externally (the cluster layer uses a mutex).
    pub fn append(&mut self, rec: VoteRecord) -> io::Result<()> {
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
}

// ── encoding / decoding ────────────────────────────────────────────────────

pub fn encode_record(rec: VoteRecord) -> [u8; VOTE_RECORD_SIZE] {
    let mut buf = [0u8; VOTE_RECORD_SIZE];
    buf[0..8].copy_from_slice(&rec.term.to_le_bytes());
    buf[8..16].copy_from_slice(&rec.voted_for.to_le_bytes());
    buf[16..20].copy_from_slice(&VOTE_MAGIC.to_le_bytes());
    let crc = crc32c::crc32c(&buf[0..20]);
    buf[20..24].copy_from_slice(&crc.to_le_bytes());
    buf
}

pub fn decode_record(bytes: &[u8]) -> io::Result<VoteRecord> {
    if bytes.len() != VOTE_RECORD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "vote: record is not 40 bytes",
        ));
    }
    let magic = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    if magic != VOTE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("vote: magic mismatch (got {:#010x})", magic),
        ));
    }
    let stored_crc = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    let actual_crc = crc32c::crc32c(&bytes[0..20]);
    if stored_crc != actual_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "vote: crc mismatch (stored={:#010x} actual={:#010x})",
                stored_crc, actual_crc
            ),
        ));
    }
    Ok(VoteRecord {
        term: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        voted_for: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_fresh() -> (TempDir, VoteStorage) {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        let storage = VoteStorage::open(&dir).unwrap();
        (td, storage)
    }

    #[test]
    fn fresh_file_has_no_last_record() {
        let (_td, mut storage) = open_fresh();
        assert_eq!(storage.last_record().unwrap(), None);
    }

    #[test]
    fn append_and_scan_round_trip() {
        let (_td, mut storage) = open_fresh();
        storage
            .append(VoteRecord {
                term: 1,
                voted_for: 0,
            })
            .unwrap();
        storage
            .append(VoteRecord {
                term: 2,
                voted_for: 7,
            })
            .unwrap();
        storage.sync().unwrap();

        let mut collected = Vec::new();
        storage.scan(|r| collected.push(r)).unwrap();
        assert_eq!(
            collected,
            vec![
                VoteRecord {
                    term: 1,
                    voted_for: 0,
                },
                VoteRecord {
                    term: 2,
                    voted_for: 7,
                },
            ]
        );
    }

    #[test]
    fn last_record_returns_most_recent() {
        let (_td, mut storage) = open_fresh();
        storage
            .append(VoteRecord {
                term: 1,
                voted_for: 0,
            })
            .unwrap();
        storage
            .append(VoteRecord {
                term: 2,
                voted_for: 7,
            })
            .unwrap();
        storage
            .append(VoteRecord {
                term: 3,
                voted_for: 0,
            })
            .unwrap();
        storage.sync().unwrap();
        assert_eq!(
            storage.last_record().unwrap(),
            Some(VoteRecord {
                term: 3,
                voted_for: 0,
            })
        );
    }

    #[test]
    fn reopen_observes_last_record() {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        {
            let mut storage = VoteStorage::open(&dir).unwrap();
            storage
                .append(VoteRecord {
                    term: 5,
                    voted_for: 11,
                })
                .unwrap();
            storage.sync().unwrap();
        }
        let mut storage = VoteStorage::open(&dir).unwrap();
        assert_eq!(
            storage.last_record().unwrap(),
            Some(VoteRecord {
                term: 5,
                voted_for: 11,
            })
        );
    }

    #[test]
    fn corrupt_trailing_record_stops_scan_gracefully() {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        {
            let mut storage = VoteStorage::open(&dir).unwrap();
            storage
                .append(VoteRecord {
                    term: 1,
                    voted_for: 4,
                })
                .unwrap();
            storage.sync().unwrap();
        }
        // Append garbage bytes directly to simulate a torn write.
        let path = td.path().join("vote.log");
        let mut raw = OpenOptions::new().append(true).open(&path).unwrap();
        raw.write_all(&[0xAB; 40]).unwrap();
        raw.sync_data().unwrap();

        let mut storage = VoteStorage::open(&dir).unwrap();
        let mut collected = Vec::new();
        storage.scan(|r| collected.push(r)).unwrap();
        assert_eq!(collected.len(), 1);
        assert_eq!(
            collected[0],
            VoteRecord {
                term: 1,
                voted_for: 4,
            }
        );
        assert_eq!(
            storage.last_record().unwrap(),
            Some(VoteRecord {
                term: 1,
                voted_for: 4,
            })
        );
    }

    #[test]
    fn decode_rejects_wrong_magic() {
        let mut bad = encode_record(VoteRecord {
            term: 1,
            voted_for: 1,
        });
        bad[16..20].copy_from_slice(&u32::from_le_bytes(*b"TERM").to_le_bytes());
        let err = decode_record(&bad).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn decode_rejects_bad_crc() {
        let mut bad = encode_record(VoteRecord {
            term: 1,
            voted_for: 1,
        });
        bad[20] ^= 0xFF;
        let err = decode_record(&bad).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
