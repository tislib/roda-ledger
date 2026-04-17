//! On-disk transaction and account index files (ADR-008).
//!
//! Built as a side effect of sealing: the Seal thread iterates WAL records
//! and writes two flat sorted arrays per sealed segment:
//!
//!   `wal_index_NNNNNN.bin`     — `(tx_id, byte_offset)` sorted by `tx_id`
//!   `account_index_NNNNNN.bin` — `(account_id, tx_id)`  sorted by `(account_id, tx_id)`
//!
//! Methods are `impl Segment` to keep them close to the data they operate on
//! while keeping `segment.rs` focused on WAL lifecycle.

use crate::entities::WalEntry;
use crate::storage::layout::{segment_account_index_path, segment_tx_index_path};
use crate::storage::segment::Segment;
use crate::storage::wal_serializer::parse_wal_record;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

impl Segment {
    /// Build transaction and account index files for this sealed segment.
    ///
    /// Iterates every WAL record, collects `(tx_id, byte_offset)` per
    /// `TxMetadata` and `(account_id, tx_id)` per `TxEntry`, then writes
    /// them as flat binary files.
    ///
    /// The transaction index is naturally sorted (WAL is append-only).
    /// The account index is `sort_unstable`d at seal time.
    pub fn build_indexes(&self) -> std::io::Result<()> {
        let data_dir = Path::new(self.data_dir());
        let mut tx_offsets: Vec<(u64, u64)> = Vec::new();
        let mut account_entries: Vec<(u64, u64)> = Vec::new();

        let wal_data = self.wal_data();
        let mut offset: usize = 0;
        while offset < wal_data.len() {
            let entry = parse_wal_record(&wal_data[offset..]).map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!(
                        "failed to parse wal record at offset {} for segment {}: {}",
                        offset,
                        self.id(),
                        e
                    ),
                )
            })?;
            match &entry {
                WalEntry::Metadata(m) => {
                    tx_offsets.push((m.tx_id, offset as u64));
                }
                WalEntry::Entry(e) => {
                    account_entries.push((e.account_id, e.tx_id));
                }
                WalEntry::Link(_)
                | WalEntry::SegmentHeader(_)
                | WalEntry::SegmentSealed(_)
                | WalEntry::FunctionRegistered(_) => {}
            }
            offset += 40;
        }

        account_entries.sort_unstable();

        // Write transaction index
        let tx_index_path = segment_tx_index_path(data_dir, self.id());
        write_index_file(&tx_index_path, &tx_offsets)?;

        // Write account index
        let account_index_path = segment_account_index_path(data_dir, self.id());
        write_index_file(&account_index_path, &account_entries)?;

        Ok(())
    }

    /// Binary-search the on-disk transaction index for `tx_id`.
    ///
    /// Returns the byte offset into the WAL segment file where the
    /// `TxMetadata` record starts, or `None` if not found.
    pub fn search_tx_index(&self, tx_id: u64) -> std::io::Result<Option<u64>> {
        let path = segment_tx_index_path(Path::new(self.data_dir()), self.id());
        let entries = read_index_file(&path)?;
        Ok(entries
            .binary_search_by_key(&tx_id, |&(id, _)| id)
            .ok()
            .map(|i| entries[i].1))
    }

    /// Binary-search the on-disk account index for entries matching `account_id`.
    ///
    /// Returns up to `limit` transaction IDs where `tx_id >= from_tx_id`,
    /// sorted ascending by `tx_id`.
    pub fn search_account_index(
        &self,
        account_id: u64,
        from_tx_id: u64,
        limit: usize,
    ) -> std::io::Result<Vec<u64>> {
        let path = segment_account_index_path(Path::new(self.data_dir()), self.id());
        let entries = read_index_file(&path)?;

        // Find the start of this account_id's range via binary search.
        let start = entries.partition_point(|&(aid, _)| aid < account_id);

        let mut result = Vec::new();
        for &(aid, tid) in &entries[start..] {
            if aid != account_id {
                break;
            }
            if tid >= from_tx_id {
                result.push(tid);
                if result.len() >= limit {
                    break;
                }
            }
        }
        Ok(result)
    }

    /// Read the transaction at the given byte offset in this segment's WAL data.
    ///
    /// Returns the `TxMetadata` followed by all its `TxEntry` records.
    /// The segment must be loaded first.
    pub fn read_transaction_at_offset(&self, offset: u64) -> std::io::Result<Vec<WalEntry>> {
        let wal_data = self.wal_data();
        let offset = offset as usize;

        if offset >= wal_data.len() {
            return Ok(Vec::new());
        }

        let metadata = parse_wal_record(&wal_data[offset..])?;
        let (entry_count, link_count) = match &metadata {
            WalEntry::Metadata(m) => (m.entry_count as usize, m.link_count as usize),
            _ => return Ok(Vec::new()),
        };

        let total = entry_count + link_count;
        let mut entries = Vec::with_capacity(1 + total);
        entries.push(metadata);

        for i in 0..total {
            let entry_offset = offset + (1 + i) * 40;
            if entry_offset >= wal_data.len() {
                break;
            }
            entries.push(parse_wal_record(&wal_data[entry_offset..])?);
        }

        Ok(entries)
    }
}

// ── File I/O helpers ─────────────────────────────────────────────────────────

/// Write a flat array of `(u64, u64)` pairs to a binary file.
///
/// Format: `[record_count: 8 bytes LE]` followed by `[u64 LE][u64 LE] × count`.
fn write_index_file(path: &Path, entries: &[(u64, u64)]) -> std::io::Result<()> {
    let file = File::create(path).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("failed to create index file at {:?}: {}", path, e),
        )
    })?;
    let mut writer = BufWriter::new(file);
    let count = entries.len() as u64;
    writer.write_all(&count.to_le_bytes()).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!(
                "failed to write entry count to index file at {:?}: {}",
                path, e
            ),
        )
    })?;
    for &(a, b) in entries {
        writer.write_all(&a.to_le_bytes()).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to write entry part A to index file at {:?}: {}",
                    path, e
                ),
            )
        })?;
        writer.write_all(&b.to_le_bytes()).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to write entry part B to index file at {:?}: {}",
                    path, e
                ),
            )
        })?;
    }
    writer.flush().map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("failed to flush index file at {:?}: {}", path, e),
        )
    })?;
    writer.get_ref().sync_all().map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("failed to sync index file at {:?}: {}", path, e),
        )
    })?;
    Ok(())
}

/// Read a flat array of `(u64, u64)` pairs from a binary index file.
fn read_index_file(path: &Path) -> std::io::Result<Vec<(u64, u64)>> {
    let file = File::open(path).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("failed to open index file at {:?}: {}", path, e),
        )
    })?;
    let mut reader = BufReader::new(file);
    let mut buf = [0u8; 8];

    reader.read_exact(&mut buf).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!(
                "failed to read entry count from index file at {:?}: {}",
                path, e
            ),
        )
    })?;
    let count = u64::from_le_bytes(buf) as usize;

    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        reader.read_exact(&mut buf).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to read entry part A (entry {}) from index file at {:?}: {}",
                    i, path, e
                ),
            )
        })?;
        let a = u64::from_le_bytes(buf);
        reader.read_exact(&mut buf).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to read entry part B (entry {}) from index file at {:?}: {}",
                    i, path, e
                ),
            )
        })?;
        let b = u64::from_le_bytes(buf);
        entries.push((a, b));
    }
    Ok(entries)
}
