//! In-memory term semantics — durable state lives in
//! `storage::TermStorage`; this layer adds the lock-free current-term
//! atomic, the RwLock-guarded hot ring of recent records, and the
//! hot-then-cold lookup used by `GetStatus` / `WaitForTransaction`.
//!
//! Writers take the writer mutex, append via the storage layer (which
//! `fdatasync`s), then push into the ring and publish `current` with
//! `Release`. Readers hit either the atomic (`get_current_term`) or the
//! ring via a read lock (`get_term_at_tx`, hot path). A miss against the
//! ring escalates to a cold disk scan through `TermStorage`.
//!
//! Ordering guarantee: "atomic value observed ⇒ record on disk and in ring".

use crate::storage::{Storage, TermRecord, TermStorage};
use spdlog::info;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// In-memory ring capacity. Older terms fall out the front and can only
/// be resolved through the cold lookup.
pub const RING_CAP: usize = 10_000;

/// Durable + in-memory term state. Clone via `Arc<Term>` to share
/// across the leader's replication tasks, the follower's AppendEntries
/// handler, and the client-facing gRPC handler.
pub struct Term {
    /// Current durable term. `Release`-stored after every successful
    /// append so readers observing a value see the corresponding record
    /// on disk and in the ring.
    current: AtomicU64,
    /// Hot ring — read-mostly, so guarded by an `RwLock`.
    ring: RwLock<TermRing>,
    /// Writer serialisation. Ordering:
    ///     1. take `writer`,
    ///     2. `storage.append` (fdatasync),
    ///     3. ring write-lock + push,
    ///     4. atomic store.
    writer: Mutex<TermStorage>,
}

/// Fixed-capacity circular buffer of the last `RING_CAP` term records.
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

    /// Oldest entry still in the ring, or `None` if empty. Used to
    /// decide whether `tx_id` is covered by the hot path.
    fn oldest(&self) -> Option<TermRecord> {
        if self.len == 0 {
            None
        } else {
            Some(self.buf[self.head])
        }
    }

    /// Record with the largest `start_tx_id <= tx_id` — `None` when no
    /// entry in the ring is old enough. Caller must have verified the
    /// ring actually covers `tx_id` (i.e. `oldest().start_tx_id <= tx_id`).
    fn find_covering(&self, tx_id: u64) -> Option<TermRecord> {
        if self.len == 0 {
            return None;
        }
        // Terms are monotonic within the ring, so the largest `term`
        // among records with `start_tx_id <= tx_id` is the term that
        // covered `tx_id`.
        let mut best: Option<TermRecord> = None;
        for i in 0..self.len {
            let rec = self.buf[(self.head + i) % RING_CAP];
            if rec.start_tx_id > tx_id {
                continue;
            }
            best = match best {
                Some(b) if b.term >= rec.term => Some(b),
                _ => Some(rec),
            };
        }
        best
    }
}

impl Term {
    /// Open `{storage.data_dir}/term.log` and hydrate the hot ring from
    /// its tail. On first boot the file is empty; `current` stays at 0.
    pub fn open(storage: &Arc<Storage>) -> io::Result<Self> {
        Self::open_in_dir(&storage.config().data_dir)
    }

    /// Lower-level constructor for tests + tools that don't hold a full `Storage`.
    pub fn open_in_dir(data_dir: &str) -> io::Result<Self> {
        let mut file = TermStorage::open(data_dir)?;
        let mut ring = TermRing::new();
        let mut current = 0u64;
        file.scan(|rec| {
            current = current.max(rec.term);
            ring.push(rec);
        })?;
        info!(
            "term: opened {} (current={}, records_loaded={})",
            file.path().display(),
            current,
            ring.len
        );
        Ok(Self {
            current: AtomicU64::new(current),
            ring: RwLock::new(ring),
            writer: Mutex::new(file),
        })
    }

    /// Current durable term. Lock-free, `Acquire`-ordered.
    #[inline]
    pub fn get_current_term(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    /// Look up the term that covered `tx_id`. Hot path reads the ring
    /// under a read lock; a miss falls through to a cold disk scan.
    /// Returns `None` if `tx_id` predates every record we have (log is
    /// empty or was truncated).
    pub fn get_term_at_tx(&self, tx_id: u64) -> io::Result<Option<TermRecord>> {
        // Hot path — read-locked, no blocking.
        let oldest = {
            let ring = self.ring.read().expect("term: ring rwlock poisoned");
            if let Some(o) = ring.oldest() {
                if o.start_tx_id <= tx_id {
                    return Ok(ring.find_covering(tx_id));
                }
                Some(o)
            } else {
                None
            }
        };

        // Cold path — ring does not cover tx_id (requested tx is older
        // than the oldest retained record, or the ring is empty).
        // `oldest` is only kept for debugging context.
        let _ = oldest;
        let mut writer = self.writer.lock().expect("term: writer mutex poisoned");
        writer.cold_lookup(tx_id)
    }

    /// Leader path: open a new term covering new transactions starting
    /// at `start_tx_id`. Blocking — serialised through the writer mutex
    /// and durable before returning.
    pub fn new_term(&self, start_tx_id: u64) -> io::Result<u64> {
        let mut writer = self.writer.lock().expect("term: writer mutex poisoned");
        let next = self
            .current
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| io::Error::other("term: u64 overflow"))?;
        let rec = TermRecord {
            term: next,
            start_tx_id,
        };
        writer.append(rec)?;
        {
            let mut ring = self.ring.write().expect("term: ring rwlock poisoned");
            ring.push(rec);
        }
        self.current.store(next, Ordering::Release);
        info!(
            "term: opened term {} at start_tx_id={} ({})",
            next,
            start_tx_id,
            writer.path().display()
        );
        Ok(next)
    }

    /// Follower path: record `term` observed from an incoming
    /// AppendEntries. Idempotent on `term == current`, rejects a strict
    /// regression, otherwise durably appends + publishes.
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

        let mut writer = self.writer.lock().expect("term: writer mutex poisoned");
        let current = self.current.load(Ordering::Acquire);
        if term <= current {
            return Ok(());
        }

        let rec = TermRecord { term, start_tx_id };
        writer.append(rec)?;
        {
            let mut ring = self.ring.write().expect("term: ring rwlock poisoned");
            ring.push(rec);
        }
        self.current.store(term, Ordering::Release);
        info!(
            "term: observed term {} at start_tx_id={} ({})",
            term,
            start_tx_id,
            writer.path().display()
        );
        Ok(())
    }

    /// Observability — the last record currently in the hot ring.
    pub fn last_record(&self) -> Option<TermRecord> {
        self.ring.read().expect("term: ring rwlock poisoned").last()
    }
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
        assert_eq!(term.get_term_at_tx(100).unwrap(), None);
    }

    #[test]
    fn new_term_and_hot_path_lookup() {
        let (_td, term) = open_in_tmp();
        term.new_term(0).unwrap(); // term 1 @ 0
        term.new_term(50).unwrap(); // term 2 @ 50
        term.new_term(100).unwrap(); // term 3 @ 100
        assert_eq!(term.get_current_term(), 3);

        assert_eq!(
            term.get_term_at_tx(5).unwrap(),
            Some(TermRecord {
                term: 1,
                start_tx_id: 0,
            })
        );
        assert_eq!(
            term.get_term_at_tx(49).unwrap(),
            Some(TermRecord {
                term: 1,
                start_tx_id: 0,
            })
        );
        assert_eq!(
            term.get_term_at_tx(75).unwrap(),
            Some(TermRecord {
                term: 2,
                start_tx_id: 50,
            })
        );
        assert_eq!(
            term.get_term_at_tx(999).unwrap(),
            Some(TermRecord {
                term: 3,
                start_tx_id: 100,
            })
        );
    }

    #[test]
    fn observe_is_idempotent_and_monotonic() {
        let (_td, term) = open_in_tmp();
        term.observe(5, 0).unwrap();
        assert_eq!(term.get_current_term(), 5);
        term.observe(5, 0).unwrap();
        term.observe(7, 100).unwrap();
        assert_eq!(term.get_current_term(), 7);
        let err = term.observe(6, 200).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn reopen_recovers_current_and_ring() {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        {
            let term = Term::open_in_dir(&dir).unwrap();
            term.new_term(0).unwrap();
            term.new_term(10).unwrap();
            term.observe(5, 20).unwrap();
            assert_eq!(term.get_current_term(), 5);
        }
        let reopened = Term::open_in_dir(&dir).unwrap();
        assert_eq!(reopened.get_current_term(), 5);
        assert_eq!(
            reopened.last_record(),
            Some(TermRecord {
                term: 5,
                start_tx_id: 20,
            })
        );
    }

    #[test]
    fn cold_lookup_resolves_tx_older_than_ring() {
        // Simulate a ring that has rotated past an older record by
        // directly writing many records through the storage layer, then
        // opening a fresh Term that loads only the tail into the ring.
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();

        // Write RING_CAP + 50 records via the low-level TermStorage so
        // the in-memory ring will drop the first 50 on load.
        {
            let mut storage = TermStorage::open(&dir).unwrap();
            let total = RING_CAP as u64 + 50;
            for t in 1..=total {
                storage
                    .append(TermRecord {
                        term: t,
                        start_tx_id: t * 10,
                    })
                    .unwrap();
            }
        }

        let term = Term::open_in_dir(&dir).unwrap();
        assert_eq!(term.get_current_term(), RING_CAP as u64 + 50);

        // Hot lookup for a tx covered by the ring's oldest entry.
        let oldest_start = (51/* first retained term */) * 10;
        let hot = term.get_term_at_tx(oldest_start + 5).unwrap().unwrap();
        assert_eq!(hot.term, 51);

        // Cold lookup: tx_id older than the ring's oldest retained
        // start_tx_id should still resolve via disk scan.
        let cold = term.get_term_at_tx(25).unwrap().unwrap();
        assert_eq!(
            cold,
            TermRecord {
                term: 2,
                start_tx_id: 20,
            }
        );
    }

    #[test]
    fn ring_caps_at_capacity() {
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
