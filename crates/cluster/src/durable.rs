//! Durable raft persistence — `Term` (boundary log) and `Vote` (per-term
//! election state) wrappers around `storage::TermStorage` /
//! `storage::VoteStorage`, plus a `DurablePersistence` adapter that
//! satisfies [`raft::Persistence`].
//!
//! Layout mirrors the legacy `cluster::raft::{term, vote}` modules with
//! two changes required by ADR-0017:
//!
//! 1. `Term::truncate_after` (new) — atomic rename-based replacement.
//! 2. `Term::commit_term` no longer rejects non-contiguous forward jumps
//!    (ADR-0017 §"Required Invariants" #7). The vote log can race ahead
//!    of the term log when a candidate self-votes at term N but later
//!    wins at term N+k; `commit_term` must accept any `current < expected`.
//!
//! `Term::get_term_at_tx` is read by the client-facing `LedgerHandler`
//! outside the raft mutex; the wrapper's internal `RwLock` + `AtomicU64`
//! keeps that path lock-free in the common case.

use raft::{Persistence, TermRecord as RaftTermRecord};
use spdlog::{debug, info, warn};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use storage::{TermRecord, TermStorage, VoteRecord, VoteStorage};

// ── Term log ──────────────────────────────────────────────────────────────

/// In-memory ring capacity. Older terms fall out the front and can only
/// be resolved through the cold lookup.
pub const RING_CAP: usize = 10_000;

/// Durable + in-memory term state. Clone via `Arc<Term>` to share across
/// the raft driver and the client-facing handlers.
pub struct Term {
    /// Current durable term. `Release`-stored after every successful
    /// append so readers observing a value see the corresponding record
    /// on disk and in the ring.
    current: AtomicU64,
    /// Hot ring — read-mostly, so guarded by an `RwLock`.
    ring: RwLock<TermRing>,
    /// Writer serialisation. Ordering:
    ///     1. take `writer`,
    ///     2. `storage.append` + `storage.sync` (fdatasync),
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

    fn oldest(&self) -> Option<TermRecord> {
        if self.len == 0 {
            None
        } else {
            Some(self.buf[self.head])
        }
    }

    /// Record with the largest `start_tx_id <= tx_id`. Caller must have
    /// verified the ring covers `tx_id`.
    fn find_covering(&self, tx_id: u64) -> Option<TermRecord> {
        if self.len == 0 {
            return None;
        }
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

    fn clear(&mut self) {
        self.len = 0;
        self.head = 0;
    }
}

impl Term {
    /// Open `{data_dir}/term.log` and hydrate the hot ring from its tail.
    /// On first boot the file is empty; `current` stays at 0.
    pub fn open_in_dir(data_dir: &str) -> io::Result<Self> {
        let mut file = TermStorage::open(data_dir)?;
        let mut ring = TermRing::new();
        let mut current = 0u64;
        file.scan(|rec| {
            current = current.max(rec.term);
            ring.push(rec);
        })?;
        debug!(
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
    pub fn get_term_at_tx(&self, tx_id: u64) -> io::Result<Option<TermRecord>> {
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
        let _ = oldest;
        let mut writer = self.writer.lock().expect("term: writer mutex poisoned");
        writer.cold_lookup(tx_id)
    }

    /// Test/tooling convenience: open a fresh term at `current + 1`,
    /// covering `start_tx_id`. Returns the new term. Production code
    /// goes through `commit_term` with an explicit expected term —
    /// `new_term` only exists so synthetic test seeders don't have to
    /// thread the increment manually.
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
        writer.sync()?;
        {
            let mut ring = self.ring.write().expect("term: ring rwlock poisoned");
            ring.push(rec);
        }
        self.current.store(next, Ordering::Release);
        debug!(
            "term: opened term {} at start_tx_id={} ({})",
            next,
            start_tx_id,
            writer.path().display()
        );
        Ok(next)
    }

    /// Candidate-on-Won path: durably commit a term boundary at a
    /// **specific** expected term. Per ADR-0017 §"Required Invariants" #7
    /// the term log permits non-contiguous forward jumps — this method
    /// accepts any `current < expected` (the legacy `current + 1 == expected`
    /// strict-equality arm is gone).
    ///
    /// - `Ok(true)`  — wrote `(expected, start_tx_id)` durably.
    /// - `Ok(false)` — `current >= expected`; treat as election-lost.
    /// - `Err(_)`    — I/O failure.
    pub fn commit_term(&self, expected: u64, start_tx_id: u64) -> io::Result<bool> {
        let mut writer = self.writer.lock().expect("term: writer mutex poisoned");
        let current = self.current.load(Ordering::Acquire);
        if current >= expected {
            return Ok(false);
        }
        let rec = TermRecord {
            term: expected,
            start_tx_id,
        };
        writer.append(rec)?;
        writer.sync()?;
        {
            let mut ring = self.ring.write().expect("term: ring rwlock poisoned");
            ring.push(rec);
        }
        self.current.store(expected, Ordering::Release);
        debug!(
            "term: committed term {} at start_tx_id={} ({})",
            expected,
            start_tx_id,
            writer.path().display()
        );
        Ok(true)
    }

    /// Follower path: record `term` observed from an incoming RPC.
    /// Idempotent on `term == current`, rejects a strict regression,
    /// otherwise durably appends + publishes.
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
        writer.sync()?;
        {
            let mut ring = self.ring.write().expect("term: ring rwlock poisoned");
            ring.push(rec);
        }
        self.current.store(term, Ordering::Release);
        debug!(
            "term: observed term {} at start_tx_id={} ({})",
            term,
            start_tx_id,
            writer.path().display()
        );
        Ok(())
    }

    /// Drop term records whose `start_tx_id > tx_id` (Raft §5.3 follower
    /// log truncation). Atomic via rename-based file replacement; the
    /// in-memory ring is rebuilt by re-scanning.
    pub fn truncate_after(&self, tx_id: u64) -> io::Result<()> {
        let mut writer = self.writer.lock().expect("term: writer mutex poisoned");
        writer.truncate_after(tx_id)?;
        let mut ring = self.ring.write().expect("term: ring rwlock poisoned");
        ring.clear();
        let mut current = 0u64;
        writer.scan(|rec| {
            current = current.max(rec.term);
            ring.push(rec);
        })?;
        self.current.store(current, Ordering::Release);
        debug!(
            "term: truncated after tx_id={} (current={}, records_loaded={}, {})",
            tx_id,
            current,
            ring.len,
            writer.path().display()
        );
        Ok(())
    }

    /// Observability — the last record currently in the hot ring.
    pub fn last_record(&self) -> Option<TermRecord> {
        self.ring.read().expect("term: ring rwlock poisoned").last()
    }
}

// ── Vote log ──────────────────────────────────────────────────────────────

/// Durable + in-memory vote state. Clone via `Arc<Vote>` to share across
/// the raft driver and any read-side consumer.
pub struct Vote {
    /// Current durable term as observed by the vote layer. May lag
    /// `Term::get_current_term()` momentarily; both converge through
    /// `observe_term`. `Release`-stored after every successful append.
    current_term: AtomicU64,
    /// Node id we voted for in `current_term`, or `0` for "no vote".
    voted_for: AtomicU64,
    /// Writer serialisation. Ordering:
    ///     1. take `writer`,
    ///     2. `storage.append` + `storage.sync` (fdatasync),
    ///     3. atomic stores.
    writer: Mutex<VoteStorage>,
}

impl Vote {
    /// Open `{data_dir}/vote.log` and hydrate `(current_term, voted_for)`
    /// from the last record.
    pub fn open_in_dir(data_dir: &str) -> io::Result<Self> {
        let mut file = VoteStorage::open(data_dir)?;
        let last = file.last_record()?;
        let (term, voted_for) = match last {
            Some(rec) => (rec.term, rec.voted_for),
            None => (0, 0),
        };
        debug!(
            "vote: opened {} (current_term={}, voted_for={})",
            file.path().display(),
            term,
            voted_for,
        );
        Ok(Self {
            current_term: AtomicU64::new(term),
            voted_for: AtomicU64::new(voted_for),
            writer: Mutex::new(file),
        })
    }

    /// Current durable term as known to the vote layer. Lock-free,
    /// `Acquire`-ordered.
    #[inline]
    pub fn get_current_term(&self) -> u64 {
        self.current_term.load(Ordering::Acquire)
    }

    /// Node we voted for in the current term, or `None`. Lock-free.
    #[inline]
    pub fn get_voted_for(&self) -> Option<u64> {
        match self.voted_for.load(Ordering::Acquire) {
            0 => None,
            n => Some(n),
        }
    }

    /// Raft §5.4.1: durably grant a vote for `candidate_id` in `term`.
    pub fn vote(&self, term: u64, candidate_id: u64) -> io::Result<bool> {
        if candidate_id == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "vote: candidate_id must be non-zero",
            ));
        }

        let mut writer = self.writer.lock().expect("vote: writer mutex poisoned");
        let current = self.current_term.load(Ordering::Acquire);

        if term < current {
            return Ok(false);
        }

        if term == current {
            let already = self.voted_for.load(Ordering::Acquire);
            if already == candidate_id {
                return Ok(true);
            }
            if already != 0 {
                return Ok(false);
            }
        }

        let rec = VoteRecord {
            term,
            voted_for: candidate_id,
        };
        writer.append(rec)?;
        writer.sync()?;
        self.voted_for.store(candidate_id, Ordering::Release);
        self.current_term.store(term, Ordering::Release);
        debug!(
            "vote: granted term={} candidate_id={} ({})",
            term,
            candidate_id,
            writer.path().display(),
        );
        Ok(true)
    }

    /// Observe a strictly-higher term advance; clears `voted_for`.
    /// Idempotent on equal term, rejects strict regressions.
    pub fn observe_term(&self, term: u64) -> io::Result<()> {
        let current = self.current_term.load(Ordering::Acquire);
        if term == current {
            return Ok(());
        }
        if term < current {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "vote: term regression incoming={} current={}",
                    term, current
                ),
            ));
        }

        let mut writer = self.writer.lock().expect("vote: writer mutex poisoned");
        let current = self.current_term.load(Ordering::Acquire);
        if term <= current {
            return Ok(());
        }

        let rec = VoteRecord { term, voted_for: 0 };
        writer.append(rec)?;
        writer.sync()?;
        self.voted_for.store(0, Ordering::Release);
        self.current_term.store(term, Ordering::Release);
        debug!(
            "vote: observed higher term={} (cleared voted_for) ({})",
            term,
            writer.path().display(),
        );
        Ok(())
    }
}

// ── DurablePersistence (raft::Persistence impl) ───────────────────────────

/// Adapter that satisfies [`raft::Persistence`] by delegating to the
/// `Term` and `Vote` wrappers. Both are kept as `Arc`s so the
/// `LedgerHandler` can read `Term::get_term_at_tx` outside the raft
/// mutex.
pub struct DurablePersistence {
    pub term: Arc<Term>,
    pub vote: Arc<Vote>,
}

impl DurablePersistence {
    pub fn open(data_dir: &str) -> io::Result<Self> {
        let term = Arc::new(Term::open_in_dir(data_dir)?);
        let vote = Arc::new(Vote::open_in_dir(data_dir)?);
        Ok(Self { term, vote })
    }
}

#[inline]
fn into_raft_record(r: TermRecord) -> RaftTermRecord {
    RaftTermRecord {
        term: r.term,
        start_tx_id: r.start_tx_id,
    }
}

// `raft::Persistence` is infallible: writes either succeed or the
// process must crash (the supervisor restarts). The internal `Term` /
// `Vote` wrappers still surface `io::Result` because tests/tooling
// want the explicit error path; the trait impl panics on any I/O
// failure. Per ADR-0017 the supervisor (`cluster::lifecycle`) is
// responsible for the restart.
impl Persistence for DurablePersistence {
    fn current_term(&self) -> u64 {
        self.term.get_current_term()
    }

    fn last_term_record(&self) -> Option<RaftTermRecord> {
        self.term.last_record().map(into_raft_record)
    }

    fn term_at_tx(&self, tx_id: u64) -> Option<RaftTermRecord> {
        match self.term.get_term_at_tx(tx_id) {
            Ok(rec) => rec.map(into_raft_record),
            Err(e) => {
                warn!(
                    "durable: term_at_tx({}) storage error: {} (returning None)",
                    tx_id, e
                );
                None
            }
        }
    }

    fn commit_term(&mut self, expected: u64, start_tx_id: u64) -> bool {
        self.term
            .commit_term(expected, start_tx_id)
            .unwrap_or_else(|e| panic!("durable: commit_term({expected},{start_tx_id}) failed: {e}"))
    }

    fn observe_term(&mut self, term: u64, start_tx_id: u64) {
        self.term
            .observe(term, start_tx_id)
            .unwrap_or_else(|e| panic!("durable: observe_term({term},{start_tx_id}) failed: {e}"));
    }

    fn truncate_term_after(&mut self, tx_id: u64) {
        self.term
            .truncate_after(tx_id)
            .unwrap_or_else(|e| panic!("durable: truncate_term_after({tx_id}) failed: {e}"));
    }

    fn vote_term(&self) -> u64 {
        self.vote.get_current_term()
    }

    fn voted_for(&self) -> Option<u64> {
        self.vote.get_voted_for()
    }

    fn vote(&mut self, term: u64, candidate_id: u64) -> bool {
        self.vote
            .vote(term, candidate_id)
            .unwrap_or_else(|e| panic!("durable: vote({term},{candidate_id}) failed: {e}"))
    }

    fn observe_vote_term(&mut self, term: u64) {
        self.vote
            .observe_term(term)
            .unwrap_or_else(|e| panic!("durable: observe_vote_term({term}) failed: {e}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_term() -> (TempDir, Term) {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        let term = Term::open_in_dir(&dir).unwrap();
        (td, term)
    }

    fn open_vote() -> (TempDir, Vote) {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        let vote = Vote::open_in_dir(&dir).unwrap();
        (td, vote)
    }

    #[test]
    fn term_fresh_starts_at_zero() {
        let (_td, term) = open_term();
        assert_eq!(term.get_current_term(), 0);
        assert_eq!(term.last_record(), None);
    }

    #[test]
    fn term_observe_idempotent_and_monotonic() {
        let (_td, term) = open_term();
        term.observe(5, 0).unwrap();
        assert_eq!(term.get_current_term(), 5);
        term.observe(5, 0).unwrap();
        term.observe(7, 100).unwrap();
        assert_eq!(term.get_current_term(), 7);
        let err = term.observe(6, 200).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn term_commit_term_accepts_non_contiguous_forward_jumps() {
        let (_td, term) = open_term();
        // ADR-0017 §"Required Invariants" #7: the vote log can race ahead,
        // so commit_term accepts any current < expected.
        assert_eq!(term.commit_term(5, 100).unwrap(), true);
        assert_eq!(term.get_current_term(), 5);
        assert_eq!(
            term.last_record(),
            Some(TermRecord {
                term: 5,
                start_tx_id: 100,
            })
        );
    }

    #[test]
    fn term_commit_term_returns_false_when_already_advanced() {
        let (_td, term) = open_term();
        term.observe(5, 100).unwrap();
        assert_eq!(term.commit_term(3, 50).unwrap(), false);
        assert_eq!(term.commit_term(5, 200).unwrap(), false);
        assert_eq!(term.get_current_term(), 5);
    }

    #[test]
    fn term_truncate_after_drops_records_and_rebuilds_ring() {
        let (_td, term) = open_term();
        term.observe(1, 0).unwrap();
        term.observe(2, 50).unwrap();
        term.observe(3, 100).unwrap();
        term.observe(4, 200).unwrap();
        assert_eq!(term.get_current_term(), 4);

        term.truncate_after(120).unwrap();

        // current_term reflects the new max in-file
        assert_eq!(term.get_current_term(), 3);
        assert_eq!(
            term.last_record(),
            Some(TermRecord {
                term: 3,
                start_tx_id: 100,
            })
        );
        // Record at start_tx_id=200 (term=4) is gone.
        assert_eq!(term.get_term_at_tx(250).unwrap().unwrap().term, 3);
    }

    #[test]
    fn vote_grant_and_observe() {
        let (_td, vote) = open_vote();
        assert!(vote.vote(1, 7).unwrap());
        assert_eq!(vote.get_voted_for(), Some(7));
        assert!(!vote.vote(1, 11).unwrap());
        vote.observe_term(2).unwrap();
        assert_eq!(vote.get_voted_for(), None);
        assert!(vote.vote(2, 11).unwrap());
    }

    #[test]
    fn durable_persistence_round_trips_through_trait() {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        let mut p = DurablePersistence::open(&dir).unwrap();
        assert_eq!(p.current_term(), 0);
        assert_eq!(p.last_term_record(), None);
        assert!(p.commit_term(3, 0));
        assert_eq!(p.current_term(), 3);
        assert_eq!(
            p.last_term_record(),
            Some(RaftTermRecord {
                term: 3,
                start_tx_id: 0,
            })
        );
        assert!(p.vote(3, 1));
        assert_eq!(p.voted_for(), Some(1));
        assert_eq!(p.vote_term(), 3);

        p.truncate_term_after(0);
        // start_tx_id=0 record has term=3 — kept (start_tx_id <= 0).
        assert_eq!(p.current_term(), 3);

        p.observe_term(5, 100);
        p.truncate_term_after(50);
        // Term 5's record (start_tx_id=100) was truncated; term 3 remains.
        assert_eq!(p.current_term(), 3);
    }
}
