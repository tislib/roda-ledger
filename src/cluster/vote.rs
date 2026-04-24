//! In-memory vote semantics — durable state lives in
//! `storage::VoteStorage`; this layer adds the lock-free
//! `(current_term, voted_for)` atomics and the writer mutex that
//! serialises `vote.log` appends.
//!
//! Writers take the writer mutex, append via the storage layer (which
//! `fdatasync`s), then publish `current_term` / `voted_for` with
//! `Release` stores. Readers hit the atomics with `Acquire` loads.
//!
//! Ordering guarantee: "atomic value observed ⇒ record on disk".
//!
//! The `Vote` type tracks the Raft persistent vote state on its own
//! file, intentionally **separate** from `cluster::Term`:
//! - `Term` represents the term boundaries of the local log
//!   (`(term, start_tx_id)` pairs answering "what term covers tx N").
//! - `Vote` represents per-term election state
//!   (`(term, voted_for)` answering "did we vote in this term").
//!
//! Mixing them in one file produced records-per-term ambiguity and
//! polluted the term-boundary lookup path with election churn.
//!
//! `voted_for == 0` represents the absence of a vote in the current
//! term. Real node ids are non-zero (config validation guarantees this
//! starting in ADR-0016 §1).

use crate::storage::{Storage, VoteRecord, VoteStorage};
use spdlog::info;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Durable + in-memory vote state. Clone via `Arc<Vote>` to share
/// across the supervisor, the candidate loop, and the `RequestVote`
/// handler.
pub struct Vote {
    /// Current durable term as observed by the vote layer. May lag
    /// `Term::get_current_term()` momentarily; both converge through
    /// `observe_term`. `Release`-stored after every successful append.
    current_term: AtomicU64,
    /// Node id we voted for in `current_term`, or `0` for "no vote".
    /// Always written together with `current_term` while holding the
    /// writer mutex.
    voted_for: AtomicU64,
    /// Writer serialisation. Ordering:
    ///     1. take `writer`,
    ///     2. `storage.append` (fdatasync),
    ///     3. atomic stores.
    writer: Mutex<VoteStorage>,
}

impl Vote {
    /// Open `{storage.data_dir}/vote.log` and hydrate
    /// `(current_term, voted_for)` from the last record. On first boot
    /// the file is empty; both atomics stay at 0.
    pub fn open(storage: &Arc<Storage>) -> io::Result<Self> {
        Self::open_in_dir(&storage.config().data_dir)
    }

    /// Lower-level constructor for tests + tools that don't hold a full `Storage`.
    pub fn open_in_dir(data_dir: &str) -> io::Result<Self> {
        let mut file = VoteStorage::open(data_dir)?;
        let last = file.last_record()?;
        let (term, voted_for) = match last {
            Some(rec) => (rec.term, rec.voted_for),
            None => (0, 0),
        };
        info!(
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

    /// Node we voted for in the current term, or `None` if we have not
    /// voted in this term. Lock-free, `Acquire`-ordered.
    #[inline]
    pub fn get_voted_for(&self) -> Option<u64> {
        match self.voted_for.load(Ordering::Acquire) {
            0 => None,
            n => Some(n),
        }
    }

    /// Raft §5.4.1: durably grant a vote for `candidate_id` in `term`.
    ///
    /// Returns:
    /// - `Ok(true)` if the vote was granted (either no prior vote in
    ///   this term, or the prior vote was for the same candidate —
    ///   idempotent).
    /// - `Ok(false)` if we already voted for a different candidate in
    ///   this term, or `term` is older than `current_term`.
    /// - `Err(_)` on bad input or I/O failure during the durable append.
    ///
    /// On grant, the record is fdatasync'd before the function
    /// returns, guaranteeing the vote survives a crash before the
    /// caller replies to the candidate.
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
            // No vote yet in this term — fall through and grant.
        }

        let rec = VoteRecord {
            term,
            voted_for: candidate_id,
        };
        writer.append(rec)?;
        self.voted_for.store(candidate_id, Ordering::Release);
        self.current_term.store(term, Ordering::Release);
        info!(
            "vote: granted term={} candidate_id={} ({})",
            term,
            candidate_id,
            writer.path().display(),
        );
        Ok(true)
    }

    /// Observe a term advance from an incoming RPC (typically
    /// `AppendEntries` from a new leader, or any response carrying a
    /// higher term). If `term > current_term`, records a "no-vote"
    /// (`voted_for = 0`) record durably so the next vote in this term
    /// is still allowed. Idempotent on `term == current_term`. Strict
    /// regressions are an error.
    pub fn observe_term(&self, term: u64) -> io::Result<()> {
        let current = self.current_term.load(Ordering::Acquire);
        if term == current {
            return Ok(());
        }
        if term < current {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("vote: term regression incoming={} current={}", term, current),
            ));
        }

        let mut writer = self.writer.lock().expect("vote: writer mutex poisoned");
        let current = self.current_term.load(Ordering::Acquire);
        if term <= current {
            return Ok(());
        }

        let rec = VoteRecord {
            term,
            voted_for: 0,
        };
        writer.append(rec)?;
        self.voted_for.store(0, Ordering::Release);
        self.current_term.store(term, Ordering::Release);
        info!(
            "vote: observed higher term={} (cleared voted_for) ({})",
            term,
            writer.path().display(),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_in_tmp() -> (TempDir, Vote) {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        let vote = Vote::open_in_dir(&dir).unwrap();
        (td, vote)
    }

    #[test]
    fn fresh_log_starts_unvoted_at_term_zero() {
        let (_td, vote) = open_in_tmp();
        assert_eq!(vote.get_current_term(), 0);
        assert_eq!(vote.get_voted_for(), None);
    }

    #[test]
    fn first_vote_in_term_is_granted_and_durable() {
        let (_td, vote) = open_in_tmp();
        assert!(vote.vote(1, 7).unwrap());
        assert_eq!(vote.get_current_term(), 1);
        assert_eq!(vote.get_voted_for(), Some(7));
    }

    #[test]
    fn same_term_same_candidate_is_idempotent() {
        let (_td, vote) = open_in_tmp();
        assert!(vote.vote(3, 9).unwrap());
        assert!(vote.vote(3, 9).unwrap());
        assert_eq!(vote.get_voted_for(), Some(9));
    }

    #[test]
    fn same_term_different_candidate_is_refused() {
        let (_td, vote) = open_in_tmp();
        assert!(vote.vote(3, 9).unwrap());
        assert!(!vote.vote(3, 11).unwrap());
        assert_eq!(vote.get_voted_for(), Some(9));
    }

    #[test]
    fn older_term_vote_request_is_refused() {
        let (_td, vote) = open_in_tmp();
        assert!(vote.vote(5, 1).unwrap());
        assert!(!vote.vote(4, 2).unwrap());
        assert_eq!(vote.get_current_term(), 5);
        assert_eq!(vote.get_voted_for(), Some(1));
    }

    #[test]
    fn higher_term_vote_advances_term_and_records_grant() {
        let (_td, vote) = open_in_tmp();
        assert!(vote.vote(1, 1).unwrap());
        assert!(vote.vote(2, 7).unwrap());
        assert_eq!(vote.get_current_term(), 2);
        assert_eq!(vote.get_voted_for(), Some(7));
    }

    #[test]
    fn observe_higher_term_clears_voted_for() {
        let (_td, vote) = open_in_tmp();
        assert!(vote.vote(1, 1).unwrap());
        vote.observe_term(2).unwrap();
        assert_eq!(vote.get_current_term(), 2);
        assert_eq!(vote.get_voted_for(), None);
        // And we can grant a new vote in the new term.
        assert!(vote.vote(2, 5).unwrap());
        assert_eq!(vote.get_voted_for(), Some(5));
    }

    #[test]
    fn observe_same_term_is_idempotent() {
        let (_td, vote) = open_in_tmp();
        assert!(vote.vote(3, 4).unwrap());
        vote.observe_term(3).unwrap();
        assert_eq!(vote.get_current_term(), 3);
        assert_eq!(vote.get_voted_for(), Some(4));
    }

    #[test]
    fn observe_lower_term_is_rejected() {
        let (_td, vote) = open_in_tmp();
        assert!(vote.vote(5, 1).unwrap());
        let err = vote.observe_term(4).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn zero_candidate_id_is_rejected() {
        let (_td, vote) = open_in_tmp();
        let err = vote.vote(1, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn reopen_recovers_term_and_voted_for() {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        {
            let vote = Vote::open_in_dir(&dir).unwrap();
            assert!(vote.vote(7, 12).unwrap());
        }
        let vote = Vote::open_in_dir(&dir).unwrap();
        assert_eq!(vote.get_current_term(), 7);
        assert_eq!(vote.get_voted_for(), Some(12));
    }

    #[test]
    fn reopen_after_observe_recovers_cleared_vote() {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_string_lossy().into_owned();
        {
            let vote = Vote::open_in_dir(&dir).unwrap();
            assert!(vote.vote(5, 3).unwrap());
            vote.observe_term(6).unwrap();
        }
        let vote = Vote::open_in_dir(&dir).unwrap();
        assert_eq!(vote.get_current_term(), 6);
        assert_eq!(vote.get_voted_for(), None);
    }
}
