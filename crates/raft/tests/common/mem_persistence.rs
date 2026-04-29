//! In-memory `Persistence` implementation for tests and the simulator.
//! Fully deterministic, no I/O, no temp dirs.
//!
//! Mirrors the semantic contract of any disk-backed implementation
//! exactly — the library should behave identically against either.

use std::io;

use raft::types::Term as TermNum;
use raft::{NodeId, Persistence, TermRecord, TxId};

#[derive(Clone, Default)]
pub struct MemPersistence {
    term_log: Vec<TermRecord>,
    vote_term: TermNum,
    voted_for: NodeId,
}

impl MemPersistence {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-load state to simulate restart from a previous run.
    #[allow(dead_code)]
    pub fn with_state(term_log: Vec<TermRecord>, vote_term: TermNum, voted_for: NodeId) -> Self {
        Self {
            term_log,
            vote_term,
            voted_for,
        }
    }
}

impl Persistence for MemPersistence {
    fn current_term(&self) -> TermNum {
        // Term-log term only — `current_term` and `vote_term` may
        // diverge while a candidate has self-voted for a term it
        // hasn't won. The library reads `max(current_term, vote_term)`
        // for "what term am I in".
        self.term_log.last().map(|r| r.term).unwrap_or(0)
    }

    fn last_term_record(&self) -> Option<TermRecord> {
        self.term_log.last().copied()
    }

    fn term_at_tx(&self, tx_id: TxId) -> Option<TermRecord> {
        let mut best: Option<TermRecord> = None;
        for rec in &self.term_log {
            if rec.start_tx_id > tx_id {
                continue;
            }
            best = match best {
                Some(b) if b.term >= rec.term => Some(b),
                _ => Some(*rec),
            };
        }
        best
    }

    fn commit_term(&mut self, expected: TermNum, start_tx_id: TxId) -> io::Result<bool> {
        let current = self.current_term();
        if current >= expected {
            return Ok(false);
        }
        if current + 1 != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "mem_persistence: commit_term current={} expected={} (must observe first)",
                    current, expected
                ),
            ));
        }
        self.term_log.push(TermRecord {
            term: expected,
            start_tx_id,
        });
        Ok(true)
    }

    fn observe_term(&mut self, term: TermNum, start_tx_id: TxId) -> io::Result<()> {
        let current = self.current_term();
        if term == current {
            return Ok(());
        }
        if term < current {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "mem_persistence: observe_term regression incoming={} current={}",
                    term, current
                ),
            ));
        }
        self.term_log.push(TermRecord { term, start_tx_id });
        Ok(())
    }

    fn truncate_term_after(&mut self, tx_id: TxId) -> io::Result<()> {
        self.term_log.retain(|r| r.start_tx_id <= tx_id);
        Ok(())
    }

    fn vote_term(&self) -> TermNum {
        self.vote_term
    }

    fn voted_for(&self) -> Option<NodeId> {
        match self.voted_for {
            0 => None,
            n => Some(n),
        }
    }

    fn vote(&mut self, term: TermNum, candidate_id: NodeId) -> io::Result<bool> {
        if candidate_id == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mem_persistence: candidate_id must be non-zero",
            ));
        }
        if term < self.vote_term {
            return Ok(false);
        }
        if term == self.vote_term {
            if self.voted_for == candidate_id {
                return Ok(true);
            }
            if self.voted_for != 0 {
                return Ok(false);
            }
        }
        self.vote_term = term;
        self.voted_for = candidate_id;
        Ok(true)
    }

    fn observe_vote_term(&mut self, term: TermNum) -> io::Result<()> {
        if term == self.vote_term {
            return Ok(());
        }
        if term < self.vote_term {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "mem_persistence: observe_vote_term regression incoming={} current={}",
                    term, self.vote_term
                ),
            ));
        }
        self.vote_term = term;
        self.voted_for = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_starts_at_zero() {
        let p = MemPersistence::new();
        assert_eq!(p.current_term(), 0);
        assert_eq!(p.voted_for(), None);
        assert_eq!(p.last_term_record(), None);
    }

    #[test]
    fn commit_term_then_observe_advances() {
        let mut p = MemPersistence::new();
        assert!(p.commit_term(1, 0).unwrap());
        assert_eq!(p.current_term(), 1);
        p.observe_term(5, 50).unwrap();
        assert_eq!(p.current_term(), 5);
    }

    #[test]
    fn commit_term_returns_false_when_already_advanced() {
        let mut p = MemPersistence::new();
        p.observe_term(5, 0).unwrap();
        assert!(!p.commit_term(3, 0).unwrap());
        assert_eq!(p.current_term(), 5);
    }

    #[test]
    fn commit_term_errors_on_lag() {
        let mut p = MemPersistence::new();
        let err = p.commit_term(5, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn truncate_after_drops_higher_records() {
        let mut p = MemPersistence::new();
        p.commit_term(1, 0).unwrap();
        p.commit_term(2, 50).unwrap();
        p.commit_term(3, 100).unwrap();
        p.truncate_term_after(75).unwrap();
        assert_eq!(p.current_term(), 2);
    }

    #[test]
    fn term_at_tx_resolves_in_memory() {
        let mut p = MemPersistence::new();
        p.commit_term(1, 0).unwrap();
        p.commit_term(2, 50).unwrap();
        p.commit_term(3, 100).unwrap();
        assert_eq!(p.term_at_tx(0).unwrap().term, 1);
        assert_eq!(p.term_at_tx(75).unwrap().term, 2);
        assert_eq!(p.term_at_tx(150).unwrap().term, 3);
    }

    #[test]
    fn vote_flow_matches_disk_semantics() {
        let mut p = MemPersistence::new();
        assert!(p.vote(1, 7).unwrap());
        assert!(p.vote(1, 7).unwrap()); // idempotent
        assert!(!p.vote(1, 9).unwrap()); // different candidate refused
        p.observe_vote_term(2).unwrap();
        assert_eq!(p.voted_for(), None);
        assert!(p.vote(2, 9).unwrap());
    }

    #[test]
    fn vote_with_zero_candidate_id_errors() {
        let mut p = MemPersistence::new();
        let err = p.vote(1, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn observe_vote_lower_term_errors() {
        let mut p = MemPersistence::new();
        p.observe_vote_term(5).unwrap();
        let err = p.observe_vote_term(4).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    // ── term-log boundary semantics ─────────────────────────────────────────

    /// `commit_term(0)` returns `Ok(false)` — `current >= expected`
    /// short-circuits before the lag check. Semantically a no-op.
    #[test]
    fn commit_term_zero_returns_false() {
        let mut p = MemPersistence::new();
        assert!(!p.commit_term(0, 0).unwrap());
    }

    /// `commit_term(5)` after `observe(5)` is a refused stale-election.
    #[test]
    fn commit_term_after_same_observe_returns_false() {
        let mut p = MemPersistence::new();
        p.observe_term(5, 0).unwrap();
        assert!(!p.commit_term(5, 0).unwrap());
    }

    /// `commit_term(5)` after `observe(7)` is also refused — current
    /// has advanced past expected.
    #[test]
    fn commit_term_after_higher_observe_returns_false() {
        let mut p = MemPersistence::new();
        p.observe_term(7, 0).unwrap();
        assert!(!p.commit_term(5, 0).unwrap());
    }

    /// `term_at_tx(0)` when the only record starts at tx 0 returns it.
    #[test]
    fn term_at_tx_zero_with_zero_start_returns_record() {
        let mut p = MemPersistence::new();
        p.commit_term(1, 0).unwrap();
        p.observe_term(5, 0).unwrap();
        let r = p.term_at_tx(0).unwrap();
        assert_eq!(r.term, 5);
        assert_eq!(r.start_tx_id, 0);
    }

    /// `term_at_tx(0)` when the only record's start is past 0 returns None.
    #[test]
    fn term_at_tx_below_only_records_start_returns_none() {
        let mut p = MemPersistence::new();
        p.observe_term(5, 100).unwrap();
        assert_eq!(p.term_at_tx(0), None);
    }

    /// `term_at_tx(u64::MAX)` returns the latest record.
    #[test]
    fn term_at_tx_max_returns_latest() {
        let mut p = MemPersistence::new();
        p.commit_term(1, 0).unwrap();
        p.commit_term(2, 50).unwrap();
        p.commit_term(3, 100).unwrap();
        let r = p.term_at_tx(u64::MAX).unwrap();
        assert_eq!(r.term, 3);
        assert_eq!(r.start_tx_id, 100);
    }

    /// `truncate_after(u64::MAX)` is a no-op.
    #[test]
    fn truncate_after_u64_max_is_a_noop() {
        let mut p = MemPersistence::new();
        p.commit_term(1, 0).unwrap();
        p.commit_term(2, 50).unwrap();
        let before_term = p.current_term();
        p.truncate_term_after(u64::MAX).unwrap();
        assert_eq!(p.current_term(), before_term);
    }

    // ── vote-log semantics ──────────────────────────────────────────────────

    /// Cross-term votes for the same node across an `observe_vote_term`
    /// gap.
    #[test]
    fn cross_term_vote_sequence_for_same_node() {
        let mut p = MemPersistence::new();
        assert!(p.vote(1, 7).unwrap());
        p.observe_vote_term(2).unwrap();
        assert!(p.vote(2, 7).unwrap());
        assert_eq!(p.voted_for(), Some(7));
        // A vote at the older term is refused (vote-log term has
        // moved past it).
        assert!(!p.vote(1, 9).unwrap());
    }

    /// `observe_vote_term` clears the slot; an immediate `vote` in
    /// the new term is granted.
    #[test]
    fn observe_vote_term_clears_then_vote_grants() {
        let mut p = MemPersistence::new();
        assert!(p.vote(7, 5).unwrap());
        p.observe_vote_term(9).unwrap();
        assert_eq!(p.voted_for(), None);
        assert!(p.vote(9, 11).unwrap());
        assert_eq!(p.voted_for(), Some(11));
    }
}
