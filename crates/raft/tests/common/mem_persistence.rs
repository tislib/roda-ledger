//! In-memory `Persistence` implementation for tests and the simulator.
//! Fully deterministic, no I/O, no temp dirs.
//!
//! Mirrors the semantic contract of any disk-backed implementation
//! exactly — the library should behave identically against either.

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

    fn commit_term(&mut self, expected: TermNum, start_tx_id: TxId) -> bool {
        let current = self.current_term();
        if current >= expected {
            return false;
        }
        // Allow term-log skips: when the vote log raced ahead of the
        // term log (observed-higher-term-via-RPC, no entries yet) the
        // candidate may legitimately commit a term several steps
        // beyond `current`. The contract is `current < expected`,
        // not `current + 1 == expected`.
        self.term_log.push(TermRecord {
            term: expected,
            start_tx_id,
        });
        true
    }

    fn observe_term(&mut self, term: TermNum, start_tx_id: TxId) {
        let current = self.current_term();
        if term == current {
            return;
        }
        assert!(
            term > current,
            "mem_persistence: observe_term regression incoming={} current={}",
            term,
            current
        );
        self.term_log.push(TermRecord { term, start_tx_id });
    }

    fn truncate_term_after(&mut self, tx_id: TxId) {
        self.term_log.retain(|r| r.start_tx_id <= tx_id);
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

    fn vote(&mut self, term: TermNum, candidate_id: NodeId) -> bool {
        assert!(
            candidate_id != 0,
            "mem_persistence: candidate_id must be non-zero"
        );
        if term < self.vote_term {
            return false;
        }
        if term == self.vote_term {
            if self.voted_for == candidate_id {
                return true;
            }
            if self.voted_for != 0 {
                return false;
            }
        }
        self.vote_term = term;
        self.voted_for = candidate_id;
        true
    }

    fn observe_vote_term(&mut self, term: TermNum) {
        if term == self.vote_term {
            return;
        }
        assert!(
            term > self.vote_term,
            "mem_persistence: observe_vote_term regression incoming={} current={}",
            term,
            self.vote_term
        );
        self.vote_term = term;
        self.voted_for = 0;
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
        assert!(p.commit_term(1, 0));
        assert_eq!(p.current_term(), 1);
        p.observe_term(5, 50);
        assert_eq!(p.current_term(), 5);
    }

    #[test]
    fn commit_term_returns_false_when_already_advanced() {
        let mut p = MemPersistence::new();
        p.observe_term(5, 0);
        assert!(!p.commit_term(3, 0));
        assert_eq!(p.current_term(), 5);
    }

    /// `commit_term` accepts arbitrary forward jumps — the vote log
    /// is allowed to race ahead of the term log, and Raft never
    /// required term-log records to be contiguous.
    #[test]
    fn commit_term_accepts_forward_skips() {
        let mut p = MemPersistence::new();
        assert!(p.commit_term(5, 0));
        assert_eq!(p.current_term(), 5);
    }

    #[test]
    fn truncate_after_drops_higher_records() {
        let mut p = MemPersistence::new();
        p.commit_term(1, 0);
        p.commit_term(2, 50);
        p.commit_term(3, 100);
        p.truncate_term_after(75);
        assert_eq!(p.current_term(), 2);
    }

    #[test]
    fn term_at_tx_resolves_in_memory() {
        let mut p = MemPersistence::new();
        p.commit_term(1, 0);
        p.commit_term(2, 50);
        p.commit_term(3, 100);
        assert_eq!(p.term_at_tx(0).unwrap().term, 1);
        assert_eq!(p.term_at_tx(75).unwrap().term, 2);
        assert_eq!(p.term_at_tx(150).unwrap().term, 3);
    }

    #[test]
    fn vote_flow_matches_disk_semantics() {
        let mut p = MemPersistence::new();
        assert!(p.vote(1, 7));
        assert!(p.vote(1, 7)); // idempotent
        assert!(!p.vote(1, 9)); // different candidate refused
        p.observe_vote_term(2);
        assert_eq!(p.voted_for(), None);
        assert!(p.vote(2, 9));
    }

    #[test]
    #[should_panic(expected = "candidate_id must be non-zero")]
    fn vote_with_zero_candidate_id_panics() {
        let mut p = MemPersistence::new();
        let _ = p.vote(1, 0);
    }

    #[test]
    #[should_panic(expected = "observe_vote_term regression")]
    fn observe_vote_lower_term_panics() {
        let mut p = MemPersistence::new();
        p.observe_vote_term(5);
        p.observe_vote_term(4);
    }

    // ── term-log boundary semantics ─────────────────────────────────────────

    /// `commit_term(0)` returns `false` — `current >= expected`
    /// short-circuits before the lag check. Semantically a no-op.
    #[test]
    fn commit_term_zero_returns_false() {
        let mut p = MemPersistence::new();
        assert!(!p.commit_term(0, 0));
    }

    /// `commit_term(5)` after `observe(5)` is a refused stale-election.
    #[test]
    fn commit_term_after_same_observe_returns_false() {
        let mut p = MemPersistence::new();
        p.observe_term(5, 0);
        assert!(!p.commit_term(5, 0));
    }

    /// `commit_term(5)` after `observe(7)` is also refused — current
    /// has advanced past expected.
    #[test]
    fn commit_term_after_higher_observe_returns_false() {
        let mut p = MemPersistence::new();
        p.observe_term(7, 0);
        assert!(!p.commit_term(5, 0));
    }

    /// `term_at_tx(0)` when the only record starts at tx 0 returns it.
    #[test]
    fn term_at_tx_zero_with_zero_start_returns_record() {
        let mut p = MemPersistence::new();
        p.commit_term(1, 0);
        p.observe_term(5, 0);
        let r = p.term_at_tx(0).unwrap();
        assert_eq!(r.term, 5);
        assert_eq!(r.start_tx_id, 0);
    }

    /// `term_at_tx(0)` when the only record's start is past 0 returns None.
    #[test]
    fn term_at_tx_below_only_records_start_returns_none() {
        let mut p = MemPersistence::new();
        p.observe_term(5, 100);
        assert_eq!(p.term_at_tx(0), None);
    }

    /// `term_at_tx(u64::MAX)` returns the latest record.
    #[test]
    fn term_at_tx_max_returns_latest() {
        let mut p = MemPersistence::new();
        p.commit_term(1, 0);
        p.commit_term(2, 50);
        p.commit_term(3, 100);
        let r = p.term_at_tx(u64::MAX).unwrap();
        assert_eq!(r.term, 3);
        assert_eq!(r.start_tx_id, 100);
    }

    /// `truncate_after(u64::MAX)` is a no-op.
    #[test]
    fn truncate_after_u64_max_is_a_noop() {
        let mut p = MemPersistence::new();
        p.commit_term(1, 0);
        p.commit_term(2, 50);
        let before_term = p.current_term();
        p.truncate_term_after(u64::MAX);
        assert_eq!(p.current_term(), before_term);
    }

    // ── vote-log semantics ──────────────────────────────────────────────────

    /// Cross-term votes for the same node across an `observe_vote_term`
    /// gap.
    #[test]
    fn cross_term_vote_sequence_for_same_node() {
        let mut p = MemPersistence::new();
        assert!(p.vote(1, 7));
        p.observe_vote_term(2);
        assert!(p.vote(2, 7));
        assert_eq!(p.voted_for(), Some(7));
        // A vote at the older term is refused (vote-log term has
        // moved past it).
        assert!(!p.vote(1, 9));
    }

    /// `observe_vote_term` clears the slot; an immediate `vote` in
    /// the new term is granted.
    #[test]
    fn observe_vote_term_clears_then_vote_grants() {
        let mut p = MemPersistence::new();
        assert!(p.vote(7, 5));
        p.observe_vote_term(9);
        assert_eq!(p.voted_for(), None);
        assert!(p.vote(9, 11));
        assert_eq!(p.voted_for(), Some(11));
    }
}
