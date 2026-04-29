//! Pure in-memory term-log model.
//!
//! ADR-0017: the library performs no I/O. The driver owns disk-side
//! persistence; this module models only the state machine's view of
//! "which term covered which `tx_id` range." Mutators update the
//! in-memory record list and the caller (`RaftNode`) emits a matching
//! `Action::PersistTerm` (or `Action::TruncateLog`, which carries the
//! pair-truncation hint) so the driver can persist.

use crate::types::{Term as TermNum, TxId};

/// One term-log record. The library's analogue of the on-disk record
/// format the driver eventually writes — but without any encoding or
/// CRC concerns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TermRecord {
    pub term: TermNum,
    pub start_tx_id: TxId,
}

/// Library-internal term log. Single-threaded — `RaftNode` is the
/// sole owner. No I/O; the driver mirrors writes via the action
/// stream.
pub struct Term {
    /// Records in append order. Truncation drops the suffix.
    records: Vec<TermRecord>,
    /// `records.last().term` (or 0). Cached so the read API doesn't
    /// repeatedly index into `records`.
    current_term: TermNum,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitTermError {
    /// Caller asked to commit `expected` but `current + 1 < expected` —
    /// they need to call `observe(expected - 1, _)` first. Programming
    /// error from the candidate path.
    Lag { current: TermNum, expected: TermNum },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObserveError {
    /// Strict regression: incoming term is less than the one we have.
    /// The caller is expected to drop the inbound RPC instead.
    Regression { current: TermNum, incoming: TermNum },
}

impl Term {
    /// New, empty term log. Used when the driver opens a fresh node.
    pub fn empty() -> Self {
        Self {
            records: Vec::new(),
            current_term: 0,
        }
    }

    /// Construct from records the driver hydrated from disk. Records
    /// must be in append order (oldest first); the constructor does
    /// not sort.
    pub fn from_records(records: Vec<TermRecord>) -> Self {
        let current_term = records.last().map(|r| r.term).unwrap_or(0);
        Self {
            records,
            current_term,
        }
    }

    #[inline]
    pub fn current_term(&self) -> TermNum {
        self.current_term
    }

    #[inline]
    pub fn last_record(&self) -> Option<TermRecord> {
        self.records.last().copied()
    }

    /// Read-only borrow of the in-memory record list. The driver may
    /// use this for replay debugging or invariant checks.
    pub fn records(&self) -> &[TermRecord] {
        &self.records
    }

    /// Look up the term that was active at `tx_id`: the record with
    /// the largest `start_tx_id <= tx_id`. Returns `None` if `tx_id`
    /// predates every record (or the log is empty).
    pub fn term_at_tx(&self, tx_id: TxId) -> Option<TermRecord> {
        let mut best: Option<TermRecord> = None;
        for rec in &self.records {
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

    /// Election-win path: stamp the term boundary at `expected` only
    /// when `current + 1 == expected`. The strict equality check is
    /// the §5.4 "Election Safety" invariant in ADR-0017.
    ///
    /// - `Ok(true)` — appended `(expected, start_tx_id)` to the
    ///   in-memory log.
    /// - `Ok(false)` — `current >= expected`; the candidate raced an
    ///   observer and must step down.
    /// - `Err(Lag)` — caller bumped term in memory without observing
    ///   intermediate; programming error.
    pub fn commit_term(
        &mut self,
        expected: TermNum,
        start_tx_id: TxId,
    ) -> Result<bool, CommitTermError> {
        if self.current_term >= expected {
            return Ok(false);
        }
        if self.current_term + 1 != expected {
            return Err(CommitTermError::Lag {
                current: self.current_term,
                expected,
            });
        }
        self.records.push(TermRecord {
            term: expected,
            start_tx_id,
        });
        self.current_term = expected;
        Ok(true)
    }

    /// Follower path: record a higher `term` observed via inbound
    /// RPC. Idempotent on equal term. `start_tx_id` is the tx_id of
    /// the first entry in this term — for catch-up boundaries from
    /// vote-only observations this is `local_log_index + 1` (no
    /// existing entry is shadowed).
    pub fn observe(&mut self, term: TermNum, start_tx_id: TxId) -> Result<(), ObserveError> {
        if term == self.current_term {
            return Ok(());
        }
        if term < self.current_term {
            return Err(ObserveError::Regression {
                current: self.current_term,
                incoming: term,
            });
        }
        self.records.push(TermRecord { term, start_tx_id });
        self.current_term = term;
        Ok(())
    }

    /// Drop every term record whose `start_tx_id > tx_id`. Pairs
    /// with the log-suffix truncation Raft §5.3 demands. No I/O —
    /// the driver mirrors the truncation when it processes
    /// `Action::TruncateLog`.
    pub fn truncate_after(&mut self, tx_id: TxId) {
        self.records.retain(|r| r.start_tx_id <= tx_id);
        self.current_term = self.records.last().map(|r| r.term).unwrap_or(0);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_log_starts_at_zero() {
        let term = Term::empty();
        assert_eq!(term.current_term(), 0);
        assert_eq!(term.last_record(), None);
        assert_eq!(term.term_at_tx(100), None);
    }

    #[test]
    fn from_records_recovers_current_term() {
        let term = Term::from_records(vec![
            TermRecord {
                term: 1,
                start_tx_id: 0,
            },
            TermRecord {
                term: 2,
                start_tx_id: 50,
            },
        ]);
        assert_eq!(term.current_term(), 2);
        assert_eq!(term.len(), 2);
    }

    #[test]
    fn commit_term_writes_at_expected_when_current_plus_one() {
        let mut term = Term::empty();
        assert!(term.commit_term(1, 0).unwrap());
        assert_eq!(term.current_term(), 1);
        assert!(term.commit_term(2, 50).unwrap());
        assert_eq!(term.current_term(), 2);
    }

    #[test]
    fn commit_term_returns_false_when_already_advanced() {
        let mut term = Term::empty();
        term.observe(5, 1).unwrap();
        assert!(!term.commit_term(3, 0).unwrap());
        assert_eq!(term.current_term(), 5);
        assert!(!term.commit_term(5, 0).unwrap());
    }

    #[test]
    fn commit_term_errors_on_lag() {
        let mut term = Term::empty();
        let err = term.commit_term(5, 0).unwrap_err();
        assert!(matches!(
            err,
            CommitTermError::Lag {
                current: 0,
                expected: 5
            }
        ));
        assert_eq!(term.current_term(), 0);
    }

    #[test]
    fn observe_advances_strict_higher_terms() {
        let mut term = Term::empty();
        term.observe(3, 1).unwrap();
        term.observe(3, 1).unwrap();
        term.observe(7, 50).unwrap();
        let err = term.observe(6, 100).unwrap_err();
        assert!(matches!(err, ObserveError::Regression { .. }));
        assert_eq!(term.current_term(), 7);
    }

    #[test]
    fn term_at_tx_resolves_in_memory() {
        let mut term = Term::empty();
        term.commit_term(1, 0).unwrap();
        term.commit_term(2, 50).unwrap();
        term.commit_term(3, 100).unwrap();
        assert_eq!(term.term_at_tx(0).unwrap().term, 1);
        assert_eq!(term.term_at_tx(49).unwrap().term, 1);
        assert_eq!(term.term_at_tx(75).unwrap().term, 2);
        assert_eq!(term.term_at_tx(150).unwrap().term, 3);
    }

    #[test]
    fn truncate_after_drops_records_strictly_past_tx_id() {
        let mut term = Term::empty();
        term.commit_term(1, 0).unwrap();
        term.commit_term(2, 50).unwrap();
        term.commit_term(3, 100).unwrap();

        term.truncate_after(75);

        assert_eq!(term.current_term(), 2);
        assert_eq!(term.len(), 2);
        assert_eq!(term.term_at_tx(60).unwrap().term, 2);
    }

    #[test]
    fn truncate_after_zero_drops_everything_above() {
        let mut term = Term::empty();
        term.commit_term(1, 0).unwrap();
        term.commit_term(2, 50).unwrap();
        term.truncate_after(0);
        // Term 1 (start_tx_id 0) survives because 0 <= 0; term 2 goes.
        assert_eq!(term.current_term(), 1);
        assert_eq!(term.len(), 1);
    }

    #[test]
    fn truncate_after_is_a_noop_when_nothing_strictly_past() {
        let mut term = Term::empty();
        term.commit_term(1, 0).unwrap();
        term.commit_term(2, 50).unwrap();
        term.truncate_after(50);
        assert_eq!(term.current_term(), 2);
        assert_eq!(term.len(), 2);
    }
}
