//! Range descriptor for a contiguous, same-term chunk of the log.
//!
//! Raft does not own log durability or payload routing — the driver
//! moves payload bytes between storage and the wire on its own. The
//! library only ever talks about *ranges*: a `start_tx_id`, a
//! `count`, and the `term` shared by every entry in the chunk. The
//! driver expands the range when it actually appends entries to its
//! WAL or copies them onto the wire.
//!
//! ## Why one range, one term
//!
//! Raft's §5.3 prev-log-term check is a single (tx_id, term) pair.
//! Each AppendEntries carries one range, and that range is
//! constrained to a single term so the follower can update its
//! durable term log with one `observe_term` call (not N). When a
//! leader's log spans multiple terms it ships one AE per
//! same-term chunk, in order, walking the peer's `next_index`
//! forward across term boundaries.
//!
//! Ranges with `count == 0` represent heartbeats — no entries to
//! append, just the §5.3 / leader_commit propagation that an empty
//! AE carries.

use crate::types::{Term, TxId};

/// Contiguous, same-term chunk of the log: `[start_tx_id ..
/// start_tx_id + count)` with every entry at `term`. `count == 0`
/// is a valid heartbeat (no entries, just §5.3 + commit advance).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct LogEntryRange {
    pub start_tx_id: TxId,
    pub count: u64,
    pub term: Term,
}

impl LogEntryRange {
    /// Empty range — used for heartbeats.
    pub const fn empty() -> Self {
        Self {
            start_tx_id: 0,
            count: 0,
            term: 0,
        }
    }

    pub const fn new(start_tx_id: TxId, count: u64, term: Term) -> Self {
        Self {
            start_tx_id,
            count,
            term,
        }
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Largest `tx_id` covered by this range, or `None` if empty.
    #[inline]
    pub const fn last_tx_id(&self) -> Option<TxId> {
        if self.count == 0 {
            None
        } else {
            Some(self.start_tx_id + self.count - 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_range_has_no_last_tx_id() {
        let r = LogEntryRange::empty();
        assert!(r.is_empty());
        assert_eq!(r.last_tx_id(), None);
    }

    #[test]
    fn last_tx_id_is_start_plus_count_minus_one() {
        let r = LogEntryRange::new(5, 3, 9);
        assert!(!r.is_empty());
        assert_eq!(r.last_tx_id(), Some(7));
    }

    #[test]
    fn single_entry_range_has_last_equal_to_start() {
        let r = LogEntryRange::new(42, 1, 1);
        assert_eq!(r.last_tx_id(), Some(42));
    }
}
