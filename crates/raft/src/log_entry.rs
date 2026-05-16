//! Range descriptor for a contiguous chunk of the log.
//!
//! Raft does not own log durability or payload routing — the driver
//! moves payload bytes between storage and the wire on its own. The
//! library only ever talks about *ranges*: a `start_tx_id` and a
//! `count`. The driver expands the range when it actually appends
//! entries to its WAL or copies them onto the wire.

use crate::types::TxId;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct LogEntryRange {
    pub start_tx_id: TxId,
    pub count: u64,
}

impl LogEntryRange {
    pub const fn empty() -> Self {
        Self {
            start_tx_id: 0,
            count: 0,
        }
    }

    pub const fn new(start_tx_id: TxId, count: u64) -> Self {
        Self { start_tx_id, count }
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

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
        let r = LogEntryRange::new(5, 3);
        assert!(!r.is_empty());
        assert_eq!(r.last_tx_id(), Some(7));
    }

    #[test]
    fn single_entry_range_has_last_equal_to_start() {
        let r = LogEntryRange::new(42, 1);
        assert_eq!(r.last_tx_id(), Some(42));
    }
}
