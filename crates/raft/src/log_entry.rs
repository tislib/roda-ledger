//! Metadata shape for a single Raft log entry.
//!
//! Raft does not own log durability or payload routing — the driver
//! moves payload bytes between storage and the wire on its own. The
//! library only carries the `(tx_id, term)` pair: enough to perform
//! §5.3 prev_log_term matching, advance `next_index`/`match_index`,
//! and tell the storage layer which entries to append/truncate. The
//! actual payload is fetched/written by the driver, indexed by
//! `tx_id`.

use crate::types::{Term, TxId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogEntryMeta {
    /// Leader-assigned tx_id. Monotonic per leader window.
    pub tx_id: TxId,
    /// Term in which this entry was created.
    pub term: Term,
}

impl LogEntryMeta {
    pub fn new(tx_id: TxId, term: Term) -> Self {
        Self { tx_id, term }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_are_publicly_constructable() {
        let e = LogEntryMeta::new(7, 3);
        assert_eq!(e.tx_id, 7);
        assert_eq!(e.term, 3);
    }
}
