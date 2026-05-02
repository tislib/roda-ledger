//! Shared scalar types for the Raft library.
//!
//! Kept in their own module so every other module can `use crate::types::*`
//! without dragging in the full event/action machinery.

/// Stable cluster identifier for a single node. `0` is reserved for "no
/// node" — matches `storage::VoteRecord::voted_for == 0` semantics and
/// the wire-format guarantee that real ids are non-zero.
pub type NodeId = u64;

/// `tx_id` and `term` are both `u64`. Raft entries are addressed by
/// `tx_id` (assigned by the leader's ledger, ADR-0017 §"Log Ownership
/// Model"); `term` is the election term that produced them.
pub type TxId = u64;
pub type Term = u64;

/// Reason a peer rejected an `AppendEntries`. Carried back through
/// [`AppendResult::Reject`](crate::AppendResult::Reject) into
/// `Replication::peer(_).append_result(...)` so the leader can pick
/// the right recovery strategy (decrement `next_index` vs. step down
/// vs. retry).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectReason {
    /// `prev_log_term` did not match the follower's record at
    /// `prev_log_tx_id`. Leader §5.3-decrements `next_index` for this
    /// peer.
    LogMismatch,
    /// Peer's term is strictly higher than the request's. Leader
    /// must step down.
    TermBehind,
    /// RPC fired and never came back inside its deadline. Surfaced by
    /// the cluster driver via
    /// [`AppendResult::Timeout`](crate::AppendResult::Timeout); the
    /// reply handler clears `in_flight` and leaves indexes alone.
    RpcTimeout,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_reason_eq_is_value_based() {
        assert_eq!(RejectReason::LogMismatch, RejectReason::LogMismatch);
        assert_ne!(RejectReason::LogMismatch, RejectReason::TermBehind);
        assert_ne!(RejectReason::TermBehind, RejectReason::RpcTimeout);
    }
}
