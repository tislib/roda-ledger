//! Follower role-specific state.
//!
//! The follower's only Raft-relevant memory beyond the durable
//! `(current_term, voted_for)` is which node it currently believes is
//! leader — used to ignore stale `AppendEntries` from a previous
//! leader's late RPCs. The election timer lives on `RaftNode`
//! directly because Initializing also runs one.
//!
//! `pending_reply` parks an in-flight AppendEntries success reply
//! while the driver is still durably persisting the entries it just
//! got handed. Raft's write-ahead invariant requires the follower to
//! have the entries on disk before telling the leader it accepted
//! them; the library expresses that by deferring the reply until
//! `Event::LogAppendComplete` arrives.

use crate::types::{NodeId, Term, TxId};

/// A `SendAppendEntriesReply { success: true, .. }` parked on the
/// follower while the driver is durably persisting the freshly-
/// appended entries. Drained by `take_ready_reply` once the durability
/// ack reaches `last_commit_id`. `leader_commit` rides along so the
/// follower can apply Raft §5.3's `commit_index = min(leader_commit,
/// last_new_entry_index)` rule at the moment the entries actually
/// land on disk, not before.
///
/// Carries the two-watermark pair that goes back to the leader (see
/// ADR-0017 §"AE reply: write vs commit watermark"). Today the
/// driver waits for full durability before posting
/// `Event::LogAppendComplete`, so when the parked reply fires both
/// fields equal the same `parked_tx_id`. A future split-ack
/// architecture would let `last_write_id` drain ahead of
/// `last_commit_id`, and `take_ready_reply` would gate solely on the
/// commit ack.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingReply {
    pub to: NodeId,
    pub term: Term,
    pub last_commit_id: TxId,
    pub last_write_id: TxId,
    pub leader_commit: TxId,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FollowerState {
    /// `Some(id)` after a valid `AppendEntries` from `id`. Cleared
    /// only on transition out of Follower, not on heartbeat timeout
    /// — until the next leader speaks, the previous leader id
    /// remains the best guess for routing client redirects.
    pub leader_id: Option<NodeId>,
    /// Deferred AppendEntries success reply waiting on a
    /// `LogAppendComplete` durability ack from the driver. At most
    /// one in flight per follower because the leader serialises one
    /// AE per peer at a time and the follower processes them
    /// in arrival order.
    pub pending_reply: Option<PendingReply>,
}

impl FollowerState {
    pub fn new() -> Self {
        Self {
            leader_id: None,
            pending_reply: None,
        }
    }

    pub fn with_leader(leader_id: NodeId) -> Self {
        Self {
            leader_id: Some(leader_id),
            pending_reply: None,
        }
    }

    pub fn observe_leader(&mut self, leader_id: NodeId) {
        self.leader_id = Some(leader_id);
    }

    /// Park a success reply waiting for durability ack at `last_tx_id`.
    /// If a previous reply was already parked it is overwritten — the
    /// newer AE supersedes it (the next `LogAppendComplete` will be
    /// at least as far along).
    pub fn park_reply(&mut self, reply: PendingReply) {
        self.pending_reply = Some(reply);
    }

    /// If a parked reply's `last_commit_id` has been durably persisted
    /// (i.e. `acked_tx_id >= last_commit_id`), take it; otherwise leave
    /// it in place. Returns the reply ready to send.
    pub fn take_ready_reply(&mut self, acked_tx_id: TxId) -> Option<PendingReply> {
        match self.pending_reply {
            Some(p) if p.last_commit_id <= acked_tx_id => self.pending_reply.take(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_leader_records_id() {
        let mut s = FollowerState::new();
        assert_eq!(s.leader_id, None);
        s.observe_leader(7);
        assert_eq!(s.leader_id, Some(7));
        s.observe_leader(11);
        assert_eq!(s.leader_id, Some(11));
    }

    #[test]
    fn take_ready_reply_returns_none_when_unacked() {
        let mut s = FollowerState::new();
        s.park_reply(PendingReply {
            to: 2,
            term: 3,
            last_commit_id: 5,
            last_write_id: 5,
            leader_commit: 0,
        });
        assert_eq!(s.take_ready_reply(4), None);
        assert!(s.pending_reply.is_some());
    }

    #[test]
    fn take_ready_reply_drains_when_acked() {
        let mut s = FollowerState::new();
        let parked = PendingReply {
            to: 2,
            term: 3,
            last_commit_id: 5,
            last_write_id: 5,
            leader_commit: 0,
        };
        s.park_reply(parked);
        let drained = s.take_ready_reply(5);
        assert_eq!(drained, Some(parked));
        assert!(s.pending_reply.is_none());
    }

    #[test]
    fn take_ready_reply_drains_when_ack_passes_target() {
        let mut s = FollowerState::new();
        s.park_reply(PendingReply {
            to: 2,
            term: 3,
            last_commit_id: 5,
            last_write_id: 5,
            leader_commit: 0,
        });
        let drained = s.take_ready_reply(7);
        assert!(drained.is_some());
    }

    #[test]
    fn park_reply_overwrites_previous() {
        let mut s = FollowerState::new();
        s.park_reply(PendingReply {
            to: 2,
            term: 3,
            last_commit_id: 5,
            last_write_id: 5,
            leader_commit: 0,
        });
        s.park_reply(PendingReply {
            to: 2,
            term: 3,
            last_commit_id: 8,
            last_write_id: 8,
            leader_commit: 0,
        });
        assert_eq!(s.pending_reply.unwrap().last_commit_id, 8);
    }
}
