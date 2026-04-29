//! Lock-free atomic mirror of raft state for client-facing handlers.
//!
//! The `LedgerHandler` (client gRPC service) and any other reader that
//! must NOT block on the raft mutex queries `ClusterMirror` instead. The
//! mirror is updated by the raft driver immediately after every
//! successful `RaftNode::step`, while the driver still holds the raft
//! mutex — so a reader either sees pre-step or post-step state, never
//! an inconsistent middle (the mutex's release happens-before the
//! reader's `Acquire` load).
//!
//! Mirror reads are equivalent to but not synchronised with raft state;
//! the driver is the source of truth, and the mirror is a snapshot of
//! "raft state as of the last completed step."
//!
//! This type subsumes the legacy `RoleFlag` and `ClusterCommitIndex`
//! atomics; both are now folded into one shared snapshot.

use ::proto::node as proto;
use raft::Role;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};

const ROLE_INITIALIZING: u8 = 0;
const ROLE_FOLLOWER: u8 = 1;
const ROLE_CANDIDATE: u8 = 2;
const ROLE_LEADER: u8 = 3;

#[inline]
fn role_to_u8(r: Role) -> u8 {
    match r {
        Role::Initializing => ROLE_INITIALIZING,
        Role::Follower => ROLE_FOLLOWER,
        Role::Candidate => ROLE_CANDIDATE,
        Role::Leader => ROLE_LEADER,
    }
}

#[inline]
fn role_from_u8(v: u8) -> Role {
    match v {
        ROLE_LEADER => Role::Leader,
        ROLE_FOLLOWER => Role::Follower,
        ROLE_CANDIDATE => Role::Candidate,
        _ => Role::Initializing,
    }
}

/// Lock-free read surface mirroring the raft state machine. One per
/// cluster node; cloned via `Arc` to every consumer.
pub struct ClusterMirror {
    role: AtomicU8,
    current_term: AtomicU64,
    /// Local commit index (this node's durable log). May be ahead of
    /// `cluster_commit_index` on the leader (locally durable but not
    /// quorum-acked yet).
    commit_index: AtomicU64,
    /// Quorum-committed cluster watermark — the apply gate for
    /// `LedgerHandler::wait_for_transaction_level`.
    cluster_commit_index: AtomicU64,
    /// Best-known leader id; `0` for "no leader" (Initializing /
    /// Candidate, or freshly stepped-down).
    current_leader: AtomicU64,
    voted_for: AtomicU64,
    /// Flipped on `Action::FatalError`. Once set, the raft state machine
    /// is frozen and the supervisor must shut the node down.
    fatal: AtomicBool,
}

impl ClusterMirror {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            role: AtomicU8::new(ROLE_INITIALIZING),
            current_term: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            cluster_commit_index: AtomicU64::new(0),
            current_leader: AtomicU64::new(0),
            voted_for: AtomicU64::new(0),
            fatal: AtomicBool::new(false),
        })
    }

    #[inline]
    pub fn role(&self) -> Role {
        role_from_u8(self.role.load(Ordering::Acquire))
    }

    /// Wire-format role for `Ping` / `Status` responses. `Candidate` has
    /// no dedicated proto value and reports as `Recovering` (the closest
    /// "not serving writes" signal).
    #[inline]
    pub fn role_proto(&self) -> proto::NodeRole {
        match self.role() {
            Role::Initializing | Role::Candidate => proto::NodeRole::Recovering,
            Role::Follower => proto::NodeRole::Follower,
            Role::Leader => proto::NodeRole::Leader,
        }
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        matches!(self.role(), Role::Leader)
    }

    #[inline]
    pub fn is_follower(&self) -> bool {
        matches!(self.role(), Role::Follower)
    }

    #[inline]
    pub fn current_term(&self) -> u64 {
        self.current_term.load(Ordering::Acquire)
    }

    #[inline]
    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::Acquire)
    }

    #[inline]
    pub fn cluster_commit_index(&self) -> u64 {
        self.cluster_commit_index.load(Ordering::Acquire)
    }

    #[inline]
    pub fn current_leader(&self) -> Option<u64> {
        match self.current_leader.load(Ordering::Acquire) {
            0 => None,
            n => Some(n),
        }
    }

    #[inline]
    pub fn voted_for(&self) -> Option<u64> {
        match self.voted_for.load(Ordering::Acquire) {
            0 => None,
            n => Some(n),
        }
    }

    #[inline]
    pub fn is_fatal(&self) -> bool {
        self.fatal.load(Ordering::Acquire)
    }

    /// Snapshot raft state into the mirror. Called by the raft driver
    /// under the raft mutex, immediately after every `step` returns.
    /// Public to the cluster crate; external callers go through the
    /// driver.
    pub(crate) fn snapshot_from<P: raft::Persistence>(&self, node: &raft::RaftNode<P>) {
        self.role
            .store(role_to_u8(node.role()), Ordering::Release);
        self.current_term
            .store(node.current_term(), Ordering::Release);
        self.commit_index
            .store(node.commit_index(), Ordering::Release);
        self.cluster_commit_index
            .store(node.cluster_commit_index(), Ordering::Release);
        self.current_leader
            .store(node.current_leader().unwrap_or(0), Ordering::Release);
        self.voted_for
            .store(node.voted_for().unwrap_or(0), Ordering::Release);
    }

    /// Flip the fatal latch. Idempotent; only the driver calls this in
    /// response to `Action::FatalError`.
    pub(crate) fn set_fatal(&self) {
        self.fatal.store(true, Ordering::Release);
    }

    /// Pin the role for standalone mode (no raft running). The cluster
    /// path never calls this — the raft driver's `snapshot_from`
    /// updates the role from `RaftNode::role()` instead.
    pub fn set_role_for_standalone(&self, role: Role) {
        self.role.store(role_to_u8(role), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_mirror_has_default_state() {
        let m = ClusterMirror::new();
        assert_eq!(m.role(), Role::Initializing);
        assert_eq!(m.current_term(), 0);
        assert_eq!(m.commit_index(), 0);
        assert_eq!(m.cluster_commit_index(), 0);
        assert_eq!(m.current_leader(), None);
        assert_eq!(m.voted_for(), None);
        assert!(!m.is_fatal());
        assert!(!m.is_leader());
    }

    #[test]
    fn role_proto_maps_correctly() {
        let m = ClusterMirror::new();
        m.role.store(ROLE_LEADER, Ordering::Release);
        assert_eq!(m.role_proto(), proto::NodeRole::Leader);
        m.role.store(ROLE_FOLLOWER, Ordering::Release);
        assert_eq!(m.role_proto(), proto::NodeRole::Follower);
        m.role.store(ROLE_INITIALIZING, Ordering::Release);
        assert_eq!(m.role_proto(), proto::NodeRole::Recovering);
        m.role.store(ROLE_CANDIDATE, Ordering::Release);
        assert_eq!(m.role_proto(), proto::NodeRole::Recovering);
    }

    #[test]
    fn fatal_is_sticky() {
        let m = ClusterMirror::new();
        m.set_fatal();
        assert!(m.is_fatal());
    }
}
