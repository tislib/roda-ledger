//! Atomic role state shared across handlers + supervisor (ADR-0016 §2).
//!
//! Every gRPC handler that needs to know "what role is this node
//! playing right now?" reads from a single `Arc<RoleFlag>`. The
//! [`crate::cluster::supervisor::RoleSupervisor`] is the only writer.
//! Lock-free reads on the hot path; transitions happen rarely
//! (boot, election, divergence reseed) and use an `AcqRel`
//! read-modify-write to publish a new role.
//!
//! `Role` mirrors the wire enum [`crate::cluster::proto::node::NodeRole`]
//! 1:1 so `Ping` / future `Status` responses can stamp the current
//! value without extra mapping layers.
//!
//! On boot in Stage 3b the supervisor decides the initial role:
//! - **Standalone** (`config.cluster.is_none()`) — never constructs a
//!   `RoleFlag`; the standalone path runs its writable client server
//!   without a role concept.
//! - **Single-node cluster** — `Role::Leader`.
//! - **Multi-node cluster** — `Role::Initializing`. Stage 4 elections
//!   transition out of this; in Stage 3b the node sits in
//!   Initializing forever (no leader to drive replication).

use crate::cluster::proto::node as proto;
use std::sync::atomic::{AtomicU8, Ordering};

/// Runtime role of a clustered node. Mirrors `proto::NodeRole`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Role {
    /// Pre-election bring-up state (and post-step-down state, post
    /// Stage 4). Node serves the peer-facing Node RPCs but the
    /// client-facing Ledger RPCs reject every write with
    /// `FAILED_PRECONDITION`.
    Initializing = proto::NodeRole::Recovering as u8,
    /// Writable leader — accepts client writes, ships
    /// `AppendEntries` to peers.
    Leader = proto::NodeRole::Leader as u8,
    /// Read-only follower — accepts incoming `AppendEntries`,
    /// rejects client writes with `FAILED_PRECONDITION`.
    Follower = proto::NodeRole::Follower as u8,
    /// Stage 4: between Initializing and Leader during an election.
    /// Reserved here so the wire protocol mapping is exhaustive.
    Candidate = 100, // distinct from any proto::NodeRole value
}

impl Role {
    #[inline]
    pub fn as_proto(self) -> proto::NodeRole {
        match self {
            Role::Initializing => proto::NodeRole::Recovering,
            Role::Leader => proto::NodeRole::Leader,
            Role::Follower => proto::NodeRole::Follower,
            // Candidate has no proto representation today; report as
            // `Recovering` (the closest "not serving writes" wire
            // signal) until ADR-016 adds a dedicated value.
            Role::Candidate => proto::NodeRole::Recovering,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            x if x == Role::Leader as u8 => Role::Leader,
            x if x == Role::Follower as u8 => Role::Follower,
            x if x == Role::Candidate as u8 => Role::Candidate,
            _ => Role::Initializing,
        }
    }
}

/// Shared atomic role pointer. All clones see the same underlying
/// state; transitions are visible to readers via `Acquire` loads.
pub struct RoleFlag {
    inner: AtomicU8,
}

impl RoleFlag {
    pub fn new(initial: Role) -> Self {
        Self {
            inner: AtomicU8::new(initial as u8),
        }
    }

    /// Lock-free `Acquire` read — used on every gRPC handler hot path.
    #[inline]
    pub fn get(&self) -> Role {
        Role::from_u8(self.inner.load(Ordering::Acquire))
    }

    /// `Release` write. Only the supervisor calls this.
    #[inline]
    pub fn set(&self, role: Role) {
        self.inner.store(role as u8, Ordering::Release);
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.get() == Role::Leader
    }

    #[inline]
    pub fn is_follower(&self) -> bool {
        self.get() == Role::Follower
    }

    #[inline]
    pub fn is_initializing(&self) -> bool {
        self.get() == Role::Initializing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_round_trips_through_atomic() {
        let f = RoleFlag::new(Role::Initializing);
        assert!(f.is_initializing());

        f.set(Role::Leader);
        assert!(f.is_leader());
        assert_eq!(f.get(), Role::Leader);

        f.set(Role::Follower);
        assert!(f.is_follower());
        assert_eq!(f.get().as_proto(), proto::NodeRole::Follower);

        f.set(Role::Candidate);
        // Candidate has no dedicated proto value yet; reports as Recovering.
        assert_eq!(f.get(), Role::Candidate);
        assert_eq!(f.get().as_proto(), proto::NodeRole::Recovering);
    }

    #[test]
    fn unknown_byte_falls_back_to_initializing() {
        // Defensive: the `from_u8` ladder must never panic on an
        // unexpected wire value.
        assert_eq!(Role::from_u8(99), Role::Initializing);
        assert_eq!(Role::from_u8(0), Role::Initializing);
    }
}
