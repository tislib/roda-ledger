//! Raft consensus primitives — the algorithmic core decoupled from the
//! surrounding cluster plumbing (gRPC servers, ledger integration,
//! supervisor orchestration).
//!
//! Layering:
//!
//! - **Persistent state** — [`Term`], [`Vote`]: durable per-node election
//!   state backed by [`storage`].
//! - **Volatile role state** — [`Role`], [`RoleFlag`]: atomic role enum
//!   shared across handlers and the supervisor.
//! - **Election** — [`ElectionTimer`], [`run_election_round`]: randomized
//!   timeout + one-shot election driver.
//! - **Replication** — [`Leader`], [`PeerReplication`], [`Quorum`]: leader
//!   bring-up, per-peer `AppendEntries` shipping, and the lock-free
//!   majority-commit tracker.
//!
//! Consumers in `crate::cluster::` (supervisor, handlers, server, node)
//! reach into these types via the explicit `cluster::raft::` path —
//! `cluster::` does not re-export them at its root.

pub mod candidate;
pub mod election_timer;
pub mod leader;
pub mod peer_replication;
pub mod quorum;
pub mod role_flag;
pub mod term;
pub mod vote;

pub use candidate::{ElectionOutcome, run_election_round};
pub use election_timer::{ElectionTimer, ElectionTimerConfig};
pub use leader::{Leader, LeaderHandles};
pub use peer_replication::{PeerReplication, ReplicationParams};
pub use quorum::Quorum;
pub use role_flag::{Role, RoleFlag};
pub use term::Term;
pub use vote::Vote;
