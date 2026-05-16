//! `raft` — pure-state-machine Raft consensus library specialised to
//! roda-ledger. Implements ADR-0017.
//!
//! The library has no internal threads, no async runtime, **no I/O**
//! of its own. Driver-facing entry points:
//!
//! - [`RaftNode::election`] → [`Election`] — election-side driver.
//! - [`RaftNode::validate_handshake`] — follower-side handshake
//!   validation. Returns a [`HandshakeDecision`]. The only raft-side
//!   validation point in a replication stream.
//! - [`RaftNode::replication`] → [`Replication`] / [`PeerReplication`]
//!   — leader-side per-peer driver.
//! - [`RaftNode::advance_local_index`] / [`RaftNode::advance_cluster_index`]
//!   — driver-side commit-watermark advances. Local advance is the only
//!   path that mutates `local_commit_index` on a follower; cluster
//!   advance mirrors the leader's claim (clamped to local).

pub mod consensus;
pub mod follower;
pub mod leader;
pub mod log_entry;
pub mod node;
pub mod persistence;
pub mod quorum;
pub mod replication;
pub mod request_vote;
pub mod role;
pub mod timer;
pub mod types;

pub use consensus::{CandidateState, Election, VoteOutcome, Wakeup};
pub use log_entry::LogEntryRange;
pub use node::{HandshakeDecision, RaftConfig, RaftNode};
pub use persistence::{Persistence, TermRecord};
pub use replication::{AppendEntriesRequest, AppendResult, PeerReplication, Replication};
pub use request_vote::{RequestVote, RequestVoteResult};
pub use role::Role;
pub use timer::ElectionTimerConfig;
pub use types::{NodeId, RejectReason, Term, TxId};
