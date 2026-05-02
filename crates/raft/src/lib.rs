//! `raft` — pure-state-machine Raft consensus library specialised to
//! roda-ledger. Implements ADR-0017.
//!
//! The library has no internal threads, no async runtime, **no I/O**
//! of its own. Driver-facing entry points:
//!
//! - [`RaftNode::election`] → [`Election`] — election-side driver:
//!   `tick` (next deadline), `start` (transition to Candidate),
//!   `get_requests` (concurrent outbound `RequestVote`s),
//!   `handle_votes` (drain a batch of replies),
//!   `handle_request_vote` (voter-side direct method).
//! - [`RaftNode::validate_append_entries_request`] — follower-side
//!   inbound `AppendEntries`. Returns an [`AppendEntriesDecision`]
//!   describing what the cluster driver must do with its WAL.
//! - [`RaftNode::replication`] → [`Replication`] / [`PeerReplication`]
//!   — leader-side per-peer driver: `get_append_range(now)` to pull
//!   the next AE, `append_result(now, AppendResult)` to feed the
//!   reply back.
//! - [`RaftNode::advance`] — driver-side durability ack. Updates the
//!   local write/commit watermarks and (on followers) drains any
//!   deferred `leader_commit` into `cluster_commit_index`.
//!
//! Role transitions are observable via [`RaftNode::role`] polling —
//! there is no action stream.
//!
//! Durable state lives behind the synchronous [`Persistence`] trait.
//! Production wires a disk-backed implementation (term log + vote
//! log files); tests wire an in-memory fake. `RaftNode<P>` is
//! generic over the choice — the library itself never touches
//! `std::fs`.
//!
//! ## Public surface
//!
//! - [`RaftNode`] / [`RaftConfig`] — single-node decision system.
//!   Generic over `P: Persistence`.
//! - [`Election`] / [`VoteOutcome`] / [`CandidateState`] — election
//!   borrow view and its inputs.
//! - [`AppendEntriesDecision`] — the follower-path return shape from
//!   `validate_append_entries_request`.
//! - [`Persistence`] / [`TermRecord`] — durable-state contract.
//! - [`Role`], [`RejectReason`], [`LogEntryRange`], [`NodeId`] —
//!   small value types the API returns.
//!
//! Anything not re-exported here is internal.

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
pub use node::{AppendEntriesDecision, RaftConfig, RaftNode};
pub use persistence::{Persistence, TermRecord};
pub use replication::{AppendEntriesRequest, AppendResult, PeerReplication, Replication};
pub use request_vote::{RequestVoteReply, RequestVoteRequest};
pub use role::Role;
pub use timer::ElectionTimerConfig;
pub use types::{NodeId, RejectReason, Term, TxId};
