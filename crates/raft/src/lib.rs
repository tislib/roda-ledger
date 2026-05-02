//! `raft` — pure-state-machine Raft consensus library specialised to
//! roda-ledger. Implements ADR-0017.
//!
//! The library has no internal threads, no async runtime, **no I/O**
//! of its own. Three driver-facing entry points:
//!
//! - [`RaftNode::step`] — leader-side events (`Tick`,
//!   `AppendEntriesReply`, `RequestVoteRequest`, `RequestVoteReply`).
//!   Returns the [`Action`]s the driver must execute.
//! - [`RaftNode::validate_append_entries_request`] — follower-side
//!   inbound `AppendEntries`. Returns an [`AppendEntriesDecision`]
//!   describing what the cluster driver must do with its WAL.
//! - [`RaftNode::advance`] — driver-side durability ack. Updates the
//!   local write/commit watermarks and (on followers) drains any
//!   deferred `leader_commit` into `cluster_commit_index`.
//!
//! Durable state lives behind the synchronous [`Persistence`] trait.
//! Production wires a disk-backed implementation (term log + vote
//! log files); tests wire an in-memory fake. `RaftNode<P>` is
//! generic over the choice — the library itself never touches
//! `std::fs`.
//!
//! ADR-0017 §"Architectural Boundary":
//!
//! > The library transitions state and returns actions (or
//! > decisions) describing what the driver must do. The driver
//! > (cluster crate) executes them and feeds results back via the
//! > next entry-point call.
//!
//! ## Public surface
//!
//! - [`RaftNode`] / [`RaftConfig`] — single-node decision system.
//!   Generic over `P: Persistence`.
//! - [`AppendEntriesDecision`] — the follower-path return shape from
//!   `validate_append_entries_request`.
//! - [`Persistence`] / [`TermRecord`] — durable-state contract.
//! - [`Event`] — every input the state machine reacts to via `step`.
//! - [`Action`] — every output the driver applies (outbound wire
//!   sends, role notifications, wakeup scheduling).
//! - [`Role`], [`RejectReason`], [`LogEntryRange`], [`NodeId`] —
//!   small value types the API returns.
//!
//! Anything not re-exported here is internal.

pub mod action;
pub mod candidate;
pub mod event;
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

pub use action::Action;
pub use event::Event;
pub use log_entry::LogEntryRange;
pub use node::{AppendEntriesDecision, RaftConfig, RaftNode};
pub use persistence::{Persistence, TermRecord};
pub use replication::{AppendEntriesRequest, AppendResult, PeerReplication, Replication};
pub use request_vote::{RequestVoteReply, RequestVoteRequest};
pub use role::Role;
pub use timer::ElectionTimerConfig;
pub use types::{NodeId, RejectReason, Term, TxId};
