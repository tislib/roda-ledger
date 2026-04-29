//! `raft` — pure-state-machine Raft consensus library specialised to
//! roda-ledger. Implements ADR-0017.
//!
//! The library has no internal threads, no async runtime, **no I/O**
//! of its own. Every transition is driven by feeding [`Event`]s into
//! [`RaftNode::step`] and applying the returned [`Action`]s in the
//! driver.
//!
//! Durable state lives behind the synchronous [`Persistence`] trait.
//! Production wires a disk-backed implementation (term log + vote
//! log files); tests wire an in-memory fake. `RaftNode<P>` is
//! generic over the choice — the library itself never touches
//! `std::fs`.
//!
//! ADR-0017 §"Architectural Boundary":
//!
//! > The library transitions state and returns actions describing
//! > what the driver must do. The driver (cluster crate) executes
//! > actions and feeds results back as new events.
//!
//! ## Public surface
//!
//! - [`RaftNode`] / [`RaftConfig`] — single-node decision system.
//!   Generic over `P: Persistence`.
//! - [`Persistence`] / [`TermRecord`] — durable-state contract.
//! - [`Event`] — every input the state machine reacts to.
//! - [`Action`] — every output the driver applies (wire sends, log
//!   directives, role notifications, wakeup scheduling).
//! - [`Role`], [`RejectReason`], [`LogEntryMeta`], [`NodeId`] —
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
pub mod role;
pub mod timer;
pub mod types;

pub use action::Action;
pub use event::Event;
pub use log_entry::LogEntryRange;
pub use node::{RaftConfig, RaftNode};
pub use persistence::{Persistence, TermRecord};
pub use role::Role;
pub use timer::ElectionTimerConfig;
pub use types::{NodeId, RejectReason, Term, TxId};
