//! `raft` — pure-state-machine Raft consensus library specialised to
//! roda-ledger. Implements ADR-0017.
//!
//! The library has no internal threads, no async runtime, **no I/O**.
//! Every transition is driven by feeding [`Event`]s into
//! [`RaftNode::step`] and applying the returned [`Action`]s in the
//! driver. Durable persistence of the term log and vote log lives
//! in the driver: raft emits [`Action::PersistTerm`] and
//! [`Action::PersistVote`]; the driver writes them and feeds back
//! [`Event::TermPersisted`] / [`Event::VotePersisted`].
//!
//! ADR-0017 §"Architectural Boundary":
//!
//! > The library transitions state and returns actions describing
//! > what the driver must do. The driver (cluster crate) executes
//! > actions and feeds results back as new events.
//!
//! ## Public surface
//!
//! - [`RaftNode`] / [`RaftInitialState`] — single-node decision
//!   system. `RaftNode::new` takes the durable state the driver
//!   hydrated from disk; the library stores nothing about
//!   filesystems itself.
//! - [`Event`] — every input the state machine reacts to.
//! - [`Action`] — every output the driver applies (durable writes,
//!   wire sends, log directives, role notifications, wakeup
//!   scheduling).
//! - [`Role`], [`RejectReason`], [`LogEntryMeta`], [`NodeId`],
//!   [`TermRecord`] — small value types the API returns.
//!
//! Anything not re-exported here is internal.

pub mod action;
pub mod candidate;
pub mod event;
pub mod follower;
pub mod leader;
pub mod log_entry;
pub mod node;
pub mod quorum;
pub mod role;
pub mod term;
pub mod timer;
pub mod types;
pub mod vote;

pub use action::Action;
pub use event::Event;
pub use log_entry::LogEntryMeta;
pub use node::{RaftConfig, RaftInitialState, RaftNode};
pub use role::Role;
pub use term::TermRecord;
pub use timer::ElectionTimerConfig;
pub use types::{NodeId, RejectReason, Term, TxId};
