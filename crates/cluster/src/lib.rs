//! Cluster mode (ADR-015 / ADR-017) — leader/follower replication layered
//! on top of `Ledger` without touching its internals. The raft state
//! machine lives in the separate `raft` crate; this crate is the
//! cluster-side I/O driver.

pub mod cluster_mirror;
mod command;
pub mod config;
mod consensus;
pub mod durable;
pub mod ledger_handler;
pub mod ledger_slot;
pub(crate) mod lifecycle;
pub mod mapping;
pub mod node;
pub mod node_handler;
mod raft_loop;
mod replication;
pub mod server;

pub use cluster_mirror::ClusterMirror;
pub use config::Config;
pub use durable::{DurablePersistence, Term, Vote};
pub use ledger_handler::LedgerHandler;
pub use ledger_slot::LedgerSlot;
pub use node_handler::{NodeHandler, NodeHandlerCore};
pub use raft::Role;
pub use server::{NodeServerRuntime, Server};
