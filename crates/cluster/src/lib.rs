//! Cluster mode (ADR-015) — leader/follower replication layered on top of
//! `Ledger` without touching its internals.

pub mod cluster_commit;
pub mod config;
pub mod ledger_handler;
pub mod ledger_slot;
pub(crate) mod lifecycle;
pub mod mapping;
pub mod node;
pub mod node_handler;
pub mod raft;
pub mod server;
pub mod supervisor;

pub use cluster_commit::ClusterCommitIndex;
pub use config::Config;
pub use ledger_handler::LedgerHandler;
pub use ledger_slot::LedgerSlot;
pub use node::ClusterNode;
pub use node_handler::{NodeHandler, NodeHandlerCore};
pub use raft::{Role, RoleFlag, Term, Vote};
pub use server::{NodeServerRuntime, Server};
