//! Cluster mode (ADR-015) — leader/follower replication layered on top of
//! `Ledger` without touching its internals.
//!
//! The cluster owns both gRPC surfaces:
//!
//! - **Client-facing Ledger service** (`handler_ledger` + `server::Server`)
//!   — the external submit/query API. On leaders it is writable; on
//!   followers it runs in read-only mode. A "single node" setup is just
//!   a cluster with zero peers.
//! - **Peer-facing Node service** (`handler_node` + `server::NodeServerRuntime`)
//!   — leader ↔ follower replication RPCs (`AppendEntries`, `Ping`,
//!   future `RequestVote` / `InstallSnapshot`).
//!
//! The leader tails its own WAL via `WalTailer` and ships bytes to
//! followers via `AppendEntries`; followers decode and hand them to
//! `Ledger::append_wal_entries`.

pub mod candidate;
pub mod cluster_commit;
pub mod config;
pub mod election_timer;
pub mod leader;
pub mod ledger_slot;
pub mod ledger_handler;
pub mod mapping;
pub mod node;
pub mod node_handler;
pub mod peer_replication;
pub mod quorum;
pub mod role_flag;
pub mod server;
pub mod supervisor;
pub mod term;
pub mod vote;

pub use crate::storage::{TermRecord, VoteRecord};
pub use cluster_commit::ClusterCommitIndex;
pub use election_timer::{ElectionTimer, ElectionTimerConfig};
pub use config::{
    ClusterNodeSection, ClusterSection, Config, ConfigError, PeerConfig, ServerSection,
};
pub use leader::{Leader, LeaderHandles};
pub use ledger_handler::LedgerHandler;
pub use ledger_slot::LedgerSlot;
pub use node::{ClusterNode, Handles};
pub use node_handler::{NodeHandler, NodeHandlerCore};
pub use peer_replication::{PeerReplication, ReplicationParams};
pub use quorum::Quorum;
pub use role_flag::{Role, RoleFlag};
pub use server::{NodeServerRuntime, Server};
pub use supervisor::{RoleSupervisor, SupervisorHandles};
pub use term::Term;
pub use vote::Vote;

/// Generated protobuf types for both gRPC services owned by the cluster:
///
/// - [`proto::ledger`] — external client API (`LedgerClient`,
///   `SubmitOperationRequest`, balances, transaction status, WASM
///   registry, …). Generated from `proto/ledger.proto`.
/// - [`proto::node`] — peer-to-peer replication (`NodeClient`,
///   `AppendEntriesRequest`, `Ping`, …). Generated from
///   `proto/node.proto`.
pub mod proto {
    pub mod ledger {
        tonic::include_proto!("roda.ledger.v1");
    }
    pub mod node {
        tonic::include_proto!("roda.node.v1");
    }
}
