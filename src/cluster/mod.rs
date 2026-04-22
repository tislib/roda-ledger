//! Cluster mode (ADR-015) — leader/follower replication layered on top of
//! `Ledger` without touching its internals.
//!
//! The cluster owns both gRPC surfaces:
//!
//! - **Client-facing Ledger service** (`client_handler` + `client_server`)
//!   — the external submit/query API. On leaders it is writable; on
//!   followers it runs in read-only mode. A "single node" setup is just
//!   a cluster with zero peers.
//! - **Peer-facing Node service** (`node_server`) — leader ↔ follower
//!   replication RPCs (`AppendEntries`, `Ping`, future `RequestVote` /
//!   `InstallSnapshot`).
//!
//! The leader tails its own WAL via `WalTailer` and ships bytes to
//! followers via `AppendEntries`; followers decode and hand them to
//! `Ledger::append_wal_entries`.

pub mod client_handler;
pub mod client_server;
pub mod config;
pub mod follower;
pub mod leader;
pub mod node;
pub mod node_server;
pub mod peer_replication;
pub mod proto_mapping;
pub mod quorum;
pub mod term;

pub use crate::storage::TermRecord;
pub use client_handler::LedgerHandler;
pub use client_server::GrpcServer;
pub use config::{
    ClusterConfig, ClusterMode, GrpcServerSection, NodeServerSection, PeerConfig, ServerConfig,
    ServerConfigError,
};
pub use follower::{Follower, FollowerHandles};
pub use leader::{Leader, LeaderHandles};
pub use node::{Cluster, ClusterHandles};
pub use peer_replication::{PeerReplication, ReplicationParams};
pub use quorum::Quorum;
pub use term::Term;

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
