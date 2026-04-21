//! Cluster mode (ADR-015) — leader/follower replication layered on top of
//! `Ledger` without touching its internals.
//!
//! Two processes (or two instances inside a test) speak the `Node` gRPC
//! service on their own port, in addition to the client-facing `Ledger`
//! service. The leader tails its own WAL via `WalTailer` and ships bytes
//! to followers via `AppendEntries`; followers decode and hand them to
//! `Ledger::append_wal_entries`.

pub mod config;
pub mod node;
pub mod replication;
pub mod server;

pub use config::{ClusterConfig, ClusterMode, PeerConfig};
pub use node::Cluster;

pub mod proto {
    tonic::include_proto!("roda.node.v1");
}
