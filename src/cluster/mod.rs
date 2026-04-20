//! Cluster subsystem (ADR-015): node-proto gRPC service, follower-side
//! replication wiring, and the multi-node `cluster` binary's config
//! root. Isolated from the single-node client-facing `grpc` module so
//! the two services can be mounted on separate ports.

pub mod config;
pub mod handler;
pub mod server;

pub use config::{ClusterConfig, ClusterServerConfig, NodeMode, PeerConfig};
pub use handler::NodeHandler;
pub use server::ClusterServer;

/// Generated tonic types for `roda.node.v1`. Kept local to this module
/// so the rest of the crate doesn't need the node-proto symbols in its
/// public namespace.
pub mod proto {
    tonic::include_proto!("roda.node.v1");
}
