//! Control plane backed by a real cluster.
//!
//! Implements `roda.control.v1.Control` over a tonic gRPC server with
//! `tonic-web` for browser compatibility (so `@connectrpc/connect-web`
//! can reach it from a Vite dev server). State is split between a
//! long-lived [`cluster_handle::ClusterHandle`] (provisioner +
//! [`client::ClusterClient`]) and an in-memory [`event_store::EventStore`]
//! for ephemeral history (faults, scenario runs, recent submissions).
//!
//! The cluster outlives individual scenario runs — `RunScenario` drives
//! [`runner::ScenarioRunner::run_against_existing`] against the live
//! `ClusterClient` rather than re-provisioning. `UpdateClusterConfig`
//! and `SetNodeCount` reprovision via the underlying `ProcessProvisioner`.

pub mod cluster_handle;
pub mod event_store;
pub mod provisioner;
pub mod runner;
pub mod server;
pub mod service;

pub use cluster_handle::ClusterHandle;
pub use event_store::EventStore;
pub use server::serve;
