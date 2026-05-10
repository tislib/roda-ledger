//! Control plane backed by a real cluster.
//!
//! Implements two gRPC services on a single port via `tonic-web` (so
//! browsers using `@connectrpc/connect-web`'s `createGrpcWebTransport`
//! can reach it directly):
//!
//! - `roda.control.v1.Control` — cluster monitoring, fault injection,
//!   provisioning, scenarios. State is split between a long-lived
//!   [`cluster_handle::ClusterHandle`] (provisioner +
//!   [`client::ClusterClient`]) and an in-memory
//!   [`event_store::EventStore`] for ephemeral history (faults,
//!   scenario runs, recent submissions).
//!
//! - `roda.ledger.v1.Ledger` — transparent proxy
//!   ([`ledger_proxy::LedgerProxy`]) that forwards every call to the
//!   live cluster's `ClusterClient`, inheriting its leader discovery,
//!   round-robin reads, and retry policy. A `node-selector` request
//!   metadata header pins a call to a specific peer.
//!
//! The cluster outlives individual scenario runs — `RunScenario` drives
//! [`runner::ScenarioRunner::run_against_existing`] against the live
//! `ClusterClient` rather than re-provisioning. `UpdateClusterConfig`
//! and `SetNodeCount` reprovision via the underlying `ProcessProvisioner`;
//! the proxy picks up the new cluster on its next `handle.client()`
//! load with no extra plumbing.

pub mod cluster_handle;
pub mod event_store;
pub mod ledger_proxy;
pub mod provisioner;
pub mod runner;
pub mod server;
pub mod service;

pub use cluster_handle::ClusterHandle;
pub use event_store::EventStore;
pub use ledger_proxy::{LedgerProxy, NODE_SELECTOR_METADATA_KEY};
pub use server::serve;
