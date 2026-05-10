//! Control plane for the operational UI.
//!
//! Implements two gRPC services on a single port via `tonic-web` (so
//! browsers using `@connectrpc/connect-web`'s `createGrpcWebTransport`
//! can reach it directly):
//!
//! - `roda.control.v1.Control` — cluster monitoring, fault injection,
//!   provisioning, and load scenarios. State is held entirely in
//!   memory behind a single `parking_lot::RwLock`; a 250 ms background
//!   tick handles election fallback and scenario step progression.
//!
//! - `roda.ledger.v1.Ledger` — transparent proxy that forwards every
//!   call to the provisioned cluster peers. Reads round-robin across
//!   peers; writes are routed to the cached leader (rotating on
//!   "not a leader" rejections); a `node-selector` request metadata
//!   header pins a call to a specific peer regardless of role.

pub mod background;
pub mod ledger_proxy;
pub mod server;
pub mod service;
pub mod state;

pub use ledger_proxy::{LedgerProxy, NODE_SELECTOR_METADATA_KEY, PeerSpec};
pub use server::serve;
pub use state::InMemoryState;
