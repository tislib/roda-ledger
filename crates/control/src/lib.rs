//! Control plane in-memory mock for the operational UI.
//!
//! Implements `roda.control.v1.Control` over a tonic gRPC server with
//! `tonic-web` for browser compatibility (so `@connectrpc/connect-web` can
//! reach it from a Vite dev server). State is held entirely in memory
//! behind a single `parking_lot::RwLock`; a 250 ms background tick advances
//! the staged-pipeline watermarks, drives election fallback when the
//! current leader is stopped, and runs scenarios.
//!
//! This crate intentionally has no dependency on `ledger`, `raft`, or
//! `cluster`. It exists to make the UI's control-plane RPCs round-trip
//! against a real server while the production control plane is built out.

pub mod background;
pub mod scenario;
pub mod server;
pub mod service;
pub mod state;

pub use server::serve;
pub use state::InMemoryState;
