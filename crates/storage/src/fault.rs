//! Fault-injection seams for the storage layer.
//!
//! Gated behind the workspace `fault-injection` Cargo feature.
//! Production builds (default features) do not compile this module
//! and the IO call sites bypass the hook entirely — see
//! `Syncer::sync` and `Segment::write_pending_entries`.
//!
//! ADR-018 specifies the surface; this module is the storage-side
//! trait the cluster / ledger layers implement against.

use std::sync::Arc;

/// Hook consulted by `Segment::write_pending_entries` (before
/// `write_all`) and `Syncer::sync` (before `fdatasync`). Concrete
/// implementations (e.g. `LedgerFaultInjector` in the ledger crate)
/// park, delay, or no-op per the test's scenario.
///
/// Default impls return immediately so wrapper implementations that
/// only care about one axis don't need to spell out the pass-through.
pub trait WalFault: Send + Sync {
    /// Called immediately before `write_all` on the active segment's
    /// WAL file. `buf_len` is the byte count about to be written —
    /// fault impls can use it to model bandwidth ceilings.
    fn before_write(&self, _buf_len: usize) {}

    /// Called immediately before `fdatasync` on the active segment's
    /// WAL file.
    fn before_sync(&self) {}
}

/// Convenience: hook handle threaded through `Storage → Segment →
/// Syncer`. `Arc<dyn WalFault>` lets multiple Syncers share one hook
/// without taking ownership.
pub type WalFaultHook = Arc<dyn WalFault>;
