//! Ledger ownership wrapper supporting atomic swap-and-rebuild on
//! divergence — the "supervisor reseed" path documented on
//! `Ledger::start_with_recovery_until` (ADR-0016 §9).
//!
//! Consumers hold `Arc<LedgerSlot>` and call `.current()` for a normal
//! borrow; the replication handshake calls `.reseed(watermark)` when
//! raft signals a divergent uncommitted tail.

use arc_swap::ArcSwap;
use ledger::config::LedgerConfig;
use ledger::ledger::Ledger;
use spdlog::debug;
use std::io;
use std::sync::Arc;

pub struct LedgerSlot {
    inner: ArcSwap<Ledger>,
    config: LedgerConfig,
}

impl LedgerSlot {
    pub fn new(initial: Ledger, config: LedgerConfig) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(initial)),
            config,
        }
    }

    #[inline]
    pub fn current(&self) -> Arc<Ledger> {
        self.inner.load_full()
    }

    /// Drop the current Ledger and reconstruct via
    /// `Ledger::start_with_recovery_until(watermark)`. Called from the
    /// replication-handshake path when raft returns
    /// `Reject { LogMismatch, truncate_after: Some(_) }`.
    pub fn reseed(&self, watermark: u64) -> io::Result<()> {
        debug!("ledger_slot: reseed begin watermark={}", watermark);
        let mut fresh = Ledger::new(self.config.clone());
        fresh.start_with_recovery_until(watermark)?;
        let prev = self.inner.swap(Arc::new(fresh));
        debug!(
            "ledger_slot: reseed complete watermark={} (prev_strong={})",
            watermark,
            Arc::strong_count(&prev),
        );
        Ok(())
    }
}
