//! `LedgerSlot` — the single shared mutation point that the supervisor
//! uses to swap the live `Arc<Ledger>` during a divergence reseed
//! (ADR-0016 §9).
//!
//! Every long-lived handler (`NodeHandlerCore`, `LedgerHandler`, the
//! gRPC `Server`, the supervisor itself) holds an `Arc<LedgerSlot>`
//! instead of `Arc<Ledger>` directly. On every RPC, the handler does:
//!
//! ```ignore
//! let ledger = self.slot.ledger();   // brief mutex lock
//! // ... use `ledger` ...
//! ```
//!
//! When the supervisor detects divergence, it builds a new
//! `Arc<Ledger>` via `Ledger::start_with_recovery_until(watermark)`
//! and calls `slot.replace(new)`. In-flight RPCs that already cloned
//! the previous `Arc` finish against the old ledger; new RPCs see
//! the new one. The last `Arc<Ledger>` reference dropped triggers
//! the old `Ledger::Drop` (which joins its pipeline threads).
//!
//! Choice of `std::sync::Mutex<Arc<Ledger>>` over alternatives:
//! - `RwLock` — readers + writers contend at this granularity, but
//!   the lock is held for microseconds (clone an `Arc`), so a plain
//!   `Mutex` is simpler and good enough.
//! - `arc-swap` — would be lock-free and faster on the hot path but
//!   adds a dependency for marginal gain at ledger-RPC frequency.
//! - `tokio::sync::Mutex` — only needed if we'd hold the guard
//!   across an `.await`. We never do — clone-and-drop happens
//!   synchronously.

use crate::ledger::Ledger;
use std::sync::{Arc, Mutex};

pub struct LedgerSlot {
    inner: Mutex<Arc<Ledger>>,
}

impl LedgerSlot {
    pub fn new(initial: Arc<Ledger>) -> Self {
        Self {
            inner: Mutex::new(initial),
        }
    }

    /// Hot path. Clone the current `Arc<Ledger>` out under a brief
    /// lock and return it. Callers should keep the returned `Arc`
    /// only for the duration of one operation; long-lived references
    /// would block reseeds from observing the old ledger drop.
    #[inline]
    pub fn ledger(&self) -> Arc<Ledger> {
        self.inner.lock().expect("LedgerSlot mutex poisoned").clone()
    }

    /// Reseed path. Replace the slot with a freshly-started ledger,
    /// returning the old `Arc<Ledger>` so the caller can drop it on
    /// its own thread (avoids a long synchronous `Drop` running
    /// inside an unrelated handler).
    pub fn replace(&self, new: Arc<Ledger>) -> Arc<Ledger> {
        let mut guard = self.inner.lock().expect("LedgerSlot mutex poisoned");
        std::mem::replace(&mut *guard, new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LedgerConfig;
    use std::time::Duration;

    fn build_started_ledger() -> Arc<Ledger> {
        let mut cfg = LedgerConfig::temp();
        cfg.storage.temporary = true;
        let mut ledger = Ledger::new(cfg);
        ledger.start().unwrap();
        Arc::new(ledger)
    }

    #[test]
    fn ledger_returns_clone_under_brief_lock() {
        let slot = LedgerSlot::new(build_started_ledger());
        let a = slot.ledger();
        let b = slot.ledger();
        // Both should be the same underlying Ledger.
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(Arc::strong_count(&a), 3); // a, b, slot
    }

    #[test]
    fn replace_returns_old_arc_for_caller_to_drop() {
        let slot = LedgerSlot::new(build_started_ledger());
        let new_ledger = build_started_ledger();
        let old = slot.replace(new_ledger.clone());
        // The slot now holds the new ledger.
        let after = slot.ledger();
        assert!(Arc::ptr_eq(&after, &new_ledger));
        // The returned `old` is the original — caller drops it on
        // their thread (so the Ledger's Drop runs there).
        drop(old);
        // Give any spawned drop work a moment (Ledger::Drop joins
        // pipeline threads synchronously).
        std::thread::sleep(Duration::from_millis(10));
    }
}
