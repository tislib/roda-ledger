//! `LedgerSlot` — the single shared mutation point that the supervisor
//! uses to swap the live `Arc<Ledger>` during a divergence reseed
//! (ADR-0016 §9).
//!
//! Every long-lived holder (`NodeHandlerCore`, `LedgerHandler`, the
//! gRPC `Server`, the supervisor itself) holds an `Arc<LedgerSlot>`
//! and reads through `slot.ledger()` on every RPC. The supervisor
//! calls `slot.replace(new)` after building a fresh ledger.
//!
//! Implementation: [`arc_swap::ArcSwap`] — lock-free atomic swap of
//! `Arc<Ledger>`. Hot-path reads (`ledger()`) compile down to an
//! atomic pointer load + Arc bump, comparable to a single
//! `AtomicUsize` load. No `Mutex`, no `await`, no contention with
//! the async runtime threads.

use crate::ledger::Ledger;
use arc_swap::ArcSwap;
use std::sync::Arc;

pub struct LedgerSlot {
    inner: ArcSwap<Ledger>,
}

impl LedgerSlot {
    pub fn new(initial: Arc<Ledger>) -> Self {
        Self {
            inner: ArcSwap::new(initial),
        }
    }

    /// Hot path. Returns a fresh `Arc<Ledger>` clone of the
    /// currently-published ledger. Lock-free (atomic load + Arc
    /// bump). Callers should only retain the returned `Arc` for the
    /// duration of one operation; long-lived references would block
    /// the dropped ledger's `Drop` from running until released.
    #[inline]
    pub fn ledger(&self) -> Arc<Ledger> {
        self.inner.load_full()
    }

    /// Reseed path. Atomically install `new` and return the
    /// previously-held `Arc<Ledger>` so the caller can drop it on
    /// its own task — `Ledger::Drop` joins pipeline threads
    /// synchronously, and we never want that running inside a gRPC
    /// handler thread.
    pub fn replace(&self, new: Arc<Ledger>) -> Arc<Ledger> {
        // `swap` returns the previously-held `Arc` (extracted from
        // the `Guard` ArcSwap stores internally). We can drop it on
        // whatever thread we please.
        self.inner.swap(new)
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
    fn ledger_returns_clone_with_lock_free_load() {
        let initial = build_started_ledger();
        let slot = LedgerSlot::new(initial.clone());
        let a = slot.ledger();
        let b = slot.ledger();
        assert!(Arc::ptr_eq(&a, &b));
        // initial + slot's internal arc + a + b
        assert!(Arc::strong_count(&a) >= 3);
    }

    #[test]
    fn replace_returns_old_arc_for_caller_to_drop() {
        let slot = LedgerSlot::new(build_started_ledger());
        let new_ledger = build_started_ledger();
        let old = slot.replace(new_ledger.clone());
        let after = slot.ledger();
        assert!(Arc::ptr_eq(&after, &new_ledger));
        drop(old);
        std::thread::sleep(Duration::from_millis(10));
    }
}
