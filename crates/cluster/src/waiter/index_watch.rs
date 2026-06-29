//! [`IndexWatch`] — a u64 progress index async tasks can await crossing a
//! threshold, driven from the ledger's (sync, std-thread) index hook. The value
//! is lock-free; `Notify` is a pure edge signal carrying no data.
//!
//! There is deliberately no `close`/shutdown path (ADR-027 §D3): every
//! `wait_reach` is awaited under the node `CancellationToken` (or tonic graceful
//! shutdown), which drops the parked future from the outside.

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;

pub struct IndexWatch {
    value: AtomicU64,
    notify: Notify,
}

impl IndexWatch {
    pub fn new(initial: u64) -> Self {
        Self {
            value: AtomicU64::new(initial),
            notify: Notify::new(),
        }
    }

    /// Publish a new value and wake waiters. Called from the index hook — sync,
    /// non-blocking. Plain store: a single ordered publisher per index keeps it
    /// monotonic within one ledger incarnation, and a reseed correctly moves it
    /// back down to the truncated baseline.
    #[inline]
    pub fn advance(&self, value: u64) {
        self.value.store(value, Ordering::Release);
        self.notify.notify_waiters();
    }

    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    /// Await until the index is `>= target` (returns immediately if already
    /// reached). Cancel-safe: dropping the future unregisters the waiter.
    pub async fn wait_reach(&self, target: u64) {
        loop {
            // Register interest BEFORE the check: `notify_waiters` keeps no
            // permit, so a concurrent `advance` must not slip in between.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.value.load(Ordering::Acquire) >= target {
                return;
            }
            notified.await;
        }
    }
}
