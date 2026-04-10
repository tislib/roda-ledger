use rustc_hash::FxHashMap;

/// Flip-flop deduplication cache (ADR-009).
///
/// Maintains two `FxHashMap<u64, u64>` maps (user_ref → tx_id) that flip on a
/// configurable time window. A duplicate `user_ref` within the window is
/// detected and the original `tx_id` is returned.
///
/// Effective window: `window_ms` to `2 × window_ms`.
pub struct DedupCache {
    active: FxHashMap<u64, u64>,
    previous: FxHashMap<u64, u64>,
    window_ms: u64,
    window_start: u64,
    enabled: bool,
}

/// Result of a dedup check.
pub enum DedupResult {
    /// No duplicate found — proceed with transaction.
    Proceed,
    /// Duplicate detected — original tx_id returned.
    Duplicate(u64),
}

impl DedupCache {
    pub fn new(enabled: bool, window_ms: u64) -> Self {
        Self {
            active: FxHashMap::default(),
            previous: FxHashMap::default(),
            window_ms,
            window_start: 0,
            enabled,
        }
    }

    /// Check if `user_ref` is a duplicate within the dedup window.
    ///
    /// Must be called with the current timestamp (in milliseconds).
    /// Returns `DedupResult::Duplicate(original_tx_id)` if found.
    pub fn check(&mut self, user_ref: u64, now_ms: u64) -> DedupResult {
        if !self.enabled || user_ref == 0 {
            return DedupResult::Proceed;
        }

        self.maybe_flip(now_ms);

        if let Some(&tx_id) = self.active.get(&user_ref) {
            return DedupResult::Duplicate(tx_id);
        }
        if let Some(&tx_id) = self.previous.get(&user_ref) {
            return DedupResult::Duplicate(tx_id);
        }

        DedupResult::Proceed
    }

    /// Record a committed transaction's user_ref → tx_id mapping.
    ///
    /// Called after a transaction is processed (whether it succeeded or failed),
    /// so that future submissions with the same user_ref are detected.
    pub fn insert(&mut self, user_ref: u64, tx_id: u64) {
        if !self.enabled || user_ref == 0 {
            return;
        }
        self.active.insert(user_ref, tx_id);
    }

    /// Rebuild a single entry during WAL recovery.
    ///
    /// `timestamp_ms` is the transaction's timestamp converted to milliseconds.
    /// Entries within `2 × window_ms` of `now_ms` are inserted into the
    /// appropriate bucket.
    pub fn recover_entry(&mut self, user_ref: u64, tx_id: u64, timestamp_ms: u64, now_ms: u64) {
        if !self.enabled || user_ref == 0 {
            return;
        }

        // Only recover entries within the dedup window
        if now_ms.saturating_sub(timestamp_ms) > 2 * self.window_ms {
            return;
        }

        // Initialize window_start on first recovery entry
        if self.window_start == 0 {
            self.window_start = now_ms;
        }

        // Place in active or previous based on whether the entry is from
        // the current window or the previous one
        if timestamp_ms >= self.window_start.saturating_sub(self.window_ms) {
            self.active.insert(user_ref, tx_id);
        } else {
            self.previous.insert(user_ref, tx_id);
        }
    }

    fn maybe_flip(&mut self, now_ms: u64) {
        if self.window_start == 0 {
            self.window_start = now_ms;
            return;
        }

        if now_ms.saturating_sub(self.window_start) >= self.window_ms {
            // swap: previous = old active, active = cleared (reuses allocation)
            std::mem::swap(&mut self.active, &mut self.previous);
            self.active.clear();
            self.window_start = now_ms;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_dedup_when_disabled() {
        let mut cache = DedupCache::new(false, 10_000);
        cache.insert(100, 1);
        assert!(matches!(cache.check(100, 1000), DedupResult::Proceed));
    }

    #[test]
    fn no_dedup_for_zero_user_ref() {
        let mut cache = DedupCache::new(true, 10_000);
        cache.insert(0, 1);
        assert!(matches!(cache.check(0, 1000), DedupResult::Proceed));
    }

    #[test]
    fn detects_duplicate_in_active() {
        let mut cache = DedupCache::new(true, 10_000);
        assert!(matches!(cache.check(100, 1000), DedupResult::Proceed));
        cache.insert(100, 42);
        match cache.check(100, 2000) {
            DedupResult::Duplicate(tx_id) => assert_eq!(tx_id, 42),
            DedupResult::Proceed => panic!("expected duplicate"),
        }
    }

    #[test]
    fn detects_duplicate_in_previous_after_flip() {
        let mut cache = DedupCache::new(true, 10_000);
        // Insert at t=1000
        assert!(matches!(cache.check(100, 1000), DedupResult::Proceed));
        cache.insert(100, 42);

        // Flip at t=12000 (12s > 10s window)
        // user_ref=100 should be in previous now
        match cache.check(100, 12_000) {
            DedupResult::Duplicate(tx_id) => assert_eq!(tx_id, 42),
            DedupResult::Proceed => panic!("expected duplicate in previous"),
        }
    }

    #[test]
    fn entry_expires_after_two_flips() {
        let mut cache = DedupCache::new(true, 10_000);
        // Insert at t=1000
        assert!(matches!(cache.check(100, 1000), DedupResult::Proceed));
        cache.insert(100, 42);

        // First flip at t=12000
        assert!(matches!(cache.check(999, 12_000), DedupResult::Proceed));
        // Second flip at t=23000 — entry should be gone
        assert!(matches!(cache.check(100, 23_000), DedupResult::Proceed));
    }

    #[test]
    fn recover_entry_within_window() {
        let mut cache = DedupCache::new(true, 10_000);
        let now = 50_000u64;
        // Entry from 5 seconds ago — within 2×window (20s)
        cache.recover_entry(100, 42, now - 5_000, now);
        match cache.check(100, now) {
            DedupResult::Duplicate(tx_id) => assert_eq!(tx_id, 42),
            DedupResult::Proceed => panic!("expected duplicate after recovery"),
        }
    }

    #[test]
    fn recover_entry_outside_window_ignored() {
        let mut cache = DedupCache::new(true, 10_000);
        let now = 50_000u64;
        // Entry from 25 seconds ago — outside 2×window (20s)
        cache.recover_entry(100, 42, now - 25_000, now);
        assert!(matches!(cache.check(100, now), DedupResult::Proceed));
    }
}
