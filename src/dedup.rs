use rustc_hash::FxHashMap;

/// Flip-flop deduplication cache (ADR-009, ADR-013).
///
/// Maintains two `FxHashMap<u64, u64>` maps (user_ref → tx_id) that flip on a
/// transaction-count window. A duplicate `user_ref` within the window is
/// detected and the original `tx_id` is returned.
///
/// Deduplication is always on. `user_ref == 0` skips the check per-transaction.
///
/// Effective window: `window_size` to `2 × window_size` transactions.
pub struct DedupCache {
    active: FxHashMap<u64, u64>,
    previous: FxHashMap<u64, u64>,
    window_size: u64,
    window_start_tx_id: u64,
}

/// Result of a dedup check.
pub enum DedupResult {
    /// No duplicate found — proceed with transaction.
    Proceed,
    /// Duplicate detected — original tx_id returned.
    Duplicate(u64),
}

impl DedupCache {
    pub fn new(window_size: u64) -> Self {
        Self {
            active: FxHashMap::default(),
            previous: FxHashMap::default(),
            window_size,
            window_start_tx_id: 0,
        }
    }

    /// Check if `user_ref` is a duplicate within the dedup window.
    ///
    /// Must be called with the current transaction ID.
    /// Returns `DedupResult::Duplicate(original_tx_id)` if found.
    pub fn check(&mut self, user_ref: u64, tx_id: u64) -> DedupResult {
        if user_ref == 0 {
            return DedupResult::Proceed;
        }

        self.maybe_flip(tx_id);

        if let Some(&original_tx_id) = self.active.get(&user_ref) {
            return DedupResult::Duplicate(original_tx_id);
        }
        if let Some(&original_tx_id) = self.previous.get(&user_ref) {
            return DedupResult::Duplicate(original_tx_id);
        }

        DedupResult::Proceed
    }

    /// Record a committed transaction's user_ref → tx_id mapping.
    ///
    /// Called after a transaction is processed (whether it succeeded or failed),
    /// so that future submissions with the same user_ref are detected.
    pub fn insert(&mut self, user_ref: u64, tx_id: u64) {
        if user_ref == 0 {
            return;
        }
        self.active.insert(user_ref, tx_id);
    }

    /// Rebuild a single entry during WAL recovery.
    ///
    /// `tx_id` is the transaction's ID. Entries within `2 × window_size` of
    /// `last_tx_id` are inserted into the appropriate bucket.
    pub fn recover_entry(&mut self, user_ref: u64, tx_id: u64, last_tx_id: u64) {
        if user_ref == 0 {
            return;
        }

        // Only recover entries within the dedup window (2 × window_size)
        if self.window_size > 0 && last_tx_id.saturating_sub(tx_id) > 2 * self.window_size {
            return;
        }

        // Initialize window_start on first recovery entry
        if self.window_start_tx_id == 0 && self.window_size > 0 {
            // Place window start so that current entries fall into active
            self.window_start_tx_id = last_tx_id.saturating_sub(self.window_size);
        }

        // Place in active or previous based on tx_id relative to window boundary
        if tx_id >= self.window_start_tx_id {
            self.active.insert(user_ref, tx_id);
        } else {
            self.previous.insert(user_ref, tx_id);
        }
    }

    fn maybe_flip(&mut self, tx_id: u64) {
        if self.window_size == 0 {
            return;
        }

        if self.window_start_tx_id == 0 {
            self.window_start_tx_id = tx_id;
            return;
        }

        if tx_id.saturating_sub(self.window_start_tx_id) >= self.window_size {
            // swap: previous = old active, active = cleared (reuses allocation)
            std::mem::swap(&mut self.active, &mut self.previous);
            self.active.clear();
            self.window_start_tx_id = tx_id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_dedup_for_zero_user_ref() {
        let mut cache = DedupCache::new(1000);
        cache.insert(0, 1);
        assert!(matches!(cache.check(0, 2), DedupResult::Proceed));
    }

    #[test]
    fn detects_duplicate_in_active() {
        let mut cache = DedupCache::new(1000);
        assert!(matches!(cache.check(100, 1), DedupResult::Proceed));
        cache.insert(100, 1);
        match cache.check(100, 2) {
            DedupResult::Duplicate(tx_id) => assert_eq!(tx_id, 1),
            DedupResult::Proceed => panic!("expected duplicate"),
        }
    }

    #[test]
    fn detects_duplicate_in_previous_after_flip() {
        let mut cache = DedupCache::new(10);
        // Insert at tx_id=1
        assert!(matches!(cache.check(100, 1), DedupResult::Proceed));
        cache.insert(100, 1);

        // Flip at tx_id=12 (12 - 1 = 11 >= window_size 10)
        // user_ref=100 should be in previous now
        match cache.check(100, 12) {
            DedupResult::Duplicate(tx_id) => assert_eq!(tx_id, 1),
            DedupResult::Proceed => panic!("expected duplicate in previous"),
        }
    }

    #[test]
    fn entry_expires_after_two_flips() {
        let mut cache = DedupCache::new(10);
        // Insert at tx_id=1
        assert!(matches!(cache.check(100, 1), DedupResult::Proceed));
        cache.insert(100, 1);

        // First flip at tx_id=12
        assert!(matches!(cache.check(999, 12), DedupResult::Proceed));
        // Second flip at tx_id=23 — entry should be gone
        assert!(matches!(cache.check(100, 23), DedupResult::Proceed));
    }

    #[test]
    fn recover_entry_within_window() {
        let mut cache = DedupCache::new(1000);
        let last_tx_id = 5000u64;
        // Entry from 500 transactions ago — within 2×window (2000)
        cache.recover_entry(100, last_tx_id - 500, last_tx_id);
        match cache.check(100, last_tx_id) {
            DedupResult::Duplicate(tx_id) => assert_eq!(tx_id, last_tx_id - 500),
            DedupResult::Proceed => panic!("expected duplicate after recovery"),
        }
    }

    #[test]
    fn recover_entry_outside_window_ignored() {
        let mut cache = DedupCache::new(1000);
        let last_tx_id = 5000u64;
        // Entry from 2500 transactions ago — outside 2×window (2000)
        cache.recover_entry(100, last_tx_id - 2500, last_tx_id);
        assert!(matches!(cache.check(100, last_tx_id), DedupResult::Proceed));
    }
}
