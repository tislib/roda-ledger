//! Lock-free majority-commit tracker.
//!
//! One `AtomicU64` per peer plus a cached `majority_index`. Every successful
//! `AppendEntries` calls `advance(peer_id, index)`; readers (e.g. the leader
//! commit path, observability tools) call `get()`. No locks, no allocation
//! after construction, no backwards movement of `majority_index`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks per-peer match indices and the majority-committed index.
///
/// `peer_id` is positional — use it directly as an index into `match_index`.
pub struct Quorum {
    /// Per-peer replicated index; `peer_id` is used directly as the index.
    match_index: Vec<AtomicU64>,
    /// Cached majority result. Monotonically non-decreasing via `fetch_max`.
    majority_index: AtomicU64,
    /// Quorum size: `(peer_count / 2) + 1`.
    majority: usize,
}

impl Quorum {
    /// Build with `peer_count` positional peers. `majority = peer_count/2 + 1`.
    pub fn new(peer_count: usize) -> Self {
        let match_index: Vec<AtomicU64> =
            (0..peer_count).map(|_| AtomicU64::new(0)).collect();
        Self {
            match_index,
            majority_index: AtomicU64::new(0),
            majority: peer_count / 2 + 1,
        }
    }

    /// Number of peers this tracker was built with.
    #[inline]
    pub fn peer_count(&self) -> usize {
        self.match_index.len()
    }

    /// Quorum size.
    #[inline]
    pub fn majority(&self) -> usize {
        self.majority
    }

    /// Publish `index` for `peer_id` and recompute the cached majority.
    ///
    /// - Writes `match_index[peer_id]` with `Release`.
    /// - Snapshots every peer's value into a thread-local buffer with
    ///   `Relaxed`, sorts descending, takes the `majority - 1` element.
    /// - Publishes the result via `majority_index.fetch_max(..., Release)`
    ///   so the visible value never regresses.
    pub fn advance(&self, peer_id: u32, index: u64) {
        let pid = peer_id as usize;
        debug_assert!(
            pid < self.match_index.len(),
            "peer_id {} out of bounds (peer_count={})",
            peer_id,
            self.match_index.len()
        );

        self.match_index[pid].store(index, Ordering::Release);

        let n = self.match_index.len();
        if n == 0 {
            return;
        }
        let mut snapshot: Vec<u64> = Vec::with_capacity(n);
        for a in &self.match_index {
            snapshot.push(a.load(Ordering::Relaxed));
        }
        // Sort descending so snapshot[majority - 1] is the largest index
        // acked by at least `majority` peers.
        snapshot.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = snapshot[self.majority - 1];

        self.majority_index.fetch_max(candidate, Ordering::Release);
    }

    /// Read the cached majority-committed index.
    #[inline]
    pub fn get(&self) -> u64 {
        self.majority_index.load(Ordering::Acquire)
    }

    /// Read a single peer's latest published index (observability).
    #[inline]
    pub fn peer(&self, peer_id: u32) -> u64 {
        let pid = peer_id as usize;
        debug_assert!(pid < self.match_index.len());
        self.match_index[pid].load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_cluster_stays_at_zero() {
        let mi = Quorum::new(0);
        assert_eq!(mi.majority(), 1);
        assert_eq!(mi.get(), 0);
    }

    #[test]
    fn single_peer_reports_its_own_index() {
        let mi = Quorum::new(1);
        assert_eq!(mi.majority(), 1);
        mi.advance(0, 42);
        assert_eq!(mi.get(), 42);
    }

    #[test]
    fn three_peers_majority_is_second_highest() {
        // 3 peers → majority = 2. Majority index = sorted_desc[1].
        let mi = Quorum::new(3);
        assert_eq!(mi.majority(), 2);

        mi.advance(0, 10);
        // Only one peer at 10; the other two are 0 → 2nd-highest = 0.
        assert_eq!(mi.get(), 0);

        mi.advance(1, 20);
        // Two peers at >= 10 → 2nd-highest = 10.
        assert_eq!(mi.get(), 10);

        mi.advance(2, 30);
        // Three peers at 10, 20, 30 → 2nd-highest = 20.
        assert_eq!(mi.get(), 20);
    }

    #[test]
    fn five_peers_majority_is_third_highest() {
        // 5 peers → majority = 3. Majority index = sorted_desc[2].
        let mi = Quorum::new(5);
        assert_eq!(mi.majority(), 3);

        for (pid, idx) in [(0u32, 10u64), (1, 20), (2, 30), (3, 40), (4, 50)] {
            mi.advance(pid, idx);
        }
        // Sorted desc: [50, 40, 30, 20, 10]. Majority = [2] = 30.
        assert_eq!(mi.get(), 30);
    }

    #[test]
    fn majority_never_regresses() {
        let mi = Quorum::new(3);
        mi.advance(0, 100);
        mi.advance(1, 100);
        assert_eq!(mi.get(), 100);

        // A late straggler reports a lower index for its own slot. The
        // majority must not drop below 100.
        mi.advance(2, 1);
        assert_eq!(mi.get(), 100);

        // Even if everyone "regresses" their slot to 0 (e.g. restart),
        // `get()` still returns the high-water mark.
        mi.advance(0, 0);
        mi.advance(1, 0);
        assert_eq!(mi.get(), 100);
    }

    #[test]
    fn peer_accessor_reads_latest_slot_value() {
        let mi = Quorum::new(3);
        mi.advance(1, 7);
        assert_eq!(mi.peer(1), 7);
        assert_eq!(mi.peer(0), 0);
        assert_eq!(mi.peer(2), 0);
    }
}
