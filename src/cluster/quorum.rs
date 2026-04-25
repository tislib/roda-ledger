//! Lock-free majority-commit tracker.
//!
//! One `AtomicU64` per cluster node (leader + every peer) plus a cached
//! `majority_index`. Slot 0 is conventionally the leader itself —
//! advanced via `Ledger::on_commit` so the leader's own commit progress
//! counts toward the majority (Raft-style, N/2+1 out of N including self).
//! Slots 1..=peer_count are the peers, advanced by each
//! `PeerReplication` task on every successful `AppendEntries`.
//!
//! Readers (observability, future quorum-gated commit in ADR-016) call
//! `get()`. No locks, no allocation after construction, no backwards
//! movement of `majority_index`.

use crate::cluster::cluster_commit::ClusterCommitIndex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks per-node match indices and the majority-committed index.
///
/// `node_index` is positional — use it directly as an index into
/// `match_index`. Convention: slot 0 is the leader; 1..=peer_count are
/// the peers in the same order as `Config::peers`.
pub struct Quorum {
    /// Per-node replicated index; `node_index` is used directly as the index.
    match_index: Vec<AtomicU64>,
    /// Cached majority result. Monotonically non-decreasing via `fetch_max`.
    majority_index: AtomicU64,
    /// Quorum size: `(node_count / 2) + 1`.
    majority: usize,
    /// Shared cluster-commit watermark. `advance` mirrors the recomputed
    /// majority into this atomic so readers (LedgerHandler) don't need to
    /// hold a reference to `Quorum` itself.
    cluster_commit_index: Arc<ClusterCommitIndex>,
}

impl Quorum {
    /// Build with `node_count` slots (= leader + every peer).
    /// `majority = node_count/2 + 1`.
    pub fn new(node_count: usize) -> Self {
        let match_index: Vec<AtomicU64> = (0..node_count).map(|_| AtomicU64::new(0)).collect();
        Self {
            match_index,
            majority_index: AtomicU64::new(0),
            majority: node_count / 2 + 1,
            cluster_commit_index: ClusterCommitIndex::new(),
        }
    }

    /// Handle to the shared cluster-commit watermark. The leader hands
    /// this to the client-facing `Server` / `LedgerHandler`.
    #[inline]
    pub fn cluster_commit_index(&self) -> Arc<ClusterCommitIndex> {
        self.cluster_commit_index.clone()
    }

    /// Quorum size.
    #[cfg(test)]
    #[inline]
    fn majority(&self) -> usize {
        self.majority
    }

    /// Reset every slot **except** the leader's own (slot 0 by
    /// convention) to `0`. Called by the supervisor on Leader entry
    /// (ADR-0016 §3.3a) so a re-elected leader doesn't see stale
    /// match indices left over from a previous leadership window
    /// or from a different leader's perspective. The leader's own
    /// slot is left untouched — its commit watermark is monotonic
    /// and will be re-published via the on-commit hook anyway.
    ///
    /// Lock-free `Relaxed` stores; the next `advance` call publishes
    /// a fresh snapshot via `Release`.
    pub fn reset_peers(&self, leader_slot: u32) {
        let leader = leader_slot as usize;
        for (idx, slot) in self.match_index.iter().enumerate() {
            if idx == leader {
                continue;
            }
            slot.store(0, Ordering::Relaxed);
        }
    }

    /// Publish `index` for `node_index` and recompute the cached majority.
    ///
    /// Slot 0 is conventionally the leader; callers advance it via the
    /// `Ledger::on_commit` hook. Peer replication tasks advance slots
    /// `1..=peer_count` on every successful `AppendEntries`.
    ///
    /// - Writes `match_index[node_index]` with `Release`.
    /// - Snapshots every slot's value into a thread-local buffer with
    ///   `Relaxed`, sorts descending, takes the `majority - 1` element.
    /// - Publishes the result via `majority_index.fetch_max(..., Release)`
    ///   so the visible value never regresses.
    pub fn advance(&self, node_index: u32, index: u64) {
        let pid = node_index as usize;
        debug_assert!(
            pid < self.match_index.len(),
            "node_index {} out of bounds (node_count={})",
            node_index,
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
        // acked by at least `majority` nodes.
        snapshot.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = snapshot[self.majority - 1];

        self.majority_index.fetch_max(candidate, Ordering::Release);
        self.cluster_commit_index.set_from_quorum(candidate);
    }

    /// Read the cached majority-committed index.
    #[inline]
    pub fn get(&self) -> u64 {
        self.majority_index.load(Ordering::Acquire)
    }

    /// Read a single slot's latest published index (observability).
    #[cfg(test)]
    #[inline]
    fn peer(&self, node_index: u32) -> u64 {
        let pid = node_index as usize;
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

    #[test]
    fn advance_mirrors_majority_into_cluster_commit_index() {
        let mi = Quorum::new(5);
        let cci = mi.cluster_commit_index();

        for (pid, idx) in [(0u32, 10u64), (1, 20), (2, 30), (3, 40), (4, 50)] {
            mi.advance(pid, idx);
            assert_eq!(cci.get(), mi.get(), "cci mirrors majority after advance");
        }
        assert_eq!(cci.get(), 30);

        // A straggler's regression to 1 must not drop the watermark.
        mi.advance(4, 1);
        assert_eq!(cci.get(), 30);
        assert_eq!(mi.get(), 30);
    }
}
