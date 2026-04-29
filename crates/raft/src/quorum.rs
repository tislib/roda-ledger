//! Per-peer match-index tracking and cluster-commit calculation.
//!
//! ADR-0017 §"Two Commit Indexes" — `cluster_commit_index` is the
//! largest tx_id known quorum-committed across the cluster. The
//! leader recomputes it whenever an `AppendEntriesReply` advances a
//! peer's `match_index`. Because the library is single-threaded, this
//! is just a `Vec<u64>` plus a sort each time `advance` is called.
//!
//! Slot 0 is conventionally the leader's own progress, fed by
//! `Event::LocalCommitAdvanced`. Slots `1..=N` are the peers in the
//! same order as `RaftNode::new`'s `peers: Vec<NodeId>`.
//!
//! `match_index[node]` is monotonically non-decreasing — once a peer
//! reports `K`, that report stays put. `cluster_commit_index` itself
//! is also monotonic: a fresh sort might compute a lower
//! "majority-acked" value if a slot's monotonicity is broken (it
//! shouldn't be), but we clamp via `max` to keep the watermark
//! stable.

pub struct Quorum {
    /// `match_index[i]` = highest tx_id node `i` has acked. Slot 0
    /// is the leader; slots 1..=peer_count are peers.
    match_index: Vec<u64>,
    /// `(node_count / 2) + 1`. Cached at construction.
    majority: usize,
    /// Highest tx_id ever observed as quorum-acked. Monotonic.
    cluster_commit_index: u64,
}

impl Quorum {
    /// `node_count = 1 + peer_count`. A single-node cluster has
    /// majority = 1, so the leader's own commit advances are
    /// immediately quorum-committed.
    pub fn new(node_count: usize) -> Self {
        let node_count = node_count.max(1);
        Self {
            match_index: vec![0; node_count],
            majority: node_count / 2 + 1,
            cluster_commit_index: 0,
        }
    }

    pub fn node_count(&self) -> usize {
        self.match_index.len()
    }

    pub fn majority(&self) -> usize {
        self.majority
    }

    /// Wipe every peer slot (everything except `leader_slot`) back to
    /// zero. Called on Leader entry so a re-elected leader doesn't
    /// see stale `match_index` values left over from a previous
    /// leadership window — ADR-0017's term-bump-before-win fix
    /// ensures these stale slots can't otherwise accidentally drive
    /// a premature commit.
    pub fn reset_peers(&mut self, leader_slot: usize) {
        for (i, slot) in self.match_index.iter_mut().enumerate() {
            if i == leader_slot {
                continue;
            }
            *slot = 0;
        }
    }

    /// Publish `index` for slot `node_index` and recompute the
    /// majority-acked watermark. Clamped via `max` so concurrent
    /// out-of-order arrivals (a late ack from a previously-stale
    /// peer) cannot drop the watermark.
    ///
    /// Returns `Some(new_value)` if the watermark advanced, `None`
    /// otherwise — the caller uses this to decide whether to emit
    /// `Action::AdvanceClusterCommit`.
    pub fn advance(&mut self, node_index: usize, index: u64) -> Option<u64> {
        debug_assert!(node_index < self.match_index.len());
        if index > self.match_index[node_index] {
            self.match_index[node_index] = index;
        }
        let mut snapshot = self.match_index.clone();
        snapshot.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = snapshot[self.majority - 1];
        if candidate > self.cluster_commit_index {
            self.cluster_commit_index = candidate;
            Some(candidate)
        } else {
            None
        }
    }

    #[inline]
    pub fn cluster_commit_index(&self) -> u64 {
        self.cluster_commit_index
    }

    #[inline]
    pub fn match_index(&self, node_index: usize) -> u64 {
        self.match_index[node_index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_cluster_treated_as_one_node() {
        // Constructor clamps node_count to 1 so the library never
        // panics on a degenerate input — single-node majority of 1
        // always commits.
        let mut q = Quorum::new(0);
        assert_eq!(q.node_count(), 1);
        assert_eq!(q.majority(), 1);
        assert_eq!(q.advance(0, 5), Some(5));
        assert_eq!(q.cluster_commit_index(), 5);
    }

    #[test]
    fn single_node_immediately_commits_its_own_advance() {
        let mut q = Quorum::new(1);
        assert_eq!(q.majority(), 1);
        assert_eq!(q.advance(0, 42), Some(42));
        assert_eq!(q.cluster_commit_index(), 42);
    }

    #[test]
    fn three_node_majority_is_second_highest() {
        let mut q = Quorum::new(3);
        assert_eq!(q.majority(), 2);

        // Only the leader at 10 — second-highest = 0.
        assert_eq!(q.advance(0, 10), None);
        assert_eq!(q.cluster_commit_index(), 0);

        // Add one peer at 20 — second-highest = 10, advance to 10.
        assert_eq!(q.advance(1, 20), Some(10));

        // Third peer at 30 — second-highest = 20.
        assert_eq!(q.advance(2, 30), Some(20));
    }

    #[test]
    fn five_node_majority_is_third_highest() {
        let mut q = Quorum::new(5);
        assert_eq!(q.majority(), 3);

        for (node, idx) in [(0, 10u64), (1, 20), (2, 30), (3, 40), (4, 50)] {
            q.advance(node, idx);
        }
        assert_eq!(q.cluster_commit_index(), 30);
    }

    #[test]
    fn watermark_is_monotonic() {
        let mut q = Quorum::new(3);
        q.advance(0, 100);
        q.advance(1, 100);
        assert_eq!(q.cluster_commit_index(), 100);

        // Late report for peer 2 at a lower index must not drop
        // the watermark.
        assert_eq!(q.advance(2, 1), None);
        assert_eq!(q.cluster_commit_index(), 100);
    }

    /// Two-node degenerate cluster — majority = 2, both must agree
    /// before a commit advances.
    #[test]
    fn two_node_cluster_requires_both_to_commit() {
        let mut q = Quorum::new(2);
        assert_eq!(q.majority(), 2);
        // Only slot 0 — second-highest is 0.
        assert_eq!(q.advance(0, 100), None);
        assert_eq!(q.cluster_commit_index(), 0);
        // Now both at 100 — second-highest = 100, advance.
        assert_eq!(q.advance(1, 100), Some(100));
        assert_eq!(q.cluster_commit_index(), 100);
    }

    /// Out-of-order advances cannot drop the watermark.
    #[test]
    fn watermark_monotonicity_under_reordered_advances() {
        let mut q = Quorum::new(3);
        q.advance(0, 100);
        q.advance(1, 100);
        assert_eq!(q.cluster_commit_index(), 100);
        // Late, lower advance for slot 0. Per-slot guard
        // (`if index > match_index[i]`) keeps slot 0 at 100; cluster
        // commit unchanged.
        assert_eq!(q.advance(0, 50), None);
        assert_eq!(q.cluster_commit_index(), 100);
        assert_eq!(q.match_index(0), 100);
    }

    /// `reset_peers` does not move the watermark.
    #[test]
    fn reset_peers_does_not_advance_the_watermark() {
        let mut q = Quorum::new(3);
        q.advance(0, 50);
        q.advance(1, 50);
        q.advance(2, 50);
        assert_eq!(q.cluster_commit_index(), 50);
        q.reset_peers(0);
        assert_eq!(q.cluster_commit_index(), 50);
    }

    #[test]
    fn reset_peers_clears_followers_only() {
        let mut q = Quorum::new(3);
        q.advance(0, 50);
        q.advance(1, 50);
        q.advance(2, 50);
        assert_eq!(q.cluster_commit_index(), 50);

        q.reset_peers(0);
        assert_eq!(q.match_index(0), 50);
        assert_eq!(q.match_index(1), 0);
        assert_eq!(q.match_index(2), 0);

        // Watermark is unchanged — `advance` is the only writer.
        assert_eq!(q.cluster_commit_index(), 50);
    }
}
