use crate::tx_ring::ring::TxRing;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub struct TxRingReleaser {
    pub(super) ring: Arc<TxRing>,
    pub(super) cursor: usize, // local release head, monotonic
}

impl TxRingReleaser {
    // Move the release index to an absolute logical index; forward only.
    pub fn advance_to(&mut self, index: usize) {
        debug_assert!(
            index.wrapping_sub(self.cursor) <= self.ring.capacity,
            "TxRingReleaser: release moved backward or skipped a lap"
        );
        debug_assert!(
            self.ring.write.load(Ordering::Acquire).wrapping_sub(index) <= self.ring.capacity,
            "TxRingReleaser: release passed the write cursor"
        );
        self.cursor = index;
        self.ring.release.store(index, Ordering::Release);
    }

    pub fn released(&self) -> usize {
        self.cursor
    }
}
