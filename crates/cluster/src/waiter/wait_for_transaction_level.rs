//! [`Waiter::wait_for_transaction_level`] — block until a transaction
//! reaches a pipeline wait level (Computed / Committed / Snapshot /
//! ClusterCommit).

use super::Waiter;
use ::proto::ledger as proto;
use spdlog::{trace, warn};
use std::time::Duration;
use tokio::task::yield_now as tokio_yield_now;

impl Waiter {
    pub async fn wait_for_transaction_level(
        &self,
        transaction_id: u64,
        level: proto::WaitLevel,
    ) -> std::io::Result<Duration> {
        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(2);
        let mut iter = 0u32;
        trace!(
            "wait_for_transaction_level: tx_id={} level={:?} timeout={:?} starting",
            transaction_id, level, timeout
        );

        loop {
            iter += 1;
            let ledger = self.ledger.current();

            let reached = match level {
                proto::WaitLevel::Computed => ledger.last_compute_id() >= transaction_id,
                proto::WaitLevel::Committed => ledger.last_commit_id() >= transaction_id,
                proto::WaitLevel::Snapshot => ledger.last_snapshot_id() >= transaction_id,
                proto::WaitLevel::ClusterCommit => {
                    self.consensus.self_advance();
                    let cluster_commit = self.consensus.cluster_commit_index();
                    ledger.last_snapshot_id() >= transaction_id
                        && ledger.last_commit_id() >= transaction_id
                        && cluster_commit >= transaction_id
                }
            };

            if reached {
                // Reached on the first check ⇒ the stage was already at/past
                // the target: nothing was in flight, so there was no real
                // wait. Return zero so the latency probe can exclude it as a
                // non-sample rather than polluting the median with ~0s.
                return Ok(if iter == 1 {
                    Duration::ZERO
                } else {
                    start_time.elapsed()
                });
            }
            tokio_yield_now().await;
            if start_time.elapsed() >= timeout {
                warn!(
                    "wait_for_transaction_level: tx_id={} level={:?} TIMED OUT after {}ms ({} iterations)",
                    transaction_id,
                    level,
                    start_time.elapsed().as_millis(),
                    iter,
                );
                return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
            }
        }
    }
}
