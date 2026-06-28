//! [`Waiter::wait_for_transaction_level`] — block until a transaction
//! reaches a pipeline wait level (Computed / Committed / Snapshot /
//! ClusterCommit).

use super::Waiter;
use ::proto::ledger as proto;
use spdlog::{trace, warn};
use std::time::Duration;

impl Waiter {
    pub async fn wait_for_transaction_level(
        &self,
        transaction_id: u64,
        level: proto::WaitLevel,
    ) -> std::io::Result<()> {
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
            let compute = ledger.last_compute_id();
            let commit = ledger.last_commit_id();
            let snapshot = ledger.last_snapshot_id();
            self.consensus.self_advance();
            let cluster_commit = self.consensus.cluster_commit_index();
            let reached = match level {
                proto::WaitLevel::Computed => compute >= transaction_id,
                proto::WaitLevel::Committed => commit >= transaction_id,
                proto::WaitLevel::Snapshot => snapshot >= transaction_id,
                proto::WaitLevel::ClusterCommit => {
                    // Require all three watermarks to have passed the tx.
                    // `cluster_commit_index` reflects the leader's view of
                    // quorum replication; the local `commit_index` and
                    // `snapshot_index` ensure the tx is also durably
                    // stored and queryable on this node.
                    snapshot >= transaction_id
                        && commit >= transaction_id
                        && cluster_commit >= transaction_id
                }
            };

            if reached {
                trace!(
                    "wait_for_transaction_level: tx_id={} level={:?} reached after {}ms ({} iterations) — compute={} commit={} snapshot={} cluster_commit={}",
                    transaction_id,
                    level,
                    start_time.elapsed().as_millis(),
                    iter,
                    compute,
                    commit,
                    snapshot,
                    cluster_commit
                );
                return Ok(());
            }

            // Periodic progress log so multi-second waits aren't silent.
            // With a 100µs poll a 2 s wait can hit ~20 000 iters, so
            // bump the log cadence to keep it once-per-second-ish.
            if iter.is_multiple_of(10_000) {
                trace!(
                    "wait_for_transaction_level: tx_id={} level={:?} still waiting after {}ms — compute={} commit={} snapshot={} cluster_commit={}",
                    transaction_id,
                    level,
                    start_time.elapsed().as_millis(),
                    compute,
                    commit,
                    snapshot,
                    cluster_commit
                );
            }

            // Poll, don't spin. `yield_now` only re-queues the task —
            // when many `*_and_wait` handlers are in flight, that
            // produces a CPU storm of raft-mutex acquisitions. 100µs
            // is well below typical cluster-commit latency (a few ms
            // on LAN, fdatasync-bound) so the latency hit is
            // negligible while CPU drops by orders of magnitude.
            tokio::time::sleep(Duration::from_micros(100)).await;

            if start_time.elapsed() >= timeout {
                warn!(
                    "wait_for_transaction_level: tx_id={} level={:?} TIMED OUT after {}ms ({} iterations) — compute={} commit={} snapshot={} cluster_commit={}",
                    transaction_id,
                    level,
                    start_time.elapsed().as_millis(),
                    iter,
                    compute,
                    commit,
                    snapshot,
                    cluster_commit
                );
                return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
            }
        }
    }
}
