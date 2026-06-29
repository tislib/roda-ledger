//! [`Waiter::wait_for_transaction_level`] — block until a transaction reaches a
//! pipeline wait level. All levels are reactive: each parks on an [`IndexWatch`]
//! fed by the ledger index hook (compute/commit/snapshot) or the raft advance
//! sites (cluster_commit). No polling.

use super::Waiter;
use ::proto::ledger as proto;
use spdlog::warn;
use std::io;
use std::time::Duration;

const WAIT_TIMEOUT: Duration = Duration::from_secs(2);

impl Waiter {
    pub async fn wait_for_transaction_level(
        &self,
        transaction_id: u64,
        level: proto::WaitLevel,
    ) -> io::Result<()> {
        let outcome = match level {
            proto::WaitLevel::Computed => {
                tokio::time::timeout(WAIT_TIMEOUT, self.compute().wait_reach(transaction_id)).await
            }
            proto::WaitLevel::Committed => {
                tokio::time::timeout(WAIT_TIMEOUT, self.commit().wait_reach(transaction_id)).await
            }
            proto::WaitLevel::Snapshot => {
                tokio::time::timeout(WAIT_TIMEOUT, self.snapshot().wait_reach(transaction_id)).await
            }
            proto::WaitLevel::ClusterCommit => {
                tokio::time::timeout(WAIT_TIMEOUT, self.wait_cluster_commit(transaction_id)).await
            }
        };
        match outcome {
            Ok(()) => Ok(()),
            Err(_) => {
                warn!(
                    "wait_for_transaction_level: tx_id={transaction_id} level={level:?} TIMED OUT after {WAIT_TIMEOUT:?}"
                );
                Err(io::Error::new(io::ErrorKind::TimedOut, "timeout"))
            }
        }
    }

    /// ClusterCommit requires the tx to be locally durable + queryable *and*
    /// quorum-replicated. Awaiting all three watches in sequence waits for the
    /// last to arrive (each returns immediately once already reached).
    async fn wait_cluster_commit(&self, tx: u64) {
        self.commit().wait_reach(tx).await;
        self.snapshot().wait_reach(tx).await;
        self.cluster_commit().wait_reach(tx).await;
    }
}
