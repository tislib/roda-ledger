//! [`Waiter::wait_for_transaction_level`] — block until a transaction reaches a
//! pipeline wait level. Computed/Committed/Snapshot are reactive (parked on an
//! [`IndexWatch`] fed by the ledger index hook); ClusterCommit stays poll-based
//! because `cluster_commit_index` is driven by raft quorum, not the ledger hook.

use super::Waiter;
use super::index_watch::IndexWatch;
use ::proto::ledger as proto;
use spdlog::warn;
use std::io;
use std::time::{Duration, Instant};

const WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const CLUSTER_POLL: Duration = Duration::from_micros(100);

impl Waiter {
    pub async fn wait_for_transaction_level(
        &self,
        transaction_id: u64,
        level: proto::WaitLevel,
    ) -> io::Result<()> {
        match level {
            proto::WaitLevel::Computed => {
                self.await_index(&self.compute, transaction_id, level).await
            }
            proto::WaitLevel::Committed => {
                self.await_index(&self.commit, transaction_id, level).await
            }
            proto::WaitLevel::Snapshot => {
                self.await_index(&self.snapshot, transaction_id, level)
                    .await
            }
            proto::WaitLevel::ClusterCommit => self.await_cluster_commit(transaction_id).await,
        }
    }

    /// Reactive wait: park on the watch until it reaches `tx`, bounded by the
    /// shared timeout. Shutdown drops this future from the outside (node
    /// `CancellationToken` / tonic graceful shutdown) — no explicit close path
    /// (ADR-027 §D3); never bare-await this off the cancellation path.
    async fn await_index(
        &self,
        watch: &IndexWatch,
        tx: u64,
        level: proto::WaitLevel,
    ) -> io::Result<()> {
        match tokio::time::timeout(WAIT_TIMEOUT, watch.wait_reach(tx)).await {
            Ok(()) => Ok(()),
            Err(_) => {
                warn!(
                    "wait_for_transaction_level: tx_id={tx} level={level:?} TIMED OUT after {WAIT_TIMEOUT:?} (index={})",
                    watch.get()
                );
                Err(io::Error::new(io::ErrorKind::TimedOut, "timeout"))
            }
        }
    }

    /// ClusterCommit requires the tx to be locally durable + queryable *and*
    /// quorum-replicated. `cluster_commit_index` advances via raft quorum, not
    /// the ledger index hook, so this level stays poll-based until a raft-side
    /// notifier exists.
    async fn await_cluster_commit(&self, transaction_id: u64) -> io::Result<()> {
        let start = Instant::now();
        loop {
            self.consensus.self_advance();
            let cluster_commit = self.consensus.cluster_commit_index();
            let ledger = self.ledger.current();
            let commit = ledger.last_commit_id();
            let snapshot = ledger.last_snapshot_id();
            if snapshot >= transaction_id
                && commit >= transaction_id
                && cluster_commit >= transaction_id
            {
                return Ok(());
            }
            if start.elapsed() >= WAIT_TIMEOUT {
                warn!(
                    "wait_for_transaction_level: tx_id={transaction_id} level=ClusterCommit TIMED OUT after {WAIT_TIMEOUT:?} — commit={commit} snapshot={snapshot} cluster_commit={cluster_commit}"
                );
                return Err(io::Error::new(io::ErrorKind::TimedOut, "timeout"));
            }
            tokio::time::sleep(CLUSTER_POLL).await;
        }
    }
}
