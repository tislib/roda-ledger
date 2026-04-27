//! Per-peer replication: owns one `WalTailer` cursor, one gRPC client, and
//! the shipping window for a single follower. Its `run()` is the long-lived
//! async task that `Leader` spawns (one per peer).
//!
//! The key state is `(from_tx_id, peer_last_tx)`:
//! - `from_tx_id` is the next tx we still need to ship.
//! - `peer_last_tx` is the high-water mark the follower has acked (stamped
//!   onto every AppendEntries as `prev_tx_id` for Raft-style ordering).

use super::Quorum;
use crate::cluster::config::PeerConfig;
use crate::cluster::proto::node as proto;
use crate::cluster::proto::node::node_client::NodeClient;
use crate::cluster::supervisor::{Transition, TransitionTx};
use crate::ledger::Ledger;
use crate::storage::WalTailer;
use crate::tools::backoff::{Backoff, BackoffPolicy};
use spdlog::{debug, info, trace, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint};

pub(super) const WAL_RECORD_SIZE: usize = 40;

/// Static replication parameters shared across all peer tasks.
#[derive(Clone, Debug)]
pub struct ReplicationParams {
    pub leader_id: u64,
    pub term: u64,
    pub max_bytes_per_rpc: usize,
    pub rpc_message_size_limit: usize,
    pub poll_interval: Duration,
}

impl ReplicationParams {
    pub fn new(
        leader_id: u64,
        term: u64,
        max_bytes_per_rpc: usize,
        poll_interval: Duration,
    ) -> Self {
        let records_per_rpc = (max_bytes_per_rpc / WAL_RECORD_SIZE).max(1);
        let max_bytes = records_per_rpc * WAL_RECORD_SIZE;
        // Leave protobuf framing headroom (doubled + 4 KiB) so the client
        // doesn't trip tonic's default 4 MiB decode/encode limit.
        let rpc_message_size_limit = max_bytes * 2 + 4 * 1024;
        Self {
            leader_id,
            term,
            max_bytes_per_rpc: max_bytes,
            rpc_message_size_limit,
            poll_interval,
        }
    }
}

/// Per-peer replicator. One per follower.
pub struct PeerReplication {
    peer: PeerConfig,
    /// Positional id (0-based) used as the `Quorum` slot.
    peer_index: u32,
    params: ReplicationParams,
    /// Per-leader-instance cancellation. Flipped to `false` by the
    /// supervisor when this Leader steps down (higher term observed,
    /// divergence reseed, etc.). Distinct from `supervisor_running`
    /// so a step-down doesn't drag the whole supervisor with it.
    running: Arc<AtomicBool>,
    /// Process-wide supervisor flag. Flipped to `false` by
    /// `SupervisorHandles::abort`. Critical for failover tests:
    /// without checking this, an aborted leader's peer tasks would
    /// outlive the abort and keep shipping zombie heartbeats to
    /// surviving followers, resetting their election timers and
    /// preventing re-election.
    supervisor_running: Arc<AtomicBool>,
    tailer: WalTailer,
    /// DIAG-flake-replication: kept so the periodic "still tailing 0 bytes"
    /// diagnostic can read `last_commit_id`, `last_segment_id`, and the
    /// active wal file length on the same `Ledger` instance the tailer
    /// was bound to. Pointer comparison (`storage_ptr`) lets us detect
    /// a ledger swap (reseed) — see H1 in the diagnose plan.
    ledger_for_diag: Arc<Ledger>,
    from_tx_id: u64,
    peer_last_tx: u64,
    /// Shared majority-commit tracker owned by `Leader`. The
    /// `leader_commit_tx_id` stamped on every outgoing AppendEntries
    /// reads from here via `majority.get()`.
    majority: Arc<Quorum>,
    /// Channel back to the supervisor's role driver. Used to push a
    /// `StepDownHigherTerm` transition when a peer responds with
    /// `term > params.term` (ADR-0016 §5, §11). On step-down this
    /// task self-terminates by clearing the cooperative `running`
    /// flag's local view through an early return.
    transition_tx: TransitionTx,
}

impl PeerReplication {
    pub fn new(
        peer: PeerConfig,
        peer_index: u32,
        ledger: Arc<Ledger>,
        params: ReplicationParams,
        running: Arc<AtomicBool>,
        supervisor_running: Arc<AtomicBool>,
        majority: Arc<Quorum>,
        transition_tx: TransitionTx,
    ) -> Self {
        let tailer = ledger.wal_tailer();
        Self {
            peer,
            peer_index,
            params,
            running,
            supervisor_running,
            tailer,
            ledger_for_diag: ledger,
            from_tx_id: 1,
            peer_last_tx: 0,
            majority,
            transition_tx,
        }
    }

    /// True iff both the per-leader cancel flag and the
    /// supervisor-wide running flag are still set. Either being
    /// `false` is sufficient cause to drain.
    #[inline]
    fn alive(&self) -> bool {
        self.running.load(Ordering::Relaxed) && self.supervisor_running.load(Ordering::Relaxed)
    }

    /// If the peer's response carries a strictly-higher term, push a
    /// step-down transition to the supervisor and return `true` so
    /// the caller can return early. The mpsc channel uses
    /// `try_send`; on a momentarily-full channel we degrade
    /// gracefully (the next response will retry — and the supervisor
    /// only needs *one* such signal to step down).
    fn maybe_step_down(&self, peer_term: u64) -> bool {
        if peer_term > self.params.term {
            warn!(
                "replication: peer {} responded with term {} > our {}; signalling step-down",
                self.peer.peer_id, peer_term, self.params.term
            );
            let _ = self.transition_tx.try_send(Transition::StepDownHigherTerm {
                observed: peer_term,
            });
            return true;
        }
        false
    }

    /// Long-lived async driver. Runs until `running` is cleared or an
    /// unrecoverable error occurs; otherwise retries transport + reject
    /// failures in place.
    pub async fn run(mut self) {
        info!(
            "replication: starting peer-task for node_id={} ({}) — term={}, poll_interval={:?}",
            self.peer.peer_id, self.peer.host, self.params.term, self.params.poll_interval
        );

        // Connect retry: start at 200 ms and double up to 3.2 s. A
        // peer that takes a few seconds to bind (cluster cold start,
        // node restart) settles on the first or second retry; a
        // permanently-down peer doesn't drown the log in tight-loop
        // warns.
        let connect_policy =
            BackoffPolicy::exponential(Duration::from_millis(200), Duration::from_millis(3_200));
        let mut connect_backoff = Backoff::new(connect_policy);
        let mut client = loop {
            if !self.alive() {
                debug!(
                    "replication: peer-task for node_id={} aborted before connect (running flag cleared)",
                    self.peer.peer_id
                );
                return;
            }
            let attempt_no = connect_backoff.attempt() + 1;
            match connect(&self.peer.host, self.params.rpc_message_size_limit).await {
                Ok(c) => {
                    debug!(
                        "replication: connected to peer node_id={} ({}) after {} attempt(s)",
                        self.peer.peer_id, self.peer.host, attempt_no
                    );
                    break c;
                }
                Err(e) => {
                    let next = connect_backoff.peek_delay();
                    warn!(
                        "replication: connect to peer node_id={} ({}) failed (attempt {}): {} — retrying in {}ms",
                        self.peer.peer_id,
                        self.peer.host,
                        attempt_no,
                        e,
                        next.as_millis()
                    );
                    connect_backoff.wait().await;
                }
            }
        };

        let mut buf = vec![0u8; self.params.max_bytes_per_rpc];
        let mut heartbeats_sent = 0u64;
        let mut shipments_sent = 0u64;

        // DIAG-flake-replication: snapshot the leader's view of its own
        // ledger at peer-task launch. Pair this with the periodic
        // "still tailing 0 bytes" print to see whether the writer is
        // making progress while the tailer reads nothing.
        let diag_storage_ptr_at_start = self.ledger_for_diag.storage_ptr();
        info!(
            "DIAG-flake-replication: peer-task starting node_id={} from_tx_id={} \
             ledger.last_commit_id={} storage.last_segment_id={} \
             active_wal_file_len={:?} storage_ptr={:#x}",
            self.peer.peer_id,
            self.from_tx_id,
            self.ledger_for_diag.last_commit_id(),
            self.ledger_for_diag.last_segment_id(),
            self.ledger_for_diag.active_wal_file_len(),
            diag_storage_ptr_at_start,
        );

        while self.alive() {
            let n = self.tailer.tail(self.from_tx_id, &mut buf) as usize;
            if n == 0 {
                // Idle heartbeat: send an empty `AppendEntries` so the
                // follower's fresh `last_commit_id` flows back into our
                // `Quorum` even when the writer has stopped producing.
                // Without this, the majority watermark would freeze at
                // the value the follower returned on the previous
                // non-empty RPC — which is always one batch stale, since
                // `append_wal_entries` queues-then-returns on the follower.
                heartbeats_sent += 1;
                trace!(
                    "replication: heartbeat #{} to peer node_id={} (peer_last_tx={}, leader_commit={})",
                    heartbeats_sent,
                    self.peer.peer_id,
                    self.peer_last_tx,
                    self.majority.get()
                );
                // DIAG-flake-replication: every 250 idle heartbeats
                // (~1 second at the test's 2 ms poll), dump enough
                // state to tell whether (a) the leader's pipeline is
                // still committing locally, (b) the WAL on disk is
                // growing, and (c) the storage Arc has been swapped
                // out from under us by a reseed. Cheap — runs at most
                // once per second per peer.
                if heartbeats_sent.is_multiple_of(250) && shipments_sent == 0 {
                    let cur_storage_ptr = self.ledger_for_diag.storage_ptr();
                    info!(
                        "DIAG-flake-replication: still tailing 0 bytes node_id={} \
                         from_tx_id={} heartbeats_sent={} \
                         ledger.last_commit_id={} storage.last_segment_id={} \
                         active_wal_file_len={:?} storage_ptr={:#x} \
                         storage_ptr_changed_since_start={}",
                        self.peer.peer_id,
                        self.from_tx_id,
                        heartbeats_sent,
                        self.ledger_for_diag.last_commit_id(),
                        self.ledger_for_diag.last_segment_id(),
                        self.ledger_for_diag.active_wal_file_len(),
                        cur_storage_ptr,
                        cur_storage_ptr != diag_storage_ptr_at_start,
                    );
                }
                if !self.send_heartbeat(&mut client).await {
                    // step-down observed on the heartbeat — exit
                    // cleanly so the supervisor can drain us.
                    info!(
                        "replication: peer-task for node_id={} stepping down after higher-term heartbeat response",
                        self.peer.peer_id
                    );
                    return;
                }
                sleep(self.params.poll_interval).await;
                continue;
            }

            shipments_sent += 1;
            let shipment_last_tx = tx_id_of_last_record(&buf[..n])
                .expect("tailer returned non-empty bytes with no tx-bearing record");
            debug!(
                "replication: shipment #{} to peer node_id={} ({} bytes, tx_id={}..={})",
                shipments_sent, self.peer.peer_id, n, self.from_tx_id, shipment_last_tx
            );
            let bytes = buf[..n].to_vec();

            if !self
                .ship_until_accepted(&mut client, bytes, shipment_last_tx)
                .await
            {
                return;
            }
        }

        info!(
            "replication: peer-task for node_id={} stopped — sent {} shipments, {} heartbeats",
            self.peer.peer_id, shipments_sent, heartbeats_sent
        );
    }

    /// Retry `AppendEntries` for the given chunk until the peer accepts it
    /// or `running` is cleared. Returns `true` on success, `false` on shutdown.
    async fn ship_until_accepted(
        &mut self,
        client: &mut NodeClient<Channel>,
        bytes: Vec<u8>,
        shipment_last_tx: u64,
    ) -> bool {
        // Exponential backoff for the retry loop — start at 50 ms,
        // double up to 3.2 s. Without the cap, a peer that's
        // permanently shutting down (Unavailable on every RPC) would
        // produce ~20 warns/sec; with it, the rate degrades to
        // roughly one warn per 3 s.
        let mut backoff = Backoff::new(BackoffPolicy::exponential(
            Duration::from_millis(50),
            Duration::from_millis(3_200),
        ));
        loop {
            if !self.alive() {
                debug!(
                    "replication: ship_until_accepted aborted for peer node_id={} (running flag cleared)",
                    self.peer.peer_id
                );
                return false;
            }
            let attempt_no = backoff.attempt() + 1;
            let req = proto::AppendEntriesRequest {
                leader_id: self.params.leader_id,
                term: self.params.term,
                prev_tx_id: self.peer_last_tx,
                prev_term: self.params.term,
                from_tx_id: self.from_tx_id,
                to_tx_id: shipment_last_tx,
                wal_bytes: bytes.clone(),
                leader_commit_tx_id: self.majority.get(),
            };

            match client.append_entries(req).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    // Inspect term *before* committing to the
                    // success/reject branches: a term > ours
                    // unconditionally triggers step-down (ADR-0016
                    // §5/§11) regardless of `success`.
                    if self.maybe_step_down(r.term) {
                        return false;
                    }
                    if r.success {
                        debug!(
                            "replication: peer node_id={} accepted shipment tx_id={}..={} (peer_last_tx now {}) on attempt {}",
                            self.peer.peer_id,
                            self.from_tx_id,
                            shipment_last_tx,
                            r.last_tx_id,
                            attempt_no
                        );
                        self.peer_last_tx = shipment_last_tx;
                        self.from_tx_id = shipment_last_tx + 1;
                        // Publish this peer's latest commit watermark and
                        // let the shared tracker recompute the cluster
                        // majority. `advance` is monotonically safe via
                        // `fetch_max` inside `Quorum`.
                        self.majority.advance(self.peer_index, r.last_tx_id);
                        return true;
                    }
                    let next = backoff.peek_delay();
                    warn!(
                        "replication: peer node_id={} rejected append on attempt {} (reason={}, peer last_tx_id={}) — retrying in {}ms",
                        self.peer.peer_id,
                        attempt_no,
                        r.reject_reason,
                        r.last_tx_id,
                        next.as_millis()
                    );
                    backoff.wait().await;
                }
                Err(e) => {
                    let next = backoff.peek_delay();
                    warn!(
                        "replication: AppendEntries to peer node_id={} failed on attempt {} (code={:?}): {} — reconnecting + retrying in {}ms",
                        self.peer.peer_id,
                        attempt_no,
                        e.code(),
                        e.message(),
                        next.as_millis()
                    );
                    backoff.wait().await;
                    match connect(&self.peer.host, self.params.rpc_message_size_limit).await {
                        Ok(c) => {
                            debug!(
                                "replication: reconnected to peer node_id={} after RPC failure",
                                self.peer.peer_id
                            );
                            *client = c;
                        }
                        Err(reconnect_err) => {
                            debug!(
                                "replication: reconnect to peer node_id={} failed: {} — keeping stale channel for now",
                                self.peer.peer_id, reconnect_err
                            );
                        }
                    }
                }
            }
        }
    }

    /// Send a zero-byte `AppendEntries` to refresh the peer's
    /// `last_commit_id` in our `Quorum`. Failures are swallowed — the
    /// next heartbeat (or real shipment) will retry; the purpose here is
    /// purely observational.
    ///
    /// Returns `false` iff the heartbeat response carried a strictly
    /// higher term — the caller must exit immediately so the
    /// supervisor's leader-drain can collect us. All other outcomes
    /// (success, transport error, ignored reject) return `true` and
    /// the run loop carries on.
    async fn send_heartbeat(&mut self, client: &mut NodeClient<Channel>) -> bool {
        let req = proto::AppendEntriesRequest {
            leader_id: self.params.leader_id,
            term: self.params.term,
            prev_tx_id: self.peer_last_tx,
            prev_term: self.params.term,
            from_tx_id: self.from_tx_id,
            to_tx_id: self.peer_last_tx,
            wal_bytes: Vec::new(),
            leader_commit_tx_id: self.majority.get(),
        };
        match client.append_entries(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                // Even an idle heartbeat must observe a higher term:
                // a fresh leader could have been elected while we
                // were silent, in which case we step down here
                // before the next shipment loop iteration ever
                // wakes.
                if self.maybe_step_down(r.term) {
                    return false;
                }
                if r.success {
                    self.majority.advance(self.peer_index, r.last_tx_id);
                }
                true
            }
            Err(_) => {
                // Transport hiccup on an idle heartbeat is not worth
                // acting on — the next cycle will reconnect via the
                // normal `ship_until_accepted` path.
                true
            }
        }
    }
}

pub(crate) async fn connect(
    addr: &str,
    max_message_bytes: usize,
) -> Result<NodeClient<Channel>, tonic::transport::Error> {
    let endpoint = Endpoint::from_shared(addr.to_string())?;
    let channel = endpoint.connect().await?;
    Ok(NodeClient::new(channel)
        .max_decoding_message_size(max_message_bytes)
        .max_encoding_message_size(max_message_bytes))
}

/// tx_id field at offset 8 of the last 40-byte record in `bytes`.
/// Returns `None` when `bytes` is empty or not record-aligned.
fn tx_id_of_last_record(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(WAL_RECORD_SIZE) {
        return None;
    }
    let off = bytes.len() - WAL_RECORD_SIZE + 8;
    Some(u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?))
}
