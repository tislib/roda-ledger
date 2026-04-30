//! Leader role-specific state.
//!
//! Owns one `PeerProgress` per follower — `next_index`,
//! `match_index`, `in_flight` window, and `next_heartbeat` deadline.
//! The state machine consults it on every `Event::AppendEntriesReply`
//! to advance/regress `next_index` (Raft §5.3 `next_index -= 1` on
//! `LogMismatch`) and on every `Tick` to decide which peers need a
//! fresh AppendEntries.
//!
//! The reply's two watermarks (`last_write_id`, `last_commit_id`) drive
//! `next_index` and `match_index` independently — see ADR-0017 §"AE
//! reply: write vs commit watermark".

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::types::{NodeId, TxId};

/// One in-flight AppendEntries window. The state machine only
/// matches on `expires_at` (for timeouts); the field
/// `last_tx_id_in_batch` is kept for diagnostics/symmetry but is no
/// longer consulted to advance `match_index` — replies carry a
/// `last_commit_id` of their own (the peer's durable end), which is
/// what the leader uses. No rpc-id is needed because the library
/// serialises one in-flight RPC per peer at a time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InFlightAppend {
    pub last_tx_id_in_batch: TxId,
    pub expires_at: Instant,
}

#[derive(Clone, Debug)]
pub struct PeerProgress {
    /// Next tx_id to ship to this peer. On `LogMismatch` reject we
    /// decrement (clamped at 1) until we find the agreement point.
    /// On success we advance to the reply's `last_write_id + 1` (the
    /// peer's accepted/written end — "what the peer already has, so
    /// don't re-ship it"). See ADR-0017 §"AE reply: write vs commit
    /// watermark" for why this is the write-id, not the commit-id.
    pub next_index: TxId,
    /// Highest tx_id known durably replicated on this peer. Advanced
    /// from each reply's `last_commit_id` (the peer's durable end).
    /// Drives the cluster-wide quorum.
    pub match_index: TxId,
    pub in_flight: Option<InFlightAppend>,
    /// Deadline at which this peer needs a fresh AppendEntries (real
    /// or heartbeat). Re-armed on every send.
    pub next_heartbeat: Instant,
}

impl PeerProgress {
    pub fn new(initial_next_index: TxId, next_heartbeat: Instant) -> Self {
        Self {
            next_index: initial_next_index,
            match_index: 0,
            in_flight: None,
            next_heartbeat,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LeaderState {
    /// Per-peer progress keyed by `NodeId`. `BTreeMap` so iteration
    /// order is deterministic across runs — `leader_drive` scans
    /// peers each tick to decide who to send to, and the simulator
    /// relies on stable order for reproducibility.
    pub peers: BTreeMap<NodeId, PeerProgress>,
    /// Cadence at which we send heartbeats to each peer.
    pub heartbeat_interval: Duration,
    /// Per-RPC deadline. Independent of the heartbeat cadence — an
    /// RPC fired late in a heartbeat window can still time out
    /// before the next one is queued.
    pub rpc_timeout: Duration,
    /// Highest tx_id durably written to the leader's raft log.
    /// Bounds the replication window in `leader_send_to`. Advanced
    /// by `Event::LocalWriteAdvanced`. Initialised at leader-win to
    /// the leader's `local_log_index` so a fresh leader can
    /// immediately replicate whatever it already has on disk.
    /// Independent of `local_log_index` and `cluster_commit_index`
    /// — see the event doc for the separation of concerns.
    pub last_written: TxId,
}

impl LeaderState {
    /// Construct from the leader's view at bring-up. `last_local_tx`
    /// is the leader's own commit progress; new peers start at
    /// `next_index = last_local_tx + 1`. `last_written` seeds the
    /// replication-window watermark — pass the leader's
    /// `local_log_index` at win time so already-on-disk entries are
    /// shippable from the first `Action::SendAppendEntries`.
    pub fn new(
        peer_ids: &[NodeId],
        last_local_tx: TxId,
        last_written: TxId,
        now: Instant,
        heartbeat_interval: Duration,
        rpc_timeout: Duration,
    ) -> Self {
        let initial_next = last_local_tx + 1;
        let peers = peer_ids
            .iter()
            .copied()
            .map(|p| (p, PeerProgress::new(initial_next, now)))
            .collect();
        Self {
            peers,
            heartbeat_interval,
            rpc_timeout,
            last_written,
        }
    }

    /// Soonest `next_heartbeat` across all peers, or the soonest
    /// `expires_at` of any in-flight RPC, whichever is earlier.
    /// Returned to the state machine as the candidate `Action::SetWakeup`
    /// time.
    pub fn next_wakeup(&self) -> Option<Instant> {
        let mut best: Option<Instant> = None;
        for p in self.peers.values() {
            if let Some(d) = best {
                best = Some(d.min(p.next_heartbeat));
            } else {
                best = Some(p.next_heartbeat);
            }
            if let Some(infl) = p.in_flight {
                let d = best.unwrap_or(infl.expires_at).min(infl.expires_at);
                best = Some(d);
            }
        }
        best
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn new_initializes_peers_at_next_after_local_tx() {
        let now = t0();
        let state = LeaderState::new(
            &[2, 3],
            10,
            10,
            now,
            Duration::from_millis(50),
            Duration::from_millis(200),
        );
        assert_eq!(state.peers.len(), 2);
        assert_eq!(state.last_written, 10);
        for p in state.peers.values() {
            assert_eq!(p.next_index, 11);
            assert_eq!(p.match_index, 0);
            assert!(p.in_flight.is_none());
            assert_eq!(p.next_heartbeat, now);
        }
    }

    #[test]
    fn next_wakeup_picks_earliest_deadline() {
        let now = t0();
        let mut state = LeaderState::new(
            &[2, 3, 4],
            0,
            0,
            now,
            Duration::from_millis(50),
            Duration::from_millis(200),
        );
        // Push peer 3 forward; soonest is still peer 2 / 4 at `now`.
        state.peers.get_mut(&3).unwrap().next_heartbeat = now + Duration::from_secs(10);
        assert_eq!(state.next_wakeup(), Some(now));

        // Add an in-flight RPC due before `now` — wakeup snaps back
        // to that. (Possible if the test pretends time advanced.)
        state.peers.get_mut(&2).unwrap().in_flight = Some(InFlightAppend {
            last_tx_id_in_batch: 5,
            expires_at: now - Duration::from_millis(1),
        });
        assert_eq!(state.next_wakeup(), Some(now - Duration::from_millis(1)));
    }
}
