use crate::consensus::state::Consensus;
use proto::node::RejectReason as ProtoRejectReason;
use proto::node::{
    ReplicationFollowerMessage, ReplicationFollowerMessageHandshakeResponse,
    ReplicationLeaderMessage, ReplicationLeaderMessageHandshake,
};
use raft::{HandshakeDecision, RejectReason};
use spdlog::{debug, error};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, Streaming};

pub struct ReplicationInputStream {
    pub(super) inbound: Streaming<ReplicationLeaderMessage>,
    pub(super) tx: Sender<Result<ReplicationFollowerMessage, Status>>,
}

impl Consensus {
    pub async fn replication_stream(
        self: &Arc<Self>,
        inbound: Streaming<ReplicationLeaderMessage>,
    ) -> ReceiverStream<Result<ReplicationFollowerMessage, Status>> {
        let (tx, rx) = mpsc::channel::<Result<ReplicationFollowerMessage, Status>>(64);
        let is = ReplicationInputStream { inbound, tx };
        if let Err(e) = self.replication_input_tx.send(is).await {
            error!(
                "replication_stream[{}]: driver channel closed: {}",
                self.node_id(),
                e
            );
        } else {
            debug!(
                "replication_stream[{}]: inbound stream accepted",
                self.node_id()
            );
        }
        ReceiverStream::new(rx)
    }

    pub async fn replication_follower_handshake(
        &self,
        handshake_msg: ReplicationLeaderMessageHandshake,
    ) -> ReplicationFollowerMessageHandshakeResponse {
        let nid = self.node_id();
        debug!(
            "replication_follower[{}]: handshake received leader={} term={} prev_log_tx_id={} prev_log_term={}",
            nid,
            handshake_msg.leader_id,
            handshake_msg.leader_term,
            handshake_msg.prev_log_tx_id,
            handshake_msg.prev_log_term
        );
        let now = Instant::now();
        let decision = {
            let mut node = self.raft_node.lock().expect("raft mutex poisoned");
            let records = if handshake_msg.leader_term_first_tx_id != 0 {
                vec![raft::TermRecord {
                    term: handshake_msg.leader_term,
                    start_tx_id: handshake_msg.leader_term_first_tx_id,
                }]
            } else {
                Vec::new()
            };
            let d = node.validate_handshake(
                now,
                handshake_msg.leader_id,
                handshake_msg.leader_term,
                &records,
                handshake_msg.prev_log_tx_id,
                handshake_msg.prev_log_term,
            );
            if matches!(d, HandshakeDecision::Accept) {
                node.advance_cluster_index(handshake_msg.leader_commit);
            }
            d
        };
        self.notify_role();
        self.publish_cluster_commit();

        // ADR-0016 §9 supervisor: when raft signals a divergent
        // uncommitted tail, reseed the ledger BEFORE we report the
        // watermark back so the leader's next WalUpdate lands on the
        // truncated state.
        let truncate_after = match &decision {
            HandshakeDecision::Reject { truncate_after, .. } => *truncate_after,
            _ => None,
        };
        if let Some(after) = truncate_after
            && let Err(e) = self.ledger.reseed(after)
        {
            error!(
                "replication_follower[{}]: reseed(after={}) failed: {}",
                nid, after, e
            );
        }

        let (last_term, last_term_first_tx_id) = self
            .durable
            .term
            .last_record()
            .map(|r| (r.term, r.start_tx_id))
            .unwrap_or((0, 0));
        let last_term_curr_tx_id = self
            .raft_node
            .lock()
            .expect("raft mutex poisoned")
            .commit_index();

        let (success, reject_reason) = match decision {
            HandshakeDecision::Accept => (true, ProtoRejectReason::RejectNone),
            HandshakeDecision::Reject { reason, .. } => {
                let mapped = match reason {
                    RejectReason::TermBehind => ProtoRejectReason::RejectTermStale,
                    RejectReason::LogMismatch => ProtoRejectReason::RejectPrevMismatch,
                    RejectReason::RpcTimeout => ProtoRejectReason::RejectNone,
                };
                (false, mapped)
            }
        };

        debug!(
            "replication_follower[{}]: handshake decision success={} reject={:?}",
            nid, success, reject_reason
        );
        ReplicationFollowerMessageHandshakeResponse {
            success,
            last_term,
            last_term_first_tx_id,
            last_term_curr_tx_id,
            reject_reason: reject_reason as u32,
        }
    }
}
