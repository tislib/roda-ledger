//! Commands flowing into the raft loop's mpsc channel.
//!
//! The loop is the single owner of `RaftNode`. Every mutating
//! interaction arrives here as a `Command`. Inbound RPCs from peers
//! carry a `oneshot::Sender` for their reply. Outbound RPC results
//! (RequestVote replies, AppendEntries replies) flow back through
//! direct method calls — `Election::handle_votes` /
//! `Replication::peer(peer_id).append_result(...)` — not through this
//! channel.
//!
//! Adding a new inbound RPC means adding a variant here, a match arm
//! in the loop's `handle_command`, and a sender call site in the gRPC
//! handler. No correlation IDs, no shared state — the `oneshot` is
//! the request/response pairing.

use ::proto::node as proto;
use tokio::sync::oneshot;

/// One message into the raft loop. Variants split by reply shape:
/// inbound RPCs need a typed reply channel, outbound RPC results just
/// feed an `AppendResult` back into the state machine.
pub enum Command {
    /// Inbound `AppendEntries` from a peer leader. The reply may be sent
    /// synchronously (heartbeat, reject) or deferred (parked until the
    /// follower's WAL durably covers `to_tx_id` — see `RaftLoop`'s
    /// pending-replies map).
    AppendEntries {
        req: proto::AppendEntriesRequest,
        reply: oneshot::Sender<proto::AppendEntriesResponse>,
    },

    /// Inbound `RequestVote` from a candidate peer. Always replied
    /// synchronously — votes are decided immediately on receipt and the
    /// durable vote write happens before the reply is sent.
    RequestVote {
        req: proto::RequestVoteRequest,
        reply: oneshot::Sender<proto::RequestVoteResponse>,
    },
}