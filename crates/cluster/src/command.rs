//! Inbound RPC envelopes flowing into the raft / consensus loops.
//!
//! There are two channels so the loops can drain independently:
//!
//! - [`AppendEntriesMsg`] is consumed by `RaftLoop`'s command loop —
//!   AE is replication-side and naturally lives next to the ledger
//!   pipeline plumbing.
//! - [`RequestVoteMsg`] is consumed by `ConsensusLoop` — election is
//!   a separate concern that runs alongside the command loop and the
//!   replication loop.
//!
//! Each variant carries a `oneshot::Sender` for its reply so the gRPC
//! handler can `await` the response without correlation IDs or
//! shared state.

use ::proto::node as proto;
use tokio::sync::oneshot;

/// Inbound `AppendEntries`. Consumed by the raft command loop.
pub struct AppendEntriesMsg {
    pub req: proto::AppendEntriesRequest,
    pub reply: oneshot::Sender<proto::AppendEntriesResponse>,
}

/// Inbound `RequestVote`. Consumed by the consensus loop.
pub struct RequestVoteMsg {
    pub req: proto::RequestVoteRequest,
    pub reply: oneshot::Sender<proto::RequestVoteResponse>,
}
