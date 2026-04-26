//! Candidate state — runs one Raft election round (ADR-0016 §5).
//!
//! The supervisor calls [`run_election_round`] when its election
//! timer expires while the node is in `Initializing` (or `Follower`,
//! once that role is reachable post-Stage 4 step-down). One round:
//!
//! 1. Bump our durable term: `term.new_term(start_tx)`.
//! 2. Persist self-vote: `vote.vote(new_term, self_id)`.
//! 3. Fan out `RequestVote` to every other peer in parallel.
//! 4. Tally responses against the cluster-wide majority. Three
//!    outcomes:
//!    - **Won** — strict majority granted; promote to Leader.
//!    - **HigherTermSeen(term)** — any peer reported a term above
//!      our new term; step down to Initializing and observe the
//!      new term.
//!    - **Lost** — neither won nor saw a higher term within a
//!      bounded election deadline; supervisor will spin Initializing
//!      again with a fresh randomised election timeout.

use crate::cluster::config::{Config, PeerConfig};
use crate::cluster::peer_replication::connect;
use crate::cluster::proto::node as proto;
use crate::cluster::{Term, Vote};
use crate::ledger::Ledger;
use spdlog::{info, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

/// Outcome of a single election round.
#[derive(Debug)]
pub enum ElectionOutcome {
    /// We received `granted_count` of the votes (always
    /// `>= majority`); promote to Leader at `term = elected_term`.
    Won { elected_term: u64 },
    /// At least one peer responded with a strictly higher term —
    /// step down to Initializing and observe the new term.
    HigherTermSeen { observed_term: u64 },
    /// Neither won nor saw a higher term. Caller should re-arm the
    /// election timer and try again.
    Lost,
}

/// Run one election round. The caller is responsible for:
/// - Detecting election timer expiry (we don't manage timers).
/// - Re-randomising the election timeout between rounds (passed in
///   as `round_deadline_ms`).
/// - Stepping down to Initializing on `HigherTermSeen`.
/// - Promoting to Leader on `Won`.
pub async fn run_election_round(
    config: &Config,
    ledger: &Arc<Ledger>,
    term: &Arc<Term>,
    vote: &Arc<Vote>,
    round_deadline: Duration,
) -> std::io::Result<ElectionOutcome> {
    let cluster = config
        .cluster
        .as_ref()
        .expect("run_election_round requires a clustered config");
    let self_id = config.node_id();

    // 1. Bump our durable term. `start_tx_id` is the first tx_id we
    // would write if we won — Raft §5.2 says elections claim a
    // fresh term, and our `Term::new_term` records the boundary
    // tx_id that the new term covers.
    let start_tx = ledger.last_commit_id();
    let new_term = term.new_term(start_tx)?;
    info!(
        "candidate: node_id={} bumped to term {} at start_tx_id={}",
        self_id, new_term, start_tx
    );

    // 2. Durable self-vote. `vote.vote` fdatasyncs `vote.log`
    // before returning. Granting our own vote here is one of the
    // votes counted toward the majority below.
    let self_vote_granted = vote.vote(new_term, self_id)?;
    if !self_vote_granted {
        // We had voted for someone else in this term already. This
        // shouldn't happen on a fresh term-bump (we just minted
        // `new_term`) but guard against weird `Vote` interactions.
        warn!(
            "candidate: node_id={} could not self-vote in term {}; aborting round",
            self_id, new_term
        );
        return Ok(ElectionOutcome::Lost);
    }

    let majority = cluster.peers.len() / 2 + 1;
    let mut granted_count: usize = 1; // self
    let mut highest_observed_term = new_term;

    // Fast path: single-node cluster (zero other peers) —
    // self-vote is already a majority of 1.
    let other_peers: Vec<PeerConfig> = config.other_peers().cloned().collect();
    if other_peers.is_empty() {
        return Ok(ElectionOutcome::Won {
            elected_term: new_term,
        });
    }

    // 3. Fan out RequestVote in parallel.
    let our_last_tx_id = ledger.last_commit_id();
    let our_last_term = term.last_record().map(|r| r.term).unwrap_or(new_term);
    let rpc_max_bytes = cluster.append_entries_max_bytes * 2 + 4 * 1024;

    let mut tasks: JoinSet<RoundReply> = JoinSet::new();
    for peer in other_peers {
        let req = proto::RequestVoteRequest {
            term: new_term,
            candidate_id: self_id,
            last_tx_id: our_last_tx_id,
            last_term: our_last_term,
        };
        tasks.spawn(async move { request_vote_one(peer, req, rpc_max_bytes).await });
    }

    // 4. Collect with an overall deadline. We stop early on
    // majority-won OR higher-term-seen.
    let collect = async {
        while let Some(joined) = tasks.join_next().await {
            let reply = match joined {
                Ok(r) => r,
                Err(e) => {
                    warn!("candidate: rpc task join error: {}", e);
                    continue;
                }
            };
            match reply {
                RoundReply::Granted { peer_term } => {
                    if peer_term > highest_observed_term {
                        // Edge case: a peer granted under a higher
                        // term than ours? Treat as step-down signal.
                        return ElectionOutcome::HigherTermSeen {
                            observed_term: peer_term,
                        };
                    }
                    granted_count += 1;
                    if granted_count >= majority {
                        return ElectionOutcome::Won {
                            elected_term: new_term,
                        };
                    }
                }
                RoundReply::Refused { peer_term } => {
                    if peer_term > highest_observed_term {
                        highest_observed_term = peer_term;
                    }
                    if peer_term > new_term {
                        return ElectionOutcome::HigherTermSeen {
                            observed_term: peer_term,
                        };
                    }
                }
                RoundReply::TransportError => {
                    // Counts as a no-vote; election proceeds.
                }
            }
        }
        // All RPCs returned but we didn't reach majority.
        ElectionOutcome::Lost
    };

    match tokio::time::timeout(round_deadline, collect).await {
        Ok(outcome) => Ok(outcome),
        Err(_) => Ok(ElectionOutcome::Lost),
    }
}

#[derive(Debug)]
enum RoundReply {
    Granted { peer_term: u64 },
    Refused { peer_term: u64 },
    TransportError,
}

async fn request_vote_one(
    peer: PeerConfig,
    req: proto::RequestVoteRequest,
    rpc_max_bytes: usize,
) -> RoundReply {
    let mut client = match connect(&peer.host, rpc_max_bytes).await {
        Ok(c) => c,
        Err(_) => return RoundReply::TransportError,
    };
    match client.request_vote(req).await {
        Ok(resp) => {
            let r = resp.into_inner();
            if r.vote_granted {
                RoundReply::Granted { peer_term: r.term }
            } else {
                RoundReply::Refused { peer_term: r.term }
            }
        }
        Err(_) => RoundReply::TransportError,
    }
}
