//! Candidate state — runs one Raft election round (ADR-0016 §5).
//!
//! The supervisor calls [`run_election_round`] when its election
//! timer expires while the node is in `Initializing` (or `Follower`,
//! once that role is reachable post-Stage 4 step-down). One round:
//!
//! 1. Compute `new_term` in memory (no `term.log` write yet).
//! 2. Persist the term claim via `vote.vote(new_term, self_id)` —
//!    Raft §5.2: a candidate's claim to a new term is recorded by
//!    voting for itself.
//! 3. Fan out `RequestVote` to every other peer in parallel.
//! 4. Tally responses against the cluster-wide majority. Three
//!    outcomes:
//!    - **Won** — strict majority granted; commit the durable term
//!      boundary via `term.commit_term(new_term, start_tx)` and
//!      promote to Leader.
//!    - **HigherTermSeen(term)** — any peer reported a term above
//!      ours; observe it locally (`term.observe` + `vote.observe_term`)
//!      and step down to Initializing.
//!    - **Lost** — neither won nor saw a higher term within a
//!      bounded election deadline; supervisor will spin Initializing
//!      again with a fresh randomised election timeout. The term
//!      log is **not** mutated by a lost round.

use super::peer_replication::connect;
use super::{Term, Vote};
use crate::cluster::config::{Config, PeerConfig};
use crate::cluster::proto::node as proto;
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

    // 1. Compute `new_term` in memory only. Use max-of-(term, vote)
    // because `vote.current_term` may legitimately lead `term.current`
    // after an asymmetric step-down (e.g. supervisor's `term.observe`
    // succeeded but `vote.observe_term` failed in a previous round, or
    // vice versa). We do **not** touch `term.log` here — that record
    // is only legitimate if we actually win this round. Lost rounds
    // must not pollute the durable term-boundary log.
    let new_term = term
        .get_current_term()
        .max(vote.get_current_term())
        .checked_add(1)
        .ok_or_else(|| std::io::Error::other("candidate: u64 term overflow"))?;

    // 2. Persist the term claim via the vote log — Raft §5.2.
    // `vote.vote` refuses (`Ok(false)`) if a concurrent observer
    // already pushed `vote.current_term` past `new_term`, in which
    // case we abandon this round.
    if !vote.vote(new_term, self_id)? {
        warn!(
            "candidate: node_id={} could not self-vote in term {}; aborting round",
            self_id, new_term
        );
        return Ok(ElectionOutcome::Lost);
    }
    info!(
        "candidate: node_id={} claimed term {}",
        self_id, new_term
    );

    let majority = cluster.peers.len() / 2 + 1;
    let mut granted_count: usize = 1; // self

    // Fast path: single-node cluster — self-vote is already a
    // majority of 1.
    let other_peers: Vec<PeerConfig> = config.other_peers().cloned().collect();
    if other_peers.is_empty() {
        return commit_won(term, ledger, vote, new_term);
    }

    // 3. Fan out RequestVote in parallel.
    let our_last_tx_id = ledger.last_commit_id();
    // Match the follower's default at `node_handler.rs::request_vote`
    // (the §5.4.1 up-to-date check). An empty term log means term 0.
    let our_last_term = term.last_record().map(|r| r.term).unwrap_or(0);
    // RequestVote is four u64s on the wire. A small fixed cap is
    // appropriate; sizing off `append_entries_max_bytes` would be
    // misleading.
    let rpc_max_bytes = 64 * 1024;
    // Per-RPC timeout: a single stuck connect should not burn the
    // whole round. Half the round deadline leaves headroom for the
    // collect loop to drain.
    let per_rpc_deadline = round_deadline / 2;

    let mut tasks: JoinSet<RoundReply> = JoinSet::new();
    for peer in other_peers {
        let req = proto::RequestVoteRequest {
            term: new_term,
            candidate_id: self_id,
            last_tx_id: our_last_tx_id,
            last_term: our_last_term,
        };
        tasks.spawn(async move {
            tokio::time::timeout(per_rpc_deadline, request_vote_one(peer, req, rpc_max_bytes))
                .await
                .unwrap_or(RoundReply::TransportError)
        });
    }

    // 4. Collect with an overall deadline. We stop early on
    // majority-won OR higher-term-seen. We deliberately do not
    // return `ElectionOutcome` from inside the async block — that
    // would force `term`/`vote`/`ledger` to be moved in. Use a
    // primitive `RoundResult` and dispatch after the await.
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
                    // A peer granting while reporting a higher term
                    // is protocol-violating: a grant means "I voted
                    // for you in your term", so the peer's term must
                    // not exceed ours. Treat as no-vote noise rather
                    // than letting one misbehaving peer torpedo a
                    // healthy election.
                    if peer_term > new_term {
                        warn!(
                            "candidate: peer granted with term {} > {} — ignoring as protocol violation",
                            peer_term, new_term
                        );
                        continue;
                    }
                    granted_count += 1;
                    if granted_count >= majority {
                        return RoundResult::Won;
                    }
                }
                RoundReply::Refused { peer_term } => {
                    if peer_term > new_term {
                        return RoundResult::HigherTerm(peer_term);
                    }
                }
                RoundReply::TransportError => {
                    // Counts as a no-vote; election proceeds.
                }
            }
        }
        RoundResult::Lost
    };

    let result = match tokio::time::timeout(round_deadline, collect).await {
        Ok(r) => r,
        Err(_) => RoundResult::Lost,
    };

    match result {
        RoundResult::Won => commit_won(term, ledger, vote, new_term),
        RoundResult::HigherTerm(observed) => {
            // Candidate observes the higher term itself before
            // returning, so the supervisor can simply set Role and
            // step down. Both calls are durable + idempotent.
            if let Err(e) = term.observe(observed, ledger.last_commit_id()) {
                warn!(
                    "candidate: term.observe({}) on step-down failed: {}",
                    observed, e
                );
            }
            if let Err(e) = vote.observe_term(observed) {
                warn!(
                    "candidate: vote.observe_term({}) on step-down failed: {}",
                    observed, e
                );
            }
            Ok(ElectionOutcome::HigherTermSeen {
                observed_term: observed,
            })
        }
        RoundResult::Lost => Ok(ElectionOutcome::Lost),
    }
}

/// Won-branch tail: durably commit the term boundary at `new_term`.
/// Handles two edge cases:
/// - `term.current` lags `vote.current_term` (asymmetric step-down in
///   a prior round): catch up via `term.observe(new_term - 1, _)`.
/// - A concurrent `request_vote` / `AppendEntries` handler observed a
///   higher term while our election was in flight (`commit_term`
///   returns `Ok(false)`): align the vote layer and report
///   `HigherTermSeen`.
fn commit_won(
    term: &Arc<Term>,
    ledger: &Arc<Ledger>,
    vote: &Arc<Vote>,
    new_term: u64,
) -> std::io::Result<ElectionOutcome> {
    let start_tx = ledger.last_commit_id();
    let cur = term.get_current_term();
    if cur + 1 < new_term {
        term.observe(new_term - 1, start_tx)?;
    }
    match term.commit_term(new_term, start_tx)? {
        true => Ok(ElectionOutcome::Won {
            elected_term: new_term,
        }),
        false => {
            let observed = term.get_current_term();
            if observed > vote.get_current_term()
                && let Err(e) = vote.observe_term(observed)
            {
                warn!(
                    "candidate: vote.observe_term({}) on race step-down failed: {}",
                    observed, e
                );
            }
            Ok(ElectionOutcome::HigherTermSeen {
                observed_term: observed,
            })
        }
    }
}

/// Internal collect-loop result. Kept primitive so the inner async
/// block doesn't capture `term`/`vote`/`ledger`.
enum RoundResult {
    Won,
    HigherTerm(u64),
    Lost,
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
