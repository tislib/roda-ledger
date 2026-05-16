use super::state::Consensus;
use crate::config::Config;
use ledger::transaction::Operation;
use proto::node::RequestVoteRequest;
use proto::node::node_client::NodeClient;
use raft::{NodeId, RequestVote, Role, VoteOutcome};
use spdlog::{debug, error};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

const VOTE_RPC_TIMEOUT: Duration = Duration::from_millis(50);
const VOTE_CONNECT_TIMEOUT: Duration = Duration::from_millis(10);

impl Consensus {
    pub async fn run_loop(&self, cancel: CancellationToken) -> Result<(), String> {
        let self_id = self
            .raft_node
            .lock()
            .expect("raft mutex poisoned")
            .self_id();
        debug!("election[{}]: loop started", self_id);

        loop {
            let now = Instant::now();
            let wakeup = {
                let mut node = self.raft_node.lock().expect("raft mutex poisoned");
                node.election().tick(now)
            };
            self.self_advance();
            let started = {
                let mut node = self.raft_node.lock().expect("raft mutex poisoned");
                node.election().start(now)
            };
            if started {
                self.notify_role();
            }

            if started {
                let (requests, term_after) = {
                    let mut node = self.raft_node.lock().expect("raft mutex poisoned");
                    let term = node.current_term();
                    (node.election().get_requests(), term)
                };
                debug!(
                    "election[{}]: election started term={} peers={}",
                    self_id,
                    term_after,
                    requests.len()
                );
                if !requests.is_empty() {
                    self.self_advance();
                    let responses =
                        send_request_votes_concurrent(self_id, requests, self.config.clone()).await;
                    let grant_count = responses
                        .iter()
                        .filter(|(_, o)| matches!(o, VoteOutcome::Granted { .. }))
                        .count() as u16;
                    {
                        let mut node = self.raft_node.lock().expect("raft mutex poisoned");
                        node.election().handle_votes(Instant::now(), responses);
                    }
                    self.notify_role();
                    let (post_term, post_role) = {
                        let node = self.raft_node.lock().expect("raft mutex poisoned");
                        (node.current_term(), node.role())
                    };
                    debug!(
                        "election[{}]: post-vote term={} role={:?} grants={}",
                        self_id, post_term, post_role, grant_count
                    );
                    if post_role == Role::Leader {
                        let node_count = self
                            .config
                            .cluster
                            .as_ref()
                            .map(|c| c.peers.len())
                            .unwrap_or(0) as u16;
                        self.ledger.current().submit(Operation::NewTerm {
                            term: post_term,
                            node_id: self_id,
                            node_count,
                            node_voted: grant_count,
                        });
                        debug!(
                            "election[{}]: NewTerm submitted term={} node_count={} grants={}",
                            self_id, post_term, node_count, grant_count
                        );
                    }
                }
            }

            let sleep = tokio::time::sleep_until(wakeup.deadline.into());
            tokio::select! {
                _ = sleep => {}
                _ = cancel.cancelled() => {
                    debug!("election[{}]: cancel signalled; shutting down", self_id);
                    break;
                }
            }
        }

        debug!("election[{}]: loop exiting", self_id);
        Ok(())
    }
}

async fn send_request_votes_concurrent(
    candidate_id: NodeId,
    requests: Vec<(NodeId, RequestVote)>,
    config: Arc<Config>,
) -> Vec<(NodeId, VoteOutcome)> {
    let mut handles = Vec::with_capacity(requests.len());
    for (peer, req) in requests {
        let config = config.clone();
        let handle = tokio::spawn(async move {
            let outcome = send_one_request_vote(candidate_id, peer, req, config).await;
            (peer, outcome)
        });
        handles.push(handle);
    }
    let mut results = Vec::with_capacity(handles.len());
    for h in handles {
        match h.await {
            Ok(pair) => results.push(pair),
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => error!(
                "election[{}]: RequestVote task cancelled: {}",
                candidate_id, e
            ),
        }
    }
    results
}

async fn send_one_request_vote(
    candidate_id: NodeId,
    peer: NodeId,
    req: RequestVote,
    config: Arc<Config>,
) -> VoteOutcome {
    let mut client = match get_node_client(&config, peer).await {
        Ok(c) => c,
        Err(err) => {
            error!(
                "election[{}]: peer={} connect failed: {}",
                candidate_id, peer, err
            );
            return VoteOutcome::Failed;
        }
    };
    let rpc_fut = client.request_vote(RequestVoteRequest {
        term: req.term,
        candidate_id,
        last_tx_id: req.last_tx_id,
        last_term: req.last_term,
    });
    let resp = match tokio::time::timeout(VOTE_RPC_TIMEOUT, rpc_fut).await {
        Ok(Ok(r)) => r.into_inner(),
        Ok(Err(e)) => {
            error!(
                "election[{}]: peer={} rpc failed: {}",
                candidate_id, peer, e
            );
            return VoteOutcome::Failed;
        }
        Err(_) => {
            error!(
                "election[{}]: peer={} timed out after {:?}",
                candidate_id, peer, VOTE_RPC_TIMEOUT
            );
            return VoteOutcome::Failed;
        }
    };
    if resp.vote_granted {
        debug!(
            "election[{}]: peer={} granted term={}",
            candidate_id, peer, resp.term
        );
        VoteOutcome::Granted { term: resp.term }
    } else {
        debug!(
            "election[{}]: peer={} denied term={}",
            candidate_id, peer, resp.term
        );
        VoteOutcome::Denied { term: resp.term }
    }
}

async fn get_node_client(
    config: &Config,
    node_id: NodeId,
) -> Result<NodeClient<Channel>, ConnectError> {
    let host = config
        .cluster
        .as_ref()
        .ok_or(ConnectError::NotClustered)?
        .peers
        .iter()
        .find(|p| p.peer_id == node_id)
        .ok_or(ConnectError::UnknownPeer(node_id))?
        .host
        .clone();

    match tokio::time::timeout(VOTE_CONNECT_TIMEOUT, NodeClient::connect(host)).await {
        Ok(Ok(client)) => Ok(client),
        Ok(Err(e)) => Err(ConnectError::Transport(e)),
        Err(_) => Err(ConnectError::Timeout),
    }
}

#[derive(Debug)]
enum ConnectError {
    NotClustered,
    UnknownPeer(NodeId),
    Timeout,
    Transport(tonic::transport::Error),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::NotClustered => write!(f, "not clustered"),
            ConnectError::UnknownPeer(id) => write!(f, "unknown peer {}", id),
            ConnectError::Timeout => write!(f, "timeout"),
            ConnectError::Transport(e) => write!(f, "transport error: {}", e),
        }
    }
}
