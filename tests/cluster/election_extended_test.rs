//! Leader election + term/vote durability (cluster-level).

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::node as nproto;
use roda_ledger::cluster::proto::node::node_client::NodeClient;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl, Role, Vote};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tonic::Request;

async fn ping(node_port: u16) -> Option<(nproto::NodeRole, u64, u64)> {
    let mut client = NodeClient::connect(format!("http://127.0.0.1:{}", node_port))
        .await
        .ok()?;
    let resp = client
        .ping(nproto::PingRequest {
            from_node_id: 0,
            nonce: 0,
        })
        .await
        .ok()?;
    let r = resp.into_inner();
    let role = nproto::NodeRole::try_from(r.role).ok()?;
    Some((role, r.term, r.last_tx_id))
}

// ── Cold-boot election + invariants ---------------------------------------

/// Cold-boot election in a 3-node cluster: exactly one Leader, others
/// settle as Follower, and no two leaders ever appear in the same term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_elect_a_unique_leader() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");

    let leader_idx = ctl
        .wait_for_leader(Duration::from_millis(5 * 300))
        .await
        .expect("cold-boot leader");
    let leader_id = ctl.node_id(leader_idx).unwrap();

    // For ~400 ms, no two nodes may report Leader in the same term.
    let deadline = Instant::now() + Duration::from_millis(400);
    while Instant::now() < deadline {
        let mut leader_terms: Vec<u64> = Vec::new();
        for i in 0..ctl.len() {
            if let Some((nproto::NodeRole::Leader, term, _)) =
                ping(ctl.node_port(i).unwrap()).await
            {
                leader_terms.push(term);
            }
        }
        let unique: std::collections::HashSet<u64> = leader_terms.iter().copied().collect();
        assert!(
            !(leader_terms.len() > 1 && unique.len() == 1),
            "two leaders in the same term: {:?}",
            leader_terms
        );
        sleep(Duration::from_millis(40)).await;
    }
    eprintln!("[election] leader = node {}", leader_id);
}

/// Failover: kill the elected leader; survivors run a new election;
/// new leader id ≠ killed id and the new term is strictly higher.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_elects_different_leader_with_higher_term() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");

    let leader_idx = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("first leader");
    let leader_id = ctl.node_id(leader_idx).unwrap();
    let leader_port = ctl.node_port(leader_idx).unwrap();
    let (_, term_pre, _) = ping(leader_port).await.expect("pre-kill ping");

    ctl.stop_node(leader_idx).expect("stop leader");
    sleep(Duration::from_millis(150)).await;

    let new_leader_idx = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("new leader");
    let new_leader_id = ctl.node_id(new_leader_idx).unwrap();
    assert_ne!(new_leader_id, leader_id);

    let (_, term_post, _) = ping(ctl.node_port(new_leader_idx).unwrap())
        .await
        .expect("post-kill ping");
    assert!(
        term_post > term_pre,
        "term must strictly increase across failover: {} -> {}",
        term_pre,
        term_post
    );
}

/// Single-node cluster wins by self-vote with no `RequestVote` RPC fan-out.
/// Observed via `wait_for_leader` returning immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_cluster_self_elects() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let started = Instant::now();
    let leader_idx = ctl
        .wait_for_leader(Duration::from_millis(500))
        .await
        .expect("leader");
    assert_eq!(leader_idx, 0);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "single-node election should not require an election round"
    );
}

/// Election timer reset: a leader's heartbeats keep followers from ever
/// becoming Candidate. Over 3 seconds, no new term is opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeats_prevent_followers_from_starting_election() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_port = ctl.node_port(leader_idx).unwrap();
    let (_, term_at_start, _) = ping(leader_port).await.unwrap();

    sleep(Duration::from_secs(3)).await;

    // Term must not have churned — leader is still leader, no election restarts.
    let (_, term_at_end, _) = ping(leader_port).await.unwrap();
    assert_eq!(
        term_at_end, term_at_start,
        "term churned despite healthy heartbeats: {} -> {}",
        term_at_start, term_at_end
    );
}

// ── Vote layer (Raft §5.4.1) ---------------------------------------------

/// `RequestVote`: stale-term request → refused, response carries our
/// current (higher) term so the candidate can step down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_vote_stale_term_refused_with_our_term() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let term = ctl.term(0).unwrap();
    term.observe(7, 0).unwrap(); // bring our term up to 7
    let handler = ctl.node_handler(0, 2).expect("handler");

    use roda_ledger::cluster::proto::node::node_server::Node;
    let resp = handler
        .request_vote(Request::new(nproto::RequestVoteRequest {
            term: 5, // < our 7
            candidate_id: 99,
            last_tx_id: 0,
            last_term: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.vote_granted);
    assert_eq!(resp.term, 7);
}

/// `RequestVote`: same-term, different candidate already voted for →
/// refused. Same candidate → idempotent grant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_vote_already_voted_for_other_refused_same_idempotent() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let handler = ctl.node_handler(0, 2).expect("handler");

    use roda_ledger::cluster::proto::node::node_server::Node;

    // First vote in term 3 for candidate 5. The handler observes term
    // 3 (term log gets a record `(3, 0)`), so for the up-to-date check
    // candidate must claim `last_term >= 3`.
    let r1 = handler
        .request_vote(Request::new(nproto::RequestVoteRequest {
            term: 3,
            candidate_id: 5,
            last_tx_id: 0,
            last_term: 3,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(r1.vote_granted, "first vote in fresh term must be granted");

    // Same term, different candidate — refused.
    let r2 = handler
        .request_vote(Request::new(nproto::RequestVoteRequest {
            term: 3,
            candidate_id: 7,
            last_tx_id: 0,
            last_term: 3,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!r2.vote_granted);

    // Same term, same candidate — idempotent grant.
    let r3 = handler
        .request_vote(Request::new(nproto::RequestVoteRequest {
            term: 3,
            candidate_id: 5,
            last_tx_id: 0,
            last_term: 3,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(r3.vote_granted);
}

/// `RequestVote`: up-to-date check. Candidate with stale log
/// (smaller last_tx_id under the same term) is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_vote_refuses_stale_log() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let term = ctl.term(0).unwrap();
    let ledger = ctl.ledger(0).unwrap();
    term.new_term(0).unwrap(); // term 1

    // Commit a tx so our last_tx_id > 0.
    let tx_id = ledger.submit(roda_ledger::transaction::Operation::Deposit {
        account: 1,
        amount: 1,
        user_ref: 0,
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while ledger.last_commit_id() < tx_id {
        if Instant::now() > deadline {
            panic!("commit timeout");
        }
        sleep(Duration::from_millis(2)).await;
    }
    assert!(ledger.last_commit_id() >= 1);

    let handler = ctl.node_handler(0, 2).expect("handler");
    use roda_ledger::cluster::proto::node::node_server::Node;

    // Candidate claims (last_term=1, last_tx_id=0). Ours is
    // (last_term=1, last_tx_id=1). Lex: candidate < ours → refuse.
    let r = handler
        .request_vote(Request::new(nproto::RequestVoteRequest {
            term: 2, // higher term, otherwise stale-term blocks
            candidate_id: 99,
            last_tx_id: 0,
            last_term: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        !r.vote_granted,
        "stale-log candidate must be refused (Raft §5.4.1)"
    );
}

/// Higher-term observation persists `voted_for=0` durably before the
/// reply. We send a higher-term `RequestVote` for a candidate the
/// follower's log can't accept (stale log), then reopen `Vote` and
/// confirm the bumped term + cleared vote landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_vote_higher_term_durably_observed() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");

    // Pre-conditions: our term is 1.
    let term = ctl.term(0).unwrap();
    term.new_term(0).unwrap();
    assert_eq!(term.get_current_term(), 1);

    // Commit so our log has something — candidate's stale log triggers refusal.
    let ledger = ctl.ledger(0).unwrap();
    let tx_id = ledger.submit(roda_ledger::transaction::Operation::Deposit {
        account: 1,
        amount: 1,
        user_ref: 0,
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while ledger.last_commit_id() < tx_id {
        if Instant::now() > deadline {
            panic!("commit timeout");
        }
        sleep(Duration::from_millis(2)).await;
    }

    let handler = ctl.node_handler(0, 2).expect("handler");
    use roda_ledger::cluster::proto::node::node_server::Node;

    // Higher-term but stale-log: must be refused, but term observed.
    let _ = handler
        .request_vote(Request::new(nproto::RequestVoteRequest {
            term: 9,
            candidate_id: 99,
            last_tx_id: 0,
            last_term: 1,
        }))
        .await
        .unwrap()
        .into_inner();

    // Term must have advanced.
    assert_eq!(ctl.term(0).unwrap().get_current_term(), 9);
    // voted_for cleared (refused vote, but observe_term wiped it).
    assert_eq!(ctl.vote(0).unwrap().get_voted_for(), None);

    // Durably so: reopen and confirm.
    let dir = ctl.data_dir(0).unwrap().to_string_lossy().into_owned();
    let v_reopen = Vote::open_in_dir(&dir).unwrap();
    assert_eq!(v_reopen.get_current_term(), 9);
    assert_eq!(v_reopen.get_voted_for(), None);
}

/// Two clustered nodes restarted with persistent dirs: term progression
/// is monotonic across restarts (no node observes term going backwards).
/// Uses `start_node` to BRING BACK the killed node so quorum stays
/// reachable across restarts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn term_monotonic_across_restarts() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");

    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let port_init = ctl.node_port(leader_idx).unwrap();
    let (_, term_initial, _) = ping(port_init).await.unwrap();

    // Cycle the leader: stop, restart on the same dir.
    ctl.stop_node(leader_idx).expect("stop");
    sleep(Duration::from_millis(200)).await;
    ctl.start_node(leader_idx).await.expect("restart");

    let new_idx = ctl.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    let (_, term_after, _) = ping(ctl.node_port(new_idx).unwrap()).await.unwrap();
    assert!(
        term_after > term_initial,
        "term must strictly increase across restart: {} -> {}",
        term_initial,
        term_after
    );

    // Cycle whichever node is leading now.
    ctl.stop_node(new_idx).expect("stop");
    sleep(Duration::from_millis(200)).await;
    ctl.start_node(new_idx).await.expect("restart");

    let new_idx2 = ctl.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    let (_, term_after2, _) = ping(ctl.node_port(new_idx2).unwrap()).await.unwrap();
    assert!(term_after2 > term_after);
}
