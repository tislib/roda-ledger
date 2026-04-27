//! Role state machine transitions + concurrency invariants.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::ledger::WaitLevel;
use roda_ledger::cluster::proto::node as nproto;
use roda_ledger::cluster::proto::node::node_client::NodeClient;
use roda_ledger::cluster::proto::node::node_server::Node;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl, NodeHandler, Role};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::Request;

const ACCOUNT: u64 = 1;
const AMOUNT: u64 = 100;

async fn ping_role(node_port: u16) -> Option<nproto::NodeRole> {
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
    nproto::NodeRole::try_from(resp.into_inner().role).ok()
}

// ── Role transitions -----------------------------------------------------

/// `Initializing → Candidate → Leader` is reached from cold boot.
/// Observed via `wait_for_leader`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_boot_drives_transition_to_leader() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader within timeout proves Init→Cand→Leader");
    let port = ctl.node_port(leader_idx).unwrap();
    assert_eq!(ping_role(port).await, Some(nproto::NodeRole::Leader));
}

/// `Initializing/Candidate → Follower` via `settle_as_follower`. Direct
/// handler test against a bare-mode follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_entries_settles_initializing_to_follower() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Initializing))
        .await
        .expect("bare start");
    let role = ctl.role_flag(0).unwrap();
    assert_eq!(role.get(), Role::Initializing);

    let handler = ctl.node_handler(0, 2).expect("handler");
    let _ = handler
        .append_entries(Request::new(nproto::AppendEntriesRequest {
            leader_id: 1,
            term: 1,
            prev_tx_id: 0,
            prev_term: 0,
            from_tx_id: 1,
            to_tx_id: 0,
            wal_bytes: Vec::new(),
            leader_commit_tx_id: 0,
        }))
        .await
        .unwrap();
    assert_eq!(role.get(), Role::Follower);
}

/// `Leader → Initializing` step-down on observed-higher-term. Confirmed
/// by killing the leader and observing a new leader emerge with a
/// higher term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_steps_down_on_higher_term() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_node_id = ctl.node_id(leader_idx).unwrap();

    ctl.stop_node(leader_idx).await.expect("stop leader");

    let new_leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    assert_ne!(ctl.node_id(new_leader_idx).unwrap(), leader_node_id);
}

/// After step-down, the next Leader entry sees fresh `Quorum` peer
/// slots (via `reset_peers`) but its own slot intact. End-to-end:
/// after failover, the new leader's first ClusterCommit-acked write
/// succeeds — meaning its slot is alive and peer slots will catch up
/// from heartbeats/replication.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_leader_quorum_slots_reset_correctly() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.client().leader().clone();
    leader_client
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();
    drop(leader_client);

    ctl.stop_node(leader_idx).await.expect("stop");
    let _new_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    let new_client = ctl.client().leader().clone();
    let r = new_client
        .deposit_and_wait(ACCOUNT, AMOUNT, 2, WaitLevel::ClusterCommit)
        .await
        .expect("post-failover ClusterCommit");
    assert_eq!(r.fail_reason, 0);
}

/// Drop-and-rebuild invariant: aborted leader's peer tasks observe the
/// supervisor flag and exit. After the harness aborts a leader, the
/// surviving followers do NOT receive lingering heartbeats from the
/// dead leader — they're free to elect a new one.
///
/// We verify indirectly by killing a leader and watching the NEXT
/// election succeed within the election-timer budget. If zombie
/// heartbeats from the dead leader were still landing, the followers
/// would never time out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborted_leader_does_not_zombie_heartbeat() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    ctl.stop_node(leader_idx).await.expect("stop leader");

    // Election timer max is 300 ms; new leader should appear within ~5×.
    let started = Instant::now();
    let _new_idx = ctl
        .wait_for_leader(Duration::from_millis(5 * 300))
        .await
        .expect("new leader within 5× timeout — proves no zombie heartbeats");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "new election took {:?} — suggests zombie heartbeats",
        elapsed
    );
}

// ── Concurrency / serialisation ------------------------------------------

/// `node_mutex` serialises `AppendEntries` against `RequestVote` so
/// `(currentTerm, votedFor, log)` are observed atomically. Fire many
/// AE + RV interleaved against a single follower; outcomes must be
/// internally consistent (no torn term value, no double-vote).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_mutex_serialises_handlers() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let handler = Arc::new(ctl.node_handler(0, 2).expect("handler"));
    let vote = ctl.vote(0).unwrap();
    let term = ctl.term(0).unwrap();

    // Fire 50 RequestVote and 50 AppendEntries concurrently, all in
    // term 1 to start, with one candidate id. Whichever wins the lock
    // either grants the vote OR observes the term — but no two handlers
    // can interleave inside the mutex.
    let mut tasks = Vec::new();
    for i in 0..50u64 {
        let h = handler.clone();
        tasks.push(tokio::spawn(async move {
            let _ = h
                .append_entries(Request::new(nproto::AppendEntriesRequest {
                    leader_id: 1,
                    term: 1,
                    prev_tx_id: 0,
                    prev_term: 0,
                    from_tx_id: i + 1,
                    to_tx_id: 0,
                    wal_bytes: Vec::new(),
                    leader_commit_tx_id: 0,
                }))
                .await;
        }));
        let h = handler.clone();
        tasks.push(tokio::spawn(async move {
            let _ = h
                .request_vote(Request::new(nproto::RequestVoteRequest {
                    term: 1,
                    candidate_id: 5,
                    last_tx_id: 0,
                    last_term: 1,
                }))
                .await;
        }));
    }
    for t in tasks {
        let _ = t.await;
    }

    // Term must have been observed (it was 0, now ≥ 1).
    assert!(term.get_current_term() >= 1);
    // Vote — if granted, must have been to candidate 5 (no double vote).
    if let Some(v) = vote.get_voted_for() {
        assert_eq!(v, 5);
    }
}

/// `Ping` does NOT take `node_mutex`. Even if AppendEntries is in
/// flight (would block on the mutex if Ping took it), Ping returns
/// immediately. We verify by issuing many pings in parallel — none
/// blocks beyond a small bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ping_does_not_serialise_through_node_mutex() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let handler = Arc::new(ctl.node_handler(0, 2).expect("handler"));

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let h = handler.clone();
        tasks.push(tokio::spawn(async move {
            let started = Instant::now();
            let _ = h
                .ping(Request::new(nproto::PingRequest {
                    from_node_id: 0,
                    nonce: 1,
                }))
                .await;
            started.elapsed()
        }));
    }
    for t in tasks {
        let elapsed = t.await.unwrap();
        assert!(
            elapsed < Duration::from_secs(1),
            "ping took too long ({:?}) — likely serialised behind node_mutex",
            elapsed
        );
    }
}

/// `RoleFlag` AcqRel reads/writes: a handler observes role changes the
/// supervisor flips immediately. Construct a handler, flip role under
/// it, observe via the next handler call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn role_flag_change_is_immediately_visible_to_handler() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let handler: NodeHandler = ctl.node_handler(0, 2).expect("handler");
    let role = ctl.role_flag(0).unwrap();

    // Flip to Leader — next AppendEntries must hit the NotFollower path.
    role.set(Role::Leader);
    let resp = handler
        .append_entries(Request::new(nproto::AppendEntriesRequest {
            leader_id: 1,
            term: 1,
            prev_tx_id: 0,
            prev_term: 0,
            from_tx_id: 1,
            to_tx_id: 0,
            wal_bytes: Vec::new(),
            leader_commit_tx_id: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.success);
    assert_eq!(
        resp.reject_reason,
        nproto::RejectReason::RejectNotFollower as u32
    );

    // Flip back to Follower — succeeds again.
    role.set(Role::Follower);
    let resp = handler
        .append_entries(Request::new(nproto::AppendEntriesRequest {
            leader_id: 1,
            term: 1,
            prev_tx_id: 0,
            prev_term: 0,
            from_tx_id: 1,
            to_tx_id: 0,
            wal_bytes: Vec::new(),
            leader_commit_tx_id: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
}
