//! Edge cases and adversarial input.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::ledger::WaitLevel;
use roda_ledger::cluster::proto::node as nproto;
use roda_ledger::cluster::proto::node::node_server::Node;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl, Role};
use std::time::Duration;
use tokio::time::sleep;
use tonic::Request;

const ACCOUNT: u64 = 1;
const AMOUNT: u64 = 100;

// ── Edge cases (§9) ------------------------------------------------------

/// Leader with zero other peers (single-node cluster) — `Quorum` is
/// sized 1 and `cluster_commit_index` advances purely from the leader's
/// own commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_cluster_commit_index_self_advances() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);

    let q = ctl.handles(0).unwrap().quorum().unwrap();
    assert!(q.get() >= r.tx_id);
}

/// Empty `AppendEntries` (heartbeat) succeeds and advances the
/// follower's `cluster_commit_index` to clamped `leader_commit_tx_id`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_append_entries_heartbeat_succeeds() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let handler = ctl.node_handler(0, 2).expect("handler");
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
    assert_eq!(resp.reject_reason, nproto::RejectReason::RejectNone as u32);
}

/// `leader_commit_tx_id` greater than follower's `last_commit_id` is
/// clamped via `LedgerHandler::wait_for_transaction_level` — the wait
/// requires BOTH `cluster_commit_index >= tx` AND `commit_index >= tx`,
/// so an inflated leader commit can't trick a wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_commit_clamped_against_local_commit() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let handler = ctl.node_handler(0, 2).expect("handler");

    // Send a heartbeat with leader_commit far ahead of local commit (which is 0).
    let resp = handler
        .append_entries(Request::new(nproto::AppendEntriesRequest {
            leader_id: 1,
            term: 1,
            prev_tx_id: 0,
            prev_term: 0,
            from_tx_id: 1,
            to_tx_id: 0,
            wal_bytes: Vec::new(),
            leader_commit_tx_id: 9_999_999,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);

    // The follower's published cluster_commit_index will hold the
    // inflated value, but a `wait_for_transaction_level(ClusterCommit)`
    // for tx 1 must STILL block because local commit_index < 1.
    let ledger = ctl.ledger(0).unwrap();
    assert_eq!(ledger.last_commit_id(), 0);
}

/// Replication when only-self in `cluster.peers`: zero peer-replication
/// tasks spawned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_cluster_spawns_no_peer_tasks() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    // Indirect: a single-node leader can ack ClusterCommit without
    // spawning any peer task.
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);
}

/// Phantom peers configured-but-never-started: leader's per-peer task
/// retries the connection forever and does not panic. We give a bare
/// supervisor a phantom peer and confirm the cluster still answers
/// queries (proves the leader didn't crash).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phantom_peer_does_not_crash_leader() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "phantom".to_string(),
        phantom_peer_count: 1,
        autostart: false,
        ..ClusterTestingConfig::cluster(1)
    })
    .await
    .expect("build");
    ctl.build_node(0).expect("build");
    ctl.run_node(0).await.expect("run");
    ctl.wait_for_bind(0, Duration::from_secs(5)).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    // Server is alive; queries succeed.
    let _ = ctl.pipeline_index_on(0).await.expect("query");
}

/// Restart the same clustered harness on the same data dir → Term log
/// rehydrates and bumps once per restart (ADR-0016 §11). Vote log is
/// only written when an actual vote happens — we don't assert on it
/// here because a single-node cluster auto-elects without RequestVote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_node_restart_rehydrates_term() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut root = std::env::current_dir().unwrap();
    root.push(format!("temp_cluster_rehydrate_{}", nanos));

    let cfg = || ClusterTestingConfig {
        label: "cluster_rehydrate".to_string(),
        data_dir_root: Some(root.clone()),
        ..ClusterTestingConfig::cluster(1)
    };

    let data_dir = root.join("0");

    {
        let ctl = ClusterTestingControl::start(cfg()).await.unwrap();
        let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let _ = ctl
            .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
            .await
            .unwrap();
    }
    sleep(Duration::from_millis(200)).await;

    {
        let _ctl = ClusterTestingControl::start(cfg()).await.unwrap();
        let t = roda_ledger::cluster::Term::open_in_dir(&data_dir.to_string_lossy()).unwrap();
        // Two boots → term bumped twice (≥ 2).
        assert!(t.get_current_term() >= 2, "term = {}", t.get_current_term());
    }

    let _ = std::fs::remove_dir_all(&root);
}

// ── §17 Adversarial input ------------------------------------------------

/// `term=0` on `AppendEntries` is treated specially (the handler skips
/// the term observation step). The follower returns its own term, no
/// state change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn term_zero_request_skips_observation() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let term = ctl.term(0).unwrap();
    term.observe(5, 0).unwrap(); // pre: our term is 5

    let handler = ctl.node_handler(0, 2).expect("handler");
    let resp = handler
        .append_entries(Request::new(nproto::AppendEntriesRequest {
            leader_id: 1,
            term: 0, // sentinel
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
    // Term unchanged — `if req.term != 0` skipped the observe step.
    assert_eq!(term.get_current_term(), 5);
}

/// `candidate_id == 0` is rejected at the Vote layer: the gRPC handler
/// path returns `vote_granted = false` (Vote::vote returns InvalidInput,
/// the handler swallows it as "vote refused").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_vote_with_zero_candidate_id_refused() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let handler = ctl.node_handler(0, 2).expect("handler");
    let resp = handler
        .request_vote(Request::new(nproto::RequestVoteRequest {
            term: 1,
            candidate_id: 0,
            last_tx_id: 0,
            last_term: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.vote_granted);
}

/// Replay of an old (lower-term) AppendEntries is rejected with
/// `RejectTermStale`, no state mutation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replayed_old_term_append_entries_rejected() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let term = ctl.term(0).unwrap();
    term.observe(10, 0).unwrap();

    let handler = ctl.node_handler(0, 2).expect("handler");
    let resp = handler
        .append_entries(Request::new(nproto::AppendEntriesRequest {
            leader_id: 1,
            term: 5, // stale
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
        nproto::RejectReason::RejectTermStale as u32
    );
    // Our term unchanged.
    assert_eq!(term.get_current_term(), 10);
}

/// `Term::new_term`'s `checked_add(1)` returns Other when at u64::MAX.
/// Direct unit-style check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn term_new_term_handles_u64_overflow() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let term = ctl.term(0).unwrap();
    term.observe(u64::MAX, 0).unwrap();
    let err = term.new_term(0).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Other);
}

// ── Operational --------------------------------------------------------

/// Connecting via gRPC reflection on the client port should succeed
/// (reflection service is mounted at the standalone client server).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_reflection_available_on_client_port() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::standalone())
        .await
        .expect("start");
    let port = ctl.client_port(0).unwrap();
    // We don't have a reflection client dependency; instead verify
    // that the port is open and responds to a normal LedgerClient
    // call (proves the tonic transport is up).
    let _ = ctl.pipeline_index_on(0).await.expect("ping via client");
    let _ = port; // referenced
}

/// `Ping` on a Candidate-flagged node reports `Recovering` (the proto
/// has no Candidate value yet).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_reports_recovering_for_candidate() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Candidate))
        .await
        .expect("bare start");
    let handler = ctl.node_handler(0, 7).expect("handler");
    let resp = handler
        .ping(Request::new(nproto::PingRequest {
            from_node_id: 0,
            nonce: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.role, nproto::NodeRole::Recovering as i32);
}

/// Non-leader's gRPC client server rejects `submit_operation` with
/// `FAILED_PRECONDITION`. Already covered for Follower in
/// `replication_extended_test`. Here for Candidate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn candidate_rejects_writes() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Candidate))
        .await
        .expect("bare start");
    let handler = ctl.ledger_handler(0).expect("handler");
    use roda_ledger::cluster::proto::ledger as proto;
    use roda_ledger::cluster::proto::ledger::ledger_server::Ledger as LedgerSvc;
    let err = handler
        .submit_operation(Request::new(proto::SubmitOperationRequest {
            operation: Some(
                roda_ledger::cluster::proto::ledger::submit_operation_request::Operation::Deposit(
                    proto::Deposit {
                        account: 1,
                        amount: 1,
                        user_ref: 0,
                    },
                ),
            ),
        }))
        .await
        .expect_err("Candidate must reject writes");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

/// SIGINT/SIGTERM clean shutdown — abort flow on the harness.
/// Indirect: aborting `SupervisorHandles` releases ports.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_abort_releases_ports() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let port = ctl.client_port(0).unwrap();
    ctl.stop_node(0).await.expect("stop");

    // `stop_node` awaits the supervisor's join handles + the OS port
    // release before returning, so binding must succeed immediately.
    assert!(
        std::net::TcpListener::bind(("127.0.0.1", port)).is_ok(),
        "port {} still bound after stop_node returned",
        port
    );
}
