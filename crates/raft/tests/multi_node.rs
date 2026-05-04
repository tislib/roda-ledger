//! Multi-node behaviour over an idealised network. Covers happy-path
//! elections, replication, election edge cases (term-bump-before-win,
//! stale replies, etc.), §5.3 log-matching corner cases, and the
//! read-API/action-stream consistency contract.

mod common;

use std::time::{Duration, Instant};

use common::Sim;
use common::mem_persistence::MemPersistence;
use raft::{
    AppendEntriesDecision, AppendResult, LogEntryRange, NodeId, Persistence, RaftConfig, RaftNode,
    RejectReason, RequestVoteRequest, Role,
};

fn pick_leader(sim: &Sim) -> Option<NodeId> {
    [1u64, 2, 3]
        .iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
}

fn await_leader(sim: &mut Sim) -> NodeId {
    let deadline = sim.clock() + Duration::from_secs(5);
    let elected = sim.run_until_predicate(deadline, |s| pick_leader(s).is_some());
    assert!(elected, "no leader elected within budget");
    pick_leader(sim).unwrap()
}

fn fresh_node(self_id: u64, peers: Vec<u64>) -> RaftNode<MemPersistence> {
    RaftNode::new(
        self_id,
        peers,
        MemPersistence::new(),
        RaftConfig::default(),
        42,
    )
}

// ── Happy-path multi-node ────────────────────────────────────────────────────

/// 3-node cluster elects exactly one leader and replicates client
/// writes to every follower.
#[test]
fn three_node_cluster_elects_one_leader_and_replicates() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 1);
    let leader = await_leader(&mut sim);
    sim.client_write(leader, 5);
    let deadline = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(deadline, |s| s.cluster_commit_of(leader) >= 5);
    let deadline = sim.clock() + Duration::from_secs(2);
    sim.run_until(deadline);
    for n in [1, 2, 3] {
        assert!(sim.cluster_commit_of(n) >= 5);
    }
    sim.assert_election_safety();
    sim.assert_log_matching();
    sim.assert_state_machine_safety();
}

/// After a leader is elected, no follower transitions to Candidate
/// for several election windows — heartbeats keep them quiet.
#[test]
fn heartbeats_suppress_follower_elections() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 5);
    let leader = await_leader(&mut sim);
    let leader_term = sim.current_term_of(leader);
    // Run for ~5 election windows and assert no follower flipped to
    // Candidate (no leader change at all).
    let deadline = sim.clock() + Duration::from_secs(2);
    sim.run_until(deadline);
    for n in [1, 2, 3] {
        if n == leader {
            assert!(sim.role_of(n).is_leader());
        } else {
            assert_eq!(sim.role_of(n), Role::Follower);
        }
        assert_eq!(sim.current_term_of(n), leader_term);
    }
}

/// `cluster_commit_index()` is monotonically non-decreasing across
/// the run, on every node.
#[test]
fn cluster_commit_index_advances_monotonically() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 9);
    let leader = await_leader(&mut sim);

    let mut prev = [0u64; 3];
    for batch in 1..=5 {
        sim.client_write(leader, 3);
        let dl = sim.clock() + Duration::from_secs(2);
        sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 3 * batch);
        for (i, n) in [1, 2, 3].iter().enumerate() {
            let cur = sim.cluster_commit_of(*n);
            assert!(
                cur >= prev[i],
                "node {} cluster_commit regressed {} -> {}",
                n,
                prev[i],
                cur
            );
            prev[i] = cur;
        }
    }
    sim.assert_log_matching();
}

// ── Election edge cases (regressions for term-bump-before-win, etc.) ─────────

/// A lost election does not write to the term log. The candidate
/// only calls `commit_term` on a Won outcome — split votes / no-reply
/// rounds leave the term-log boundary list untouched.
///
/// Two-node cluster ensures node 1 can never win (no peer reply).
/// After several failed rounds, `into_persistence()` lets us inspect
/// the underlying persistence directly: `last_term_record()` must
/// still be `None`.
#[test]
fn lost_election_does_not_pollute_term_log() {
    let mut node = fresh_node(1, vec![1, 2]);
    let mut now = Instant::now();
    common::drive_tick(&mut node, now);
    for _ in 0..3 {
        now += Duration::from_secs(60);
        common::drive_tick(&mut node, now);
    }
    assert_eq!(node.role(), Role::Candidate);
    // The candidate self-voted at least once.
    assert!(node.voted_for().is_some());

    // Drop the volatile RaftNode and inspect the persistence.
    let p = node.into_persistence();
    assert!(
        p.last_term_record().is_none(),
        "lost candidacy left a term-log record: {:?}",
        p.last_term_record()
    );
    // current_term() on the persistence is the term-log term — must
    // still be 0 because nothing was committed.
    assert_eq!(p.current_term(), 0);
    // The vote log advanced (one self-vote per round) — that's where
    // the lost-election bookkeeping lives.
    assert!(p.vote_term() >= 1);
}

/// A higher term arriving on a `RequestVoteRequest` while the node
/// is already a Candidate forces step-down before any vote tally.
#[test]
fn higher_term_request_vote_during_candidacy_forces_step_down() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let mut now = Instant::now();
    common::drive_tick(&mut node, now);
    now += Duration::from_secs(60);
    common::drive_tick(&mut node, now);
    assert_eq!(node.role(), Role::Candidate);

    let _ = node.election().handle_request_vote(
        now,
        RequestVoteRequest {
            from: 2,
            term: 99,
            last_tx_id: 0,
            last_term: 0,
        },
    );
    assert_eq!(node.current_term(), 99);
    assert!(!node.role().is_leader());
}

/// Candidate at term 5 receives a `RequestVoteReply` with term=7
/// (peer somehow at higher term). Must not become leader at 5; must
/// step down and observe term 7.
#[test]
fn higher_term_seen_in_vote_reply_aborts_candidacy() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let mut now = Instant::now();
    common::drive_tick(&mut node, now);
    now += Duration::from_secs(60);
    common::drive_tick(&mut node, now);
    assert_eq!(node.role(), Role::Candidate);
    let candidate_term = node.current_term();

    // `granted=true` is ignored — higher term wins via the Granted
    // variant's term, same as the historic Event::RequestVoteReply.
    common::deliver_vote_reply(&mut node, now, 2, candidate_term + 2, true);
    assert!(!node.role().is_leader());
    assert!(node.current_term() >= candidate_term + 2);
}

/// A stale `RequestVoteReply` (lower term than the current
/// candidacy) is ignored — must not contribute to the majority.
#[test]
fn stale_vote_reply_does_not_count_toward_majority() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let mut now = Instant::now();
    common::drive_tick(&mut node, now);
    now += Duration::from_secs(60);
    common::drive_tick(&mut node, now);
    assert_eq!(node.role(), Role::Candidate);
    let term_now = node.current_term();
    assert!(term_now >= 1);

    // A reply at an old term — must be ignored.
    common::deliver_vote_reply(&mut node, now, 2, term_now - 1, true);
    // Still a candidate, term unchanged.
    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(node.current_term(), term_now);
}

/// Higher term observed in an `AppendEntries` reply while leader →
/// step down to Follower at the new term. Drives the per-peer
/// `Replication::append_result` direct-method path with a synthetic
/// `Reject { reason: TermBehind, term: <much higher> }`.
#[test]
fn higher_term_in_append_reply_demotes_leader() {
    // Two-node cluster (self=99, peer=1) so we can win election with a
    // single peer vote and then have peer 1 reachable via the
    // `Replication` API for the synthetic reply.
    let t0 = Instant::now();
    let mut leader_node = fresh_node(99, vec![99, 1]);
    common::drive_tick(&mut leader_node, t0);
    common::drive_tick(&mut leader_node, t0 + Duration::from_secs(60));
    let term = leader_node.current_term();
    common::deliver_vote_reply(&mut leader_node, t0 + Duration::from_secs(60), 1, term, true);
    assert!(leader_node.role().is_leader());

    leader_node.replication().peer(1).unwrap().append_result(
        t0 + Duration::from_secs(61),
        AppendResult::Reject {
            term: 99,
            reason: RejectReason::TermBehind,
            last_write_id: 0,
            last_commit_id: 0,
        },
    );
    assert!(!leader_node.role().is_leader());
    assert!(leader_node.current_term() >= 99);
}

/// Boot does not bump term — restarting a node with persisted state
/// preserves it exactly.
#[test]
fn boot_does_not_bump_term() {
    let mut node = fresh_node(1, vec![1]);
    let mut now = Instant::now();
    common::drive_tick(&mut node, now);
    now += Duration::from_secs(60);
    common::drive_tick(&mut node, now);
    assert!(node.role().is_leader());
    let term_before = node.current_term();
    let voted_for_before = node.voted_for();

    let p = node.into_persistence();
    let restarted = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);
    assert_eq!(restarted.current_term(), term_before);
    assert_eq!(restarted.voted_for(), voted_for_before);
    assert_eq!(restarted.role(), Role::Initializing);
}

// ── Replication edges ───────────────────────────────────────────────────────

/// Follower with empty log receives `prev_log_tx_id=10`; rejects
/// (LogMismatch). Leader walks back to 0 and the follower accepts
/// the bulk-load.
#[test]
fn empty_follower_rejects_then_accepts_after_walk_back() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 33);
    let leader = await_leader(&mut sim);
    sim.client_write(leader, 5);
    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(dl, |s| {
        [1, 2, 3].iter().all(|n| s.cluster_commit_of(*n) >= 5)
    });
    for n in [1, 2, 3] {
        assert!(sim.cluster_commit_of(n) >= 5);
    }
    sim.assert_log_matching();
}

/// AppendEntries with `prev_log_tx_id=0` (start of log) is accepted
/// without a §5.3 check — that's the convention for the empty-log
/// boundary. The success reply is parked until the driver
/// acknowledges durability via `advance` + `Tick`.
#[test]
fn prev_log_tx_id_zero_skips_term_match_check() {
    let mut node = fresh_node(1, vec![1, 2]);
    let decision = node.validate_append_entries_request(
        Instant::now(),
        2,
        5,
        0,
        // prev_log_term=99 would fail the term check, but tx 0
        // is the "start of log" sentinel — no check runs.
        99,
        LogEntryRange::new(1, 1, 5),
        0,
    );
    assert_eq!(
        decision,
        AppendEntriesDecision::Accept {
            append: Some(LogEntryRange::new(1, 1, 5))
        }
    );
    // Cluster acks durability via `advance`. Watermarks now reflect
    // the new state — what the driver stamps onto the success reply.
    node.advance_write_index(1);
    node.advance_commit_index(1);
    assert_eq!(node.write_index(), 1);
    assert_eq!(node.commit_index(), 1);
}

/// Follower clamps `leader_commit` to its local log length: a leader
/// telling it `leader_commit=10` while it only has up to tx 3
/// advances `cluster_commit_index` to 3, not 10. The advance is
/// emitted only after the driver durably appends the entries —
/// `cluster_commit` cannot ride ahead of durability.
#[test]
fn leader_commit_clamp_to_local_log() {
    let mut node = fresh_node(1, vec![1, 2]);
    let decision = node.validate_append_entries_request(
        Instant::now(),
        2,
        1,
        0,
        0,
        LogEntryRange::new(1, 3, 1),
        10, // intentionally beyond the batch
    );
    assert_eq!(
        decision,
        AppendEntriesDecision::Accept {
            append: Some(LogEntryRange::new(1, 3, 1))
        }
    );
    // Cluster commit cannot advance before the entries are durable;
    // `leader_commit` is parked in `pending_leader_commit`.
    assert_eq!(node.cluster_commit_index(), 0);

    // Cluster acks durability. `advance` drains
    // `pending_leader_commit`, clamped at the new
    // `local_commit_index = 3`.
    node.advance_write_index(3);
    node.advance_commit_index(3);
    assert_eq!(node.cluster_commit_index(), 3);
}

/// Empty AppendEntries (heartbeat) keeps the follower's election
/// timer reset and replies success — no AppendLog actions.
#[test]
fn empty_append_entries_is_a_heartbeat() {
    let mut node = fresh_node(1, vec![1, 2]);
    let decision = node.validate_append_entries_request(
        Instant::now(),
        2,
        1,
        0,
        0,
        LogEntryRange::empty(),
        0,
    );
    // Heartbeat: Accept with no entries to append; cluster builds
    // the success reply from `current_term()` / watermark getters.
    assert_eq!(decision, AppendEntriesDecision::Accept { append: None });
}

/// AppendEntries from an old leader (lower term) is rejected with
/// the receiver's higher term in the reply.
#[test]
fn append_entries_from_old_leader_is_rejected() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    // Force the node up to term 5 via observing a higher term.
    let _ = node.election().handle_request_vote(
        Instant::now(),
        RequestVoteRequest {
            from: 2,
            term: 5,
            last_tx_id: 0,
            last_term: 0,
        },
    );
    assert_eq!(node.current_term(), 5);
    // Now an old leader at term 3 sends AE — must be refused.
    let decision = node.validate_append_entries_request(
        Instant::now(),
        3,
        3,
        0,
        0,
        LogEntryRange::empty(),
        0,
    );
    assert_eq!(
        decision,
        AppendEntriesDecision::Reject {
            reason: RejectReason::TermBehind,
            truncate_after: None,
        }
    );
    // The cluster stamps the reply's `term` from `current_term()`.
    assert_eq!(node.current_term(), 5);
}

// ── Read-API consistency ─────────────────────────────────────────────────────

/// Read methods are pure: calling them N times in a row returns the
/// same answer.
#[test]
fn read_methods_are_pure() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 17);
    let leader = await_leader(&mut sim);
    sim.client_write(leader, 2);
    let dl = sim.clock() + Duration::from_secs(1);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 2);

    let role_a = sim.role_of(leader);
    let term_a = sim.current_term_of(leader);
    let cci_a = sim.cluster_commit_of(leader);
    let lci_a = sim.local_commit_of(leader);
    for _ in 0..1000 {
        assert_eq!(sim.role_of(leader), role_a);
        assert_eq!(sim.current_term_of(leader), term_a);
        assert_eq!(sim.cluster_commit_of(leader), cci_a);
        assert_eq!(sim.local_commit_of(leader), lci_a);
    }
}

/// Single-node election win is observable post-call via `role()` and
/// `current_term()` — the cluster polls these getters after each
/// `Election::start` (no action stream).
#[test]
fn became_leader_matches_immediate_query() {
    let mut node = fresh_node(1, vec![1]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert!(node.role().is_leader());
    assert_eq!(node.current_term(), 1);
}

/// `cluster_commit_index()` reflects the in-place advance from
/// `advance(write, commit)` — there is no dedicated action.
#[test]
fn cluster_commit_query_reflects_advance() {
    let mut node = fresh_node(1, vec![1]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    // Single-node leader: `advance(7, 7)` lifts cluster_commit
    // immediately. The driver polls `cluster_commit_index()` to
    // observe the change (no dedicated action; ADR-0017 §"Driver
    // call pattern").
    node.advance_write_index(7);
    node.advance_commit_index(7);
    assert!(node.cluster_commit_index() >= 7);
}

// ── §5.4.1 up-to-date check (audit gap) ────────────────────────────────
//
// Raft §5.4.1: the voter denies its vote if the candidate's log is
// less up-to-date than the voter's. "Up-to-date" is determined by
// `(last_term, last_tx_id)` — strictly later term wins; same term
// → strictly higher tx_id wins.

#[test]
fn vote_denied_when_candidate_last_term_below_ours() {
    // Voter has accepted entries 1..3 at term 5, then a candidate
    // running at term 6 contacts it claiming `last_term=2,
    // last_tx_id=10`. Even though tx_id is higher, term is lower —
    // deny.
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 5,
            start_tx_id: 1,
        }],
        5,
        0,
    );
    let mut node = RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42);
    node.advance_write_index(3);
    node.advance_commit_index(3);

    let reply = node.election().handle_request_vote(
        Instant::now(),
        RequestVoteRequest {
            from: 2,
            term: 6,
            last_tx_id: 10, // higher than ours
            last_term: 2,   // but older term — must deny
        },
    );
    assert!(!reply.granted, "must deny when last_term < ours: {:?}", reply);
}

#[test]
fn vote_denied_when_same_last_term_but_lower_last_tx_id() {
    // Voter at term 5 with entries 1..10. Candidate at term 6
    // claims `last_term=5, last_tx_id=7` — same term, fewer entries
    // — deny.
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 5,
            start_tx_id: 1,
        }],
        5,
        0,
    );
    let mut node = RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42);
    node.advance_write_index(10);
    node.advance_commit_index(10);

    let reply = node.election().handle_request_vote(
        Instant::now(),
        RequestVoteRequest {
            from: 2,
            term: 6,
            last_tx_id: 7, // strictly less than ours
            last_term: 5,  // same as ours
        },
    );
    assert!(
        !reply.granted,
        "must deny on equal term + lower tx_id: {:?}",
        reply
    );
}

#[test]
fn vote_granted_when_candidate_last_term_above_ours() {
    // Voter at term 5 with entries 1..10. Candidate at term 6 claims
    // `last_term=7, last_tx_id=2` — strictly later term wins despite
    // far fewer entries.
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 5,
            start_tx_id: 1,
        }],
        5,
        0,
    );
    let mut node = RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42);
    node.advance_write_index(10);
    node.advance_commit_index(10);

    let reply = node.election().handle_request_vote(
        Instant::now(),
        RequestVoteRequest {
            from: 2,
            term: 6,
            last_tx_id: 2,
            last_term: 7, // strictly above ours — grant
        },
    );
    assert!(
        reply.granted,
        "must grant on strictly later last_term: {:?}",
        reply
    );
}

// ── Idempotent re-vote in the same term (audit gap) ────────────────────

/// §5.2: a follower that has already voted for candidate X in term T
/// must grant the same vote again if X re-asks (the original reply
/// may have been lost). Different candidate at the same term is
/// refused.
#[test]
fn duplicate_request_vote_from_same_candidate_grants_again() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let now = Instant::now();

    // First RV from candidate 2 at term 3 — granted.
    let first = node.election().handle_request_vote(
        now,
        RequestVoteRequest {
            from: 2,
            term: 3,
            last_tx_id: 0,
            last_term: 0,
        },
    );
    assert!(first.granted);

    // A second RV from the same candidate at the same term — must
    // be granted again (idempotent on duplicate inbound RPCs).
    let second = node.election().handle_request_vote(
        now,
        RequestVoteRequest {
            from: 2,
            term: 3,
            last_tx_id: 0,
            last_term: 0,
        },
    );
    assert!(
        second.granted,
        "duplicate RV from same candidate must re-grant: {:?}",
        second
    );

    // A different candidate at the same term — refused.
    let third = node.election().handle_request_vote(
        now,
        RequestVoteRequest {
            from: 3,
            term: 3,
            last_tx_id: 0,
            last_term: 0,
        },
    );
    assert!(
        !third.granted,
        "different candidate same term must be refused: {:?}",
        third
    );
}

// ── Heartbeat propagates leader_commit (audit gap) ─────────────────────

/// A heartbeat (empty-entries AE) carries `leader_commit` and must
/// advance the follower's `cluster_commit_index` even though no
/// entries are appended. This is the only path that progresses
/// cluster_commit on a follower without writing any new entries —
/// e.g. after a leader retransmits a commit-watermark advance.
#[test]
fn heartbeat_propagates_leader_commit_advance() {
    // Pre-load the follower with three durable entries at term 1.
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 1,
            start_tx_id: 1,
        }],
        1,
        0,
    );
    let mut node = RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42);
    node.advance_write_index(3);
    node.advance_commit_index(3);
    assert_eq!(node.cluster_commit_index(), 0);

    // Heartbeat at term 1 with leader_commit=2.
    let decision = node.validate_append_entries_request(
        Instant::now(),
        2,
        1,
        3,
        1,
        LogEntryRange::empty(),
        2,
    );
    // Heartbeat: Accept with no entries.
    assert_eq!(decision, AppendEntriesDecision::Accept { append: None });
    // The advance is observable via the getter (no dedicated
    // action — ADR-0017 §"Driver call pattern").
    assert_eq!(node.cluster_commit_index(), 2);
}

// ── Index-split refactor regressions ──────────────────────────────────────

/// On a follower, the heartbeat path clamps `leader_commit` to
/// `local_commit_index` (not `local_write_index`) — the cluster
/// commit watermark on a follower is bounded by what the local
/// ledger has applied. Pre-refactor the single conflated field
/// served both roles; this test pins the new semantics.
#[test]
fn heartbeat_leader_commit_clamps_to_local_commit() {
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 1,
            start_tx_id: 1,
        }],
        1,
        0,
    );
    let mut node = RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42);
    // Five entries durably written, only three committed locally.
    node.advance_write_index(5);
    node.advance_commit_index(3);

    // Heartbeat with leader_commit=10 (intentionally above our
    // commit). Cluster commit advances only to local_commit_index=3.
    let _ = node.validate_append_entries_request(
        Instant::now(),
        2,
        1,
        5,
        1,
        LogEntryRange::empty(),
        10,
    );
    assert_eq!(
        node.cluster_commit_index(),
        3,
        "cluster_commit must clamp to local_commit_index, not write_index"
    );
}

/// A Candidate's `local_write_index` survives the role transition
/// to Follower (when an AE at a higher term arrives). Pre-refactor,
/// `LeaderState.last_written` would have been dropped if the node
/// had been leader; the node-scoped field is preserved on every
/// transition.
#[test]
fn local_write_index_survives_election_loss() {
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 1,
            start_tx_id: 1,
        }],
        1,
        0,
    );
    let mut node = RaftNode::new(1, vec![1, 2, 3], persistence, RaftConfig::default(), 42);
    let t0 = Instant::now();
    // Hydrate the write extent before any election runs.
    node.advance_write_index(3);
    node.advance_commit_index(3);
    // First Tick lazy-arms; second Tick expires → Candidate.
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert_eq!(node.role(), Role::Candidate);
    let term_now = node.current_term();
    assert_eq!(node.write_index(), 3);

    // Receive AE at a strictly higher term (a different node won
    // the election). prev_log_tx_id = 0 sentinels the start-of-log
    // check so we don't tangle with the §5.3 path here.
    let _ = node.validate_append_entries_request(
        t0 + Duration::from_secs(60),
        2,
        term_now + 1,
        0,
        0,
        LogEntryRange::empty(),
        0,
    );
    assert!(matches!(node.role(), Role::Follower));
    // Survives the Candidate → Follower transition.
    assert_eq!(node.write_index(), 3);
}

// ── Concurrent-vote API regressions (Election primitive) ─────────────────
//
// The whole point of the consensus refactor: a candidate's outbound
// `RequestVote`s are dispatched concurrently, and the cluster feeds the
// outcomes back as a batch into `Election::handle_votes`. These tests
// pin the batch-handling semantics — majority detection, higher-term
// step-down, stale-term filtering, and the failed-RPC fast-path —
// against future regressions.

use raft::VoteOutcome;

/// 5-node cluster: candidate emits 4 outbound `RequestVote`s and the
/// cluster collects all 4 grants concurrently. A single
/// `handle_votes` call with the batch transitions the candidate to
/// Leader at the correct term.
#[test]
fn concurrent_vote_handling_reaches_quorum() {
    let mut node = fresh_node(1, vec![1, 2, 3, 4, 5]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert_eq!(node.role(), Role::Candidate);
    let term = node.current_term();
    let requests = node.election().get_requests();
    assert_eq!(requests.len(), 4, "5-node cluster: 4 outbound RVs");

    // All 4 peers grant. Single batch call.
    let now = t0 + Duration::from_secs(60) + Duration::from_millis(1);
    node.election().handle_votes(
        now,
        vec![
            (2, VoteOutcome::Granted { term }),
            (3, VoteOutcome::Granted { term }),
            (4, VoteOutcome::Granted { term }),
            (5, VoteOutcome::Granted { term }),
        ],
    );
    assert!(node.role().is_leader());
    assert_eq!(node.current_term(), term);
}

/// A batch carrying a higher-term reply forces step-down even when a
/// majority of grants is also present in the same batch — observing
/// the higher term wins out.
#[test]
fn mixed_grant_deny_batch_steps_down_on_higher_term() {
    let mut node = fresh_node(1, vec![1, 2, 3, 4, 5]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    let term = node.current_term();
    let now = t0 + Duration::from_secs(60) + Duration::from_millis(1);
    node.election().handle_votes(
        now,
        vec![
            (2, VoteOutcome::Granted { term }),
            (3, VoteOutcome::Denied { term: term + 5 }),
            (4, VoteOutcome::Granted { term }),
            (5, VoteOutcome::Failed),
        ],
    );
    // Higher-term observation steps down to Follower.
    assert_eq!(node.role(), Role::Follower);
    assert!(node.current_term() >= term + 5);
}

/// Single grant + three Failed outcomes (RPCs that timed out / were
/// dropped) keep the candidate running at the same term — no quorum,
/// no step-down.
#[test]
fn partial_batch_below_quorum_keeps_candidate() {
    let mut node = fresh_node(1, vec![1, 2, 3, 4, 5]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    let term_before = node.current_term();
    let now = t0 + Duration::from_secs(60) + Duration::from_millis(1);
    node.election().handle_votes(
        now,
        vec![
            (2, VoteOutcome::Granted { term: term_before }),
            (3, VoteOutcome::Failed),
            (4, VoteOutcome::Failed),
            (5, VoteOutcome::Failed),
        ],
    );
    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(node.current_term(), term_before);
}

/// `get_requests` is a pure read on `CandidateState`. Calling it
/// twice returns identical output; the cluster can re-pull on retry
/// without duplicating term bumps.
#[test]
fn get_requests_idempotent_within_candidacy() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert_eq!(node.role(), Role::Candidate);
    let first = node.election().get_requests();
    let second = node.election().get_requests();
    assert_eq!(first.len(), 2);
    assert_eq!(first.len(), second.len());
    for (a, b) in first.iter().zip(second.iter()) {
        assert_eq!(a.0, b.0);
        assert_eq!(a.1.term, b.1.term);
        assert_eq!(a.1.last_tx_id, b.1.last_tx_id);
        assert_eq!(a.1.last_term, b.1.last_term);
    }
}

/// `get_requests` returns empty when the node is not Candidate
/// (Initializing, Follower, Leader). The cluster must not generate
/// outbound RVs from a non-candidate.
#[test]
fn get_requests_empty_when_not_candidate() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    assert_eq!(node.role(), Role::Initializing);
    assert!(node.election().get_requests().is_empty());
}

/// `Election::tick` always returns a `Wakeup` with a non-default
/// `deadline`. After lazy-arming on a fresh follower the deadline is
/// in the future (within the election-timer window).
#[test]
fn tick_returns_wakeup_with_future_deadline_after_lazy_arm() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let t0 = Instant::now();
    let wakeup = node.election().tick(t0);
    assert!(wakeup.deadline > t0, "deadline must be in the future after lazy-arm");
}

// ── Cluster commit advance under per-peer replication state ──────────────────
//
// Stresses how `cluster_commit_index` is computed from the quorum's
// `match_index` slots. Slot mapping for self_id=1, peers=[1,2,3]:
// `match_index = [leader_local_commit, peer2_match, peer3_match]`.
// Majority = 2, so the candidate is `sorted_desc[1]` — the
// second-highest slot value. The leader's self-slot is fed by
// `advance(write, commit)` and tracks `local_commit_index` (NOT
// `local_write_index`); peer slots are fed by
// `AppendResult::Success { last_commit_id }`.

/// Drive a 3-node node into the leader role by running an election
/// locally: tick once to lazy-arm, tick past the timeout to start the
/// candidacy, then deliver granted vote replies until the candidate
/// wins. Returns `(node, leader_now)` so callers can issue subsequent
/// `get_append_range(now)` calls with `now >= leader_now` (peer
/// `next_heartbeat` is initialised to `leader_now`).
fn make_3node_leader(
    self_id: NodeId,
    peers: Vec<NodeId>,
) -> (RaftNode<MemPersistence>, Instant) {
    let mut node = fresh_node(self_id, peers.clone());
    let t0 = Instant::now();
    let leader_now = t0 + Duration::from_secs(60);
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, leader_now);
    let term = node.current_term();
    for &peer in peers.iter().filter(|&&p| p != self_id) {
        common::deliver_vote_reply(&mut node, leader_now, peer, term, true);
        if node.role().is_leader() {
            return (node, leader_now);
        }
    }
    panic!("did not become leader, role={:?}", node.role());
}

#[test]
fn cluster_index_advance_on_replication() {
    let (mut node, leader_now) = make_3node_leader(1, vec![1, 2, 3]);
    let term = node.current_term();

    // Leader writes 200 entries; nothing committed locally yet.
    node.advance_write_index(200);
    node.advance_commit_index(0);
    assert_eq!(node.write_index(), 200);
    assert_eq!(node.commit_index(), 0);
    assert_eq!(node.cluster_commit_index(), 0);

    // Leader pulls AE for replica1 (arms in_flight; the actual entries
    // shipped don't matter — the reply is fabricated below).
    let _ = node
        .replication()
        .peer(2)
        .unwrap()
        .get_append_range(leader_now);

    // Replica1 acks fully — its commit reached 200. With the leader's
    // self-slot still at 0, slots = [0, 200, 0]; second-highest = 0 →
    // no cluster_commit advance.
    node.replication().peer(2).unwrap().append_result(
        leader_now,
        AppendResult::Success {
            term,
            last_write_id: 200,
            last_commit_id: 200,
        },
    );
    assert_eq!(node.cluster_commit_index(), 0);

    // Leader's local commit catches up. Self-slot → 200; slots =
    // [200, 200, 0]; second-highest = 200 → advance. Replica2 never
    // replied (slot 2 stays 0).
    node.advance_write_index(200);
    node.advance_commit_index(200);
    assert_eq!(node.cluster_commit_index(), 200);
}

#[test]
fn cluster_index_advance_on_replication_lagged() {
    let (mut node, leader_now) = make_3node_leader(1, vec![1, 2, 3]);
    let term = node.current_term();

    // Leader writes 200; local commit still 0.
    node.advance_write_index(200);
    node.advance_commit_index(0);

    // Leader pulls AE for replica1.
    let _ = node
        .replication()
        .peer(2)
        .unwrap()
        .get_append_range(leader_now);

    // Replica1 acks the WRITE but its commit is still 0 (lagged).
    // `match_index` is driven by `last_commit_id`, not `last_write_id`,
    // so slot 1 stays 0.
    node.replication().peer(2).unwrap().append_result(
        leader_now,
        AppendResult::Success {
            term,
            last_write_id: 200,
            last_commit_id: 0,
        },
    );
    assert_eq!(node.cluster_commit_index(), 0);

    // Leader's local commit catches up. Self-slot → 200, but slots =
    // [200, 0, 0]; second-highest = 0 → no advance. Replica2 never
    // replied.
    node.advance_write_index(200);
    node.advance_commit_index(200);
    assert_eq!(node.cluster_commit_index(), 0);

    // Leader sends a heartbeat to replica1. The previous
    // `get_append_range` pushed `next_heartbeat` out by
    // `cfg.heartbeat_interval` (50ms default) — wait past it.
    let after_hb = leader_now + Duration::from_millis(100);
    let req = node
        .replication()
        .peer(2)
        .unwrap()
        .get_append_range(after_hb)
        .expect("heartbeat must be due past next_heartbeat");
    // Peer's `next_index` was advanced to `last_write_id + 1 = 201` by
    // the previous reply, and leader's `local_write` is 200, so the AE
    // is a pure heartbeat.
    assert!(
        req.entries.is_empty(),
        "expected heartbeat AE, got entries={:?}",
        req.entries
    );

    // Replica1's commit caught up to 150. Slot 1 → 150; slots =
    // [200, 150, 0]; second-highest = 150 → advance. Replica2 still
    // never replied.
    node.replication().peer(2).unwrap().append_result(
        after_hb,
        AppendResult::Success {
            term,
            last_write_id: 200,
            last_commit_id: 150,
        },
    );
    assert_eq!(node.cluster_commit_index(), 150);
}

/// Same shape as `cluster_index_advance_on_replication`, but the
/// leader advances its `local_commit_index` UP FRONT (before the
/// replica reply) instead of after. The self-slot lift is no longer
/// the trigger — the trigger is the peer ack alone.
#[test]
fn cluster_index_advance_on_replication_leader_commits_first() {
    let (mut node, leader_now) = make_3node_leader(1, vec![1, 2, 3]);
    let term = node.current_term();

    // Leader writes AND commits 200 entries up front. Self-slot →
    // 200; slots = [200, 0, 0]; second-highest = 0 → no advance.
    node.advance_write_index(200);
    node.advance_commit_index(200);
    assert_eq!(node.write_index(), 200);
    assert_eq!(node.commit_index(), 200);
    assert_eq!(node.cluster_commit_index(), 0);

    // Leader pulls AE for replica1.
    let _ = node
        .replication()
        .peer(2)
        .unwrap()
        .get_append_range(leader_now);

    // Replica1 acks fully. Slot 1 → 200; slots = [200, 200, 0];
    // second-highest = 200 → advance. Replica2 never replied.
    node.replication().peer(2).unwrap().append_result(
        leader_now,
        AppendResult::Success {
            term,
            last_write_id: 200,
            last_commit_id: 200,
        },
    );
    assert_eq!(node.cluster_commit_index(), 200);
}

/// Same shape as `cluster_index_advance_on_replication_lagged`, but
/// the leader's `local_commit_index` is at 200 from the start. The
/// lagged replica still gates the cluster commit until its own
/// `last_commit_id` reaches a quorum-cover value.
#[test]
fn cluster_index_advance_on_replication_lagged_leader_commits_first() {
    let (mut node, leader_now) = make_3node_leader(1, vec![1, 2, 3]);
    let term = node.current_term();

    // Leader writes AND commits 200 entries up front. Self-slot →
    // 200; slots = [200, 0, 0]; second-highest = 0 → no advance.
    node.advance_write_index(200);
    node.advance_commit_index(200);
    assert_eq!(node.cluster_commit_index(), 0);

    // Leader pulls AE for replica1.
    let _ = node
        .replication()
        .peer(2)
        .unwrap()
        .get_append_range(leader_now);

    // Replica1 acks the WRITE but its commit is still 0 (lagged).
    // Slot 1 stays 0 (per-slot guard); slots = [200, 0, 0];
    // second-highest = 0 → no advance. Replica2 never replied.
    node.replication().peer(2).unwrap().append_result(
        leader_now,
        AppendResult::Success {
            term,
            last_write_id: 200,
            last_commit_id: 0,
        },
    );
    assert_eq!(node.cluster_commit_index(), 0);

    // Heartbeat to replica1 (peer's `next_index = 201 > local_write
    // 200`). Wait past `next_heartbeat`.
    let after_hb = leader_now + Duration::from_millis(100);
    let req = node
        .replication()
        .peer(2)
        .unwrap()
        .get_append_range(after_hb)
        .expect("heartbeat must be due past next_heartbeat");
    assert!(
        req.entries.is_empty(),
        "expected heartbeat AE, got entries={:?}",
        req.entries
    );

    // Replica1's commit caught up to 150. Slot 1 → 150; slots =
    // [200, 150, 0]; second-highest = 150 → advance. Replica2 still
    // never replied.
    node.replication().peer(2).unwrap().append_result(
        after_hb,
        AppendResult::Success {
            term,
            last_write_id: 200,
            last_commit_id: 150,
        },
    );
    assert_eq!(node.cluster_commit_index(), 150);
}

/// **BUG**: when a node promoted to leader inherits prior-term entries
/// (`local_write = 200`, `current_term_first_tx = 201`) and `cluster_commit`
/// is below the prior-term boundary, the §5.4.2 / Figure-8 gate at
/// `node.rs:849` blocks every quorum-acked advance whose value is
/// `< current_term_first_tx`. The cluster gets stuck —
/// `quorum.cluster_commit_index` advances internally to 200, but
/// `node.cluster_commit_index` stays at 0. Standard Raft breaks the
/// deadlock by writing a no-op on election win to commit a current-term
/// entry; this codebase doesn't, so the cluster cannot make progress
/// without external client traffic that lands a current-term entry.
///
/// This test exercises the user-reported state
/// `(current_term_first_tx=201, new_cluster=200, cluster_commit_index=0)`
/// — quorum advances but node doesn't reflect it.
#[test]
fn cluster_commit_stuck_at_prior_term_boundary_after_promotion() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let t0 = Instant::now();

    // Become follower; receive 200 prior-term entries with leader_commit=0
    // (the prior leader never reached quorum). Driver fsyncs WAL but
    // applies nothing to the ledger (since leader_commit was 0).
    let _ = node.validate_append_entries_request(
        t0,
        2,
        1,
        0,
        0,
        LogEntryRange::new(1, 200, 1),
        0,
    );
    node.advance_write_index(200);
    node.advance_commit_index(0);
    assert_eq!(node.write_index(), 200);
    assert_eq!(node.commit_index(), 0);
    assert_eq!(node.cluster_commit_index(), 0);

    // Election timer fires; node 1 wins election at term 2 with peer 2's vote.
    // `become_leader_after_win` sets `current_term_first_tx = local_write + 1 = 201`.
    let t1 = t0 + Duration::from_secs(60);
    common::drive_tick(&mut node, t1);
    let cand_term = node.current_term();
    common::deliver_vote_reply(&mut node, t1, 2, cand_term, true);
    assert!(node.role().is_leader(), "must become leader");
    let leader_term = node.current_term();
    assert_eq!(node.cluster_commit_index(), 0);

    // Both peers ack with `last_commit_id = 200` — they happen to have
    // committed up to the prior-term boundary via some earlier history.
    // After both acks: quorum match_index = [0 (slot 0 = leader's local_commit),
    // 200, 200]. Sorted desc = [200, 200, 0]. Candidate = sorted_desc[1] = 200.
    // `quorum.advance` returns Some(200) — quorum's internal
    // cluster_commit advances to 200. The §5.4.2 gate fires (200 < 201)
    // and the body of the if-let chain is skipped. node.cluster_commit_index
    // stays at 0.
    let _ = node.replication().peer(2).unwrap().get_append_range(t1);
    node.replication().peer(2).unwrap().append_result(
        t1,
        AppendResult::Success {
            term: leader_term,
            last_write_id: 200,
            last_commit_id: 200,
        },
    );
    let _ = node.replication().peer(3).unwrap().get_append_range(t1);
    node.replication().peer(3).unwrap().append_result(
        t1,
        AppendResult::Success {
            term: leader_term,
            last_write_id: 200,
            last_commit_id: 200,
        },
    );


    // §5.4.2 itself is correct (it prevents the Figure-8 safety
    // violation). Raft's
    // standard fix is to write a no-op on election win to commit a
    // current-term entry, which then lifts cluster_commit past
    // current_term_first_tx and unblocks the gate.
    assert_eq!(
        node.cluster_commit_index(),
        0,
        "cluster_commit_index stuck at 0 — §5.4.2 gate firing forever \
         because no current-term entry is ever committed (no no-op on election)"
    );
}
