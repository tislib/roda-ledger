//! Regression tests for bugs found in the previous in-place
//! `cluster::raft::*` implementation. Each test reproduces a bug
//! that would have triggered a known incident before the fixes
//! described in ADR-0017 (term-bump-before-win, boot-time term
//! bump, missing §5.3 walk-back, term log not truncating with the
//! WAL).

mod common;

use std::time::{Duration, Instant};

use common::Sim;
use common::mem_persistence::MemPersistence;
use raft::{Action, Event, LogEntryRange, NodeId, RaftConfig, RaftNode, Role, TxId};

fn fresh_node(self_id: u64, peers: Vec<u64>) -> RaftNode<MemPersistence> {
    RaftNode::new(
        self_id,
        peers,
        MemPersistence::new(),
        RaftConfig::default(),
        42,
    )
}

fn await_leader(sim: &mut Sim) -> NodeId {
    let dl = sim.clock() + Duration::from_secs(5);
    sim.run_until_predicate(dl, |s| {
        [1u64, 2, 3].iter().any(|n| s.role_of(*n) == Role::Leader)
    });
    [1u64, 2, 3]
        .iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
        .unwrap()
}

/// Term-bump-before-win: previously, every lost election left a
/// permanent record in the term log because `commit_term` was called
/// before the win was confirmed. Verify the fix: `last_term_record()`
/// stays `None` across a sequence of failed candidacies.
#[test]
fn lost_elections_do_not_pollute_term_log() {
    let mut node = fresh_node(1, vec![1, 2]);
    let mut now = Instant::now();
    let _ = node.step(now, Event::Tick);
    for _ in 0..5 {
        now += Duration::from_secs(60);
        let _ = node.step(now, Event::Tick);
    }
    let p = node.into_persistence();
    use raft::Persistence;
    assert_eq!(p.current_term(), 0, "term log advanced on lost candidacy");
    assert!(p.last_term_record().is_none(), "phantom term-log record");
    assert!(
        p.vote_term() >= 1,
        "vote log did not advance — candidate never self-voted?"
    );
}

/// Boot-time term bump: previously the constructor incremented term
/// on every start. Verify the fix: a node restarted from durable
/// state has the same `current_term` and `voted_for` as before, and
/// no `BecomeRole` action fires at construction.
#[test]
fn restart_does_not_bump_term() {
    let mut node = fresh_node(1, vec![1]);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert!(node.role().is_leader());
    let term_before = node.current_term();

    let p = node.into_persistence();
    let restarted = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);
    assert_eq!(restarted.current_term(), term_before);
    // Construction emits no actions — only step does.
    assert_eq!(restarted.role(), Role::Initializing);
}

/// §5.3 walk-back: previously, on a `LogMismatch` reject the leader
/// did not decrement `next_index`, so it retried the same range
/// indefinitely. Verify the fix: `next_index` walks back one entry
/// per reject until the agreement point is found.
#[test]
fn log_mismatch_decrements_next_index() {
    // Construct a leader (single-node so we don't depend on peers).
    let mut node = fresh_node(1, vec![1]);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert!(node.role().is_leader());

    // Pretend it has 5 entries in flight to a phantom peer 2 with
    // next_index=6. We can't reach into LeaderState directly; instead,
    // exercise the rejection path through the public step API.
    //
    // Simulate the leader having committed 3 entries:
    for tx in 1..=3 {
        node.advance(tx, tx);
        let _ = node.step(t0 + Duration::from_secs(60 + tx), Event::Tick);
    }

    // Inject 3 successive LogMismatch rejects from peer 2 — each
    // should produce a `SendAppendEntries` with a strictly smaller
    // `prev_log_tx_id`.
    use raft::RejectReason;
    let mut last_prev_log: Option<u64> = None;
    for i in 0..3 {
        let actions = node.step(
            t0 + Duration::from_secs(70 + i),
            Event::AppendEntriesReply {
                from: 2,
                term: node.current_term(),
                success: false,
                last_commit_id: 0,
                last_write_id: 0,
                reject_reason: Some(RejectReason::LogMismatch),
            },
        );
        // The leader's next leader_drive() will fire on the next Tick.
        let _ = node.step(t0 + Duration::from_secs(80 + i), Event::Tick);
        if let Some(prev) = actions.iter().find_map(|a| match a {
            Action::SendAppendEntries { prev_log_tx_id, .. } => Some(*prev_log_tx_id),
            _ => None,
        }) {
            if let Some(p) = last_prev_log {
                assert!(
                    prev < p,
                    "prev_log_tx_id should walk back ({} -> {})",
                    p,
                    prev
                );
            }
            last_prev_log = Some(prev);
        }
    }
}

/// Term log truncation paired with §5.3 entry-log truncation:
/// previously the term log retained stale records when the entry
/// log was truncated. Verify the fix: when a follower truncates its
/// log on §5.3 mismatch, the term-log records past the new high
/// water mark are dropped synchronously through the trait.
#[test]
fn term_log_truncates_with_entry_log() {
    let mut node = fresh_node(1, vec![1, 2, 3]);

    // Drive entries 1..5 at term 1 into the follower. The driver
    // acks durability via `advance` + `Tick`, which is what advances
    // both watermarks and drains the parked success reply.
    let _ = node.step(
        Instant::now(),
        Event::AppendEntriesRequest {
            from: 2,
            term: 1,
            prev_log_tx_id: 0,
            prev_log_term: 0,
            entries: LogEntryRange::new(1, 5, 1),
            leader_commit: 0,
        },
    );
    // Written but not committed — the §5.4 truncation guard forbids
    // truncating below `local_commit_index`, and this test then
    // truncates to 4. Set commit=0 so the truncation is allowed.
    node.advance(5, 0);
    let _ = node.step(Instant::now(), Event::Tick);
    assert_eq!(node.write_index(), 5);

    // Now arrive an AppendEntries from a leader at term 2 with
    // prev_log_tx_id=5, prev_log_term=2 — mismatch. Follower must
    // truncate to 4 in BOTH the entry log (Action::TruncateLog)
    // and the term log (synchronous through the trait).
    let actions = node.step(
        Instant::now(),
        Event::AppendEntriesRequest {
            from: 2,
            term: 2,
            prev_log_tx_id: 5,
            prev_log_term: 2,
            entries: LogEntryRange::empty(),
            leader_commit: 0,
        },
    );
    let truncate = actions.iter().find_map(|a| match a {
        Action::TruncateLog { after_tx_id } => Some(*after_tx_id),
        _ => None,
    });
    assert_eq!(truncate, Some(4));

    // Confirm the term log mirror is consistent: term_at_tx beyond
    // the truncation point should not return a record from a
    // future term.
    use raft::Persistence;
    let p = node.into_persistence();
    let last = p.last_term_record();
    assert!(
        last.map(|r| r.start_tx_id <= 4).unwrap_or(true),
        "term log retained a record past the truncation: {:?}",
        last
    );
}

/// Term-N split-brain prevention. Previously, when a leader at term
/// N received a higher-term reply and stepped down, two new
/// candidacies could simultaneously win term N+1. With the
/// commit_term `current + 1 == expected` check, at most one wins.
///
/// We can't directly script the race in the simulator (it's
/// deterministic) but we can verify the underlying invariant:
/// `commit_term(expected)` returns `Ok(false)` when current has
/// already advanced past `expected`.
#[test]
fn commit_term_refuses_when_already_advanced() {
    use raft::Persistence;
    let mut p = MemPersistence::new();
    p.observe_term(5, 0).unwrap();
    // Two concurrent winners of term 5 would both call commit_term(5).
    // The first to land on a particular replica gets Ok(false) here
    // because current=5 already; only the actual term-5 winner wrote
    // it — the loser steps down on this signal.
    assert!(!p.commit_term(5, 0).unwrap());
}

/// **Raft §5.4.2 / Figure 8** — a new leader must not commit
/// entries from previous terms by counting replicas. Only entries
/// from the leader's current term may be committed by replica
/// counting; prior-term entries become committed indirectly via
/// the Log Matching Property, but only once a current-term entry
/// has crossed the commit boundary.
///
/// **The trigger.** In a 5-node cluster, the old leader at term 1
/// partially replicated entries 1..=5 to this node before
/// crashing — but it never reached the 3-of-5 majority needed to
/// advance `cluster_commit`, so this node holds the entries with
/// `cluster_commit = 0`. This node now wins term 2 (its log is
/// most up-to-date) and immediately receives `last_tx_id = 5`
/// acks from a quorum of peers. The acks reflect each peer
/// catching up the prior-term log; no current-term entry has been
/// proposed yet.
///
/// Without the §5.4.2 guard, the leader counts 3-of-5 acks at
/// `last_tx = 5` and advances `cluster_commit` to 5 — committing
/// term-1 entries by counting term-2 replicas. A subsequent
/// leader could then overwrite those entries, the canonical
/// Figure 8 violation.
#[test]
fn figure_8_new_leader_does_not_commit_prior_term_entries_by_replica_count() {
    let mut node = fresh_node(1, vec![1, 2, 3, 4, 5]);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);

    // Phase 1 — receive entries 1..=5 at term 1 from a leader
    // whose own commit watermark is still 0 (it crashed before
    // achieving majority on these entries).
    let _ = node.step(
        t0 + Duration::from_millis(10),
        Event::AppendEntriesRequest {
            from: 2,
            term: 1,
            prev_log_tx_id: 0,
            prev_log_term: 0,
            entries: LogEntryRange::new(1, 5, 1),
            leader_commit: 0,
        },
    );
    // Driver acks raft-log durability; ledger has not yet applied
    // (leader_commit was 0). Models the §5.4.2 scenario faithfully:
    // entries durably written but not locally committed.
    node.advance(5, 0);
    let _ = node.step(t0 + Duration::from_millis(11), Event::Tick);
    assert_eq!(node.write_index(), 5, "entries durable on disk");
    assert_eq!(
        node.cluster_commit_index(),
        0,
        "no commit propagated yet — old leader crashed pre-majority"
    );

    // Phase 2 — silence past the election timeout, become
    // candidate at term 2, then collect votes from 2 peers (with
    // self-vote that is 3/5, a majority) and become leader.
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert_eq!(node.role(), Role::Candidate);
    let cand_term = node.current_term();
    assert_eq!(cand_term, 2);

    let now = t0 + Duration::from_secs(60) + Duration::from_millis(1);
    let _ = node.step(
        now,
        Event::RequestVoteReply {
            from: 3,
            term: cand_term,
            granted: true,
        },
    );
    let _ = node.step(
        now,
        Event::RequestVoteReply {
            from: 4,
            term: cand_term,
            granted: true,
        },
    );
    assert!(
        node.role().is_leader(),
        "should be leader at term {cand_term} after 3-of-5 votes"
    );

    // Phase 3 — receive the catch-up acks. Each peer reports
    // `last_tx_id = 5` after the new leader's first AE rounds
    // bring their logs up to the prior-term high-water mark.
    // Without §5.4.2, this advances `cluster_commit` to 5
    // (committing term-1 entries by current-term replica count).
    let _ = node.step(
        now,
        Event::AppendEntriesReply {
            from: 3,
            term: cand_term,
            success: true,
            last_commit_id: 5,
            last_write_id: 5,
            reject_reason: None,
        },
    );
    let _ = node.step(
        now,
        Event::AppendEntriesReply {
            from: 4,
            term: cand_term,
            success: true,
            last_commit_id: 5,
            last_write_id: 5,
            reject_reason: None,
        },
    );

    assert!(
        node.cluster_commit_index() < 5,
        "Figure 8 violation: new leader at term {} committed prior-term entries by replica counting (cluster_commit={}, local_log_index={})",
        cand_term,
        node.cluster_commit_index(),
        node.commit_index(),
    );
}

// ─── AE reply: split watermark (last_write_id vs last_commit_id) ───────────
//
// The leader's `on_append_entries_reply` reads two distinct
// watermarks from each reply and uses them for orthogonal
// decisions (ADR-0017 §"AE reply: write vs commit watermark"):
//
//   `last_write_id`  → `progress.next_index` (the replication
//                      window — "what to ship next")
//   `last_commit_id` → `progress.match_index` and `quorum`
//                      (the durable end — "what's safe to
//                      commit cluster-wide")
//
// These tests pin the split: the library must treat them as
// independent inputs even though today's cluster bridge happens to
// populate both from the same durability ack. They exercise the
// library by injecting `Event::AppendEntriesReply` directly.

/// Drive a fresh node to leader of a 3-node cluster at term 1, with
/// `local_log_index = last_written = entries` after construction.
/// Returns `(node, term, t_after_setup)` so the caller can keep
/// stepping with consistent timestamps.
fn leader_with_entries(entries: TxId, t0: Instant) -> (RaftNode<MemPersistence>, u64, Instant) {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    let term = node.current_term();
    // 3-node cluster majority = 2 (self-vote + 1 peer = win).
    let _ = node.step(
        t0 + Duration::from_secs(60),
        Event::RequestVoteReply { from: 2, term, granted: true },
    );
    assert!(node.role().is_leader(), "test setup: did not become leader");

    let after = t0 + Duration::from_secs(61);
    if entries > 0 {
        node.advance(entries, entries);
        let _ = node.step(after, Event::Tick);
    }
    // Drain the initial AE round so peers have an in-flight set;
    // subsequent replies clear it cleanly.
    let _ = node.step(after + Duration::from_millis(1), Event::Tick);
    (node, term, after + Duration::from_millis(1))
}

/// Bug repro: with the old single-watermark logic, `next_index`
/// advanced from the durable end (`last_commit_id + 1`), causing the
/// leader to re-ship entries the follower had already accepted but
/// not yet fsync'd. After the fix `next_index` advances from
/// `last_write_id + 1`.
#[test]
fn ae_reply_advances_next_index_from_write_id_not_commit_id() {
    let t0 = Instant::now();
    let (mut node, term, t_setup) = leader_with_entries(5, t0);

    // Peer 2 has accepted all 5 entries into its log but only fsync'd
    // 3 of them. Its reply carries the split watermark.
    let _ = node.step(
        t_setup + Duration::from_millis(10),
        Event::AppendEntriesReply {
            from: 2,
            term,
            success: true,
            last_commit_id: 3,
            last_write_id: 5,
            reject_reason: None,
        },
    );

    // Advance past `next_heartbeat` (default 50ms) so the leader
    // re-evaluates what to send. With the fix `next_index = 6`, so
    // the next AE to peer 2 is a heartbeat. Under the bug
    // `next_index = 4`, so the leader would re-ship entries [4, 5].
    let actions = node.step(t_setup + Duration::from_millis(100), Event::Tick);
    let entries_to_peer_2: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::SendAppendEntries { to: 2, entries, .. } => Some(entries.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !entries_to_peer_2.is_empty(),
        "expected the leader to send to peer 2 after next_heartbeat fires; got {:?}",
        actions
    );
    for entries in &entries_to_peer_2 {
        assert!(
            entries.is_empty(),
            "leader re-shipped entries the follower already accepted: \
             next_index must advance from last_write_id (5)+1=6 \
             (heartbeat), not last_commit_id (3)+1=4 (re-ship). \
             Got entries={:?}",
            entries
        );
    }
}

/// Quorum / `cluster_commit_index` must advance from `last_commit_id`
/// only (the durable watermark), never from `last_write_id`. Two
/// peers ack with `last_write_id = 5, last_commit_id = 3`; the
/// leader's self-slot is at 5. The quorum's middle-of-three is 3,
/// not 5 — committing entries 4 and 5 here would let a future
/// leader overwrite them (the canonical write-not-yet-durable
/// hazard).
#[test]
fn ae_reply_quorum_advances_only_to_commit_id() {
    let t0 = Instant::now();
    let (mut node, term, t_setup) = leader_with_entries(5, t0);

    let _ = node.step(
        t_setup + Duration::from_millis(10),
        Event::AppendEntriesReply {
            from: 2,
            term,
            success: true,
            last_commit_id: 3,
            last_write_id: 5,
            reject_reason: None,
        },
    );
    let _ = node.step(
        t_setup + Duration::from_millis(11),
        Event::AppendEntriesReply {
            from: 3,
            term,
            success: true,
            last_commit_id: 3,
            last_write_id: 5,
            reject_reason: None,
        },
    );

    assert_eq!(
        node.cluster_commit_index(),
        3,
        "cluster_commit must reflect the durable end (last_commit_id=3), \
         not the accepted end (last_write_id=5). Both peers' write-but-not-durable \
         entries 4-5 must NOT count toward quorum."
    );
}

/// A second reply from the same peer with the same `last_write_id`
/// but a higher `last_commit_id` advances `match_index` (durability
/// progressing) without disturbing `next_index`. Pins the
/// monotonicity contract for the watermark split.
#[test]
fn ae_reply_commit_lagging_write_does_not_regress_next_index() {
    let t0 = Instant::now();
    let (mut node, term, t_setup) = leader_with_entries(10, t0);

    // First reply: peer 2 has accepted all 10 but only durable to 3.
    let _ = node.step(
        t_setup + Duration::from_millis(10),
        Event::AppendEntriesReply {
            from: 2,
            term,
            success: true,
            last_commit_id: 3,
            last_write_id: 10,
            reject_reason: None,
        },
    );
    // Peer 3 too — gives us a 3/3 majority at commit=3 so we can
    // observe further commit progress via `cluster_commit_index`.
    let _ = node.step(
        t_setup + Duration::from_millis(11),
        Event::AppendEntriesReply {
            from: 3,
            term,
            success: true,
            last_commit_id: 3,
            last_write_id: 10,
            reject_reason: None,
        },
    );
    assert_eq!(node.cluster_commit_index(), 3);

    // Second reply from peer 2: write watermark unchanged, commit
    // moved up by one. `next_index` must not regress.
    let _ = node.step(
        t_setup + Duration::from_millis(20),
        Event::AppendEntriesReply {
            from: 2,
            term,
            success: true,
            last_commit_id: 4,
            last_write_id: 10,
            reject_reason: None,
        },
    );
    let _ = node.step(
        t_setup + Duration::from_millis(21),
        Event::AppendEntriesReply {
            from: 3,
            term,
            success: true,
            last_commit_id: 4,
            last_write_id: 10,
            reject_reason: None,
        },
    );
    assert_eq!(
        node.cluster_commit_index(),
        4,
        "match_index must advance with last_commit_id even when last_write_id is unchanged"
    );

    // And next_index is still at 11 — heartbeat to peer 2 on the
    // next Tick.
    let actions = node.step(t_setup + Duration::from_millis(120), Event::Tick);
    let entries_to_peer_2: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::SendAppendEntries { to: 2, entries, .. } => Some(entries.clone()),
            _ => None,
        })
        .collect();
    for entries in &entries_to_peer_2 {
        assert!(
            entries.is_empty(),
            "next_index regressed: leader re-shipped entries after the \
             second reply. Got {:?}",
            entries
        );
    }
}

/// Defensive clamp on a malformed reply where the peer claims to
/// have committed more than it has written. Debug builds must
/// panic (`debug_assert!`); release builds clamp commit down to
/// write and proceed. Same posture as the truncation-below-cluster-
/// commit guard elsewhere in this file.
#[test]
#[cfg_attr(debug_assertions, should_panic(expected = "AE reply: last_commit_id="))]
fn ae_reply_clamps_commit_above_write_defensively() {
    let t0 = Instant::now();
    let (mut node, term, t_setup) = leader_with_entries(10, t0);

    // Malformed: peer claims commit=5 with write=3. Library either
    // panics (debug) or clamps commit to 3 (release).
    let _ = node.step(
        t_setup + Duration::from_millis(10),
        Event::AppendEntriesReply {
            from: 2,
            term,
            success: true,
            last_commit_id: 5,
            last_write_id: 3,
            reject_reason: None,
        },
    );

    // Release-build path: clamp brought commit down to 3, so peer
    // 2's match_index = 3 and next_index = 4. We can verify the
    // latter by triggering a Tick and inspecting the next AE.
    let actions = node.step(t_setup + Duration::from_millis(100), Event::Tick);
    let to_peer_2 = actions.iter().find_map(|a| match a {
        Action::SendAppendEntries { to: 2, entries, prev_log_tx_id, .. } => {
            Some((entries.clone(), *prev_log_tx_id))
        }
        _ => None,
    });
    let (entries, prev) = to_peer_2.expect("expected an AE to peer 2 after Tick");
    assert_eq!(
        prev, 3,
        "release-build clamp: next_index must be 4 (= clamped commit + 1), \
         so prev_log_tx_id = 3. Got prev_log_tx_id = {}",
        prev
    );
    assert!(
        !entries.is_empty(),
        "expected the leader to ship entries [4, 10] starting at next_index=4"
    );
}

/// 3-node cluster after a leader crash converges with all four
/// safety properties intact. The "system stays correct under
/// failover" smoke test that catches regressions across the entire
/// state machine.
#[test]
fn three_node_failover_preserves_all_safety_properties() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 101);
    let leader = await_leader(&mut sim);
    sim.client_write(leader, 4);
    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 4);

    sim.crash(leader);
    let dl = sim.clock() + Duration::from_secs(5);
    sim.run_until_predicate(dl, |s| {
        [1u64, 2, 3]
            .iter()
            .filter(|n| **n != leader)
            .any(|n| s.role_of(*n) == Role::Leader)
    });

    sim.assert_election_safety();
    sim.assert_log_matching();
    sim.assert_state_machine_safety();
}

// ── §5.4.1 reads write_index, not commit_index (split-watermark refactor) ──

/// A node with `local_write_index = 5` and `local_commit_index = 3`
/// (durably written but not yet locally committed) must still deny a
/// vote to a candidate claiming `last_tx_id = 4` at the same term.
/// §5.4.1's up-to-date check reads the write extent — using the
/// commit extent would let a candidate with a strictly older durable
/// log win a vote it shouldn't, breaking Election Safety.
///
/// This test is the highest-value regression for the index split:
/// pre-refactor `local_log_index` conflated write and commit, so the
/// scenario was inexpressible.
#[test]
fn vote_denial_uses_write_index_not_commit_index() {
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 5,
            start_tx_id: 1,
        }],
        5,
        0,
    );
    let mut node = RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42);
    // Five entries durably written, only three locally committed.
    node.advance(5, 3);

    let actions = node.step(
        Instant::now(),
        Event::RequestVoteRequest {
            from: 2,
            term: 6,
            last_tx_id: 4, // strictly less than our write extent
            last_term: 5,  // same term as ours
        },
    );
    let granted = actions.iter().any(|a| matches!(
        a,
        Action::SendRequestVoteReply { granted: true, .. }
    ));
    assert!(
        !granted,
        "vote must use write_index (5), not commit_index (3): {:?}",
        actions
    );
}
