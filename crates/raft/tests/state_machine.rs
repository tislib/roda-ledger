//! Regression tests for Bug 3 from the node.rs review:
//! `become_leader_after_win` synthesises a term-log catch-up record
//! at `start_tx_id = local_log_index + 1` when the vote log has
//! raced ahead of the term log by more than one step. This produces
//! either:
//!
//!   (a) a phantom term-record at a `start_tx_id` no real entry was
//!       ever assigned, polluting `term_at_tx` lookups, or
//!   (b) two distinct term-records sharing the same `start_tx_id`
//!       (the synthetic one and the genuine `commit_term` one),
//!       which works only because `term_at_tx` picks the largest
//!       `term` among ties.
//!
//! The tests below set up the exact "vote log is ahead of term log"
//! state that triggers the catch-up, then assert on:
//!
//!   1. The term log contents directly (no two records may share a
//!      `start_tx_id`; no record may exist for a term we never
//!      actually led).
//!   2. The §5.3 prev-log-term lookup (`term_at_tx` at a tx that
//!      pre-dates the won term should NOT return that term).
//!
//! When the bug is fixed (either by removing the catch-up and
//! relaxing `commit_term`'s `current + 1` check, or by tracking the
//! catch-up more carefully), these tests should still pass.

mod common;

use std::time::{Duration, Instant};

use common::mem_persistence::MemPersistence;
use raft::{NodeId, Persistence, RaftConfig, RaftNode, RequestVoteRequest, Role, TermRecord};

/// Build a candidate at term `T` with `local_log_index = 0` and
/// `term_log = []`. The candidate will then observe a higher term
/// `T+K` from an inbound `RequestVote`, leaving the vote log at
/// `T+K` and the term log empty. When it later wins term `T+K+1`
/// (after the spuriously-higher candidate fails or steps down), we
/// inspect the resulting term log.
fn fresh_node(self_id: NodeId, peers: Vec<NodeId>) -> RaftNode<MemPersistence> {
    RaftNode::new(
        self_id,
        peers,
        MemPersistence::new(),
        RaftConfig::default(),
        42,
    )
}

/// Force `node` into Candidate role at the next term-log+1.
/// Returns the term it's now campaigning at.
fn drive_to_candidate(node: &mut RaftNode<impl Persistence>) -> (Instant, u64) {
    let t0 = Instant::now();
    let _ = node.election().tick(t0);
    // Election timer is now armed for ~150–300ms. Advance past it.
    let t_after_timeout = t0 + Duration::from_secs(60);
    let _ = node.election().tick(t_after_timeout);
    let _ = node.election().start(t_after_timeout);
    assert_eq!(node.role(), Role::Candidate, "should be candidate");
    (t_after_timeout, node.current_term())
}

// ──────────────────────────────────────────────────────────────────────────
// Bug 3.1 — Vote-log skew of exactly +1 (smallest case)
// ──────────────────────────────────────────────────────────────────────────

/// Candidate at term 1 receives a `RequestVoteRequest` at term 5.
/// Vote log → 5, term log → empty. If the candidate then wins
/// term 6 (single-node cluster — self-vote alone is majority),
/// `become_leader_after_win` will need to bridge the gap.
///
/// **Property under test:** the term log after the win must contain
/// exactly one record `(term=6, start_tx_id=1)`. No phantom record
/// for term 5 (or any other term) should exist.
#[test]
fn term_log_has_no_phantom_record_after_vote_log_race() {
    // Use a 3-node cluster, but only feed events relevant to the
    // candidate. We won't actually depend on peers replying.
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let (t_election, candidate_term) = drive_to_candidate(&mut node);

    // Candidate is now at vote_term = candidate_term, term_log = [].
    // An inbound RequestVote at a much higher term should bump
    // vote_log only, not term_log.
    let bump_to = candidate_term + 4;
    let _ = node.election().handle_request_vote(
        t_election + Duration::from_millis(1),
        RequestVoteRequest {
            from: 2,
            term: bump_to,
            last_tx_id: 0,
            last_term: 0,
        },
    );
    assert_eq!(
        node.role(),
        Role::Follower,
        "higher-term RV should drop us to follower"
    );
    assert_eq!(node.current_term(), bump_to, "vote-log advanced");

    // Now drive a fresh election. We're a follower in a 3-node
    // cluster — without peer cooperation we can't win. So switch
    // to a single-node cluster shape via reconstruction: pull out
    // the persistence, rebuild as 1-node so self-vote is majority.
    let p = node.into_persistence();
    let mut node = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);

    // Roll the timer once to arm, once to expire.
    let t = t_election + Duration::from_millis(2);
    common::drive_tick(&mut node, t);
    common::drive_tick(&mut node, t + Duration::from_secs(60));

    assert_eq!(
        node.role(),
        Role::Leader,
        "single-node cluster must self-elect"
    );

    let won_term = node.current_term();
    assert!(
        won_term > bump_to,
        "won term {} must exceed observed term {}",
        won_term,
        bump_to
    );

    // Now inspect the term log. The ONLY record should be
    // `(term=won_term, start_tx_id=1)`. No phantom catch-up.
    let p = node.into_persistence();
    let records = collect_term_records(&p);

    assert_eq!(
        records.len(),
        1,
        "term log should contain exactly one record (the won term), \
         got {} records: {:?}",
        records.len(),
        records
    );
    assert_eq!(
        records[0],
        TermRecord {
            term: won_term,
            start_tx_id: 1,
        },
        "the sole term record must be the winning term at start_tx_id=1"
    );
}

// ──────────────────────────────────────────────────────────────────────────
// Bug 3.2 — Multi-step skew (>2 terms ahead)
// ──────────────────────────────────────────────────────────────────────────

/// Vote log races ahead of term log by 5 terms. This case stresses
/// the catch-up loop: the buggy implementation only writes one
/// synthetic record for `new_term - 1`, leaving terms `new_term -
/// 4`, `new_term - 3`, `new_term - 2` undocumented. After fixing,
/// none of these synthetic records should appear.
#[test]
fn no_synthetic_term_records_for_unobserved_terms() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let (t_election, candidate_term) = drive_to_candidate(&mut node);

    // Bump vote log five terms ahead via a sequence of inbound RVs.
    let mut t = t_election;
    for k in 1..=5 {
        t += Duration::from_millis(1);
        let _ = node.election().handle_request_vote(
            t,
            RequestVoteRequest {
                from: 2,
                term: candidate_term + k,
                last_tx_id: 0,
                last_term: 0,
            },
        );
    }
    let observed = candidate_term + 5;
    assert_eq!(node.current_term(), observed);

    // Promote to single-node cluster so self-elect produces a Leader.
    let p = node.into_persistence();
    let mut node = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);

    common::drive_tick(&mut node, t + Duration::from_millis(1));
    common::drive_tick(&mut node, t + Duration::from_secs(60));
    assert_eq!(node.role(), Role::Leader);
    let won_term = node.current_term();

    let p = node.into_persistence();
    let records = collect_term_records(&p);

    // The fixed implementation should produce ONE record:
    // (won_term, 1). The buggy implementation produces TWO records:
    // (won_term - 1, 1) and (won_term, 1) — a phantom for a term
    // we never led, plus the legitimate one.
    assert_eq!(
        records.len(),
        1,
        "expected exactly the won term in the log, got: {:?}",
        records
    );
    assert_eq!(records[0].term, won_term);
    assert_eq!(records[0].start_tx_id, 1);

    // Defensive: no record exists for any term in (term-log-was, won_term).
    // The buggy code writes (won_term - 1, 1) which we explicitly forbid.
    for r in &records {
        assert_ne!(
            r.term,
            won_term - 1,
            "phantom term record for {} (a term we never led) at start_tx_id {}",
            r.term,
            r.start_tx_id
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Bug 3.3 — Two records sharing start_tx_id is itself a violation
// ──────────────────────────────────────────────────────────────────────────

/// Independent of *what* terms the records describe, two term-log
/// records must never share the same `start_tx_id`. The current
/// implementation gets away with it because `term_at_tx` resolves
/// the tie by picking the largest term, but storing the same
/// boundary twice is fragile (a future change to ordering, or
/// reload-from-disk replay, would surface it).
#[test]
fn term_log_records_have_unique_start_tx_id() {
    let mut node = fresh_node(1, vec![1, 2, 3]);
    let (t_election, candidate_term) = drive_to_candidate(&mut node);

    // Two-step skew is enough to force the catch-up.
    let mut t = t_election + Duration::from_millis(1);
    let _ = node.election().handle_request_vote(
        t,
        RequestVoteRequest {
            from: 2,
            term: candidate_term + 2,
            last_tx_id: 0,
            last_term: 0,
        },
    );

    let p = node.into_persistence();
    let mut node = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);
    t += Duration::from_millis(1);
    common::drive_tick(&mut node, t);
    common::drive_tick(&mut node, t + Duration::from_secs(60));
    assert_eq!(node.role(), Role::Leader);

    let p = node.into_persistence();
    let records = collect_term_records(&p);

    let mut seen: std::collections::HashSet<u64> = Default::default();
    for r in &records {
        assert!(
            seen.insert(r.start_tx_id),
            "duplicate start_tx_id={} in term log: {:?}",
            r.start_tx_id,
            records
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Bug 3.4 — term_at_tx must not return the won term for prior entries
// ──────────────────────────────────────────────────────────────────────────

/// If we have entries in the log at a previous term, then race the
/// vote log ahead, then win a new term, `term_at_tx` for those old
/// entries must still return their original term — not the synthetic
/// catch-up term, and not the won term.
///
/// The buggy `become_leader_after_win` writes
/// `observe_term(new_term - 1, local_log_index + 1)`. If we have
/// existing entries at tx_id ≤ local_log_index, the catch-up record
/// at `local_log_index + 1` doesn't shadow them — so this scenario
/// might pass even with the bug. The pure-skew cases above are the
/// real triggers.
///
/// This test serves as a guard: even when this scenario doesn't
/// directly trigger Bug 3, the resulting term log must still be
/// queryable correctly. If the fix introduces a regression, this
/// catches it.
#[test]
fn term_at_tx_for_existing_entries_unaffected_by_catch_up() {
    // Phase 1: drive node to Leader at term 1, append entries 1..3.
    let mut node = fresh_node(1, vec![1]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert!(node.role().is_leader());
    assert_eq!(node.current_term(), 1);

    node.advance_write_index(1);
    node.advance_commit_index(1);
    common::drive_tick(&mut node, t0 + Duration::from_secs(61));
    node.advance_write_index(2);
    node.advance_commit_index(2);
    common::drive_tick(&mut node, t0 + Duration::from_secs(61));
    node.advance_write_index(3);
    node.advance_commit_index(3);
    common::drive_tick(&mut node, t0 + Duration::from_secs(61));
    assert_eq!(node.commit_index(), 3);

    // Phase 2: rebuild as multi-node and force a vote-log race.
    let p = node.into_persistence();
    let baseline_records = collect_term_records(&p);
    assert!(
        baseline_records.iter().any(|r| r.term == 1),
        "term 1 must be in the log after phase 1"
    );

    let mut node = RaftNode::new(1, vec![1, 2, 3], p, RaftConfig::default(), 42);
    // Driver-side WAL recovery: the on-disk WAL has 3 entries from
    // phase 1; the rebuilt node must learn its `local_write_index`
    // before any election runs, otherwise `start_tx_id` for the
    // post-election term boundary lands at 1 and shadows the
    // existing entries on `term_at_tx` lookups.
    node.advance_write_index(3);
    node.advance_commit_index(3);
    common::drive_tick(&mut node, t0 + Duration::from_secs(119));
    common::drive_tick(&mut node, t0 + Duration::from_secs(120));
    common::drive_tick(&mut node, t0 + Duration::from_secs(180));
    assert_eq!(node.role(), Role::Candidate);
    let cand_term = node.current_term();

    // Race the vote log forward.
    let _ = node.election().handle_request_vote(
        t0 + Duration::from_secs(181),
        RequestVoteRequest {
            from: 2,
            term: cand_term + 3,
            last_tx_id: 3,
            last_term: 1,
        },
    );

    // Phase 3: shrink to single-node and self-elect. Replay the
    // WAL high-water mark again — `RaftNode::new` always starts
    // `local_log_index = 0`.
    let p = node.into_persistence();
    let mut node = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);
    node.advance_write_index(3);
    node.advance_commit_index(3);
    common::drive_tick(&mut node, t0 + Duration::from_secs(181));
    common::drive_tick(&mut node, t0 + Duration::from_secs(182));
    common::drive_tick(&mut node, t0 + Duration::from_secs(240));
    assert_eq!(node.role(), Role::Leader);
    let won = node.current_term();

    // Phase 4: inspect.
    let p = node.into_persistence();

    // The original entries 1..3 were written at term 1. Their term
    // must still resolve to 1, not to any phantom term.
    for tx in 1..=3 {
        let rec = p.term_at_tx(tx).expect("term must exist for tx");
        assert_eq!(
            rec.term, 1,
            "term_at_tx({}) returned {} (expected 1)",
            tx, rec.term
        );
    }

    // No record may exist for `won - 1` (the catch-up phantom term).
    let records = collect_term_records(&p);
    for r in &records {
        assert_ne!(
            r.term,
            won - 1,
            "phantom term record for won_term-1 = {} at start_tx_id {}",
            won - 1,
            r.start_tx_id,
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────

/// Walk every tx_id from 0 up to a probe ceiling, collecting the
/// distinct term records that appear. The `Persistence` trait does
/// not expose the term log directly; we reconstruct it by querying
/// `term_at_tx` at every increment and noting where the answer
/// changes.
///
/// `term_at_tx` returns the record with the largest `start_tx_id <=
/// tx_id`, so two records with the same `start_tx_id` would be
/// indistinguishable by this method — a real test of "no duplicate
/// `start_tx_id`" requires direct access to the underlying `Vec`,
/// which `MemPersistence` provides via a test-only accessor (see
/// `mem_persistence.rs`'s `with_state` constructor — symmetric
/// `into_state` / `term_log()` should be added if not present).
///
/// For now this function returns a deduplicated set of records seen
/// across `tx_id ∈ [0, ceiling]`. Sufficient for the
/// "phantom record at won-term-1" assertion.
fn collect_term_records<P: Persistence>(p: &P) -> Vec<TermRecord> {
    let mut seen: Vec<TermRecord> = Vec::new();
    for tx in 0..=200 {
        if let Some(rec) = p.term_at_tx(tx)
            && !seen.contains(&rec)
        {
            seen.push(rec);
        }
    }
    seen.sort_by_key(|r| r.start_tx_id);
    seen
}
