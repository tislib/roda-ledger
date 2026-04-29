//! Failing regression tests for the bugs surfaced by the audit at
//! `~/.claude/plans/investigate-raft-crate-and-cheeky-swing.md`.
//!
//! Each test pins one of the audit findings:
//!
//! - T1: silent failure of `Persistence::truncate_term_after` must be
//!   promoted to `Action::FatalError` (audit finding #1).
//! - T2: silent failure of `Persistence::observe_term` must be
//!   promoted to `Action::FatalError` (finding #2).
//! - T3: once the library has emitted `FatalError`, subsequent
//!   `step()` calls must be no-ops (finding #1/#2 follow-on).
//! - T4: `SendAppendEntriesReply { success: true }` must not be
//!   emitted in the same `step()` as the `AppendLog` action — it
//!   must wait for `Event::LogAppendComplete` so the leader cannot
//!   count an entry as durable on the follower before the driver
//!   has actually persisted it (finding #3).
//! - T5: a follower-side truncation that would drop
//!   `cluster_commit_index` is a Raft invariant violation; the
//!   library must `debug_assert!` instead of silently clamping
//!   (finding #4).

mod common;

use std::time::{Duration, Instant};

use common::mem_persistence::MemPersistence;
use raft::{Action, Event, LogEntryRange, RaftConfig, RaftNode, TermRecord};

fn make_follower(persistence: MemPersistence) -> RaftNode<MemPersistence> {
    RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42)
}

// ─── T1: truncate_term_after I/O failure → FatalError ─────────────────────

#[test]
fn truncate_term_after_io_error_emits_fatal_error() {
    // Pre-load: term log has (term=2, start=1), vote_term=2 — i.e.
    // this follower has already lived through term 2 and accepted
    // entries 1..=5 from it.
    let mut persistence = MemPersistence::with_state(
        vec![TermRecord { term: 2, start_tx_id: 1 }],
        2,
        0,
    );
    persistence.fail_truncate = true;

    let mut node = make_follower(persistence);
    let now = Instant::now();

    // Hydrate `local_log_index` to 5 without touching persistence.
    let _ = node.step(now, Event::LogAppendComplete { tx_id: 5 });
    assert_eq!(node.commit_index(), 5);

    // Phantom AppendEntries from peer 2 at term 2 with a deliberately
    // bogus prev_log_term so the follower takes the §5.3 truncation
    // path. The injected fault makes `truncate_term_after` return
    // `Err`.
    let actions = node.step(
        now,
        Event::AppendEntriesRequest {
            from: 2,
            term: 2,
            prev_log_tx_id: 3,
            prev_log_term: 99, // mismatched with our term-2 record
            entries: LogEntryRange::empty(),
            leader_commit: 0,
        },
    );

    // FatalError must be emitted.
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::FatalError { .. })),
        "expected Action::FatalError, got {:?}",
        actions
    );
    // Driver must not be told to truncate the entry log — the term
    // log truncation failed, so doing the entry-log truncation
    // anyway would leave the two out of sync.
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::TruncateLog { .. })),
        "TruncateLog must not be emitted when term-log truncation failed: {:?}",
        actions
    );
    // No success reply: we did not durably accept anything.
    assert!(
        !actions.iter().any(|a| matches!(
            a,
            Action::SendAppendEntriesReply { success: true, .. }
        )),
        "must not reply success after a fatal: {:?}",
        actions
    );
    // local_log_index must not have moved.
    assert_eq!(node.commit_index(), 5);
}

// ─── T2: observe_term I/O failure → FatalError ────────────────────────────

#[test]
fn observe_term_io_error_emits_fatal_error() {
    let mut persistence = MemPersistence::with_state(
        vec![TermRecord { term: 1, start_tx_id: 1 }],
        1,
        0,
    );
    persistence.fail_observe_term = true;

    let mut node = make_follower(persistence);
    let now = Instant::now();

    // Inbound AppendEntries at a brand-new term 5 with non-empty
    // entries — this is exactly the path that calls
    // `observe_term(5, ...)`.
    let actions = node.step(
        now,
        Event::AppendEntriesRequest {
            from: 2,
            term: 5,
            prev_log_tx_id: 0,
            prev_log_term: 0,
            entries: LogEntryRange::new(1, 3, 5),
            leader_commit: 0,
        },
    );

    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::FatalError { .. })),
        "expected Action::FatalError, got {:?}",
        actions
    );
    assert!(
        !actions.iter().any(|a| matches!(a, Action::AppendLog { .. })),
        "AppendLog must not be emitted when observe_term failed: {:?}",
        actions
    );
    assert!(
        !actions.iter().any(|a| matches!(
            a,
            Action::SendAppendEntriesReply { success: true, .. }
        )),
        "must not reply success after a fatal: {:?}",
        actions
    );
    assert_eq!(node.commit_index(), 0);
}

// ─── T3: step() after a fatal returns no further actions ─────────────────

#[test]
fn step_after_fatal_error_yields_no_actions() {
    // Trigger a fatal via the same scenario as T1.
    let mut persistence = MemPersistence::with_state(
        vec![TermRecord { term: 2, start_tx_id: 1 }],
        2,
        0,
    );
    persistence.fail_truncate = true;
    let mut node = make_follower(persistence);
    let now = Instant::now();

    let _ = node.step(now, Event::LogAppendComplete { tx_id: 5 });
    let first = node.step(
        now,
        Event::AppendEntriesRequest {
            from: 2,
            term: 2,
            prev_log_tx_id: 3,
            prev_log_term: 99,
            entries: LogEntryRange::empty(),
            leader_commit: 0,
        },
    );
    assert!(
        first.iter().any(|a| matches!(a, Action::FatalError { .. })),
        "precondition: first step must fatal-out, got {:?}",
        first
    );

    // Subsequent ticks and inbound RPCs must produce no actions.
    let after_tick = node.step(now + Duration::from_secs(60), Event::Tick);
    assert!(
        after_tick.is_empty(),
        "step after fatal must be a no-op, got {:?}",
        after_tick
    );

    let after_rpc = node.step(
        now + Duration::from_secs(120),
        Event::AppendEntriesRequest {
            from: 2,
            term: 9,
            prev_log_tx_id: 0,
            prev_log_term: 0,
            entries: LogEntryRange::empty(),
            leader_commit: 0,
        },
    );
    assert!(
        after_rpc.is_empty(),
        "step after fatal must be a no-op, got {:?}",
        after_rpc
    );
}

// ─── T4: success reply must wait for LogAppendComplete ───────────────────

#[test]
fn success_reply_waits_for_log_append_complete() {
    let persistence = MemPersistence::new();
    let mut node = make_follower(persistence);
    let now = Instant::now();

    // Inbound AppendEntries from a fresh leader at term 1, three new
    // entries 1..=3.
    let first = node.step(
        now,
        Event::AppendEntriesRequest {
            from: 2,
            term: 1,
            prev_log_tx_id: 0,
            prev_log_term: 0,
            entries: LogEntryRange::new(1, 3, 1),
            leader_commit: 0,
        },
    );

    // The library must have asked the driver to append, but must NOT
    // yet have replied success — the entries are not durable until
    // the driver acks via `Event::LogAppendComplete`.
    assert!(
        first.iter().any(|a| matches!(a, Action::AppendLog { .. })),
        "AppendLog must be emitted, got {:?}",
        first
    );
    assert!(
        !first.iter().any(|a| matches!(
            a,
            Action::SendAppendEntriesReply { success: true, .. }
        )),
        "success reply must not be emitted before LogAppendComplete: {:?}",
        first
    );

    // Driver acknowledges durability — NOW the success reply fires.
    let second = node.step(now, Event::LogAppendComplete { tx_id: 3 });
    let success_reply = second.iter().find(|a| {
        matches!(
            a,
            Action::SendAppendEntriesReply { success: true, .. }
        )
    });
    let reply = success_reply.unwrap_or_else(|| {
        panic!(
            "success reply must be emitted after LogAppendComplete, got {:?}",
            second
        )
    });
    if let Action::SendAppendEntriesReply {
        to,
        success,
        last_tx_id,
        ..
    } = reply
    {
        assert_eq!(*to, 2);
        assert!(*success);
        assert_eq!(*last_tx_id, 3);
    }
}

// ─── T5: truncation below cluster_commit_index must panic in debug ───────

/// Constructs an internally inconsistent state where
/// `cluster_commit_index = 5` and the next AE drives a truncation to
/// tx_id 1. In a correct Raft run this is impossible (Leader
/// Completeness §5.4 guarantees committed entries appear in every
/// future leader's log), so the library must `debug_assert!` rather
/// than silently clamping.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "truncation below cluster_commit_index")]
fn truncation_below_cluster_commit_index_panics_in_debug() {
    let persistence = MemPersistence::with_state(
        vec![TermRecord { term: 1, start_tx_id: 1 }],
        1,
        0,
    );
    let mut node = make_follower(persistence);
    let now = Instant::now();

    // Hydrate local_log_index to 10.
    let _ = node.step(now, Event::LogAppendComplete { tx_id: 10 });

    // First AE: matching prev_log to advance cluster_commit_index to 5.
    let _ = node.step(
        now,
        Event::AppendEntriesRequest {
            from: 2,
            term: 1,
            prev_log_tx_id: 10,
            prev_log_term: 1,
            entries: LogEntryRange::empty(),
            leader_commit: 5,
        },
    );
    assert_eq!(node.cluster_commit_index(), 5);

    // Second AE: deliberately mismatched prev_log_term, forcing
    // truncation to tx_id 1 — strictly below cluster_commit_index=5.
    // Triggers the debug_assert.
    let _ = node.step(
        now,
        Event::AppendEntriesRequest {
            from: 2,
            term: 1,
            prev_log_tx_id: 2,
            prev_log_term: 99,
            entries: LogEntryRange::empty(),
            leader_commit: 5,
        },
    );
}

// ─── T6: RaftConfig::validate rejects unsafe configurations ──────────────
//
// `validate()` is also unit-tested inline in `node.rs`; the integration
// path here checks that `RaftNode::new` panics on a misconfig (so a
// production caller sees the failure at construction, not on the
// first split-vote storm).

#[test]
#[should_panic(expected = "RaftConfig")]
fn raftnode_new_panics_on_misconfigured_heartbeat_interval() {
    use raft::ElectionTimerConfig;

    let cfg = RaftConfig {
        heartbeat_interval: Duration::from_millis(200), // 200 * 2 > min_ms
        election_timer: ElectionTimerConfig {
            min_ms: 150,
            max_ms: 300,
        },
        rpc_timeout: Duration::from_millis(500),
        max_entries_per_append: 64,
    };
    let _ = RaftNode::new(1, vec![1, 2], MemPersistence::new(), cfg, 42);
}

// ─── pending_reply lifecycle (audit gaps) ─────────────────────────────────

/// A `Follower → Candidate` transition (election timeout fires while
/// the follower has a parked AppendEntries success reply waiting on
/// `LogAppendComplete`) discards the parked reply along with the rest
/// of the follower state. The leader will time out its in-flight RPC
/// and retry; nothing in the library leaks the stale reply into the
/// candidate role.
#[test]
fn pending_reply_lost_on_follower_to_candidate_transition() {
    let mut node = make_follower(MemPersistence::new());
    let now = Instant::now();

    // Park a reply by sending non-empty entries.
    let first = node.step(
        now,
        Event::AppendEntriesRequest {
            from: 2,
            term: 1,
            prev_log_tx_id: 0,
            prev_log_term: 0,
            entries: LogEntryRange::new(1, 3, 1),
            leader_commit: 0,
        },
    );
    assert!(first.iter().any(|a| matches!(a, Action::AppendLog { .. })));
    assert!(!first.iter().any(|a| matches!(
        a,
        Action::SendAppendEntriesReply { success: true, .. }
    )));

    // Election timeout fires → transition_to_follower replaces the
    // FollowerState (drops the pending). LogAppendComplete arrives
    // *after* — the now-Candidate has nowhere to send the reply.
    let _ = node.step(now + Duration::from_secs(60), Event::Tick);
    assert_eq!(node.role(), raft::Role::Candidate);

    let after_ack = node.step(
        now + Duration::from_secs(60),
        Event::LogAppendComplete { tx_id: 3 },
    );
    assert!(
        !after_ack.iter().any(|a| matches!(
            a,
            Action::SendAppendEntriesReply { success: true, .. }
        )),
        "candidate must not emit the parked follower reply: {:?}",
        after_ack
    );
}

/// Two AppendEntries arrive in rapid succession before the driver
/// acks the first via `LogAppendComplete`. The follower must
/// overwrite the parked reply with the second one. Replying for the
/// older `last_tx_id` would tell the leader less than what was
/// already shipped to the entry log; the leader's quorum logic is
/// monotonic on success, so the single success reply at the higher
/// `last_tx_id` is correct.
///
/// We model the "leader retransmits with an extended batch before
/// the first ack lands" case: both AEs share `prev_log_tx_id=0`
/// (start of log), so the §5.3 check passes for both even though
/// the follower's `local_log_index` has not yet advanced.
#[test]
fn pending_reply_overwritten_when_second_ae_arrives_before_ack() {
    let mut node = make_follower(MemPersistence::new());
    let now = Instant::now();

    // First AE: prev=0, entries 1..3.
    let _ = node.step(
        now,
        Event::AppendEntriesRequest {
            from: 2,
            term: 1,
            prev_log_tx_id: 0,
            prev_log_term: 0,
            entries: LogEntryRange::new(1, 3, 1),
            leader_commit: 0,
        },
    );

    // Second AE before any ack: same prev=0, but an extended range
    // 1..5. (Models a leader retransmission with one more entry
    // tacked on.)
    let _ = node.step(
        now,
        Event::AppendEntriesRequest {
            from: 2,
            term: 1,
            prev_log_tx_id: 0,
            prev_log_term: 0,
            entries: LogEntryRange::new(1, 5, 1),
            leader_commit: 0,
        },
    );

    // Driver finally acks durability up to 5 (covers the second
    // batch, which subsumes the first).
    let after = node.step(now, Event::LogAppendComplete { tx_id: 5 });
    let replies: Vec<u64> = after
        .iter()
        .filter_map(|a| match a {
            Action::SendAppendEntriesReply {
                success: true,
                last_tx_id,
                ..
            } => Some(*last_tx_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        replies,
        vec![5],
        "only the most recent batch's reply should fire: {:?}",
        after
    );
}

// ─── observe_vote_term I/O failure (audit gap) ───────────────────────────

/// `observe_vote_term` failing on a higher-term RPC is benign because
/// the next durable `vote(...)` call advances `vote_term` itself (the
/// `Persistence` contract bumps the vote-log term inside `vote`). The
/// node does NOT freeze; it stays operable through the recovery path.
/// This pins the contract so a future change cannot quietly turn it
/// into a fatal.
#[test]
fn observe_vote_term_failure_is_recoverable_via_next_vote() {
    let mut persistence = MemPersistence::new();
    persistence.fail_observe_vote_term = true;
    let mut node = make_follower(persistence);
    let now = Instant::now();

    // RV at higher term — observe_vote_term fails inside the handler.
    let actions = node.step(
        now,
        Event::RequestVoteRequest {
            from: 2,
            term: 5,
            last_tx_id: 0,
            last_term: 0,
        },
    );

    // Must not have frozen the node.
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::FatalError { .. })),
        "observe_vote_term failure must not be fatal: {:?}",
        actions
    );
    // The vote() call inside the handler still bumps vote_term (its
    // own write does not depend on observe_vote_term having
    // succeeded), so the reply carries the new term.
    let granted_at_5 = actions.iter().any(|a| matches!(
        a,
        Action::SendRequestVoteReply {
            term: 5,
            granted: true,
            ..
        }
    ));
    assert!(granted_at_5, "vote should still grant at the new term: {:?}", actions);
    assert_eq!(node.current_term(), 5);
    assert_eq!(node.voted_for(), Some(2));
}

// ─── commit_term failure paths at election win (audit gap) ───────────────

/// `commit_term` returning `Err` at election win means the durable
/// state was not modified (per the trait contract). The candidate
/// steps down rather than promoting to leader. This is recoverable —
/// not a fatal — because no on-disk divergence exists.
#[test]
fn commit_term_io_error_at_election_win_steps_down() {
    let mut persistence = MemPersistence::new();
    persistence.fail_commit_term = true;
    let mut node = RaftNode::new(1, vec![1], persistence, RaftConfig::default(), 42);
    let now = Instant::now();

    // Single-node cluster: first Tick arms timer; second Tick after
    // the timeout starts election + immediately commits the term as
    // self-vote = majority. With fail_commit_term the win is
    // refused → step down.
    let _ = node.step(now, Event::Tick);
    let actions = node.step(now + Duration::from_secs(60), Event::Tick);

    assert!(
        !actions.iter().any(|a| matches!(
            a,
            Action::BecomeRole {
                role: raft::Role::Leader,
                ..
            }
        )),
        "must not transition to Leader on commit_term Err: {:?}",
        actions
    );
    assert_ne!(node.role(), raft::Role::Leader);
    // Not a fatal — the node should be running and able to retry.
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::FatalError { .. })));
}

// Note on `commit_term` returning `Ok(false)` at election win: the
// branch exists in `become_leader_after_win`, but is structurally
// unreachable via the public step() API — the `start_election` →
// vote() → become_leader_after_win → commit_term() chain runs inside
// a single step, and `new_term` is always computed as `current + 1`
// where `current = max(term_log, vote_log)`. There is no concurrent
// writer that can bump `term_log` between the vote and the commit
// inside one step. The branch is defense-in-depth for a corrupted
// persistence; the persistence layer's own unit tests
// (`mem_persistence::tests::commit_term_returns_false_when_already_advanced`)
// cover the trait semantics.

// ─── current_term composition (audit gap) ────────────────────────────────

/// `current_term()` returns `max(term_log term, vote_log term)`.
/// Pin the composition explicitly: a vote-log that has raced ahead of
/// the term log (candidate self-voted at a term it has not yet won)
/// is reflected in the public read.
#[test]
fn current_term_returns_max_of_term_log_and_vote_log() {
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 3,
            start_tx_id: 0,
        }],
        7, // vote-log raced ahead by 4 terms
        2,
    );
    let node = RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42);
    assert_eq!(node.current_term(), 7);
}

#[test]
fn current_term_returns_term_log_when_vote_log_is_lower() {
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 9,
            start_tx_id: 0,
        }],
        4,
        0,
    );
    let node = RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42);
    assert_eq!(node.current_term(), 9);
}

// ─── failed flag is volatile (audit gap) ─────────────────────────────────

/// The `failed` flag lives on the volatile `RaftNode` struct, not in
/// `Persistence`. A graceful `into_persistence` + reconstruction (the
/// closest the library models to "process restart with surviving
/// disk") un-fails the node. The driver is expected to give up on
/// the failed instance, but if it spins up a fresh one against the
/// same on-disk state, the new instance starts clean.
#[test]
fn failed_flag_is_volatile_across_into_persistence_rebuild() {
    let mut persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 2,
            start_tx_id: 1,
        }],
        2,
        0,
    );
    persistence.fail_truncate = true;
    let mut node = make_follower(persistence);
    let now = Instant::now();

    let _ = node.step(now, Event::LogAppendComplete { tx_id: 5 });
    let actions = node.step(
        now,
        Event::AppendEntriesRequest {
            from: 2,
            term: 2,
            prev_log_tx_id: 3,
            prev_log_term: 99,
            entries: LogEntryRange::empty(),
            leader_commit: 0,
        },
    );
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::FatalError { .. }
    )));
    // Now the node is frozen.
    assert!(node
        .step(now, Event::Tick)
        .is_empty());

    // Recover the persistence (clear the injected fault before we
    // hand it to the new node — restart implies the underlying I/O
    // problem has been resolved). Build a fresh RaftNode from it.
    let mut persistence = node.into_persistence();
    persistence.fail_truncate = false;
    let mut restarted = make_follower(persistence);

    // The restarted node is NOT frozen — it processes events
    // normally. A simple Tick arms the election timer (initial).
    let actions = restarted.step(now, Event::Tick);
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::FatalError { .. })),
        "restarted node must not be in failed state: {:?}",
        actions
    );
}
