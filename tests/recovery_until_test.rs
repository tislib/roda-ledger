//! Phase 2a integration tests for `Ledger::start_with_recovery_until`
//! (ADR-0016 §9 / §10).
//!
//! The hard guarantee being tested: after `start_with_recovery_until(W)`,
//! transactions with `tx_id > W` are physically gone — a subsequent
//! plain `start()` must NOT recover them, and balances must reflect
//! only the surviving prefix.

use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::StorageConfig;
use roda_ledger::transaction::Operation;
use std::fs;
use std::path::Path;
use std::time::Duration;

/// Small segment, **no snapshots** — exercises the multi-segment
/// truncation path through pure WAL replay.
fn small_segment_no_snapshot_config(temp_dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: temp_dir.to_string(),
            transaction_count_per_segment: 50,
            snapshot_frequency: 1_000_000, // effectively never
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    }
}

/// Single-segment config (no snapshots) for the "active wal.bin only"
/// truncation case.
fn single_segment_config(temp_dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: temp_dir.to_string(),
            transaction_count_per_segment: 1_000_000,
            snapshot_frequency: 1,
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    }
}

fn fresh_temp_dir(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = format!("temp_recovery_until_{}_{}", label, nanos);
    if Path::new(&dir).exists() {
        let _ = fs::remove_dir_all(&dir);
    }
    dir
}

#[test]
fn truncated_transactions_are_lost_across_plain_restart() {
    // ── Phase 1: build a ledger with N transactions ───────────────────
    //
    // Cluster invariant under ADR-0016 §10: sealed segments contain
    // only cluster-committed transactions. We model that here by
    // setting `seal_watermark = 100` before submitting tx 101..=200,
    // so segments past tx 100 stay closed-but-unsealed — exactly the
    // state truncation is allowed to operate on.
    let temp_dir = fresh_temp_dir("plain_restart");
    let n: u64 = 200;
    let watermark: u64 = 100;

    {
        let mut ledger = Ledger::new(small_segment_no_snapshot_config(&temp_dir));
        ledger.start().unwrap();

        // 100 deposits to account 1 (tx_id 1..=100). Allow these to
        // seal — they are "cluster-committed" in our test model.
        ledger.set_seal_watermark(100);
        for _ in 0..100 {
            ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        // Wait for tx 100 to be committed before submitting the
        // next batch — otherwise the seal stage might not have
        // processed all the segments containing 1..=100 yet.
        ledger.wait_for_transaction(100);

        // 100 deposits to account 2 (tx 101..=200). seal_watermark
        // stays at 100 so segments containing these txs DO NOT seal.
        let last = ledger.submit_batch(
            (0..100)
                .map(|_| Operation::Deposit {
                    account: 2,
                    amount: 1,
                    user_ref: 0,
                })
                .collect(),
        );
        ledger.wait_for_transaction(last);

        assert_eq!(ledger.last_commit_id(), n);
        assert_eq!(ledger.get_balance(1), 100);
        assert_eq!(ledger.get_balance(2), 100);
    }

    // ── Phase 2: re-open with start_with_recovery_until(watermark) ────
    {
        let mut ledger = Ledger::new(small_segment_no_snapshot_config(&temp_dir));
        ledger.start_with_recovery_until(watermark).unwrap();

        assert_eq!(
            ledger.last_commit_id(),
            watermark,
            "commit index must be clamped to watermark"
        );
        assert_eq!(
            ledger.get_balance(1),
            100,
            "account 1 took its 100 deposits before watermark and must survive"
        );
        assert_eq!(
            ledger.get_balance(2),
            0,
            "account 2's deposits were all past the watermark and must be gone"
        );
    }

    // ── Phase 3: plain start() must observe the truncation as durable ──
    // This is the strongest assertion: nothing about start_with_recovery_until's
    // bounded replay can be "remembered" — the WAL on disk simply does not
    // contain anything past the watermark anymore.
    {
        let mut ledger = Ledger::new(small_segment_no_snapshot_config(&temp_dir));
        ledger.start().unwrap();

        assert_eq!(
            ledger.last_commit_id(),
            watermark,
            "after a plain restart, commit index must STILL be at the watermark — \
             truncated transactions are gone from disk forever"
        );
        assert_eq!(
            ledger.get_balance(1),
            100,
            "account 1 still 100 on cold boot"
        );
        assert_eq!(
            ledger.get_balance(2),
            0,
            "account 2 still 0 on cold boot — truncation is durable"
        );

        // Submit one more transaction. Sequencer must continue from
        // watermark + 1 (= 101), not from N + 1 (= 201).
        let next = ledger.submit(Operation::Deposit {
            account: 3,
            amount: 7,
            user_ref: 0,
        });
        assert_eq!(
            next,
            watermark + 1,
            "sequencer must resume from watermark + 1, not from the original N + 1"
        );
        ledger.wait_for_transaction(next);
        assert_eq!(ledger.get_balance(3), 7);
        assert_eq!(ledger.last_commit_id(), watermark + 1);
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn truncation_with_no_segment_rotation_active_only() {
    // Single big segment — exercises the active-wal.bin truncation path
    // without snapshot involvement.
    let temp_dir = fresh_temp_dir("active_only");
    let n: u64 = 50;
    let watermark: u64 = 30;

    {
        let mut ledger = Ledger::new(single_segment_config(&temp_dir));
        ledger.start().unwrap();
        for i in 0..n {
            ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
            // Mix in a transfer occasionally so balances are non-trivial
            if i == 25 {
                ledger.submit(Operation::Deposit {
                    account: 2,
                    amount: 100,
                    user_ref: 0,
                });
            }
        }
        ledger.wait_for_transaction(n + 1);
        assert_eq!(ledger.last_commit_id(), n + 1);
        assert_eq!(ledger.get_balance(1), n as i64);
        assert_eq!(ledger.get_balance(2), 100);
    }

    {
        let mut ledger = Ledger::new(single_segment_config(&temp_dir));
        ledger.start_with_recovery_until(watermark).unwrap();
        assert_eq!(ledger.last_commit_id(), watermark);
        // First 26 deposits to account 1 (tx 1..=26), then a deposit to
        // account 2 (tx 27), then 3 more to account 1 (tx 28..=30).
        // So at watermark=30: account 1 has 29, account 2 has 100.
        assert_eq!(ledger.get_balance(1), 29);
        assert_eq!(ledger.get_balance(2), 100);
    }

    // Plain restart confirms durability.
    {
        let mut ledger = Ledger::new(single_segment_config(&temp_dir));
        ledger.start().unwrap();
        assert_eq!(ledger.last_commit_id(), watermark);
        assert_eq!(ledger.get_balance(1), 29);
        assert_eq!(ledger.get_balance(2), 100);
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn watermark_above_last_tx_is_a_noop() {
    // start_with_recovery_until with a watermark beyond what we have
    // should behave exactly like plain start() — no truncation, no
    // clamping.
    let temp_dir = fresh_temp_dir("noop");
    let n: u64 = 30;

    {
        let mut ledger = Ledger::new(single_segment_config(&temp_dir));
        ledger.start().unwrap();
        for _ in 0..n {
            ledger.submit(Operation::Deposit {
                account: 1,
                amount: 2,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(n);
        assert_eq!(ledger.last_commit_id(), n);
        assert_eq!(ledger.get_balance(1), (n * 2) as i64);
    }

    {
        let mut ledger = Ledger::new(single_segment_config(&temp_dir));
        ledger
            .start_with_recovery_until(10_000) // far above n
            .unwrap();
        assert_eq!(
            ledger.last_commit_id(),
            n,
            "watermark above last_tx must clamp to last_tx, not the watermark"
        );
        assert_eq!(ledger.get_balance(1), (n * 2) as i64);
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn start_with_recovery_until_errors_when_sealed_segment_would_need_truncation() {
    // The cluster invariant from ADR-0016 §10 says a sealed segment
    // never contains tx beyond the cluster-commit watermark, so a
    // recovery watermark always lands in the active or closed-but-
    // unsealed tail. To test the failure path we deliberately violate
    // that invariant: leave `seal_watermark = u64::MAX` (default), let
    // segments seal freely, then ask `start_with_recovery_until` to
    // cut into a sealed segment. It must refuse rather than silently
    // rewrite committed history.
    let temp_dir = fresh_temp_dir("sealed_violation");

    {
        let mut ledger = Ledger::new(small_segment_no_snapshot_config(&temp_dir));
        ledger.start().unwrap();
        // Default seal_watermark = u64::MAX → segments seal freely.
        for _ in 0..150 {
            ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(150);
        // Give the seal thread a couple ticks to actually seal the
        // closed segments. seal_check_internal = 1ms.
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    // Asking for a watermark inside the second segment should fail
    // because that segment is sealed and we refuse to mutate it.
    {
        let mut ledger = Ledger::new(small_segment_no_snapshot_config(&temp_dir));
        let err = ledger
            .start_with_recovery_until(75)
            .expect_err("must refuse to truncate inside a sealed segment");
        let msg = format!("{}", err);
        assert!(
            msg.contains("sealed segment"),
            "error message should explain why: {}",
            msg
        );
        assert!(
            msg.contains("ADR-0016") || msg.contains("seal_watermark"),
            "error should reference the invariant: {}",
            msg
        );
    }

    // The data dir should be unchanged — plain start() still works
    // and recovers the full original log. No silent mutation.
    {
        let mut ledger = Ledger::new(small_segment_no_snapshot_config(&temp_dir));
        ledger.start().unwrap();
        assert_eq!(ledger.last_commit_id(), 150);
        assert_eq!(ledger.get_balance(1), 150);
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn watermark_at_zero_drops_everything() {
    // Aggressive case — watermark 0 means "throw away the entire log".
    // The sequencer should restart at 1.
    let temp_dir = fresh_temp_dir("zero");

    {
        let mut ledger = Ledger::new(single_segment_config(&temp_dir));
        ledger.start().unwrap();
        for _ in 0..20 {
            ledger.submit(Operation::Deposit {
                account: 9,
                amount: 5,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(20);
        assert_eq!(ledger.last_commit_id(), 20);
        assert_eq!(ledger.get_balance(9), 100);
    }

    {
        let mut ledger = Ledger::new(single_segment_config(&temp_dir));
        ledger.start_with_recovery_until(0).unwrap();
        assert_eq!(ledger.last_commit_id(), 0);
        assert_eq!(ledger.get_balance(9), 0);

        let next = ledger.submit(Operation::Deposit {
            account: 9,
            amount: 7,
            user_ref: 0,
        });
        assert_eq!(next, 1);
        ledger.wait_for_transaction(1);
        assert_eq!(ledger.get_balance(9), 7);
    }

    // Plain restart from this state.
    {
        let mut ledger = Ledger::new(single_segment_config(&temp_dir));
        ledger.start().unwrap();
        assert_eq!(ledger.last_commit_id(), 1);
        assert_eq!(ledger.get_balance(9), 7);
    }

    let _ = fs::remove_dir_all(&temp_dir);
}
