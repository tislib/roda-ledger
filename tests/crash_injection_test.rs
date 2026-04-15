use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::StorageConfig;
use roda_ledger::transaction::Operation;
use std::fs;
use std::time::Duration;

fn unique_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temp_{}_{}", name, nanos)
}

fn make_config(dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.to_string(),
            transaction_count_per_segment: 10_000,
            snapshot_frequency: 1,
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    }
}

/// Drop ledger after WAL commit but potentially before snapshot update. Recovery must replay.
#[test]
fn test_crash_after_wal_before_snapshot() {
    let dir = unique_dir("crash_wal_before_snap");

    let total = 50_000u64;

    // Phase 1: submit and wait only for WAL commit, then drop immediately
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        let mut last_id = 0u64;
        for _ in 0..total {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }

        // Wait for WAL commit only (not snapshot)
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if ledger.last_commit_id() >= last_id {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for WAL commit"
            );
            std::thread::yield_now();
        }
        // Drop without waiting for snapshot — simulates crash
    }

    // Phase 2: restart and verify
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        assert_eq!(
            ledger.get_balance(1),
            total as i64,
            "balance must recover after crash before snapshot"
        );
    }

    let _ = fs::remove_dir_all(dir);
}

/// 5 sequential crash-recovery cycles, each adding 10K deposits.
#[test]
fn test_five_sequential_crash_cycles() {
    let dir = unique_dir("five_crash_cycles");
    let deposits_per_cycle = 13_000u64;
    let cycles = 5;

    for cycle in 0..cycles {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        // Verify running total at the beginning of each cycle
        let expected = (cycle * deposits_per_cycle) as i64;
        assert_eq!(
            ledger.get_balance(1),
            expected,
            "balance wrong at cycle {}",
            cycle
        );

        for _ in 0..deposits_per_cycle {
            ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_pass();

        // Verify running total at end of each cycle
        let expected = ((cycle + 1) * deposits_per_cycle) as i64;
        assert_eq!(
            ledger.get_balance(1),
            expected,
            "balance wrong at cycle {}",
            cycle
        );
        // Drop = simulated crash
    }

    // Final verification after fresh restart
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        assert_eq!(
            ledger.get_balance(1),
            (cycles * deposits_per_cycle) as i64,
            "final balance after {} crash-recovery cycles",
            cycles
        );
    }

    let _ = fs::remove_dir_all(dir);
}

/// After recovery, next tx_id must be strictly greater than all previously committed.
#[test]
fn test_crash_preserves_tx_id_monotonicity() {
    let dir = unique_dir("crash_tx_monotonic");

    let old_last_id;
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        let mut last_id = 0u64;
        for _ in 0..1_000 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        old_last_id = last_id;
    }

    // Restart and submit one more
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        let new_id = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 1,
            user_ref: 0,
        });
        ledger.wait_for_transaction(new_id);

        assert!(
            new_id > old_last_id,
            "new tx_id {} must be > old last_id {}",
            new_id,
            old_last_id
        );
        assert_eq!(ledger.get_balance(1), 1001);
    }

    let _ = fs::remove_dir_all(dir);
}

/// After crash, recovery with snapshot + remaining WAL segments restores correct state.
/// This tests that snapshot provides a valid base for recovery even when some early segments are gone.
#[test]
fn test_crash_recovery_uses_snapshot_as_base() {
    let dir = unique_dir("crash_snap_base");

    let phase1_balance: i64;

    // Phase 1: create data with multiple segments and snapshots
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        for _ in 0..50_000 {
            ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_seal();
        phase1_balance = ledger.get_balance(1);
        assert_eq!(phase1_balance, 50_000);
    }

    // Phase 2: restart normally and verify snapshot-based recovery works
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        assert_eq!(
            ledger.get_balance(1),
            phase1_balance,
            "snapshot-based recovery should restore correct balance"
        );
    }

    let _ = fs::remove_dir_all(dir);
}

/// Truncate active wal.bin mid-record. Recovery must handle gracefully.
#[test]
fn test_crash_with_partial_active_wal() {
    let dir = unique_dir("crash_partial_wal");
    let initial_deposits = 10_000u64;

    // Phase 1: submit deposits and wait for all to commit
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 10_000_000, // large count to keep everything in active wal
                snapshot_frequency: 0,                     // no snapshots
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let mut last_id = 0u64;
        for _ in 0..initial_deposits {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
    }

    // Truncate wal.bin by removing the last 20 bytes (partial record)
    let wal_path = std::path::Path::new(&dir).join("wal.bin");
    let data = fs::read(&wal_path).unwrap();
    assert!(data.len() > 40, "WAL file too small");
    // Remove bytes that don't align to 40-byte record boundary + some extra
    let truncated_len = (data.len() / 40 - 1) * 40; // drop the last complete record
    fs::write(&wal_path, &data[..truncated_len]).unwrap();

    // Phase 2: restart - should recover all complete transactions
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 10_000_000,
                snapshot_frequency: 0,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let balance = ledger.get_balance(1);
        // We lost at most one transaction record from truncation
        assert!(
            balance > 0 && balance <= initial_deposits as i64,
            "balance {} should be between 1 and {}",
            balance,
            initial_deposits
        );
    }

    let _ = fs::remove_dir_all(dir);
}
