use ledger::ledger::{Ledger, LedgerConfig};
use storage::StorageConfig;
use ledger::transaction::Operation;
use std::fs;
use std::time::Duration;

fn unique_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temp_{}_{}", name, nanos)
}

/// Mixed workload on 20 accounts, restart, all 20 balances must match live state.
#[test]
fn test_replay_identical_balances() {
    let dir = unique_dir("replay_identical");
    let num_accounts = 20u64;

    let live_balances: Vec<i64>;

    // Phase 1: diverse workload
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 100,
                snapshot_frequency: 1,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut last_id = 0u64;

        // Pre-fund accounts
        for acct in 1..=num_accounts {
            last_id = ledger.submit(Operation::Deposit {
                account: acct,
                amount: 100_000,
                user_ref: 0,
            });
        }

        // Mixed operations
        for _ in 0..50_000 {
            let op = match rng.gen_range(0..3) {
                0 => Operation::Deposit {
                    account: rng.gen_range(1..=num_accounts),
                    amount: rng.gen_range(1..100),
                    user_ref: 0,
                },
                1 => {
                    let from = rng.gen_range(1..=num_accounts);
                    let mut to = rng.gen_range(1..=num_accounts);
                    while to == from {
                        to = rng.gen_range(1..=num_accounts);
                    }
                    Operation::Transfer {
                        from,
                        to,
                        amount: rng.gen_range(1..10),
                        user_ref: 0,
                    }
                }
                _ => Operation::Withdrawal {
                    account: rng.gen_range(1..=num_accounts),
                    amount: rng.gen_range(1..5),
                    user_ref: 0,
                },
            };
            last_id = ledger.submit(op);
        }

        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();

        // Capture live balances
        live_balances = (0..=num_accounts).map(|a| ledger.get_balance(a)).collect();
    }

    // Phase 2: restart and compare
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 100,
                snapshot_frequency: 1,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        for acct in 0..=num_accounts {
            let replayed = ledger.get_balance(acct);
            assert_eq!(
                replayed, live_balances[acct as usize],
                "balance mismatch for account {} after replay: live={} replayed={}",
                acct, live_balances[acct as usize], replayed
            );
        }
    }

    let _ = fs::remove_dir_all(dir);
}

/// 200K deposits across 50 accounts with 1MB segments. Replay across many segments.
#[test]
fn test_replay_across_many_segments() {
    let dir = unique_dir("replay_many_segs");
    let num_accounts = 50u64;
    let total_deposits = 200_000u64;
    let deposits_per_account = total_deposits / num_accounts;

    let live_balances: Vec<i64>;

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 100,
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let mut last_id = 0u64;
        for i in 0..total_deposits {
            let acct = (i % num_accounts) + 1;
            last_id = ledger.submit(Operation::Deposit {
                account: acct,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();

        live_balances = (0..=num_accounts).map(|a| ledger.get_balance(a)).collect();

        // Verify expected deposits per account
        for acct in 1..=num_accounts {
            assert_eq!(
                live_balances[acct as usize], deposits_per_account as i64,
                "unexpected live balance for account {}",
                acct
            );
        }
    }

    // Restart and compare
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 100,
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        for acct in 0..=num_accounts {
            assert_eq!(
                ledger.get_balance(acct),
                live_balances[acct as usize],
                "replay mismatch for account {}",
                acct
            );
        }
    }

    let _ = fs::remove_dir_all(dir);
}

/// Deposits + failing withdrawals, restart, balance matches.
#[test]
fn test_replay_with_rejections() {
    let dir = unique_dir("replay_rejections");

    let live_balance: i64;

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 100,
                snapshot_frequency: 1,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        // Deposit 1000 into account 1
        let mut last_id = 0u64;
        for _ in 0..1_000 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);

        // Withdraw 3 each, 500 times. Many will fail once balance depletes.
        for _ in 0..500 {
            last_id = ledger.submit(Operation::Withdrawal {
                account: 1,
                amount: 3,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();

        live_balance = ledger.get_balance(1);
        // Expected: 1000 - (333 * 3) = 1000 - 999 = 1 (333 successful withdrawals)
        assert!(
            (0..=1000).contains(&live_balance),
            "unexpected live balance: {}",
            live_balance
        );
    }

    // Restart and compare
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 100,
                snapshot_frequency: 1,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        assert_eq!(
            ledger.get_balance(1),
            live_balance,
            "replay balance mismatch with rejections"
        );
    }

    let _ = fs::remove_dir_all(dir);
}

/// Snapshot restoration + partial WAL replay produces same state as live.
#[test]
fn test_replay_snapshot_plus_partial_wal() {
    let dir = unique_dir("replay_snap_partial");

    let live_balance: i64;

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 100,
                snapshot_frequency: 1,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        // Submit enough for multiple sealed segments with snapshots
        let mut last_id = 0u64;
        for _ in 0..100_000 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();

        // Submit more (goes into active WAL, not yet sealed/snapshotted)
        for _ in 0..5_000 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);

        live_balance = ledger.get_balance(1);
        assert_eq!(live_balance, 105_000);
    }

    // Restart: should use snapshot + replay active WAL
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 100,
                snapshot_frequency: 1,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        assert_eq!(
            ledger.get_balance(1),
            live_balance,
            "snapshot + partial WAL replay mismatch"
        );
    }

    let _ = fs::remove_dir_all(dir);
}
