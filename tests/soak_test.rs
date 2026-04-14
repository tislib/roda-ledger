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

/// 10M mixed operations with periodic zero-sum checkpoint.
#[test]
#[ignore]
#[allow(unused_assignments)]
fn test_10m_mixed_workload() {
    let max_accounts = 200usize;
    let config = LedgerConfig {
        max_accounts,
        storage: StorageConfig {
            transaction_count_per_segment: 10_000_000,
            snapshot_frequency: 4,
            ..StorageConfig::default()
        },
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    use rand::Rng;
    let mut rng = rand::thread_rng();
    let total_ops = 10_000_000u64;
    let checkpoint_interval = 1_000_000u64;
    let num_accounts = 100u64;

    // Pre-fund accounts
    let mut last_id = 0u64;
    for acct in 1..=num_accounts {
        last_id = ledger.submit(Operation::Deposit {
            account: acct,
            amount: 10_000_000,
            user_ref: 0,
        });
    }
    ledger.wait_for_transaction(last_id);

    for i in 0..total_ops {
        let op = match rng.gen_range(0..4) {
            0 => Operation::Deposit {
                account: rng.gen_range(1..=num_accounts),
                amount: rng.gen_range(1..100),
                user_ref: 0,
            },
            1 | 2 => {
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

        // Periodic checkpoint
        if (i + 1) % checkpoint_interval == 0 {
            ledger.wait_for_transaction(last_id);
            let mut total: i64 = 0;
            for acct in 0..max_accounts as u64 {
                total += ledger.get_balance(acct);
            }
            assert_eq!(
                total,
                0,
                "zero-sum violated at checkpoint {} (after {} ops)",
                (i + 1) / checkpoint_interval,
                i + 1
            );
            eprintln!(
                "Checkpoint {}: zero-sum OK after {} ops",
                (i + 1) / checkpoint_interval,
                i + 1
            );
        }
    }

    ledger.wait_for_transaction(last_id);

    // Final zero-sum check
    let mut total: i64 = 0;
    for acct in 0..max_accounts as u64 {
        total += ledger.get_balance(acct);
    }
    assert_eq!(total, 0, "final zero-sum invariant violated");
}

/// 20 crash-recovery cycles with 50K deposits each.
#[test]
#[ignore]
fn test_20_crash_recovery_cycles() {
    let dir = unique_dir("soak_20_cycles");
    let deposits_per_cycle = 50_000u64;
    let cycles = 20u64;

    for cycle in 0..cycles {
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

        let mut last_id = 0u64;
        for _ in 0..deposits_per_cycle {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);

        let expected = ((cycle + 1) * deposits_per_cycle) as i64;
        assert_eq!(
            ledger.get_balance(1),
            expected,
            "balance wrong at cycle {}",
            cycle
        );
        eprintln!("Cycle {}/{}: balance={} OK", cycle + 1, cycles, expected);
        // Drop = simulated crash
    }

    // Final fresh restart
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
            (cycles * deposits_per_cycle) as i64,
            "final balance after {} crash-recovery cycles",
            cycles
        );
    }

    let _ = fs::remove_dir_all(dir);
}

/// 5M transactions, track memory stability (no unbounded growth).
#[allow(unused_assignments)]
#[test]
#[ignore]
fn test_memory_stability() {
    use sysinfo::{Pid, System};

    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    let pid = Pid::from(std::process::id() as usize);
    let mut sys = System::new();
    let total = 5_000_000u64;
    let mut rss_at = Vec::new();

    let mut last_id = 0u64;
    for i in 0..total {
        last_id = ledger.submit(Operation::Deposit {
            account: (i % 1000) + 1,
            amount: 1,
            user_ref: 0,
        });

        if (i + 1) % 1_000_000 == 0 {
            ledger.wait_for_transaction(last_id);
            sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
            if let Some(process) = sys.process(pid) {
                let rss_mb = process.memory() / (1024 * 1024);
                eprintln!("At {}M ops: RSS = {} MB", (i + 1) / 1_000_000, rss_mb);
                rss_at.push(rss_mb);
            }
        }
    }

    // RSS at 5M should not be more than 3x RSS at 1M (allowing for some growth)
    if rss_at.len() >= 2 {
        let rss_1m = rss_at[0];
        let rss_5m = *rss_at.last().unwrap();
        assert!(
            rss_5m <= rss_1m * 3,
            "memory grew too much: {}MB at 1M -> {}MB at 5M (>3x)",
            rss_1m,
            rss_5m
        );
    }
}
