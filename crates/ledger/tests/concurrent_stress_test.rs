use ledger::ledger::{Ledger, LedgerConfig};

use ledger::transaction::Operation;
use std::sync::Arc;
use std::time::Duration;

/// 8 threads x 50K deposits of 1 to the same account. Final balance must equal 400K.
#[test]
fn test_concurrent_deposits_final_balance() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();
    let ledger = Arc::new(ledger);

    let num_threads = 8;
    let ops_per_thread = 50_000u64;
    let account = 1u64;

    let mut handles = Vec::new();
    for _ in 0..num_threads {
        let l = ledger.clone();
        handles.push(std::thread::spawn(move || {
            let mut last = 0u64;
            for _ in 0..ops_per_thread {
                last = l.submit(Operation::Deposit {
                    account,
                    amount: 1,
                    user_ref: 0,
                });
            }
            last
        }));
    }

    let max_id = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .max()
        .unwrap();

    ledger.wait_for_transaction(max_id);

    let expected = (num_threads * ops_per_thread) as i64;
    assert_eq!(ledger.get_balance(account), expected);
    assert_eq!(ledger.get_balance(0), -expected);
}

/// Pre-fund 10 accounts, 8 threads do random transfers. Sum of all balances == 0.
#[test]
fn test_concurrent_transfers_conserve_total() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();
    let ledger = Arc::new(ledger);

    let num_accounts = 10u64;
    let fund_amount = 100_000u64;

    // Pre-fund accounts 1..=10
    let mut last_id = 0u64;
    for acct in 1..=num_accounts {
        last_id = ledger.submit(Operation::Deposit {
            account: acct,
            amount: fund_amount,
            user_ref: 0,
        });
    }
    ledger.wait_for_transaction(last_id);

    let num_threads = 8;
    let ops_per_thread = 10_000u64;

    let mut handles = Vec::new();
    for t in 0..num_threads {
        let l = ledger.clone();
        handles.push(std::thread::spawn(move || {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let mut last = 0u64;
            for _ in 0..ops_per_thread {
                let from = rng.gen_range(1..=num_accounts);
                let mut to = rng.gen_range(1..=num_accounts);
                while to == from {
                    to = rng.gen_range(1..=num_accounts);
                }
                let amount = rng.gen_range(1..=10u64);
                last = l.submit(Operation::Transfer {
                    from,
                    to,
                    amount,
                    user_ref: t as u64,
                });
            }
            last
        }));
    }

    let max_id = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .max()
        .unwrap();

    ledger.wait_for_transaction(max_id);

    // Sum of all balances (system account 0 + accounts 1..=10) must be zero
    let mut total: i64 = 0;
    for acct in 0..=num_accounts {
        total += ledger.get_balance(acct);
    }
    assert_eq!(total, 0, "zero-sum invariant violated");
}

/// 4 deposit + 2 transfer + 2 withdrawal threads, verify final state consistency.
#[test]
fn test_concurrent_mixed_operations() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();
    let ledger = Arc::new(ledger);

    // Pre-fund accounts 1..=5
    let mut last_id = 0u64;
    for acct in 1..=5u64 {
        last_id = ledger.submit(Operation::Deposit {
            account: acct,
            amount: 1_000_000,
            user_ref: 0,
        });
    }
    ledger.wait_for_transaction(last_id);

    let ops_per_thread = 10_000u64;
    let mut handles = Vec::new();

    // 4 deposit threads
    for _ in 0..4 {
        let l = ledger.clone();
        handles.push(std::thread::spawn(move || {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let mut last = 0u64;
            for _ in 0..ops_per_thread {
                last = l.submit(Operation::Deposit {
                    account: rng.gen_range(1..=5),
                    amount: 1,
                    user_ref: 0,
                });
            }
            last
        }));
    }

    // 2 transfer threads
    for _ in 0..2 {
        let l = ledger.clone();
        handles.push(std::thread::spawn(move || {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let mut last = 0u64;
            for _ in 0..ops_per_thread {
                let from = rng.gen_range(1..=5u64);
                let mut to = rng.gen_range(1..=5u64);
                while to == from {
                    to = rng.gen_range(1..=5u64);
                }
                last = l.submit(Operation::Transfer {
                    from,
                    to,
                    amount: 1,
                    user_ref: 0,
                });
            }
            last
        }));
    }

    // 2 withdrawal threads (small amounts, some may fail)
    for _ in 0..2 {
        let l = ledger.clone();
        handles.push(std::thread::spawn(move || {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let mut last = 0u64;
            for _ in 0..ops_per_thread {
                last = l.submit(Operation::Withdrawal {
                    account: rng.gen_range(1..=5),
                    amount: 1,
                    user_ref: 0,
                });
            }
            last
        }));
    }

    let max_id = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .max()
        .unwrap();

    ledger.wait_for_transaction(max_id);

    // Zero-sum invariant across all accounts including system account 0
    let mut total: i64 = 0;
    for acct in 0..=5 {
        total += ledger.get_balance(acct);
    }
    assert_eq!(
        total, 0,
        "zero-sum invariant violated under mixed concurrent load"
    );
}

/// 4 threads collect 10K tx IDs each. Merged list must be unique and contiguous from 1..N.
#[test]
fn test_concurrent_monotonic_tx_ids() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();
    let ledger = Arc::new(ledger);

    let num_threads = 4;
    let ops_per_thread = 10_000usize;

    let mut handles = Vec::new();
    for _ in 0..num_threads {
        let l = ledger.clone();
        handles.push(std::thread::spawn(move || {
            let mut ids = Vec::with_capacity(ops_per_thread);
            for _ in 0..ops_per_thread {
                ids.push(l.submit(Operation::Deposit {
                    account: 1,
                    amount: 1,
                    user_ref: 0,
                }));
            }
            ids
        }));
    }

    let mut all_ids: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    all_ids.sort();

    let total = num_threads * ops_per_thread;
    assert_eq!(all_ids.len(), total);
    // No duplicates
    all_ids.dedup();
    assert_eq!(all_ids.len(), total, "duplicate tx IDs found");
    // Contiguous from 1..=total
    assert_eq!(all_ids[0], 1);
    assert_eq!(*all_ids.last().unwrap(), total as u64);
}

/// Tiny queue_size=8 with 4 threads x 10K deposits. Backpressure must not lose transactions.
#[test]
fn test_concurrent_small_queue_backpressure() {
    let config = LedgerConfig {
        queue_size: 8,
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();
    let ledger = Arc::new(ledger);

    let num_threads = 4;
    let ops_per_thread = 10_000u64;

    let mut handles = Vec::new();
    for _ in 0..num_threads {
        let l = ledger.clone();
        handles.push(std::thread::spawn(move || {
            let mut last = 0u64;
            for _ in 0..ops_per_thread {
                last = l.submit(Operation::Deposit {
                    account: 1,
                    amount: 1,
                    user_ref: 0,
                });
            }
            last
        }));
    }

    let max_id = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .max()
        .unwrap();

    ledger.wait_for_transaction(max_id);

    let expected = (num_threads * ops_per_thread) as i64;
    assert_eq!(
        ledger.get_balance(1),
        expected,
        "transactions lost under backpressure"
    );
}
