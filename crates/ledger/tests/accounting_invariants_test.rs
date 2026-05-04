use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::Operation;
use std::time::Duration;
use storage::entities::FailReason;

/// After 10K mixed operations, sum of all account balances (including system account 0) must be 0.
#[test]
fn test_global_zero_sum() {
    let max_accounts = 100usize;
    let config = LedgerConfig {
        max_accounts,
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut last_id = 0u64;

    for _ in 0..10_000 {
        let op = match rng.gen_range(0..3) {
            0 => Operation::Deposit {
                account: rng.gen_range(1..20u64),
                amount: rng.gen_range(1..100),
                user_ref: 0,
            },
            1 => {
                let from = rng.gen_range(1..20u64);
                let mut to = rng.gen_range(1..20u64);
                while to == from {
                    to = rng.gen_range(1..20u64);
                }
                Operation::Transfer {
                    from,
                    to,
                    amount: rng.gen_range(1..10),
                    user_ref: 0,
                }
            }
            _ => Operation::Withdrawal {
                account: rng.gen_range(1..20u64),
                amount: rng.gen_range(1..10),
                user_ref: 0,
            },
        };
        last_id = ledger.submit(op);
    }

    ledger.wait_for_transaction(last_id);

    let mut total: i64 = 0;
    for acct in 0..max_accounts as u64 {
        total += ledger.get_balance(acct);
    }
    assert_eq!(total, 0, "global zero-sum invariant violated");
}

/// Withdraw more than balance -> INSUFFICIENT_FUNDS, balance unchanged.
#[test]
fn test_insufficient_funds_rejected() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    let dep_id = ledger.submit(Operation::Deposit {
        account: 5,
        amount: 100,
        user_ref: 0,
    });
    ledger.wait_for_transaction(dep_id);
    assert_eq!(ledger.get_balance(5), 100);

    let wd_id = ledger.submit(Operation::Withdrawal {
        account: 5,
        amount: 200,
        user_ref: 0,
    });
    ledger.wait_for_transaction(wd_id);

    let status = ledger.get_transaction_status(wd_id);
    assert!(status.is_err(), "withdrawal should have been rejected");
    assert_eq!(status.error_reason(), FailReason::INSUFFICIENT_FUNDS);
    assert_eq!(
        ledger.get_balance(5),
        100,
        "balance must not change on rejection"
    );
}

/// Multiple transfers between funded accounts, verify net-zero per transaction at the balance level.
#[test]
fn test_transfer_conserves_balances() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    // Fund accounts 1..=5
    let mut last_id = 0u64;
    for acct in 1..=5u64 {
        last_id = ledger.submit(Operation::Deposit {
            account: acct,
            amount: 10_000,
            user_ref: 0,
        });
    }
    ledger.wait_for_transaction(last_id);

    // Capture balances before transfers (accounts 0..=5)
    let mut pre_total: i64 = 0;
    for acct in 0..=5 {
        pre_total += ledger.get_balance(acct);
    }

    // 1000 random transfers
    use rand::Rng;
    let mut rng = rand::thread_rng();
    for _ in 0..1_000 {
        let from = rng.gen_range(1..=5u64);
        let mut to = rng.gen_range(1..=5u64);
        while to == from {
            to = rng.gen_range(1..=5u64);
        }
        last_id = ledger.submit(Operation::Transfer {
            from,
            to,
            amount: rng.gen_range(1..=5),
            user_ref: 0,
        });
    }
    ledger.wait_for_transaction(last_id);

    let mut post_total: i64 = 0;
    for acct in 0..=5 {
        post_total += ledger.get_balance(acct);
    }
    assert_eq!(
        pre_total, post_total,
        "transfers must not change total balance"
    );
    assert_eq!(post_total, 0, "total must remain zero");
}

/// Deposit creates a matching credit on the account and debit on the system account.
#[test]
fn test_deposit_creates_matching_debit_credit() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    let id = ledger.submit(Operation::Deposit {
        account: 7,
        amount: 500,
        user_ref: 42,
    });
    ledger.wait_for_transaction(id);

    assert_eq!(ledger.get_balance(7), 500, "account should receive deposit");
    assert_eq!(
        ledger.get_balance(0),
        -500,
        "system account should have matching debit"
    );
}
