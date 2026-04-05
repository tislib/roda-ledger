use roda_ledger::index::IndexedTxEntry;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::snapshot::{QueryKind, QueryRequest, QueryResponse};
use roda_ledger::transaction::Operation;
use std::time::Duration;

fn temp_ledger() -> Ledger {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    Ledger::new(config)
}

fn query_transaction(ledger: &Ledger, tx_id: u64) -> Option<Vec<IndexedTxEntry>> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    ledger.query(QueryRequest {
        kind: QueryKind::GetTransaction { tx_id },
        respond: Box::new(move |resp| {
            let _ = tx.send(resp);
        }),
    });
    match rx.recv().unwrap() {
        QueryResponse::Transaction(result) => result,
        _ => panic!("unexpected response type"),
    }
}

fn query_account_history(
    ledger: &Ledger,
    account_id: u64,
    from_tx_id: u64,
    limit: usize,
) -> Vec<IndexedTxEntry> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    ledger.query(QueryRequest {
        kind: QueryKind::GetAccountHistory {
            account_id,
            from_tx_id,
            limit,
        },
        respond: Box::new(move |resp| {
            let _ = tx.send(resp);
        }),
    });
    match rx.recv().unwrap() {
        QueryResponse::AccountHistory(result) => result,
        _ => panic!("unexpected response type"),
    }
}

// ── GetTransaction hot path ──────────────────────────────────────────────────

#[test]
fn test_get_transaction_deposit() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    let tx_id = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 500,
        user_ref: 42,
    });
    ledger.wait_for_transaction(tx_id);

    let entries = query_transaction(&ledger, tx_id).expect("transaction should be found");
    // Deposit produces 2 entries: credit from system account, debit to target account
    assert_eq!(entries.len(), 2);
    // One entry should be for account 1
    assert!(entries.iter().any(|e| e.account_id == 1 && e.amount == 500));
}

#[test]
fn test_get_transaction_transfer() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    // Fund account 10 first
    let dep_id = ledger.submit(Operation::Deposit {
        account: 10,
        amount: 1000,
        user_ref: 0,
    });
    ledger.wait_for_transaction(dep_id);

    let tx_id = ledger.submit(Operation::Transfer {
        from: 10,
        to: 20,
        amount: 300,
        user_ref: 99,
    });
    ledger.wait_for_transaction(tx_id);

    let entries = query_transaction(&ledger, tx_id).expect("transaction should be found");
    assert_eq!(entries.len(), 2);
    assert!(
        entries
            .iter()
            .any(|e| e.account_id == 10 && e.amount == 300)
    );
    assert!(
        entries
            .iter()
            .any(|e| e.account_id == 20 && e.amount == 300)
    );
}

#[test]
fn test_get_transaction_miss_returns_none() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    let result = query_transaction(&ledger, 999_999);
    assert!(result.is_none());
}

#[test]
fn test_get_transaction_multiple_sequential() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    let mut ids = Vec::new();
    for i in 0..100u64 {
        ids.push(ledger.submit(Operation::Deposit {
            account: 1 + (i % 10),
            amount: 100 + i,
            user_ref: i,
        }));
    }
    ledger.wait_for_transaction(*ids.last().unwrap());

    for &tx_id in &ids {
        let entries = query_transaction(&ledger, tx_id);
        assert!(entries.is_some(), "tx_id {} should be in hot cache", tx_id);
    }
}

// ── GetAccountHistory hot path ───────────────────────────────────────────────

#[test]
fn test_account_history_single_account() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    let mut last_id = 0;
    for _ in 0..5 {
        last_id = ledger.submit(Operation::Deposit {
            account: 42,
            amount: 100,
            user_ref: 0,
        });
    }
    ledger.wait_for_transaction(last_id);

    let history = query_account_history(&ledger, 42, 0, 10);
    // Each deposit has 2 entries touching account 42 and system account.
    // Account history for 42 should have 5 entries (one per deposit).
    assert_eq!(history.len(), 5);
    // Newest first
    assert!(history[0].tx_id > history[4].tx_id);
    // All entries are for account 42
    assert!(history.iter().all(|e| e.account_id == 42));
}

#[test]
fn test_account_history_limit_respected() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    let mut last_id = 0;
    for _ in 0..20 {
        last_id = ledger.submit(Operation::Deposit {
            account: 7,
            amount: 50,
            user_ref: 0,
        });
    }
    ledger.wait_for_transaction(last_id);

    let history = query_account_history(&ledger, 7, 0, 5);
    assert_eq!(history.len(), 5);
}

#[test]
fn test_account_history_from_tx_id_lower_bound() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    let mut ids = Vec::new();
    for _ in 0..10 {
        ids.push(ledger.submit(Operation::Deposit {
            account: 15,
            amount: 10,
            user_ref: 0,
        }));
    }
    ledger.wait_for_transaction(*ids.last().unwrap());

    // Only entries with tx_id >= ids[5]
    let from_id = ids[5];
    let history = query_account_history(&ledger, 15, from_id, 100);
    assert!(history.iter().all(|e| e.tx_id >= from_id));
    assert_eq!(history.len(), 5);
}

#[test]
fn test_account_history_empty_for_unknown_account() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    let tx_id = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    ledger.wait_for_transaction(tx_id);

    let history = query_account_history(&ledger, 999_999, 0, 10);
    assert!(history.is_empty());
}

#[test]
fn test_account_history_computed_balance_progression() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    let mut last_id = 0;
    for i in 1..=5u64 {
        last_id = ledger.submit(Operation::Deposit {
            account: 33,
            amount: i * 100,
            user_ref: 0,
        });
    }
    ledger.wait_for_transaction(last_id);

    let history = query_account_history(&ledger, 33, 0, 10);
    // Newest first: computed_balance should be decreasing as we go back in time
    for i in 0..history.len() - 1 {
        assert!(
            history[i].computed_balance >= history[i + 1].computed_balance,
            "balance should be monotonically non-decreasing going forward in time"
        );
    }
    // Latest balance should be sum(100+200+300+400+500) = 1500
    assert_eq!(history[0].computed_balance, 1500);
}

// ── Read-your-own-writes consistency ─────────────────────────────────────────

#[test]
fn test_read_your_own_writes_consistency() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    // Submit and immediately query — the single queue guarantees ordering.
    let tx_id = ledger.submit(Operation::Deposit {
        account: 50,
        amount: 777,
        user_ref: 0,
    });
    ledger.wait_for_transaction(tx_id);

    // The query goes through the same queue, so it must see the deposit.
    let entries = query_transaction(&ledger, tx_id);
    assert!(
        entries.is_some(),
        "read-your-own-writes: should see the deposit"
    );
}
