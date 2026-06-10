use ledger::ledger::{Ledger, LedgerConfig};
use ledger::snapshot::{QueryKind, QueryRequest, QueryResponse};
use ledger::transaction::Operation;
use std::time::Duration;
use storage::entities::WalEntry;

/// Flattened view of a queried balance entry, mirroring the old
/// `IndexedTxEntry` fields so the assertions below read unchanged.
#[derive(Debug)]
#[allow(dead_code)] // mirrors IndexedTxEntry's shape; not every field is asserted
struct Row {
    tx_id: u64,
    account_id: u64,
    amount: u64,
    computed_balance: i64,
}

fn temp_ledger() -> Ledger {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    Ledger::new(config)
}

fn query_transaction(ledger: &Ledger, tx_id: u64) -> Option<Vec<Row>> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    ledger.query(QueryRequest {
        kind: QueryKind::GetTransaction { tx_id },
        respond: Box::new(move |resp| {
            let _ = tx.send(resp);
        }),
    });
    match rx.recv().unwrap() {
        QueryResponse::Transaction(result) => result.map(|r| {
            let tx_id = r.meta.tx_id;
            r.entries
                .iter()
                .filter_map(|e| match e {
                    WalEntry::Entry(x) => Some(Row {
                        tx_id,
                        account_id: x.account_id,
                        amount: x.amount,
                        computed_balance: x.computed_balance,
                    }),
                    _ => None,
                })
                .collect()
        }),
        _ => panic!("unexpected response type"),
    }
}

// ── GetTransaction hot path ──────────────────────────────────────────────────

#[test]
fn test_get_transaction_deposit() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

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
    ledger.open_accounts(100);

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
    ledger.open_accounts(100);

    let result = query_transaction(&ledger, 999_999);
    assert!(result.is_none());
}

#[test]
fn test_get_transaction_multiple_sequential() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

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

// ── Read-your-own-writes consistency ─────────────────────────────────────────

#[test]
fn test_read_your_own_writes_consistency() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

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
