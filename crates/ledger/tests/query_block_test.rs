use ledger::ledger::{Ledger, LedgerConfig};
use ledger::snapshot::{QueryKind, QueryRequest, QueryResponse};
use ledger::transaction::Operation;
use std::time::Duration;

fn temp_ledger() -> Ledger {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    Ledger::new(config)
}

#[test]
fn test_query_block_transaction() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    let tx_id = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 500,
        user_ref: 42,
    });
    ledger.wait_for_transaction(tx_id);

    // Call query_block with same parameters as query would take
    let response = ledger.query_block(QueryRequest {
        kind: QueryKind::GetTransaction { tx_id },
        respond: Box::new(|_| {
            // This is the original respond callback
            println!("Callback called!");
        }),
    });

    match response {
        QueryResponse::Transaction(Some(result)) => {
            assert_eq!(result.entries.len(), 2);
            assert!(
                result
                    .entries
                    .iter()
                    .any(|e| e.account_id == 1 && e.amount == 500)
            );
        }
        _ => panic!("Expected Transaction response"),
    }
}

#[test]
fn test_query_block_account_history() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();

    let tx_id1 = ledger.submit(Operation::Deposit {
        account: 10,
        amount: 1000,
        user_ref: 1,
    });
    let tx_id2 = ledger.submit(Operation::Deposit {
        account: 10,
        amount: 2000,
        user_ref: 2,
    });
    ledger.wait_for_transaction(tx_id2);

    let response = ledger.query_block(QueryRequest {
        kind: QueryKind::GetAccountHistory {
            account_id: 10,
            from_tx_id: 0,
            limit: 10,
        },
        respond: Box::new(|_| {}),
    });

    match response {
        QueryResponse::AccountHistory(entries) => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].tx_id, tx_id2);
            assert_eq!(entries[1].tx_id, tx_id1);
        }
        _ => panic!("Expected AccountHistory response"),
    }
}
