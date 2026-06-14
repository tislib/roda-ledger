use ledger::ledger::{Ledger, LedgerConfig};
use ledger::snapshot::{QueryKind, QueryRequest, QueryResponse};
use ledger::transactor::transaction::Operation;
use std::fs;
use std::time::Duration;
use storage::StorageConfig;
use storage::entities::{FailReason, TxLinkKind, WalEntry};

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
            snapshot_frequency: 1,
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    }
}

fn temp_ledger() -> Ledger {
    Ledger::new(LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    })
}

/// A duplicate-detected link, flattened so the assertions below read unchanged.
struct LinkRow {
    kind: TxLinkKind,
    to_tx_id: u64,
}

/// Split view of a queried transaction: balance entries (counted) and links.
struct TxView {
    entries: Vec<()>,
    links: Vec<LinkRow>,
}

fn query_transaction(ledger: &Ledger, tx_id: u64) -> Option<TxView> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    ledger.query(QueryRequest {
        kind: QueryKind::GetTransaction { tx_id },
        respond: Box::new(move |resp| {
            let _ = tx.send(resp);
        }),
    });
    match rx.recv().unwrap() {
        QueryResponse::Transaction(result) => result.map(|r| {
            let mut view = TxView {
                entries: Vec::new(),
                links: Vec::new(),
            };
            for e in &r.entries {
                match e {
                    WalEntry::Entry(_) => view.entries.push(()),
                    WalEntry::Link(l) => view.links.push(LinkRow {
                        kind: l.kind(),
                        to_tx_id: l.to_tx_id,
                    }),
                    _ => {}
                }
            }
            view
        }),
        _ => panic!("unexpected response type"),
    }
}

// ── Basic dedup tests ────────────────────────────────────────────────────────

/// Duplicate submission is rejected and only original affects balance.
#[test]
fn test_dedup_rejects_duplicate_deposit() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

    let _id1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 500,
        user_ref: 100,
    });
    let result2 = ledger.submit_and_wait_result(Operation::Deposit {
        account: 1,
        amount: 500,
        user_ref: 100,
    });
    assert!(result2.is_err(), "second tx should be ERROR");
    assert_eq!(result2.get_fail_reason(), FailReason::DUPLICATE);
    assert_eq!(
        ledger.get_balance(1),
        500,
        "only first deposit should commit"
    );
    assert!(result2.is_err(), "second tx should be ERROR");
    assert_eq!(result2.get_fail_reason(), FailReason::DUPLICATE);
}

/// Duplicate submission produces a TxLink { kind: Duplicate } pointing to original.
#[test]
fn test_dedup_creates_duplicate_link() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

    let id1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 200,
    });
    let id2 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 200,
    });
    ledger.wait_for_transaction(id2);

    // Query the duplicate transaction — it should have a Duplicate link
    let result = query_transaction(&ledger, id2).expect("duplicate tx should be in index");
    assert_eq!(result.entries.len(), 0, "duplicate has no balance entries");
    assert_eq!(result.links.len(), 1, "duplicate should have one link");
    assert_eq!(result.links[0].kind, TxLinkKind::Duplicate);
    assert_eq!(
        result.links[0].to_tx_id, id1,
        "link should point to original tx"
    );
}

/// Different user_refs are not treated as duplicates.
#[test]
fn test_different_user_refs_not_deduped() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

    let result1 = ledger.submit_and_wait_result(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 1,
    });
    let result2 = ledger.submit_and_wait_result(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 2,
    });

    assert_eq!(ledger.get_balance(1), 200);
    assert!(!result1.is_err());
    assert!(!result2.is_err());
}

/// Multiple retries with same user_ref all get rejected.
#[test]
fn test_dedup_multiple_retries() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

    let id1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1000,
        user_ref: 42,
    });
    let id2 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1000,
        user_ref: 42,
    });
    let id3 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1000,
        user_ref: 42,
    });
    ledger.wait_for_transaction(id3);

    assert_eq!(ledger.get_balance(1), 1000, "only first should commit");

    // Both duplicates should link back to original
    let r2 = query_transaction(&ledger, id2).unwrap();
    assert_eq!(r2.links[0].to_tx_id, id1);
    let r3 = query_transaction(&ledger, id3).unwrap();
    assert_eq!(r3.links[0].to_tx_id, id1);
}

/// Dedup works for transfers too.
#[test]
fn test_dedup_transfer() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

    // Fund account 1
    let fund = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1000,
        user_ref: 0,
    });
    ledger.wait_for_transaction(fund);

    let _id1 = ledger.submit(Operation::Transfer {
        from: 1,
        to: 2,
        amount: 300,
        user_ref: 55,
    });
    let result2 = ledger.submit_and_wait_result(Operation::Transfer {
        from: 1,
        to: 2,
        amount: 300,
        user_ref: 55, // duplicate
    });

    assert_eq!(
        ledger.get_balance(1),
        700,
        "only one transfer should happen"
    );
    assert_eq!(ledger.get_balance(2), 300);
    assert_eq!(result2.get_fail_reason(), FailReason::DUPLICATE);
}

/// Dedup works for withdrawals.
#[test]
fn test_dedup_withdrawal() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

    let fund = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1000,
        user_ref: 0,
    });
    ledger.wait_for_transaction(fund);

    let _id1 = ledger.submit(Operation::Withdrawal {
        account: 1,
        amount: 200,
        user_ref: 77,
    });
    let id2 = ledger.submit(Operation::Withdrawal {
        account: 1,
        amount: 200,
        user_ref: 77, // duplicate
    });
    ledger.wait_for_transaction(id2);

    assert_eq!(
        ledger.get_balance(1),
        800,
        "only one withdrawal should happen"
    );
}

// ── Dedup with restart / crash recovery ���─────────────────────────────────────

/// After restart, dedup cache is rebuilt from WAL and still rejects duplicates.
#[test]
fn test_dedup_survives_restart() {
    let dir = unique_dir("dedup_restart");

    let id1;
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();
        ledger.open_accounts(100);

        id1 = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 500,
            user_ref: 999,
        });
        ledger.wait_for_transaction(id1);
    }

    // Restart
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();
        ledger.open_accounts(100);

        // Same user_ref should be rejected after restart
        let result2 = ledger.submit_and_wait_result(Operation::Deposit {
            account: 1,
            amount: 500,
            user_ref: 999,
        });

        assert_eq!(
            ledger.get_balance(1),
            500,
            "duplicate after restart should be rejected"
        );
        assert_eq!(result2.get_fail_reason(), FailReason::DUPLICATE);
    }

    let _ = fs::remove_dir_all(dir);
}

/// After restart with seal, dedup cache is rebuilt and rejects duplicates.
/// All transactions fit in one segment so the target tx is always in the
/// active WAL on restart, and dedup recovery finds it.
#[test]
fn test_dedup_survives_restart_with_seal() {
    let dir = unique_dir("dedup_seal");

    let id1;
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();
        ledger.open_accounts(100);

        // Submit some filler transactions (all fit in one segment with count=100)
        for i in 0..50 {
            ledger.submit(Operation::Deposit {
                account: i % 10,
                amount: 1,
                user_ref: 0, // user_ref=0 bypasses dedup
            });
        }

        id1 = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 500,
            user_ref: 888,
        });
        ledger.wait_for_transaction(id1);
    }

    // Restart — dedup cache rebuilt from active WAL
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();
        ledger.open_accounts(100);

        let id2 = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 500,
            user_ref: 888,
        });
        ledger.wait_for_transaction(id2);

        // The target tx is in the active WAL, so dedup recovery
        // should detect it and reject the duplicate.
        let balance = ledger.get_balance(1);
        assert!(
            balance <= 510,
            "balance {} suggests duplicate was not properly handled",
            balance
        );
    }

    let _ = fs::remove_dir_all(dir);
}

// ── Dedup with different operation types ─────────────────────────────────────

/// Dedup works across operation types — same user_ref on different ops.
#[test]
fn test_dedup_cross_operation_types() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

    // First: deposit with user_ref=50
    let fund = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1000,
        user_ref: 0,
    });
    ledger.wait_for_transaction(fund);

    let _id1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 50,
    });
    // Second: withdrawal with same user_ref=50 — should be deduped
    let result2 = ledger.submit_and_wait_result(Operation::Withdrawal {
        account: 1,
        amount: 100,
        user_ref: 50,
    });

    assert_eq!(
        ledger.get_balance(1),
        1100,
        "withdrawal with same user_ref should be rejected"
    );
    assert_eq!(result2.get_fail_reason(), FailReason::DUPLICATE);
}
// ��─ Link query tests ─────────────────────────────────────────────────────────

/// Original transaction has no links.
#[test]
fn test_original_tx_has_no_links() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

    let id1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 300,
    });
    ledger.wait_for_transaction(id1);

    let result = query_transaction(&ledger, id1).expect("should find tx");
    assert_eq!(
        result.entries.len(),
        2,
        "deposit has 2 entries (debit+credit)"
    );
    assert!(result.links.is_empty(), "original tx should have no links");
}

/// Duplicate transaction can be queried and links are returned.
#[test]
fn test_duplicate_tx_queryable_with_link() {
    let mut ledger = temp_ledger();
    ledger.start().unwrap();
    ledger.open_accounts(100);

    let id1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 400,
    });
    let id2 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 400,
    });
    ledger.wait_for_transaction(id2);

    // Duplicate is queryable
    let dup = query_transaction(&ledger, id2).expect("duplicate should be queryable");
    assert_eq!(dup.entries.len(), 0);
    assert_eq!(dup.links.len(), 1);
    assert_eq!(dup.links[0].kind, TxLinkKind::Duplicate);
    assert_eq!(dup.links[0].to_tx_id, id1);

    // Original is still queryable and has no links
    let orig = query_transaction(&ledger, id1).expect("original should be queryable");
    assert_eq!(orig.entries.len(), 2);
    assert!(orig.links.is_empty());
}

// ── Restart scenarios ────────────────────────────────────────────────────────

/// Duplicate detection works after multiple restarts.
#[test]
fn test_dedup_multiple_restarts() {
    let dir = unique_dir("dedup_multi_restart");

    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();
        ledger.open_accounts(100);
        let id = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 100,
            user_ref: 777,
        });
        ledger.wait_for_transaction(id);
    }

    // Restart 3 times, each time try to submit duplicate
    for restart in 0..3 {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();
        ledger.open_accounts(100);

        let dup_id = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 100,
            user_ref: 777,
        });
        ledger.wait_for_transaction(dup_id);

        assert_eq!(
            ledger.get_balance(1),
            100,
            "restart #{}: balance should remain 100",
            restart + 1
        );
    }

    let _ = fs::remove_dir_all(dir);
}

/// New user_ref after restart is accepted.
#[test]
fn test_new_user_ref_after_restart() {
    let dir = unique_dir("dedup_new_after_restart");

    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();
        ledger.open_accounts(100);
        let id = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 100,
            user_ref: 111,
        });
        ledger.wait_for_transaction(id);
    }

    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();
        ledger.open_accounts(100);

        // Different user_ref should succeed
        let result2 = ledger.submit_and_wait_result(Operation::Deposit {
            account: 1,
            amount: 200,
            user_ref: 222,
        });

        assert_eq!(ledger.get_balance(1), 300);
        assert!(!result2.is_err());
    }

    let _ = fs::remove_dir_all(dir);
}
