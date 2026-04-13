use roda_ledger::index::IndexedTxEntry;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::snapshot::{QueryKind, QueryRequest, QueryResponse};
use roda_ledger::storage::{Storage, StorageConfig};
use roda_ledger::transaction::Operation;
use std::fs;
use std::path::Path;
use std::sync::mpsc::channel;
use std::time::Duration;

fn unique_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temp_{}_{}", name, nanos)
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
        QueryResponse::Transaction(result) => result.map(|r| r.entries),
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

fn cold_storage_config(dir: &str) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_string(),
        wal_segment_size_mb: 1,
        snapshot_frequency: 0,
        ..Default::default()
    }
}

fn cold_ledger_config(dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: cold_storage_config(dir),
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    }
}

/// Large segment, no seal — keeps all data in wal.bin without rotation.
fn no_rotation_config(dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.to_string(),
            wal_segment_size_mb: 2048,
            snapshot_frequency: 0,
            ..Default::default()
        },
        disable_seal: true,
        ..Default::default()
    }
}

// ── Cold tier: index file generation ─────────────────────────────────────────

#[test]
fn test_seal_creates_index_files() {
    let dir = unique_dir("seal_index_files");
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                snapshot_frequency: 2,
                ..cold_storage_config(&dir)
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let mut last_id = 0;
        for i in 0..20_000u64 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1 + (i % 10),
                amount: 100,
                user_ref: i,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    let entries: Vec<_> = fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).collect();
    let tx_indexes: Vec<_> = entries
        .iter()
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("wal_index_") && name.ends_with(".bin")
        })
        .collect();
    let account_indexes: Vec<_> = entries
        .iter()
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("account_index_") && name.ends_with(".bin")
        })
        .collect();

    assert!(
        !tx_indexes.is_empty(),
        "sealed segments should have transaction index files"
    );
    assert!(
        !account_indexes.is_empty(),
        "sealed segments should have account index files"
    );
    assert_eq!(
        tx_indexes.len(),
        account_indexes.len(),
        "each segment should have both index types"
    );

    fs::remove_dir_all(&dir).ok();
}

// ── Cold tier: search_tx_index ───────────────────────────────────────────────

#[test]
fn test_search_tx_index_finds_transaction() {
    let dir = unique_dir("cold_tx_search");
    let mut first_tx_id = 0u64;
    let mut mid_tx_id = 0u64;

    {
        let mut ledger = Ledger::new(cold_ledger_config(&dir));
        ledger.start().unwrap();

        let mut last_id = 0;
        for i in 0..20_000u64 {
            let id = ledger.submit(Operation::Deposit {
                account: 1 + (i % 10),
                amount: 100 + i,
                user_ref: i,
            });
            if i == 0 {
                first_tx_id = id;
            }
            if i == 10_000 {
                mid_tx_id = id;
            }
            last_id = id;
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    let storage = Storage::new(cold_storage_config(&dir)).unwrap();
    let segments = storage.list_all_segments().unwrap();

    let mut found_first = false;
    let mut found_mid = false;

    for mut segment in segments {
        segment.load().unwrap();

        if let Ok(Some(offset)) = segment.search_tx_index(first_tx_id) {
            let tx_entries = segment.read_transaction_at_offset(offset).unwrap();
            assert!(!tx_entries.is_empty(), "should read entries at offset");
            found_first = true;
        }
        if let Ok(Some(offset)) = segment.search_tx_index(mid_tx_id) {
            let tx_entries = segment.read_transaction_at_offset(offset).unwrap();
            assert!(!tx_entries.is_empty());
            found_mid = true;
        }
    }

    assert!(
        found_first,
        "first transaction should be findable in cold tier"
    );
    assert!(found_mid, "mid transaction should be findable in cold tier");

    fs::remove_dir_all(&dir).ok();
}

// ── Cold tier: search_account_index ──────────────────────────────────────────

#[test]
fn test_search_account_index_finds_entries() {
    let dir = unique_dir("cold_account_search");

    {
        let mut ledger = Ledger::new(cold_ledger_config(&dir));
        ledger.start().unwrap();

        let mut last_id = 0;
        for i in 0..20_000u64 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1 + (i % 10),
                amount: 100,
                user_ref: i,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    let storage = Storage::new(cold_storage_config(&dir)).unwrap();
    let segments = storage.list_all_segments().unwrap();

    let mut total_account5_txs = 0usize;
    for segment in &segments {
        if let Ok(tx_ids) = segment.search_account_index(5, 0, 100_000) {
            total_account5_txs += tx_ids.len();
        }
    }

    assert!(
        total_account5_txs > 0,
        "account 5 should have transactions in sealed segments"
    );

    fs::remove_dir_all(&dir).ok();
}

// ── Cold tier: read_transaction_at_offset ────────────────────────────────────

#[test]
fn test_read_transaction_at_offset_correct_entries() {
    let dir = unique_dir("cold_read_offset");

    {
        let mut ledger = Ledger::new(cold_ledger_config(&dir));
        ledger.start().unwrap();

        let mut last_id = 0;
        for i in 0..20_000u64 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1 + (i % 5),
                amount: 100 + i,
                user_ref: i,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    let storage = Storage::new(cold_storage_config(&dir)).unwrap();
    let segments = storage.list_all_segments().unwrap();

    for mut segment in segments {
        segment.load().unwrap();

        if let Ok(tx_ids) = segment.search_account_index(3, 0, 5) {
            for &searched_tx_id in &tx_ids {
                if let Ok(Some(offset)) = segment.search_tx_index(searched_tx_id) {
                    let wal_entries = segment.read_transaction_at_offset(offset).unwrap();
                    assert!(
                        matches!(wal_entries[0], roda_ledger::entities::WalEntry::Metadata(_)),
                        "first entry at offset should be TxMetadata"
                    );
                    for e in &wal_entries[1..] {
                        assert!(
                            matches!(e, roda_ledger::entities::WalEntry::Entry(_)),
                            "subsequent entries should be TxEntry"
                        );
                    }
                }
            }
            if !tx_ids.is_empty() {
                fs::remove_dir_all(&dir).ok();
                return;
            }
        }
    }
    fs::remove_dir_all(&dir).ok();
    panic!("no transactions found in sealed segments for verification");
}

// ── Rotation survival: queries during active segment rotation ────────────────

#[test]
fn test_query_survives_segment_rotation() {
    let dir = unique_dir("rotation_survival");

    let mut ledger = Ledger::new(cold_ledger_config(&dir));
    ledger.start().unwrap();

    let mut all_tx_ids: Vec<u64> = Vec::new();

    for batch in 0..10 {
        let mut batch_ids = Vec::new();
        for i in 0..5_000u64 {
            batch_ids.push(ledger.submit(Operation::Deposit {
                account: 1 + (i % 10),
                amount: 100,
                user_ref: batch * 5_000 + i,
            }));
        }
        let last_id = *batch_ids.last().unwrap();
        ledger.wait_for_transaction(last_id);

        let latest = query_transaction(&ledger, last_id);
        assert!(
            latest.is_some(),
            "batch {}: latest tx {} should be in hot cache after wait",
            batch,
            last_id
        );

        all_tx_ids.extend(batch_ids);
    }

    // After all rotations, the most recent transactions should still be queryable
    let last_10: Vec<u64> = all_tx_ids.iter().rev().take(10).copied().collect();
    for &tx_id in &last_10 {
        let result = query_transaction(&ledger, tx_id);
        assert!(
            result.is_some(),
            "tx {} should still be in hot cache after rotations",
            tx_id
        );
    }

    let history = query_account_history(&ledger, 1, 0, 20);
    assert!(
        !history.is_empty(),
        "account 1 should have history entries after rotations"
    );
    assert!(history.iter().all(|e| e.account_id == 1));

    drop(ledger);
    fs::remove_dir_all(&dir).ok();
}

// ── Pre-seal and post-seal: data accessible via cold tier after seal ─────────

#[test]
#[ignore = "flaky test"]
fn test_cold_tier_accessible_after_seal() {
    let dir = unique_dir("cold_after_seal");

    let mut sealed_tx_ids: Vec<u64> = Vec::new();

    {
        let mut ledger = Ledger::new(cold_ledger_config(&dir));
        ledger.start().unwrap();

        let mut last_id = 0;
        for i in 0..30_000u64 {
            let id = ledger.submit(Operation::Deposit {
                account: 1 + (i % 10),
                amount: 100 + i,
                user_ref: i,
            });
            if i % 5_000 == 0 {
                sealed_tx_ids.push(id);
            }
            last_id = id;
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    // After ledger shutdown, open sealed segments and verify sampled tx_ids
    let storage = Storage::new(cold_storage_config(&dir)).unwrap();

    let mut found_count = 0;
    for &target_tx_id in &sealed_tx_ids {
        for mut segment in storage.list_all_segments().unwrap() {
            segment.load().unwrap();
            if let Ok(Some(offset)) = segment.search_tx_index(target_tx_id) {
                let entries = segment.read_transaction_at_offset(offset).unwrap();
                assert!(!entries.is_empty());
                found_count += 1;
                break;
            }
        }
    }

    assert_eq!(
        found_count,
        sealed_tx_ids.len(),
        "all sampled transactions should be findable in cold tier"
    );

    drop(storage);
    fs::remove_dir_all(&dir).ok();
}

// ── Continuous queries during rotation: no data loss ─────────────────────────

#[test]
fn test_continuous_queries_during_rotation_no_data_loss() {
    let dir = unique_dir("continuous_query_rotation");

    let mut ledger = Ledger::new(cold_ledger_config(&dir));
    ledger.start().unwrap();

    // Submit transactions one at a time, querying each immediately after commit.
    // Read-your-own-writes guarantees no transaction is "lost".
    let total = 30u64;
    let mut query_failures_hot = Vec::new();

    for i in 0..total {
        let tx_id = ledger.submit(Operation::Deposit {
            account: 1 + (i % 10),
            amount: 100,
            user_ref: i,
        });
        ledger.wait_for_transaction(tx_id);

        let result = query_transaction(&ledger, tx_id);
        if result.is_none() {
            query_failures_hot.push(tx_id);
        }
    }

    assert!(
        query_failures_hot.is_empty(),
        "no transaction should be missing from hot cache immediately after commit; \
         failures: {:?}",
        &query_failures_hot[..query_failures_hot.len().min(10)]
    );

    drop(ledger);

    let mut ledger = Ledger::new(cold_ledger_config(&dir));
    ledger.start().unwrap();

    let (s, r) = channel();

    ledger.query(QueryRequest {
        kind: QueryKind::GetTransaction { tx_id: 1 },
        respond: Box::new(move |result| match result {
            QueryResponse::Transaction(tx) => {
                s.send(tx).unwrap();
            }
            _ => unreachable!(),
        }),
    });

    let res = r.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();

    assert_eq!(res.entries.len(), 2);
}

// ── Query on active segment (wal.bin) after restart ─────────────────────────

#[test]
fn test_query_active_segment_after_restart() {
    let dir = unique_dir("query_active_restart");

    let mut tx_ids = Vec::new();
    {
        let mut ledger = Ledger::new(no_rotation_config(&dir));
        ledger.start().unwrap();

        for i in 0..10u64 {
            tx_ids.push(ledger.submit(Operation::Deposit {
                account: 5,
                amount: 100 + i,
                user_ref: i,
            }));
        }
        ledger.wait_for_transaction(*tx_ids.last().unwrap());
    }
    // Ledger dropped — data lives in wal.bin (active segment, never rotated)

    // Reopen from the same directory
    let mut ledger = Ledger::new(no_rotation_config(&dir));
    ledger.start().unwrap();

    // GetTransaction: every submitted tx must be recovered from wal.bin
    for &tx_id in &tx_ids {
        let result = query_transaction(&ledger, tx_id);
        assert!(
            result.is_some(),
            "tx {} should be found on active segment after restart",
            tx_id
        );
        assert_eq!(result.unwrap().len(), 2, "deposit produces 2 entries");
    }

    // GetAccountHistory: account 5 was credited 10 times
    let history = query_account_history(&ledger, 5, 0, 100);
    assert_eq!(
        history.len(),
        10,
        "account 5 should have 10 entries after restart"
    );
    assert!(history.iter().all(|e| e.account_id == 5));
    // Newest first
    for w in history.windows(2) {
        assert!(w[0].tx_id > w[1].tx_id);
    }

    drop(ledger);
    fs::remove_dir_all(&dir).ok();
}

// ── Query on closed-but-not-sealed segment ──────────────────────────────────

#[test]
fn test_query_closed_unsealed_segment_after_restart() {
    let dir = unique_dir("query_closed_unsealed");

    let mut tx_ids = Vec::new();
    {
        let mut ledger = Ledger::new(no_rotation_config(&dir));
        ledger.start().unwrap();

        for i in 0..10u64 {
            tx_ids.push(ledger.submit(Operation::Deposit {
                account: 7,
                amount: 200 + i,
                user_ref: i,
            }));
        }
        ledger.wait_for_transaction(*tx_ids.last().unwrap());
    }
    // Ledger dropped — wal.bin exists with all data

    // Simulate manual close: rename wal.bin → wal_000001.bin
    // (segment 1 is the first active segment id assigned by Storage::new on a fresh dir)
    let wal_active = Path::new(&dir).join("wal.bin");
    let wal_closed = Path::new(&dir).join("wal_000001.bin");
    fs::rename(&wal_active, &wal_closed).expect("should rename active wal to closed");

    // No .seal file exists → segment is CLOSED but not SEALED
    assert!(
        !Path::new(&dir).join("wal_000001.seal").exists(),
        "segment must not be sealed"
    );

    // Reopen with seal disabled so it stays unsealed
    let mut ledger = Ledger::new(no_rotation_config(&dir));
    ledger.start().unwrap();

    // GetTransaction on closed unsealed segment
    for &tx_id in &tx_ids {
        let result = query_transaction(&ledger, tx_id);
        assert!(
            result.is_some(),
            "tx {} should be found on closed unsealed segment",
            tx_id
        );
    }

    // GetAccountHistory on closed unsealed segment
    let history = query_account_history(&ledger, 7, 0, 100);
    assert_eq!(
        history.len(),
        10,
        "account 7 should have 10 entries from closed unsealed segment"
    );
    assert!(history.iter().all(|e| e.account_id == 7));

    drop(ledger);
    fs::remove_dir_all(&dir).ok();
}

// ── Query on sealed segment after restart ───────────────────────────────────

#[test]
fn test_query_sealed_segment_after_restart() {
    let dir = unique_dir("query_sealed_restart");

    let mut sampled_tx_ids = Vec::new();
    {
        let mut ledger = Ledger::new(cold_ledger_config(&dir));
        ledger.start().unwrap();

        let mut last_id = 0;
        for i in 0..20_000u64 {
            let id = ledger.submit(Operation::Deposit {
                account: 1 + (i % 10),
                amount: 100 + i,
                user_ref: i,
            });
            // Sample a few tx_ids spread across segments
            if i % 4_000 == 0 {
                sampled_tx_ids.push(id);
            }
            last_id = id;
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }
    // All segments sealed — .seal files exist on disk

    // Reopen from the same directory
    let mut ledger = Ledger::new(cold_ledger_config(&dir));
    ledger.start().unwrap();

    // GetTransaction: sampled tx_ids must be recovered from sealed segments
    for &tx_id in &sampled_tx_ids {
        let result = query_transaction(&ledger, tx_id);
        assert!(
            result.is_some(),
            "tx {} should be found on sealed segment after restart",
            tx_id
        );
    }

    // GetAccountHistory: account 1 received deposits at i=0,10,20,...
    // With 20k txs and 10 accounts, account 1 has ~2000 deposits.
    let history = query_account_history(&ledger, 1, 0, 50);
    assert_eq!(history.len(), 50, "should return exactly the requested limit");
    assert!(history.iter().all(|e| e.account_id == 1));
    // Newest first
    for w in history.windows(2) {
        assert!(w[0].tx_id > w[1].tx_id);
    }

    drop(ledger);
    fs::remove_dir_all(&dir).ok();
}
