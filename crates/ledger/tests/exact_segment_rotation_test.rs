use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transactor::transaction::Operation;
use std::time::Duration;
use storage::entities::WalEntry;
use storage::{Storage, StorageConfig};

fn unique_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temp_{}_{}", name, nanos)
}

/// `transaction_count_per_segment` must be respected: with the limit set to 10_000
/// and 100_000 transactions inserted, every sealed segment must hold exactly 10_000
/// transactions (ADR-0013 §1: rotate when `tx_id % transaction_count_per_segment == 0`),
/// producing 10 sealed segments.
#[test]
fn test_transaction_count_per_segment_respected() {
    let dir = unique_dir("tx_count_per_segment");
    let tx_per_segment: u64 = 10_000;
    let total_txs: u64 = 100_000;

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: tx_per_segment,
                snapshot_frequency: 0, // focus on WAL segmentation, skip snapshots
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();
        // Open account 1 before depositing. This commits tx 1, becoming the first
        // tx of segment 1; rotation is span-based (last_tx - segment_start), so each
        // sealed segment still holds exactly tx_per_segment transactions and the
        // segment count is unchanged.
        ledger.open_accounts(100);

        let mut last_id = 0u64;
        for i in 0..total_txs {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 100,
                user_ref: i, // unique → no dedup hits
            });
        }

        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
        // Ledger dropped here, stopping threads and flushing state.
    }

    // Inspect the sealed segments on disk.
    {
        let storage = Storage::new(StorageConfig {
            data_dir: dir.clone(),
            transaction_count_per_segment: tx_per_segment,
            ..Default::default()
        })
        .unwrap();

        let segments = storage.list_all_segments().unwrap();
        let segment_count: usize = storage.segment_count();

        let expected_segments = (total_txs / tx_per_segment) as usize + 1; // plus active segment; // 10
        assert_eq!(
            segment_count,
            expected_segments,
            "expected {} sealed segments, got {}",
            expected_segments,
            segments.len()
        );

        // Count transactions per segment by TxMetadata records (one per transaction).
        // A Deposit emits 3 records (1 TxMetadata + 2 TxEntry), so record_count() is ~3x.
        for mut seg in segments {
            seg.load().unwrap();
            let mut txs = 0u64;
            seg.visit_wal_records(|e| {
                if matches!(e, WalEntry::Metadata(_)) {
                    txs += 1;
                }
            })
            .unwrap();
            assert_eq!(
                txs,
                tx_per_segment,
                "segment {} holds {} transactions, expected {}",
                seg.id(),
                txs,
                tx_per_segment
            );
        }
    }

    // Balance sanity: every deposit credited account 1 by 100.
    {
        let mut ledger = Ledger::new(LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: tx_per_segment,
                snapshot_frequency: 0,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        });
        ledger.start().unwrap();
        assert_eq!(ledger.get_balance(1), 100 * total_txs as i64);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
