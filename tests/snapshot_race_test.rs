use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::StorageConfig;
use roda_ledger::transaction::Operation;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

/// Integration-level reproduction of the race condition documented in the snapshot unit test.
///
/// Race window:
///   SnapshotRunner writes `seal_notification` (segment sealed)
///     ↕  ← race window: new transactions arrive and are processed in normal mode
///   SnapshotStorer sets `checkpoint_requested`
///
/// Observable consequence at the ledger level:
///   A "witness" account (account 2) is only transacted AFTER the seal of segment 1.
///   Segment 1's snapshot should therefore capture account 2 balance = 0.
///   If the race is hit, the snapshot captures account 2 balance = 50 instead.
///   On restart, WAL replay adds another +50 for segment 2 on top of the restored snapshot:
///     - Correct path : 0  (snapshot) + 50 (replay) = 50
///     - Race path    : 50 (snapshot) + 50 (replay) = 100  ← double-counted
///
/// This test polls `ledger.last_seal_notification()` to detect the exact moment the race
/// window opens, then injects the witness transaction — making the reproduction deterministic.
#[test]
fn test_race_post_seal_transaction_double_counted_on_restart() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_snapshot_race_test_{}", nanos);
    if Path::new(&temp_dir).exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }

    // Account 1 : used only to fill segment 1 and trigger the seal.
    const ACCOUNT_FILLER: u64 = 1;
    // Account 2 : "witness" — touched only after the seal, inside the race window.
    const ACCOUNT_WITNESS: u64 = 2;
    const WITNESS_AMOUNT: u64 = 50;

    // Phase 1 — run ledger, force a segment 1 seal, inject witness tx in the race window.
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: temp_dir.clone(),
                transaction_count_per_segment: 100, // small segments → seal happens quickly
                snapshot_frequency: 1,              // snapshot on every seal
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..LedgerConfig::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        // Fill segment 1: enough transactions to trigger at least one seal.
        // 35 000 × 2 records × 33 bytes ≈ 2.3 MB → more than 1 MB → at least one rotation.
        let mut last_filler_id = 0u64;
        for _ in 0..35_000 {
            last_filler_id = ledger.submit(Operation::Deposit {
                account: ACCOUNT_FILLER,
                amount: 1,
                user_ref: 0,
            });
        }

        // Wait until the SnapshotRunner has published seal_notification > 0.
        // At this point checkpoint_requested has NOT yet been set by SnapshotStorer —
        // the race window is open.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if ledger.last_sealed_segment_id() > 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for seal_notification — no WAL segment was sealed"
            );
        }

        // RACE WINDOW IS OPEN.
        // Inject the witness transaction before SnapshotStorer sets checkpoint_requested.
        // Because checkpoint_mode is still false the SnapshotRunner will update
        // sb.checkpoint in "normal mode", writing WITNESS_AMOUNT into the segment-1 snapshot.
        let witness_id = ledger.submit(Operation::Deposit {
            account: ACCOUNT_WITNESS,
            amount: WITNESS_AMOUNT,
            user_ref: 0,
        });

        // Wait for the witness transaction to be fully processed by the snapshot pipeline
        // so it is visible in sb.checkpoint before we shut down.
        ledger.wait_for_transaction(witness_id);

        // Also wait for the filler transactions to be committed so WAL is consistent.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if ledger.last_commit_id() >= last_filler_id {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for filler commits"
            );
            std::thread::yield_now();
        }
    }

    // Phase 2 — restart the ledger and verify the witness account balance.
    //
    // Expected (correct) path:
    //   snapshot_000001.bin contains account 2 balance = 0 (not yet touched in segment 1)
    //   WAL replay (segments > 1 + active wal.bin) adds +50 for the witness tx
    //   Final balance = 0 + 50 = 50
    //
    // Race-condition path (what actually happens today):
    //   snapshot_000001.bin contains account 2 balance = 50 (leaked from segment 2)
    //   WAL replay adds another +50 for the same witness tx
    //   Final balance = 50 + 50 = 100  ← double-counted, demonstrating the bug
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: temp_dir.clone(),
                transaction_count_per_segment: 100,
                snapshot_frequency: 1,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..LedgerConfig::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let balance = ledger.get_balance(ACCOUNT_WITNESS);

        // This assertion FAILS when the race is hit:
        //   actual = 100 (double-counted), expected = 50
        assert_eq!(
            balance, WITNESS_AMOUNT as i64,
            "RACE CONDITION DETECTED at ledger level: witness account balance is {} but \
             should be {}. The segment-2 transaction was captured in the segment-1 snapshot \
             (race window between seal_notification and checkpoint_requested), then replayed \
             again from WAL, causing a double-count.",
            balance, WITNESS_AMOUNT
        );
    }

    let _ = fs::remove_dir_all(temp_dir);
}
