use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::{SNAPSHOT_MAGIC, StorageConfig};
use roda_ledger::transaction::Operation;
use std::fs;
use std::path::Path;
use std::time::Duration;

/// Force snapshot creation: very small WAL segments (1MB) + snapshot every seal.
/// With 35K transactions × 2 records × 33 bytes ≈ 2.3MB → at least 2 seals → snapshot.
const SMALL_SEGMENT_MB: u64 = 1;
const SNAP_EVERY_SEAL: u32 = 1;

#[test]
fn test_snapshot_creation() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_snapshot_test_{}", nanos);
    if Path::new(&temp_dir).exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }

    let config = LedgerConfig {
        storage: StorageConfig {
            data_dir: temp_dir.clone(),
            wal_segment_size_mb: SMALL_SEGMENT_MB,
            snapshot_frequency: SNAP_EVERY_SEAL,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    // Submit enough transactions to trigger at least one WAL segment seal
    for _ in 0..35_000 {
        ledger.submit(Operation::Deposit {
            account: 1,
            amount: 1,
            user_ref: 0,
        });
    }
    // Also set account 2 balance
    let last_id = ledger.submit(Operation::Deposit {
        account: 2,
        amount: 200,
        user_ref: 0,
    });
    ledger.wait_for_transaction(last_id);

    drop(ledger);

    // There should be at least one snapshot_NNNNNN.bin file
    let snapshot_files: Vec<_> = fs::read_dir(&temp_dir)
        .unwrap()
        .flatten()
        .filter(|e| {
            let n = e.file_name().to_string_lossy().to_string();
            n.starts_with("snapshot_") && n.ends_with(".bin")
        })
        .collect();
    assert!(
        !snapshot_files.is_empty(),
        "Expected at least one snapshot file, found none in {}",
        temp_dir
    );

    // Pick the first snapshot and verify its format
    let mut snap_files_sorted = snapshot_files;
    snap_files_sorted.sort_by_key(|e| e.file_name());
    let snap_path = snap_files_sorted[0].path();
    let snap_name = snap_path.file_name().unwrap().to_string_lossy().to_string();

    let bin_data = fs::read(&snap_path).expect("snapshot file should be readable");

    // Header: 36 bytes
    assert!(
        bin_data.len() >= 36,
        "Snapshot must have at least 36-byte header"
    );

    // Validate magic
    let magic = u32::from_le_bytes(bin_data[0..4].try_into().unwrap());
    assert_eq!(
        magic, SNAPSHOT_MAGIC,
        "Snapshot magic should be SNAP (0x534E4150)"
    );

    // Validate version
    let version = bin_data[4];
    assert_eq!(version, 1, "Snapshot version should be 1");

    // Validate segment_id > 0
    let segment_id = u32::from_le_bytes(bin_data[5..9].try_into().unwrap());
    assert!(segment_id > 0, "Snapshot segment_id should be > 0");

    // account_count at offset 17 (after magic4+version1+segment_id4+last_tx_id8)
    let account_count = u64::from_le_bytes(bin_data[17..25].try_into().unwrap());
    assert!(
        account_count > 0,
        "Snapshot should have non-zero account count"
    );

    // Verify .crc sidecar exists and has correct format
    let crc_name = snap_name.replace(".bin", ".crc");
    let crc_path = Path::new(&temp_dir).join(&crc_name);
    assert!(
        crc_path.exists(),
        ".crc sidecar should exist alongside snapshot"
    );

    let crc_data = fs::read(&crc_path).unwrap();
    assert_eq!(
        crc_data.len(),
        16,
        ".crc sidecar should be exactly 16 bytes"
    );

    let sidecar_magic = u32::from_le_bytes(crc_data[12..16].try_into().unwrap());
    assert_eq!(
        sidecar_magic, SNAPSHOT_MAGIC,
        ".crc sidecar magic should be SNAP"
    );

    let stored_size = u64::from_le_bytes(crc_data[4..12].try_into().unwrap());
    assert_eq!(
        stored_size,
        bin_data.len() as u64,
        ".crc sidecar size should match snapshot file size"
    );

    // Verify file CRC
    let stored_crc = u32::from_le_bytes(crc_data[0..4].try_into().unwrap());
    let actual_crc = crc32c::crc32c(&bin_data);
    assert_eq!(
        stored_crc, actual_crc,
        ".crc sidecar CRC should match snapshot file"
    );

    // Cleanup
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn test_snapshot_restore_correct_balances() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_snapshot_restore_test_{}", nanos);
    if Path::new(&temp_dir).exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }

    let account_id = 42u64;
    let deposit_amount = 100u64;

    // Phase 1: write transactions and force a snapshot
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: temp_dir.clone(),
                wal_segment_size_mb: SMALL_SEGMENT_MB,
                snapshot_frequency: SNAP_EVERY_SEAL,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let mut last_id = 0u64;
        // Enough txs to trigger at least one seal → snapshot
        for _ in 0..35_000 {
            last_id = ledger.submit(Operation::Deposit {
                account: account_id,
                amount: deposit_amount,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);

        // Verify balance before shutdown
        let balance = ledger.get_balance(account_id);
        assert_eq!(balance, 35_000 * deposit_amount as i64);
    }

    // Phase 2: restart and verify balances are restored from snapshot + WAL replay
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: temp_dir.clone(),
                wal_segment_size_mb: SMALL_SEGMENT_MB,
                snapshot_frequency: SNAP_EVERY_SEAL,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let balance = ledger.get_balance(account_id);
        assert_eq!(
            balance,
            35_000 * deposit_amount as i64,
            "Balance should be restored from snapshot + WAL replay"
        );
    }

    let _ = fs::remove_dir_all(temp_dir);
}
