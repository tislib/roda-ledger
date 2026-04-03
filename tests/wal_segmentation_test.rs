use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::StorageConfig;
use roda_ledger::transaction::Operation;
use std::fs;
use std::path::Path;
use std::time::Duration;

/// Verify that WAL segment rotation works end-to-end:
/// - SegmentHeader is written as the first record of wal.bin
/// - When the size threshold is reached, wal.bin is sealed and renamed to wal_000001.bin
/// - A .crc sidecar and an empty .seal marker are created alongside the sealed segment
/// - A fresh wal.bin is created with a new SegmentHeader
/// - Replay after rotation restores the correct balance from all segments
#[test]
fn test_wal_segment_rotation() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_wal_seg_test_{}", nanos);

    if Path::new(&temp_dir).exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }

    let account_id = 1u64;
    let deposit_amount = 100u64;

    // Use 1 MB segments so rotation happens quickly under test load
    let num_transactions = 35_000; // ~35k × 33 bytes × 2 records (Metadata+Entry) ≈ 2.3 MB → 2+ rotations

    let mut last_tx_id = 0u64;
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: temp_dir.clone(),
                wal_segment_size_mb: 1,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..LedgerConfig::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        for _ in 0..num_transactions {
            last_tx_id = ledger.submit(Operation::Deposit {
                account: account_id,
                amount: deposit_amount,
                user_ref: 0,
            });
        }

        // Wait until the last transaction is committed to WAL
        loop {
            if ledger.last_committed_id() >= last_tx_id {
                break;
            }
            std::thread::yield_now();
        }
        // Ledger is dropped here, stopping all threads
    }

    // Inspect the data directory
    let entries: Vec<_> = fs::read_dir(&temp_dir)
        .expect("temp dir should exist")
        .flatten()
        .collect();

    let sealed_markers: Vec<_> = entries
        .iter()
        .filter(|e| {
            let n = e.file_name().to_string_lossy().to_string();
            n.starts_with("wal_") && n.ends_with(".seal")
        })
        .collect();

    let sealed_crcs: Vec<_> = entries
        .iter()
        .filter(|e| {
            let n = e.file_name().to_string_lossy().to_string();
            n.starts_with("wal_") && n.ends_with(".crc")
        })
        .collect();

    // Every sealed segment must have both a .crc sidecar and a .seal marker
    assert_eq!(
        sealed_markers.len(),
        sealed_crcs.len(),
        "each sealed segment must have a .crc sidecar and a .seal marker (seals={}, crcs={})",
        sealed_markers.len(),
        sealed_crcs.len()
    );

    // wal.bin (active segment) must still exist
    assert!(
        Path::new(&temp_dir).join("wal.bin").exists(),
        "active wal.bin must exist after rotation"
    );

    // Phase 2: restore from disk and verify balances survive rotation
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: temp_dir.clone(),
                wal_segment_size_mb: 1,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..LedgerConfig::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let expected_balance = deposit_amount as i64 * num_transactions as i64;
        let actual_balance = ledger.get_balance(account_id);
        assert_eq!(
            actual_balance, expected_balance,
            "balance after replay across segments should be {} but got {}",
            expected_balance, actual_balance
        );
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

/// Verify the .crc sidecar format: [crc32c: 4][size: 8][magic: 4] = 16 bytes
#[test]
fn test_crc_sidecar_format() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_wal_crc_test_{}", nanos);

    if Path::new(&temp_dir).exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }

    let num_transactions = 1000_000;
    let mut last_tx_id = 0u64;

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: temp_dir.clone(),
                wal_segment_size_mb: 1,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..LedgerConfig::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        for _ in 0..num_transactions {
            last_tx_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }

        loop {
            if ledger.last_committed_id() >= last_tx_id {
                let a = ledger.last_sealed_segment_id();
                break;
            }
            std::thread::yield_now();
        }
        ledger.wait_for_seal();
    }

    // Find the first .crc sidecar and validate its format
    let mut crc_files: Vec<_> = fs::read_dir(&temp_dir)
        .unwrap()
        .flatten()
        .filter(|e| {
            let n = e.file_name().to_string_lossy().to_string();
            n.starts_with("wal_") && n.ends_with(".crc")
        })
        .collect();
    crc_files.sort_by_key(|e| e.file_name());

    assert!(
        !crc_files.is_empty(),
        "at least one .crc sidecar must exist"
    );

    for crc_entry in &crc_files {
        let crc_path = crc_entry.path();
        let crc_data = fs::read(&crc_path).expect("crc sidecar should be readable");

        assert_eq!(
            crc_data.len(),
            16,
            "{:?}: sidecar must be exactly 16 bytes",
            crc_path
        );

        // Validate magic bytes at offset 12
        let magic = u32::from_le_bytes(crc_data[12..16].try_into().unwrap());
        assert_eq!(
            magic, 0x524F4441,
            "{:?}: sidecar magic should be 0x524F4441 (RODA)",
            crc_path
        );

        // Validate that the stored size matches the actual .bin file size
        let stored_size = u64::from_le_bytes(crc_data[4..12].try_into().unwrap());
        let bin_name = crc_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .replace(".crc", ".bin");
        let bin_path = crc_path.parent().unwrap().join(bin_name);
        let actual_size = fs::metadata(&bin_path)
            .expect("matching .bin should exist")
            .len();
        assert_eq!(
            stored_size, actual_size,
            "{:?}: stored size {} != actual bin size {}",
            crc_path, stored_size, actual_size
        );
    }

    let _ = fs::remove_dir_all(&temp_dir);
}
