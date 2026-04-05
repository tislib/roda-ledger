use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::{Storage, StorageConfig};
use roda_ledger::transaction::Operation;
use std::path::Path;
use std::time::Duration;

fn unique_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temp_{}_{}", name, nanos)
}

#[test]
fn test_sealing_at_rotation_boundary() {
    let dir = unique_dir("sealing_at_boundary");
    let wal_size_mb = 1;
    let wal_size_bytes = wal_size_mb * 1024 * 1024;
    let wal_entry_size = 40;

    let record_count = wal_size_bytes / (3 * wal_entry_size) + 1;

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                wal_segment_size_mb: wal_size_mb as u64,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        for i in 0..record_count {
            ledger.submit(Operation::Deposit {
                account: 1,
                amount: 100,
                user_ref: i as u64,
            });
        }

        ledger.wait_for_seal();
        // Ledger dropped here
    }

    // open storage manually in same folder
    let storage_config = StorageConfig {
        data_dir: dir.clone(),
        ..Default::default()
    };
    let storage = Storage::new(storage_config).unwrap();
    let segments = storage.list_all_segments().unwrap();

    // check if size = 1 and is sealed
    assert_eq!(
        segments.len(),
        1,
        "Test 1: Expected exactly 1 sealed segment"
    );

    // Check if sealed (by existence of .seal file)
    let seal_file = Path::new(&dir).join("wal_000001.seal");
    assert!(
        seal_file.exists(),
        "Test 1: Seal file for segment 1 should exist"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_sealing_below_rotation_boundary() {
    let dir = unique_dir("sealing_below_boundary");
    let wal_size_mb = 1;
    let wal_size_bytes = wal_size_mb * 1024 * 1024;
    let wal_entry_size = 40;

    // Test 2: record_count = wal_size / (3 * wal_entry_size) - 1
    let record_count = (wal_size_bytes / (3 * wal_entry_size)) - 1;

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                wal_segment_size_mb: wal_size_mb as u64,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        for i in 0..record_count {
            ledger.submit(Operation::Deposit {
                account: 1,
                amount: 100,
                user_ref: i as u64,
            });
        }

        ledger.wait_for_seal();
        // Ledger dropped here
    }

    // open storage manually in same folder
    let storage_config = StorageConfig {
        data_dir: dir.clone(),
        ..Default::default()
    };
    let storage = Storage::new(storage_config).unwrap();
    let segments = storage.list_all_segments().unwrap();

    // check if size = 0
    assert_eq!(segments.len(), 0, "Test 2: Expected 0 sealed segments");

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);
}
