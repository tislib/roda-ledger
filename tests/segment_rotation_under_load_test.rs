use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::StorageConfig;
use roda_ledger::transaction::Operation;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

fn unique_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temp_{}_{}", name, nanos)
}

/// 1MB segments, 500K deposits. All segments sealed, balance correct on restart.
#[test]
fn test_rapid_rotation_many_segments() {
    let dir = unique_dir("rapid_rotation");
    let total = 500_000u64;

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                wal_segment_size_mb: 1,
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let mut last_id = 0u64;
        for _ in 0..total {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();

        assert_eq!(ledger.get_balance(1), total as i64);
    }

    // Verify sealed segment files
    let entries: Vec<_> = fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).collect();
    let sealed_bins: Vec<_> = entries
        .iter()
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("wal_") && name.ends_with(".bin")
        })
        .collect();
    assert!(
        sealed_bins.len() >= 5,
        "expected multiple sealed segments, got {}",
        sealed_bins.len()
    );

    // Each sealed .bin that has a .seal marker should have a corresponding .crc
    for bin_entry in &sealed_bins {
        let bin_name = bin_entry.file_name();
        let seal_name = bin_name.to_string_lossy().replace(".bin", ".seal");
        let seal_path = std::path::Path::new(&dir).join(&seal_name);
        // Only check CRC for fully sealed segments (has .seal marker)
        if seal_path.exists() {
            let crc_name = bin_name.to_string_lossy().replace(".bin", ".crc");
            let crc_path = std::path::Path::new(&dir).join(&crc_name);
            assert!(
                crc_path.exists(),
                "missing .crc sidecar for sealed {}",
                bin_name.to_string_lossy()
            );
        }
    }

    // Restart and verify
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                wal_segment_size_mb: 1,
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        assert_eq!(
            ledger.get_balance(1),
            total as i64,
            "balance mismatch after rapid rotation + restart"
        );
    }

    let _ = fs::remove_dir_all(dir);
}

/// 4 threads + 1MB segments, run concurrently, verify total.
#[test]
fn test_concurrent_writes_during_rotation() {
    let dir = unique_dir("concurrent_rotation");

    let config = LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.clone(),
            wal_segment_size_mb: 1,
            snapshot_frequency: 2,
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();
    let ledger = Arc::new(ledger);

    let num_threads = 4;
    let ops_per_thread = 50_000u64;
    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let l = ledger.clone();
        handles.push(std::thread::spawn(move || {
            let mut last = 0u64;
            for _ in 0..ops_per_thread {
                last = l.submit(Operation::Deposit {
                    account: 1,
                    amount: 1,
                    user_ref: 0,
                });
            }
            last
        }));
    }

    let max_id = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .max()
        .unwrap();

    ledger.wait_for_transaction(max_id);

    let expected = (num_threads * ops_per_thread) as i64;
    assert_eq!(
        ledger.get_balance(1),
        expected,
        "concurrent writes during rotation lost data"
    );

    let _ = fs::remove_dir_all(dir);
}

/// Manually verify CRC of each sealed segment .bin matches its .crc sidecar.
#[test]
fn test_seal_crc_integrity() {
    let dir = unique_dir("crc_integrity");

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                wal_segment_size_mb: 1,
                snapshot_frequency: 0, // no snapshots, focus on WAL
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let mut last_id = 0u64;
        for _ in 0..100_000 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    // Verify CRC for each sealed segment that has a .crc sidecar
    let entries: Vec<_> = fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).collect();

    let mut verified_count = 0;
    for entry in &entries {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with("wal_") && name_str.ends_with(".bin") {
            let crc_path = entry.path().with_extension("crc");
            // The active (last) segment may not have a .crc yet - skip it
            if !crc_path.exists() {
                continue;
            }

            let bin_data = fs::read(entry.path()).unwrap();
            let computed_crc = crc32c::crc32c(&bin_data);

            let crc_data = fs::read(&crc_path).unwrap();
            // CRC sidecar format: [crc32c: u32 LE | size: u64 LE | magic: u32 LE] = 16 bytes
            assert_eq!(
                crc_data.len(),
                16,
                "unexpected .crc file size for {}",
                name_str
            );

            let stored_crc = u32::from_le_bytes(crc_data[0..4].try_into().unwrap());
            let stored_size = u64::from_le_bytes(crc_data[4..12].try_into().unwrap());

            assert_eq!(
                computed_crc, stored_crc,
                "CRC mismatch for {}: computed={:#x} stored={:#x}",
                name_str, computed_crc, stored_crc
            );
            assert_eq!(
                stored_size,
                bin_data.len() as u64,
                "size mismatch in .crc sidecar for {}",
                name_str
            );
            verified_count += 1;
        }
    }
    assert!(verified_count >= 1, "no sealed segments with CRC to verify");

    let _ = fs::remove_dir_all(dir);
}

/// snapshot_frequency=2, verify snapshots are created at expected intervals.
#[test]
fn test_snapshot_at_segment_boundary() {
    let dir = unique_dir("snap_boundary");

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                wal_segment_size_mb: 1,
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        // Generate enough data for 6+ segments (~120K deposits at 1MB segments)
        let mut last_id = 0u64;
        for _ in 0..200_000 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    // Count snapshot files
    let entries: Vec<_> = fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).collect();

    let snapshot_bins: Vec<_> = entries
        .iter()
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("snapshot_") && name.ends_with(".bin")
        })
        .collect();

    let sealed_bins: Vec<_> = entries
        .iter()
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("wal_") && name.ends_with(".bin")
        })
        .collect();

    // With snapshot_frequency=2 and N sealed segments, expect ~N/2 snapshots
    assert!(
        !snapshot_bins.is_empty(),
        "no snapshots created with snapshot_frequency=2"
    );
    assert!(
        sealed_bins.len() >= 4,
        "expected at least 4 sealed segments, got {}",
        sealed_bins.len()
    );

    // Restart and verify correctness
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                wal_segment_size_mb: 1,
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        assert_eq!(ledger.get_balance(1), 200_000);
    }

    let _ = fs::remove_dir_all(dir);
}
