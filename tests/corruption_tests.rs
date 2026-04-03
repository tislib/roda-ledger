use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::StorageConfig;
use roda_ledger::transaction::Operation;
use std::fs;
use std::path::Path;
use std::time::Duration;

// 1MB WAL segments + snapshot every 2 seals → ~10 seals, ~5 snapshots
// 160K txns × 2 records × ~33 bytes ≈ 10.6MB → ~10 WAL segment seals
const SMALL_SEGMENT_MB: u64 = 1;
const SNAP_FREQUENCY: u32 = 1;
const NUM_TRANSACTIONS: u64 = 160_000;
const ACCOUNT_ID: u64 = 7;
const DEPOSIT_AMOUNT: u64 = 1;

fn make_config(dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.to_string(),
            wal_segment_size_mb: SMALL_SEGMENT_MB,
            snapshot_frequency: SNAP_FREQUENCY,
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    }
}

fn unique_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temp_{}_{}", name, nanos)
}

/// Boot a fresh ledger, submit NUM_TRANSACTIONS deposits, wait for commit + snapshot flush.
/// Returns the verified balance before shutdown.
fn setup_ledger(dir: &str) -> i64 {
    let config = make_config(dir);
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    let mut last_id = 0u64;
    for _ in 0..NUM_TRANSACTIONS {
        last_id = ledger.submit(Operation::Deposit {
            account: ACCOUNT_ID,
            amount: DEPOSIT_AMOUNT,
            user_ref: 0,
        });
    }
    ledger.wait_for_transaction(last_id);

    let balance = ledger.get_balance(ACCOUNT_ID);

    assert_eq!(
        balance,
        (NUM_TRANSACTIONS * DEPOSIT_AMOUNT) as i64,
        "initial balance must equal total deposits"
    );

    ledger.wait_for_seal();

    drop(ledger);
    balance
}

/// Collect file names in `dir` whose name starts with `prefix` and ends with `suffix`.
fn list_files(dir: &str, prefix: &str, suffix: &str) -> Vec<String> {
    fs::read_dir(dir)
        .expect("cannot read temp dir")
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with(prefix) && name.ends_with(suffix) {
                Some(name)
            } else {
                None
            }
        })
        .collect()
}

/// Parse the numeric id from `snapshot_NNNNNN.bin` or `wal_NNNNNN.bin`.
fn parse_id(name: &str, prefix: &str, suffix: &str) -> Option<u32> {
    let inner = name.strip_prefix(prefix)?.strip_suffix(suffix)?;
    inner.parse().ok()
}

// ─── test 1: recoverable corruptions ────────────────────────────────────────

/// Verifies that the ledger survives several recoverable corruption scenarios:
///   A. Latest snapshot .bin deleted       → falls back to previous snapshot + WAL replay
///   B. WAL segments before last snapshot deleted → snapshot covers them; balance intact
///   C. Oldest snapshot deleted            → latest snapshot still valid; balance intact
#[test]
fn test1_recoverable_corruptions() {
    let dir = unique_dir("corruption_recoverable");
    if Path::new(&dir).exists() {
        let _ = fs::remove_dir_all(&dir);
    }

    let expected_balance = setup_ledger(&dir);
    assert_eq!(
        expected_balance,
        (NUM_TRANSACTIONS * DEPOSIT_AMOUNT) as i64,
        "initial balance must equal total deposits"
    );

    // Collect snapshot ids (sorted ascending)
    let mut snap_ids: Vec<u32> = list_files(&dir, "snapshot_", ".bin")
        .iter()
        .filter_map(|n| parse_id(n, "snapshot_", ".bin"))
        .collect();
    snap_ids.sort_unstable();
    assert!(
        snap_ids.len() >= 2,
        "need at least 2 snapshots for recoverable tests, got {}",
        snap_ids.len()
    );

    // ── Scenario A: delete the latest snapshot .bin ──────────────────────────
    {
        let latest_snap_id = *snap_ids.last().unwrap();
        let snap_bin = Path::new(&dir).join(format!("snapshot_{:06}.bin", latest_snap_id));
        fs::remove_file(&snap_bin)
            .unwrap_or_else(|e| panic!("could not delete latest snapshot: {}", e));

        let config = make_config(&dir);
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap(); // must not panic; falls back to previous snapshot

        let balance = ledger.get_balance(ACCOUNT_ID);
        assert_eq!(
            balance, expected_balance,
            "Scenario A: balance must be correct after deleting latest snapshot .bin"
        );
        drop(ledger);
    }

    // ── Scenario B: delete WAL segments that predate the latest snapshot ──────
    // After Scenario A the previous snapshot is the new "latest". Identify it and
    // remove all sealed WAL segments whose id is ≤ that snapshot id.
    {
        // Re-scan because Scenario A's restart may have created new snapshot files
        let mut remaining_snap_ids: Vec<u32> = list_files(&dir, "snapshot_", ".bin")
            .iter()
            .filter_map(|n| parse_id(n, "snapshot_", ".bin"))
            .collect();
        remaining_snap_ids.sort_unstable();
        let latest_snap_id = *remaining_snap_ids.last().unwrap();

        // Delete sealed WAL .bin and .crc files whose segment_id <= latest_snap_id
        let wal_ids: Vec<u32> = list_files(&dir, "wal_", ".bin")
            .iter()
            .filter_map(|n| parse_id(n, "wal_", ".bin"))
            .filter(|id| *id < latest_snap_id)
            .collect();

        for id in &wal_ids {
            let bin = Path::new(&dir).join(format!("wal_{:06}.bin", id));
            let crc = Path::new(&dir).join(format!("wal_{:06}.crc", id));
            let _ = fs::remove_file(&bin);
            let _ = fs::remove_file(&crc);
        }
        assert!(
            !wal_ids.is_empty(),
            "expected some pre-snapshot WAL files to delete"
        );

        let config = make_config(&dir);
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap(); // must not panic; snapshot covers deleted WALs

        let balance = ledger.get_balance(ACCOUNT_ID);
        assert_eq!(
            balance, expected_balance,
            "Scenario B: balance must be correct after deleting pre-snapshot WAL segments"
        );
        drop(ledger);
    }

    // ── Scenario C: delete the oldest snapshot (all variants) ────────────────
    {
        let mut current_snap_ids: Vec<u32> = list_files(&dir, "snapshot_", ".bin")
            .iter()
            .filter_map(|n| parse_id(n, "snapshot_", ".bin"))
            .collect();
        current_snap_ids.sort_unstable();

        if let Some(&oldest_id) = current_snap_ids.first() {
            let snap_bin = Path::new(&dir).join(format!("snapshot_{:06}.bin", oldest_id));
            let snap_crc = Path::new(&dir).join(format!("snapshot_{:06}.crc", oldest_id));
            let _ = fs::remove_file(&snap_bin);
            let _ = fs::remove_file(&snap_crc);
        }

        let config = make_config(&dir);
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap(); // must not panic; other snapshots still valid

        let balance = ledger.get_balance(ACCOUNT_ID);
        assert_eq!(
            balance, expected_balance,
            "Scenario C: balance must be correct after deleting oldest snapshot"
        );
        drop(ledger);
    }

    let _ = fs::remove_dir_all(&dir);
}

// ─── test 2: non-recoverable corruptions ────────────────────────────────────

/// Verifies that non-recoverable corruptions are detected correctly:
///   A. Last sealed WAL .crc corrupted → ledger panics on startup (caught via catch_unwind)
///   B. All snapshot .crc files corrupted + early WAL segments deleted
///      → no valid snapshot, partial WAL replay → balance is LOWER than expected
#[test]
fn test2_non_recoverable_corruptions() {
    // ── Scenario A: corrupt last sealed WAL .crc ─────────────────────────────
    //
    // After setup the latest snapshot covers all sealed WAL segments up to its
    // segment_id N, so segments ≤ N are never replayed (filter is > N).  To
    // ensure the corrupted segment IS replayed we:
    //   1. Find the two most-recent snapshots (N and N-2).
    //   2. Delete the latest snapshot (N) so recovery falls back to N-2.
    //   3. Corrupt wal_{N}.crc — now segment N > (N-2) and will be replayed.
    {
        let dir = unique_dir("corruption_wal_crc");
        if Path::new(&dir).exists() {
            let _ = fs::remove_dir_all(&dir);
        }
        setup_ledger(&dir);

        // Collect and sort snapshot ids descending
        let mut snap_ids: Vec<u32> = list_files(&dir, "snapshot_", ".bin")
            .iter()
            .filter_map(|n| parse_id(n, "snapshot_", ".bin"))
            .collect();
        snap_ids.sort_unstable_by(|a, b| b.cmp(a)); // descending
        assert!(
            snap_ids.len() >= 2,
            "need at least 2 snapshots for Scenario A, got {}",
            snap_ids.len()
        );

        let latest_snap_id = snap_ids[0];
        let fallback_snap_id = snap_ids[1];

        // Delete the latest snapshot so recovery falls back to fallback_snap_id
        let _ =
            fs::remove_file(Path::new(&dir).join(format!("snapshot_{:06}.bin", latest_snap_id)));
        let _ =
            fs::remove_file(Path::new(&dir).join(format!("snapshot_{:06}.crc", latest_snap_id)));

        // Find WAL .crc files with segment_id > fallback_snap_id (will be replayed)
        let mut replayable_wal_ids: Vec<u32> = list_files(&dir, "wal_", ".crc")
            .iter()
            .filter_map(|n| parse_id(n, "wal_", ".crc"))
            .filter(|id| *id > fallback_snap_id)
            .collect();
        replayable_wal_ids.sort_unstable();
        let target_wal_id = *replayable_wal_ids
            .last()
            .expect("expected at least one replayable WAL .crc after fallback snapshot");

        let crc_path = Path::new(&dir).join(format!("wal_{:06}.crc", target_wal_id));

        // Corrupt the stored CRC (first 4 bytes) by flipping all bits
        let mut sidecar = fs::read(&crc_path).expect("cannot read WAL .crc sidecar");
        assert_eq!(sidecar.len(), 16, "WAL .crc sidecar must be 16 bytes");
        sidecar[0] ^= 0xFF;
        sidecar[1] ^= 0xFF;
        sidecar[2] ^= 0xFF;
        sidecar[3] ^= 0xFF;
        fs::write(&crc_path, &sidecar).expect("cannot write corrupted WAL .crc sidecar");

        // Attempting to start the ledger with a corrupt WAL .crc must panic
        let dir_clone = dir.clone();
        let config = make_config(&dir_clone);
        let mut ledger = Ledger::new(config);
        let result = ledger.start();

        assert!(
            result.is_err(),
            "Scenario A: expected a panic when a replayable sealed WAL .crc is corrupted"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Scenario B: latest snapshot .crc corrupted → must panic ─────────────
    //
    // When the most-recent snapshot's .crc sidecar is corrupted the ledger
    // must refuse to start and panic.  restore() stops at the first Err —
    // i.e. the latest snapshot — and never falls back to an earlier one.
    {
        let dir = unique_dir("corruption_snap_crc");
        if Path::new(&dir).exists() {
            let _ = fs::remove_dir_all(&dir);
        }
        setup_ledger(&dir);

        // Collect all snapshot ids (sorted)
        let mut snap_ids: Vec<u32> = list_files(&dir, "snapshot_", ".crc")
            .iter()
            .filter_map(|n| parse_id(n, "snapshot_", ".crc"))
            .collect();
        snap_ids.sort_unstable();
        assert!(
            !snap_ids.is_empty(),
            "expected snapshot .crc files after setup"
        );

        // Corrupt ALL snapshot .crc sidecars
        for id in &snap_ids {
            let crc_path = Path::new(&dir).join(format!("snapshot_{:06}.crc", id));
            let mut sidecar = fs::read(&crc_path).expect("cannot read snapshot .crc sidecar");
            assert_eq!(sidecar.len(), 16, "snapshot .crc sidecar must be 16 bytes");
            sidecar[0] ^= 0xFF;
            sidecar[1] ^= 0xFF;
            sidecar[2] ^= 0xFF;
            sidecar[3] ^= 0xFF;
            fs::write(&crc_path, &sidecar).expect("cannot write corrupted snapshot .crc sidecar");
        }

        // Attempting to start the ledger with corrupted snapshot .crc files must panic
        let dir_clone = dir.clone();
        let config = make_config(&dir_clone);
        let mut ledger = Ledger::new(config);
        let result = ledger.start();

        assert!(
            result.is_err(),
            "Scenario B: expected a panic when all snapshot .crc sidecars are corrupted"
        );

        let _ = fs::remove_dir_all(&dir);
    }
}
