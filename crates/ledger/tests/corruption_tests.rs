use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::Operation;
use std::fs;
use std::path::Path;
use std::time::Duration;
use storage::StorageConfig;

// 100 tx/segment, snapshot every seal. Segment/snapshot count is throughput-
// dependent (batched ingest); 20_000 reliably yields the many each scenario needs.
const SMALL_SEGMENT_TX_COUNT: u64 = 1000;
const SNAP_FREQUENCY: u32 = 1;
const NUM_TRANSACTIONS: u64 = 20_000;
const ACCOUNT_ID: u64 = 7;
const DEPOSIT_AMOUNT: u64 = 1;

fn make_config(dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.to_string(),
            transaction_count_per_segment: SMALL_SEGMENT_TX_COUNT,
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

// ─── test 3: file-level problems reported during restore ─────────────────────

/// Helper for test3: pick the sealed WAL segment id that will be replayed
/// after the latest snapshot is deleted. Returns `(target_wal_id,
/// fallback_snap_id)`.
fn pick_replayable_wal(dir: &str) -> (u32, u32) {
    let mut snap_ids: Vec<u32> = list_files(dir, "snapshot_", ".bin")
        .iter()
        .filter_map(|n| parse_id(n, "snapshot_", ".bin"))
        .collect();
    snap_ids.sort_unstable_by(|a, b| b.cmp(a));
    assert!(
        snap_ids.len() >= 2,
        "need ≥2 snapshots, got {}",
        snap_ids.len()
    );

    let latest_snap_id = snap_ids[0];
    let fallback_snap_id = snap_ids[1];

    let _ = fs::remove_file(Path::new(dir).join(format!("snapshot_{:06}.bin", latest_snap_id)));
    let _ = fs::remove_file(Path::new(dir).join(format!("snapshot_{:06}.crc", latest_snap_id)));

    let mut replayable: Vec<u32> = list_files(dir, "wal_", ".crc")
        .iter()
        .filter_map(|n| parse_id(n, "wal_", ".crc"))
        .filter(|id| *id > fallback_snap_id)
        .collect();
    replayable.sort_unstable();
    let target = *replayable
        .last()
        .expect("expected at least one replayable WAL .crc");
    (target, fallback_snap_id)
}

/// Each scenario corrupts a single file-level property reported during
/// restore (data CRC mismatch, sidecar size mismatch, sidecar magic
/// mismatch, snapshot data CRC mismatch). All four are no-error states
/// from the filesystem's point of view — bytes are present and
/// readable — but the integrity checks in `verify_wal_data` /
/// snapshot verification fire and `Ledger::start` returns `Err`.
#[test]
fn test3_file_level_problems() {
    // ── Scenario A: WAL data bytes modified (a "transaction moved" in
    //    the middle of wal_{N}.bin). Stored CRC sidecar is intact;
    //    recomputed CRC of the data differs → CRC mismatch error.
    {
        let dir = unique_dir("corruption_wal_data");
        if Path::new(&dir).exists() {
            let _ = fs::remove_dir_all(&dir);
        }
        setup_ledger(&dir);

        let (target_wal_id, _) = pick_replayable_wal(&dir);
        let bin_path = Path::new(&dir).join(format!("wal_{:06}.bin", target_wal_id));

        // Flip a byte in the middle of the WAL data — simulates a
        // transaction record being moved/overwritten in place.
        let mut data = fs::read(&bin_path).expect("cannot read wal .bin");
        assert!(data.len() > 40, "wal .bin too small to corrupt mid-record");
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        fs::write(&bin_path, &data).expect("cannot write modified wal .bin");

        let mut ledger = Ledger::new(make_config(&dir));
        let result = ledger.start();
        assert!(
            result.is_err(),
            "Scenario A: modifying bytes inside wal_{:06}.bin must fail restore (CRC mismatch)",
            target_wal_id,
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // ── Scenario B: WAL sidecar size-field rewritten to a wrong value.
    //    `verify_wal_data` reports a stored-size vs actual-size mismatch.
    {
        let dir = unique_dir("corruption_wal_size");
        if Path::new(&dir).exists() {
            let _ = fs::remove_dir_all(&dir);
        }
        setup_ledger(&dir);

        let (target_wal_id, _) = pick_replayable_wal(&dir);
        let crc_path = Path::new(&dir).join(format!("wal_{:06}.crc", target_wal_id));

        // Rewrite bytes 4..12 (the stored size field) to a value that
        // doesn't match the .bin length.
        let mut sidecar = fs::read(&crc_path).expect("cannot read wal .crc");
        assert_eq!(sidecar.len(), 16);
        let bogus = (u64::MAX - 1).to_le_bytes();
        sidecar[4..12].copy_from_slice(&bogus);
        fs::write(&crc_path, &sidecar).expect("cannot write modified wal .crc");

        let mut ledger = Ledger::new(make_config(&dir));
        let result = ledger.start();
        assert!(
            result.is_err(),
            "Scenario B: bogus size-field in wal_{:06}.crc must fail restore (size mismatch)",
            target_wal_id,
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // ── Scenario C: WAL sidecar magic field rewritten to garbage.
    //    `verify_wal_data` reports a magic mismatch.
    {
        let dir = unique_dir("corruption_wal_magic");
        if Path::new(&dir).exists() {
            let _ = fs::remove_dir_all(&dir);
        }
        setup_ledger(&dir);

        let (target_wal_id, _) = pick_replayable_wal(&dir);
        let crc_path = Path::new(&dir).join(format!("wal_{:06}.crc", target_wal_id));

        // Rewrite the trailing 4 magic bytes.
        let mut sidecar = fs::read(&crc_path).expect("cannot read wal .crc");
        sidecar[12..16].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        fs::write(&crc_path, &sidecar).expect("cannot write modified wal .crc");

        let mut ledger = Ledger::new(make_config(&dir));
        let result = ledger.start();
        assert!(
            result.is_err(),
            "Scenario C: wrong magic in wal_{:06}.crc must fail restore (magic mismatch)",
            target_wal_id,
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // ── Scenario D: snapshot data bytes modified. Sidecar CRC is
    //    intact; recomputed CRC of the snapshot differs → CRC
    //    mismatch error during snapshot restore.
    {
        let dir = unique_dir("corruption_snap_data");
        if Path::new(&dir).exists() {
            let _ = fs::remove_dir_all(&dir);
        }
        setup_ledger(&dir);

        let mut snap_ids: Vec<u32> = list_files(&dir, "snapshot_", ".bin")
            .iter()
            .filter_map(|n| parse_id(n, "snapshot_", ".bin"))
            .collect();
        snap_ids.sort_unstable_by(|a, b| b.cmp(a));
        assert!(!snap_ids.is_empty(), "expected snapshot files after setup");

        // Corrupt all snapshot .bin files so the restore can't fall
        // back to a clean earlier one — start must surface the error.
        for id in &snap_ids {
            let bin_path = Path::new(&dir).join(format!("snapshot_{:06}.bin", id));
            let mut data = fs::read(&bin_path).expect("cannot read snapshot .bin");
            assert!(data.len() > 40, "snapshot too small to corrupt mid-file");
            let mid = data.len() / 2;
            data[mid] ^= 0xFF;
            fs::write(&bin_path, &data).expect("cannot write modified snapshot .bin");
        }

        let mut ledger = Ledger::new(make_config(&dir));
        let result = ledger.start();
        assert!(
            result.is_err(),
            "Scenario D: modifying bytes inside snapshot .bin files must fail restore"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
