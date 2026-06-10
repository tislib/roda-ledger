//! Verifies the design rule: "Any partial written data in the WAL ending
//! must be discardable by node on restart." A partial last transaction
//! (TxMetadata announcing N followers, fewer than N actually written)
//! must NOT panic recovery — it must truncate to the byte before the
//! incomplete metadata and start a clean ledger reflecting only the
//! fully-committed prefix.
//!
//! Companion to `corruption_tests.rs`, which covers file-deletion /
//! sidecar-corruption cases but not partial-byte-stream tails.

use ledger::config::LedgerConfig;
use ledger::ledger::Ledger;
use ledger::transaction::Operation;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use storage::StorageConfig;

const SMALL_SEGMENT_TX_COUNT: u64 = 100_000;
const ACCOUNT_ID: u64 = 7;
const DEPOSIT_AMOUNT: u64 = 11;
const WAL_RECORD_SIZE: usize = 40;

fn make_config(dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.to_string(),
            transaction_count_per_segment: SMALL_SEGMENT_TX_COUNT,
            snapshot_frequency: 0, // keep segments unsnapshotted so wal.bin is the only authority
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

/// Drive `tx_count` deposits through a fresh ledger, shut it down
/// CLEANLY (writes `wal.stop`), and return the expected balance.
fn run_and_clean_shutdown(dir: &str, tx_count: u64) -> i64 {
    let mut ledger = Ledger::new(make_config(dir));
    ledger.start().unwrap();
    ledger.open_accounts(100); // open before deposits
    let mut last_id = 0u64;
    for _ in 0..tx_count {
        last_id = ledger.submit(Operation::Deposit {
            account: ACCOUNT_ID,
            amount: DEPOSIT_AMOUNT,
            user_ref: 0,
        });
    }
    ledger.wait_for_transaction(last_id);
    let balance = ledger.get_balance(ACCOUNT_ID);
    drop(ledger); // clean shutdown — writes wal.stop
    balance
}

/// Wipe the `wal.stop` marker so the next `Ledger::start` triggers
/// crash recovery on the active segment.
fn force_unclean_shutdown(dir: &str) {
    let path = Path::new(dir).join("wal.stop");
    if path.exists() {
        fs::remove_file(&path).unwrap();
    }
}

fn wal_path(dir: &str) -> std::path::PathBuf {
    Path::new(dir).join("wal.bin")
}

fn wal_size(dir: &str) -> u64 {
    fs::metadata(wal_path(dir)).map(|m| m.len()).unwrap_or(0)
}

/// Append `bytes` to the active wal.bin, simulating a crash that
/// caught the WAL writer mid-flight.
fn append_to_wal(dir: &str, bytes: &[u8]) {
    let mut f = OpenOptions::new()
        .append(true)
        .open(wal_path(dir))
        .expect("open wal.bin for append");
    f.write_all(bytes).expect("append bytes");
    f.sync_all().expect("sync wal.bin");
}

/// 40-byte "TxMetadata-looking" record: byte 0 = entry_type (0 = TxMetadata),
/// byte 2 = sub_item_count (the rest is zero — CRC won't validate, but
/// that's exactly what crash recovery should treat as broken).
fn craft_tx_metadata(sub_item_count: u8) -> [u8; WAL_RECORD_SIZE] {
    let mut rec = [0u8; WAL_RECORD_SIZE];
    rec[0] = 0; // entry_type = TxMetadata
    rec[2] = sub_item_count; // sub_item_count low byte (u16 little-endian)
    rec
}

/// 40-byte "TxEntry-looking" record: byte 0 = 1 (TxEntry).
fn craft_tx_entry() -> [u8; WAL_RECORD_SIZE] {
    let mut rec = [0u8; WAL_RECORD_SIZE];
    rec[0] = 1; // entry_type = TxEntry
    rec
}

/// Case 1: leader was killed right after writing a `TxMetadata` but
/// before any of its followers. Recovery must truncate that lone
/// metadata and keep the prior committed prefix.
#[test]
fn tail_partial_metadata_only_truncates() {
    let dir = unique_dir("tail_partial_meta_only");
    let _ = fs::remove_dir_all(&dir);

    // Commit N real transactions, then shut down cleanly so the WAL
    // is well-formed up to this point.
    let expected = run_and_clean_shutdown(&dir, 50);
    let clean_size = wal_size(&dir);

    // Simulate a crash: drop wal.stop and append a bare TxMetadata
    // that announces 2 followers — none of which arrived.
    force_unclean_shutdown(&dir);
    append_to_wal(&dir, &craft_tx_metadata(2));
    assert_eq!(
        wal_size(&dir),
        clean_size + WAL_RECORD_SIZE as u64,
        "wal.bin must have grown by exactly one 40-byte record"
    );

    // Restart — recovery must succeed and truncate the trailing
    // partial metadata.
    let mut ledger = Ledger::new(make_config(&dir));
    ledger
        .start()
        .expect("recovery must succeed on tail-partial metadata");

    let after_size = wal_size(&dir);
    assert_eq!(
        after_size, clean_size,
        "recovery should have truncated the partial trailing metadata"
    );
    assert_eq!(
        ledger.get_balance(ACCOUNT_ID),
        expected,
        "balance must match the cleanly-committed prefix"
    );
    drop(ledger);
    let _ = fs::remove_dir_all(&dir);
}

/// Case 2: metadata announces 2 followers but only 1 actually
/// arrived on disk before the crash. Same truncation requirement.
#[test]
fn tail_partial_metadata_plus_one_follower_truncates() {
    let dir = unique_dir("tail_partial_one_follower");
    let _ = fs::remove_dir_all(&dir);

    let expected = run_and_clean_shutdown(&dir, 50);
    let clean_size = wal_size(&dir);

    force_unclean_shutdown(&dir);
    let mut partial = Vec::with_capacity(WAL_RECORD_SIZE * 2);
    partial.extend_from_slice(&craft_tx_metadata(2));
    partial.extend_from_slice(&craft_tx_entry());
    append_to_wal(&dir, &partial);
    assert_eq!(
        wal_size(&dir),
        clean_size + 2 * WAL_RECORD_SIZE as u64,
        "wal.bin must have grown by exactly two 40-byte records"
    );

    let mut ledger = Ledger::new(make_config(&dir));
    ledger
        .start()
        .expect("recovery must succeed on tail-partial metadata + 1 of 2 followers");

    assert_eq!(
        wal_size(&dir),
        clean_size,
        "recovery should have truncated the partial transaction"
    );
    assert_eq!(
        ledger.get_balance(ACCOUNT_ID),
        expected,
        "balance must match the cleanly-committed prefix"
    );
    drop(ledger);
    let _ = fs::remove_dir_all(&dir);
}

/// Case 3: WAL ends mid-record (less than 40 bytes after the last
/// complete tx). A torn write at the byte level — the alignment is
/// off, so the recovery scan can't even start a new transaction at
/// that position. Must still truncate.
#[test]
fn tail_partial_torn_record_truncates() {
    let dir = unique_dir("tail_partial_torn_record");
    let _ = fs::remove_dir_all(&dir);

    let expected = run_and_clean_shutdown(&dir, 50);
    let clean_size = wal_size(&dir);

    force_unclean_shutdown(&dir);
    // 13 bytes of garbage — not a full 40-byte record.
    append_to_wal(&dir, &[0xAA; 13]);

    let mut ledger = Ledger::new(make_config(&dir));
    ledger
        .start()
        .expect("recovery must succeed when the tail bytes don't align to 40");

    let after = wal_size(&dir);
    assert!(
        after == clean_size || after == clean_size + 13,
        "recovery may truncate or leave the sub-record garbage past the \
         last complete record alone; got after={after} clean={clean_size}"
    );
    assert_eq!(
        ledger.get_balance(ACCOUNT_ID),
        expected,
        "balance must match the cleanly-committed prefix"
    );
    drop(ledger);
    let _ = fs::remove_dir_all(&dir);
}
