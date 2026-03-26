use roda_ledger::wallet::{Wallet, WalletConfig};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

#[test]
fn test_snapshot_creation() {
    let temp_dir = "temp_snapshot_test_1";
    if Path::new(temp_dir).exists() {
        let _ = fs::remove_dir_all(temp_dir);
    }

    let config = WalletConfig {
        location: Some(temp_dir.to_string()),
        in_memory: false,
        snapshot_interval: Duration::from_millis(500), // Fast snapshots for testing
        ..Default::default()
    };

    let mut wallet = Wallet::new_with_config(config);
    wallet.start();

    wallet.deposit(1, 100);
    wallet.deposit(2, 200);
    wallet.wait_pending_operations();

    // Wait for a snapshot to be created
    std::thread::sleep(Duration::from_secs(1));

    let snapshot_path = Path::new(temp_dir).join("snapshot.bin");
    assert!(snapshot_path.exists(), "Snapshot file should exist");

    // Read and verify
    let mut file = fs::File::open(&snapshot_path).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();

    // Snapshot format now:
    // Header: 3 * u64 = 24 bytes: checkpoint_id, last_transaction_id, last_wal_position
    // Then entries: each is u64 (8 bytes) + WalletBalance (8 bytes) = 16 bytes per entry.

    assert_eq!(
        buffer.len(),
        24 + 16 * 3,
        "Snapshot should contain header (24 bytes) and two 16-byte entries"
    );

    // Verify header exists and wal_position stored
    let _checkpoint_id_first = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
    let _last_tx_id_first = u64::from_le_bytes(buffer[8..16].try_into().unwrap());
    let wal_pos_first = u64::from_le_bytes(buffer[16..24].try_into().unwrap());
    assert!(
        wal_pos_first > 0,
        "wal_position in snapshot header must be > 0"
    );

    // Verify first snapshot contents
    let acc1_id_first = u64::from_le_bytes(buffer[24..32].try_into().unwrap());
    let acc1_bal_first = u64::from_le_bytes(buffer[32..40].try_into().unwrap());
    let acc2_id_first = u64::from_le_bytes(buffer[40..48].try_into().unwrap());
    let acc2_bal_first = u64::from_le_bytes(buffer[48..56].try_into().unwrap());

    assert_eq!(acc1_id_first, 1);
    assert_eq!(acc1_bal_first, 100);
    assert_eq!(acc2_id_first, 2);
    assert_eq!(acc2_bal_first, 200);

    // Let's do another deposit and wait for next snapshot
    wallet.deposit(1, 50);
    wallet.wait_pending_operations();

    // Wait for next snapshot
    std::thread::sleep(Duration::from_secs(1));

    let mut file = fs::File::open(&snapshot_path).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();

    assert_eq!(buffer.len(), 24 + 32);

    // Check header again and ensure wal_position increased
    let _checkpoint_id = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
    let _last_tx_id = u64::from_le_bytes(buffer[8..16].try_into().unwrap());
    let wal_pos = u64::from_le_bytes(buffer[16..24].try_into().unwrap());
    assert!(
        wal_pos >= wal_pos_first,
        "wal_position should not decrease between snapshots"
    );

    // Check balances for accounts
    // Entries are sorted by account_id.
    let acc1_id = u64::from_le_bytes(buffer[24..32].try_into().unwrap());
    let acc1_bal = u64::from_le_bytes(buffer[32..40].try_into().unwrap());

    let acc2_id = u64::from_le_bytes(buffer[40..48].try_into().unwrap());
    let acc2_bal = u64::from_le_bytes(buffer[48..56].try_into().unwrap());

    assert_eq!(acc1_id, 1);
    assert_eq!(acc1_bal, 150);
    assert_eq!(acc2_id, 2);
    assert_eq!(acc2_bal, 200);

    // Cleanup
    let _ = fs::remove_dir_all(temp_dir);
}
