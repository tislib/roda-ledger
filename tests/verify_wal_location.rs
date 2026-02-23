use roda_ledger::wallet::{Wallet, WalletConfig};
use std::fs;
use std::path::Path;

#[test]
fn test_wal_location_is_populated() {
    let temp_dir = "temp_wal_location_test";
    if Path::new(temp_dir).exists() {
        let _ = fs::remove_dir_all(temp_dir);
    }

    let config = WalletConfig {
        location: Some(temp_dir.to_string()),
        capacity: 100,
        ..Default::default()
    };

    let mut wallet = Wallet::new_with_config(config);
    wallet.start();

    // Deposit 1
    wallet.deposit(1, 100);
    wallet.wait_pending_operations();

    // The first transaction should have wal_location 0
    // But we need a way to inspect the transactions.
    // Since we don't have a direct way to inspect transactions from Wallet,
    // let's look at the WAL file on disk.

    let wal_path = Path::new(temp_dir).join("wal.bin");
    assert!(wal_path.exists(), "WAL file should exist");

    let file_size = fs::metadata(&wal_path).unwrap().len();
    assert!(file_size > 0, "WAL file should not be empty");

    // Let's do another deposit
    wallet.deposit(1, 50);
    wallet.wait_pending_operations();

    let new_file_size = fs::metadata(&wal_path).unwrap().len();
    assert!(new_file_size > file_size, "WAL file should grow");

    // Now check the content of the WAL file
    // We expect Transactions to be at the beginning of the file with their wal_location populated.

    let content = fs::read(&wal_path).unwrap();

    // Each Transaction<WalletTransaction, WalletBalance>
    // WalletTransaction size:
    // #[repr(C)]
    // #[derive(Copy, Clone, Debug, Default, Pod, Zeroable)]
    // pub struct WalletTransaction {
    //     pub account_id: u64,
    //     pub to_account_id: Option<u64>,
    //     pub amount: u64,
    //     pub tx_type: WalletTransactionType,
    // }
    // WalletTransactionType is an enum, but it's probably repr(C) u8 or something.
    // Let's check the size.

    // Actually, we don't need to know the exact size if we know the structure of Transaction:
    // pub struct Transaction<Data, BalanceData> {
    //     pub(crate) id: u64,
    //     pub(crate) wal_location: u64,
    //     data: Data,
    //     ...
    // }

    // The first 8 bytes are id, the next 8 bytes are wal_location.

    let wal_loc1 = u64::from_le_bytes(content[8..16].try_into().unwrap());
    assert_eq!(wal_loc1, 0);

    let tx_size = new_file_size / 2; // Since we have 2 transactions and they are of same size

    let wal_loc2 = u64::from_le_bytes(
        content[(tx_size as usize + 8)..(tx_size as usize + 16)]
            .try_into()
            .unwrap(),
    );
    assert_eq!(wal_loc2, tx_size);

    // Cleanup
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn test_wal_location_persistence() {
    let temp_dir = "temp_wal_persistence_test";
    if Path::new(temp_dir).exists() {
        let _ = fs::remove_dir_all(temp_dir);
    }

    let config = WalletConfig {
        location: Some(temp_dir.to_string()),
        capacity: 100,
        ..Default::default()
    };
    let tx_size: u64;
    {
        let mut wallet = Wallet::new_with_config(config.clone());
        wallet.start();

        // Deposit 1
        wallet.deposit(1, 100);
        wallet.wait_pending_operations();

        let wal_path = Path::new(temp_dir).join("wal.bin");
        tx_size = fs::metadata(&wal_path).unwrap().len();
    } // wallet dropped here

    // Re-open wallet
    {
        let mut wallet = Wallet::new_with_config(config);
        wallet.start();

        // Deposit 2
        wallet.deposit(1, 50);
        wallet.wait_pending_operations();

        let wal_path = Path::new(temp_dir).join("wal.bin");
        let content = fs::read(&wal_path).unwrap();

        assert_eq!(content.len(), (tx_size * 2) as usize);

        let wal_loc2 = u64::from_le_bytes(
            content[(tx_size as usize + 8)..(tx_size as usize + 16)]
                .try_into()
                .unwrap(),
        );
        assert_eq!(wal_loc2, tx_size);
    }

    // Cleanup
    let _ = fs::remove_dir_all(temp_dir);
}
