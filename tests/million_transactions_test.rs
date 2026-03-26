use roda_ledger::wallet::{Wallet, WalletConfig};

#[test]
fn million_deposits_final_balance() {
    let total_txs: u64 = 1_000_000;
    // Use a moderate capacity; consumers run concurrently, so we don't need full size
    let mut wallet = Wallet::new_with_config(WalletConfig {
        queue_size: 1024,
        location: None,
        in_memory: true,
        ..Default::default()
    });
    wallet.start();

    // Generate 1,000,000 deposit transactions of amount 1 into account 1
    for _ in 0..total_txs {
        wallet.deposit(1, 1);
    }

    wallet.wait_pending_operations();

    let balance = wallet.get_balance(1);
    assert_eq!(balance, total_txs as i64);

    let system_balance = wallet.get_balance(0);
    assert_eq!(system_balance, -(total_txs as i64));
}
