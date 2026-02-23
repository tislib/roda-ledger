use roda_ledger::wallet::{Wallet, WalletConfig};

#[test]
fn million_deposits_final_balance() {
    let total_txs: u64 = 1_000_000;
    // Use a moderate capacity; consumers run concurrently, so we don't need full size
    let mut wallet = Wallet::new_with_config(WalletConfig {
        capacity: 1024,
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
    assert_eq!(balance.balance, total_txs);
}
