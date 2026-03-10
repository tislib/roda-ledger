use roda_ledger::client::Client;
use roda_ledger::ledger::LedgerConfig;
use roda_ledger::server::{Server, ServerConfig};
use roda_ledger::wallet::balance::WalletBalance;
use roda_ledger::wallet::transaction::WalletTransaction;
use std::time::Duration;

#[tokio::test]
async fn test_server_client_integration() {
    let addr = "127.0.0.1:8081".to_string();

    let server_config = ServerConfig {
        addr: addr.clone(),
        worker_threads: 1,
        ledger_config: LedgerConfig {
            in_memory: true,
            capacity: 1024,
            ..Default::default()
        },
    };

    let server = Server::<WalletTransaction, WalletBalance>::new(server_config);

    tokio::spawn(async move {
        if let Err(e) = server.run_async().await {
            eprintln!("Server error: {}", e);
        }
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = Client::<WalletTransaction, WalletBalance>::new(addr);

    let account_id = 1001;
    let deposit_amount = 5000;

    // 1. Register Transaction
    let tx_id = client
        .register_transaction(WalletTransaction::deposit(account_id, deposit_amount))
        .await
        .expect("Failed to register transaction");

    assert!(tx_id > 0);

    // 2. Get Status
    let mut status = client
        .get_status(tx_id)
        .await
        .expect("Failed to get status");
    println!("Transaction status: {:?}", status);

    // Wait for it to be processed
    let mut attempts = 0;
    while !status.balance_ready() && attempts < 10 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        status = client
            .get_status(tx_id)
            .await
            .expect("Failed to get status");
        attempts += 1;
    }

    // 3. Get Balance
    let balance = client
        .get_balance(account_id)
        .await
        .expect("Failed to get balance");
    assert_eq!(balance.balance, deposit_amount);
}
