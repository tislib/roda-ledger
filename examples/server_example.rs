use roda_ledger::client::Client;
use roda_ledger::ledger::LedgerConfig;
use roda_ledger::server::{Server, ServerConfig};
use roda_ledger::wallet::transaction::WalletTransaction;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:8080".to_string();

    // 1. Initialize Server Config
    let server_config = ServerConfig {
        addr: addr.clone(),
        worker_threads: 1,
        ledger_config: LedgerConfig {
            in_memory: true,
            queue_size: 1024,
            ..Default::default()
        },
    };

    // 2. Start the Server in a background task
    let server = Server::<WalletTransaction>::new(server_config);

    let _server_handle = std::thread::spawn(move || {
        if let Err(e) = server.run() {
            eprintln!("Server error: {}", e);
        }
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 3. Connect as a client using our new Client implementation
    let mut client = Client::<WalletTransaction>::new(addr);
    println!("Client initialized");

    let account_id = 1001;
    let deposit_amount = 5000;

    let tx_id = client
        .register_transaction(WalletTransaction::deposit(account_id, deposit_amount))
        .await?;
    println!(
        "Sent deposit request for account {} with amount {}. Transaction ID: {}",
        account_id, deposit_amount, tx_id
    );

    // 4. Send another transaction (transfer)
    let to_account_id = 1002;
    let transfer_amount = 2000;

    let tx_id_2 = client
        .register_transaction(WalletTransaction::transfer(
            account_id,
            to_account_id,
            transfer_amount,
        ))
        .await?;
    println!(
        "Sent transfer request from {} to {} with amount {}. Transaction ID: {}",
        account_id, to_account_id, transfer_amount, tx_id_2
    );

    // 5. Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 6. Query balances
    let balance_1 = client.get_balance(account_id).await?;
    let balance_2 = client.get_balance(to_account_id).await?;

    println!("Account {} balance: {}", account_id, balance_1);
    println!("Account {} balance: {}", to_account_id, balance_2);

    println!("Example finished.");
    Ok(())
}
