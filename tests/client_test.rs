#[cfg(feature = "grpc")]
mod tests {
    use roda_ledger::client::LedgerClient;
    use roda_ledger::grpc::proto::WaitLevel;
    use roda_ledger::grpc::server::GrpcServer;
    use roda_ledger::ledger::{Ledger, LedgerConfig};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::time::{Duration, sleep};

    async fn setup() -> (Arc<Ledger>, LedgerClient) {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        let ledger = Arc::new(ledger);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        drop(listener);

        let server_ledger = ledger.clone();
        tokio::spawn(async move {
            GrpcServer::new(server_ledger, addr).run().await.unwrap();
        });

        sleep(Duration::from_millis(100)).await;

        let client = LedgerClient::connect(addr).await.unwrap();
        (ledger, client)
    }

    // -- Submit (fire-and-forget) -------------------------------------------

    #[tokio::test]
    async fn test_deposit() {
        let (_ledger, client) = setup().await;
        let tx_id = client.deposit(1, 1000, 0).await.unwrap();
        assert!(tx_id > 0);
    }

    #[tokio::test]
    async fn test_withdraw() {
        let (_ledger, client) = setup().await;
        // Fund first.
        client.deposit_and_wait(1, 2000, 0, WaitLevel::Snapshot).await.unwrap();
        let tx_id = client.withdraw(1, 500, 0).await.unwrap();
        assert!(tx_id > 0);
    }

    #[tokio::test]
    async fn test_transfer() {
        let (_ledger, client) = setup().await;
        client.deposit_and_wait(1, 1000, 0, WaitLevel::Snapshot).await.unwrap();
        let tx_id = client.transfer(1, 2, 400, 0).await.unwrap();
        assert!(tx_id > 0);
    }

    // -- Submit and wait ----------------------------------------------------

    #[tokio::test]
    async fn test_deposit_and_wait_snapshot() {
        let (_ledger, client) = setup().await;
        let result = client
            .deposit_and_wait(1, 500, 0, WaitLevel::Snapshot)
            .await
            .unwrap();

        assert!(result.tx_id > 0);
        assert_eq!(result.fail_reason, 0);

        let balance = client.get_balance(1).await.unwrap();
        assert_eq!(balance.balance, 500);
    }

    #[tokio::test]
    async fn test_withdraw_and_wait_insufficient_funds() {
        let (_ledger, client) = setup().await;
        let result = client
            .withdraw_and_wait(99, 1000, 0, WaitLevel::Committed)
            .await
            .unwrap();

        assert!(result.tx_id > 0);
        assert_eq!(result.fail_reason, 1); // INSUFFICIENT_FUNDS
    }

    #[tokio::test]
    async fn test_transfer_and_wait() {
        let (_ledger, client) = setup().await;
        client
            .deposit_and_wait(1, 1000, 0, WaitLevel::Snapshot)
            .await
            .unwrap();

        let result = client
            .transfer_and_wait(1, 2, 400, 0, WaitLevel::Snapshot)
            .await
            .unwrap();

        assert_eq!(result.fail_reason, 0);

        let b1 = client.get_balance(1).await.unwrap();
        let b2 = client.get_balance(2).await.unwrap();
        assert_eq!(b1.balance, 600);
        assert_eq!(b2.balance, 400);
    }

    // -- Batch operations ---------------------------------------------------

    #[tokio::test]
    async fn test_deposit_batch() {
        let (_ledger, client) = setup().await;
        let tx_ids = client
            .deposit_batch(&[(1, 100, 0), (2, 200, 0), (3, 300, 0)])
            .await
            .unwrap();

        assert_eq!(tx_ids.len(), 3);
        assert!(tx_ids[0] > 0);
        assert!(tx_ids[2] > tx_ids[0]);
    }

    #[tokio::test]
    async fn test_deposit_batch_and_wait() {
        let (_ledger, client) = setup().await;
        let results = client
            .deposit_batch_and_wait(
                &[(1, 100, 0), (2, 200, 0)],
                WaitLevel::Snapshot,
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].fail_reason, 0);
        assert_eq!(results[1].fail_reason, 0);

        let b1 = client.get_balance(1).await.unwrap();
        let b2 = client.get_balance(2).await.unwrap();
        assert_eq!(b1.balance, 100);
        assert_eq!(b2.balance, 200);
    }

    // -- Balance queries ----------------------------------------------------

    #[tokio::test]
    async fn test_get_balance_empty() {
        let (_ledger, client) = setup().await;
        let balance = client.get_balance(42).await.unwrap();
        assert_eq!(balance.balance, 0);
    }

    #[tokio::test]
    async fn test_get_balances() {
        let (_ledger, client) = setup().await;
        client
            .deposit_batch_and_wait(
                &[(10, 100, 0), (11, 200, 0)],
                WaitLevel::Snapshot,
            )
            .await
            .unwrap();

        let balances = client.get_balances(&[10, 11, 12]).await.unwrap();
        assert_eq!(balances, vec![100, 200, 0]);
    }

    // -- Transaction status -------------------------------------------------

    #[tokio::test]
    async fn test_get_transaction_status() {
        let (_ledger, client) = setup().await;
        let result = client
            .deposit_and_wait(1, 1000, 0, WaitLevel::Snapshot)
            .await
            .unwrap();

        let (status, fail_reason) = client
            .get_transaction_status(result.tx_id)
            .await
            .unwrap();

        assert_eq!(status, 3); // ON_SNAPSHOT
        assert_eq!(fail_reason, 0);
    }

    #[tokio::test]
    async fn test_get_transaction_statuses() {
        let (_ledger, client) = setup().await;
        let r1 = client
            .deposit_and_wait(1, 100, 0, WaitLevel::Snapshot)
            .await
            .unwrap();
        let r2 = client
            .deposit_and_wait(2, 200, 0, WaitLevel::Snapshot)
            .await
            .unwrap();

        let statuses = client
            .get_transaction_statuses(&[r1.tx_id, r2.tx_id])
            .await
            .unwrap();

        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].0, 3); // ON_SNAPSHOT
        assert_eq!(statuses[1].0, 3);
    }

    // -- Pipeline index -----------------------------------------------------

    #[tokio::test]
    async fn test_get_pipeline_index() {
        let (_ledger, client) = setup().await;
        let result = client
            .deposit_and_wait(1, 100, 0, WaitLevel::Snapshot)
            .await
            .unwrap();

        let idx = client.get_pipeline_index().await.unwrap();
        assert!(idx.compute >= result.tx_id);
        assert!(idx.commit >= result.tx_id);
        assert!(idx.snapshot >= result.tx_id);
    }

    // -- Transaction query --------------------------------------------------

    #[tokio::test]
    async fn test_get_transaction() {
        let (_ledger, client) = setup().await;
        let result = client
            .deposit_and_wait(5, 777, 0, WaitLevel::Snapshot)
            .await
            .unwrap();

        let tx = client.get_transaction(result.tx_id).await.unwrap();
        assert_eq!(tx.tx_id, result.tx_id);
        assert_eq!(tx.entries.len(), 2); // credit system + debit account
    }

    // -- Account history ----------------------------------------------------

    #[tokio::test]
    async fn test_get_account_history() {
        let (_ledger, client) = setup().await;
        client
            .deposit_and_wait(1, 100, 0, WaitLevel::Snapshot)
            .await
            .unwrap();
        client
            .deposit_and_wait(1, 200, 0, WaitLevel::Snapshot)
            .await
            .unwrap();
        client
            .deposit_and_wait(1, 300, 0, WaitLevel::Snapshot)
            .await
            .unwrap();

        let history = client.get_account_history(1, 0, 10).await.unwrap();
        assert!(!history.entries.is_empty());
    }

    // -- Reconnect after restart --------------------------------------------

    /// Start server, deposit, stop server, restart on same port with same data,
    /// reconnect client, withdraw. Verifies the client can survive a server restart.
    #[tokio::test]
    async fn test_deposit_restart_withdraw() {
        // Use a persistent (non-temp) data dir so WAL survives restart.
        let mut config = LedgerConfig::temp();
        let data_dir = config.storage.data_dir.clone();

        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();
        let ledger = Arc::new(ledger);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        drop(listener);

        let server_ledger = ledger.clone();
        let server_handle = tokio::spawn(async move {
            GrpcServer::new(server_ledger, addr).run().await.unwrap();
        });

        sleep(Duration::from_millis(100)).await;

        // -- Deposit --
        let client = LedgerClient::connect(addr).await.unwrap();
        let result = client
            .deposit_and_wait(1, 1000, 0, WaitLevel::Snapshot)
            .await
            .unwrap();
        assert_eq!(result.fail_reason, 0);

        let balance = client.get_balance(1).await.unwrap();
        assert_eq!(balance.balance, 1000);

        // -- Stop server (drop ledger + abort task) --
        server_handle.abort();
        drop(ledger);

        // Small delay for port to free up.
        sleep(Duration::from_millis(200)).await;

        // -- Restart server on same port, same data dir --
        let mut config2 = LedgerConfig::temp();
        config2.storage.data_dir = data_dir.clone();

        let mut ledger2 = Ledger::new(config2);
        ledger2.start().unwrap();
        let ledger2 = Arc::new(ledger2);

        let server_ledger2 = ledger2.clone();
        tokio::spawn(async move {
            GrpcServer::new(server_ledger2, addr).run().await.unwrap();
        });

        sleep(Duration::from_millis(100)).await;

        // -- Reconnect and withdraw --
        let client2 = LedgerClient::connect(addr).await.unwrap();

        let balance = client2.get_balance(1).await.unwrap();
        assert_eq!(balance.balance, 1000, "balance not recovered after restart");

        let result = client2
            .withdraw_and_wait(1, 300, 0, WaitLevel::Snapshot)
            .await
            .unwrap();
        assert_eq!(result.fail_reason, 0);

        let balance = client2.get_balance(1).await.unwrap();
        assert_eq!(balance.balance, 700);

        // Cleanup data dir.
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
