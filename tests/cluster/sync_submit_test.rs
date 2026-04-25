#[cfg(feature = "cluster")]
mod tests {
    use roda_ledger::cluster::proto::ledger::ledger_client::LedgerClient;
    use roda_ledger::cluster::proto::ledger::{
        Deposit, SubmitAndWaitRequest, SubmitBatchAndWaitRequest, Transfer, WaitLevel, Withdrawal,
    };
    use roda_ledger::cluster::{ClusterCommitIndex, Role, RoleFlag, Server, Term};
    use roda_ledger::ledger::{Ledger, LedgerConfig};
    use roda_ledger::transaction::{Operation, WaitLevel as InternalWaitLevel};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    // ---- Ledger API (library-mode) tests ----

    #[test]
    fn test_submit_and_wait_processed_deposit() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();

        let result = ledger.submit_and_wait(
            Operation::Deposit {
                account: 1,
                amount: 1000,
                user_ref: 0,
            },
            InternalWaitLevel::Computed,
        );

        assert!(result.tx_id > 0);
        assert!(result.fail_reason.is_success());
    }

    #[test]
    fn test_submit_and_wait_committed_deposit() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();

        let result = ledger.submit_and_wait(
            Operation::Deposit {
                account: 1,
                amount: 500,
                user_ref: 0,
            },
            InternalWaitLevel::Committed,
        );

        assert!(result.tx_id > 0);
        assert!(result.fail_reason.is_success());
    }

    #[test]
    fn test_submit_and_wait_snapshotted_deposit() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();

        let result = ledger.submit_and_wait(
            Operation::Deposit {
                account: 1,
                amount: 2000,
                user_ref: 0,
            },
            InternalWaitLevel::OnSnapshot,
        );

        assert!(result.tx_id > 0);
        assert!(result.fail_reason.is_success());

        // After Snapshotted, balance must reflect the deposit
        let balance = ledger.get_balance(1);
        assert_eq!(balance, 2000);
    }

    #[test]
    fn test_submit_and_wait_rejection_insufficient_funds() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();

        // Withdraw from empty account — should be rejected immediately
        let result = ledger.submit_and_wait(
            Operation::Withdrawal {
                account: 1,
                amount: 500,
                user_ref: 0,
            },
            InternalWaitLevel::Committed,
        );

        assert!(result.fail_reason.is_failure());
    }

    #[test]
    fn test_submit_and_wait_rejection_at_processed_level() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();

        let result = ledger.submit_and_wait(
            Operation::Withdrawal {
                account: 99,
                amount: 1,
                user_ref: 0,
            },
            InternalWaitLevel::Computed,
        );

        assert!(result.fail_reason.is_failure());
    }

    #[test]
    fn test_submit_and_wait_transfer_committed() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();

        // Deposit first
        ledger.submit_and_wait(
            Operation::Deposit {
                account: 1,
                amount: 1000,
                user_ref: 0,
            },
            InternalWaitLevel::OnSnapshot,
        );

        let result = ledger.submit_and_wait(
            Operation::Transfer {
                from: 1,
                to: 2,
                amount: 400,
                user_ref: 0,
            },
            InternalWaitLevel::OnSnapshot,
        );

        assert!(result.fail_reason.is_success());
        assert_eq!(ledger.get_balance(1), 600);
        assert_eq!(ledger.get_balance(2), 400);
    }

    #[test]
    fn test_submit_batch_and_wait() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();

        let ops = vec![
            Operation::Deposit {
                account: 1,
                amount: 100,
                user_ref: 0,
            },
            Operation::Deposit {
                account: 2,
                amount: 200,
                user_ref: 0,
            },
            Operation::Deposit {
                account: 3,
                amount: 300,
                user_ref: 0,
            },
        ];

        let results = ledger.submit_batch_and_wait(ops, InternalWaitLevel::OnSnapshot);

        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.fail_reason.is_success());
        }
        assert_eq!(ledger.get_balance(1), 100);
        assert_eq!(ledger.get_balance(2), 200);
        assert_eq!(ledger.get_balance(3), 300);
    }

    #[test]
    fn test_submit_batch_and_wait_with_rejection() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();

        let ops = vec![
            Operation::Deposit {
                account: 1,
                amount: 100,
                user_ref: 0,
            },
            Operation::Withdrawal {
                account: 99,
                amount: 999,
                user_ref: 0,
            },
            Operation::Deposit {
                account: 2,
                amount: 200,
                user_ref: 0,
            },
        ];

        let results = ledger.submit_batch_and_wait(ops, InternalWaitLevel::OnSnapshot);

        assert_eq!(results.len(), 3);
        assert!(results[0].fail_reason.is_success());
        assert!(results[1].fail_reason.is_failure());
        assert!(results[2].fail_reason.is_success());
    }

    #[test]
    fn test_submit_batch_and_wait_empty() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();

        let results = ledger.submit_batch_and_wait(vec![], InternalWaitLevel::Committed);
        assert!(results.is_empty());
    }

    // ---- gRPC tests ----

    async fn setup_grpc_server() -> (Arc<Ledger>, SocketAddr) {
        let cfg = LedgerConfig::temp();
        let data_dir = cfg.storage.data_dir.clone();
        let mut ledger = Ledger::new(cfg);
        ledger.start().unwrap();
        let ledger = Arc::new(ledger);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let server_ledger = ledger.clone();
        let term = Arc::new(Term::open_in_dir(&data_dir).unwrap());
        let cci = ClusterCommitIndex::from_ledger(&ledger);
        tokio::spawn(async move {
            let server = Server::new(server_ledger, addr, std::sync::Arc::new(RoleFlag::new(Role::Leader)), term, cci);
            server.run().await.unwrap();
        });

        sleep(Duration::from_millis(100)).await;

        (ledger, addr)
    }

    #[tokio::test]
    async fn test_grpc_submit_and_wait_deposit_committed() {
        let (_ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let request = SubmitAndWaitRequest {
            operation: Some(
                roda_ledger::cluster::proto::ledger::submit_and_wait_request::Operation::Deposit(
                    Deposit {
                        account: 1,
                        amount: 1000,
                        user_ref: 0,
                    },
                ),
            ),
            wait_level: WaitLevel::Committed as i32,
        };

        let response = client.submit_and_wait(request).await.unwrap().into_inner();

        assert!(response.transaction_id > 0);
        assert_eq!(response.fail_reason, 0);
    }

    #[tokio::test]
    async fn test_grpc_submit_and_wait_deposit_snapshotted() {
        let (ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let request = SubmitAndWaitRequest {
            operation: Some(
                roda_ledger::cluster::proto::ledger::submit_and_wait_request::Operation::Deposit(
                    Deposit {
                        account: 5,
                        amount: 500,
                        user_ref: 0,
                    },
                ),
            ),
            wait_level: WaitLevel::Snapshot as i32,
        };

        let response = client.submit_and_wait(request).await.unwrap().into_inner();

        assert!(response.transaction_id > 0);
        assert_eq!(response.fail_reason, 0);

        // Balance must already be visible since we waited for snapshot
        let balance = ledger.get_balance(5);
        assert_eq!(balance, 500);
    }

    #[tokio::test]
    async fn test_grpc_submit_and_wait_rejection() {
        let (_ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let request = SubmitAndWaitRequest {
            operation: Some(
                roda_ledger::cluster::proto::ledger::submit_and_wait_request::Operation::Withdrawal(
                    Withdrawal {
                        account: 100,
                        amount: 999,
                        user_ref: 0,
                    },
                ),
            ),
            wait_level: WaitLevel::Committed as i32,
        };

        let response = client.submit_and_wait(request).await.unwrap().into_inner();

        assert!(response.transaction_id > 0);
        // INSUFFICIENT_FUNDS = 1
        assert_eq!(response.fail_reason, 1);
    }

    #[tokio::test]
    async fn test_grpc_submit_and_wait_transfer() {
        let (ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        // Deposit first
        client
            .submit_and_wait(SubmitAndWaitRequest {
                operation: Some(
                    roda_ledger::cluster::proto::ledger::submit_and_wait_request::Operation::Deposit(
                        Deposit {
                            account: 1,
                            amount: 1000,
                            user_ref: 0,
                        },
                    ),
                ),
                wait_level: WaitLevel::Snapshot as i32,
            })
            .await
            .unwrap();

        let response = client
            .submit_and_wait(SubmitAndWaitRequest {
                operation: Some(
                    roda_ledger::cluster::proto::ledger::submit_and_wait_request::Operation::Transfer(
                        Transfer {
                            from: 1,
                            to: 2,
                            amount: 400,
                            user_ref: 0,
                        },
                    ),
                ),
                wait_level: WaitLevel::Snapshot as i32,
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fail_reason, 0);
        assert_eq!(ledger.get_balance(1), 600);
        assert_eq!(ledger.get_balance(2), 400);
    }

    #[tokio::test]
    async fn test_grpc_submit_batch_and_wait() {
        let (ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let request = SubmitBatchAndWaitRequest {
            operations: vec![
                SubmitAndWaitRequest {
                    operation: Some(
                        roda_ledger::cluster::proto::ledger::submit_and_wait_request::Operation::Deposit(
                            Deposit {
                                account: 1,
                                amount: 100,
                                user_ref: 0,
                            },
                        ),
                    ),
                    wait_level: 0,
                },
                SubmitAndWaitRequest {
                    operation: Some(
                        roda_ledger::cluster::proto::ledger::submit_and_wait_request::Operation::Deposit(
                            Deposit {
                                account: 2,
                                amount: 200,
                                user_ref: 0,
                            },
                        ),
                    ),
                    wait_level: 0,
                },
            ],
            wait_level: WaitLevel::Snapshot as i32,
        };

        let response = client
            .submit_batch_and_wait(request)
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].fail_reason, 0);
        assert_eq!(response.results[1].fail_reason, 0);

        assert_eq!(ledger.get_balance(1), 100);
        assert_eq!(ledger.get_balance(2), 200);
    }

    #[tokio::test]
    async fn test_grpc_submit_batch_and_wait_with_rejection() {
        let (_ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let request = SubmitBatchAndWaitRequest {
            operations: vec![
                SubmitAndWaitRequest {
                    operation: Some(
                        roda_ledger::cluster::proto::ledger::submit_and_wait_request::Operation::Deposit(
                            Deposit {
                                account: 1,
                                amount: 100,
                                user_ref: 0,
                            },
                        ),
                    ),
                    wait_level: 0,
                },
                SubmitAndWaitRequest {
                    operation: Some(
                        roda_ledger::cluster::proto::ledger::submit_and_wait_request::Operation::Withdrawal(
                            Withdrawal {
                                account: 99,
                                amount: 999,
                                user_ref: 0,
                            },
                        ),
                    ),
                    wait_level: 0,
                },
            ],
            wait_level: WaitLevel::Snapshot as i32,
        };

        let response = client
            .submit_batch_and_wait(request)
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].fail_reason, 0); // success
        assert_eq!(response.results[1].fail_reason, 1); // INSUFFICIENT_FUNDS
    }

    #[tokio::test]
    async fn test_grpc_submit_and_wait_computed_level() {
        let (_ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let request = SubmitAndWaitRequest {
            operation: Some(
                roda_ledger::cluster::proto::ledger::submit_and_wait_request::Operation::Deposit(
                    Deposit {
                        account: 1,
                        amount: 100,
                        user_ref: 0,
                    },
                ),
            ),
            wait_level: WaitLevel::Computed as i32,
        };

        let response = client.submit_and_wait(request).await.unwrap().into_inner();

        assert!(response.transaction_id > 0);
        assert_eq!(response.fail_reason, 0);
    }
}
