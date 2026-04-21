#[cfg(feature = "grpc")]
mod tests {
    use roda_ledger::grpc::GrpcServer;
    use roda_ledger::grpc::proto::ledger_client::LedgerClient;
    use roda_ledger::grpc::proto::{
        Deposit, GetBalanceRequest, GetBalancesRequest, GetPipelineIndexRequest, GetStatusRequest,
        GetStatusesRequest, SubmitBatchRequest, SubmitOperationRequest, Transfer, Withdrawal,
    };
    use roda_ledger::ledger::{Ledger, LedgerConfig};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    async fn setup_grpc_server() -> (Arc<Ledger>, SocketAddr) {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        let ledger = Arc::new(ledger);

        // Find a free port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let server_ledger = ledger.clone();
        tokio::spawn(async move {
            let server = GrpcServer::new(server_ledger, addr);
            server.run().await.unwrap();
        });

        // Give the server a moment to start
        sleep(Duration::from_millis(100)).await;

        (ledger, addr)
    }

    #[tokio::test]
    async fn test_grpc_submit_operation_deposit() {
        let (ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let request = SubmitOperationRequest {
            operation: Some(
                roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(Deposit {
                    account: 1,
                    amount: 1000,
                    user_ref: 123,
                }),
            ),
        };

        let response = client.submit_operation(request).await.unwrap().into_inner();
        let tx_id = response.transaction_id;

        assert!(tx_id > 0);

        // Wait for processing
        ledger.wait_for_transaction(tx_id);

        let balance = ledger.get_balance(1);
        assert_eq!(balance, 1000);
    }

    #[tokio::test]
    async fn test_grpc_submit_operation_withdrawal() {
        let (ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        // Deposit first
        client
            .submit_operation(SubmitOperationRequest {
                operation: Some(
                    roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                        Deposit {
                            account: 1,
                            amount: 2000,
                            user_ref: 1,
                        },
                    ),
                ),
            })
            .await
            .unwrap();

        let request = SubmitOperationRequest {
            operation: Some(
                roda_ledger::grpc::proto::submit_operation_request::Operation::Withdrawal(
                    Withdrawal {
                        account: 1,
                        amount: 500,
                        user_ref: 2,
                    },
                ),
            ),
        };

        let response = client.submit_operation(request).await.unwrap().into_inner();
        let tx_id = response.transaction_id;

        // Wait for processing
        ledger.wait_for_transaction(tx_id);

        let balance = ledger.get_balance(1);
        assert_eq!(balance, 1500);
    }

    #[tokio::test]
    async fn test_grpc_submit_operation_transfer() {
        let (ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        // Deposit to account 1
        client
            .submit_operation(SubmitOperationRequest {
                operation: Some(
                    roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                        Deposit {
                            account: 1,
                            amount: 1000,
                            user_ref: 1,
                        },
                    ),
                ),
            })
            .await
            .unwrap();

        let request = SubmitOperationRequest {
            operation: Some(
                roda_ledger::grpc::proto::submit_operation_request::Operation::Transfer(Transfer {
                    from: 1,
                    to: 2,
                    amount: 400,
                    user_ref: 2,
                }),
            ),
        };

        let response = client.submit_operation(request).await.unwrap().into_inner();
        let tx_id = response.transaction_id;

        // Wait for processing
        ledger.wait_for_transaction(tx_id);

        assert_eq!(ledger.get_balance(1), 600);
        assert_eq!(ledger.get_balance(2), 400);
    }

    #[tokio::test]
    async fn test_grpc_submit_batch() {
        let (ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let request = SubmitBatchRequest {
            operations: vec![
                SubmitOperationRequest {
                    operation: Some(
                        roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                            Deposit {
                                account: 1,
                                amount: 100,
                                user_ref: 1,
                            },
                        ),
                    ),
                },
                SubmitOperationRequest {
                    operation: Some(
                        roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                            Deposit {
                                account: 2,
                                amount: 200,
                                user_ref: 2,
                            },
                        ),
                    ),
                },
            ],
        };

        let response = client.submit_batch(request).await.unwrap().into_inner();
        assert_eq!(response.results.len(), 2);

        let last_tx_id = response.results[1].transaction_id;

        // Wait for processing
        ledger.wait_for_transaction(last_tx_id);

        assert_eq!(ledger.get_balance(1), 100);
        assert_eq!(ledger.get_balance(2), 200);
    }

    #[tokio::test]
    async fn test_grpc_get_balance() {
        let (_ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        // Deposit
        client
            .submit_operation(SubmitOperationRequest {
                operation: Some(
                    roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                        Deposit {
                            account: 5,
                            amount: 500,
                            user_ref: 1,
                        },
                    ),
                ),
            })
            .await
            .unwrap();

        // Poll for balance until it's updated (as it reflects snapshot)
        let mut balance = 0;
        for _ in 0..20 {
            let response = client
                .get_balance(GetBalanceRequest { account_id: 5 })
                .await
                .unwrap()
                .into_inner();
            balance = response.balance;
            if balance == 500 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(balance, 500);
    }

    #[tokio::test]
    async fn test_grpc_get_balances() {
        let (_ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        // Deposits
        client
            .submit_batch(SubmitBatchRequest {
                operations: vec![
                    SubmitOperationRequest {
                        operation: Some(
                            roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                                Deposit {
                                    account: 10,
                                    amount: 100,
                                    user_ref: 1,
                                },
                            ),
                        ),
                    },
                    SubmitOperationRequest {
                        operation: Some(
                            roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                                Deposit {
                                    account: 11,
                                    amount: 200,
                                    user_ref: 2,
                                },
                            ),
                        ),
                    },
                ],
            })
            .await
            .unwrap();

        let mut balances = vec![];
        for _ in 0..20 {
            let response = client
                .get_balances(GetBalancesRequest {
                    account_ids: vec![10, 11],
                })
                .await
                .unwrap()
                .into_inner();
            balances = response.balances;
            if balances == vec![100, 200] {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(balances, vec![100, 200]);
    }

    #[tokio::test]
    async fn test_grpc_get_status_and_statuses() {
        let (_ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let res1 = client
            .submit_operation(SubmitOperationRequest {
                operation: Some(
                    roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                        Deposit {
                            account: 1,
                            amount: 1000,
                            user_ref: 1,
                        },
                    ),
                ),
            })
            .await
            .unwrap()
            .into_inner();

        let tx_id1 = res1.transaction_id;

        let mut status = 0;
        for _ in 0..20 {
            let response = client
                .get_transaction_status(GetStatusRequest {
                    transaction_id: tx_id1,
                    term: 0,
                })
                .await
                .unwrap()
                .into_inner();
            status = response.status;
            // 3 is ON_SNAPSHOT
            if status == 3 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(status, 3);

        // Test multi statuses
        let response = client
            .get_transaction_statuses(GetStatusesRequest {
                transaction_ids: vec![tx_id1],
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].status, 3);
    }

    #[tokio::test]
    async fn test_grpc_get_pipeline_index() {
        let (_ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let res = client
            .submit_operation(SubmitOperationRequest {
                operation: Some(
                    roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                        Deposit {
                            account: 1,
                            amount: 1000,
                            user_ref: 1,
                        },
                    ),
                ),
            })
            .await
            .unwrap()
            .into_inner();

        let tx_id = res.transaction_id;

        let mut snapshot_index = 0;
        for _ in 0..20 {
            let response = client
                .get_pipeline_index(GetPipelineIndexRequest {})
                .await
                .unwrap()
                .into_inner();
            snapshot_index = response.snapshot_index;
            if snapshot_index >= tx_id {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        assert!(snapshot_index >= tx_id);
    }

    #[tokio::test]
    async fn test_grpc_failed_operation() {
        let (_ledger, addr) = setup_grpc_server().await;
        let mut client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        // Withdraw from empty account
        let res = client
            .submit_operation(SubmitOperationRequest {
                operation: Some(
                    roda_ledger::grpc::proto::submit_operation_request::Operation::Withdrawal(
                        Withdrawal {
                            account: 100,
                            amount: 1000,
                            user_ref: 1,
                        },
                    ),
                ),
            })
            .await
            .unwrap()
            .into_inner();

        let tx_id = res.transaction_id;

        let mut status = 0;
        let mut fail_reason = 0;
        for _ in 0..20 {
            let response = client
                .get_transaction_status(GetStatusRequest {
                    transaction_id: tx_id,
                    term: 0,
                })
                .await
                .unwrap()
                .into_inner();
            status = response.status;
            fail_reason = response.fail_reason;
            // 4 is ERROR
            if status == 4 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(status, 4);
        // 1 is INSUFFICIENT_FUNDS
        assert_eq!(fail_reason, 1);
    }
}
