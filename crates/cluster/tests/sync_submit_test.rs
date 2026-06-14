mod tests {
    use ledger::ledger::{Ledger, LedgerConfig};
    use ledger::transactor::transaction::{Operation, WaitLevel as InternalWaitLevel};

    // ---- Ledger API (library-mode) tests ----

    #[test]
    fn test_submit_and_wait_processed_deposit() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        ledger.open_accounts(100); // existence enforcement (ADR-022)

        let result = ledger.submit_and_wait_result(Operation::Deposit {
            account: 1,
            amount: 1000,
            user_ref: 0,
        });

        assert!(result.tx_id() > 0);
        assert!(result.is_success());
    }

    #[test]
    fn test_submit_and_wait_committed_deposit() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        ledger.open_accounts(100); // existence enforcement (ADR-022)

        let result = ledger.submit_and_wait_result(Operation::Deposit {
            account: 1,
            amount: 500,
            user_ref: 0,
        });

        assert!(result.tx_id() > 0);
        assert!(result.is_success());
    }

    #[test]
    fn test_submit_and_wait_snapshotted_deposit() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        ledger.open_accounts(100); // existence enforcement (ADR-022)

        let result = ledger.submit_and_wait_result(Operation::Deposit {
            account: 1,
            amount: 2000,
            user_ref: 0,
        });

        assert!(result.tx_id() > 0);
        assert!(result.is_success());

        // After Snapshotted, balance must reflect the deposit
        let balance = ledger.get_balance(1);
        assert_eq!(balance, 2000);
    }

    #[test]
    fn test_submit_and_wait_rejection_insufficient_funds() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        ledger.open_accounts(100); // existence enforcement (ADR-022)

        // Withdraw from empty account — should be rejected immediately
        let result = ledger.submit_and_wait_result(Operation::Withdrawal {
            account: 1,
            amount: 500,
            user_ref: 0,
        });

        assert!(result.is_err());
    }

    #[test]
    fn test_submit_and_wait_rejection_at_processed_level() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        ledger.open_accounts(100); // existence enforcement (ADR-022)

        let result = ledger.submit_and_wait_result(Operation::Withdrawal {
            account: 99,
            amount: 1,
            user_ref: 0,
        });

        assert!(result.is_err());
    }

    #[test]
    fn test_submit_and_wait_transfer_committed() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        ledger.open_accounts(100); // existence enforcement (ADR-022)

        // Deposit first
        ledger.submit_and_wait(
            Operation::Deposit {
                account: 1,
                amount: 1000,
                user_ref: 0,
            },
            InternalWaitLevel::OnSnapshot,
        );

        let result = ledger.submit_and_wait_result(Operation::Transfer {
            from: 1,
            to: 2,
            amount: 400,
            user_ref: 0,
        });

        assert!(result.is_success());
        assert_eq!(ledger.get_balance(1), 600);
        assert_eq!(ledger.get_balance(2), 400);
    }

    #[test]
    fn test_submit_batch_and_wait() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        ledger.open_accounts(100); // existence enforcement (ADR-022)

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

        let results = ledger.submit_batch_and_wait_result(ops, InternalWaitLevel::OnSnapshot);

        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.is_success());
        }
        assert_eq!(ledger.get_balance(1), 100);
        assert_eq!(ledger.get_balance(2), 200);
        assert_eq!(ledger.get_balance(3), 300);
    }

    #[test]
    fn test_submit_batch_and_wait_with_rejection() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        ledger.open_accounts(100); // existence enforcement (ADR-022)

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

        let results = ledger.submit_batch_and_wait_result(ops, InternalWaitLevel::OnSnapshot);

        assert_eq!(results.len(), 3);
        assert!(results[0].is_success());
        assert!(results[1].is_err());
        assert!(results[2].is_success());
    }

    #[test]
    fn test_submit_batch_and_wait_empty() {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        ledger.open_accounts(100); // existence enforcement (ADR-022)

        let results = ledger.submit_batch_and_wait(vec![], InternalWaitLevel::Committed);
        assert!(results.is_empty());
    }

    // gRPC-level submission coverage moved to client_test.rs, which
    // drives the harness's Standalone server through `client::NodeClient`
    // — the supported entrypoint for tests.
}
