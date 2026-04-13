//! `E2EContext` — the single handle passed to every E2E test.
//!
//! Knows about all running nodes, their gRPC clients, and their runtime
//! state. Adding Raft means `ctx` manages N nodes instead of 1 — the
//! test interface does not change.

use crate::e2e::lib::backend::E2EBackend;
use crate::e2e::lib::backend_inline::InlineNode;
use crate::e2e::lib::profile::Profile;
use roda_ledger::grpc::proto::ledger_client::LedgerClient;
use roda_ledger::grpc::proto::{
    Deposit, GetBalanceRequest, GetBalancesRequest, GetPipelineIndexRequest, SubmitAndWaitRequest,
    SubmitBatchAndWaitRequest, Transfer, WaitLevel, Withdrawal,
};
use tokio::time::{Duration, sleep};
use tonic::transport::Channel;

// ---------------------------------------------------------------------------
// Node handle — one per backend node
// ---------------------------------------------------------------------------

pub(crate) enum NodeHandle {
    Inline(InlineNode),
    #[allow(dead_code)]
    Process,
    #[allow(dead_code)]
    Docker,
    #[allow(dead_code)]
    Cloud,
}

// ---------------------------------------------------------------------------
// E2EContext
// ---------------------------------------------------------------------------

pub struct E2EContext {
    pub backend: E2EBackend,
    pub profile: Profile,
    nodes: Vec<NodeHandle>,
}

impl E2EContext {
    /// Boot an E2E context for the given profile.
    ///
    /// Starts `profile.nodes` ledger instances using the active backend,
    /// each with its own gRPC server and connected client.
    pub async fn new(profile: Profile) -> Self {
        let backend = E2EBackend::from_env();
        let nodes = match backend {
            E2EBackend::Inline => Self::start_inline_nodes(profile.nodes).await,
            E2EBackend::Process => todo!("Process backend not yet implemented"),
            E2EBackend::Docker => todo!("Docker backend not yet implemented"),
            E2EBackend::Cloud => todo!("Cloud backend not yet implemented"),
        };

        Self {
            backend,
            profile,
            nodes,
        }
    }

    // -- Inline backend bootstrap -------------------------------------------

    async fn start_inline_nodes(count: usize) -> Vec<NodeHandle> {
        let mut nodes = Vec::with_capacity(count);
        for _ in 0..count {
            nodes.push(NodeHandle::Inline(InlineNode::start().await));
        }
        nodes
    }

    // -- Node access --------------------------------------------------------

    fn client(&self, node: usize) -> LedgerClient<Channel> {
        match &self.nodes[node] {
            NodeHandle::Inline(n) => n.client(),
            _ => panic!("client() not supported on this backend"),
        }
    }

    // -- Actions ------------------------------------------------------------

    /// Submit a deposit via gRPC `SubmitAndWait`. Returns the transaction id.
    pub async fn deposit(&self, node: usize, account: u64, amount: u64, wait_level: i32) -> u64 {
        let mut client = self.client(node);
        let response = client
            .submit_and_wait(SubmitAndWaitRequest {
                operation: Some(
                    roda_ledger::grpc::proto::submit_and_wait_request::Operation::Deposit(
                        Deposit {
                            account,
                            amount,
                            user_ref: 0,
                        },
                    ),
                ),
                wait_level,
            })
            .await
            .expect("deposit RPC failed")
            .into_inner();

        response.transaction_id
    }

    /// Submit a withdrawal via gRPC `SubmitAndWait`. Returns the transaction id.
    pub async fn withdraw(&self, node: usize, account: u64, amount: u64, wait_level: i32) -> u64 {
        let mut client = self.client(node);
        let response = client
            .submit_and_wait(SubmitAndWaitRequest {
                operation: Some(
                    roda_ledger::grpc::proto::submit_and_wait_request::Operation::Withdrawal(
                        Withdrawal {
                            account,
                            amount,
                            user_ref: 0,
                        },
                    ),
                ),
                wait_level,
            })
            .await
            .expect("withdraw RPC failed")
            .into_inner();

        response.transaction_id
    }

    /// Submit a transfer via gRPC `SubmitAndWait`. Returns the transaction id.
    pub async fn transfer(
        &self,
        node: usize,
        from: u64,
        to: u64,
        amount: u64,
        wait_level: i32,
    ) -> u64 {
        let mut client = self.client(node);
        let response = client
            .submit_and_wait(SubmitAndWaitRequest {
                operation: Some(
                    roda_ledger::grpc::proto::submit_and_wait_request::Operation::Transfer(
                        Transfer {
                            from,
                            to,
                            amount,
                            user_ref: 0,
                        },
                    ),
                ),
                wait_level,
            })
            .await
            .expect("transfer RPC failed")
            .into_inner();

        response.transaction_id
    }

    /// Submit N identical deposits via gRPC `SubmitBatchAndWait`.
    /// Returns the last transaction id.
    pub async fn batch_deposit(
        &self,
        node: usize,
        account: u64,
        amount: u64,
        count: usize,
        wait_level: i32,
    ) -> u64 {
        let mut client = self.client(node);
        let operations = (0..count)
            .map(|_| SubmitAndWaitRequest {
                operation: Some(
                    roda_ledger::grpc::proto::submit_and_wait_request::Operation::Deposit(
                        Deposit {
                            account,
                            amount,
                            user_ref: 0,
                        },
                    ),
                ),
                wait_level: 0, // individual wait level ignored in batch
            })
            .collect();

        let response = client
            .submit_batch_and_wait(SubmitBatchAndWaitRequest {
                operations,
                wait_level,
            })
            .await
            .expect("batch_deposit RPC failed")
            .into_inner();

        response
            .results
            .last()
            .expect("empty batch response")
            .transaction_id
    }

    // -- Reading ------------------------------------------------------------

    /// Get the balance for an account via gRPC `GetBalance`.
    pub async fn get_balance(&self, node: usize, account: u64) -> i64 {
        let mut client = self.client(node);
        let response = client
            .get_balance(GetBalanceRequest {
                account_id: account,
            })
            .await
            .expect("get_balance RPC failed")
            .into_inner();

        response.balance
    }

    /// Get the sum of all account balances via gRPC `GetBalances`.
    pub async fn get_balance_sum(&self, node: usize, max_accounts: u64) -> i64 {
        let mut client = self.client(node);
        let account_ids: Vec<u64> = (0..max_accounts).collect();
        let response = client
            .get_balances(GetBalancesRequest { account_ids })
            .await
            .expect("get_balances RPC failed")
            .into_inner();

        response.balances.iter().sum()
    }

    /// Get the last committed transaction id via gRPC `GetPipelineIndex`.
    pub async fn get_last_committed_id(&self, node: usize) -> u64 {
        let mut client = self.client(node);
        let response = client
            .get_pipeline_index(GetPipelineIndexRequest {})
            .await
            .expect("get_pipeline_index RPC failed")
            .into_inner();

        response.commit_index
    }

    // -- Runtime intervention -----------------------------------------------

    /// Poll `GetPipelineIndex` until `commit_index >= tx_id`.
    pub async fn wait_until_committed(&self, node: usize, tx_id: u64) {
        let timeout = Duration::from_secs(20);
        let start = tokio::time::Instant::now();

        loop {
            let committed = self.get_last_committed_id(node).await;
            if committed >= tx_id {
                return;
            }
            if start.elapsed() >= timeout {
                panic!(
                    "wait_until_committed timed out: tx_id={}, last_committed={}",
                    tx_id, committed
                );
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
}
