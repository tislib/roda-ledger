//! `E2EContext` — the single handle passed to every E2E test.
//!
//! Knows about all running nodes, their gRPC clients, and their runtime
//! state. Adding Raft means `ctx` manages N nodes instead of 1 — the
//! test interface does not change.

use crate::e2e::lib::backend::E2EBackend;
use crate::e2e::lib::backend_inline::InlineNode;
use crate::e2e::lib::backend_process::ProcessNode;
use crate::e2e::lib::profile::Profile;
use roda_ledger::grpc::proto::ledger_client::LedgerClient;
use roda_ledger::grpc::proto::{
    Deposit, GetBalanceRequest, GetBalancesRequest, GetPipelineIndexRequest, SubmitAndWaitRequest,
    SubmitBatchAndWaitRequest, Transfer, WaitLevel, Withdrawal,
};
use tokio::time::{Duration, sleep};
use tonic::transport::Channel;

/// Max operations per gRPC batch to stay within message size limits.
const BATCH_CHUNK_SIZE: usize = 50_000;

// ---------------------------------------------------------------------------
// Node handle — one per backend node
// ---------------------------------------------------------------------------

pub(crate) enum NodeHandle {
    Inline(InlineNode),
    Process(ProcessNode),
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
            E2EBackend::Inline => Self::start_inline_nodes(&profile).await,
            E2EBackend::Process => Self::start_process_nodes(&profile).await,
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

    async fn start_inline_nodes(profile: &Profile) -> Vec<NodeHandle> {
        let mut nodes = Vec::with_capacity(profile.nodes);
        for _ in 0..profile.nodes {
            nodes.push(NodeHandle::Inline(InlineNode::start(profile).await));
        }
        nodes
    }

    // -- Process backend bootstrap ------------------------------------------

    async fn start_process_nodes(profile: &Profile) -> Vec<NodeHandle> {
        let mut nodes = Vec::with_capacity(profile.nodes);
        for _ in 0..profile.nodes {
            nodes.push(NodeHandle::Process(ProcessNode::start(profile).await));
        }
        nodes
    }

    // -- Node access --------------------------------------------------------

    fn client(&self, node: usize) -> LedgerClient<Channel> {
        match &self.nodes[node] {
            NodeHandle::Inline(n) => n.client(),
            NodeHandle::Process(n) => n.client(),
            _ => panic!("client() not supported on this backend"),
        }
    }

    /// Get a cloned gRPC client for direct use (e.g. in spawned tasks).
    pub fn raw_client(&self, node: usize) -> LedgerClient<Channel> {
        self.client(node)
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

    /// Deposit to multiple accounts. Each entry is `(account_id, amount)`.
    /// Large batches are chunked to stay within gRPC message limits.
    /// Returns the last transaction id.
    pub async fn deposit_all(&self, node: usize, deposits: &[(u64, u64)], wait_level: i32) -> u64 {
        let mut last_tx_id = 0u64;

        for chunk in deposits.chunks(BATCH_CHUNK_SIZE) {
            let mut client = self.client(node);
            let operations = chunk
                .iter()
                .map(|(account, amount)| SubmitAndWaitRequest {
                    operation: Some(
                        roda_ledger::grpc::proto::submit_and_wait_request::Operation::Deposit(
                            Deposit {
                                account: *account,
                                amount: *amount,
                                user_ref: 0,
                            },
                        ),
                    ),
                    wait_level: 0,
                })
                .collect();

            let response = client
                .submit_batch_and_wait(SubmitBatchAndWaitRequest {
                    operations,
                    wait_level,
                })
                .await
                .expect("deposit_all RPC failed")
                .into_inner();

            last_tx_id = response
                .results
                .last()
                .expect("empty batch response")
                .transaction_id;
        }

        last_tx_id
    }

    /// Submit a batch of transfers. Each entry is `(from, to, amount)`.
    /// Large batches are chunked to stay within gRPC message limits.
    /// Returns `(last_tx_id, rejected_count)`.
    pub async fn transfer_batch(
        &self,
        node: usize,
        transfers: &[(u64, u64, u64)],
        wait_level: i32,
    ) -> (u64, usize) {
        let mut last_tx_id = 0u64;
        let mut total_rejected = 0usize;

        for chunk in transfers.chunks(BATCH_CHUNK_SIZE) {
            let mut client = self.client(node);
            let operations = chunk
                .iter()
                .map(|(from, to, amount)| SubmitAndWaitRequest {
                    operation: Some(
                        roda_ledger::grpc::proto::submit_and_wait_request::Operation::Transfer(
                            Transfer {
                                from: *from,
                                to: *to,
                                amount: *amount,
                                user_ref: 0,
                            },
                        ),
                    ),
                    wait_level: 0,
                })
                .collect();

            let response = client
                .submit_batch_and_wait(SubmitBatchAndWaitRequest {
                    operations,
                    wait_level,
                })
                .await
                .expect("transfer_batch RPC failed")
                .into_inner();

            total_rejected += response
                .results
                .iter()
                .filter(|r| r.fail_reason != 0)
                .count();
            last_tx_id = response
                .results
                .last()
                .expect("empty batch response")
                .transaction_id;
        }

        (last_tx_id, total_rejected)
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

    /// Get balances for multiple accounts via gRPC `GetBalances`.
    /// Large ranges are chunked. Returns a vec of `(account_id, balance)`.
    pub async fn get_balances(&self, node: usize, account_ids: &[u64]) -> Vec<(u64, i64)> {
        let mut result = Vec::with_capacity(account_ids.len());

        for chunk in account_ids.chunks(BATCH_CHUNK_SIZE) {
            let mut client = self.client(node);
            let response = client
                .get_balances(GetBalancesRequest {
                    account_ids: chunk.to_vec(),
                })
                .await
                .expect("get_balances RPC failed")
                .into_inner();

            for (id, bal) in chunk.iter().zip(response.balances.iter()) {
                result.push((*id, *bal));
            }
        }

        result
    }

    /// Get the sum of all account balances via gRPC `GetBalances`.
    /// Large ranges are chunked to stay within gRPC message limits.
    pub async fn get_balance_sum(&self, node: usize, max_accounts: u64) -> i64 {
        let mut total: i64 = 0;

        for chunk_start in (0..max_accounts).step_by(BATCH_CHUNK_SIZE) {
            let chunk_end = (chunk_start + BATCH_CHUNK_SIZE as u64).min(max_accounts);
            let account_ids: Vec<u64> = (chunk_start..chunk_end).collect();

            let mut client = self.client(node);
            let response = client
                .get_balances(GetBalancesRequest { account_ids })
                .await
                .expect("get_balances RPC failed")
                .into_inner();

            total += response.balances.iter().sum::<i64>();
        }

        total
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

    /// Kill a node (SIGKILL). Only supported on Process backend.
    /// The node cannot serve requests until `restart_node()` is called.
    pub fn kill_node(&mut self, node: usize) {
        match &mut self.nodes[node] {
            NodeHandle::Process(n) => n.kill(),
            NodeHandle::Inline(_) => panic!("kill_node() not supported on Inline backend"),
            _ => panic!("kill_node() not supported on this backend"),
        }
    }

    /// Restart a node after `kill_node()`. Reuses the same data directory —
    /// the server recovers from WAL on startup.
    pub async fn restart_node(&mut self, node: usize) {
        match &mut self.nodes[node] {
            NodeHandle::Process(n) => n.restart().await,
            NodeHandle::Inline(_) => panic!("restart_node() not supported on Inline backend"),
            _ => panic!("restart_node() not supported on this backend"),
        }
    }

    /// Get the current pipeline index (compute, commit, snapshot) for a node.
    pub async fn get_pipeline_index(&self, node: usize) -> (u64, u64, u64) {
        let mut client = self.client(node);
        let response = client
            .get_pipeline_index(GetPipelineIndexRequest {})
            .await
            .expect("get_pipeline_index RPC failed")
            .into_inner();

        (
            response.compute_index,
            response.commit_index,
            response.snapshot_index,
        )
    }

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

    /// Poll `GetPipelineIndex` until `snapshot_index >= tx_id`.
    pub async fn wait_for_snapshot(&self, node: usize, tx_id: u64) {
        let timeout = Duration::from_secs(20);
        let start = tokio::time::Instant::now();

        loop {
            let mut client = self.client(node);
            let response = client
                .get_pipeline_index(GetPipelineIndexRequest {})
                .await
                .expect("get_pipeline_index RPC failed")
                .into_inner();

            if response.snapshot_index >= tx_id {
                return;
            }
            if start.elapsed() >= timeout {
                panic!(
                    "wait_for_snapshot timed out: tx_id={}, snapshot_index={}",
                    tx_id, response.snapshot_index
                );
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
}
