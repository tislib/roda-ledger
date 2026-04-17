//! High-level gRPC client for roda-ledger.
//!
//! `LedgerClient` is a thin facade over the generated tonic client.
//! It provides ergonomic Rust types, hides proto construction, and
//! exposes every RPC as a simple async method.

use crate::grpc::proto;
use crate::grpc::proto::ledger_client::LedgerClient as TonicLedgerClient;
use std::net::SocketAddr;
use tonic::transport::Channel;

/// Result type for client operations.
pub type Result<T> = std::result::Result<T, tonic::Status>;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Result of a submitted operation.
#[derive(Debug, Clone)]
pub struct SubmitResult {
    pub tx_id: u64,
    pub fail_reason: u32,
}

/// Pipeline progress indices.
#[derive(Debug, Clone)]
pub struct PipelineIndex {
    pub compute: u64,
    pub commit: u64,
    pub snapshot: u64,
}

/// Balance for a single account.
#[derive(Debug, Clone)]
pub struct Balance {
    pub balance: i64,
    pub last_snapshot_tx_id: u64,
}

/// A single entry within a transaction.
#[derive(Debug, Clone)]
pub struct TxEntry {
    pub account_id: u64,
    pub amount: u64,
    pub kind: i32,
    pub computed_balance: i64,
}

/// A link between transactions.
#[derive(Debug, Clone)]
pub struct TxLink {
    pub to_tx_id: u64,
    pub kind: i32,
}

/// Full transaction details.
#[derive(Debug, Clone)]
pub struct Transaction {
    pub tx_id: u64,
    pub entries: Vec<TxEntry>,
    pub links: Vec<TxLink>,
}

/// A single entry in account history.
pub type HistoryEntry = TxEntry;

/// Account history response.
#[derive(Debug, Clone)]
pub struct AccountHistory {
    pub entries: Vec<HistoryEntry>,
    pub next_tx_id: u64,
}

/// Metadata for a single registered WASM function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionInfo {
    pub name: String,
    pub version: u16,
    pub crc32c: u32,
}

// ---------------------------------------------------------------------------
// LedgerClient
// ---------------------------------------------------------------------------

/// High-level client for the roda-ledger gRPC API.
///
/// Wraps the tonic-generated client with ergonomic methods.
/// All methods take `&self` — the underlying channel is cloneable.
#[derive(Clone)]
pub struct LedgerClient {
    inner: TonicLedgerClient<Channel>,
}

impl LedgerClient {
    /// Connect to a roda-ledger server at the given address.
    pub async fn connect(addr: SocketAddr) -> std::result::Result<Self, tonic::transport::Error> {
        let inner = TonicLedgerClient::connect(format!("http://{}", addr)).await?;
        Ok(Self { inner })
    }

    /// Connect to a roda-ledger server at the given URL string (e.g. `http://127.0.0.1:50051`).
    pub async fn connect_url(url: &str) -> std::result::Result<Self, tonic::transport::Error> {
        let inner = TonicLedgerClient::connect(url.to_string()).await?;
        Ok(Self { inner })
    }

    // -- Submit operations --------------------------------------------------

    /// Submit a deposit (fire-and-forget). Returns the transaction ID.
    pub async fn deposit(&self, account: u64, amount: u64, user_ref: u64) -> Result<u64> {
        let mut client = self.inner.clone();
        let resp = client
            .submit_operation(proto::SubmitOperationRequest {
                operation: Some(proto::submit_operation_request::Operation::Deposit(
                    proto::Deposit {
                        account,
                        amount,
                        user_ref,
                    },
                )),
            })
            .await?
            .into_inner();
        Ok(resp.transaction_id)
    }

    /// Submit a withdrawal (fire-and-forget). Returns the transaction ID.
    pub async fn withdraw(&self, account: u64, amount: u64, user_ref: u64) -> Result<u64> {
        let mut client = self.inner.clone();
        let resp = client
            .submit_operation(proto::SubmitOperationRequest {
                operation: Some(proto::submit_operation_request::Operation::Withdrawal(
                    proto::Withdrawal {
                        account,
                        amount,
                        user_ref,
                    },
                )),
            })
            .await?
            .into_inner();
        Ok(resp.transaction_id)
    }

    /// Submit a transfer (fire-and-forget). Returns the transaction ID.
    pub async fn transfer(&self, from: u64, to: u64, amount: u64, user_ref: u64) -> Result<u64> {
        let mut client = self.inner.clone();
        let resp = client
            .submit_operation(proto::SubmitOperationRequest {
                operation: Some(proto::submit_operation_request::Operation::Transfer(
                    proto::Transfer {
                        from,
                        to,
                        amount,
                        user_ref,
                    },
                )),
            })
            .await?
            .into_inner();
        Ok(resp.transaction_id)
    }

    // -- Submit and wait ----------------------------------------------------

    /// Submit a deposit and wait until it reaches the given pipeline level.
    pub async fn deposit_and_wait(
        &self,
        account: u64,
        amount: u64,
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        let mut client = self.inner.clone();
        let resp = client
            .submit_and_wait(proto::SubmitAndWaitRequest {
                operation: Some(proto::submit_and_wait_request::Operation::Deposit(
                    proto::Deposit {
                        account,
                        amount,
                        user_ref,
                    },
                )),
                wait_level: wait_level as i32,
            })
            .await?
            .into_inner();
        Ok(SubmitResult {
            tx_id: resp.transaction_id,
            fail_reason: resp.fail_reason,
        })
    }

    /// Submit a withdrawal and wait.
    pub async fn withdraw_and_wait(
        &self,
        account: u64,
        amount: u64,
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        let mut client = self.inner.clone();
        let resp = client
            .submit_and_wait(proto::SubmitAndWaitRequest {
                operation: Some(proto::submit_and_wait_request::Operation::Withdrawal(
                    proto::Withdrawal {
                        account,
                        amount,
                        user_ref,
                    },
                )),
                wait_level: wait_level as i32,
            })
            .await?
            .into_inner();
        Ok(SubmitResult {
            tx_id: resp.transaction_id,
            fail_reason: resp.fail_reason,
        })
    }

    /// Submit a transfer and wait.
    pub async fn transfer_and_wait(
        &self,
        from: u64,
        to: u64,
        amount: u64,
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        let mut client = self.inner.clone();
        let resp = client
            .submit_and_wait(proto::SubmitAndWaitRequest {
                operation: Some(proto::submit_and_wait_request::Operation::Transfer(
                    proto::Transfer {
                        from,
                        to,
                        amount,
                        user_ref,
                    },
                )),
                wait_level: wait_level as i32,
            })
            .await?
            .into_inner();
        Ok(SubmitResult {
            tx_id: resp.transaction_id,
            fail_reason: resp.fail_reason,
        })
    }

    // -- Batch operations ---------------------------------------------------

    /// Submit a batch of deposit operations (fire-and-forget).
    /// Each entry is `(account, amount, user_ref)`.
    /// Returns one transaction ID per operation.
    pub async fn deposit_batch(&self, deposits: &[(u64, u64, u64)]) -> Result<Vec<u64>> {
        let mut client = self.inner.clone();
        let operations = deposits
            .iter()
            .map(
                |(account, amount, user_ref)| proto::SubmitOperationRequest {
                    operation: Some(proto::submit_operation_request::Operation::Deposit(
                        proto::Deposit {
                            account: *account,
                            amount: *amount,
                            user_ref: *user_ref,
                        },
                    )),
                },
            )
            .collect();

        let resp = client
            .submit_batch(proto::SubmitBatchRequest { operations })
            .await?
            .into_inner();

        Ok(resp.results.iter().map(|r| r.transaction_id).collect())
    }

    /// Submit a batch of deposit operations and wait for the given level.
    /// Each entry is `(account, amount, user_ref)`.
    /// Returns one `SubmitResult` per operation.
    pub async fn deposit_batch_and_wait(
        &self,
        deposits: &[(u64, u64, u64)],
        wait_level: proto::WaitLevel,
    ) -> Result<Vec<SubmitResult>> {
        let mut client = self.inner.clone();
        let operations = deposits
            .iter()
            .map(|(account, amount, user_ref)| proto::SubmitAndWaitRequest {
                operation: Some(proto::submit_and_wait_request::Operation::Deposit(
                    proto::Deposit {
                        account: *account,
                        amount: *amount,
                        user_ref: *user_ref,
                    },
                )),
                wait_level: 0,
            })
            .collect();

        let resp = client
            .submit_batch_and_wait(proto::SubmitBatchAndWaitRequest {
                operations,
                wait_level: wait_level as i32,
            })
            .await?
            .into_inner();

        Ok(resp
            .results
            .iter()
            .map(|r| SubmitResult {
                tx_id: r.transaction_id,
                fail_reason: r.fail_reason,
            })
            .collect())
    }

    /// Submit a batch of transfer operations and wait for the given level.
    /// Each entry is `(from, to, amount)`.
    /// Returns one `SubmitResult` per operation.
    pub async fn transfer_batch_and_wait(
        &self,
        transfers: &[(u64, u64, u64)],
        wait_level: proto::WaitLevel,
    ) -> Result<Vec<SubmitResult>> {
        let mut client = self.inner.clone();
        let operations = transfers
            .iter()
            .map(|(from, to, amount)| proto::SubmitAndWaitRequest {
                operation: Some(proto::submit_and_wait_request::Operation::Transfer(
                    proto::Transfer {
                        from: *from,
                        to: *to,
                        amount: *amount,
                        user_ref: 0,
                    },
                )),
                wait_level: 0,
            })
            .collect();

        let resp = client
            .submit_batch_and_wait(proto::SubmitBatchAndWaitRequest {
                operations,
                wait_level: wait_level as i32,
            })
            .await?
            .into_inner();

        Ok(resp
            .results
            .iter()
            .map(|r| SubmitResult {
                tx_id: r.transaction_id,
                fail_reason: r.fail_reason,
            })
            .collect())
    }

    // -- Balance queries ----------------------------------------------------

    /// Get the balance for a single account.
    pub async fn get_balance(&self, account_id: u64) -> Result<Balance> {
        let mut client = self.inner.clone();
        let resp = client
            .get_balance(proto::GetBalanceRequest { account_id })
            .await?
            .into_inner();
        Ok(Balance {
            balance: resp.balance,
            last_snapshot_tx_id: resp.last_snapshot_tx_id,
        })
    }

    /// Get balances for multiple accounts. Returns balances in the same order.
    pub async fn get_balances(&self, account_ids: &[u64]) -> Result<Vec<i64>> {
        let mut client = self.inner.clone();
        let resp = client
            .get_balances(proto::GetBalancesRequest {
                account_ids: account_ids.to_vec(),
            })
            .await?
            .into_inner();
        Ok(resp.balances)
    }

    // -- Transaction status -------------------------------------------------

    /// Get the status of a single transaction.
    pub async fn get_transaction_status(&self, transaction_id: u64) -> Result<(i32, u32)> {
        let mut client = self.inner.clone();
        let resp = client
            .get_transaction_status(proto::GetStatusRequest { transaction_id })
            .await?
            .into_inner();
        Ok((resp.status, resp.fail_reason))
    }

    /// Get statuses for multiple transactions. Returns `(status, fail_reason)` pairs.
    pub async fn get_transaction_statuses(
        &self,
        transaction_ids: &[u64],
    ) -> Result<Vec<(i32, u32)>> {
        let mut client = self.inner.clone();
        let resp = client
            .get_transaction_statuses(proto::GetStatusesRequest {
                transaction_ids: transaction_ids.to_vec(),
            })
            .await?
            .into_inner();
        Ok(resp
            .results
            .iter()
            .map(|r| (r.status, r.fail_reason))
            .collect())
    }

    // -- Pipeline index -----------------------------------------------------

    /// Get the current pipeline progress indices.
    pub async fn get_pipeline_index(&self) -> Result<PipelineIndex> {
        let mut client = self.inner.clone();
        let resp = client
            .get_pipeline_index(proto::GetPipelineIndexRequest {})
            .await?
            .into_inner();
        Ok(PipelineIndex {
            compute: resp.compute_index,
            commit: resp.commit_index,
            snapshot: resp.snapshot_index,
        })
    }

    // -- Transaction queries ------------------------------------------------

    /// Get full transaction details by ID.
    pub async fn get_transaction(&self, tx_id: u64) -> Result<Transaction> {
        let mut client = self.inner.clone();
        let resp = client
            .get_transaction(proto::GetTransactionRequest { tx_id })
            .await?
            .into_inner();
        Ok(Transaction {
            tx_id: resp.tx_id,
            entries: resp
                .entries
                .iter()
                .map(|e| TxEntry {
                    account_id: e.account_id,
                    amount: e.amount,
                    kind: e.kind,
                    computed_balance: e.computed_balance,
                })
                .collect(),
            links: resp
                .links
                .iter()
                .map(|l| TxLink {
                    to_tx_id: l.to_tx_id,
                    kind: l.kind,
                })
                .collect(),
        })
    }

    /// Get account history (newest first) with pagination.
    pub async fn get_account_history(
        &self,
        account_id: u64,
        from_tx_id: u64,
        limit: u32,
    ) -> Result<AccountHistory> {
        let mut client = self.inner.clone();
        let resp = client
            .get_account_history(proto::GetAccountHistoryRequest {
                account_id,
                from_tx_id,
                limit,
            })
            .await?
            .into_inner();
        Ok(AccountHistory {
            entries: resp
                .entries
                .iter()
                .map(|e| TxEntry {
                    account_id: e.account_id,
                    amount: e.amount,
                    kind: e.kind,
                    computed_balance: e.computed_balance,
                })
                .collect(),
            next_tx_id: resp.next_tx_id,
        })
    }

    // -- WASM function registry ---------------------------------------------

    /// Register a WASM function. Blocks on the server side until the
    /// Snapshot stage has committed the record. Returns
    /// `(version, crc32c)`.
    pub async fn register_function(
        &self,
        name: &str,
        binary: &[u8],
        override_existing: bool,
    ) -> Result<(u16, u32)> {
        let mut client = self.inner.clone();
        let resp = client
            .register_function(proto::RegisterFunctionRequest {
                name: name.to_string(),
                binary: binary.to_vec(),
                override_existing,
            })
            .await?
            .into_inner();
        Ok((resp.version as u16, resp.crc32c))
    }

    /// Unregister a WASM function by name. Returns the version stamped on
    /// the unregister record.
    pub async fn unregister_function(&self, name: &str) -> Result<u16> {
        let mut client = self.inner.clone();
        let resp = client
            .unregister_function(proto::UnregisterFunctionRequest {
                name: name.to_string(),
            })
            .await?
            .into_inner();
        Ok(resp.version as u16)
    }

    /// List every currently-loaded function.
    pub async fn list_functions(&self) -> Result<Vec<FunctionInfo>> {
        let mut client = self.inner.clone();
        let resp = client
            .list_functions(proto::ListFunctionsRequest {})
            .await?
            .into_inner();
        Ok(resp
            .functions
            .into_iter()
            .map(|f| FunctionInfo {
                name: f.name,
                version: f.version as u16,
                crc32c: f.crc32c,
            })
            .collect())
    }

    /// Submit an `Operation::Function` with 8 positional `i64` params
    /// and wait until the given pipeline level.
    pub async fn function_and_wait(
        &self,
        name: &str,
        params: [i64; 8],
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        let mut client = self.inner.clone();
        let resp = client
            .submit_and_wait(proto::SubmitAndWaitRequest {
                operation: Some(proto::submit_and_wait_request::Operation::Function(
                    proto::Function {
                        name: name.to_string(),
                        params: params.to_vec(),
                        user_ref,
                    },
                )),
                wait_level: wait_level as i32,
            })
            .await?
            .into_inner();
        Ok(SubmitResult {
            tx_id: resp.transaction_id,
            fail_reason: resp.fail_reason,
        })
    }
}
