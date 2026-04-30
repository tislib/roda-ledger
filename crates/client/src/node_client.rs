//! High-level gRPC client for roda-ledger.
//!
//! `LedgerClient` is a thin facade over the generated tonic client.
//! It provides ergonomic Rust types, hides proto construction, and
//! exposes every RPC as a simple async method.
//!
//! Every public RPC method is wrapped with [`NodeClient::with_retry`],
//! which retries on any `tonic::Status` error using exponential
//! backoff. This shields callers from transient cluster-side hiccups
//! (a node that's restarting, an in-flight leader election, a peer
//! that hasn't finished its first heartbeat) without them having to
//! hand-roll a retry loop. Tune via [`RetryConfig`] on construction.

use ::proto::ledger as proto;
use ::proto::ledger::ledger_client::LedgerClient as TonicLedgerClient;
use ledger::tools::backoff::{Backoff, BackoffPolicy};
use spdlog::{trace, warn};
use std::net::SocketAddr;
use std::time::Duration;
use tonic::transport::Channel;

/// Result type for client operations.
pub type Result<T> = std::result::Result<T, tonic::Status>;

// ---------------------------------------------------------------------------
// Retry config
// ---------------------------------------------------------------------------

/// Retry policy for [`NodeClient`] and `ClusterClient` RPCs. Every
/// public method runs through a retry loop that retries on any
/// `tonic::Status` error using exponential backoff. Internally each
/// loop drives a [`ledger::tools::backoff::Backoff`] built from
/// [`Self::backoff_policy`] — the same primitive the cluster's
/// peer-replication loop uses.
///
/// The defaults — 10 retries, 100 ms base backoff, 1.6 s cap — give
/// the schedule 100, 200, 400, 800, 1600, 1600, … ms. Tuned wide
/// enough to absorb the cluster-side transient failures that show up
/// under concurrent test load: a node restarting, an election in
/// flight, a freshly-promoted leader whose snapshot hasn't caught
/// up yet, or a few rotations through the peer list while the
/// cluster client hunts for the new leader.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retries *after* the initial attempt. With
    /// the default of 10, an operation makes up to 11 total attempts
    /// before bubbling the last error up to the caller.
    pub max_retry_count: u32,
    /// Base backoff in milliseconds — delay before the first retry.
    /// Subsequent retries double up to [`Self::max_backoff_ms`].
    pub base_backoff_ms: u64,
    /// Cap on any single backoff. Defaults to 1600 ms so high retry
    /// counts can't wait minutes between attempts.
    pub max_backoff_ms: u64,
}

impl RetryConfig {
    pub fn no_retry() -> RetryConfig {
        Self {
            max_retry_count: 0,
            base_backoff_ms: 0,
            max_backoff_ms: 0,
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retry_count: 10,
            base_backoff_ms: 100,
            max_backoff_ms: 1_600,
        }
    }
}

impl RetryConfig {
    /// Build a [`BackoffPolicy`] matching this config — used by the
    /// retry loops in `NodeClient` and `ClusterClient`.
    pub fn backoff_policy(&self) -> BackoffPolicy {
        BackoffPolicy::exponential(
            Duration::from_millis(self.base_backoff_ms),
            Duration::from_millis(self.max_backoff_ms),
        )
    }
}

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
    /// Highest transaction_id known quorum-committed cluster-wide.
    /// On the leader this is `Quorum::get()`. On followers this is
    /// the watermark advertised by the most recent AppendEntries
    /// from the leader, clamped to the follower's `last_commit_id`.
    /// `0` in single-node mode.
    pub cluster_commit: u64,
    /// True iff the responding node is currently the cluster leader.
    /// Cluster-aware clients use this to discover the leader without
    /// having to attempt a write.
    pub is_leader: bool,
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
/// Every RPC method runs through [`Self::with_retry`] using the
/// [`RetryConfig`] supplied at construction (default 5 retries,
/// 100 ms base backoff). Use [`Self::with_retry_config`] to
/// override the policy, or [`Self::set_retry_config`] to mutate
/// it on an existing client.
#[derive(Clone)]
pub struct NodeClient {
    inner: TonicLedgerClient<Channel>,
    retry: RetryConfig,
    url: String,
}

impl NodeClient {
    /// Connect to a roda-ledger server at the given address.
    pub async fn connect(addr: SocketAddr) -> std::result::Result<Self, tonic::transport::Error> {
        let inner = TonicLedgerClient::connect(format!("http://{}", addr)).await?;
        Ok(Self {
            inner,
            retry: RetryConfig::default(),
            url: addr.to_string(),
        })
    }

    /// Connect to a roda-ledger server at the given URL string (e.g. `http://127.0.0.1:50051`).
    pub async fn connect_url(url: &str) -> std::result::Result<Self, tonic::transport::Error> {
        let inner = TonicLedgerClient::connect(url.to_string()).await?;
        Ok(Self {
            inner,
            retry: RetryConfig::default(),
            url: url.to_string(),
        })
    }

    /// Build a client wrapping `inner` with a custom retry policy.
    /// Useful in tests that want to opt into deterministic
    /// no-retry behaviour or shorten the backoff.
    pub fn with_retry_config(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Mutate the retry policy on an existing client.
    pub fn set_retry_config(&mut self, retry: RetryConfig) {
        self.retry = retry;
    }

    /// Read-only view of the active retry policy.
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry
    }

    /// Run `op` with retry + exponential backoff. Logs a `warn` on
    /// every retry and on final give-up. Backoff before retry N
    /// (1-indexed) is `base_backoff_ms * 2^(N-1)` — defaults give
    /// 100 ms, 200 ms, 400 ms, 800 ms, 1600 ms across the 5
    /// configured retries.
    ///
    /// Retries on any `tonic::Status` error. Application-level
    /// rejections (insufficient funds, dedup duplicates, etc.) come
    /// back as `fail_reason` fields on `Ok` responses, not as
    /// `tonic::Status`, so they are NOT retried.
    async fn with_retry<F, Fut, T>(&self, op_name: &str, op: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let max = self.retry.max_retry_count;
        let mut backoff = Backoff::new(self.retry.backoff_policy());
        for attempt in 0..=max {
            match op().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt == max {
                        warn!(
                            "client::{}: failed after {} retries, giving up: {} (code={:?})",
                            op_name,
                            attempt,
                            e.message(),
                            e.code()
                        );
                        return Err(e);
                    }
                    let retry_num = attempt + 1;
                    let next = backoff.peek_delay();
                    warn!(
                        "client::{}: error '{}' (code={:?}) — retrying ({}/{}) after {}ms",
                        op_name,
                        e.message(),
                        e.code(),
                        retry_num,
                        max,
                        next.as_millis()
                    );
                    backoff.wait().await;
                }
            }
        }
        unreachable!("retry loop must return inside the for-body")
    }

    // -- Submit operations --------------------------------------------------

    /// Submit a deposit (fire-and-forget). Returns the transaction ID.
    pub async fn deposit(&self, account: u64, amount: u64, user_ref: u64) -> Result<u64> {
        self.with_retry("deposit", || async {
            let mut client = self.inner.clone();
            trace!("deposit: account = {:?} amount = {:?} user_ref = {:?} at node_url: {:?}", account, amount, user_ref, self.url);
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
        })
        .await
    }

    /// Submit a withdrawal (fire-and-forget). Returns the transaction ID.
    pub async fn withdraw(&self, account: u64, amount: u64, user_ref: u64) -> Result<u64> {
        self.with_retry("withdraw", || async {
            let mut client = self.inner.clone();
            trace!("withdraw: account = {:?} amount = {:?} user_ref = {:?} at node_url: {:?}", account, amount, user_ref, self.url);
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
        })
        .await
    }

    /// Submit a transfer (fire-and-forget). Returns the transaction ID.
    pub async fn transfer(&self, from: u64, to: u64, amount: u64, user_ref: u64) -> Result<u64> {
        self.with_retry("transfer", || async {
            let mut client = self.inner.clone();
            trace!("transfer: from = {:?} to = {:?} amount = {:?} user_ref = {:?} at node_url: {:?}", from, to, amount, user_ref, self.url);
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
        })
        .await
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
        self.with_retry("deposit_and_wait", || async {
            let mut client = self.inner.clone();
            trace!("deposit_and_wait: account = {:?}, amount = {:?}, user_ref = {:?}, wait_level = {:?} at node_url: {:?}", account, amount, user_ref, wait_level, self.url);
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
        })
        .await
    }

    /// Submit a withdrawal and wait.
    pub async fn withdraw_and_wait(
        &self,
        account: u64,
        amount: u64,
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        self.with_retry("withdraw_and_wait", || async {
            let mut client = self.inner.clone();
            trace!("withdraw_and_wait: account = {:?}, amount = {:?}, user_ref = {:?}, wait_level = {:?} at node_url: {:?}", account, amount, user_ref, wait_level, self.url);
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
        })
        .await
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
        self.with_retry("transfer_and_wait", || async {
            let mut client = self.inner.clone();
            trace!("transfer_and_wait: from = {:?}, to = {:?}, amount = {:?}, user_ref = {:?}, wait_level = {:?} at node_url: {:?}", from, to, amount, user_ref, wait_level, self.url);
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
        })
        .await
    }

    // -- Batch operations ---------------------------------------------------

    /// Submit a batch of deposit operations (fire-and-forget).
    /// Each entry is `(account, amount, user_ref)`.
    /// Returns one transaction ID per operation.
    pub async fn deposit_batch(&self, deposits: &[(u64, u64, u64)]) -> Result<Vec<u64>> {
        self.with_retry("deposit_batch", || async {
            let mut client = self.inner.clone();
            trace!("deposit_batch: deposits = {:?} at node_url: {:?}", deposits, self.url);
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
        })
        .await
    }

    /// Submit a batch of deposit operations and wait for the given level.
    /// Each entry is `(account, amount, user_ref)`.
    /// Returns one `SubmitResult` per operation.
    pub async fn deposit_batch_and_wait(
        &self,
        deposits: &[(u64, u64, u64)],
        wait_level: proto::WaitLevel,
    ) -> Result<Vec<SubmitResult>> {
        self.with_retry("deposit_batch_and_wait", || async {
            let mut client = self.inner.clone();
            trace!("deposit_batch_and_wait: deposits = {:?} wait_level = {:?} at node_url: {:?}", deposits, wait_level, self.url);
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
        })
        .await
    }

    /// Submit a batch of transfer operations and wait for the given level.
    /// Each entry is `(from, to, amount)`.
    /// Returns one `SubmitResult` per operation.
    pub async fn transfer_batch_and_wait(
        &self,
        transfers: &[(u64, u64, u64)],
        wait_level: proto::WaitLevel,
    ) -> Result<Vec<SubmitResult>> {
        self.with_retry("transfer_batch_and_wait", || async {
            let mut client = self.inner.clone();
            trace!("transfer_batch_and_wait: transfers = {:?} wait_level = {:?} at node_url: {:?}", transfers, wait_level, self.url);
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
        })
        .await
    }

    // -- Balance queries ----------------------------------------------------

    /// Get the balance for a single account.
    pub async fn get_balance(&self, account_id: u64) -> Result<Balance> {
        self.with_retry("get_balance", || async {
            let mut client = self.inner.clone();
            trace!("get_balance: account_id = {:?} at node_url: {:?}", account_id, self.url);
            let resp = client
                .get_balance(proto::GetBalanceRequest { account_id })
                .await?
                .into_inner();
            Ok(Balance {
                balance: resp.balance,
                last_snapshot_tx_id: resp.last_snapshot_tx_id,
            })
        })
        .await
    }

    /// Get balances for multiple accounts. Returns balances in the same order.
    pub async fn get_balances(&self, account_ids: &[u64]) -> Result<Vec<i64>> {
        self.with_retry("get_balances", || async {
            let mut client = self.inner.clone();
            trace!("get_balances: account_ids = {:?} at node_url: {:?}", account_ids, self.url);
            let resp = client
                .get_balances(proto::GetBalancesRequest {
                    account_ids: account_ids.to_vec(),
                })
                .await?
                .into_inner();
            Ok(resp.balances)
        })
        .await
    }

    // -- Transaction status -------------------------------------------------

    /// Get the status of a single transaction.
    pub async fn get_transaction_status(&self, transaction_id: u64) -> Result<(i32, u32)> {
        self.with_retry("get_transaction_status", || async {
            let mut client = self.inner.clone();
            trace!("get_transaction_status: transaction_id = {:?} at node_url: {:?}", transaction_id, self.url);
            let resp = client
                .get_transaction_status(proto::GetStatusRequest {
                    transaction_id,
                    term: 0,
                })
                .await?
                .into_inner();
            Ok((resp.status, resp.fail_reason))
        })
        .await
    }

    /// Get statuses for multiple transactions. Returns `(status, fail_reason)` pairs.
    pub async fn get_transaction_statuses(
        &self,
        transaction_ids: &[u64],
    ) -> Result<Vec<(i32, u32)>> {
        self.with_retry("get_transaction_statuses", || async {
            let mut client = self.inner.clone();
            trace!("get_transaction_statuses: transaction_ids = {:?} at node_url: {:?}", transaction_ids, self.url);
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
        })
        .await
    }

    // -- Pipeline index -----------------------------------------------------

    /// Get the current pipeline progress indices.
    pub async fn get_pipeline_index(&self) -> Result<PipelineIndex> {
        self.with_retry("get_pipeline_index", || async {
            let mut client = self.inner.clone();
            trace!("get_pipeline_index at node_url: {:?}", self.url);
            let resp = client
                .get_pipeline_index(proto::GetPipelineIndexRequest {})
                .await?
                .into_inner();
            Ok(PipelineIndex {
                compute: resp.compute_index,
                commit: resp.commit_index,
                snapshot: resp.snapshot_index,
                cluster_commit: resp.cluster_commit_index,
                is_leader: resp.is_leader,
            })
        })
        .await
    }

    // -- Transaction queries ------------------------------------------------

    /// Get full transaction details by ID.
    pub async fn get_transaction(&self, tx_id: u64) -> Result<Transaction> {
        self.with_retry("get_transaction", || async {
            let mut client = self.inner.clone();
            trace!("get_transaction: tx_id = {:?} at node_url: {:?}", tx_id, self.url);
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
        })
        .await
    }

    /// Get account history (newest first) with pagination.
    pub async fn get_account_history(
        &self,
        account_id: u64,
        from_tx_id: u64,
        limit: u32,
    ) -> Result<AccountHistory> {
        self.with_retry("get_account_history", || async {
            let mut client = self.inner.clone();
            trace!("get_account_history: account_id = {:?}, from_tx_id = {:?}, limit = {:?} at node_url: {:?}", account_id, from_tx_id, limit, self.url);
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
        })
        .await
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
        self.with_retry("register_function", || async {
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
        })
        .await
    }

    /// Unregister a WASM function by name. Returns the version stamped on
    /// the unregister record.
    pub async fn unregister_function(&self, name: &str) -> Result<u16> {
        self.with_retry("unregister_function", || async {
            let mut client = self.inner.clone();
            let resp = client
                .unregister_function(proto::UnregisterFunctionRequest {
                    name: name.to_string(),
                })
                .await?
                .into_inner();
            Ok(resp.version as u16)
        })
        .await
    }

    /// List every currently-loaded function.
    pub async fn list_functions(&self) -> Result<Vec<FunctionInfo>> {
        self.with_retry("list_functions", || async {
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
        })
        .await
    }

    /// Submit an `Operation::Function` with 8 positional `i64` params
    /// and wait until the given pipeline level.
    pub async fn submit_function_and_wait(
        &self,
        name: &str,
        params: [i64; 8],
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        self.with_retry("submit_function_and_wait", || async {
            let mut client = self.inner.clone();
            trace!("submit_function_and_wait: name = {:?}, params = {:?}, user_ref = {:?}, wait_level = {:?} at node_url: {:?}", name, params, user_ref, wait_level, self.url);
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
        })
        .await
    }
}
