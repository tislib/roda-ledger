//! Cluster-aware gRPC client.
//!
//! Two layered clients on top of [`NodeClient`]:
//!
//! - [`ClusterLeaderClient`] — same surface as [`NodeClient`], but
//!   resolves the current leader before every call and follows
//!   "not a leader" rejections by rotating to the next node (or the
//!   `leader-node-id` hint if the server attached one). Each call
//!   has its own retry+backoff loop on top of the per-node retry.
//! - [`ClusterClient`] — top-level facade for a known set of cluster
//!   nodes. Write-style RPCs delegate to the embedded
//!   [`ClusterLeaderClient`]; read-style RPCs round-robin across
//!   every node so a slow or restarting node doesn't block reads.
//!
//! The standalone [`NodeClient`] keeps working unchanged; cluster
//! clients are an additive layer.

use crate::client::node_client::{
    AccountHistory, Balance, FunctionInfo, NodeClient, PipelineIndex, Result, RetryConfig,
    SubmitResult, Transaction, backoff_for,
};
use crate::cluster::proto::ledger as proto;
use spdlog::warn;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use tonic::Code;

/// Metadata key used by the server to hint at the current leader's
/// position in the cluster's peer list (0-indexed). Optional —
/// `ClusterLeaderClient` falls back to "next node in rotation" when
/// the hint is missing or out of range.
pub const LEADER_HINT_METADATA_KEY: &str = "leader-node-index";

// ---------------------------------------------------------------------------
// ClusterClient
// ---------------------------------------------------------------------------

/// Top-level client for a roda-ledger cluster.
///
/// Holds one [`NodeClient`] per node and an embedded
/// [`ClusterLeaderClient`] that knows how to route writes through the
/// current leader. Read RPCs round-robin across nodes to spread load
/// and to keep working when a single node is restarting.
///
/// Per-node retries are disabled inside the cluster client — retry
/// (and leader failover) happens at the cluster level so callers see
/// at most one warn-stream per logical RPC instead of nested loops.
#[derive(Clone)]
pub struct ClusterClient {
    nodes: Arc<Vec<NodeClient>>,
    leader: ClusterLeaderClient,
    next_read: Arc<AtomicUsize>,
    retry: RetryConfig,
}

impl ClusterClient {
    /// Connect to every node in `urls` (e.g. `http://127.0.0.1:50051`)
    /// using the default [`RetryConfig`]. Panics on empty input.
    pub async fn connect(
        urls: &[String],
    ) -> std::result::Result<Self, tonic::transport::Error> {
        Self::connect_with_retry(urls, RetryConfig::default()).await
    }

    /// Connect to every node in `urls` with a custom [`RetryConfig`].
    /// Per-node clients are built with `max_retry_count = 0` — the
    /// cluster-level loop owns retry/backoff.
    ///
    /// Panics on empty `urls` — a cluster client without any nodes is
    /// unusable, and the caller has a programming bug.
    pub async fn connect_with_retry(
        urls: &[String],
        retry: RetryConfig,
    ) -> std::result::Result<Self, tonic::transport::Error> {
        assert!(
            !urls.is_empty(),
            "ClusterClient::connect_with_retry requires at least one URL"
        );
        let inner_retry = RetryConfig {
            max_retry_count: 0,
            base_backoff_ms: retry.base_backoff_ms,
        };
        let mut nodes = Vec::with_capacity(urls.len());
        for url in urls {
            let nc = NodeClient::connect_url(url)
                .await?
                .with_retry_config(inner_retry.clone());
            nodes.push(nc);
        }
        Ok(Self::from_nodes(nodes, retry))
    }

    /// Build a `ClusterClient` directly from already-connected
    /// [`NodeClient`]s. The caller is responsible for tuning their
    /// per-node retry — we recommend `max_retry_count = 0` so the
    /// cluster-level loop is the only retry layer.
    ///
    /// Panics on empty `nodes`.
    pub fn from_nodes(nodes: Vec<NodeClient>, retry: RetryConfig) -> Self {
        assert!(
            !nodes.is_empty(),
            "ClusterClient::from_nodes requires at least one node"
        );
        let nodes = Arc::new(nodes);
        let leader = ClusterLeaderClient::new(nodes.clone(), retry.clone());
        Self {
            nodes,
            leader,
            next_read: Arc::new(AtomicUsize::new(0)),
            retry,
        }
    }

    /// Borrow the embedded [`ClusterLeaderClient`].
    pub fn leader(&self) -> &ClusterLeaderClient {
        &self.leader
    }

    /// Borrow node `i` directly. Panics if `i >= node_count()`.
    pub fn node(&self, i: usize) -> &NodeClient {
        &self.nodes[i]
    }

    /// Number of nodes the client is connected to.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Read-only view of the active retry policy.
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry
    }

    /// Pick the next node in the round-robin rotation. Used by every
    /// read facade.
    fn pick_read_node(&self) -> usize {
        self.next_read.fetch_add(1, Ordering::Relaxed) % self.nodes.len()
    }

    /// Run `op` against round-robin-selected nodes, retrying on any
    /// `tonic::Status` error. Each retry advances the rotation by one,
    /// so a single broken node can't trap the read forever.
    async fn with_read_retry<F, Fut, T>(&self, op_name: &str, op: F) -> Result<T>
    where
        F: Fn(NodeClient) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let max = self.retry.max_retry_count;
        for attempt in 0..=max {
            let idx = self.pick_read_node();
            let client = self.nodes[idx].clone();
            match op(client).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt == max {
                        warn!(
                            "cluster::{}: failed after {} retries (last node[{}]): {} (code={:?})",
                            op_name,
                            attempt,
                            idx,
                            e.message(),
                            e.code()
                        );
                        return Err(e);
                    }
                    let retry_num = attempt + 1;
                    let backoff_ms = backoff_for(self.retry.base_backoff_ms, attempt);
                    warn!(
                        "cluster::{}: error '{}' (code={:?}) on node[{}] — retrying ({}/{}) on next node after {}ms",
                        op_name,
                        e.message(),
                        e.code(),
                        idx,
                        retry_num,
                        max,
                        backoff_ms
                    );
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
            }
        }
        unreachable!("retry loop must return inside the for-body")
    }

    // ── Write facades — go through the leader ────────────────────────

    pub async fn deposit(&self, account: u64, amount: u64, user_ref: u64) -> Result<u64> {
        self.leader.deposit(account, amount, user_ref).await
    }

    pub async fn withdraw(&self, account: u64, amount: u64, user_ref: u64) -> Result<u64> {
        self.leader.withdraw(account, amount, user_ref).await
    }

    pub async fn transfer(&self, from: u64, to: u64, amount: u64, user_ref: u64) -> Result<u64> {
        self.leader.transfer(from, to, amount, user_ref).await
    }

    pub async fn deposit_and_wait(
        &self,
        account: u64,
        amount: u64,
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        self.leader
            .deposit_and_wait(account, amount, user_ref, wait_level)
            .await
    }

    pub async fn withdraw_and_wait(
        &self,
        account: u64,
        amount: u64,
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        self.leader
            .withdraw_and_wait(account, amount, user_ref, wait_level)
            .await
    }

    pub async fn transfer_and_wait(
        &self,
        from: u64,
        to: u64,
        amount: u64,
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        self.leader
            .transfer_and_wait(from, to, amount, user_ref, wait_level)
            .await
    }

    pub async fn deposit_batch(&self, deposits: &[(u64, u64, u64)]) -> Result<Vec<u64>> {
        self.leader.deposit_batch(deposits).await
    }

    pub async fn deposit_batch_and_wait(
        &self,
        deposits: &[(u64, u64, u64)],
        wait_level: proto::WaitLevel,
    ) -> Result<Vec<SubmitResult>> {
        self.leader
            .deposit_batch_and_wait(deposits, wait_level)
            .await
    }

    pub async fn transfer_batch_and_wait(
        &self,
        transfers: &[(u64, u64, u64)],
        wait_level: proto::WaitLevel,
    ) -> Result<Vec<SubmitResult>> {
        self.leader
            .transfer_batch_and_wait(transfers, wait_level)
            .await
    }

    pub async fn submit_function_and_wait(
        &self,
        name: &str,
        params: [i64; 8],
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        self.leader
            .submit_function_and_wait(name, params, user_ref, wait_level)
            .await
    }

    pub async fn register_function(
        &self,
        name: &str,
        binary: &[u8],
        override_existing: bool,
    ) -> Result<(u16, u32)> {
        self.leader
            .register_function(name, binary, override_existing)
            .await
    }

    pub async fn unregister_function(&self, name: &str) -> Result<u16> {
        self.leader.unregister_function(name).await
    }

    // ── Read facades — round-robin across nodes ──────────────────────

    pub async fn get_balance(&self, account_id: u64) -> Result<Balance> {
        self.with_read_retry("get_balance", move |c| async move {
            c.get_balance(account_id).await
        })
        .await
    }

    pub async fn get_balances(&self, account_ids: &[u64]) -> Result<Vec<i64>> {
        let account_ids: Arc<[u64]> = account_ids.into();
        self.with_read_retry("get_balances", move |c| {
            let account_ids = account_ids.clone();
            async move { c.get_balances(&account_ids).await }
        })
        .await
    }

    pub async fn get_transaction_status(&self, transaction_id: u64) -> Result<(i32, u32)> {
        self.with_read_retry("get_transaction_status", move |c| async move {
            c.get_transaction_status(transaction_id).await
        })
        .await
    }

    pub async fn get_transaction_statuses(
        &self,
        transaction_ids: &[u64],
    ) -> Result<Vec<(i32, u32)>> {
        let transaction_ids: Arc<[u64]> = transaction_ids.into();
        self.with_read_retry("get_transaction_statuses", move |c| {
            let transaction_ids = transaction_ids.clone();
            async move { c.get_transaction_statuses(&transaction_ids).await }
        })
        .await
    }

    pub async fn get_pipeline_index(&self) -> Result<PipelineIndex> {
        self.with_read_retry("get_pipeline_index", move |c| async move {
            c.get_pipeline_index().await
        })
        .await
    }

    pub async fn get_transaction(&self, tx_id: u64) -> Result<Transaction> {
        self.with_read_retry("get_transaction", move |c| async move {
            c.get_transaction(tx_id).await
        })
        .await
    }

    pub async fn get_account_history(
        &self,
        account_id: u64,
        from_tx_id: u64,
        limit: u32,
    ) -> Result<AccountHistory> {
        self.with_read_retry("get_account_history", move |c| async move {
            c.get_account_history(account_id, from_tx_id, limit).await
        })
        .await
    }

    pub async fn list_functions(&self) -> Result<Vec<FunctionInfo>> {
        self.with_read_retry("list_functions", move |c| async move {
            c.list_functions().await
        })
        .await
    }
}

// ---------------------------------------------------------------------------
// ClusterLeaderClient
// ---------------------------------------------------------------------------

/// Leader-aware client. Holds [`NodeClient`]s for every cluster
/// member; tracks the current leader index; on a "not a leader"
/// rejection rotates to either the server-supplied
/// `leader-node-index` hint (if present) or the next node in order,
/// then retries the call with exponential backoff.
///
/// The same surface as [`NodeClient`], so it is a drop-in replacement
/// when the caller knows it needs leader-level guarantees but does
/// not want to handle failover manually.
#[derive(Clone)]
pub struct ClusterLeaderClient {
    nodes: Arc<Vec<NodeClient>>,
    /// Cached "best guess" for which node is currently the leader.
    /// Updated on every leader-routed call: cleared on success
    /// (kept), advanced to a hinted index or the next node on a
    /// "not a leader" rejection.
    current_leader: Arc<AtomicUsize>,
    retry: RetryConfig,
}

impl ClusterLeaderClient {
    pub(crate) fn new(nodes: Arc<Vec<NodeClient>>, retry: RetryConfig) -> Self {
        Self {
            nodes,
            current_leader: Arc::new(AtomicUsize::new(0)),
            retry,
        }
    }

    /// Index into the cluster's node list for whichever node the
    /// client currently believes is the leader. Updated lazily as
    /// "not leader" errors bounce the cursor forward.
    pub fn current_leader_index(&self) -> usize {
        self.current_leader.load(Ordering::Acquire) % self.nodes.len()
    }

    /// Read-only view of the active retry policy.
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry
    }

    /// Run `op` against the current leader. On a "not a leader"
    /// rejection, advance the cursor (preferring the
    /// `leader-node-index` metadata hint if the server included it)
    /// and retry. On any other `tonic::Status` error, retry against
    /// the same node with exponential backoff.
    async fn with_leader_retry<F, Fut, T>(&self, op_name: &str, op: F) -> Result<T>
    where
        F: Fn(NodeClient) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let max = self.retry.max_retry_count;
        let n_nodes = self.nodes.len();
        for attempt in 0..=max {
            let idx = self.current_leader.load(Ordering::Acquire) % n_nodes;
            let client = self.nodes[idx].clone();
            match op(client).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt == max {
                        warn!(
                            "leader::{}: failed after {} retries (last node[{}]): {} (code={:?})",
                            op_name,
                            attempt,
                            idx,
                            e.message(),
                            e.code()
                        );
                        return Err(e);
                    }
                    let retry_num = attempt + 1;
                    let backoff_ms = backoff_for(self.retry.base_backoff_ms, attempt);

                    // Rotate the cached leader index whenever the
                    // current node looks unable to serve the write —
                    // either an explicit "not a leader" rejection or
                    // a transport-level failure that suggests the node
                    // is dead/restarting. The hint takes precedence
                    // when present and points somewhere other than
                    // the current node; otherwise step to the next
                    // node in order.
                    if should_rotate_leader(&e) {
                        let next_idx = read_leader_hint(&e, n_nodes)
                            .filter(|hint| *hint != idx)
                            .unwrap_or((idx + 1) % n_nodes);
                        // Compare-exchange so concurrent failovers
                        // don't trample each other into a position
                        // that has already been moved past.
                        let _ = self.current_leader.compare_exchange(
                            idx,
                            next_idx,
                            Ordering::Release,
                            Ordering::Acquire,
                        );
                        warn!(
                            "leader::{}: node[{}] cannot serve writes (code={:?}, msg='{}') — switching to node[{}] and retrying ({}/{}) after {}ms",
                            op_name,
                            idx,
                            e.code(),
                            e.message(),
                            next_idx,
                            retry_num,
                            max,
                            backoff_ms
                        );
                    } else {
                        warn!(
                            "leader::{}: error '{}' (code={:?}) on node[{}] — retrying ({}/{}) after {}ms",
                            op_name,
                            e.message(),
                            e.code(),
                            idx,
                            retry_num,
                            max,
                            backoff_ms
                        );
                    }
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
            }
        }
        unreachable!("retry loop must return inside the for-body")
    }

    // ── Same surface as NodeClient ────────────────────────────────────

    pub async fn deposit(&self, account: u64, amount: u64, user_ref: u64) -> Result<u64> {
        self.with_leader_retry("deposit", move |c| async move {
            c.deposit(account, amount, user_ref).await
        })
        .await
    }

    pub async fn withdraw(&self, account: u64, amount: u64, user_ref: u64) -> Result<u64> {
        self.with_leader_retry("withdraw", move |c| async move {
            c.withdraw(account, amount, user_ref).await
        })
        .await
    }

    pub async fn transfer(&self, from: u64, to: u64, amount: u64, user_ref: u64) -> Result<u64> {
        self.with_leader_retry("transfer", move |c| async move {
            c.transfer(from, to, amount, user_ref).await
        })
        .await
    }

    pub async fn deposit_and_wait(
        &self,
        account: u64,
        amount: u64,
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        self.with_leader_retry("deposit_and_wait", move |c| async move {
            c.deposit_and_wait(account, amount, user_ref, wait_level)
                .await
        })
        .await
    }

    pub async fn withdraw_and_wait(
        &self,
        account: u64,
        amount: u64,
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        self.with_leader_retry("withdraw_and_wait", move |c| async move {
            c.withdraw_and_wait(account, amount, user_ref, wait_level)
                .await
        })
        .await
    }

    pub async fn transfer_and_wait(
        &self,
        from: u64,
        to: u64,
        amount: u64,
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        self.with_leader_retry("transfer_and_wait", move |c| async move {
            c.transfer_and_wait(from, to, amount, user_ref, wait_level)
                .await
        })
        .await
    }

    pub async fn deposit_batch(&self, deposits: &[(u64, u64, u64)]) -> Result<Vec<u64>> {
        let deposits: Arc<[(u64, u64, u64)]> = deposits.into();
        self.with_leader_retry("deposit_batch", move |c| {
            let deposits = deposits.clone();
            async move { c.deposit_batch(&deposits).await }
        })
        .await
    }

    pub async fn deposit_batch_and_wait(
        &self,
        deposits: &[(u64, u64, u64)],
        wait_level: proto::WaitLevel,
    ) -> Result<Vec<SubmitResult>> {
        let deposits: Arc<[(u64, u64, u64)]> = deposits.into();
        self.with_leader_retry("deposit_batch_and_wait", move |c| {
            let deposits = deposits.clone();
            async move { c.deposit_batch_and_wait(&deposits, wait_level).await }
        })
        .await
    }

    pub async fn transfer_batch_and_wait(
        &self,
        transfers: &[(u64, u64, u64)],
        wait_level: proto::WaitLevel,
    ) -> Result<Vec<SubmitResult>> {
        let transfers: Arc<[(u64, u64, u64)]> = transfers.into();
        self.with_leader_retry("transfer_batch_and_wait", move |c| {
            let transfers = transfers.clone();
            async move { c.transfer_batch_and_wait(&transfers, wait_level).await }
        })
        .await
    }

    pub async fn submit_function_and_wait(
        &self,
        name: &str,
        params: [i64; 8],
        user_ref: u64,
        wait_level: proto::WaitLevel,
    ) -> Result<SubmitResult> {
        let name: Arc<str> = name.into();
        self.with_leader_retry("submit_function_and_wait", move |c| {
            let name = name.clone();
            async move {
                c.submit_function_and_wait(&name, params, user_ref, wait_level)
                    .await
            }
        })
        .await
    }

    pub async fn register_function(
        &self,
        name: &str,
        binary: &[u8],
        override_existing: bool,
    ) -> Result<(u16, u32)> {
        let name: Arc<str> = name.into();
        let binary: Arc<[u8]> = binary.into();
        self.with_leader_retry("register_function", move |c| {
            let name = name.clone();
            let binary = binary.clone();
            async move {
                c.register_function(&name, &binary, override_existing)
                    .await
            }
        })
        .await
    }

    pub async fn unregister_function(&self, name: &str) -> Result<u16> {
        let name: Arc<str> = name.into();
        self.with_leader_retry("unregister_function", move |c| {
            let name = name.clone();
            async move { c.unregister_function(&name).await }
        })
        .await
    }

    // Read facades on the leader specifically — same surface as
    // NodeClient. Tests sometimes need a "leader-only read" channel.

    pub async fn get_balance(&self, account_id: u64) -> Result<Balance> {
        self.with_leader_retry("get_balance", move |c| async move {
            c.get_balance(account_id).await
        })
        .await
    }

    pub async fn get_balances(&self, account_ids: &[u64]) -> Result<Vec<i64>> {
        let account_ids: Arc<[u64]> = account_ids.into();
        self.with_leader_retry("get_balances", move |c| {
            let account_ids = account_ids.clone();
            async move { c.get_balances(&account_ids).await }
        })
        .await
    }

    pub async fn get_transaction_status(&self, transaction_id: u64) -> Result<(i32, u32)> {
        self.with_leader_retry("get_transaction_status", move |c| async move {
            c.get_transaction_status(transaction_id).await
        })
        .await
    }

    pub async fn get_transaction_statuses(
        &self,
        transaction_ids: &[u64],
    ) -> Result<Vec<(i32, u32)>> {
        let transaction_ids: Arc<[u64]> = transaction_ids.into();
        self.with_leader_retry("get_transaction_statuses", move |c| {
            let transaction_ids = transaction_ids.clone();
            async move { c.get_transaction_statuses(&transaction_ids).await }
        })
        .await
    }

    pub async fn get_pipeline_index(&self) -> Result<PipelineIndex> {
        self.with_leader_retry("get_pipeline_index", move |c| async move {
            c.get_pipeline_index().await
        })
        .await
    }

    pub async fn get_transaction(&self, tx_id: u64) -> Result<Transaction> {
        self.with_leader_retry("get_transaction", move |c| async move {
            c.get_transaction(tx_id).await
        })
        .await
    }

    pub async fn get_account_history(
        &self,
        account_id: u64,
        from_tx_id: u64,
        limit: u32,
    ) -> Result<AccountHistory> {
        self.with_leader_retry("get_account_history", move |c| async move {
            c.get_account_history(account_id, from_tx_id, limit).await
        })
        .await
    }

    pub async fn list_functions(&self) -> Result<Vec<FunctionInfo>> {
        self.with_leader_retry("list_functions", move |c| async move {
            c.list_functions().await
        })
            .await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// True when the server explicitly told us "this node is not
/// currently the leader; route writes elsewhere". Today the gRPC
/// handlers mark these with [`tonic::Code::FailedPrecondition`] and
/// a message containing the substring `"not a leader"` — the
/// handler at `src/cluster/ledger_handler.rs::ensure_writable` is the
/// only producer.
fn is_not_leader_error(status: &tonic::Status) -> bool {
    status.code() == Code::FailedPrecondition && status.message().contains("not a leader")
}

/// True when retrying the same node is unlikely to help and we should
/// rotate to a different cluster member. Covers the explicit
/// "not a leader" rejection plus the transport-level failures that
/// happen when a node dies, restarts, or hasn't finished booting yet
/// — in all of those cases the only useful next step is to try a
/// different node.
fn should_rotate_leader(status: &tonic::Status) -> bool {
    if is_not_leader_error(status) {
        return true;
    }
    matches!(
        status.code(),
        Code::Unavailable | Code::Cancelled | Code::Unknown | Code::Internal
    )
}

/// Parse the optional `leader-node-index` metadata hint into a node
/// index. Returns `None` when the metadata is absent, malformed, or
/// out of range for the current cluster size.
fn read_leader_hint(status: &tonic::Status, n_nodes: usize) -> Option<usize> {
    let meta = status.metadata().get(LEADER_HINT_METADATA_KEY)?;
    let s = meta.to_str().ok()?;
    let id: usize = s.parse().ok()?;
    if id < n_nodes { Some(id) } else { None }
}
