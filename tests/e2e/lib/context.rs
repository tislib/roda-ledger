//! `E2EContext` — the single handle passed to every E2E test.
//!
//! Knows about all running nodes, their gRPC clients, and their runtime
//! state. Adding Raft means `ctx` manages N nodes instead of 1 — the
//! test interface does not change.

use crate::e2e::lib::backend::E2EBackend;
use crate::e2e::lib::backend_inline::InlineNode;
use crate::e2e::lib::backend_process::ProcessNode;
use crate::e2e::lib::profile::Profile;
use roda_ledger::client::{FunctionInfo, NodeClient, SubmitResult};
use roda_ledger::cluster::proto::ledger::WaitLevel;
use std::net::SocketAddr;
use tokio::time::{Duration, sleep};

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

    async fn start_inline_nodes(profile: &Profile) -> Vec<NodeHandle> {
        let mut nodes = Vec::with_capacity(profile.nodes);
        for _ in 0..profile.nodes {
            nodes.push(NodeHandle::Inline(InlineNode::start(profile).await));
        }
        nodes
    }

    async fn start_process_nodes(profile: &Profile) -> Vec<NodeHandle> {
        let mut nodes = Vec::with_capacity(profile.nodes);
        for _ in 0..profile.nodes {
            nodes.push(NodeHandle::Process(ProcessNode::start(profile).await));
        }
        nodes
    }

    // -- Node access (private) ----------------------------------------------

    fn client(&self, node: usize) -> &NodeClient {
        match &self.nodes[node] {
            NodeHandle::Inline(n) => n.client(),
            NodeHandle::Process(n) => n.client(),
            _ => panic!("client() not supported on this backend"),
        }
    }

    fn node_addr(&self, node: usize) -> SocketAddr {
        match &self.nodes[node] {
            NodeHandle::Inline(n) => n.addr,
            NodeHandle::Process(n) => n.addr,
            _ => panic!("node_addr() not supported on this backend"),
        }
    }

    // -- Actions ------------------------------------------------------------

    pub async fn deposit(&self, node: usize, account: u64, amount: u64, wait_level: i32) -> u64 {
        let wl = wait_level_from_i32(wait_level);
        let result = self
            .client(node)
            .deposit_and_wait(account, amount, 0, wl)
            .await
            .expect("deposit RPC failed");
        result.tx_id
    }

    pub async fn withdraw(&self, node: usize, account: u64, amount: u64, wait_level: i32) -> u64 {
        let wl = wait_level_from_i32(wait_level);
        let result = self
            .client(node)
            .withdraw_and_wait(account, amount, 0, wl)
            .await
            .expect("withdraw RPC failed");
        result.tx_id
    }

    pub async fn transfer(
        &self,
        node: usize,
        from: u64,
        to: u64,
        amount: u64,
        wait_level: i32,
    ) -> u64 {
        let wl = wait_level_from_i32(wait_level);
        let result = self
            .client(node)
            .transfer_and_wait(from, to, amount, 0, wl)
            .await
            .expect("transfer RPC failed");
        result.tx_id
    }

    pub async fn batch_deposit(
        &self,
        node: usize,
        account: u64,
        amount: u64,
        count: usize,
        wait_level: i32,
    ) -> u64 {
        let wl = wait_level_from_i32(wait_level);
        let deposits: Vec<(u64, u64, u64)> = (0..count).map(|_| (account, amount, 0)).collect();
        let results = self
            .client(node)
            .deposit_batch_and_wait(&deposits, wl)
            .await
            .expect("batch_deposit RPC failed");
        results.last().expect("empty batch response").tx_id
    }

    /// Deposit to multiple accounts. Each entry is `(account_id, amount)`.
    /// Large batches are chunked automatically.
    pub async fn deposit_all(&self, node: usize, deposits: &[(u64, u64)], wait_level: i32) -> u64 {
        let wl = wait_level_from_i32(wait_level);
        let mut last_tx_id = 0u64;

        for chunk in deposits.chunks(BATCH_CHUNK_SIZE) {
            let batch: Vec<(u64, u64, u64)> =
                chunk.iter().map(|(acct, amt)| (*acct, *amt, 0)).collect();
            let results = self
                .client(node)
                .deposit_batch_and_wait(&batch, wl)
                .await
                .expect("deposit_all RPC failed");
            last_tx_id = results.last().expect("empty batch response").tx_id;
        }

        last_tx_id
    }

    /// Submit a batch of transfers. Each entry is `(from, to, amount)`.
    /// Returns `(last_tx_id, rejected_count)`.
    pub async fn transfer_batch(
        &self,
        node: usize,
        transfers: &[(u64, u64, u64)],
        wait_level: i32,
    ) -> (u64, usize) {
        let wl = wait_level_from_i32(wait_level);
        let mut last_tx_id = 0u64;
        let mut total_rejected = 0usize;

        for chunk in transfers.chunks(BATCH_CHUNK_SIZE) {
            // Use the raw tonic client for transfer batches since the facade
            // doesn't have a transfer_batch_and_wait method yet.
            let results = self
                .client(node)
                .transfer_batch_and_wait(chunk, wl)
                .await
                .expect("transfer_batch RPC failed");

            total_rejected += results.iter().filter(|r| r.fail_reason != 0).count();
            last_tx_id = results.last().expect("empty batch response").tx_id;
        }

        (last_tx_id, total_rejected)
    }

    /// Submit deposits concurrently. Joins all tasks and waits for snapshot.
    #[allow(clippy::too_many_arguments)]
    pub async fn deposit_batch_concurrent(
        &self,
        node: usize,
        account: u64,
        amount: u64,
        batch_count: usize,
        batch_size: usize,
        wait_level: i32,
        retries: usize,
    ) {
        let handles = self.deposit_batch_concurrent_detached(
            node,
            account,
            amount,
            batch_count,
            batch_size,
            wait_level,
            retries,
        );

        let mut max_tx_id = 0u64;
        for handle in handles {
            let tx_id = handle.await.expect("batch task panicked");
            max_tx_id = max_tx_id.max(tx_id);
        }

        if max_tx_id > 0 {
            self.wait_for_snapshot(node, max_tx_id).await;
        }
    }

    /// Like `deposit_batch_concurrent` but returns JoinHandles without joining.
    /// The caller can kill/restart while tasks are in flight.
    ///
    /// At most 10 RPCs are in flight at once (semaphore-limited) to avoid
    /// overwhelming the server after a restart.
    #[allow(clippy::too_many_arguments)]
    pub fn deposit_batch_concurrent_detached(
        &self,
        node: usize,
        account: u64,
        amount: u64,
        batch_count: usize,
        batch_size: usize,
        wait_level: i32,
        retries: usize,
    ) -> Vec<tokio::task::JoinHandle<u64>> {
        const MAX_CONCURRENCY: usize = 10;

        let addr = self.node_addr(node);
        let wl = wait_level_from_i32(wait_level);
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENCY));
        let mut handles = Vec::with_capacity(batch_count);

        for batch_idx in 0..batch_count {
            let client = self.client(node).clone();
            let user_ref_start = (batch_idx * batch_size) as u64 + 1;
            let sem = sem.clone();

            handles.push(tokio::spawn(async move {
                let deposits: Vec<(u64, u64, u64)> = (0..batch_size)
                    .map(|i| (account, amount, user_ref_start + i as u64))
                    .collect();

                let mut current_client = client;
                let mut attempts_left = retries + 1;

                loop {
                    let _permit = sem.acquire().await.unwrap();
                    eprintln!(
                        "begin batch {} ({} of {})",
                        batch_idx,
                        batch_idx * batch_size,
                        batch_count * batch_size
                    );
                    let result = current_client.deposit_batch_and_wait(&deposits, wl).await;
                    drop(_permit);

                    match result {
                        Ok(results) => {
                            eprintln!("batch {} completed", batch_idx);
                            return results.last().map(|r| r.tx_id).unwrap_or(0);
                        }
                        Err(e) => {
                            attempts_left -= 1;
                            if attempts_left == 0 {
                                panic!(
                                    "batch {} exhausted {} retries, last error: {}",
                                    batch_idx, retries, e
                                );
                            }
                            let jitter_ms = (batch_idx as u64 * 7 + 13) % 500;
                            sleep(Duration::from_millis(2000 + jitter_ms)).await;
                            if let Ok(new_client) = NodeClient::connect(addr).await {
                                current_client = new_client;
                            }
                        }
                    }
                }
            }));
        }

        handles
    }

    // -- Reading ------------------------------------------------------------

    pub async fn get_balance(&self, node: usize, account: u64) -> i64 {
        self.client(node)
            .get_balance(account)
            .await
            .expect("get_balance RPC failed")
            .balance
    }

    pub async fn get_balances(&self, node: usize, account_ids: &[u64]) -> Vec<(u64, i64)> {
        let mut result = Vec::with_capacity(account_ids.len());

        for chunk in account_ids.chunks(BATCH_CHUNK_SIZE) {
            let balances = self
                .client(node)
                .get_balances(chunk)
                .await
                .expect("get_balances RPC failed");

            for (id, bal) in chunk.iter().zip(balances.iter()) {
                result.push((*id, *bal));
            }
        }

        result
    }

    pub async fn get_balance_sum(&self, node: usize, max_accounts: u64) -> i64 {
        let mut total: i64 = 0;

        for chunk_start in (0..max_accounts).step_by(BATCH_CHUNK_SIZE) {
            let chunk_end = (chunk_start + BATCH_CHUNK_SIZE as u64).min(max_accounts);
            let account_ids: Vec<u64> = (chunk_start..chunk_end).collect();

            let balances = self
                .client(node)
                .get_balances(&account_ids)
                .await
                .expect("get_balances RPC failed");

            total += balances.iter().sum::<i64>();
        }

        total
    }

    pub async fn get_last_committed_id(&self, node: usize) -> u64 {
        self.client(node)
            .get_pipeline_index()
            .await
            .expect("get_pipeline_index RPC failed")
            .commit
    }

    pub async fn get_pipeline_index(&self, node: usize) -> (u64, u64, u64) {
        let idx = self
            .client(node)
            .get_pipeline_index()
            .await
            .expect("get_pipeline_index RPC failed");
        (idx.compute, idx.commit, idx.snapshot)
    }

    // -- WASM function registry --------------------------------------------

    /// Register a WASM function on `node`. Blocks server-side until the
    /// handler is installed. Returns `(version, crc32c)`.
    pub async fn register_function(
        &self,
        node: usize,
        name: &str,
        binary: &[u8],
        override_existing: bool,
    ) -> (u16, u32) {
        self.client(node)
            .register_function(name, binary, override_existing)
            .await
            .expect("register_function RPC failed")
    }

    /// Unregister a WASM function on `node`. Blocks server-side until the
    /// handler is gone. Returns the unregister version.
    pub async fn unregister_function(&self, node: usize, name: &str) -> u16 {
        self.client(node)
            .unregister_function(name)
            .await
            .expect("unregister_function RPC failed")
    }

    /// List every currently-loaded function on `node`.
    pub async fn list_functions(&self, node: usize) -> Vec<FunctionInfo> {
        self.client(node)
            .list_functions()
            .await
            .expect("list_functions RPC failed")
    }

    /// Submit `Operation::Function { name, params, user_ref }` and wait
    /// until the given pipeline level. Returns the full `SubmitResult`
    /// so tests can inspect the `fail_reason`.
    pub async fn submit_function(
        &self,
        node: usize,
        name: &str,
        params: [i64; 8],
        user_ref: u64,
        wait_level: i32,
    ) -> SubmitResult {
        let wl = wait_level_from_i32(wait_level);
        self.client(node)
            .submit_function_and_wait(name, params, user_ref, wl)
            .await
            .expect("submit_function_and_wait RPC failed")
    }

    // -- Runtime intervention -----------------------------------------------

    pub fn kill_node(&mut self, node: usize) {
        match &mut self.nodes[node] {
            NodeHandle::Process(n) => n.kill(),
            NodeHandle::Inline(_) => panic!("kill_node() not supported on Inline backend"),
            _ => panic!("kill_node() not supported on this backend"),
        }
    }

    pub async fn restart_node(&mut self, node: usize) {
        match &mut self.nodes[node] {
            NodeHandle::Process(n) => n.restart().await,
            NodeHandle::Inline(_) => panic!("restart_node() not supported on Inline backend"),
            _ => panic!("restart_node() not supported on this backend"),
        }
    }

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

    pub async fn wait_for_snapshot(&self, node: usize, tx_id: u64) {
        let timeout = Duration::from_secs(60);
        let start = tokio::time::Instant::now();

        loop {
            let idx = self
                .client(node)
                .get_pipeline_index()
                .await
                .expect("get_pipeline_index RPC failed");

            if idx.snapshot >= tx_id {
                return;
            }
            if start.elapsed() >= timeout {
                panic!(
                    "wait_for_snapshot timed out: tx_id={}, snapshot_index={}",
                    tx_id, idx.snapshot
                );
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    pub fn node_data_dir(&self, node: usize) -> std::path::PathBuf {
        match &self.nodes[node] {
            NodeHandle::Process(n) => n.data_dir(),
            _ => panic!("node_data_dir() only supported on Process backend"),
        }
    }
}

/// Map the macro-generated `i32` wait level to the proto enum.
fn wait_level_from_i32(wl: i32) -> WaitLevel {
    match wl {
        0 => WaitLevel::Computed,
        1 => WaitLevel::Committed,
        2 => WaitLevel::Snapshot,
        _ => panic!("unknown wait level: {}", wl),
    }
}
