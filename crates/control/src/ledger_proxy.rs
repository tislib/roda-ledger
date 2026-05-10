//! Ledger gRPC proxy.
//!
//! The control plane process exposes `roda.ledger.v1.Ledger` on the same
//! port as the operational `Control` service and forwards each call to
//! one of the ledger nodes it was provisioned with.
//!
//! Routing is delegated to [`client::ClusterClient`], so the proxy
//! inherits the cluster client's leader discovery, leader rotation on
//! `not a leader` rejections, round-robin reads, and exponential
//! backoff. The proxy adds two things on top of that:
//!
//! - **Per-node pinning** — if the request carries a `node-selector`
//!   metadata header (decimal node_id), the call goes to that specific
//!   peer with no retry, regardless of role. The server's response is
//!   surfaced verbatim so the caller can see exactly what that node
//!   returned.
//! - **Hot-swappable membership** — [`LedgerProxy::sync_peers`]
//!   rebuilds the inner [`ClusterClient`] when the operator changes
//!   cluster size. Existing peers whose `(node_id, url)` is unchanged
//!   keep their long-lived tonic channel; new peers lazily-connect;
//!   removed peers' channels are dropped when the previous
//!   `ClusterClient` is freed.
//!
//! Each Ledger handler takes a single [`ProxyState`] snapshot at the
//! start of dispatch via [`ArcSwap::load_full`], so a concurrent
//! membership swap can't shift state under an in-flight RPC.

// Body messages are passed into closures that may run multiple times
// (per-attempt retry inside `ClusterClient`). Using `.clone()` uniformly
// keeps the dispatch closures shaped the same regardless of whether
// the body is a `Copy` scalar wrapper or a heap-backed message.
#![allow(clippy::clone_on_copy)]

use std::collections::HashMap;
use std::sync::Arc;

use ::proto::ledger as pb;
use ::proto::ledger::ledger_client::LedgerClient;
use ::proto::ledger::ledger_server::Ledger;
use arc_swap::ArcSwap;
use client::{ClusterClient, NodeClient};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

/// Metadata key callers set to pin an RPC to a specific node id. The
/// value is a decimal `u64` matching one of the provisioned peers'
/// `node_id`s. Unknown ids return `INVALID_ARGUMENT`.
pub const NODE_SELECTOR_METADATA_KEY: &str = "node-selector";

/// Convenience tuple — what the operator hands us per peer.
pub type PeerSpec = (u64, String);

/// Hot-swappable inner state shared by every clone of [`LedgerProxy`].
///
/// `cluster` owns the per-peer tonic channels and the leader/RR cursors
/// that drive routing. `node_id_to_index` maps the `node-selector`
/// header value to a position in `cluster`'s node list — the cluster
/// client itself is identity-agnostic, so the proxy keeps this map
/// alongside.
struct ProxyState {
    cluster: ClusterClient,
    node_id_to_index: HashMap<u64, usize>,
    /// `(node_id, url)` for every peer in the cluster, in node-list
    /// order. Used when [`LedgerProxy::sync_peers`] diffs the desired
    /// peers against the current state to decide which `NodeClient`s
    /// can be reused verbatim.
    peer_meta: Vec<PeerSpec>,
}

/// Ledger proxy. Cheap to clone — the underlying state lives in `Arc`s.
#[derive(Clone)]
pub struct LedgerProxy {
    state: Arc<ArcSwap<ProxyState>>,
}

impl LedgerProxy {
    /// Build a proxy from a non-empty list of `(node_id, url)` peers.
    /// Channels connect lazily on the first RPC.
    pub fn new(peers: Vec<PeerSpec>) -> Result<Self, tonic::transport::Error> {
        assert!(!peers.is_empty(), "LedgerProxy requires at least one peer");
        let state = build_state(&peers, None)?;
        Ok(Self {
            state: Arc::new(ArcSwap::from_pointee(state)),
        })
    }

    /// Take a snapshot of the current proxy state. The returned `Arc`
    /// keeps the snapshot alive even if a concurrent `sync_peers`
    /// swaps in a fresh state, so an in-flight RPC keeps its
    /// `ClusterClient` reference valid.
    fn snapshot(&self) -> Arc<ProxyState> {
        self.state.load_full()
    }

    /// Current number of provisioned peers.
    pub fn peer_count(&self) -> usize {
        self.snapshot().cluster.node_count()
    }

    /// Replace the peer list with `desired`. Existing peers whose
    /// `(node_id, url)` still appear in `desired` are reused —
    /// their long-lived tonic channels survive the swap. New entries
    /// lazily-connect; removed ones have their channels dropped when
    /// the old `ProxyState` is freed.
    ///
    /// Refuses an empty target list so a stray `SetNodeCount(0)` can't
    /// leave the proxy with no routes.
    pub fn sync_peers(&self, desired: &[PeerSpec]) -> Result<(), tonic::transport::Error> {
        if desired.is_empty() {
            warn!("ledger-proxy: refusing to swap to an empty peer list — keeping current list");
            return Ok(());
        }
        let prev = self.snapshot();
        let next = build_state(desired, Some(&prev))?;
        let added = desired
            .iter()
            .filter(|(_, url)| !prev.peer_meta.iter().any(|(_, u)| u == url))
            .count();
        let removed = prev
            .peer_meta
            .iter()
            .filter(|(_, url)| !desired.iter().any(|(_, u)| u == url))
            .count();
        info!(
            "ledger-proxy: peer list synced — was={}, now={} (added={}, removed={})",
            prev.peer_meta.len(),
            desired.len(),
            added,
            removed
        );
        self.state.store(Arc::new(next));
        Ok(())
    }

    /// Resolve the optional `node-selector` metadata to a peer index in
    /// `state.cluster`. Returns `Err(Status::invalid_argument)` if the
    /// header is malformed or names an unknown node.
    #[allow(clippy::result_large_err)]
    fn parse_node_selector(
        state: &ProxyState,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Option<usize>, Status> {
        let raw = match metadata.get(NODE_SELECTOR_METADATA_KEY) {
            Some(v) => v,
            None => return Ok(None),
        };
        let s = raw
            .to_str()
            .map_err(|_| Status::invalid_argument("node-selector must be ASCII"))?;
        let id: u64 = s
            .parse()
            .map_err(|_| Status::invalid_argument(format!("node-selector '{s}' is not a u64")))?;
        let idx = state.node_id_to_index.get(&id).copied().ok_or_else(|| {
            Status::invalid_argument(format!("unknown node-selector node_id {id}"))
        })?;
        Ok(Some(idx))
    }

    /// Forward the caller's request metadata to the peer — strip the
    /// node-selector so the peer doesn't try to re-interpret it.
    fn forward_metadata<T>(req: &Request<T>) -> tonic::metadata::MetadataMap {
        let mut md = req.metadata().clone();
        md.remove(NODE_SELECTOR_METADATA_KEY);
        md
    }

    /// Run `op` against the peer at `idx` of `state.cluster`. No retry —
    /// pinned calls surface the addressed node's response verbatim.
    async fn with_pinned<F, Fut, T>(state: &ProxyState, idx: usize, op: F) -> Result<T, Status>
    where
        F: FnOnce(LedgerClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let nc = state.cluster.node(idx);
        op(nc.ledger_client()).await
    }

    /// Dispatch a write-style RPC. Honors `node-selector`; otherwise
    /// delegates to [`ClusterClient::with_leader_retry`].
    async fn dispatch_write<F, Fut, T>(
        op_name: &'static str,
        state: Arc<ProxyState>,
        md: tonic::metadata::MetadataMap,
        pinned_idx: Option<usize>,
        op: F,
    ) -> Result<Response<T>, Status>
    where
        F: Fn(LedgerClient<Channel>, tonic::metadata::MetadataMap) -> Fut + Clone + Send + Sync,
        Fut: Future<Output = Result<Response<T>, Status>> + Send,
        T: Send + 'static,
    {
        if let Some(idx) = pinned_idx {
            let md = md.clone();
            return Self::with_pinned(&state, idx, move |c| op(c, md)).await;
        }
        state
            .cluster
            .leader()
            .with_leader_retry(op_name, move |nc: NodeClient| {
                let md = md.clone();
                let op = op.clone();
                async move { op(nc.ledger_client(), md).await }
            })
            .await
    }

    /// Dispatch a read-style RPC. Honors `node-selector`; otherwise
    /// delegates to [`ClusterClient::with_read_retry`].
    async fn dispatch_read<F, Fut, T>(
        op_name: &'static str,
        state: Arc<ProxyState>,
        md: tonic::metadata::MetadataMap,
        pinned_idx: Option<usize>,
        op: F,
    ) -> Result<Response<T>, Status>
    where
        F: Fn(LedgerClient<Channel>, tonic::metadata::MetadataMap) -> Fut + Clone + Send + Sync,
        Fut: Future<Output = Result<Response<T>, Status>> + Send,
        T: Send + 'static,
    {
        if let Some(idx) = pinned_idx {
            let md = md.clone();
            return Self::with_pinned(&state, idx, move |c| op(c, md)).await;
        }
        state
            .cluster
            .with_read_retry(op_name, move |nc: NodeClient| {
                let md = md.clone();
                let op = op.clone();
                async move { op(nc.ledger_client(), md).await }
            })
            .await
    }
}

/// Build a fresh [`ProxyState`] from `peers`, reusing existing
/// [`NodeClient`]s from `prev` whenever a `(node_id, url)` is
/// unchanged so their tonic channels survive the swap.
fn build_state(
    peers: &[PeerSpec],
    prev: Option<&ProxyState>,
) -> Result<ProxyState, tonic::transport::Error> {
    use client::RetryConfig;
    let mut nodes = Vec::with_capacity(peers.len());
    let mut node_id_to_index = HashMap::with_capacity(peers.len());
    for (idx, (node_id, url)) in peers.iter().enumerate() {
        let nc = match prev.and_then(|p| {
            p.peer_meta
                .iter()
                .position(|(id, u)| id == node_id && u == url)
                .map(|i| p.cluster.node(i).clone())
        }) {
            Some(existing) => existing,
            None => NodeClient::connect_lazy(url)?,
        };
        nodes.push(nc);
        node_id_to_index.insert(*node_id, idx);
    }
    let cluster = ClusterClient::from_nodes(nodes, RetryConfig::default());
    Ok(ProxyState {
        cluster,
        node_id_to_index,
        peer_meta: peers.to_vec(),
    })
}

/// Build a fresh outbound `Request<T>` carrying `md` + `body`. Helper
/// because tonic doesn't expose a direct `Request::from_metadata_body`.
fn outbound<T>(md: tonic::metadata::MetadataMap, body: T) -> Request<T> {
    let mut req = Request::new(body);
    *req.metadata_mut() = md;
    req
}

// ── Ledger trait impl — every method delegates through the proxy ────────────

#[tonic::async_trait]
impl Ledger for LedgerProxy {
    async fn submit_operation(
        &self,
        request: Request<pb::SubmitOperationRequest>,
    ) -> Result<Response<pb::SubmitOperationResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_write("submit_operation", state, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.submit_operation(outbound(md, body)).await }
        })
        .await
    }

    async fn submit_and_wait(
        &self,
        request: Request<pb::SubmitAndWaitRequest>,
    ) -> Result<Response<pb::SubmitAndWaitResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_write("submit_and_wait", state, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.submit_and_wait(outbound(md, body)).await }
        })
        .await
    }

    async fn submit_batch(
        &self,
        request: Request<pb::SubmitBatchRequest>,
    ) -> Result<Response<pb::SubmitBatchResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_write("submit_batch", state, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.submit_batch(outbound(md, body)).await }
        })
        .await
    }

    async fn submit_batch_and_wait(
        &self,
        request: Request<pb::SubmitBatchAndWaitRequest>,
    ) -> Result<Response<pb::SubmitBatchAndWaitResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_write(
            "submit_batch_and_wait",
            state,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.submit_batch_and_wait(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn get_balance(
        &self,
        request: Request<pb::GetBalanceRequest>,
    ) -> Result<Response<pb::GetBalanceResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_read("get_balance", state, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_balance(outbound(md, body)).await }
        })
        .await
    }

    async fn get_balances(
        &self,
        request: Request<pb::GetBalancesRequest>,
    ) -> Result<Response<pb::GetBalancesResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_read("get_balances", state, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_balances(outbound(md, body)).await }
        })
        .await
    }

    async fn get_transaction_status(
        &self,
        request: Request<pb::GetStatusRequest>,
    ) -> Result<Response<pb::GetStatusResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_read(
            "get_transaction_status",
            state,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.get_transaction_status(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn get_transaction_statuses(
        &self,
        request: Request<pb::GetStatusesRequest>,
    ) -> Result<Response<pb::GetStatusesResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_read(
            "get_transaction_statuses",
            state,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.get_transaction_statuses(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn wait_for_transaction(
        &self,
        request: Request<pb::WaitForTransactionRequest>,
    ) -> Result<Response<pb::WaitForTransactionResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_read(
            "wait_for_transaction",
            state,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.wait_for_transaction(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn get_pipeline_index(
        &self,
        request: Request<pb::GetPipelineIndexRequest>,
    ) -> Result<Response<pb::GetPipelineIndexResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_read("get_pipeline_index", state, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_pipeline_index(outbound(md, body)).await }
        })
        .await
    }

    async fn get_transaction(
        &self,
        request: Request<pb::GetTransactionRequest>,
    ) -> Result<Response<pb::GetTransactionResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_read("get_transaction", state, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_transaction(outbound(md, body)).await }
        })
        .await
    }

    async fn get_account_history(
        &self,
        request: Request<pb::GetAccountHistoryRequest>,
    ) -> Result<Response<pb::GetAccountHistoryResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_read(
            "get_account_history",
            state,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.get_account_history(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn register_function(
        &self,
        request: Request<pb::RegisterFunctionRequest>,
    ) -> Result<Response<pb::RegisterFunctionResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_write("register_function", state, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.register_function(outbound(md, body)).await }
        })
        .await
    }

    async fn unregister_function(
        &self,
        request: Request<pb::UnregisterFunctionRequest>,
    ) -> Result<Response<pb::UnregisterFunctionResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_write(
            "unregister_function",
            state,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.unregister_function(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn list_functions(
        &self,
        request: Request<pb::ListFunctionsRequest>,
    ) -> Result<Response<pb::ListFunctionsResponse>, Status> {
        let state = self.snapshot();
        let pinned = LedgerProxy::parse_node_selector(&state, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        Self::dispatch_read("list_functions", state, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.list_functions(outbound(md, body)).await }
        })
        .await
    }
}
