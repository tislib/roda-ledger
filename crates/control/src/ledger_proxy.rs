//! Ledger gRPC proxy.
//!
//! The control plane process exposes `roda.ledger.v1.Ledger` on the same
//! port as the operational `Control` service and forwards each call to
//! one of the ledger nodes it was provisioned with. Routing rules:
//!
//! - **Per-node pinning** — if the request carries a `node-selector`
//!   metadata header (decimal node_id), the call is sent to that
//!   specific peer regardless of role.
//! - **Writes** — `SubmitOperation`, `SubmitAndWait`, `SubmitBatch`,
//!   `SubmitBatchAndWait`, `RegisterFunction`, `UnregisterFunction`
//!   are routed to the cached leader; "not a leader" rejections rotate
//!   the leader cursor (or follow the `leader-node-index` hint when
//!   present) and retry on the next peer.
//! - **Reads** — every other RPC round-robins across all known peers
//!   so a single slow / restarting node can't trap the read.
//!
//! Each peer keeps a long-lived `LedgerClient<Channel>`; tonic's
//! `Channel` clones cheaply for fan-out so we don't need an explicit
//! pool. The proxy holds no per-call state beyond the round-robin and
//! leader cursors.

// Body messages are passed into closures that may run multiple times
// (per-attempt retry). Using `.clone()` uniformly keeps the dispatch
// closures shaped the same regardless of whether the body is a `Copy`
// scalar wrapper or a heap-backed message; clippy's stylistic
// preference for omitting the clone on `Copy` types would force two
// closure shapes for the same routing logic.
#![allow(clippy::clone_on_copy)]

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ::proto::ledger as pb;
use ::proto::ledger::ledger_client::LedgerClient;
use ::proto::ledger::ledger_server::Ledger;
use arc_swap::ArcSwap;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Response, Status};
use tracing::{info, warn};

/// Metadata key callers set to pin an RPC to a specific node id. The
/// value is a decimal `u64` matching one of the provisioned peers'
/// `node_id`s. Unknown ids return `INVALID_ARGUMENT`.
pub const NODE_SELECTOR_METADATA_KEY: &str = "node-selector";

/// Metadata key the server may attach on a "not a leader" rejection
/// to point the client at the current leader's index. Mirrors the
/// constant in `client::cluster_client`.
const LEADER_HINT_METADATA_KEY: &str = "leader-node-index";

/// One provisioned cluster peer.
#[derive(Clone)]
pub struct Peer {
    pub node_id: u64,
    pub url: String,
    pub client: LedgerClient<Channel>,
}

impl Peer {
    /// Build a peer with a lazily-connecting tonic channel. Channel
    /// connection is deferred until the first RPC, so a peer that's
    /// down at startup doesn't fail proxy bring-up.
    pub fn connect_lazy(node_id: u64, url: String) -> Result<Self, tonic::transport::Error> {
        let endpoint = Endpoint::from_str(&url)?;
        let channel = endpoint.connect_lazy();
        Ok(Self {
            node_id,
            url,
            client: LedgerClient::new(channel),
        })
    }
}

/// Maximum attempts the proxy will make per logical RPC before giving
/// up. The leader-rotation path retries this many times against
/// successive peers; the round-robin read path retries this many times
/// against successive peers.
const MAX_PROXY_ATTEMPTS: usize = 8;

/// Ledger proxy. Cheap to clone — the underlying state is in `Arc`s.
///
/// The peer list is kept behind an [`ArcSwap`] so callers can swap it
/// at runtime when membership changes (e.g. after `Control.SetNodeCount`)
/// without locking out in-flight RPCs. Each RPC takes a single
/// [`ArcSwap::load_full`] snapshot at the start of dispatch, so a
/// concurrent membership swap can't shift indices under it mid-call.
#[derive(Clone)]
pub struct LedgerProxy {
    peers: Arc<ArcSwap<Vec<Peer>>>,
    /// Cached "best guess" for which peer is currently the leader.
    /// Updated on every write-style call: kept on success, advanced
    /// on a "not a leader" / transport-level rejection (preferring a
    /// `leader-node-index` hint when present). The value is reduced
    /// modulo `peers.len()` at every read so it stays valid across
    /// peer-list swaps.
    leader_idx: Arc<AtomicUsize>,
    /// Round-robin cursor for read-style calls. Independent of
    /// `leader_idx` so reads keep spreading load even while a write
    /// is hunting for the leader.
    read_idx: Arc<AtomicUsize>,
}

impl LedgerProxy {
    /// Build a proxy from a non-empty list of provisioned peers.
    pub fn new(peers: Vec<Peer>) -> Self {
        assert!(!peers.is_empty(), "LedgerProxy requires at least one peer");
        Self {
            peers: Arc::new(ArcSwap::from_pointee(peers)),
            leader_idx: Arc::new(AtomicUsize::new(0)),
            read_idx: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Take a snapshot of the current peer list. The returned `Arc`
    /// keeps the snapshot alive even if a concurrent `sync_peers`
    /// swaps in a fresh list — RPCs that picked an index from this
    /// snapshot keep referring to a valid `Peer`.
    fn peers_snapshot(&self) -> Arc<Vec<Peer>> {
        self.peers.load_full()
    }

    /// Current number of provisioned peers.
    pub fn peer_count(&self) -> usize {
        self.peers_snapshot().len()
    }

    /// Replace the peer list with `desired`. Existing `Peer` entries
    /// whose `(node_id, url)` still appear in `desired` are reused
    /// verbatim — their long-lived tonic channels survive the swap,
    /// so unchanged peers don't pay a connection-rebuild cost on every
    /// membership change. New entries lazily-connect; removed ones
    /// have their channels dropped when the old `Vec` is freed.
    pub fn sync_peers(
        &self,
        desired: &[(u64, String)],
    ) -> Result<(), tonic::transport::Error> {
        let current = self.peers_snapshot();
        let mut new_peers = Vec::with_capacity(desired.len());
        let mut added = 0usize;
        for (id, url) in desired {
            match current
                .iter()
                .find(|p| p.node_id == *id && &p.url == url)
                .cloned()
            {
                Some(existing) => new_peers.push(existing),
                None => {
                    new_peers.push(Peer::connect_lazy(*id, url.clone())?);
                    added += 1;
                }
            }
        }
        let removed = current.len().saturating_sub(new_peers.len() - added.min(new_peers.len()));
        info!(
            "ledger-proxy: peer list synced — was={}, now={} (added={}, removed={})",
            current.len(),
            new_peers.len(),
            added,
            removed
        );
        // The proxy can't operate without peers — refuse an empty swap
        // so RPCs don't start failing with "no peer reachable" the
        // moment a SetNodeCount(0) lands.
        if new_peers.is_empty() {
            warn!(
                "ledger-proxy: refusing to swap to an empty peer list — keeping current list"
            );
            return Ok(());
        }
        self.peers.store(Arc::new(new_peers));
        Ok(())
    }

    /// Read the optional `node-selector` metadata and resolve it to a
    /// peer index against `peers`. Returns `Err(Status::invalid_argument)`
    /// if the header is malformed or names an unknown node.
    #[allow(clippy::result_large_err)]
    fn parse_node_selector(
        peers: &[Peer],
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Option<usize>, Status> {
        let raw = match metadata.get(NODE_SELECTOR_METADATA_KEY) {
            Some(v) => v,
            None => return Ok(None),
        };
        let s = raw
            .to_str()
            .map_err(|_| Status::invalid_argument("node-selector must be ASCII"))?;
        let id: u64 = s.parse().map_err(|_| {
            Status::invalid_argument(format!("node-selector '{s}' is not a u64"))
        })?;
        let idx = peers
            .iter()
            .position(|p| p.node_id == id)
            .ok_or_else(|| Status::invalid_argument(format!("unknown node-selector node_id {id}")))?;
        Ok(Some(idx))
    }

    fn forward_metadata<T>(req: &Request<T>) -> tonic::metadata::MetadataMap {
        // Forward the caller's metadata to the peer — strip the
        // node-selector so the peer doesn't try to re-interpret it.
        let mut md = req.metadata().clone();
        md.remove(NODE_SELECTOR_METADATA_KEY);
        md
    }
}

// ── Routing helpers ─────────────────────────────────────────────────────────

/// True when the server told us "this node is not the leader". Mirrors
/// the producer in `cluster::ledger_handler::ensure_writable`.
fn is_not_leader(status: &Status) -> bool {
    status.code() == Code::FailedPrecondition && status.message().contains("not a leader")
}

/// True when retrying the same peer is unlikely to help and we should
/// rotate to a different peer.
fn should_rotate(status: &Status) -> bool {
    if is_not_leader(status) {
        return true;
    }
    matches!(
        status.code(),
        Code::Unavailable | Code::Cancelled | Code::Unknown | Code::Internal
    )
}

/// Optional `leader-node-index` hint, parsed and bounds-checked.
fn read_leader_hint(status: &Status, n_peers: usize) -> Option<usize> {
    let s = status
        .metadata()
        .get(LEADER_HINT_METADATA_KEY)?
        .to_str()
        .ok()?;
    let idx: usize = s.parse().ok()?;
    if idx < n_peers { Some(idx) } else { None }
}

impl LedgerProxy {
    /// Run `op` against the cached leader of `peers`. On a routable
    /// failure (`not a leader`, transport error), rotate the cursor
    /// and retry within the same snapshot.
    async fn with_leader_retry<F, Fut, T>(
        &self,
        op_name: &str,
        peers: &[Peer],
        op: F,
    ) -> Result<T, Status>
    where
        F: Fn(LedgerClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let n = peers.len();
        let mut last_err: Option<Status> = None;
        for attempt in 0..MAX_PROXY_ATTEMPTS {
            let idx = self.leader_idx.load(Ordering::Acquire) % n;
            let peer = &peers[idx];
            match op(peer.client.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if !should_rotate(&e) || attempt + 1 == MAX_PROXY_ATTEMPTS {
                        if attempt + 1 == MAX_PROXY_ATTEMPTS {
                            warn!(
                                "ledger-proxy::{}: exhausted {} attempts (last node_id={}, code={:?}, msg='{}')",
                                op_name,
                                MAX_PROXY_ATTEMPTS,
                                peer.node_id,
                                e.code(),
                                e.message()
                            );
                        }
                        last_err = Some(e);
                        break;
                    }
                    let next_idx =
                        read_leader_hint(&e, n).unwrap_or_else(|| (idx + 1) % n);
                    let _ = self.leader_idx.compare_exchange(
                        idx,
                        next_idx,
                        Ordering::Release,
                        Ordering::Acquire,
                    );
                    warn!(
                        "ledger-proxy::{}: node[{}] (id={}) not leader/reachable (code={:?}) — rotating to node[{}] (id={})",
                        op_name,
                        idx,
                        peer.node_id,
                        e.code(),
                        next_idx,
                        peers[next_idx].node_id,
                    );
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("no peer reachable")))
    }

    /// Run `op` against the next peer in round-robin order across the
    /// `peers` snapshot. On any `Status` error retry against the next
    /// peer, capped at `MAX_PROXY_ATTEMPTS`.
    async fn with_read_retry<F, Fut, T>(
        &self,
        op_name: &str,
        peers: &[Peer],
        op: F,
    ) -> Result<T, Status>
    where
        F: Fn(LedgerClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let n = peers.len();
        let attempts = MAX_PROXY_ATTEMPTS.min(n.saturating_mul(2)).max(n);
        let mut last_err: Option<Status> = None;
        for attempt in 0..attempts {
            let idx = self.read_idx.fetch_add(1, Ordering::Relaxed) % n;
            let peer = &peers[idx];
            match op(peer.client.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt + 1 == attempts {
                        warn!(
                            "ledger-proxy::{}: exhausted {} read attempts (last node_id={}, code={:?})",
                            op_name,
                            attempts,
                            peer.node_id,
                            e.code()
                        );
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("no peer reachable")))
    }

    /// Run `op` against the peer at `idx` of `peers`. No retry — when
    /// the caller pinned a specific node we surface the result as-is
    /// so they can see exactly what that node returned.
    async fn with_pinned<F, Fut, T>(peers: &[Peer], idx: usize, op: F) -> Result<T, Status>
    where
        F: FnOnce(LedgerClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let peer = &peers[idx];
        op(peer.client.clone()).await
    }

    /// Dispatch a write-style RPC. Honors `node-selector` if set.
    async fn dispatch_write<F, Fut, T>(
        &self,
        op_name: &str,
        peers: Arc<Vec<Peer>>,
        md: tonic::metadata::MetadataMap,
        pinned_idx: Option<usize>,
        op: F,
    ) -> Result<Response<T>, Status>
    where
        F: Fn(LedgerClient<Channel>, tonic::metadata::MetadataMap) -> Fut + Clone,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        if let Some(idx) = pinned_idx {
            let md = md.clone();
            return Self::with_pinned(&peers, idx, move |c| op(c, md)).await;
        }
        let op = op.clone();
        self.with_leader_retry(op_name, &peers, move |c| {
            let md = md.clone();
            op(c.clone(), md)
        })
        .await
    }

    /// Dispatch a read-style RPC. Honors `node-selector` if set.
    async fn dispatch_read<F, Fut, T>(
        &self,
        op_name: &str,
        peers: Arc<Vec<Peer>>,
        md: tonic::metadata::MetadataMap,
        pinned_idx: Option<usize>,
        op: F,
    ) -> Result<Response<T>, Status>
    where
        F: Fn(LedgerClient<Channel>, tonic::metadata::MetadataMap) -> Fut + Clone,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        if let Some(idx) = pinned_idx {
            let md = md.clone();
            return Self::with_pinned(&peers, idx, move |c| op(c, md)).await;
        }
        let op = op.clone();
        self.with_read_retry(op_name, &peers, move |c| {
            let md = md.clone();
            op(c.clone(), md)
        })
        .await
    }
}

// Build a fresh outbound `Request<T>` carrying `md` + `body`. Helper
// because tonic doesn't expose a direct `Request::from_metadata_body`.
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
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_write("submit_operation", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.submit_operation(outbound(md, body)).await }
        })
        .await
    }

    async fn submit_and_wait(
        &self,
        request: Request<pb::SubmitAndWaitRequest>,
    ) -> Result<Response<pb::SubmitAndWaitResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_write("submit_and_wait", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.submit_and_wait(outbound(md, body)).await }
        })
        .await
    }

    async fn submit_batch(
        &self,
        request: Request<pb::SubmitBatchRequest>,
    ) -> Result<Response<pb::SubmitBatchResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_write("submit_batch", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.submit_batch(outbound(md, body)).await }
        })
        .await
    }

    async fn submit_batch_and_wait(
        &self,
        request: Request<pb::SubmitBatchAndWaitRequest>,
    ) -> Result<Response<pb::SubmitBatchAndWaitResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_write(
            "submit_batch_and_wait",
            peers.clone(),
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
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_read("get_balance", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_balance(outbound(md, body)).await }
        })
        .await
    }

    async fn get_balances(
        &self,
        request: Request<pb::GetBalancesRequest>,
    ) -> Result<Response<pb::GetBalancesResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_read("get_balances", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_balances(outbound(md, body)).await }
        })
        .await
    }

    async fn get_transaction_status(
        &self,
        request: Request<pb::GetStatusRequest>,
    ) -> Result<Response<pb::GetStatusResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_read("get_transaction_status", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_transaction_status(outbound(md, body)).await }
        })
        .await
    }

    async fn get_transaction_statuses(
        &self,
        request: Request<pb::GetStatusesRequest>,
    ) -> Result<Response<pb::GetStatusesResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_read("get_transaction_statuses", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_transaction_statuses(outbound(md, body)).await }
        })
        .await
    }

    async fn wait_for_transaction(
        &self,
        request: Request<pb::WaitForTransactionRequest>,
    ) -> Result<Response<pb::WaitForTransactionResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_read("wait_for_transaction", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.wait_for_transaction(outbound(md, body)).await }
        })
        .await
    }

    async fn get_pipeline_index(
        &self,
        request: Request<pb::GetPipelineIndexRequest>,
    ) -> Result<Response<pb::GetPipelineIndexResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_read("get_pipeline_index", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_pipeline_index(outbound(md, body)).await }
        })
        .await
    }

    async fn get_transaction(
        &self,
        request: Request<pb::GetTransactionRequest>,
    ) -> Result<Response<pb::GetTransactionResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_read("get_transaction", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_transaction(outbound(md, body)).await }
        })
        .await
    }

    async fn get_account_history(
        &self,
        request: Request<pb::GetAccountHistoryRequest>,
    ) -> Result<Response<pb::GetAccountHistoryResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_read("get_account_history", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_account_history(outbound(md, body)).await }
        })
        .await
    }

    async fn register_function(
        &self,
        request: Request<pb::RegisterFunctionRequest>,
    ) -> Result<Response<pb::RegisterFunctionResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_write("register_function", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.register_function(outbound(md, body)).await }
        })
        .await
    }

    async fn unregister_function(
        &self,
        request: Request<pb::UnregisterFunctionRequest>,
    ) -> Result<Response<pb::UnregisterFunctionResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_write("unregister_function", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.unregister_function(outbound(md, body)).await }
        })
        .await
    }

    async fn list_functions(
        &self,
        request: Request<pb::ListFunctionsRequest>,
    ) -> Result<Response<pb::ListFunctionsResponse>, Status> {
        let peers = self.peers_snapshot();
        let pinned = LedgerProxy::parse_node_selector(&peers, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        self.dispatch_read("list_functions", peers.clone(), md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.list_functions(outbound(md, body)).await }
        })
        .await
    }
}

