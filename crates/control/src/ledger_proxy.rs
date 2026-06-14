//! Ledger gRPC proxy.
//!
//! The control plane process exposes `roda.ledger.v1.Ledger` on the same
//! port as the operational `Control` service and forwards each call to
//! the live cluster.
//!
//! Routing is delegated to [`client::ClusterClient`] (held by the
//! [`ClusterHandle`]), so the proxy inherits the cluster client's leader
//! discovery, leader rotation on `not a leader` rejections, round-robin
//! reads, and exponential backoff. Membership changes (`SetNodeCount`,
//! `UpdateClusterConfig`, `ResetCluster`) reprovision the cluster
//! through `ClusterHandle::reprovision`, which atomically swaps in a
//! fresh `ClusterClient` — the proxy's next [`ClusterHandle::client`]
//! call picks up the new cluster automatically, so there is no
//! membership-state to maintain here.
//!
//! The proxy adds two things on top of `ClusterClient`:
//!
//! - **Per-node pinning** — if the request carries a `node-selector`
//!   metadata header (decimal node_id), the call goes to that specific
//!   peer with no retry, regardless of role. The server's response is
//!   surfaced verbatim so the caller can see exactly what that node
//!   returned.
//! - **Raw metadata forwarding** — the caller's request metadata is
//!   passed through to the peer (with `node-selector` stripped), so
//!   future cross-cutting headers (auth, tracing) ride along without
//!   touching the proxy.

// Body messages are passed into closures that may run multiple times
// (per-attempt retry inside `ClusterClient`). Using `.clone()` uniformly
// keeps the dispatch closures shaped the same regardless of whether
// the body is a `Copy` scalar wrapper or a heap-backed message.
#![allow(clippy::clone_on_copy)]

use std::sync::Arc;

use ::proto::ledger as pb;
use ::proto::ledger::ledger_client::LedgerClient;
use ::proto::ledger::ledger_server::Ledger;
use client::{ClusterClient, NodeClient};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::cluster_handle::ClusterHandle;

/// Metadata key callers set to pin an RPC to a specific node id. The
/// value is a decimal `u64` matching one of the cluster's `node_id`s
/// (1-based, position in the cluster's node list). Unknown ids return
/// `INVALID_ARGUMENT`.
pub const NODE_SELECTOR_METADATA_KEY: &str = "node-selector";

/// Ledger proxy. Cheap to clone — holds only an `Arc<ClusterHandle>`
/// and the `ClusterClient` is fetched on every RPC via the handle's
/// lock-free `ArcSwap` load.
#[derive(Clone)]
pub struct LedgerProxy {
    handle: Arc<ClusterHandle>,
}

impl LedgerProxy {
    pub fn new(handle: Arc<ClusterHandle>) -> Self {
        Self { handle }
    }

    /// Resolve the optional `node-selector` metadata to a peer index
    /// in the current cluster client. Returns `Err(Status::invalid_argument)`
    /// if the header is malformed or names an unknown node.
    #[allow(clippy::result_large_err)]
    fn parse_node_selector(
        handle: &ClusterHandle,
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
        let idx = handle.idx_for_node_id(id).ok_or_else(|| {
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

    /// Run `op` against the peer at `idx` of the current cluster
    /// client. No retry — pinned calls surface the addressed node's
    /// response verbatim.
    async fn with_pinned<F, Fut, T>(cluster: &ClusterClient, idx: usize, op: F) -> Result<T, Status>
    where
        F: FnOnce(LedgerClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        if idx >= cluster.node_count() {
            return Err(Status::invalid_argument(format!(
                "node index {idx} out of range (cluster has {})",
                cluster.node_count()
            )));
        }
        op(cluster.node(idx).ledger_client()).await
    }

    /// Dispatch a write-style RPC. Honors `node-selector`; otherwise
    /// delegates to [`ClusterClient::leader`]'s `with_leader_retry`
    /// (leader cache, rotation on `not a leader`, exponential backoff).
    async fn dispatch_write<F, Fut, T>(
        op_name: &'static str,
        cluster: Arc<ClusterClient>,
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
            return Self::with_pinned(&cluster, idx, move |c| op(c, md)).await;
        }
        cluster
            .leader()
            .with_leader_retry(op_name, move |nc: NodeClient| {
                let md = md.clone();
                let op = op.clone();
                async move { op(nc.ledger_client(), md).await }
            })
            .await
    }

    /// Dispatch a read-style RPC. Honors `node-selector`; otherwise
    /// delegates to [`ClusterClient::with_read_retry`] (round-robin +
    /// retry across peers).
    async fn dispatch_read<F, Fut, T>(
        op_name: &'static str,
        cluster: Arc<ClusterClient>,
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
            return Self::with_pinned(&cluster, idx, move |c| op(c, md)).await;
        }
        cluster
            .with_read_retry(op_name, move |nc: NodeClient| {
                let md = md.clone();
                let op = op.clone();
                async move { op(nc.ledger_client(), md).await }
            })
            .await
    }
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
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_write("submit_operation", cluster, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.submit_operation(outbound(md, body)).await }
        })
        .await
    }

    async fn submit_and_wait(
        &self,
        request: Request<pb::SubmitAndWaitRequest>,
    ) -> Result<Response<pb::SubmitAndWaitResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_write("submit_and_wait", cluster, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.submit_and_wait(outbound(md, body)).await }
        })
        .await
    }

    async fn submit_and_wait_result(
        &self,
        request: Request<pb::SubmitAndWaitResultRequest>,
    ) -> Result<Response<pb::SubmitAndWaitResultResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_write(
            "submit_and_wait_result",
            cluster,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.submit_and_wait_result(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn submit_batch(
        &self,
        request: Request<pb::SubmitBatchRequest>,
    ) -> Result<Response<pb::SubmitBatchResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_write("submit_batch", cluster, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.submit_batch(outbound(md, body)).await }
        })
        .await
    }

    async fn submit_batch_and_wait(
        &self,
        request: Request<pb::SubmitBatchAndWaitRequest>,
    ) -> Result<Response<pb::SubmitBatchAndWaitResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_write(
            "submit_batch_and_wait",
            cluster,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.submit_batch_and_wait(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn submit_batch_and_wait_result(
        &self,
        request: Request<pb::SubmitBatchAndWaitResultRequest>,
    ) -> Result<Response<pb::SubmitBatchAndWaitResultResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_write(
            "submit_batch_and_wait_result",
            cluster,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.submit_batch_and_wait_result(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn get_balance(
        &self,
        request: Request<pb::GetBalanceRequest>,
    ) -> Result<Response<pb::GetBalanceResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read("get_balance", cluster, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_balance(outbound(md, body)).await }
        })
        .await
    }

    async fn get_balances(
        &self,
        request: Request<pb::GetBalancesRequest>,
    ) -> Result<Response<pb::GetBalancesResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read("get_balances", cluster, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_balances(outbound(md, body)).await }
        })
        .await
    }

    async fn get_transaction_status(
        &self,
        request: Request<pb::GetStatusRequest>,
    ) -> Result<Response<pb::GetStatusResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read(
            "get_transaction_status",
            cluster,
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
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read(
            "get_transaction_statuses",
            cluster,
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
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read(
            "wait_for_transaction",
            cluster,
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
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read(
            "get_pipeline_index",
            cluster,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.get_pipeline_index(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn get_transaction(
        &self,
        request: Request<pb::GetTransactionRequest>,
    ) -> Result<Response<pb::GetTransactionResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read("get_transaction", cluster, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_transaction(outbound(md, body)).await }
        })
        .await
    }

    async fn get_account_history(
        &self,
        request: Request<pb::GetAccountHistoryRequest>,
    ) -> Result<Response<pb::GetAccountHistoryResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read(
            "get_account_history",
            cluster,
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
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_write(
            "register_function",
            cluster,
            md,
            pinned,
            move |mut c, md| {
                let body = body.clone();
                async move { c.register_function(outbound(md, body)).await }
            },
        )
        .await
    }

    async fn unregister_function(
        &self,
        request: Request<pb::UnregisterFunctionRequest>,
    ) -> Result<Response<pb::UnregisterFunctionResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_write(
            "unregister_function",
            cluster,
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
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read("list_functions", cluster, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.list_functions(outbound(md, body)).await }
        })
        .await
    }

    async fn get_log(
        &self,
        request: Request<pb::GetLogRequest>,
    ) -> Result<Response<pb::GetLogResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read("get_log", cluster, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_log(outbound(md, body)).await }
        })
        .await
    }

    async fn get_terms(
        &self,
        request: Request<pb::GetTermsRequest>,
    ) -> Result<Response<pb::GetTermsResponse>, Status> {
        let pinned = LedgerProxy::parse_node_selector(&self.handle, request.metadata())?;
        let md = LedgerProxy::forward_metadata(&request);
        let body = request.into_inner();
        let cluster = self.handle.client();
        Self::dispatch_read("get_terms", cluster, md, pinned, move |mut c, md| {
            let body = body.clone();
            async move { c.get_terms(outbound(md, body)).await }
        })
        .await
    }
}
