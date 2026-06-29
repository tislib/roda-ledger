use crate::config::Config;
use crate::consensus::state::Consensus;
#[cfg(feature = "fault-injection")]
use crate::fault::ClusterFaultInjector;
#[cfg(feature = "fault-injection")]
use crate::handlers::fault_handler::FaultHandler;
#[cfg(feature = "latency-probe")]
use crate::handlers::latency_handler::LatencyProbeHandler;
use crate::handlers::ledger_handler::LedgerHandler;
use crate::handlers::node_handler::NodeHandler;
use crate::ledger_slot::LedgerSlot;
use crate::waiter::Waiter;
use ledger::ledger::{IndexHook, Ledger};
use ledger::transactor::transaction::Operation;
use proto::ledger::WaitLevel;
use proto::ledger::ledger_server::LedgerServer;
use proto::node::node_server::NodeServer;
use spdlog::{debug, error};
use std::process::exit;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use tokio::runtime;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;

pub struct ClusterNode {
    config: Config,
    ledger: Arc<LedgerSlot>,

    // shutdown handling
    cancellation_token: CancellationToken,
    handles: Vec<JoinHandle<()>>,
    pub consensus: Arc<Consensus>,

    /// Shared pipeline-wait primitive (single instance). Cloned into the
    /// ledger handler (`*_and_wait` RPCs) and the latency-probe handler.
    waiter: Arc<Waiter>,

    /// Per-node fault state. Owned here so the `Fault` server and
    /// (in future PRs) the consensus / replication intercept hooks
    /// can share one source of truth via an `Arc` clone.
    #[cfg(feature = "fault-injection")]
    fault_injector: Arc<ClusterFaultInjector>,
}

impl ClusterNode {
    pub fn new(config: Config) -> Result<Self, String> {
        let mut ledger = Ledger::new(config.ledger.clone());
        ledger.start().map_err(|e| format!("{}", e))?;
        let ledger = Arc::new(LedgerSlot::new(ledger, config.ledger.clone()));

        // Waiter owns the reactive index watches; seed from the live ledger
        // (cluster_commit starts at 0). Shared into Consensus so the replication
        // sender / cluster-commit driver read the same watches.
        let waiter = {
            let l = ledger.current();
            Arc::new(Waiter::new(
                l.last_compute_id(),
                l.last_commit_id(),
                l.last_snapshot_id(),
                0,
            ))
        };
        let consensus = Arc::new(Consensus::new(
            config.clone(),
            ledger.clone(),
            waiter.clone(),
        )?);

        // The cluster owns the single ledger index hook: feed the waiter's
        // compute/commit/snapshot watches. Registered via LedgerSlot so it
        // survives reseed.
        {
            let w = waiter.clone();
            ledger.set_index_hook(Arc::new(move |kind, value| w.record(kind, value)));
        }

        #[cfg(feature = "fault-injection")]
        let fault_injector = ClusterFaultInjector::new(ledger.clone());

        Ok(Self {
            config,
            ledger,
            consensus,
            waiter,
            cancellation_token: Default::default(),
            handles: vec![],
            #[cfg(feature = "fault-injection")]
            fault_injector,
        })
    }

    pub fn run(mut self) -> Result<Self, String> {
        self.start_grpc_server()?;
        // Node gRPC and replication only run with a cluster section.
        if self.config.cluster.is_some() {
            self.start_node_server()?;
            self.start_replication_driver()?;
        }
        self.start_consensus()?;

        #[cfg(feature = "fault-injection")]
        self.start_fault_server()?;

        #[cfg(feature = "latency-probe")]
        self.start_latency_probe_server()?;

        Ok(self)
    }

    fn start_grpc_server(&mut self) -> Result<(), String> {
        let config = self.config.clone();
        let ledger = self.ledger.clone();
        let cancellation_token = self.cancellation_token.clone();
        let consensus = self.consensus.clone();
        let waiter = self.waiter.clone();
        let handle = thread::Builder::new()
            .name("ledger-grpc".into())
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("ledger-grpc")
                    .build()
                    .expect("build dedicated gRPC runtime");
                rt.block_on(async {
                    let ledger_handler =
                        LedgerHandler::new(ledger.clone(), consensus.clone(), waiter);
                    let client_server = ClusterNode::run_ledger_server(
                        config,
                        ledger_handler,
                        cancellation_token.clone(),
                    );
                    if let Err(e) = client_server.await {
                        error!("ledger gRPC server exited: {}", e);
                        exit(1);
                    }
                });
            })
            .map_err(|e| format!("{}", e))?;
        self.handles.push(handle);

        Ok(())
    }

    fn start_node_server(&mut self) -> Result<(), String> {
        let config = self.config.clone();
        let cancellation_token = self.cancellation_token.clone();
        let consensus = self.consensus.clone();
        let handle = thread::Builder::new()
            .name("node-grpc".into())
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("node-grpc")
                    .build()
                    .expect("build dedicated node gRPC runtime");
                rt.block_on(async {
                    let node_handler = NodeHandler::new(consensus);
                    let server =
                        ClusterNode::run_node_server(config, node_handler, cancellation_token);
                    if let Err(e) = server.await {
                        error!("node gRPC server exited: {}", e);
                        exit(1);
                    }
                });
            })
            .map_err(|e| format!("{}", e))?;
        self.handles.push(handle);

        Ok(())
    }

    fn start_consensus(&mut self) -> Result<(), String> {
        let consensus = self.consensus.clone();
        let cancellation_token = self.cancellation_token.clone();
        let handle = thread::Builder::new()
            .name("consensus".into())
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("consensus")
                    .build()
                    .expect("build dedicated consensus runtime");
                rt.block_on(async {
                    // Reactive cluster-commit driver: advances quorum on each
                    // snapshot-index advance (drives singleton cluster_commit).
                    let cc_driver = tokio::spawn({
                        let c = consensus.clone();
                        let ct = cancellation_token.clone();
                        async move { c.run_cluster_commit_driver(ct).await }
                    });
                    if let Err(e) = consensus.run_loop(cancellation_token).await {
                        error!("consensus loop exited: {}", e);
                        exit(1);
                    }
                    let _ = cc_driver.await;
                });
            })
            .map_err(|e| format!("{}", e))?;
        self.handles.push(handle);

        Ok(())
    }

    #[cfg(feature = "fault-injection")]
    fn start_fault_server(&mut self) -> Result<(), String> {
        let config = self.config.clone();
        let cancellation_token = self.cancellation_token.clone();
        let injector = self.fault_injector.clone();
        let data_dir = self.config.ledger.storage.data_dir.clone();
        let handle = thread::Builder::new()
            .name("fault-grpc".into())
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("fault-grpc")
                    .build()
                    .expect("build dedicated fault gRPC runtime");
                rt.block_on(async {
                    let handler = FaultHandler::new(injector, data_dir);
                    let server = ClusterNode::run_fault_server(config, handler, cancellation_token);
                    // Optional debug-only infra — never load-bearing.
                    // Tests drive the injector in-process via
                    // `fault_injector()`, never over this socket, and
                    // the derived `client_port + 1000` can alias another
                    // node's OS-assigned ephemeral port under the test
                    // harness. So a bind failure here degrades to "no
                    // fault server on this node", logged — it must NOT
                    // `exit(1)` and tear the node (or the test binary)
                    // down the way a failed Ledger/Node server does.
                    if let Err(e) = server.await {
                        error!("fault gRPC server exited (continuing without it): {}", e);
                    }
                });
            })
            .map_err(|e| format!("{}", e))?;
        self.handles.push(handle);

        Ok(())
    }

    /// Per-node fault injector — `Arc` clone is cheap. Exposed for
    /// in-process tests that prefer driving the injector directly
    /// over the gRPC surface.
    #[cfg(feature = "fault-injection")]
    pub fn fault_injector(&self) -> Arc<ClusterFaultInjector> {
        self.fault_injector.clone()
    }

    #[cfg(feature = "latency-probe")]
    fn start_latency_probe_server(&mut self) -> Result<(), String> {
        let config = self.config.clone();
        let cancellation_token = self.cancellation_token.clone();
        let ledger = self.ledger.clone();
        let waiter = self.waiter.clone();
        let handle = thread::Builder::new()
            .name("latency-grpc".into())
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("latency-grpc")
                    .build()
                    .expect("build dedicated latency gRPC runtime");
                rt.block_on(async {
                    let handler = LatencyProbeHandler::new(ledger, waiter);
                    let server =
                        ClusterNode::run_latency_probe_server(config, handler, cancellation_token);
                    // Optional debug-only infra — never load-bearing, same
                    // reasoning as the fault server: the derived port can
                    // alias another node's ephemeral port under the test
                    // harness, so a bind failure degrades to "no probe
                    // server on this node" (logged), it must NOT `exit(1)`.
                    if let Err(e) = server.await {
                        error!("latency gRPC server exited (continuing without it): {}", e);
                    }
                });
            })
            .map_err(|e| format!("{}", e))?;
        self.handles.push(handle);

        Ok(())
    }

    /// In-process latency probe handler, for tests that drive the probe
    /// directly rather than over the gRPC surface. Mirrors
    /// [`fault_injector`](Self::fault_injector).
    #[cfg(feature = "latency-probe")]
    pub fn latency_probe_handler(&self) -> LatencyProbeHandler {
        LatencyProbeHandler::new(self.ledger.clone(), self.waiter.clone())
    }

    // ── In-process load surface ──────────────────────────────────────────
    // Thin wrappers over the active `Ledger` and the shared `Waiter`, used by
    // the in-process `cluster_load` / `cluster_load_latency` bins to drive a
    // single-node cluster without the gRPC hop. Mirrors `Ledger`'s own bins.

    /// Submit one operation in-process (bypassing gRPC). Goes through the same
    /// `Ledger::submit` the `LedgerHandler` uses.
    pub fn submit(&self, op: Operation) -> u64 {
        self.ledger.current().submit(op)
    }

    /// Open accounts `1..=count` on the active ledger.
    pub fn open_accounts(&self, count: u32) {
        self.ledger.current().open_accounts(count);
    }

    /// Register an index hook (see `Ledger::set_index_hook`). Routed through
    /// `LedgerSlot` so it survives reseed; first registration wins, so this is a
    /// no-op once the node's own waiter hook is installed in `new`.
    pub fn set_index_hook(&self, hook: IndexHook) {
        self.ledger.set_index_hook(hook);
    }

    pub fn last_sequenced_id(&self) -> u64 {
        self.ledger.current().last_sequenced_id()
    }
    pub fn last_compute_id(&self) -> u64 {
        self.ledger.current().last_compute_id()
    }
    pub fn last_commit_id(&self) -> u64 {
        self.ledger.current().last_commit_id()
    }
    pub fn last_snapshot_id(&self) -> u64 {
        self.ledger.current().last_snapshot_id()
    }
    pub fn cluster_commit_index(&self) -> u64 {
        self.consensus.cluster_commit_index()
    }

    /// Wait until `tx_id` reaches `level` via the shared `Waiter`. Returns
    /// `true` if reached, `false` on the waiter's internal timeout.
    pub async fn wait_for_level(&self, tx_id: u64, level: WaitLevel) -> bool {
        self.waiter
            .wait_for_transaction_level(tx_id, level)
            .await
            .is_ok()
    }

    fn start_replication_driver(&mut self) -> Result<(), String> {
        let consensus = self.consensus.clone();
        let cancellation_token = self.cancellation_token.clone();
        let handle = thread::Builder::new()
            .name("replication".into())
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("replication_driver")
                    .build()
                    .expect("build dedicated replication runtime");
                rt.block_on(async {
                    if let Err(e) = consensus.run_replication_driver(cancellation_token).await {
                        error!("replication loop exited: {}", e);
                        exit(1);
                    }
                });
            })
            .map_err(|e| format!("{}", e))?;
        self.handles.push(handle);

        Ok(())
    }

    // ── Clustered bring-up ───────────────────────────────────────────────
    pub async fn run_ledger_server(
        config: Config,
        handler: LedgerHandler,
        cancellation_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let addr = config.server.socket_addr()?;

        debug!("Ledger gRPC server listening on {}", addr);

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto::ledger::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        TonicServer::builder()
            .add_service(LedgerServer::new(handler))
            .add_service(reflection_service)
            .serve_with_shutdown(addr, async move {
                cancellation_token.cancelled().await;
            })
            .await?;

        debug!("Ledger gRPC server shut down cleanly");

        Ok(())
    }

    /// Run the `Fault` gRPC service on the **client port** (next to
    /// `Ledger`). Operational fault injection talks to nodes the same
    /// way clients do — the node port stays dedicated to consensus
    /// traffic.
    #[cfg(feature = "fault-injection")]
    pub async fn run_fault_server(
        config: Config,
        handler: FaultHandler,
        cancellation_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use proto::fault::fault_server::FaultServer;
        // Bind on (client_host, client_port + 1000) so a fault server
        // and the ledger server can coexist on the same node without
        // an extra port in the TOML schema. Keeps the per-node port
        // accounting stable — debug servers add `+1000`.
        //
        // `checked_add`, not `+`: under the test harness `client_port`
        // is an OS-assigned ephemeral port that can sit near the top of
        // the u16 range, so a raw `+ 1000` overflows and *panics* in
        // debug builds. This is optional debug infra nothing connects
        // to over the wire — on overflow we skip it (logged) rather
        // than let a panicked server thread abort the whole node.
        let mut addr = config.server.socket_addr()?;
        let Some(fault_port) = addr.port().checked_add(1000) else {
            debug!(
                "Fault gRPC server skipped: client_port {} + 1000 overflows u16",
                addr.port()
            );
            return Ok(());
        };
        addr.set_port(fault_port);

        debug!("Fault gRPC server listening on {}", addr);

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto::fault::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        TonicServer::builder()
            .add_service(FaultServer::new(handler))
            .add_service(reflection_service)
            .serve_with_shutdown(addr, async move {
                cancellation_token.cancelled().await;
            })
            .await?;

        debug!("Fault gRPC server shut down cleanly");

        Ok(())
    }

    /// Run the `LatencyProbe` gRPC service on `client_port + 2000` (the
    /// fault server uses `+1000`). Server-side, ambient pipeline-latency
    /// measurement; see `latency.proto`. Same non-load-bearing / overflow
    /// handling as the fault server.
    #[cfg(feature = "latency-probe")]
    pub async fn run_latency_probe_server(
        config: Config,
        handler: LatencyProbeHandler,
        cancellation_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use proto::latency::latency_probe_server::LatencyProbeServer;
        let mut addr = config.server.socket_addr()?;
        let Some(probe_port) = addr.port().checked_add(2000) else {
            debug!(
                "Latency gRPC server skipped: client_port {} + 2000 overflows u16",
                addr.port()
            );
            return Ok(());
        };
        addr.set_port(probe_port);

        debug!("Latency gRPC server listening on {}", addr);

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto::latency::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        TonicServer::builder()
            .add_service(LatencyProbeServer::new(handler))
            .add_service(reflection_service)
            .serve_with_shutdown(addr, async move {
                cancellation_token.cancelled().await;
            })
            .await?;

        debug!("Latency gRPC server shut down cleanly");

        Ok(())
    }

    pub async fn run_node_server(
        config: Config,
        handler: NodeHandler,
        cancellation_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cluster = config.cluster.as_ref().unwrap();
        let addr = cluster.node.socket_addr()?;

        debug!("Ledger gRPC server listening on {}", addr);

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto::ledger::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        TonicServer::builder()
            .add_service(NodeServer::new(handler))
            .add_service(reflection_service)
            .serve_with_shutdown(addr, async move {
                cancellation_token.cancelled().await;
            })
            .await?;

        debug!("Node gRPC server shut down cleanly");

        Ok(())
    }
}

impl Drop for ClusterNode {
    fn drop(&mut self) {
        self.cancellation_token.cancel();

        let handles = std::mem::take(&mut self.handles);
        for handle in handles {
            // Log, don't `unwrap()`: a worker thread that already
            // panicked makes `join()` return `Err`, and unwrapping it
            // here re-panics *during cleanup* — a non-unwinding panic
            // that SIGABRTs the process and masks the original error.
            // Reporting it keeps shutdown best-effort and the root
            // cause visible.
            if let Err(e) = handle.join() {
                error!(
                    "ClusterNode worker thread panicked during shutdown: {:?}",
                    e
                );
            }
        }
    }
}
