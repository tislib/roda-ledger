use crate::config::Config;
use crate::consensus::state::Consensus;
#[cfg(feature = "fault-injection")]
use crate::fault::ClusterFaultInjector;
#[cfg(feature = "fault-injection")]
use crate::handlers::fault_handler::FaultHandler;
use crate::handlers::ledger_handler::LedgerHandler;
use crate::handlers::node_handler::NodeHandler;
use crate::ledger_slot::LedgerSlot;
use ledger::ledger::Ledger;
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

        let consensus = Arc::new(Consensus::new(config.clone(), ledger.clone())?);

        #[cfg(feature = "fault-injection")]
        let fault_injector = ClusterFaultInjector::new(ledger.clone());

        Ok(Self {
            config,
            ledger,
            consensus,
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

        Ok(self)
    }

    fn start_grpc_server(&mut self) -> Result<(), String> {
        let config = self.config.clone();
        let ledger = self.ledger.clone();
        let cancellation_token = self.cancellation_token.clone();
        let consensus = self.consensus.clone();
        let handle = thread::Builder::new()
            .name("ledger-grpc".into())
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("ledger-grpc")
                    .build()
                    .expect("build dedicated gRPC runtime");
                rt.block_on(async {
                    let ledger_handler = LedgerHandler::new(ledger.clone(), consensus.clone());
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
                    if let Err(e) = consensus.run_loop(cancellation_token).await {
                        error!("consensus loop exited: {}", e);
                        exit(1);
                    }
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
