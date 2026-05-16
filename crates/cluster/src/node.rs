use crate::config::Config;
use crate::consensus::consensus::Consensus;
use crate::handlers::ledger_handler::LedgerHandler;
use crate::handlers::node_handler::NodeHandler;
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
    ledger: Arc<Ledger>,

    // shutdown handling
    cancellation_token: CancellationToken,
    handles: Vec<JoinHandle<()>>,
    pub consensus: Arc<Consensus>,
}

impl ClusterNode {
    pub fn new(config: Config) -> Result<Self, String> {
        let mut ledger = Ledger::new(config.ledger.clone());
        ledger.start().map_err(|e| format!("{}", e))?;
        let ledger = Arc::new(ledger);

        let consensus = Arc::new(Consensus::new(config.clone(), ledger.clone())?);

        Ok(Self {
            config,
            ledger,
            consensus,
            cancellation_token: Default::default(),
            handles: vec![],
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
        let ledger = self.ledger.clone();
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
                    let node_handler = NodeHandler::new(ledger, consensus);
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
            handle.join().unwrap();
        }
    }
}
