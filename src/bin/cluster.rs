use roda_ledger::cluster::{ClusterServer, ClusterServerConfig, LeaderPeerShipper};
use roda_ledger::ledger::Ledger;
use spdlog::info;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path: PathBuf = env::args()
        .nth(1)
        .or_else(|| env::var("RODA_CLUSTER_CONFIG").ok())
        .unwrap_or_else(|| "cluster.toml".to_string())
        .into();

    let cluster_config = ClusterServerConfig::from_file(&config_path)
        .map_err(|e| {
            format!(
                "failed to load cluster config from {}: {}",
                config_path.display(),
                e
            )
        })?
        .finalize();

    let client_addr = cluster_config.server.socket_addr()?;
    let node_addr = cluster_config.cluster.socket_addr()?;

    info!(
        "Starting roda-ledger cluster node with config from {}: {:?}",
        config_path.display(),
        cluster_config
    );

    let mut ledger = Ledger::new(cluster_config.ledger);

    if !cluster_config.cluster.is_follower() && !cluster_config.cluster.peers.is_empty() {
        let shipper = LeaderPeerShipper::new(
            &cluster_config.cluster,
            tokio::runtime::Handle::current(),
            ledger.last_committed_tx_id_atomic(),
        )
        .map_err(|e| format!("peer shipper init failed: {}", e))?;
        info!(
            "leader peer shipper installed: {} peers, semaphore capacity {}",
            shipper.peer_count(),
            shipper.semaphore_capacity()
        );
        ledger.set_peer_shipper(Arc::new(shipper));
    }

    ledger.start()?;
    let ledger_arc = Arc::new(ledger);

    let server = ClusterServer::new(
        ledger_arc,
        client_addr,
        node_addr,
        Arc::new(cluster_config.cluster),
    );

    server.run().await?;

    Ok(())
}
