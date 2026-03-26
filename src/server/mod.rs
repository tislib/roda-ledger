use crate::ledger::{Ledger, LedgerConfig};
use crate::protocol::*;
use crate::transaction::TransactionDataType;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::runtime::Builder;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub addr: String,
    pub worker_threads: usize,
    pub ledger_config: LedgerConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:8080".to_string(),
            worker_threads: 1,
            ledger_config: LedgerConfig::default(),
        }
    }
}

pub struct Server<Data: TransactionDataType> {
    config: ServerConfig,
    ledger: Arc<Ledger<Data>>,
}

impl<Data: TransactionDataType> Server<Data> {
    pub fn new(config: ServerConfig) -> Self {
        let mut ledger = Ledger::new(config.ledger_config.clone());
        ledger.start();
        Self {
            config,
            ledger: Arc::new(ledger),
        }
    }

    pub async fn run_async(self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.config.addr).await?;
        println!("Server listening on {}", self.config.addr);

        loop {
            let (mut socket, _) = listener.accept().await?;
            socket.set_nodelay(true)?;
            let ledger = self.ledger.clone();

            tokio::spawn(async move {
                let mut handler = ProtocolHandler::new();
                let mut buffer = [0u8; 4096];
                loop {
                    match socket.read(&mut buffer).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let responses = handler.handle_bytes(&buffer[..n], &ledger);
                            if !responses.is_empty()
                                && let Err(e) = socket.write_all(&responses).await
                            {
                                eprintln!("Failed to write to socket: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to read from socket: {}", e);
                            break;
                        }
                    }
                }
            });
        }
    }

    pub fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(self.config.worker_threads)
            .enable_all()
            .build()?;

        runtime.block_on(self.run_async())
    }
}
