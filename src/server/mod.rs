pub mod protocol;

use crate::ledger::{Ledger, LedgerConfig};
use crate::transaction::{Transaction, TransactionDataType, TransactionStatus};
use crate::balance::BalanceDataType;
use crate::server::protocol::*;
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

pub struct Server<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    config: ServerConfig,
    ledger: Arc<Ledger<Data, BalanceData>>,
}

impl<Data, BalanceData> Server<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
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
            let ledger = self.ledger.clone();

            tokio::spawn(async move {
                let max_req_size = std::mem::size_of::<RegisterTransactionRequest<Data>>()
                    .max(std::mem::size_of::<GetStatusRequest>())
                    .max(std::mem::size_of::<GetBalanceRequest>());
                let mut buf = vec![0u8; max_req_size];

                loop {
                    let mut header_buf = [0u8; std::mem::size_of::<ProtocolHeader>()];
                    if let Err(e) = socket.read_exact(&mut header_buf).await {
                        if e.kind() != std::io::ErrorKind::UnexpectedEof {
                            eprintln!("Failed to read header from socket: {}", e);
                        }
                        break;
                    }

                    let header: &ProtocolHeader = bytemuck::from_bytes(&header_buf);
                    match header.op_kind {
                        OperationKind::REGISTER_TRANSACTION => {
                            let size = std::mem::size_of::<RegisterTransactionRequest<Data>>();
                            if let Err(e) = socket.read_exact(&mut buf[..size]).await {
                                eprintln!("Failed to read RegisterTransactionRequest: {}", e);
                                break;
                            }
                            let request: &RegisterTransactionRequest<Data> = bytemuck::from_bytes(&buf[..size]);
                            let tx_id = ledger.submit(Transaction::new(request.data));

                            let response = RegisterTransactionResponse { transaction_id: tx_id };
                            if let Err(e) = socket.write_all(bytemuck::bytes_of(&response)).await {
                                eprintln!("Failed to write RegisterTransactionResponse: {}", e);
                                break;
                            }
                        }
                        OperationKind::GET_STATUS => {
                            let size = std::mem::size_of::<GetStatusRequest>();
                            if let Err(e) = socket.read_exact(&mut buf[..size]).await {
                                eprintln!("Failed to read GetStatusRequest: {}", e);
                                break;
                            }
                            let request: &GetStatusRequest = bytemuck::from_bytes(&buf[..size]);
                            let status = ledger.get_transaction_status(request.transaction_id);

                            let status_u8 = match status {
                                TransactionStatus::Pending => 0,
                                TransactionStatus::Computed => 1,
                                TransactionStatus::Committed => 2,
                                TransactionStatus::OnSnapshot => 3,
                                TransactionStatus::Error(_) => 4,
                            };

                            let response = GetStatusResponse { status: status_u8 };
                            if let Err(e) = socket.write_all(bytemuck::bytes_of(&response)).await {
                                eprintln!("Failed to write GetStatusResponse: {}", e);
                                break;
                            }
                        }
                        OperationKind::GET_BALANCE => {
                            let size = std::mem::size_of::<GetBalanceRequest>();
                            if let Err(e) = socket.read_exact(&mut buf[..size]).await {
                                eprintln!("Failed to read GetBalanceRequest: {}", e);
                                break;
                            }
                            let request: &GetBalanceRequest = bytemuck::from_bytes(&buf[..size]);
                            let balance = ledger.get_balance(request.account_id);

                            let response = GetBalanceResponse { balance };
                            let response_bytes = bytemuck::bytes_of(&response);
                            if let Err(e) = socket.write_all(response_bytes).await {
                                eprintln!("Failed to write GetBalanceResponse: {}", e);
                                break;
                            }
                        }
                        _ => {
                            eprintln!("Unknown operation kind: {:?}", header.op_kind);
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
