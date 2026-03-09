use crate::ledger::{Ledger, LedgerConfig};
use crate::transaction::{Transaction, TransactionDataType, TransactionStatus};
use crate::balance::BalanceDataType;
use crate::protocol::*;
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
            let (socket, _) = listener.accept().await?;
            socket.set_nodelay(true)?;
            let mut socket = socket;
            let ledger = self.ledger.clone();

            tokio::spawn(async move {
                let mut buffer = Vec::with_capacity(8192);
                let mut read_buf = [0u8; 4096];
                let mut batch_responses = Vec::new();
                let mut in_batch = 0u32;

                loop {
                    // 1. Ensure we have at least a header
                    while buffer.len() < std::mem::size_of::<ProtocolHeader>() {
                        match socket.read(&mut read_buf).await {
                            Ok(0) => return, // EOF
                            Ok(n) => buffer.extend_from_slice(&read_buf[..n]),
                            Err(e) => {
                                if e.kind() != std::io::ErrorKind::UnexpectedEof {
                                    eprintln!("Failed to read from socket: {}", e);
                                }
                                return;
                            }
                        }
                    }

                    // 2. Parse header
                    let header_size = std::mem::size_of::<ProtocolHeader>();
                    let header: ProtocolHeader = *bytemuck::from_bytes(&buffer[..header_size]);
                    let payload_size = header.length as usize;

                    // 3. Ensure we have the full payload
                    while buffer.len() < header_size + payload_size {
                        match socket.read(&mut read_buf).await {
                            Ok(0) => {
                                eprintln!("Unexpected EOF reading payload");
                                return;
                            }
                            Ok(n) => buffer.extend_from_slice(&read_buf[..n]),
                            Err(e) => {
                                eprintln!("Failed to read from socket: {}", e);
                                return;
                            }
                        }
                    }

                    // 4. Process frame
                    let payload = &buffer[header_size..header_size + payload_size];
                    let mut consumed_payload = payload_size;

                    match header.op_kind {
                        OperationKind::REGISTER_TRANSACTION => {
                            let request: &RegisterTransactionRequest<Data> = bytemuck::from_bytes(payload);
                            let tx_id = ledger.submit(Transaction::new(request.data));

                            let response = RegisterTransactionResponse { transaction_id: tx_id };
                            let resp_header = ProtocolHeader {
                                op_kind: OperationKind::REGISTER_TRANSACTION,
                                _padding: [0; 3],
                                length: std::mem::size_of::<RegisterTransactionResponse>() as u32,
                            };
                            
                            batch_responses.extend_from_slice(bytemuck::bytes_of(&resp_header));
                            batch_responses.extend_from_slice(bytemuck::bytes_of(&response));
                            
                            if in_batch > 0 {
                                in_batch -= 1;
                                if in_batch == 0 {
                                    // Update the BATCH header length to include all responses
                                    let header_size = std::mem::size_of::<ProtocolHeader>();
                                    if batch_responses.len() >= header_size {
                                        let total_payload_size = (batch_responses.len() - header_size) as u32;
                                        batch_responses[4..8].copy_from_slice(&total_payload_size.to_ne_bytes());
                                    }

                                    if let Err(e) = socket.write_all(&batch_responses).await {
                                        eprintln!("Failed to write batch responses: {}", e);
                                        break;
                                    }
                                    batch_responses.clear();
                                }
                            } else {
                                if let Err(e) = socket.write_all(&batch_responses).await {
                                    eprintln!("Failed to write RegisterTransactionResponse: {}", e);
                                    break;
                                }
                                batch_responses.clear();
                            }
                        }
                        OperationKind::GET_STATUS => {
                            let request: &GetStatusRequest = bytemuck::from_bytes(payload);
                            let status = ledger.get_transaction_status(request.transaction_id);

                            let status_u8 = match status {
                                TransactionStatus::Pending => 0,
                                TransactionStatus::Computed => 1,
                                TransactionStatus::Committed => 2,
                                TransactionStatus::OnSnapshot => 3,
                                TransactionStatus::Error(_) => 4,
                            };

                            let response = GetStatusResponse { status: status_u8 };
                            let resp_header = ProtocolHeader {
                                op_kind: OperationKind::GET_STATUS,
                                _padding: [0; 3],
                                length: std::mem::size_of::<GetStatusResponse>() as u32,
                            };
                            
                            batch_responses.extend_from_slice(bytemuck::bytes_of(&resp_header));
                            batch_responses.extend_from_slice(bytemuck::bytes_of(&response));
                            
                            if in_batch > 0 {
                                in_batch -= 1;
                                if in_batch == 0 {
                                    // Update the BATCH header length to include all responses
                                    let header_size = std::mem::size_of::<ProtocolHeader>();
                                    if batch_responses.len() >= header_size {
                                        let total_payload_size = (batch_responses.len() - header_size) as u32;
                                        batch_responses[4..8].copy_from_slice(&total_payload_size.to_ne_bytes());
                                    }

                                    if let Err(e) = socket.write_all(&batch_responses).await {
                                        eprintln!("Failed to write batch responses: {}", e);
                                        break;
                                    }
                                    batch_responses.clear();
                                }
                            } else {
                                if let Err(e) = socket.write_all(&batch_responses).await {
                                    eprintln!("Failed to write GetStatusResponse: {}", e);
                                    break;
                                }
                                batch_responses.clear();
                            }
                        }
                        OperationKind::GET_BALANCE => {
                            let request: &GetBalanceRequest = bytemuck::from_bytes(payload);
                            let balance = ledger.get_balance(request.account_id);

                            let response = GetBalanceResponse { balance };
                            let resp_header = ProtocolHeader {
                                op_kind: OperationKind::GET_BALANCE,
                                _padding: [0; 3],
                                length: std::mem::size_of::<GetBalanceResponse<BalanceData>>() as u32,
                            };
                            
                            batch_responses.extend_from_slice(bytemuck::bytes_of(&resp_header));
                            batch_responses.extend_from_slice(bytemuck::bytes_of(&response));
                            
                            if in_batch > 0 {
                                in_batch -= 1;
                                if in_batch == 0 {
                                    // Update the BATCH header length to include all responses
                                    let header_size = std::mem::size_of::<ProtocolHeader>();
                                    if batch_responses.len() >= header_size {
                                        let total_payload_size = (batch_responses.len() - header_size) as u32;
                                        batch_responses[4..8].copy_from_slice(&total_payload_size.to_ne_bytes());
                                    }

                                    if let Err(e) = socket.write_all(&batch_responses).await {
                                        eprintln!("Failed to write batch responses: {}", e);
                                        break;
                                    }
                                    batch_responses.clear();
                                }
                            } else {
                                if let Err(e) = socket.write_all(&batch_responses).await {
                                    eprintln!("Failed to write GetBalanceResponse: {}", e);
                                    break;
                                }
                                batch_responses.clear();
                            }
                        }
                        OperationKind::BATCH => {
                            let request: &BatchRequest = bytemuck::from_bytes(&payload[..std::mem::size_of::<BatchRequest>()]);
                            in_batch = request.batch_size;

                            let response = BatchResponse { batch_size: in_batch };
                            let resp_header = ProtocolHeader {
                                op_kind: OperationKind::BATCH,
                                _padding: [0; 3],
                                length: std::mem::size_of::<BatchResponse>() as u32,
                            };
                            
                            batch_responses.extend_from_slice(bytemuck::bytes_of(&resp_header));
                            batch_responses.extend_from_slice(bytemuck::bytes_of(&response));
                            
                            if in_batch == 0 {
                                if let Err(e) = socket.write_all(&batch_responses).await {
                                    eprintln!("Failed to write BatchResponse: {}", e);
                                    break;
                                }
                                batch_responses.clear();
                            }
                            consumed_payload = std::mem::size_of::<BatchRequest>();
                        }
                        _ => {
                            eprintln!("Unknown operation kind: {:?}", header.op_kind);
                            break;
                        }
                    }

                    // 5. Remove processed frame from buffer
                    buffer.drain(..header_size + consumed_payload);
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
