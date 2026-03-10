use roda_ledger::ledger::LedgerConfig;
use roda_ledger::protocol::*;
use roda_ledger::server::{Server, ServerConfig};
use roda_ledger::wallet::balance::WalletBalance;
use roda_ledger::wallet::transaction::WalletTransaction;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[tokio::test]
async fn test_batch_operation() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:8081".to_string();

    let server_config = ServerConfig {
        addr: addr.clone(),
        worker_threads: 1,
        ledger_config: LedgerConfig {
            in_memory: true,
            ..Default::default()
        },
    };

    let server = Server::<WalletTransaction, WalletBalance>::new(server_config);
    tokio::spawn(async move {
        let _ = server.run_async().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // We'll manually construct a batch to test the "aggressively read" part
    let mut stream = TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;

    let batch_size = 2;
    let tx1 = WalletTransaction::deposit(1001, 1000);
    let tx2 = WalletTransaction::deposit(1002, 2000);

    let mut batch_data = Vec::new();
    // BatchRequest
    batch_data.extend_from_slice(bytemuck::bytes_of(&BatchRequest { batch_size }));

    // Request 1
    let req1_header = ProtocolHeader {
        op_kind: OperationKind::REGISTER_TRANSACTION,
        _padding: [0; 3],
        length: std::mem::size_of::<RegisterTransactionRequest<WalletTransaction>>() as u32,
    };
    batch_data.extend_from_slice(bytemuck::bytes_of(&req1_header));
    batch_data.extend_from_slice(bytemuck::bytes_of(&RegisterTransactionRequest {
        data: tx1,
    }));

    // Request 2
    let req2_header = ProtocolHeader {
        op_kind: OperationKind::REGISTER_TRANSACTION,
        _padding: [0; 3],
        length: std::mem::size_of::<RegisterTransactionRequest<WalletTransaction>>() as u32,
    };
    batch_data.extend_from_slice(bytemuck::bytes_of(&req2_header));
    batch_data.extend_from_slice(bytemuck::bytes_of(&RegisterTransactionRequest {
        data: tx2,
    }));

    // Send BATCH header with the total length of the batch
    let batch_header = ProtocolHeader {
        op_kind: OperationKind::BATCH,
        _padding: [0; 3],
        length: batch_data.len() as u32,
    };

    let mut full_batch = Vec::new();
    full_batch.extend_from_slice(bytemuck::bytes_of(&batch_header));
    full_batch.extend_from_slice(&batch_data);
    stream.write_all(&full_batch).await?;

    // Now read the batched responses
    // 1. BatchResponse and all subsequent responses
    let mut header_buf = [0u8; std::mem::size_of::<ProtocolHeader>()];
    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut header_buf).await?;
    let resp_header: &ProtocolHeader = bytemuck::from_bytes(&header_buf);
    assert_eq!(resp_header.op_kind, OperationKind::BATCH);

    let mut payload = vec![0u8; resp_header.length as usize];
    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut payload).await?;

    let mut current_offset = 0;
    let mut batch_resp = BatchResponse { batch_size: 0 };
    bytemuck::bytes_of_mut(&mut batch_resp).copy_from_slice(
        &payload[current_offset..current_offset + std::mem::size_of::<BatchResponse>()],
    );
    current_offset += std::mem::size_of::<BatchResponse>();
    assert_eq!(batch_resp.batch_size, batch_size);

    // 2. Response 1
    let mut resp1_header = ProtocolHeader {
        op_kind: OperationKind(0),
        _padding: [0; 3],
        length: 0,
    };
    bytemuck::bytes_of_mut(&mut resp1_header).copy_from_slice(
        &payload[current_offset..current_offset + std::mem::size_of::<ProtocolHeader>()],
    );
    current_offset += std::mem::size_of::<ProtocolHeader>();
    assert_eq!(resp1_header.op_kind, OperationKind::REGISTER_TRANSACTION);

    let mut resp1 = RegisterTransactionResponse { transaction_id: 0 };
    bytemuck::bytes_of_mut(&mut resp1)
        .copy_from_slice(&payload[current_offset..current_offset + resp1_header.length as usize]);
    current_offset += resp1_header.length as usize;
    assert_eq!(resp1.transaction_id, 1);

    // 3. Response 2
    let mut resp2_header = ProtocolHeader {
        op_kind: OperationKind(0),
        _padding: [0; 3],
        length: 0,
    };
    bytemuck::bytes_of_mut(&mut resp2_header).copy_from_slice(
        &payload[current_offset..current_offset + std::mem::size_of::<ProtocolHeader>()],
    );
    current_offset += std::mem::size_of::<ProtocolHeader>();
    assert_eq!(resp2_header.op_kind, OperationKind::REGISTER_TRANSACTION);

    let mut resp2 = RegisterTransactionResponse { transaction_id: 0 };
    bytemuck::bytes_of_mut(&mut resp2)
        .copy_from_slice(&payload[current_offset..current_offset + resp2_header.length as usize]);
    assert_eq!(resp2.transaction_id, 2);

    Ok(())
}
