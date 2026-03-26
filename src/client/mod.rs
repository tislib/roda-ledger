use crate::balance::Balance;
use crate::protocol::*;
use crate::transaction::{TransactionDataType, TransactionStatus};
use std::marker::PhantomData;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::entities::FailReason;

pub struct Client<Data: TransactionDataType> {
    addr: String,
    stream: Option<TcpStream>,
    buf: Vec<u8>,
    _phantom: PhantomData<Data>,
}

impl<Data: TransactionDataType> Client<Data> {
    pub fn new(addr: String) -> Self {
        let max_size = (std::mem::size_of::<ProtocolHeader>()
            + std::mem::size_of::<RegisterTransactionResponse>())
        .max(std::mem::size_of::<ProtocolHeader>() + std::mem::size_of::<GetStatusResponse>())
        .max(
            std::mem::size_of::<ProtocolHeader>()
                + std::mem::size_of::<GetBalanceResponse>(),
        )
        .max(std::mem::size_of::<ProtocolHeader>() + std::mem::size_of::<BatchResponse>());
        Self {
            addr,
            stream: None,
            buf: vec![0u8; max_size],
            _phantom: PhantomData,
        }
    }

    pub async fn register_transaction(
        &mut self,
        data: Data,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr).await?;
            stream.set_nodelay(true)?;
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().unwrap();

        let header = ProtocolHeader {
            op_kind: OperationKind::REGISTER_TRANSACTION,
            _padding: [0; 3],
            length: std::mem::size_of::<RegisterTransactionRequest<Data>>() as u32,
        };
        let request = RegisterTransactionRequest { data };

        self.buf.clear();
        self.buf.extend_from_slice(bytemuck::bytes_of(&header));
        self.buf.extend_from_slice(bytemuck::bytes_of(&request));
        stream.write_all(&self.buf).await?;

        let mut header_buf = [0u8; std::mem::size_of::<ProtocolHeader>()];
        stream.read_exact(&mut header_buf).await?;
        let resp_header: &ProtocolHeader = bytemuck::from_bytes(&header_buf);

        let payload_size = resp_header.length as usize;
        if self.buf.len() < payload_size {
            self.buf.resize(payload_size, 0);
        }
        let buf = &mut self.buf[..payload_size];
        stream.read_exact(buf).await?;
        let response: &RegisterTransactionResponse = bytemuck::from_bytes(buf);

        Ok(response.transaction_id)
    }

    pub async fn get_status(
        &mut self,
        transaction_id: u64,
    ) -> Result<TransactionStatus, Box<dyn std::error::Error>> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr).await?;
            stream.set_nodelay(true)?;
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().unwrap();

        let header = ProtocolHeader {
            op_kind: OperationKind::GET_STATUS,
            _padding: [0; 3],
            length: std::mem::size_of::<GetStatusRequest>() as u32,
        };
        let request = GetStatusRequest { transaction_id };

        self.buf.clear();
        self.buf.extend_from_slice(bytemuck::bytes_of(&header));
        self.buf.extend_from_slice(bytemuck::bytes_of(&request));
        stream.write_all(&self.buf).await?;

        let mut header_buf = [0u8; std::mem::size_of::<ProtocolHeader>()];
        stream.read_exact(&mut header_buf).await?;
        let resp_header: &ProtocolHeader = bytemuck::from_bytes(&header_buf);

        let payload_size = resp_header.length as usize;
        if self.buf.len() < payload_size {
            self.buf.resize(payload_size, 0);
        }
        let buf = &mut self.buf[..payload_size];
        stream.read_exact(buf).await?;
        let response: &GetStatusResponse = bytemuck::from_bytes(buf);

        let status = match response.status {
            0 => TransactionStatus::Pending,
            1 => TransactionStatus::Computed,
            2 => TransactionStatus::Committed,
            3 => TransactionStatus::OnSnapshot,
            4 => TransactionStatus::Error(FailReason::INVALID_OPERATION),
            _ => return Err("Invalid status received from server".into()),
        };

        Ok(status)
    }

    pub async fn get_balance(
        &mut self,
        account_id: u64,
    ) -> Result<Balance, Box<dyn std::error::Error>> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr).await?;
            stream.set_nodelay(true)?;
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().unwrap();

        let header = ProtocolHeader {
            op_kind: OperationKind::GET_BALANCE,
            _padding: [0; 3],
            length: std::mem::size_of::<GetBalanceRequest>() as u32,
        };
        let request = GetBalanceRequest { account_id };

        self.buf.clear();
        self.buf.extend_from_slice(bytemuck::bytes_of(&header));
        self.buf.extend_from_slice(bytemuck::bytes_of(&request));
        stream.write_all(&self.buf).await?;

        let mut header_buf = [0u8; std::mem::size_of::<ProtocolHeader>()];
        stream.read_exact(&mut header_buf).await?;
        let resp_header: &ProtocolHeader = bytemuck::from_bytes(&header_buf);

        let payload_size = resp_header.length as usize;
        if self.buf.len() < payload_size {
            self.buf.resize(payload_size, 0);
        }
        let buf = &mut self.buf[..payload_size];
        stream.read_exact(buf).await?;
        let response: &GetBalanceResponse = bytemuck::from_bytes(buf);

        Ok(response.balance)
    }

    pub async fn batch(&mut self, batch_size: u32) -> Result<(), Box<dyn std::error::Error>> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr).await?;
            stream.set_nodelay(true)?;
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().unwrap();

        let header = ProtocolHeader {
            op_kind: OperationKind::BATCH,
            _padding: [0; 3],
            length: std::mem::size_of::<BatchRequest>() as u32,
        };
        let request = BatchRequest { batch_size };

        self.buf.clear();
        self.buf.extend_from_slice(bytemuck::bytes_of(&header));
        self.buf.extend_from_slice(bytemuck::bytes_of(&request));
        stream.write_all(&self.buf).await?;

        let mut header_buf = [0u8; std::mem::size_of::<ProtocolHeader>()];
        stream.read_exact(&mut header_buf).await?;
        let resp_header: &ProtocolHeader = bytemuck::from_bytes(&header_buf);

        let payload_size = resp_header.length as usize;
        if self.buf.len() < payload_size {
            self.buf.resize(payload_size, 0);
        }
        let buf = &mut self.buf[..payload_size];
        stream.read_exact(buf).await?;
        // We don't really need to do anything with BatchResponse for now as it's just a hint

        Ok(())
    }

    pub async fn register_transactions_batch(
        &mut self,
        transactions: Vec<Data>,
    ) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr).await?;
            stream.set_nodelay(true)?;
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().unwrap();

        let batch_size = transactions.len() as u32;

        self.buf.clear();
        // 1. Send BATCH request
        let header = ProtocolHeader {
            op_kind: OperationKind::BATCH,
            _padding: [0; 3],
            length: std::mem::size_of::<BatchRequest>() as u32,
        };
        let request = BatchRequest { batch_size };
        self.buf.extend_from_slice(bytemuck::bytes_of(&header));
        self.buf.extend_from_slice(bytemuck::bytes_of(&request));

        // 2. Send all requests
        for data in transactions {
            let header = ProtocolHeader {
                op_kind: OperationKind::REGISTER_TRANSACTION,
                _padding: [0; 3],
                length: std::mem::size_of::<RegisterTransactionRequest<Data>>() as u32,
            };
            let request = RegisterTransactionRequest { data };
            self.buf.extend_from_slice(bytemuck::bytes_of(&header));
            self.buf.extend_from_slice(bytemuck::bytes_of(&request));
        }
        stream.write_all(&self.buf).await?;

        // 3. Read BatchResponse and all subsequent responses in one go
        let mut header_buf = [0u8; std::mem::size_of::<ProtocolHeader>()];
        stream.read_exact(&mut header_buf).await?;
        let resp_header: &ProtocolHeader = bytemuck::from_bytes(&header_buf);
        assert_eq!(resp_header.op_kind, OperationKind::BATCH);

        let total_payload_size = resp_header.length as usize;
        if self.buf.len() < total_payload_size {
            self.buf.resize(total_payload_size, 0);
        }
        stream
            .read_exact(&mut self.buf[..total_payload_size])
            .await?;

        // 4. Parse all responses from the buffer
        let mut transaction_ids = Vec::with_capacity(batch_size as usize);
        let mut current_offset = std::mem::size_of::<BatchResponse>();
        for _ in 0..batch_size {
            let header_size = std::mem::size_of::<ProtocolHeader>();
            let mut resp_header = ProtocolHeader {
                op_kind: OperationKind(0),
                _padding: [0; 3],
                length: 0,
            };
            bytemuck::bytes_of_mut(&mut resp_header)
                .copy_from_slice(&self.buf[current_offset..current_offset + header_size]);
            current_offset += header_size;
            assert_eq!(resp_header.op_kind, OperationKind::REGISTER_TRANSACTION);

            let payload_size = resp_header.length as usize;
            let mut response = RegisterTransactionResponse { transaction_id: 0 };
            bytemuck::bytes_of_mut(&mut response)
                .copy_from_slice(&self.buf[current_offset..current_offset + payload_size]);
            current_offset += payload_size;
            transaction_ids.push(response.transaction_id);
        }

        Ok(transaction_ids)
    }

    pub async fn get_balances_batch(
        &mut self,
        account_ids: Vec<u64>,
    ) -> Result<Vec<Balance>, Box<dyn std::error::Error>> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr).await?;
            stream.set_nodelay(true)?;
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().unwrap();

        let batch_size = account_ids.len() as u32;

        self.buf.clear();
        // 1. Send BATCH request
        let header = ProtocolHeader {
            op_kind: OperationKind::BATCH,
            _padding: [0; 3],
            length: std::mem::size_of::<BatchRequest>() as u32,
        };
        let request = BatchRequest { batch_size };
        self.buf.extend_from_slice(bytemuck::bytes_of(&header));
        self.buf.extend_from_slice(bytemuck::bytes_of(&request));

        // 2. Send all requests
        for account_id in account_ids {
            let header = ProtocolHeader {
                op_kind: OperationKind::GET_BALANCE,
                _padding: [0; 3],
                length: std::mem::size_of::<GetBalanceRequest>() as u32,
            };
            let request = GetBalanceRequest { account_id };
            self.buf.extend_from_slice(bytemuck::bytes_of(&header));
            self.buf.extend_from_slice(bytemuck::bytes_of(&request));
        }
        stream.write_all(&self.buf).await?;

        // 3. Read BatchResponse and all subsequent responses in one go
        let mut header_buf = [0u8; std::mem::size_of::<ProtocolHeader>()];
        stream.read_exact(&mut header_buf).await?;
        let resp_header: &ProtocolHeader = bytemuck::from_bytes(&header_buf);
        assert_eq!(resp_header.op_kind, OperationKind::BATCH);

        let total_payload_size = resp_header.length as usize;
        if self.buf.len() < total_payload_size {
            self.buf.resize(total_payload_size, 0);
        }
        stream
            .read_exact(&mut self.buf[..total_payload_size])
            .await?;

        // 4. Parse all responses from the buffer
        let mut balances = Vec::with_capacity(batch_size as usize);
        let mut current_offset = std::mem::size_of::<BatchResponse>();
        for _ in 0..batch_size {
            let header_size = std::mem::size_of::<ProtocolHeader>();
            let mut resp_header = ProtocolHeader {
                op_kind: OperationKind(0),
                _padding: [0; 3],
                length: 0,
            };
            bytemuck::bytes_of_mut(&mut resp_header)
                .copy_from_slice(&self.buf[current_offset..current_offset + header_size]);
            current_offset += header_size;
            assert_eq!(resp_header.op_kind, OperationKind::GET_BALANCE);

            let payload_size = resp_header.length as usize;
            let mut response = GetBalanceResponse::default();
            bytemuck::bytes_of_mut(&mut response)
                .copy_from_slice(&self.buf[current_offset..current_offset + payload_size]);
            current_offset += payload_size;
            balances.push(response.balance);
        }

        Ok(balances)
    }
}
