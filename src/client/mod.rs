use crate::transaction::{TransactionDataType, TransactionStatus};
use crate::balance::BalanceDataType;
use crate::server::protocol::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use std::marker::PhantomData;

pub struct Client<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    addr: String,
    stream: Option<TcpStream>,
    buf: Vec<u8>,
    _phantom: PhantomData<(Data, BalanceData)>,
}

impl<Data, BalanceData> Client<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(addr: String) -> Self {
        let max_size = std::mem::size_of::<RegisterTransactionResponse>()
            .max(std::mem::size_of::<GetStatusResponse>())
            .max(std::mem::size_of::<GetBalanceResponse<BalanceData>>());
        Self {
            addr,
            stream: None,
            buf: vec![0u8; max_size],
            _phantom: PhantomData,
        }
    }

    pub async fn register_transaction(&mut self, data: Data) -> Result<u64, Box<dyn std::error::Error>> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr).await?;
            stream.set_nodelay(true)?;
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().unwrap();
        
        let header = ProtocolHeader { op_kind: OperationKind::REGISTER_TRANSACTION };
        stream.write_all(bytemuck::bytes_of(&header)).await?;
        
        let request = RegisterTransactionRequest { data };
        stream.write_all(bytemuck::bytes_of(&request)).await?;
        
        let size = std::mem::size_of::<RegisterTransactionResponse>();
        let buf = &mut self.buf[..size];
        self.stream.as_mut().unwrap().read_exact(buf).await?;
        let response: &RegisterTransactionResponse = bytemuck::from_bytes(&self.buf[..size]);
        
        Ok(response.transaction_id)
    }

    pub async fn get_status(&mut self, transaction_id: u64) -> Result<TransactionStatus, Box<dyn std::error::Error>> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr).await?;
            stream.set_nodelay(true)?;
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().unwrap();
        
        let header = ProtocolHeader { op_kind: OperationKind::GET_STATUS };
        stream.write_all(bytemuck::bytes_of(&header)).await?;
        
        let request = GetStatusRequest { transaction_id };
        stream.write_all(bytemuck::bytes_of(&request)).await?;
        
        let size = size_of::<GetStatusResponse>();
        let buf = &mut self.buf[..size];
        self.stream.as_mut().unwrap().read_exact(buf).await?;
        let response: &GetStatusResponse = bytemuck::from_bytes(&self.buf[..size]);
        
        let status = match response.status {
            0 => TransactionStatus::Pending,
            1 => TransactionStatus::Computed,
            2 => TransactionStatus::Committed,
            3 => TransactionStatus::OnSnapshot,
            4 => TransactionStatus::Error("Unknown error from server".to_string()),
            _ => return Err("Invalid status received from server".into()),
        };
        
        Ok(status)
    }

    pub async fn get_balance(&mut self, account_id: u64) -> Result<BalanceData, Box<dyn std::error::Error>> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr).await?;
            stream.set_nodelay(true)?;
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().unwrap();
        
        let header = ProtocolHeader { op_kind: OperationKind::GET_BALANCE };
        stream.write_all(bytemuck::bytes_of(&header)).await?;
        
        let request = GetBalanceRequest { account_id };
        stream.write_all(bytemuck::bytes_of(&request)).await?;
        
        let size = size_of::<GetBalanceResponse<BalanceData>>();
        let buf = &mut self.buf[..size];
        self.stream.as_mut().unwrap().read_exact(buf).await?;
        let response: &GetBalanceResponse<BalanceData> = bytemuck::from_bytes(&self.buf[..size]);
        
        Ok(response.balance)
    }
}
