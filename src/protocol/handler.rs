use crate::balance::BalanceDataType;
use crate::ledger::Ledger;
use crate::protocol::*;
use crate::transaction::{Transaction, TransactionDataType, TransactionStatus};

pub struct ProtocolHandler {
    buffer: Vec<u8>,
    in_batch: u32,
    batch_responses: Vec<u8>,
}

impl ProtocolHandler {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(8192),
            in_batch: 0,
            batch_responses: Vec::new(),
        }
    }

    pub fn handle_bytes<Data, BalanceData>(
        &mut self,
        data: &[u8],
        ledger: &Ledger<Data, BalanceData>,
    ) -> Vec<u8>
    where
        BalanceData: BalanceDataType,
        Data: TransactionDataType<BalanceData = BalanceData>,
    {
        self.buffer.extend_from_slice(data);
        let mut all_responses = Vec::new();

        while self.buffer.len() >= std::mem::size_of::<ProtocolHeader>() {
            let header_size = std::mem::size_of::<ProtocolHeader>();
            let header: ProtocolHeader = *bytemuck::from_bytes(&self.buffer[..header_size]);
            let payload_size = header.length as usize;

            if self.buffer.len() < header_size + payload_size {
                break;
            }

            let frame_total_size = header_size + payload_size;
            let (response, consumed_payload) = {
                let payload = self.buffer[header_size..frame_total_size].to_vec();
                self.process_frame(header, &payload, ledger)
            };

            if let Some(mut resp) = response {
                all_responses.append(&mut resp);
            }

            self.buffer.drain(..header_size + consumed_payload);
        }

        all_responses
    }

    fn process_frame<Data, BalanceData>(
        &mut self,
        header: ProtocolHeader,
        payload: &[u8],
        ledger: &Ledger<Data, BalanceData>,
    ) -> (Option<Vec<u8>>, usize)
    where
        BalanceData: BalanceDataType,
        Data: TransactionDataType<BalanceData = BalanceData>,
    {
        match header.op_kind {
            OperationKind::REGISTER_TRANSACTION => {
                let req_size = std::mem::size_of::<RegisterTransactionRequest<Data>>();
                let request: &RegisterTransactionRequest<Data> = bytemuck::from_bytes(&payload[..req_size]);
                let tx_id = ledger.submit(Transaction::new(request.data));

                let response = RegisterTransactionResponse { transaction_id: tx_id };
                let resp_header = ProtocolHeader {
                    op_kind: OperationKind::REGISTER_TRANSACTION,
                    _padding: [0; 3],
                    length: std::mem::size_of::<RegisterTransactionResponse>() as u32,
                };

                self.batch_responses.extend_from_slice(bytemuck::bytes_of(&resp_header));
                self.batch_responses.extend_from_slice(bytemuck::bytes_of(&response));
                (self.check_batch_completion(), header.length as usize)
            }
            OperationKind::GET_STATUS => {
                let req_size = std::mem::size_of::<GetStatusRequest>();
                let request: &GetStatusRequest = bytemuck::from_bytes(&payload[..req_size]);
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

                self.batch_responses.extend_from_slice(bytemuck::bytes_of(&resp_header));
                self.batch_responses.extend_from_slice(bytemuck::bytes_of(&response));
                (self.check_batch_completion(), header.length as usize)
            }
            OperationKind::GET_BALANCE => {
                let req_size = std::mem::size_of::<GetBalanceRequest>();
                let request: &GetBalanceRequest = bytemuck::from_bytes(&payload[..req_size]);
                let balance = ledger.get_balance(request.account_id);

                let response = GetBalanceResponse { balance };
                let resp_header = ProtocolHeader {
                    op_kind: OperationKind::GET_BALANCE,
                    _padding: [0; 3],
                    length: std::mem::size_of::<GetBalanceResponse<BalanceData>>() as u32,
                };

                self.batch_responses.extend_from_slice(bytemuck::bytes_of(&resp_header));
                self.batch_responses.extend_from_slice(bytemuck::bytes_of(&response));
                (self.check_batch_completion(), header.length as usize)
            }
            OperationKind::BATCH => {
                let req_size = std::mem::size_of::<BatchRequest>();
                let request: &BatchRequest = bytemuck::from_bytes(&payload[..req_size]);
                self.in_batch = request.batch_size;

                let response = BatchResponse { batch_size: self.in_batch };
                let resp_header = ProtocolHeader {
                    op_kind: OperationKind::BATCH,
                    _padding: [0; 3],
                    length: std::mem::size_of::<BatchResponse>() as u32,
                };

                self.batch_responses.extend_from_slice(bytemuck::bytes_of(&resp_header));
                self.batch_responses.extend_from_slice(bytemuck::bytes_of(&response));

                let resp = if self.in_batch == 0 {
                    Some(std::mem::take(&mut self.batch_responses))
                } else {
                    None
                };
                (resp, std::mem::size_of::<BatchRequest>())
            }
            _ => {
                eprintln!("Unknown operation kind: {:?}", header.op_kind);
                (None, header.length as usize)
            }
        }
    }

    fn check_batch_completion(&mut self) -> Option<Vec<u8>> {
        if self.in_batch > 0 {
            self.in_batch -= 1;
            if self.in_batch == 0 {
                let header_size = std::mem::size_of::<ProtocolHeader>();
                if self.batch_responses.len() >= header_size {
                    let total_payload_size = (self.batch_responses.len() - header_size) as u32;
                    self.batch_responses[4..8].copy_from_slice(&total_payload_size.to_ne_bytes());
                }
                return Some(std::mem::take(&mut self.batch_responses));
            }
            None
        } else {
            Some(std::mem::take(&mut self.batch_responses))
        }
    }
}
