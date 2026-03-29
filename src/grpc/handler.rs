use crate::grpc::proto;
use crate::grpc::proto::ledger_server::Ledger;
use crate::ledger::Ledger as InternalLedger;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct LedgerHandler {
    ledger: Arc<InternalLedger>,
}

impl LedgerHandler {
    pub fn new(ledger: Arc<InternalLedger>) -> Self {
        Self { ledger }
    }
}

#[tonic::async_trait]
impl Ledger for LedgerHandler {
    async fn submit_operation(
        &self,
        request: Request<proto::SubmitOperationRequest>,
    ) -> Result<Response<proto::SubmitOperationResponse>, Status> {
        let op = request.into_inner().try_into()?;
        let transaction_id = self.ledger.submit(op);
        Ok(Response::new(proto::SubmitOperationResponse {
            transaction_id,
        }))
    }

    async fn submit_batch(
        &self,
        request: Request<proto::SubmitBatchRequest>,
    ) -> Result<Response<proto::SubmitBatchResponse>, Status> {
        let req = request.into_inner();
        let mut results = Vec::with_capacity(req.operations.len());

        for op_req in req.operations {
            let op = op_req.try_into()?;
            let transaction_id = self.ledger.submit(op);
            results.push(proto::SubmitOperationResponse { transaction_id });
        }

        Ok(Response::new(proto::SubmitBatchResponse { results }))
    }

    async fn get_balance(
        &self,
        request: Request<proto::GetBalanceRequest>,
    ) -> Result<Response<proto::GetBalanceResponse>, Status> {
        let req = request.into_inner();
        let balance = self.ledger.get_balance(req.account_id);
        let last_snapshot_tx_id = self.ledger.last_snapshot_id();

        Ok(Response::new(proto::GetBalanceResponse {
            balance,
            last_snapshot_tx_id,
        }))
    }

    async fn get_balances(
        &self,
        request: Request<proto::GetBalancesRequest>,
    ) -> Result<Response<proto::GetBalancesResponse>, Status> {
        let req = request.into_inner();
        let mut balances = Vec::with_capacity(req.account_ids.len());

        for account_id in req.account_ids {
            balances.push(self.ledger.get_balance(account_id));
        }

        let last_snapshot_tx_id = self.ledger.last_snapshot_id();

        Ok(Response::new(proto::GetBalancesResponse {
            balances,
            last_snapshot_tx_id,
        }))
    }

    async fn get_transaction_status(
        &self,
        request: Request<proto::GetStatusRequest>,
    ) -> Result<Response<proto::GetStatusResponse>, Status> {
        let req = request.into_inner();
        let status = self.ledger.get_transaction_status(req.transaction_id);

        let fail_reason = if status.is_err() {
            status.error_reason().as_u8() as u32
        } else {
            0
        };

        Ok(Response::new(proto::GetStatusResponse {
            status: proto::TransactionStatus::from(status) as i32,
            fail_reason,
        }))
    }

    async fn get_transaction_statuses(
        &self,
        request: Request<proto::GetStatusesRequest>,
    ) -> Result<Response<proto::GetStatusesResponse>, Status> {
        let req = request.into_inner();
        let mut results = Vec::with_capacity(req.transaction_ids.len());

        for transaction_id in req.transaction_ids {
            let status = self.ledger.get_transaction_status(transaction_id);
            let fail_reason = if status.is_err() {
                status.error_reason().as_u8() as u32
            } else {
                0
            };

            results.push(proto::GetStatusResponse {
                status: proto::TransactionStatus::from(status) as i32,
                fail_reason,
            });
        }

        Ok(Response::new(proto::GetStatusesResponse { results }))
    }

    async fn get_pipeline_index(
        &self,
        _request: Request<proto::GetPipelineIndexRequest>,
    ) -> Result<Response<proto::GetPipelineIndexResponse>, Status> {
        Ok(Response::new(proto::GetPipelineIndexResponse {
            computed_index: self.ledger.last_computed_id(),
            committed_index: self.ledger.last_committed_id(),
            snapshot_index: self.ledger.last_snapshot_id(),
        }))
    }
}
