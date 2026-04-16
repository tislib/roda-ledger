use crate::grpc::proto;
use crate::grpc::proto::ledger_server::Ledger;
use crate::ledger::Ledger as InternalLedger;
use crate::snapshot::{QueryKind, QueryRequest, QueryResponse};
use crate::transaction::{Operation, WaitLevel};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::yield_now;
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

    async fn submit_and_wait(
        &self,
        request: Request<proto::SubmitAndWaitRequest>,
    ) -> Result<Response<proto::SubmitAndWaitResponse>, Status> {
        let req = request.into_inner();
        let level = proto::WaitLevel::try_from(req.wait_level)
            .map(WaitLevel::from)
            .unwrap_or(WaitLevel::Committed);
        let op = req.try_into()?;

        let tx_id = self.ledger.submit(op);
        self.wait_for_transaction_level(tx_id, level).await;

        let status = self.ledger.get_transaction_status(tx_id);
        let fail_reason = if status.is_err() {
            status.error_reason().as_u8() as u32
        } else {
            0
        };

        Ok(Response::new(proto::SubmitAndWaitResponse {
            transaction_id: tx_id,
            fail_reason,
        }))
    }

    async fn submit_batch(
        &self,
        request: Request<proto::SubmitBatchRequest>,
    ) -> Result<Response<proto::SubmitBatchResponse>, Status> {
        let req = request.into_inner();
        let len = req.operations.len();
        let mut results = Vec::with_capacity(len);
        let mut operations: Vec<Operation> = Vec::with_capacity(len);

        for op_req in req.operations {
            let op = op_req.try_into()?;
            operations.push(op);
        }

        let start_transaction_id = self.ledger.submit_batch(operations);

        for i in 0..len {
            let tx_id = start_transaction_id + i as u64;
            results.push(proto::SubmitOperationResponse {
                transaction_id: tx_id,
            });
        }

        Ok(Response::new(proto::SubmitBatchResponse { results }))
    }

    async fn submit_batch_and_wait(
        &self,
        request: Request<proto::SubmitBatchAndWaitRequest>,
    ) -> Result<Response<proto::SubmitBatchAndWaitResponse>, Status> {
        let req = request.into_inner();
        let level = proto::WaitLevel::try_from(req.wait_level)
            .map(WaitLevel::from)
            .unwrap_or(WaitLevel::Committed);

        let len = req.operations.len();
        let mut operations: Vec<Operation> = Vec::with_capacity(len);

        for op_req in req.operations {
            let op = op_req.try_into()?;
            operations.push(op);
        }

        let start_transaction_id = self.ledger.submit_batch(operations);

        // tx_ids are monotonic — waiting for the last one guarantees all
        // earlier transactions have reached the same level (or were rejected)
        let last_tx_id = start_transaction_id + (len - 1) as u64;
        self.wait_for_transaction_level(last_tx_id, level).await;

        let results = (start_transaction_id..last_tx_id)
            .map(|tx_id| {
                let status = self.ledger.get_transaction_status(tx_id);
                let fail_reason = if status.is_err() {
                    status.error_reason().as_u8() as u32
                } else {
                    0
                };
                proto::SubmitAndWaitResponse {
                    transaction_id: tx_id,
                    fail_reason,
                }
            })
            .collect();

        Ok(Response::new(proto::SubmitBatchAndWaitResponse { results }))
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
            compute_index: self.ledger.last_compute_id(),
            commit_index: self.ledger.last_commit_id(),
            snapshot_index: self.ledger.last_snapshot_id(),
        }))
    }

    async fn get_transaction(
        &self,
        request: Request<proto::GetTransactionRequest>,
    ) -> Result<Response<proto::GetTransactionResponse>, Status> {
        let tx_id = request.into_inner().tx_id;
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        self.ledger.query(QueryRequest {
            kind: QueryKind::GetTransaction { tx_id },
            respond: Box::new(move |resp| {
                let _ = tx.send(resp);
            }),
        });

        match rx.recv() {
            Ok(QueryResponse::Transaction(Some(result))) => {
                let entry_records: Vec<proto::TxEntryRecord> = result
                    .entries
                    .iter()
                    .map(|e| proto::TxEntryRecord {
                        account_id: e.account_id,
                        amount: e.amount,
                        kind: e.kind as i32,
                        computed_balance: e.computed_balance,
                    })
                    .collect();
                let link_records: Vec<proto::TxLinkRecord> = result
                    .links
                    .iter()
                    .map(|l| proto::TxLinkRecord {
                        to_tx_id: l.to_tx_id,
                        kind: l.kind as i32,
                    })
                    .collect();
                Ok(Response::new(proto::GetTransactionResponse {
                    tx_id,
                    entries: entry_records,
                    links: link_records,
                }))
            }
            Ok(QueryResponse::Transaction(None)) => Err(Status::not_found("transaction not found")),
            _ => Err(Status::internal("query failed")),
        }
    }

    async fn get_account_history(
        &self,
        request: Request<proto::GetAccountHistoryRequest>,
    ) -> Result<Response<proto::GetAccountHistoryResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            20
        } else {
            req.limit.min(1000)
        } as usize;
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        self.ledger.query(QueryRequest {
            kind: QueryKind::GetAccountHistory {
                account_id: req.account_id,
                from_tx_id: req.from_tx_id,
                limit,
            },
            respond: Box::new(move |resp| {
                let _ = tx.send(resp);
            }),
        });

        match rx.recv() {
            Ok(QueryResponse::AccountHistory(entries)) => {
                let next_tx_id = entries.last().map_or(0, |e| {
                    if e.prev_link == 0 {
                        0
                    } else {
                        e.tx_id.saturating_sub(1)
                    }
                });
                let entry_records: Vec<proto::TxEntryRecord> = entries
                    .iter()
                    .map(|e| proto::TxEntryRecord {
                        account_id: e.account_id,
                        amount: e.amount,
                        kind: e.kind as i32,
                        computed_balance: e.computed_balance,
                    })
                    .collect();
                Ok(Response::new(proto::GetAccountHistoryResponse {
                    entries: entry_records,
                    next_tx_id,
                }))
            }
            _ => Err(Status::internal("query failed")),
        }
    }
}

impl LedgerHandler {
    pub async fn wait_for_transaction_level(&self, transaction_id: u64, level: WaitLevel) {
        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(20);

        loop {
            let reached = match level {
                WaitLevel::Computed => self.ledger.last_compute_id() >= transaction_id,
                WaitLevel::Committed => self.ledger.last_commit_id() >= transaction_id,
                WaitLevel::OnSnapshot => self.ledger.last_snapshot_id() >= transaction_id,
            };

            if reached {
                return;
            }

            yield_now().await;

            if start_time.elapsed() >= timeout {
                return;
            }
        }
    }
}
