use crate::cluster::proto::ledger as proto;
use crate::cluster::proto::ledger::ledger_server::Ledger;
use crate::ledger::Ledger as InternalLedger;
use crate::snapshot::{QueryKind, QueryRequest, QueryResponse};
use crate::transaction::{Operation, WaitLevel};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::yield_now;
use tonic::{Request, Response, Status};

pub struct LedgerHandler {
    ledger: Arc<InternalLedger>,
    read_only: bool,
    /// Current leader term, stamped onto every submit response. `0` in
    /// single-node mode where there is no cluster concept.
    term: u64,
}

impl LedgerHandler {
    pub fn new(ledger: Arc<InternalLedger>) -> Self {
        Self {
            ledger,
            read_only: false,
            term: 0,
        }
    }

    /// Read-only handler: all `submit_*` / `register_function` /
    /// `unregister_function` RPCs return `FAILED_PRECONDITION`.
    pub fn new_read_only(ledger: Arc<InternalLedger>) -> Self {
        Self {
            ledger,
            read_only: true,
            term: 0,
        }
    }

    /// Override the term returned in submit responses. Used by the cluster
    /// layer (`Leader` / `Follower`) to surface `ClusterConfig::term`.
    pub fn with_term(mut self, term: u64) -> Self {
        self.term = term;
        self
    }

    // `Status` is the canonical tonic error; every handler method in this
    // file already returns `Result<_, Status>`. Boxing here would force a
    // `.map_err` at every call site for no benefit.
    #[allow(clippy::result_large_err)]
    fn ensure_writable(&self) -> Result<(), Status> {
        if self.read_only {
            Err(Status::failed_precondition(
                "node is a follower; writes are not accepted",
            ))
        } else {
            Ok(())
        }
    }
}

#[tonic::async_trait]
impl Ledger for LedgerHandler {
    async fn submit_operation(
        &self,
        request: Request<proto::SubmitOperationRequest>,
    ) -> Result<Response<proto::SubmitOperationResponse>, Status> {
        self.ensure_writable()?;
        let op = request.into_inner().try_into()?;
        let transaction_id = self.ledger.submit(op);
        Ok(Response::new(proto::SubmitOperationResponse {
            transaction_id,
            term: self.term,
        }))
    }

    async fn submit_and_wait(
        &self,
        request: Request<proto::SubmitAndWaitRequest>,
    ) -> Result<Response<proto::SubmitAndWaitResponse>, Status> {
        self.ensure_writable()?;
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
            term: self.term,
        }))
    }

    async fn submit_batch(
        &self,
        request: Request<proto::SubmitBatchRequest>,
    ) -> Result<Response<proto::SubmitBatchResponse>, Status> {
        self.ensure_writable()?;
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
                term: self.term,
            });
        }

        Ok(Response::new(proto::SubmitBatchResponse {
            results,
            term: self.term,
        }))
    }

    async fn submit_batch_and_wait(
        &self,
        request: Request<proto::SubmitBatchAndWaitRequest>,
    ) -> Result<Response<proto::SubmitBatchAndWaitResponse>, Status> {
        self.ensure_writable()?;
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

        let results = (start_transaction_id..=last_tx_id)
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
                    term: self.term,
                }
            })
            .collect();

        Ok(Response::new(proto::SubmitBatchAndWaitResponse {
            results,
            term: self.term,
        }))
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
            term_mismatch: false,
            term: self.term,
            term_start_tx_id: 0,
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
                term_mismatch: false,
                term: self.term,
                term_start_tx_id: 0,
            });
        }

        Ok(Response::new(proto::GetStatusesResponse { results }))
    }

    async fn wait_for_transaction(
        &self,
        request: Request<proto::WaitForTransactionRequest>,
    ) -> Result<Response<proto::WaitForTransactionResponse>, Status> {
        let req = request.into_inner();

        // Term fencing: caller passes `term = 0` to skip the check. If the
        // caller's expected term doesn't match ours, their transaction is
        // on a superseded branch (ADR-016 scaffolding).
        if req.term != 0 && req.term != self.term {
            return Ok(Response::new(proto::WaitForTransactionResponse {
                outcome: proto::WaitOutcome::TermMismatch as i32,
                term: self.term,
                term_start_tx_id: 0,
            }));
        }

        let level = proto::WaitLevel::try_from(req.wait_level)
            .map(WaitLevel::from)
            .unwrap_or(WaitLevel::Committed);
        self.wait_for_transaction_level(req.transaction_id, level)
            .await;

        Ok(Response::new(proto::WaitForTransactionResponse {
            outcome: proto::WaitOutcome::Reached as i32,
            term: self.term,
            term_start_tx_id: 0,
        }))
    }

    async fn get_pipeline_index(
        &self,
        _request: Request<proto::GetPipelineIndexRequest>,
    ) -> Result<Response<proto::GetPipelineIndexResponse>, Status> {
        Ok(Response::new(proto::GetPipelineIndexResponse {
            compute_index: self.ledger.last_compute_id(),
            commit_index: self.ledger.last_commit_id(),
            snapshot_index: self.ledger.last_snapshot_id(),
            term: self.term,
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

    // ------- WASM function registry ---------------------------------------

    async fn register_function(
        &self,
        request: Request<proto::RegisterFunctionRequest>,
    ) -> Result<Response<proto::RegisterFunctionResponse>, Status> {
        self.ensure_writable()?;
        let req = request.into_inner();
        match self
            .ledger
            .register_function(&req.name, &req.binary, req.override_existing)
        {
            Ok((version, crc32c)) => Ok(Response::new(proto::RegisterFunctionResponse {
                version: version as u32,
                crc32c,
            })),
            Err(e) => Err(map_registry_err(e)),
        }
    }

    async fn unregister_function(
        &self,
        request: Request<proto::UnregisterFunctionRequest>,
    ) -> Result<Response<proto::UnregisterFunctionResponse>, Status> {
        self.ensure_writable()?;
        let req = request.into_inner();
        match self.ledger.unregister_function(&req.name) {
            Ok(version) => Ok(Response::new(proto::UnregisterFunctionResponse {
                version: version as u32,
            })),
            Err(e) => Err(map_registry_err(e)),
        }
    }

    async fn list_functions(
        &self,
        _request: Request<proto::ListFunctionsRequest>,
    ) -> Result<Response<proto::ListFunctionsResponse>, Status> {
        let functions = self
            .ledger
            .list_functions()
            .into_iter()
            .map(|info| proto::FunctionInfo {
                name: info.name,
                version: info.version as u32,
                crc32c: info.crc32c,
            })
            .collect();
        Ok(Response::new(proto::ListFunctionsResponse { functions }))
    }
}

/// Map `register_function` / `unregister_function` [`std::io::Error`] kinds
/// to a canonical `tonic::Status`. Keeps the handler branches uniform.
fn map_registry_err(e: std::io::Error) -> Status {
    use std::io::ErrorKind;
    match e.kind() {
        ErrorKind::AlreadyExists => Status::already_exists(e.to_string()),
        ErrorKind::NotFound => Status::not_found(e.to_string()),
        ErrorKind::InvalidInput => Status::invalid_argument(e.to_string()),
        ErrorKind::InvalidData => Status::invalid_argument(e.to_string()),
        _ => Status::internal(e.to_string()),
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
