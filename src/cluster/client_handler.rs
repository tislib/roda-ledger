use crate::cluster::Term;
use crate::cluster::proto::ledger as proto;
use crate::cluster::proto::ledger::ledger_server::Ledger;
use crate::ledger::Ledger as InternalLedger;
use crate::snapshot::{QueryKind, QueryRequest, QueryResponse};
use crate::transaction::{Operation, WaitLevel};
use spdlog::warn;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::yield_now;
use tonic::{Request, Response, Status};

pub struct LedgerHandler {
    ledger: Arc<InternalLedger>,
    read_only: bool,
    /// Shared term state. In single-node mode the handler is constructed
    /// with a fresh in-memory-only term whose current value is 0.
    term: Arc<Term>,
}

impl LedgerHandler {
    /// Full read/write handler. Caller supplies the shared `Arc<Term>`
    /// owned by `Leader` / `Follower`.
    pub fn new(ledger: Arc<InternalLedger>, term: Arc<Term>) -> Self {
        Self {
            ledger,
            read_only: false,
            term,
        }
    }

    /// Read-only handler: all `submit_*` / `register_function` /
    /// `unregister_function` RPCs return `FAILED_PRECONDITION`.
    pub fn new_read_only(ledger: Arc<InternalLedger>, term: Arc<Term>) -> Self {
        Self {
            ledger,
            read_only: true,
            term,
        }
    }

    /// Convenience — current leader term from the shared `Arc<Term>`.
    #[inline]
    fn current_term(&self) -> u64 {
        self.term.get_current_term()
    }

    /// Resolve the term that covered `tx_id`. Hot-path ring read falls
    /// back to a disk scan through `TermStorage` when the ring has
    /// already rotated past `tx_id`.
    ///
    /// Returns `(term, term_start_tx_id)`. `(0, 0)` is the zero-value
    /// fallback for single-node mode or unknown tx ids, matching the
    /// proto's "no term context" meaning.
    fn term_for_tx(&self, tx_id: u64) -> (u64, u64) {
        match self.term.get_term_at_tx(tx_id) {
            Ok(Some(rec)) => (rec.term, rec.start_tx_id),
            Ok(None) => (0, 0),
            Err(e) => {
                warn!("term lookup for tx_id={} failed: {}", tx_id, e);
                (0, 0)
            }
        }
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

    /// Build a `GetStatusResponse` for `tx_id`, honouring the optional
    /// `expected_term` fence. Three outcomes:
    ///
    /// - `expected_term != 0` and the tx's actual term differs →
    ///   `status = TX_NOT_FOUND`, `term_mismatch = true`, `term` +
    ///   `term_start_tx_id` set to the *actual* covering term so the
    ///   caller can redirect.
    /// - Ledger reports the tx itself as unknown → `status = TX_NOT_FOUND`,
    ///   no term fence info (nothing meaningful to report).
    /// - Normal case → pipeline stage from `ledger.get_transaction_status`
    ///   plus the tx's term for observability.
    fn build_status_response(&self, tx_id: u64, expected_term: u64) -> proto::GetStatusResponse {
        // Check the term fence first so a wrong-term query can redirect
        // the caller even before we look at pipeline state.
        let (tx_term, tx_term_start) = self.term_for_tx(tx_id);
        if expected_term != 0 && tx_term != 0 && expected_term != tx_term {
            return proto::GetStatusResponse {
                status: proto::TransactionStatus::TxNotFound as i32,
                fail_reason: 0,
                term_mismatch: true,
                term: tx_term,
                term_start_tx_id: tx_term_start,
            };
        }

        let status = self.ledger.get_transaction_status(tx_id);
        let fail_reason = if status.is_err() {
            status.error_reason().as_u8() as u32
        } else {
            0
        };

        proto::GetStatusResponse {
            status: proto::TransactionStatus::from(status) as i32,
            fail_reason,
            term_mismatch: false,
            term: tx_term,
            term_start_tx_id: tx_term_start,
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
            term: self.current_term(),
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
            term: self.current_term(),
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
                term: self.current_term(),
            });
        }

        Ok(Response::new(proto::SubmitBatchResponse {
            results,
            term: self.current_term(),
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
                    term: self.current_term(),
                }
            })
            .collect();

        Ok(Response::new(proto::SubmitBatchAndWaitResponse {
            results,
            term: self.current_term(),
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
        Ok(Response::new(
            self.build_status_response(req.transaction_id, req.term),
        ))
    }

    async fn get_transaction_statuses(
        &self,
        request: Request<proto::GetStatusesRequest>,
    ) -> Result<Response<proto::GetStatusesResponse>, Status> {
        let req = request.into_inner();
        let mut results = Vec::with_capacity(req.transaction_ids.len());
        // The batched RPC has no per-entry term field — pass 0 so every
        // entry is treated as "no term fence requested".
        for transaction_id in req.transaction_ids {
            results.push(self.build_status_response(transaction_id, 0));
        }
        Ok(Response::new(proto::GetStatusesResponse { results }))
    }

    async fn wait_for_transaction(
        &self,
        request: Request<proto::WaitForTransactionRequest>,
    ) -> Result<Response<proto::WaitForTransactionResponse>, Status> {
        let req = request.into_inner();
        let tx_id = req.transaction_id;

        // Unknown tx — return NOT_FOUND immediately, never block. A
        // caller waiting on a tx that was never sequenced would never
        // make progress; the proto's `WaitOutcome::NotFound` is exactly
        // for this case.
        if matches!(
            self.ledger.get_transaction_status(tx_id),
            crate::transaction::TransactionStatus::NotFound
        ) {
            return Ok(Response::new(proto::WaitForTransactionResponse {
                outcome: proto::WaitOutcome::NotFound as i32,
                term: 0,
                term_start_tx_id: 0,
            }));
        }

        // Term fence (ADR-016 scaffolding): caller passes `term = 0` to
        // opt out. Otherwise resolve the term that actually covered
        // `tx_id` (hot ring → cold scan) and compare. A mismatch short-
        // circuits to TermMismatch with the tx's real term + its start
        // so the caller can replay against the correct branch.
        if req.term != 0 {
            let (tx_term, tx_term_start) = self.term_for_tx(tx_id);
            if tx_term != 0 && tx_term != req.term {
                return Ok(Response::new(proto::WaitForTransactionResponse {
                    outcome: proto::WaitOutcome::TermMismatch as i32,
                    term: tx_term,
                    term_start_tx_id: tx_term_start,
                }));
            }
        }

        let level = proto::WaitLevel::try_from(req.wait_level)
            .map(WaitLevel::from)
            .unwrap_or(WaitLevel::Committed);
        self.wait_for_transaction_level(tx_id, level).await;

        let (tx_term, tx_term_start) = self.term_for_tx(tx_id);
        Ok(Response::new(proto::WaitForTransactionResponse {
            outcome: proto::WaitOutcome::Reached as i32,
            term: tx_term,
            term_start_tx_id: tx_term_start,
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
            term: self.current_term(),
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
