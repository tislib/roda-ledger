use ::proto::ledger as proto;
use ledger::transaction::{Operation, TransactionStatus};
use storage::entities::FailReason;

pub fn deposit_to_op(d: proto::Deposit) -> Operation {
    Operation::Deposit {
        account: d.account,
        amount: d.amount,
        user_ref: d.user_ref,
    }
}

pub fn withdrawal_to_op(w: proto::Withdrawal) -> Operation {
    Operation::Withdrawal {
        account: w.account,
        amount: w.amount,
        user_ref: w.user_ref,
    }
}

pub fn transfer_to_op(t: proto::Transfer) -> Operation {
    Operation::Transfer {
        from: t.from,
        to: t.to,
        amount: t.amount,
        user_ref: t.user_ref,
    }
}

pub fn function_to_op(n: proto::Function) -> Operation {
    let mut params = [0i64; 8];
    for (i, p) in n.params.into_iter().take(8).enumerate() {
        params[i] = p;
    }
    Operation::Function {
        name: n.name,
        params,
        user_ref: n.user_ref,
    }
}

#[allow(clippy::result_large_err)]
pub fn submit_request_to_op(
    req: proto::SubmitOperationRequest,
) -> Result<Operation, tonic::Status> {
    match req.operation {
        Some(proto::submit_operation_request::Operation::Deposit(d)) => Ok(deposit_to_op(d)),
        Some(proto::submit_operation_request::Operation::Withdrawal(w)) => Ok(withdrawal_to_op(w)),
        Some(proto::submit_operation_request::Operation::Transfer(t)) => Ok(transfer_to_op(t)),
        Some(proto::submit_operation_request::Operation::Function(n)) => Ok(function_to_op(n)),
        None => Err(tonic::Status::invalid_argument("missing operation")),
    }
}

#[allow(clippy::result_large_err)]
pub fn submit_and_wait_request_to_op(
    req: proto::SubmitAndWaitRequest,
) -> Result<Operation, tonic::Status> {
    match req.operation {
        Some(proto::submit_and_wait_request::Operation::Deposit(d)) => Ok(deposit_to_op(d)),
        Some(proto::submit_and_wait_request::Operation::Withdrawal(w)) => Ok(withdrawal_to_op(w)),
        Some(proto::submit_and_wait_request::Operation::Transfer(t)) => Ok(transfer_to_op(t)),
        Some(proto::submit_and_wait_request::Operation::Function(n)) => Ok(function_to_op(n)),
        None => Err(tonic::Status::invalid_argument("missing operation")),
    }
}

pub fn status_to_proto(status: TransactionStatus) -> proto::TransactionStatus {
    match status {
        TransactionStatus::NotFound => proto::TransactionStatus::TxNotFound,
        TransactionStatus::Pending => proto::TransactionStatus::Pending,
        TransactionStatus::Computed => proto::TransactionStatus::Computed,
        TransactionStatus::Committed => proto::TransactionStatus::Committed,
        TransactionStatus::OnSnapshot => proto::TransactionStatus::OnSnapshot,
        TransactionStatus::Error(_) => proto::TransactionStatus::Error,
    }
}

pub fn fail_reason_to_u32(reason: FailReason) -> u32 {
    reason.as_u8() as u32
}
