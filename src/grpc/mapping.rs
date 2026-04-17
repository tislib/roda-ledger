use crate::entities::FailReason;
use crate::grpc::proto;
use crate::transaction::{
    CompositeOperation, CompositeOperationFlags, Operation, Step, TransactionStatus, WaitLevel,
};

impl From<proto::Deposit> for Operation {
    fn from(d: proto::Deposit) -> Self {
        Operation::Deposit {
            account: d.account,
            amount: d.amount,
            user_ref: d.user_ref,
        }
    }
}

impl From<proto::Withdrawal> for Operation {
    fn from(w: proto::Withdrawal) -> Self {
        Operation::Withdrawal {
            account: w.account,
            amount: w.amount,
            user_ref: w.user_ref,
        }
    }
}

impl From<proto::Transfer> for Operation {
    fn from(t: proto::Transfer) -> Self {
        Operation::Transfer {
            from: t.from,
            to: t.to,
            amount: t.amount,
            user_ref: t.user_ref,
        }
    }
}

impl From<proto::Composite> for Operation {
    fn from(c: proto::Composite) -> Self {
        let steps = c
            .steps
            .into_iter()
            .filter_map(|s| s.kind)
            .map(|k| match k {
                proto::step::Kind::Credit(c) => Step::Credit {
                    account_id: c.account_id,
                    amount: c.amount,
                },
                proto::step::Kind::Debit(d) => Step::Debit {
                    account_id: d.account_id,
                    amount: d.amount,
                },
            })
            .collect();

        Operation::Composite(Box::new(CompositeOperation {
            steps,
            flags: CompositeOperationFlags::from_bits_retain(c.flags),
            user_ref: c.user_ref,
        }))
    }
}

impl From<proto::Function> for Operation {
    fn from(n: proto::Function) -> Self {
        // `Operation::Function::params` is a fixed-arity `[i64; 8]`.
        // Proto3 has no fixed-length array type, so the wire form stays
        // `repeated int64` and we coerce here: slots beyond 8 are
        // dropped, missing slots are zero-padded.
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
}

impl TryFrom<proto::SubmitOperationRequest> for Operation {
    type Error = tonic::Status;

    fn try_from(req: proto::SubmitOperationRequest) -> Result<Self, Self::Error> {
        match req.operation {
            Some(proto::submit_operation_request::Operation::Deposit(d)) => Ok(d.into()),
            Some(proto::submit_operation_request::Operation::Withdrawal(w)) => Ok(w.into()),
            Some(proto::submit_operation_request::Operation::Transfer(t)) => Ok(t.into()),
            Some(proto::submit_operation_request::Operation::Composite(c)) => Ok(c.into()),
            Some(proto::submit_operation_request::Operation::Function(n)) => Ok(n.into()),
            None => Err(tonic::Status::invalid_argument("missing operation")),
        }
    }
}

impl TryFrom<proto::SubmitAndWaitRequest> for Operation {
    type Error = tonic::Status;

    fn try_from(req: proto::SubmitAndWaitRequest) -> Result<Self, Self::Error> {
        match req.operation {
            Some(proto::submit_and_wait_request::Operation::Deposit(d)) => Ok(d.into()),
            Some(proto::submit_and_wait_request::Operation::Withdrawal(w)) => Ok(w.into()),
            Some(proto::submit_and_wait_request::Operation::Transfer(t)) => Ok(t.into()),
            Some(proto::submit_and_wait_request::Operation::Composite(c)) => Ok(c.into()),
            Some(proto::submit_and_wait_request::Operation::Function(n)) => Ok(n.into()),
            None => Err(tonic::Status::invalid_argument("missing operation")),
        }
    }
}

impl From<TransactionStatus> for proto::TransactionStatus {
    fn from(status: TransactionStatus) -> Self {
        match status {
            TransactionStatus::Pending => proto::TransactionStatus::Pending,
            TransactionStatus::Computed => proto::TransactionStatus::Computed,
            TransactionStatus::Committed => proto::TransactionStatus::Committed,
            TransactionStatus::OnSnapshot => proto::TransactionStatus::OnSnapshot,
            TransactionStatus::Error(_) => proto::TransactionStatus::Error,
        }
    }
}

impl From<FailReason> for u32 {
    fn from(reason: FailReason) -> Self {
        reason.as_u8() as u32
    }
}

impl From<proto::WaitLevel> for WaitLevel {
    fn from(level: proto::WaitLevel) -> Self {
        match level {
            proto::WaitLevel::Computed => WaitLevel::Computed,
            proto::WaitLevel::Committed => WaitLevel::Committed,
            proto::WaitLevel::Snapshot => WaitLevel::OnSnapshot,
        }
    }
}
