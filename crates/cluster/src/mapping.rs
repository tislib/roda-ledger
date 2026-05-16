use ::proto::ledger as proto;
use ledger::transaction::{Operation, TransactionStatus};
use storage::entities::{
    EntryKind, FunctionRegistered, TxEntry, TxLink, TxLinkKind, TxMetadata, TxTerm, WalEntry,
    encode_tag,
};

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

pub fn wal_entry_to_proto(e: WalEntry) -> proto::WalLogRecord {
    use proto::wal_log_record::Entry as E;
    let entry = match e {
        WalEntry::Metadata(m) => E::Metadata(metadata_to_proto(m)),
        WalEntry::Entry(x) => E::TxEntry(entry_to_proto(x)),
        WalEntry::Link(l) => E::Link(link_to_proto(l)),
        WalEntry::Term(t) => E::Term(term_to_proto(t)),
        WalEntry::FunctionRegistered(f) => E::FunctionRegistered(function_registered_to_proto(f)),
    };
    proto::WalLogRecord { entry: Some(entry) }
}

fn metadata_to_proto(m: TxMetadata) -> proto::WalTxMetadata {
    proto::WalTxMetadata {
        tx_id: m.tx_id,
        fail_reason: m.fail_reason.as_u8() as u32,
        sub_item_count: m.sub_item_count as u32,
        crc32c: m.crc32c,
        timestamp: m.timestamp,
        user_ref: m.user_ref,
        tag: encode_tag(&m.tag),
    }
}

fn entry_to_proto(x: TxEntry) -> proto::WalTxEntry {
    let kind = match x.kind {
        EntryKind::Credit => proto::EntryKind::Credit,
        EntryKind::Debit => proto::EntryKind::Debit,
    };
    proto::WalTxEntry {
        tx_id: x.tx_id,
        account_id: x.account_id,
        amount: x.amount,
        kind: kind as i32,
        computed_balance: x.computed_balance,
    }
}

fn link_to_proto(l: TxLink) -> proto::WalTxLink {
    let kind = match l.kind() {
        TxLinkKind::Duplicate => proto::LinkKind::Duplicate,
        TxLinkKind::Reversal => proto::LinkKind::Reversal,
    };
    proto::WalTxLink {
        tx_id: l.tx_id,
        to_tx_id: l.to_tx_id,
        kind: kind as i32,
    }
}

fn term_to_proto(t: TxTerm) -> proto::WalTxTerm {
    proto::WalTxTerm {
        term: t.term,
        node_id: t.node_id,
        node_count: t.node_count as u32,
        node_voted: t.node_voted as u32,
    }
}

fn function_registered_to_proto(f: FunctionRegistered) -> proto::WalFunctionRegistered {
    proto::WalFunctionRegistered {
        name: f.name_str().to_string(),
        version: f.version as u32,
        crc32c: f.crc32c,
    }
}
