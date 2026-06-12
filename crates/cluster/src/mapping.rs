use ::proto::ledger as proto;
use ledger::transactor::transaction::CommittedTransaction;
use ledger::transactor::transaction::{Operation, TransactionStatus};
use storage::entities::{
    AccountFlagsUpdated, AccountLinked, AccountOpened, EntryKind, FunctionRegistered, TxEntry,
    TxLink, TxLinkKind, TxMetadata, TxTerm, WalEntry, encode_tag,
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

pub fn open_account_to_op(o: proto::OpenAccount) -> Operation {
    Operation::OpenAccount {
        count: o.count,
        user_ref: o.user_ref,
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
        Some(proto::submit_operation_request::Operation::OpenAccount(o)) => {
            Ok(open_account_to_op(o))
        }
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
        Some(proto::submit_and_wait_request::Operation::OpenAccount(o)) => {
            Ok(open_account_to_op(o))
        }
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

/// Convert a `WalEntry` into its proto record. Storage-side
/// `TxEntry` / `TxLink` no longer carry a `tx_id` field — it lives
/// only on the preceding `TxMetadata`. Callers stream-decode and pass
/// the running `tx_id` so the proto representation (which still has
/// the field on each message for client convenience) stays correct.
pub fn wal_entry_to_proto(e: WalEntry, tx_id: u64) -> proto::WalEntry {
    use proto::wal_entry::Entry as E;
    let entry = match e {
        WalEntry::Metadata(m) => E::Metadata(metadata_to_proto(m)),
        WalEntry::Entry(x) => E::TxEntry(entry_to_proto(x, tx_id)),
        WalEntry::Link(l) => E::Link(link_to_proto(l, tx_id)),
        WalEntry::Term(t) => E::Term(term_to_proto(t)),
        WalEntry::FunctionRegistered(f) => E::FunctionRegistered(function_registered_to_proto(f)),
        WalEntry::AccountOpened(a) => E::AccountOpened(account_opened_to_proto(a)),
        WalEntry::AccountLinked(a) => E::AccountLinked(account_linked_to_proto(a)),
        WalEntry::AccountFlagsUpdated(a) => {
            E::AccountFlagsUpdated(account_flags_updated_to_proto(a))
        }
    };
    proto::WalEntry { entry: Some(entry) }
}

/// Build a proto `Transaction`: commit `meta` leads, its followers become
/// `items`. `tx_id` lives only on the metadata, so thread it into each.
pub fn transaction_to_proto(result: CommittedTransaction) -> proto::Transaction {
    let tx_id = result.meta.tx_id;
    let items = result
        .entries
        .into_iter()
        .map(|e| wal_entry_to_proto(e, tx_id))
        .collect();
    proto::Transaction {
        meta: Some(wal_entry_to_proto(WalEntry::Metadata(result.meta), tx_id)),
        items,
    }
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

fn entry_to_proto(x: TxEntry, tx_id: u64) -> proto::WalTxEntry {
    let kind = match x.kind {
        EntryKind::Credit => proto::EntryKind::Credit,
        EntryKind::Debit => proto::EntryKind::Debit,
    };
    proto::WalTxEntry {
        tx_id,
        account_id: x.account_id,
        amount: x.amount,
        kind: kind as i32,
        computed_balance: x.computed_balance,
    }
}

fn link_to_proto(l: TxLink, tx_id: u64) -> proto::WalTxLink {
    let kind = match l.kind() {
        TxLinkKind::Duplicate => proto::LinkKind::Duplicate,
        TxLinkKind::Reversal => proto::LinkKind::Reversal,
    };
    proto::WalTxLink {
        tx_id,
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

fn account_opened_to_proto(a: AccountOpened) -> proto::WalAccountOpened {
    proto::WalAccountOpened {
        begin_account_id: a.begin_account_id,
        count: a.count,
        flags: a.flags,
        user_ref: a.user_ref,
    }
}

fn account_linked_to_proto(a: AccountLinked) -> proto::WalAccountLinked {
    proto::WalAccountLinked {
        parent_id: a.parent_id,
        type_id: a.type_id as u32,
        child_id: a.child_id,
    }
}

fn account_flags_updated_to_proto(a: AccountFlagsUpdated) -> proto::WalAccountFlagsUpdated {
    proto::WalAccountFlagsUpdated {
        account_id: a.account_id,
        prev_flags: a.prev_flags,
        new_flags: a.new_flags,
    }
}
