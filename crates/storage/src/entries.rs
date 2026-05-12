use crate::entities::{TxTerm, WalEntryKind};

pub fn wal_tx_term_entry(term: u64, node_id: u64, node_count: u16, node_voted: u16) -> TxTerm {
    TxTerm {
        entry_type: WalEntryKind::TxTerm as u8,
        _pad0: [0; 7],
        term,
        node_id,
        node_count,
        node_voted,
        _pad1: [0; 12],
    }
}
