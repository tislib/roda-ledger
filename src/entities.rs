use bytemuck::{Pod, Zeroable};

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WalEntryKind {
    TxMetadata = 0,
    TxEntry = 1,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EntryKind {
    Credit = 0,
    Debit = 1,
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct FailReason(u8);

impl FailReason {
    pub const NONE: Self = Self(0); // success
    pub const INSUFFICIENT_FUNDS: Self = Self(1);
    pub const ACCOUNT_NOT_FOUND: Self = Self(2);
    pub const ZERO_SUM_VIOLATION: Self = Self(3);
    pub const ENTRY_LIMIT_EXCEEDED: Self = Self(4);
    pub const INVALID_OPERATION: Self = Self(5);
    // 6–127 reserved for future standard reasons
    // 128–255 user-defined custom reasons

    pub fn is_success(&self) -> bool {
        self.0 == 0
    }
    pub fn is_failure(&self) -> bool {
        self.0 != 0
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct TxMetadata {
    pub tx_id: u64,                // 8
    pub timestamp: u64,            // 8
    pub user_ref: u64,             // 8 — opaque external reference, caller owns context
    pub entry_count: u8,           // 1 — 0 if transaction failed
    pub fail_reason: FailReason, // 1 — FailReason::NONE if succeeded
    pub _pad: [u8; 6],             // 6 — total: 32 bytes
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct TxEntry {
    pub tx_id: u64,        // 8 — links back to TxMetadata
    pub account_id: u64,   // 8
    pub amount: u64,       // 8 — integer minor units, no floats, no decimals
    pub kind: EntryKind, // 1 — Credit or Debit
    pub _pad: [u8; 7],     // 7 — total: 32 bytes
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WalEntry {
    Metadata(TxMetadata),
    Entry(TxEntry),
}

impl WalEntry {
    pub fn kind(&self) -> WalEntryKind {
        match self {
            WalEntry::Metadata(_) => WalEntryKind::TxMetadata,
            WalEntry::Entry(_) => WalEntryKind::TxEntry,
        }
    }

    pub fn tx_id(&self) -> u64 {
        match self {
            WalEntry::Metadata(m) => m.tx_id,
            WalEntry::Entry(e) => e.tx_id,
        }
    }
}

// Safety checks for bytemuck
unsafe impl Pod for WalEntryKind {}
unsafe impl Zeroable for WalEntryKind {}
unsafe impl Pod for EntryKind {}
unsafe impl Zeroable for EntryKind {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn test_sizes() {
        assert_eq!(size_of::<TxMetadata>(), 32);
        assert_eq!(size_of::<TxEntry>(), 32);
    }

    #[test]
    fn test_pod_tx_metadata() {
        let metadata = TxMetadata {
            tx_id: 1,
            timestamp: 2,
            user_ref: 3,
            entry_count: 4,
            fail_reason: FailReason::NONE,
            _pad: [0; 6],
        };
        let bytes = bytemuck::bytes_of(&metadata);
        assert_eq!(bytes.len(), 32);
        let metadata2: TxMetadata = *bytemuck::from_bytes(bytes);
        assert_eq!(metadata.tx_id, metadata2.tx_id);
    }

    #[test]
    fn test_pod_tx_entry() {
        let entry = TxEntry {
            tx_id: 1,
            account_id: 2,
            amount: 100,
            kind: EntryKind::Credit,
            _pad: [0; 7],
        };
        let bytes = bytemuck::bytes_of(&entry);
        assert_eq!(bytes.len(), 32);
        let entry2: TxEntry = *bytemuck::from_bytes(bytes);
        assert_eq!(entry.tx_id, entry2.tx_id);
        assert_eq!(entry2.kind, EntryKind::Credit);
    }

    #[test]
    fn test_entry_kind_values() {
        assert_eq!(EntryKind::Credit as u8, 0);
        assert_eq!(EntryKind::Debit as u8, 1);
    }

    #[test]
    fn test_wal_entry_kind_values() {
        assert_eq!(WalEntryKind::TxMetadata as u8, 0);
        assert_eq!(WalEntryKind::TxEntry as u8, 1);
    }

    #[test]
    fn test_wal_entry() {
        let metadata = TxMetadata {
            tx_id: 1,
            timestamp: 2,
            user_ref: 3,
            entry_count: 4,
            fail_reason: FailReason::NONE,
            _pad: [0; 6],
        };
        let wal_entry = WalEntry::Metadata(metadata);
        assert_eq!(wal_entry.kind(), WalEntryKind::TxMetadata);
        if let WalEntry::Metadata(m) = wal_entry {
            assert_eq!(m.tx_id, 1);
        } else {
            panic!("Expected Metadata");
        }

        let entry = TxEntry {
            tx_id: 1,
            account_id: 2,
            amount: 100,
            kind: EntryKind::Credit,
            _pad: [0; 7],
        };
        let wal_entry = WalEntry::Entry(entry);
        assert_eq!(wal_entry.kind(), WalEntryKind::TxEntry);
        if let WalEntry::Entry(e) = wal_entry {
            assert_eq!(e.amount, 100);
        } else {
            panic!("Expected Entry");
        }
    }
}
