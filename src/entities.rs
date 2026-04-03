use bytemuck::{Pod, Zeroable};

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WalEntryKind {
    TxMetadata = 0,
    TxEntry = 1,
    SegmentHeader = 2,
    SegmentSealed = 3,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EntryKind {
    Credit = 0,
    Debit = 1,
}

pub const SYSTEM_ACCOUNT_ID: u64 = 0;

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
    pub const ACCOUNT_LIMIT_EXCEEDED: Self = Self(6);
    // 7–127 reserved for future standard reasons
    // 128–255 user-defined custom reasons

    pub fn is_success(&self) -> bool {
        self.0 == 0
    }
    pub fn is_failure(&self) -> bool {
        self.0 != 0
    }
    pub fn as_u8(&self) -> u8 {
        self.0
    }
    pub fn from_u8(v: u8) -> Self {
        Self(v)
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct TxMetadata {
    pub entry_type: u8,          // 1 @ 0  — WalEntryKind::TxMetadata
    pub entry_count: u8,         // 1 @ 1  — 0 if transaction failed
    pub fail_reason: FailReason, // 1 @ 2  — FailReason::NONE if succeeded
    pub flags: u8,               // 1 @ 3
    pub crc32c: u32,             // 4 @ 4  — CRC32C; zero this field when computing
    pub tx_id: u64,              // 8 @ 8
    pub timestamp: u64,          // 8 @ 16
    pub user_ref: u64,           // 8 @ 24
    pub tag: [u8; 8],            // 8 @ 32
} // total: 40 bytes

/// First record in every WAL segment. 40 bytes.
/// entry_type is the first byte so the WAL format is [entry][entry][entry]…
/// with no separate type-byte prefix. Fields ordered to avoid implicit padding
/// (bytemuck::Pod requires zero implicit padding bytes).
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct SegmentHeader {
    pub entry_type: u8,   // 1 @ 0  — WalEntryKind::SegmentHeader
    pub version: u8,      // 1 @ 1  — WAL_VERSION
    pub _pad0: [u8; 2],   // 2 @ 2
    pub magic: u32,       // 4 @ 4  — WAL_MAGIC
    pub segment_id: u32,  // 4 @ 8  — monotonic, matches filename suffix
    pub _pad1: [u8; 4],   // 4 @ 12 — align first_tx_id to 16
    pub first_tx_id: u64, // 8 @ 16 — first tx in this segment
    pub _pad2: [u8; 16],  // 16 @ 24 — total: 40 bytes
}

/// Last record before a segment is sealed. 40 bytes.
/// entry_type at offset 0 makes the record self-describing; _pad0 absorbs the
/// u8→u32 alignment gap so bytemuck::Pod compiles without implicit padding.
/// file_crc32c is always the last field (offset 36) for easy streaming verification.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct SegmentSealed {
    pub entry_type: u8,    // 1 @ 0  — WalEntryKind::SegmentSealed
    pub _pad0: [u8; 3],    // 3 @ 1  — align segment_id to 4
    pub segment_id: u32,   // 4 @ 4  — matches SegmentHeader.segment_id
    pub last_tx_id: u64,   // 8 @ 8  — last committed tx
    pub record_count: u64, // 8 @ 16 — total records (including header + sealed)
    pub _pad1: [u8; 16],   // 12 @ 24 — pad to align file_crc32c to last 4 bytes
} // 40 bytes total

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct TxEntry {
    pub entry_type: u8,        // 1 @ 0  — WalEntryKind::TxEntry
    pub kind: EntryKind,       // 1 @ 1  — Credit or Debit
    pub _pad0: [u8; 6],        // 6 @ 2
    pub tx_id: u64,            // 8 @ 8  — links back to TxMetadata
    pub account_id: u64,       // 8 @ 16
    pub amount: u64,           // 8 @ 24 — integer minor units, no floats, no decimals
    pub computed_balance: i64, // 8 @ 32 — running balance after this entry, set by Transactor
} // total: 40 bytes

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WalEntry {
    Metadata(TxMetadata),
    Entry(TxEntry),
    SegmentHeader(SegmentHeader),
    SegmentSealed(SegmentSealed),
}

impl WalEntry {
    pub fn kind(&self) -> WalEntryKind {
        match self {
            WalEntry::Metadata(_) => WalEntryKind::TxMetadata,
            WalEntry::Entry(_) => WalEntryKind::TxEntry,
            WalEntry::SegmentHeader(_) => WalEntryKind::SegmentHeader,
            WalEntry::SegmentSealed(_) => WalEntryKind::SegmentSealed,
        }
    }

    pub fn tx_id(&self) -> u64 {
        match self {
            WalEntry::Metadata(m) => m.tx_id,
            WalEntry::Entry(e) => e.tx_id,
            WalEntry::SegmentHeader(_) => 0,
            WalEntry::SegmentSealed(_) => 0,
        }
    }
}

// Safety checks for bytemuck
unsafe impl Pod for WalEntryKind {}
unsafe impl Zeroable for WalEntryKind {}
unsafe impl Pod for EntryKind {}
unsafe impl Zeroable for EntryKind {}

// Hard compile-time guarantees: every WAL entry must be exactly 40 bytes.
const _: () = assert!(std::mem::size_of::<TxMetadata>() == 40);
const _: () = assert!(std::mem::size_of::<TxEntry>() == 40);
const _: () = assert!(std::mem::size_of::<SegmentHeader>() == 40);
const _: () = assert!(std::mem::size_of::<SegmentSealed>() == 40);
