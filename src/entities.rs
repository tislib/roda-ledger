use bytemuck::{Pod, Zeroable};

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WalEntryKind {
    TxMetadata = 0,
    TxEntry = 1,
    SegmentHeader = 2,
    SegmentSealed = 3,
    Link = 4,
    FunctionRegistered = 5,
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
    pub const DUPLICATE: Self = Self(7);
    // 8–127 reserved for future standard reasons
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
    pub entry_count: u8,         // 1 @ 1  — number of TxEntry records following
    pub link_count: u8,          // 1 @ 2  — number of TxLink records after entries
    pub fail_reason: FailReason, // 1 @ 3
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
    pub entry_type: u8,  // 1 @ 0  — WalEntryKind::SegmentHeader
    pub version: u8,     // 1 @ 1  — WAL_VERSION
    pub _pad0: [u8; 2],  // 2 @ 2
    pub magic: u32,      // 4 @ 4  — WAL_MAGIC
    pub segment_id: u32, // 4 @ 8  — monotonic, matches filename suffix
    pub _pad1: [u8; 4],  // 4 @ 12 — align first_tx_id to 16
    pub _pad2: [u8; 24], // 16 @ 24 — total: 40 bytes
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

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TxLinkKind {
    Duplicate = 0,
    Reversal = 1,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct TxLink {
    pub entry_type: u8,  // 1 @ 0  — WalEntryKind::Link (4)
    pub link_kind: u8,   // 1 @ 1  — TxLinkKind as u8
    pub _pad: [u8; 6],   // 6 @ 2
    pub tx_id: u64,      // 8 @ 8  — links back to TxMetadata
    pub to_tx_id: u64,   // 8 @ 16  — referenced transaction
    pub _pad2: [u8; 16], // 24 @ 16
} // total: 40 bytes

impl TxLink {
    pub fn kind(&self) -> TxLinkKind {
        match self.link_kind {
            0 => TxLinkKind::Duplicate,
            1 => TxLinkKind::Reversal,
            _ => TxLinkKind::Duplicate, // fallback
        }
    }
}

/// WASM function-registry WAL record. Written directly to the active
/// segment by the Ledger (not by the Transactor) whenever a function is
/// registered or unregistered. Exactly 40 bytes so it packs into the
/// same fixed-width record stream as every other `WalEntry`.
///
/// - `crc32c == 0` means the function is unregistered (dual-signal with
///   the 0-byte file on disk; WAL is source of truth on conflict).
/// - `name` is ASCII snake_case, null-padded. Max 32 bytes.
/// - `version` is a per-name monotonic counter starting at 1.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct FunctionRegistered {
    pub entry_type: u8, // 1 @ 0  — WalEntryKind::FunctionRegistered (5)
    pub _pad0: u8,      // 1 @ 1
    pub version: u16,   // 2 @ 2  — per-name monotonic, 1..=65535
    pub crc32c: u32,    // 4 @ 4  — CRC32C of the WASM binary; 0 = unregistered
    pub name: [u8; 32], // 32 @ 8 — null-padded UTF-8
} // total: 40 bytes

impl FunctionRegistered {
    /// Build a record from logical fields. Panics in debug if `name` is
    /// longer than 32 bytes — the Ledger runs `validate_name` (≤ 32 bytes)
    /// before ever reaching this constructor.
    pub fn new(name: &str, version: u16, crc32c: u32) -> Self {
        let mut name_buf = [0u8; 32];
        let bytes = name.as_bytes();
        debug_assert!(bytes.len() <= 32, "function name exceeds 32 bytes");
        let copy_len = bytes.len().min(32);
        name_buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
        Self {
            entry_type: WalEntryKind::FunctionRegistered as u8,
            _pad0: 0,
            version,
            crc32c,
            name: name_buf,
        }
    }

    /// Borrow the snake-case name with trailing nulls trimmed.
    pub fn name_str(&self) -> &str {
        let end = self.name.iter().position(|b| *b == 0).unwrap_or(32);
        std::str::from_utf8(&self.name[..end]).unwrap_or("")
    }

    /// True iff this record represents an *unregister* action.
    #[inline]
    pub fn is_unregister(&self) -> bool {
        self.crc32c == 0
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WalEntry {
    Metadata(TxMetadata),
    Entry(TxEntry),
    SegmentHeader(SegmentHeader),
    SegmentSealed(SegmentSealed),
    Link(TxLink),
    FunctionRegistered(FunctionRegistered),
}

impl WalEntry {
    pub fn kind(&self) -> WalEntryKind {
        match self {
            WalEntry::Metadata(_) => WalEntryKind::TxMetadata,
            WalEntry::Entry(_) => WalEntryKind::TxEntry,
            WalEntry::SegmentHeader(_) => WalEntryKind::SegmentHeader,
            WalEntry::SegmentSealed(_) => WalEntryKind::SegmentSealed,
            WalEntry::Link(_) => WalEntryKind::Link,
            WalEntry::FunctionRegistered(_) => WalEntryKind::FunctionRegistered,
        }
    }

    pub fn tx_id(&self) -> u64 {
        match self {
            WalEntry::Metadata(m) => m.tx_id,
            WalEntry::Entry(e) => e.tx_id,
            WalEntry::Link(e) => e.tx_id,
            WalEntry::SegmentHeader(_) => 0,
            WalEntry::SegmentSealed(_) => 0,
            // FunctionRegistered carries no tx_id; it is not a financial
            // transaction and the Pipeline's commit/snapshot indexes never
            // advance from it.
            WalEntry::FunctionRegistered(_) => 0,
        }
    }
}

/// Unit of work flowing into the WAL stage's inbound queue.
/// `Multi` is produced by the Cluster follower path (ADR-015).
#[derive(Debug)]
pub enum WalInput {
    Single(WalEntry),
    Multi(Vec<WalEntry>),
}

impl WalInput {
    pub fn is_single(&self) -> bool {
        matches!(self, WalInput::Single(_))
    }

    pub fn is_multi(&self) -> bool {
        matches!(self, WalInput::Multi(_))
    }

    pub fn single(self) -> WalEntry {
        match self {
            WalInput::Single(e) => e,
            _ => unreachable!(),
        }
    }

    pub fn multi(self) -> Vec<WalEntry> {
        match self {
            WalInput::Multi(v) => v,
            _ => unreachable!(),
        }
    }
}

// Safety checks for bytemuck
unsafe impl Pod for WalEntryKind {}
unsafe impl Zeroable for WalEntryKind {}
unsafe impl Pod for EntryKind {}
unsafe impl Zeroable for EntryKind {}
unsafe impl Pod for TxLinkKind {}
unsafe impl Zeroable for TxLinkKind {}

// Hard compile-time guarantees: every WAL entry must be exactly 40 bytes.
const _: () = assert!(std::mem::size_of::<TxMetadata>() == 40);
const _: () = assert!(std::mem::size_of::<TxEntry>() == 40);
const _: () = assert!(std::mem::size_of::<SegmentHeader>() == 40);
const _: () = assert!(std::mem::size_of::<SegmentSealed>() == 40);
const _: () = assert!(std::mem::size_of::<TxLink>() == 40);
const _: () = assert!(std::mem::size_of::<FunctionRegistered>() == 40);
