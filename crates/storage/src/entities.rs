use bytemuck::{Pod, Zeroable};

// Discriminants 2 and 3 are retired (formerly SegmentHeader / SegmentSealed);
// segment boundaries now come from filenames and the .seal / .crc sidecars.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WalEntryKind {
    TxMetadata = 0,
    TxEntry = 1,
    Link = 4,
    FunctionRegistered = 5,
    TxTerm = 6,
    AccountOpened = 7,
    AccountLinked = 8,
    AccountFlagsUpdated = 9,
    Kv = 10,
    KvConstant = 11,
}

pub struct EntryKind;
impl EntryKind {
    pub const CREDIT: u8 = 0;
    pub const DEBIT: u8 = 1;
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
    // 6 retired: ACCOUNT_LIMIT_EXCEEDED removed — u32-id-space exhaustion is
    // unreachable in practice (>50GB), so the allocator panics instead.
    pub const DUPLICATE: Self = Self(7);
    // WASM module called a host verb prohibited in the current phase (ADR-023 §6),
    // e.g. `kv_register_constant` from `execute`, or a data verb from `register`.
    pub const PROHIBITED_HOST_CALL: Self = Self(8);
    // `kv_get_constant` looked up a name that was never registered (ADR-023 §6);
    // execution is stopped so the module cannot proceed with a bogus id.
    pub const CONSTANT_NOT_FOUND: Self = Self(9);
    // 10–127 reserved for future standard reasons
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

/// Transaction commit record — written last, after its followers (trailer layout).
/// CRC32C covers the followers in push order, then this record with `crc32c` zeroed.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct TxMetadata {
    pub entry_type: u8,          // 1 @ 0  — WalEntryKind::TxMetadata
    pub fail_reason: FailReason, // 1 @ 1
    pub sub_item_count: u16, // 2 @ 2  — count of preceding TxEntry/TxLink/TxTerm/FunctionRegistered followers
    pub crc32c: u32,         // 4 @ 4  — CRC32C; zero this field when computing
    pub tx_id: u64,          // 8 @ 8
    pub timestamp: u64,      // 8 @ 16
    pub user_ref: u64,       // 8 @ 24
    pub tag: [u8; 8],        // 8 @ 32
} // total: 40 bytes

impl TxMetadata {
    /// Render `tag` as a human-readable string: trailing-null-trimmed UTF-8
    /// when valid, otherwise 16-char lowercase hex.
    pub fn tag_string(&self) -> String {
        encode_tag(&self.tag)
    }

    /// Parse `s` into an 8-byte tag: 16 hex digits decode as bytes; anything
    /// else is treated as UTF-8 truncated/null-padded to 8.
    pub fn parse_tag(s: &str) -> [u8; 8] {
        decode_tag(s)
    }
}

/// Free-function form of [`TxMetadata::tag_string`] for callers holding a
/// raw `[u8; 8]` (e.g. WAL inspection tools).
pub fn encode_tag(tag: &[u8; 8]) -> String {
    let trimmed = match tag.iter().rposition(|&b| b != 0) {
        Some(last) => &tag[..=last],
        None => &tag[..0],
    };
    match std::str::from_utf8(trimmed) {
        Ok(s) => s.to_string(),
        Err(_) => format!(
            "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            tag[0], tag[1], tag[2], tag[3], tag[4], tag[5], tag[6], tag[7]
        ),
    }
}

/// Free-function form of [`TxMetadata::parse_tag`].
pub fn decode_tag(s: &str) -> [u8; 8] {
    let mut tag = [0u8; 8];
    let is_hex16 = s.len() == 16 && s.chars().all(|c| c.is_ascii_hexdigit());
    if is_hex16 {
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            if i >= 8 {
                break;
            }
            if let Ok(cs) = std::str::from_utf8(chunk)
                && let Ok(b) = u8::from_str_radix(cs, 16)
            {
                tag[i] = b;
            }
        }
    } else {
        let bytes = s.as_bytes();
        let len = bytes.len().min(8);
        tag[..len].copy_from_slice(&bytes[..len]);
    }
    tag
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct TxEntry {
    pub entry_type: u8,        // 1 @ 0  — WalEntryKind::TxEntry
    pub kind: u8,              // 1 @ 1  — EntryKind::CREDIT | EntryKind::DEBIT
    pub _pad0: [u8; 6],        // 6 @ 2
    pub _pad1: [u8; 8],        // 8 @ 8  — reserved (tx_id is on the trailing TxMetadata)
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
    pub _pad1: [u8; 8],  // 8 @ 8  — reserved (tx_id is on the trailing TxMetadata)
    pub to_tx_id: u64,   // 8 @ 16  — referenced transaction
    pub _pad2: [u8; 16], // 16 @ 24
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

/// Raft term/quorum snapshot recorded inline in the WAL stream. Written by
/// the cluster layer at every term transition so a recovering node can read
/// the WAL alone and learn which term was active at each point. Structural
/// (carries no `tx_id`) like `FunctionRegistered`.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct TxTerm {
    pub entry_type: u8,  // 1 @ 0  — WalEntryKind::TxTerm (6)
    pub _pad0: [u8; 7],  // 7 @ 1  — align term to 8
    pub term: u64,       // 8 @ 8
    pub node_id: u64,    // 8 @ 16 — local node id
    pub node_count: u16, // 2 @ 24 — total voters in the cluster
    pub node_voted: u16, // 2 @ 26 — votes received in this term
    pub _pad1: [u8; 12], // 12 @ 28 — pad to 40
} // total: 40 bytes

/// Account-layout WAL record: brings a contiguous range of accounts into
/// existence with an initial `flags` word (status lane = OPEN). Written as a
/// follower in the trailer layout, like `TxEntry`. Carries `user_ref` so the
/// originating request is identifiable from the record alone.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct AccountOpened {
    pub entry_type: u8,        // 1 @ 0  — WalEntryKind::AccountOpened (7)
    pub _pad: [u8; 3],         // 3 @ 1
    pub count: u32,            // 4 @ 4  — number of accounts opened (>= 1)
    pub begin_account_id: u64, // 8 @ 8  — first id in the contiguous range
    pub flags: u64,            // 8 @ 16 — initial flags applied to every account in the range
    pub user_ref: u64,         // 8 @ 24 — client request reference
    pub _pad2: [u8; 8],        // 8 @ 32 — pad to 40
} // total: 40 bytes

impl AccountOpened {
    pub fn new(begin_account_id: u64, count: u32, flags: u64, user_ref: u64) -> Self {
        Self {
            entry_type: WalEntryKind::AccountOpened as u8,
            _pad: [0; 3],
            count,
            begin_account_id,
            flags,
            user_ref,
            _pad2: [0; 8],
        }
    }
}

/// Account-layout WAL record: links a bucket (`child_id`) to a `parent_id`
/// under a program-defined `type_id` (ADR-022 §3). Written as a follower in
/// the trailer layout, emitted by `linked_account`'s get-or-create path
/// alongside the bucket's `AccountOpened`.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct AccountLinked {
    pub entry_type: u8,  // 1 @ 0  — WalEntryKind::AccountLinked (8)
    pub _pad: [u8; 1],   // 1 @ 1
    pub type_id: u16,    // 2 @ 2  — program-defined bucket type (HOLD, BONUS, …)
    pub _pad2: [u8; 4],  // 4 @ 4  — align to 8
    pub parent_id: u64,  // 8 @ 8
    pub child_id: u64,   // 8 @ 16 — the linked bucket account
    pub _pad3: [u8; 16], // 16 @ 24 — pad to 40
} // total: 40 bytes

impl AccountLinked {
    pub fn new(parent_id: u64, type_id: u16, child_id: u64) -> Self {
        Self {
            entry_type: WalEntryKind::AccountLinked as u8,
            _pad: [0; 1],
            type_id,
            _pad2: [0; 4],
            parent_id,
            child_id,
            _pad3: [0; 16],
        }
    }
}

/// Account-layout WAL record: an in-place flags mutation (ADR-022 §4), emitted
/// by the WASM `set_flag` host fn. Carries the prior and resulting 8-lane flags
/// word — replay applies `new_flags`; transactor rollback restores `prev_flags`.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct AccountFlagsUpdated {
    pub entry_type: u8,  // 1 @ 0  — WalEntryKind::AccountFlagsUpdated (9)
    pub _pad: [u8; 7],   // 7 @ 1  — align to 8
    pub account_id: u64, // 8 @ 8
    pub prev_flags: u64, // 8 @ 16
    pub new_flags: u64,  // 8 @ 24
    pub _pad2: [u8; 8],  // 8 @ 32 — pad to 40
} // total: 40 bytes

impl AccountFlagsUpdated {
    pub fn new(account_id: u64, prev_flags: u64, new_flags: u64) -> Self {
        Self {
            entry_type: WalEntryKind::AccountFlagsUpdated as u8,
            _pad: [0; 7],
            account_id,
            prev_flags,
            new_flags,
            _pad2: [0; 8],
        }
    }
}

/// Programmable-state WAL record (ADR-023): one key→value mutation. A trailer
/// follower like `TxEntry`, carrying no amount. `key` is a packed `KeyPath` and
/// `value` a packed `Value` (see `kv.rs`); an empty `value` slot deletes the key.
/// Replay is a plain per-key assignment of the decoded value.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct KvEntry {
    pub entry_type: u8, // 1 @ 0   — WalEntryKind::Kv (10)
    pub key: [u8; 30],  // 30 @ 1  — packed KeyPath, zero-terminated
    pub value: [u8; 9], // 9 @ 31  — packed Value; empty = delete
} // total: 40 bytes

impl KvEntry {
    /// Build from already-packed `KeyPath` / `Value` slot bytes (`kv.rs` does
    /// the packing). `KeyPath::pack` / `Value::pack_slot` produce these.
    pub fn new(key: [u8; 30], value: [u8; 9]) -> Self {
        Self {
            entry_type: WalEntryKind::Kv as u8,
            key,
            value,
        }
    }
}

/// Defines a constant string (ADR-023): binds a `u32` key to a fixed,
/// null-terminated UTF-8 value. A standalone registration record (like
/// `FunctionRegistered`), not a trailer follower. The registry of interned
/// constants a KV key/value can reference by `key` (the referencing `Value`
/// kind is a follow-up).
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct KvConstant {
    pub entry_type: u8,  // 1 @ 0  — WalEntryKind::KvConstant (11)
    pub _pad: [u8; 3],   // 3 @ 1  — align key
    pub key: u32,        // 4 @ 4
    pub value: [u8; 32], // 32 @ 8 — null-terminated UTF-8; tail zeroed
} // total: 40 bytes

impl KvConstant {
    pub fn new(key: u32, value: &[u8]) -> Self {
        let mut buf = [0u8; 32];
        let n = value.len().min(32);
        buf[..n].copy_from_slice(&value[..n]);
        Self {
            entry_type: WalEntryKind::KvConstant as u8,
            _pad: [0; 3],
            key,
            value: buf,
        }
    }

    /// The string bytes up to the first null (or the full buffer if unterminated).
    pub fn as_bytes(&self) -> &[u8] {
        let end = self
            .value
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.value.len());
        &self.value[..end]
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WalEntry {
    Metadata(TxMetadata),
    Entry(TxEntry),
    Link(TxLink),
    FunctionRegistered(FunctionRegistered),
    Term(TxTerm),
    AccountOpened(AccountOpened),
    AccountLinked(AccountLinked),
    AccountFlagsUpdated(AccountFlagsUpdated),
    Kv(KvEntry),
    KvConstant(KvConstant),
}

impl WalEntry {
    pub fn kind(&self) -> WalEntryKind {
        match self {
            WalEntry::Metadata(_) => WalEntryKind::TxMetadata,
            WalEntry::Entry(_) => WalEntryKind::TxEntry,
            WalEntry::Link(_) => WalEntryKind::Link,
            WalEntry::FunctionRegistered(_) => WalEntryKind::FunctionRegistered,
            WalEntry::Term(_) => WalEntryKind::TxTerm,
            WalEntry::AccountOpened(_) => WalEntryKind::AccountOpened,
            WalEntry::AccountLinked(_) => WalEntryKind::AccountLinked,
            WalEntry::AccountFlagsUpdated(_) => WalEntryKind::AccountFlagsUpdated,
            WalEntry::Kv(_) => WalEntryKind::Kv,
            WalEntry::KvConstant(_) => WalEntryKind::KvConstant,
        }
    }
}

/// A committed transaction reconstructed from the WAL: its closing `TxMetadata`
/// plus the follower entries that precede it (in forward / file order). Produced
/// by `WalScanner` and the query-index path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedTransaction {
    pub meta: TxMetadata,
    pub entries: Vec<WalEntry>,
}

impl CommittedTransaction {
    pub fn tx_id(&self) -> u64 {
        self.meta.tx_id
    }
    pub fn is_err(&self) -> bool {
        self.meta.fail_reason != FailReason::NONE
    }

    pub fn is_success(&self) -> bool {
        self.meta.fail_reason == FailReason::NONE
    }

    pub fn get_fail_reason(&self) -> FailReason {
        self.meta.fail_reason
    }
}

// Hard compile-time guarantees: every WAL entry must be exactly 40 bytes.
const _: () = assert!(size_of::<TxMetadata>() == 40);
const _: () = assert!(size_of::<TxEntry>() == 40);
const _: () = assert!(size_of::<TxLink>() == 40);
const _: () = assert!(size_of::<FunctionRegistered>() == 40);
const _: () = assert!(size_of::<TxTerm>() == 40);
const _: () = assert!(size_of::<AccountOpened>() == 40);
const _: () = assert!(size_of::<AccountLinked>() == 40);
const _: () = assert!(size_of::<AccountFlagsUpdated>() == 40);
const _: () = assert!(size_of::<KvEntry>() == 40);
const _: () = assert!(align_of::<KvEntry>() == 1);
const _: () = assert!(size_of::<KvConstant>() == 40);
