//! KV key/value codec (ADR-023 §3). Packed TLV is the WAL wire form; `Value` /
//! `KeyPath` are the in-memory truth the maps key on. Solely for KV — not a
//! general-purpose serialization layer.
//!
//! A `Value` is `{typeId}{content}`: the typeId byte is `kind`(high 3 bits) and
//! `len-1`(low 5 bits); `0x00` is the terminator / empty slot; content is
//! little-endian, minimal length (endianness is a wire detail — ordering uses the
//! decoded `Ord`, never the bytes). A `KeyPath` is a run of `Value`s ended by a
//! terminator byte or the buffer edge.

use crate::entities::KvEntry;
use smallvec::SmallVec;
use std::fmt;
use thiserror::Error;

const KIND_SHIFT: u8 = 5;
const LEN_MASK: u8 = 0x1F;

/// Max content bytes a typeId can describe (5-bit `len-1`, so 1..=32).
pub const MAX_CONTENT: usize = 32;

const KIND_INT: u8 = 1;
const KIND_CONST: u8 = 2;

/// Packed byte budgets of the `KvEntry` fields (ADR-023 §2).
pub const KEY_LEN: usize = 30;
pub const VALUE_LEN: usize = 9;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KvError {
    #[error("packed form exceeds {limit}-byte budget")]
    Overflow { limit: usize },
    #[error("buffer truncated mid-value")]
    Truncated,
    #[error("unknown typeId kind {0}")]
    UnknownKind(u8),
    #[error("content length {0} out of range 1..=32")]
    BadLength(usize),
    #[error("empty key path")]
    EmptyKey,
    #[error("cannot pack a resolved constant value to the WAL")]
    Unresolved,
    #[error("cannot parse key path component {0:?}")]
    BadComponent(String),
}

/// One typed TLV unit — both a `KeyPath` element and the `KvEntry` value slot.
///
/// `Const` is an **unresolved** interned-constant reference (the constant's
/// `u32` id) — what the WAL stores. `ConstResolved` is the **resolved** value
/// (the string); the snapshot read-side resolves `Const` → `ConstResolved` and
/// only ever keeps resolved values. `ConstResolved` is never packed to the WAL.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Value {
    /// Signed integer; in-memory `Ord` is numeric (packed bytes are never sorted).
    Int(i64),
    /// Unresolved constant reference: the constant's `u32` id, packed minimally.
    Const(u32),
    /// Resolved constant value (read-side only; never packed to the WAL).
    ConstResolved(String),
}

impl Value {
    /// Build a resolved constant value from its string.
    pub fn from_constant(value: impl Into<String>) -> Value {
        Value::ConstResolved(value.into())
    }

    /// Packed size of this value (typeId + content); 0 for the unpackable
    /// `ConstResolved`.
    pub fn packed_len(&self) -> usize {
        match self {
            Value::Int(v) => 1 + minimal_int_le(*v).len(),
            Value::Const(id) => 1 + minimal_uint_le(*id).len(),
            Value::ConstResolved(_) => 0,
        }
    }

    /// Pack `{typeId}{content}` at the front of `buf`; returns bytes written.
    /// `ConstResolved` is read-side only and cannot be packed.
    pub fn pack_into(&self, buf: &mut [u8]) -> Result<usize, KvError> {
        let (kind, content): (u8, SmallVec<[u8; 16]>) = match self {
            Value::Int(v) => (KIND_INT, minimal_int_le(*v)),
            Value::Const(id) => (KIND_CONST, minimal_uint_le(*id)),
            Value::ConstResolved(_) => return Err(KvError::Unresolved),
        };
        let len = content.len();
        if len == 0 || len > MAX_CONTENT {
            return Err(KvError::BadLength(len));
        }
        let need = 1 + len;
        if need > buf.len() {
            return Err(KvError::Overflow { limit: buf.len() });
        }
        buf[0] = (kind << KIND_SHIFT) | (len as u8 - 1);
        buf[1..need].copy_from_slice(&content);
        Ok(need)
    }

    /// Read one value from the front of `buf`. `Ok(None)` = terminator / empty
    /// (`buf` empty or a leading `0x00`). Returns the value and bytes consumed.
    pub fn unpack_from(buf: &[u8]) -> Result<Option<(Value, usize)>, KvError> {
        let Some(&tid) = buf.first() else {
            return Ok(None);
        };
        if tid == 0 {
            return Ok(None);
        }
        let kind = tid >> KIND_SHIFT;
        let len = (tid & LEN_MASK) as usize + 1;
        let end = 1 + len;
        if end > buf.len() {
            return Err(KvError::Truncated);
        }
        let content = &buf[1..end];
        let value = match kind {
            KIND_INT => Value::Int(sign_extend_le(content)),
            KIND_CONST => Value::Const(uint_from_le(content)),
            other => return Err(KvError::UnknownKind(other)),
        };
        Ok(Some((value, end)))
    }

    /// Pack a value slot (`KvEntry.value`). `None` writes an empty slot (delete).
    pub fn pack_slot(slot: Option<&Value>) -> Result<[u8; VALUE_LEN], KvError> {
        let mut buf = [0u8; VALUE_LEN];
        if let Some(v) = slot {
            v.pack_into(&mut buf)?;
        }
        Ok(buf)
    }

    /// Unpack a value slot; `Ok(None)` = empty (delete).
    pub fn unpack_slot(buf: &[u8; VALUE_LEN]) -> Result<Option<Value>, KvError> {
        Ok(Value::unpack_from(buf)?.map(|(v, _)| v))
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{v}"),
            Value::Const(id) => write!(f, "constant[{id}]"),
            Value::ConstResolved(s) => f.write_str(s),
        }
    }
}

/// An ordered run of `Value`s — the KV key (ADR-023 §4).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyPath(pub SmallVec<[Value; 4]>);

impl KeyPath {
    pub fn new(items: impl IntoIterator<Item = Value>) -> Self {
        KeyPath(items.into_iter().collect())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Parse a `/`-joined string into a `KeyPath` (inverse of `Display`): a
    /// component that parses as an `i64` becomes `Int`, otherwise a resolved
    /// constant string. Rejects empty components and the empty path.
    pub fn from_string(s: &str) -> Result<KeyPath, KvError> {
        let mut items: SmallVec<[Value; 4]> = SmallVec::new();
        for token in s.split('/') {
            if token.is_empty() {
                return Err(KvError::BadComponent(token.to_string()));
            }
            items.push(match token.parse::<i64>() {
                Ok(n) => Value::Int(n),
                Err(_) => Value::ConstResolved(token.to_string()),
            });
        }
        if items.is_empty() {
            return Err(KvError::EmptyKey);
        }
        Ok(KeyPath(items))
    }

    /// Pack into the fixed `KvEntry.key` buffer; the zeroed tail is the
    /// terminator. Rejects an empty path and overflow past `KEY_LEN`.
    pub fn pack(&self) -> Result<[u8; KEY_LEN], KvError> {
        if self.0.is_empty() {
            return Err(KvError::EmptyKey);
        }
        let mut buf = [0u8; KEY_LEN];
        let mut off = 0;
        for v in &self.0 {
            off += v.pack_into(&mut buf[off..])?;
        }
        Ok(buf)
    }

    /// Unpack a `KvEntry.key` buffer: read values until a `0x00` terminator or
    /// the buffer edge. Rejects an empty path.
    pub fn unpack(buf: &[u8; KEY_LEN]) -> Result<KeyPath, KvError> {
        let mut items: SmallVec<[Value; 4]> = SmallVec::new();
        let mut off = 0;
        while let Some((value, used)) = Value::unpack_from(&buf[off..])? {
            items.push(value);
            off += used;
        }
        if items.is_empty() {
            return Err(KvError::EmptyKey);
        }
        Ok(KeyPath(items))
    }
}

impl fmt::Display for KeyPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, v) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("/")?;
            }
            write!(f, "{v}")?;
        }
        Ok(())
    }
}

impl KvEntry {
    /// Pack a key + value slot into the fixed WAL record. `value = None` writes
    /// an empty slot (delete). Errors if the key overflows its 30-byte budget.
    pub fn from_parts(key: &KeyPath, value: Option<&Value>) -> Result<Self, KvError> {
        Ok(KvEntry::new(key.pack()?, Value::pack_slot(value)?))
    }

    /// Decode the packed record into `(KeyPath, Option<Value>)`; `None` = delete.
    pub fn decode(&self) -> Result<(KeyPath, Option<Value>), KvError> {
        Ok((
            KeyPath::unpack(&self.key)?,
            Value::unpack_slot(&self.value)?,
        ))
    }
}

// Content is **little-endian / native** — matching the rest of the codebase
// (snapshot fields use `to_le_bytes`, WAL records are native-endian `Pod`).
// Endianness is NOT a sorting concern: range/ordering is done on the decoded
// Rust types (`KeyPath`/`Value` `Ord`), never on packed bytes. It's only the
// minimal-length wire form: drop the redundant most-significant bytes (the high
// end, which is last in little-endian).

/// Minimal-length two's-complement little-endian bytes (1..=8) of `v`, dropping
/// trailing bytes that are mere sign-extension of the one below them.
fn minimal_int_le(v: i64) -> SmallVec<[u8; 16]> {
    let le = v.to_le_bytes();
    let mut end = 8;
    while end > 1 {
        let top = le[end - 1];
        let below_sign = le[end - 2] & 0x80;
        let redundant = (top == 0x00 && below_sign == 0) || (top == 0xFF && below_sign != 0);
        if redundant {
            end -= 1;
        } else {
            break;
        }
    }
    SmallVec::from_slice(&le[..end])
}

/// Inverse of `minimal_int_le`: sign-extend little-endian bytes to `i64`.
fn sign_extend_le(bytes: &[u8]) -> i64 {
    let fill = if bytes.last().is_some_and(|b| b & 0x80 != 0) {
        0xFF
    } else {
        0x00
    };
    let mut arr = [fill; 8];
    arr[..bytes.len()].copy_from_slice(bytes);
    i64::from_le_bytes(arr)
}

/// Minimal-length little-endian bytes (1..=4) of an unsigned id, dropping
/// trailing zero (high) bytes.
fn minimal_uint_le(v: u32) -> SmallVec<[u8; 16]> {
    let le = v.to_le_bytes();
    let mut end = 4;
    while end > 1 && le[end - 1] == 0 {
        end -= 1;
    }
    SmallVec::from_slice(&le[..end])
}

/// Inverse of `minimal_uint_le`: little-endian bytes to `u32`.
fn uint_from_le(bytes: &[u8]) -> u32 {
    let mut arr = [0u8; 4];
    arr[..bytes.len()].copy_from_slice(bytes);
    u32::from_le_bytes(arr)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_int(v: i64) {
        assert_eq!(sign_extend_le(&minimal_int_le(v)), v, "int {v}");
    }

    #[test]
    fn int_roundtrip_edges() {
        for v in [0, 1, -1, 5, -5, 127, 128, -128, 200, -200, 255, 256] {
            roundtrip_int(v);
        }
        roundtrip_int(i64::MIN);
        roundtrip_int(i64::MAX);
    }

    #[test]
    fn int_minimal_length() {
        assert_eq!(minimal_int_le(0).as_slice(), &[0x00]);
        assert_eq!(minimal_int_le(5).as_slice(), &[0x05]);
        assert_eq!(minimal_int_le(-1).as_slice(), &[0xFF]);
        // 200 needs a leading 0x00 so it stays positive on sign-extend.
        assert_eq!(minimal_int_le(200).as_slice(), &[0xC8, 0x00]); // LE; trailing 0 keeps it positive
        assert_eq!(minimal_int_le(i64::MAX).len(), 8);
    }

    #[test]
    fn keypath_roundtrip() {
        let key = KeyPath::new([Value::Const(42), Value::Int(123)]);
        let packed = key.pack().unwrap();
        assert_eq!(KeyPath::unpack(&packed).unwrap(), key);
    }

    #[test]
    fn const_id_roundtrip_minimal() {
        for id in [0u32, 1, 67, 255, 256, 65535, u32::MAX] {
            let key = KeyPath::new([Value::Const(id)]);
            assert_eq!(KeyPath::unpack(&key.pack().unwrap()).unwrap(), key);
        }
        // 67 packs to a single content byte (typeId + 0x43).
        assert_eq!(minimal_uint_le(67).as_slice(), &[0x43]);
    }

    #[test]
    fn from_string_parses_ints_and_constants() {
        let kp = KeyPath::from_string("1/2/products").unwrap();
        assert_eq!(
            kp,
            KeyPath::new([
                Value::Int(1),
                Value::Int(2),
                Value::ConstResolved("products".into()),
            ])
        );
        assert!(KeyPath::from_string("").is_err());
        assert!(KeyPath::from_string("a//b").is_err());
    }

    #[test]
    fn numeric_constant_name_does_not_roundtrip_via_string() {
        // LIMITATION (ADR-023): the string key form is lossy for numeric
        // constant names. A constant named "100" Displays as "100", but
        // `from_string("100")` parses it back as `Int(100)` — not the constant —
        // so it cannot be queried by name via the string form. Non-numeric names
        // (the common case) are unaffected.
        let c = Value::from_constant("100");
        assert_eq!(c.to_string(), "100");
        let parsed = KeyPath::from_string("100").unwrap();
        assert_eq!(parsed.0[0], Value::Int(100));
        assert_ne!(
            parsed.0[0], c,
            "numeric constant name aliases an integer key"
        );
    }

    #[test]
    fn value_slot_delete_vs_zero() {
        // Empty slot (delete) is distinct from a stored Integer 0.
        let empty = Value::pack_slot(None).unwrap();
        assert_eq!(Value::unpack_slot(&empty).unwrap(), None);

        let zero = Value::pack_slot(Some(&Value::Int(0))).unwrap();
        assert_eq!(Value::unpack_slot(&zero).unwrap(), Some(Value::Int(0)));
        assert_ne!(empty, zero);
    }

    #[test]
    fn empty_key_rejected() {
        assert_eq!(KeyPath::default().pack(), Err(KvError::EmptyKey));
        assert_eq!(KeyPath::unpack(&[0u8; KEY_LEN]), Err(KvError::EmptyKey));
    }

    #[test]
    fn key_overflow_rejected() {
        // 30 single-byte-content consts overflow the 30-byte budget.
        let big = KeyPath::new((0..30).map(Value::Const));
        assert!(matches!(big.pack(), Err(KvError::Overflow { .. })));
    }

    #[test]
    fn full_key_no_terminator() {
        // A path filling exactly KEY_LEN has no 0x00 terminator; unpack stops
        // at the buffer edge.
        let key = KeyPath::new((0..15).map(Value::Const));
        let packed = key.pack().unwrap();
        assert_eq!(packed[KEY_LEN - 1], 0x0E); // last content byte (id 14), no trailing 0
        assert_eq!(KeyPath::unpack(&packed).unwrap(), key);
    }
}
