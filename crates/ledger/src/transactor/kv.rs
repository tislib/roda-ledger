//! Programmable KV state (ADR-023 §4): the transactor-side single map behind the
//! `ledger` host verbs `kv_get` / `kv_set`. An extension of [`Computer`].
//!
//! Each mutating verb logs a packed `KvEntry` follower carrying the resolved
//! value, and records an undo entry so `rollback` restores the map on a failed
//! tx. Recovery and follower-replay reapply `KvEntry` records via
//! [`Computer::apply_kv`] — no re-execution.

use super::computer::Computer;
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use storage::entities::{KvConstant, KvEntry, WalEntry};
use storage::{KeyPath, Value};

/// High bit marking a `u32` key component as a **constant id** rather than a
/// plain integer (ADR-023 §6). `kv_get_constant` sets it; `kv_key` strips it and
/// produces `Value::Const`. Reserves the top bit of the component space, so plain
/// integer key components must be `< 2^31` (constant ids never approach that).
pub(crate) const KV_CONST_TAG: u32 = 1 << 31;

impl Computer {
    /// v1 key shape: each of the four `u32` host args becomes a `KeyPath`
    /// component. A component carrying the [`KV_CONST_TAG`] high bit is a
    /// constant reference (`kv_get_constant` tags its result), so it packs as
    /// `Value::Const`; everything else is a plain `Value::Int`. This is what
    /// preserves "is this a constant?" through to the packed `kind` byte so the
    /// snapshot can resolve it back to a name.
    fn kv_key(key: [u32; 4]) -> KeyPath {
        KeyPath::new(key.iter().map(|&c| {
            if c & KV_CONST_TAG != 0 {
                Value::Const(c & !KV_CONST_TAG)
            } else {
                Value::Int(c as i64)
            }
        }))
    }

    /// Mutating helper: record undo, update the map, log the packed `KvEntry`
    /// follower (folded into the trailer CRC, like credit/debit).
    fn kv_record(&mut self, key: KeyPath, value: Option<Value>) {
        let prev = self.kv.get(&key).cloned();
        self.kv_undo.push((key.clone(), prev));
        match &value {
            Some(v) => {
                self.kv.insert(key.clone(), v.clone());
            }
            None => {
                self.kv.remove(&key);
            }
        }
        if let Ok(entry) = KvEntry::from_parts(&key, value.as_ref()) {
            self.push_follower(WalEntry::Kv(entry));
        }
    }

    /// Read the integer value for `key`; `0` if absent (or non-integer).
    pub fn kv_get(&self, key: [u32; 4]) -> i64 {
        match self.kv.get(&Self::kv_key(key)) {
            Some(Value::Int(v)) => *v,
            _ => 0,
        }
    }

    /// Set `key` to an integer value.
    pub fn kv_set(&mut self, key: [u32; 4], value: i64) {
        self.kv_record(Self::kv_key(key), Some(Value::Int(value)));
    }

    // ── Replay / recovery ───────────────────────────────────────────────────

    /// Apply a replicated/replayed `KvEntry` to the map (no follower, no undo).
    pub fn apply_kv(&mut self, kv: &KvEntry) {
        if let Ok((key, value)) = kv.decode() {
            match value {
                Some(v) => {
                    self.kv.insert(key, v);
                }
                None => {
                    self.kv.remove(&key);
                }
            }
        }
    }

    /// Seed the map from the recovery-folded state (ADR-023 §7).
    pub fn recover_kv(&mut self, map: &HashMap<KeyPath, Value>) {
        self.kv = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    }

    // ── Constants (ADR-023 §6) ───────────────────────────────────────────────

    /// Register a constant by name (create-if-absent), returning its stable `u32`
    /// id. First sight allocates the next id and logs a `KvConstant` follower;
    /// re-registering an existing name is a no-op that returns the same id.
    /// Callable only in the register phase — the host shim enforces that.
    pub fn kv_register_constant(&mut self, name: &[u8]) -> u32 {
        let name = &name[..name.len().min(32)];
        if let Some(&id) = self.kv_constants.get(name) {
            return id;
        }
        let id = self.next_constant_id;
        self.next_constant_id = self.next_constant_id.saturating_add(1);
        self.kv_constants.insert(name.to_vec(), id);
        self.push_follower(WalEntry::KvConstant(KvConstant::new(id, name)));
        id
    }

    /// Look up a registered constant's id by name (read-only); `None` if it was
    /// never registered. The host shim stops execution on `None`.
    pub fn kv_get_constant(&self, name: &[u8]) -> Option<u32> {
        self.kv_constants.get(&name[..name.len().min(32)]).copied()
    }

    /// Seed the constant registry from the recovery-folded `id → value` map
    /// (ADR-023 §6/§7): rebuild `name → id` and resume the allocator past the
    /// highest id.
    pub fn recover_kv_constants(&mut self, map: &HashMap<u32, [u8; 32]>) {
        let mut max_id = 0;
        for (&id, value) in map {
            let end = value.iter().position(|&b| b == 0).unwrap_or(value.len());
            self.kv_constants.insert(value[..end].to_vec(), id);
            max_id = max_id.max(id);
        }
        self.next_constant_id = max_id.saturating_add(1).max(1);
    }

    /// Drop constants created by a failed tx (ids `>= tx_start_constant_id`) and
    /// rewind the allocator.
    pub(crate) fn rollback_kv_constants(&mut self) {
        let from = self.tx_start_constant_id;
        if self.next_constant_id != from {
            self.kv_constants.retain(|_, &mut id| id < from);
            self.next_constant_id = from;
        }
    }

    /// Restore every cell mutated in the current tx (failed-tx rollback).
    pub(crate) fn rollback_kv(&mut self) {
        while let Some((key, prev)) = self.kv_undo.pop() {
            match prev {
                Some(v) => {
                    self.kv.insert(key, v);
                }
                None => {
                    self.kv.remove(&key);
                }
            }
        }
    }
}

/// The transactor's in-memory KV map (ADR-023 §4): a single point-access map,
/// no scopes / registers / tree.
pub(crate) type KvMap = FxHashMap<KeyPath, Value>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_ring::ring::TxRing;
    use crate::wait_strategy::WaitStrategy;

    fn computer() -> Computer {
        let (mut writer, _reader) = TxRing::new(256);
        writer.reserve();
        let mut c = Computer::new(16, writer, WaitStrategy::LowLatency);
        c.begin(*b"KV\0\0\0\0\0\0", 0, 0);
        c
    }

    #[test]
    fn map_set_get() {
        let mut c = computer();
        assert_eq!(c.kv_get([1, 0, 0, 0]), 0); // missing reads 0
        c.kv_set([1, 0, 0, 0], 42);
        assert_eq!(c.kv_get([1, 0, 0, 0]), 42);
        c.kv_set([1, 0, 0, 0], 7); // overwrite
        assert_eq!(c.kv_get([1, 0, 0, 0]), 7);
    }

    #[test]
    fn tagged_component_is_a_constant_distinct_from_int() {
        let mut c = computer();
        // Same numeric value, different type: Int(5) vs Const(5) (tagged) — must
        // be independent keys, proving the constant type survives kv_key.
        c.kv_set([5, 0, 0, 0], 10);
        c.kv_set([5 | KV_CONST_TAG, 0, 0, 0], 20);
        assert_eq!(c.kv_get([5, 0, 0, 0]), 10);
        assert_eq!(c.kv_get([5 | KV_CONST_TAG, 0, 0, 0]), 20);
        // And the stored key really is a Const.
        let const_key = Computer::kv_key([5 | KV_CONST_TAG, 0, 0, 0]);
        assert_eq!(const_key.0[0], Value::Const(5));
    }

    #[test]
    fn distinct_keys_are_independent() {
        let mut c = computer();
        c.kv_set([1, 0, 0, 0], 10);
        c.kv_set([1, 0, 0, 1], 30); // differs only in the last component
        c.kv_set([1, 0, 0, 0], 11); // overwrite first only
        assert_eq!(c.kv_get([1, 0, 0, 0]), 11);
        assert_eq!(c.kv_get([1, 0, 0, 1]), 30);
        assert_eq!(c.kv_get([2, 0, 0, 0]), 0);
    }

    #[test]
    fn rollback_restores_prior_values() {
        let mut c = computer();
        c.kv_set([1, 0, 0, 0], 10);
        c.begin(*b"KV2\0\0\0\0\0", 0, 0); // new tx clears undo; baseline = {1:10}
        c.kv_set([1, 0, 0, 0], 99);
        c.rollback_kv();
        assert_eq!(c.kv_get([1, 0, 0, 0]), 10);
    }

    #[test]
    fn apply_and_decode_roundtrip() {
        let mut c = computer();
        let key = Computer::kv_key([5, 0, 0, 0]);
        let entry = KvEntry::from_parts(&key, Some(&Value::Int(123))).unwrap();
        c.apply_kv(&entry);
        assert_eq!(c.kv_get([5, 0, 0, 0]), 123);
    }
}
