//! Programmable KV state (ADR-023): the transactor-side stores behind the
//! `ledger` host verbs (`kv_*`, `tree_*`, `register_*`). An extension of
//! [`Computer`] — the three access shapes (registers / map / tree) plus one
//! method per host verb.
//!
//! Each mutating verb logs a `KvEntry` follower carrying the **resolved** value
//! (a counter add records its result, not a delta) so replay is a plain per-key
//! assignment, and records an undo entry so `rollback` restores the store on a
//! failed tx. Recovery and follower-replay reapply `KvEntry` records through
//! [`Computer::apply_kv`] — no re-execution.

use super::computer::Computer;
use rustc_hash::FxHashMap;
use std::collections::{BTreeMap, HashMap};
use storage::entities::{KvEntry, WalEntry};

// `kv_scope` namespace, recorded on each `KvEntry`; recovery routes by it so a
// flat key→value fold reconstructs the right store. The map keeps the ADR's
// 0 = global / 1 = account; tree and registers extend the namespace space.
pub(crate) const KV_MAP_GLOBAL: u16 = 0;
pub(crate) const KV_MAP_ACCOUNT: u16 = 1;
pub(crate) const KV_TREE_GLOBAL: u16 = 2;
pub(crate) const KV_TREE_ACCOUNT: u16 = 3;
pub(crate) const KV_REGISTER: u16 = 4;

/// Global register slots (ADR-023 §2): 65,536 `i64`s, indexed by a `u16` id.
pub(crate) const REGISTER_COUNT: usize = 1 << 16;

/// Addressing key for a KV cell: namespace + account + 4-component path. Used by
/// the in-memory stores and by `ActiveSnapshot.kv` during recovery.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KvKey {
    pub kv_scope: u16,
    pub account_id: u64,
    pub key: [u32; 4],
}

impl KvKey {
    #[inline]
    pub fn new(kv_scope: u16, account_id: u64, key: [u32; 4]) -> Self {
        Self {
            kv_scope,
            account_id,
            key,
        }
    }
}

/// The three access shapes. Registers are a dense global array; map is point
/// access; tree is ordered for range scans. Disjoint by `kv_scope`, so every
/// `KvKey` resolves to exactly one store.
pub(crate) struct KvStores {
    registers: Box<[i64]>,
    map: FxHashMap<KvKey, i64>,
    tree: BTreeMap<KvKey, i64>,
}

impl KvStores {
    pub(crate) fn new() -> Self {
        Self {
            registers: vec![0i64; REGISTER_COUNT].into_boxed_slice(),
            map: FxHashMap::default(),
            tree: BTreeMap::new(),
        }
    }

    #[inline]
    fn get(&self, key: &KvKey) -> i64 {
        match key.kv_scope {
            KV_REGISTER => self
                .registers
                .get(key.key[0] as usize)
                .copied()
                .unwrap_or(0),
            KV_TREE_GLOBAL | KV_TREE_ACCOUNT => self.tree.get(key).copied().unwrap_or(0),
            _ => self.map.get(key).copied().unwrap_or(0),
        }
    }

    /// Set `key` to `value`; `value == 0` deletes (presence ⟺ nonzero).
    #[inline]
    fn set(&mut self, key: KvKey, value: i64) {
        match key.kv_scope {
            KV_REGISTER => {
                if let Some(slot) = self.registers.get_mut(key.key[0] as usize) {
                    *slot = value;
                }
            }
            KV_TREE_GLOBAL | KV_TREE_ACCOUNT => {
                if value == 0 {
                    self.tree.remove(&key);
                } else {
                    self.tree.insert(key, value);
                }
            }
            _ => {
                if value == 0 {
                    self.map.remove(&key);
                } else {
                    self.map.insert(key, value);
                }
            }
        }
    }
}

#[inline]
fn tree_scope(account_id: u64) -> u16 {
    if account_id == 0 {
        KV_TREE_GLOBAL
    } else {
        KV_TREE_ACCOUNT
    }
}

impl Computer {
    /// Mutating helper: record undo, update the store, and log the resolved
    /// `KvEntry` follower (folded into the trailer CRC, like credit/debit).
    fn kv_record(&mut self, key: KvKey, value: i64) {
        let prev = self.kv.get(&key);
        self.kv_undo.push((key, prev));
        self.kv.set(key, value);
        self.push_follower(WalEntry::Kv(KvEntry::new(
            key.kv_scope,
            key.account_id,
            key.key,
            value,
        )));
    }

    // ── Map (point) ─────────────────────────────────────────────────────────

    pub fn kv_get(&self, key: [u32; 4]) -> i64 {
        self.kv.get(&KvKey::new(KV_MAP_GLOBAL, 0, key))
    }
    pub fn kv_set(&mut self, key: [u32; 4], value: i64) {
        self.kv_record(KvKey::new(KV_MAP_GLOBAL, 0, key), value);
    }
    /// Read the account cell, falling back to the global one when absent.
    pub fn kv_get_scoped(&self, account_id: u64, key: [u32; 4]) -> i64 {
        let scoped = self.kv.get(&KvKey::new(KV_MAP_ACCOUNT, account_id, key));
        if scoped != 0 {
            scoped
        } else {
            self.kv.get(&KvKey::new(KV_MAP_GLOBAL, 0, key))
        }
    }
    pub fn kv_set_scoped(&mut self, account_id: u64, key: [u32; 4], value: i64) {
        self.kv_record(KvKey::new(KV_MAP_ACCOUNT, account_id, key), value);
    }
    /// Signed merge; returns the resolved value (which is what gets logged).
    pub fn kv_add(&mut self, account_id: u64, key: [u32; 4], delta: i64) -> i64 {
        let scope = if account_id == 0 {
            KV_MAP_GLOBAL
        } else {
            KV_MAP_ACCOUNT
        };
        let k = KvKey::new(scope, account_id, key);
        let new = self.kv.get(&k).saturating_add(delta);
        self.kv_record(k, new);
        new
    }

    // ── Tree (ordered) ──────────────────────────────────────────────────────

    pub fn tree_get(&self, account_id: u64, key: [u32; 4]) -> i64 {
        self.kv
            .get(&KvKey::new(tree_scope(account_id), account_id, key))
    }
    pub fn tree_set(&mut self, account_id: u64, key: [u32; 4], value: i64) {
        self.kv_record(KvKey::new(tree_scope(account_id), account_id, key), value);
    }
    /// Scan `key[3]` in `[lo, hi]` under the fixed `prefix` (= key[0..3]),
    /// returning up to `cap` `(key[3], value)` pairs in ascending key order.
    pub fn tree_range(
        &self,
        account_id: u64,
        prefix: [u32; 3],
        lo: u32,
        hi: u32,
        cap: usize,
    ) -> Vec<(u32, i64)> {
        let scope = tree_scope(account_id);
        let start = KvKey::new(scope, account_id, [prefix[0], prefix[1], prefix[2], lo]);
        let end = KvKey::new(scope, account_id, [prefix[0], prefix[1], prefix[2], hi]);
        self.kv
            .tree
            .range(start..=end)
            .take(cap)
            .map(|(k, v)| (k.key[3], *v))
            .collect()
    }

    // ── Registers (global) ──────────────────────────────────────────────────

    pub fn register_read(&self, id: u32) -> i64 {
        self.kv.get(&KvKey::new(KV_REGISTER, 0, [id, 0, 0, 0]))
    }
    pub fn register_write(&mut self, id: u32, value: i64) {
        self.kv_record(KvKey::new(KV_REGISTER, 0, [id, 0, 0, 0]), value);
    }

    // ── Replay / recovery ───────────────────────────────────────────────────

    /// Apply a replicated/replayed `KvEntry` to the store (no follower, no undo).
    pub fn apply_kv(&mut self, kv: &KvEntry) {
        self.kv
            .set(KvKey::new(kv.kv_scope, kv.account_id, kv.key), kv.value);
    }

    /// Seed the stores from the recovery-folded flat map (ADR-023 §5).
    pub fn recover_kv(&mut self, map: &HashMap<KvKey, i64>) {
        for (key, value) in map {
            self.kv.set(*key, *value);
        }
    }

    /// Restore every cell mutated in the current tx (failed-tx rollback).
    pub(crate) fn rollback_kv(&mut self) {
        while let Some((key, prev)) = self.kv_undo.pop() {
            self.kv.set(key, prev);
        }
    }
}

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
    fn map_set_get_and_delete() {
        let mut c = computer();
        assert_eq!(c.kv_get([1, 0, 0, 0]), 0); // missing reads 0
        c.kv_set([1, 0, 0, 0], 42);
        assert_eq!(c.kv_get([1, 0, 0, 0]), 42);
        c.kv_set([1, 0, 0, 0], 0); // 0 deletes
        assert_eq!(c.kv_get([1, 0, 0, 0]), 0);
    }

    #[test]
    fn add_returns_resolved_value() {
        let mut c = computer();
        assert_eq!(c.kv_add(0, [9, 0, 0, 0], 5), 5);
        assert_eq!(c.kv_add(0, [9, 0, 0, 0], 3), 8);
        assert_eq!(c.kv_get([9, 0, 0, 0]), 8);
    }

    #[test]
    fn scoped_falls_back_to_global() {
        let mut c = computer();
        c.kv_set([7, 0, 0, 0], 100); // global
        assert_eq!(c.kv_get_scoped(5, [7, 0, 0, 0]), 100); // falls back
        c.kv_set_scoped(5, [7, 0, 0, 0], 200); // account override
        assert_eq!(c.kv_get_scoped(5, [7, 0, 0, 0]), 200);
        assert_eq!(c.kv_get_scoped(6, [7, 0, 0, 0]), 100); // other account still global
    }

    #[test]
    fn registers_read_write() {
        let mut c = computer();
        assert_eq!(c.register_read(3), 0);
        c.register_write(3, 77);
        assert_eq!(c.register_read(3), 77);
    }

    #[test]
    fn tree_range_returns_sorted_window() {
        let mut c = computer();
        for k3 in [5u32, 1, 9, 3] {
            c.tree_set(0, [1, 0, 0, k3], (k3 * 10) as i64);
        }
        let got = c.tree_range(0, [1, 0, 0], 2, 8, 128);
        assert_eq!(got, vec![(3, 30), (5, 50)]);
    }

    #[test]
    fn rollback_restores_prior_values() {
        let mut c = computer();
        c.kv_set([1, 0, 0, 0], 10);
        c.register_write(2, 20);
        c.begin(*b"KV2\0\0\0\0\0", 0, 0); // new tx clears undo; baseline = {1:10, reg2:20}
        c.kv_set([1, 0, 0, 0], 99);
        c.register_write(2, 99);
        c.rollback_kv();
        assert_eq!(c.kv_get([1, 0, 0, 0]), 10);
        assert_eq!(c.register_read(2), 20);
    }

    // ── Isolation: one operation/key/scope/store must not disturb another ────

    #[test]
    fn distinct_map_keys_are_independent() {
        let mut c = computer();
        c.kv_set([1, 0, 0, 0], 10);
        c.kv_set([2, 0, 0, 0], 20);
        c.kv_set([1, 0, 0, 1], 30); // differs only in the last component
        c.kv_set([1, 0, 0, 0], 11); // overwrite the first key only
        assert_eq!(c.kv_get([1, 0, 0, 0]), 11);
        assert_eq!(c.kv_get([2, 0, 0, 0]), 20);
        assert_eq!(c.kv_get([1, 0, 0, 1]), 30);
    }

    #[test]
    fn distinct_register_slots_are_independent() {
        let mut c = computer();
        c.register_write(1, 100);
        c.register_write(2, 200);
        c.register_write(1, 101); // overwrite slot 1 only
        assert_eq!(c.register_read(0), 0); // untouched slot
        assert_eq!(c.register_read(1), 101);
        assert_eq!(c.register_read(2), 200);
    }

    #[test]
    fn account_scopes_are_isolated() {
        let mut c = computer();
        c.kv_set_scoped(5, [1, 0, 0, 0], 50);
        c.kv_set_scoped(6, [1, 0, 0, 0], 60);
        c.kv_set([1, 0, 0, 0], 1); // global, same path
        // Each scope keeps its own value; none leaks into the others.
        assert_eq!(c.kv_get_scoped(5, [1, 0, 0, 0]), 50);
        assert_eq!(c.kv_get_scoped(6, [1, 0, 0, 0]), 60);
        assert_eq!(c.kv_get([1, 0, 0, 0]), 1);
    }

    #[test]
    fn scoped_write_does_not_leak_to_global() {
        let mut c = computer();
        c.kv_set_scoped(5, [9, 0, 0, 0], 42);
        assert_eq!(c.kv_get([9, 0, 0, 0]), 0); // global path still empty
    }

    #[test]
    fn stores_do_not_share_a_namespace() {
        let mut c = computer();
        // Identical address components routed to three different stores stay
        // independent — registers / map / tree occupy disjoint namespaces.
        c.register_write(1, 10);
        c.kv_set([1, 0, 0, 0], 20);
        c.tree_set(0, [1, 0, 0, 0], 30);
        assert_eq!(c.register_read(1), 10);
        assert_eq!(c.kv_get([1, 0, 0, 0]), 20);
        assert_eq!(c.tree_get(0, [1, 0, 0, 0]), 30);
        // Mutating one leaves the other two untouched.
        c.kv_set([1, 0, 0, 0], 21);
        assert_eq!(c.register_read(1), 10);
        assert_eq!(c.tree_get(0, [1, 0, 0, 0]), 30);
    }

    #[test]
    fn counters_are_independent() {
        let mut c = computer();
        assert_eq!(c.kv_add(0, [1, 0, 0, 0], 5), 5);
        assert_eq!(c.kv_add(0, [2, 0, 0, 0], 3), 3); // separate counter
        assert_eq!(c.kv_add(0, [1, 0, 0, 0], 2), 7); // first resumes from 5
        assert_eq!(c.kv_get([2, 0, 0, 0]), 3); // second untouched
    }

    #[test]
    fn delete_only_affects_target_key() {
        let mut c = computer();
        c.kv_set([1, 0, 0, 0], 10);
        c.kv_set([2, 0, 0, 0], 20);
        c.kv_set([1, 0, 0, 0], 0); // delete key 1
        assert_eq!(c.kv_get([1, 0, 0, 0]), 0);
        assert_eq!(c.kv_get([2, 0, 0, 0]), 20); // sibling survives
    }

    #[test]
    fn tree_keys_independent_and_range_excludes_others() {
        let mut c = computer();
        c.tree_set(0, [1, 0, 0, 1], 11);
        c.tree_set(0, [1, 0, 0, 2], 12);
        c.tree_set(0, [2, 0, 0, 1], 21); // different prefix
        // Range over prefix (1,0,0) must not pick up the (2,0,0) key.
        assert_eq!(
            c.tree_range(0, [1, 0, 0], 0, 9, 128),
            vec![(1, 11), (2, 12)]
        );
        assert_eq!(c.tree_get(0, [2, 0, 0, 1]), 21);
    }
}
