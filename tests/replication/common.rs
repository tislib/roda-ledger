//! Shared test fixtures for the replication integration suite.
//!
//! The helpers in this file deliberately avoid reaching into internal
//! crate modules — they rely only on what the crate exposes publicly,
//! the same surface area a downstream consumer of the replication stage
//! would see.

use roda_ledger::config::{LedgerConfig, StorageConfig};
use roda_ledger::entities::{
    EntryKind, FailReason, TxEntry, TxMetadata, WalEntry, WalEntryKind,
};
use roda_ledger::ledger::Ledger;
use roda_ledger::replication::{AppendEntries, AppendOk, Replication};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-test term used by every request in the suite. ADR-015 runs with
/// a single static term.
pub const TEST_TERM: u64 = 1;

// ─────────────────────────────────────────────────────────────────────────
// Temp-dir ledger setup
// ─────────────────────────────────────────────────────────────────────────

/// Scoped temp directory: removes itself on drop so a failed assertion
/// does not leave files behind. The nanosecond suffix matches the
/// pattern used elsewhere in `tests/` so collisions between parallel
/// test runs are impossible.
pub struct TempDir {
    pub path: PathBuf,
}

impl TempDir {
    pub fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = PathBuf::from(format!("temp_replication_{}_{}", tag, nanos));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    pub fn as_str(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Build a `LedgerConfig` pointing at `data_dir`. `replication_mode`
/// flips the follower semantics (Transactor disabled, write APIs
/// refused).
pub fn config_for(data_dir: &str, replication_mode: bool) -> LedgerConfig {
    let mut cfg = LedgerConfig {
        queue_size: 1 << 14,
        max_accounts: 1_000_000,
        storage: StorageConfig {
            data_dir: data_dir.to_string(),
            temporary: false, // TempDir owns cleanup
            snapshot_frequency: 2,
            transaction_count_per_segment: 10_000_000,
        },
        seal_check_internal: Duration::from_millis(10),
        disable_seal: true, // seal not needed for these tests
        ..Default::default()
    };
    cfg.replication_mode = replication_mode;
    cfg
}

/// Build, start, and Arc-wrap a Ledger. Panics on start failure, like
/// the other integration tests.
pub fn start_ledger(cfg: LedgerConfig) -> Arc<Ledger> {
    let mut ledger = Ledger::new(cfg);
    ledger.start().expect("ledger start");
    Arc::new(ledger)
}

// ─────────────────────────────────────────────────────────────────────────
// WAL byte-range builders (mirror `replication::test_helpers`)
// ─────────────────────────────────────────────────────────────────────────
//
// `test_helpers` in `src/replication.rs` is `#[cfg(test)]`-gated, so
// it is invisible from an integration-test crate. Recreating the
// helpers here keeps the integration suite decoupled from the
// library's private test surface.

/// Build a single-entry Credit transaction: one 40-byte TxMetadata
/// followed by one 40-byte TxEntry, CRC filled in. Returns the raw
/// 80-byte buffer ready to go into `AppendEntries.wal_bytes`.
pub fn build_tx(tx_id: u64, account_id: u64, amount: u64) -> Vec<u8> {
    let entry = TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::Credit,
        _pad0: [0; 6],
        tx_id,
        account_id,
        amount,
        computed_balance: amount as i64,
    };
    let mut meta = TxMetadata {
        entry_type: WalEntryKind::TxMetadata as u8,
        entry_count: 1,
        link_count: 0,
        fail_reason: FailReason::NONE,
        crc32c: 0,
        tx_id,
        timestamp: 0,
        user_ref: 0,
        tag: [0; 8],
    };
    let mut digest = crc32c::crc32c(bytemuck::bytes_of(&meta));
    digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&entry));
    meta.crc32c = digest;

    let mut out = Vec::with_capacity(80);
    out.extend_from_slice(bytemuck::bytes_of(&meta));
    out.extend_from_slice(bytemuck::bytes_of(&entry));
    out
}

/// Concatenate `count` single-entry Credit transactions with
/// contiguous tx_ids starting at `first_tx_id`. Every transaction
/// credits `account_id` by `amount_each`.
pub fn build_range(first_tx_id: u64, count: u64, account_id: u64, amount_each: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity((count as usize) * 80);
    // Track running balance so each TxEntry has a correct
    // `computed_balance`. Followers use the leader's computed value
    // verbatim during recovery.
    let mut running: i64 = 0;
    for i in 0..count {
        running += amount_each as i64;
        bytes.extend(build_tx_with_balance(
            first_tx_id + i,
            account_id,
            amount_each,
            running,
        ));
    }
    bytes
}

/// Low-level variant: explicit `computed_balance`. The validator does
/// not check balance arithmetic (that's the snapshot stage's job), but
/// leaf tests that later reopen the ledger in leader mode will see the
/// value we put here, so we keep it consistent.
pub fn build_tx_with_balance(
    tx_id: u64,
    account_id: u64,
    amount: u64,
    computed_balance: i64,
) -> Vec<u8> {
    let entry = TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::Credit,
        _pad0: [0; 6],
        tx_id,
        account_id,
        amount,
        computed_balance,
    };
    let mut meta = TxMetadata {
        entry_type: WalEntryKind::TxMetadata as u8,
        entry_count: 1,
        link_count: 0,
        fail_reason: FailReason::NONE,
        crc32c: 0,
        tx_id,
        timestamp: 0,
        user_ref: 0,
        tag: [0; 8],
    };
    let mut digest = crc32c::crc32c(bytemuck::bytes_of(&meta));
    digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&entry));
    meta.crc32c = digest;

    let mut out = Vec::with_capacity(80);
    out.extend_from_slice(bytemuck::bytes_of(&meta));
    out.extend_from_slice(bytemuck::bytes_of(&entry));
    out
}

/// Decode a leader-produced WAL byte range into `WalEntry` values for
/// inspection. Uses the same parser the validator uses internally.
pub fn decode_range(data: &[u8]) -> Vec<WalEntry> {
    let mut out = Vec::with_capacity(data.len() / 40);
    let mut off = 0;
    while off + 40 <= data.len() {
        // Re-use the library's serializer by parsing byte-by-byte via
        // bytemuck. We only need `Metadata` and `Entry` shapes for the
        // round-trip tests.
        let kind = data[off];
        match kind {
            k if k == WalEntryKind::TxMetadata as u8 => {
                let m: TxMetadata = bytemuck::pod_read_unaligned(&data[off..off + 40]);
                out.push(WalEntry::Metadata(m));
            }
            k if k == WalEntryKind::TxEntry as u8 => {
                let e: TxEntry = bytemuck::pod_read_unaligned(&data[off..off + 40]);
                out.push(WalEntry::Entry(e));
            }
            _ => {}
        }
        off += 40;
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────
// Replication harness
// ─────────────────────────────────────────────────────────────────────────

/// Build an `AppendEntries` describing the contiguous range
/// `[from_tx_id, to_tx_id]` carried by `wal_bytes`. Picks `prev_tx_id`
/// /`prev_term` automatically (0/0 if `from_tx_id == 1`, otherwise the
/// predecessor under the static term).
pub fn make_append<'a>(
    from_tx_id: u64,
    to_tx_id: u64,
    wal_bytes: &'a [u8],
) -> AppendEntries<'a> {
    let (prev_tx_id, prev_term) = if from_tx_id <= 1 {
        (0, 0)
    } else {
        (from_tx_id - 1, TEST_TERM)
    };
    AppendEntries {
        term: TEST_TERM,
        prev_tx_id,
        prev_term,
        from_tx_id,
        to_tx_id,
        wal_bytes,
        leader_commit_tx_id: to_tx_id,
    }
}

/// Drive `Replication::process` and assert success. Returns the
/// `AppendOk` for further assertions.
pub fn apply_ok(replication: &Replication, req: AppendEntries<'_>, last_local_tx_id: u64) -> AppendOk {
    replication
        .process(req, last_local_tx_id)
        .expect("process should succeed on a well-formed request")
}

/// Block the caller until `ledger.last_commit_id() >= target`, with a
/// generous timeout. Replication returns as soon as records are queued
/// to the WAL runner; commit happens asynchronously behind that.
pub fn wait_for_commit(ledger: &Ledger, target: u64, timeout: Duration) {
    let start = Instant::now();
    while ledger.last_commit_id() < target {
        if start.elapsed() >= timeout {
            panic!(
                "timed out waiting for last_commit_id >= {} (saw {})",
                target,
                ledger.last_commit_id()
            );
        }
        std::thread::yield_now();
    }
}
