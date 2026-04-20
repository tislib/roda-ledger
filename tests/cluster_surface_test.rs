//! Integration tests for ADR-015 `Ledger::append_wal_entries` + `WalTailer`.

use roda_ledger::entities::{
    EntryKind, FailReason, TxEntry, TxMetadata, WalEntry, WalEntryKind,
};
use roda_ledger::ledger::{Ledger, LedgerConfig, StorageConfig};
use roda_ledger::transaction::{Operation, WaitLevel};
use std::time::{Duration, Instant};

const WAL_RECORD_SIZE: usize = 40;
const TX_ID_OFFSET: usize = 8;

fn started_ledger() -> Ledger {
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().expect("ledger start");
    ledger
}

/// Ledger that rotates every `tx_per_segment` transactions.
fn started_ledger_with_rotation(tx_per_segment: u64) -> Ledger {
    let mut cfg = LedgerConfig::temp();
    cfg.storage = StorageConfig {
        transaction_count_per_segment: tx_per_segment,
        ..cfg.storage
    };
    let mut ledger = Ledger::new(cfg);
    ledger.start().expect("ledger start");
    ledger
}

fn wait_until<F: FnMut() -> bool>(label: &str, mut f: F) {
    let start = Instant::now();
    while !f() {
        if start.elapsed() > Duration::from_secs(5) {
            panic!("timed out waiting for: {}", label);
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Metadata + Credit-entry pair as if a leader had pre-validated the tx.
fn deposit_entries(tx_id: u64, account: u64, amount: u64) -> Vec<WalEntry> {
    let meta = TxMetadata {
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
    let entry = TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::Credit,
        _pad0: [0; 6],
        tx_id,
        account_id: account,
        amount,
        computed_balance: amount as i64,
    };
    vec![WalEntry::Metadata(meta), WalEntry::Entry(entry)]
}

fn record_tx_id(record: &[u8]) -> u64 {
    u64::from_le_bytes(record[TX_ID_OFFSET..TX_ID_OFFSET + 8].try_into().unwrap())
}

/// Drive a deposit through the normal client path and block until committed.
fn deposit_client(ledger: &Ledger, account: u64, amount: u64) -> u64 {
    let r = ledger.submit_and_wait(
        Operation::Deposit { account, amount, user_ref: 0 },
        WaitLevel::Committed,
    );
    assert!(r.fail_reason.is_success());
    r.tx_id
}

// ── append_wal_entries ──────────────────────────────────────────────────────

#[test]
fn append_wal_entries_advances_commit_index() {
    let mut ledger = started_ledger();
    assert_eq!(ledger.last_commit_id(), 0);

    ledger.append_wal_entries(deposit_entries(1, 42, 500)).unwrap();
    wait_until("commit_index >= 1", || ledger.last_commit_id() >= 1);
    assert_eq!(ledger.last_commit_id(), 1);
}

#[test]
fn append_wal_entries_writes_records_visible_via_tailer() {
    let mut ledger = started_ledger();
    ledger.append_wal_entries(deposit_entries(1, 7, 1_000)).unwrap();
    wait_until("commit_index >= 1", || ledger.last_commit_id() >= 1);

    let mut tailer = ledger.wal_tailer();
    let mut buf = vec![0u8; 4096];
    let n = tailer.tail(1, &mut buf) as usize;
    assert_eq!(n, WAL_RECORD_SIZE * 2);
    assert_eq!(buf[0], WalEntryKind::TxMetadata as u8);
    assert_eq!(buf[WAL_RECORD_SIZE], WalEntryKind::TxEntry as u8);
    for rec in 0..2 {
        assert_eq!(record_tx_id(&buf[rec * WAL_RECORD_SIZE..]), 1);
    }
}

#[test]
fn append_wal_entries_multi_slot_batches_multiple_tx() {
    let mut ledger = started_ledger();
    let mut batch = Vec::new();
    for tx_id in 1..=3 {
        batch.extend(deposit_entries(tx_id, tx_id, 100));
    }
    ledger.append_wal_entries(batch).unwrap();
    wait_until("commit_index >= 3", || ledger.last_commit_id() >= 3);

    let mut tailer = ledger.wal_tailer();
    let mut buf = vec![0u8; 4096];
    let n = tailer.tail(1, &mut buf) as usize;
    assert_eq!(n, WAL_RECORD_SIZE * 6);
    for (i, tx) in [1u64, 1, 2, 2, 3, 3].iter().enumerate() {
        assert_eq!(record_tx_id(&buf[i * WAL_RECORD_SIZE..]), *tx);
    }
}

#[test]
fn append_wal_entries_empty_batch_is_noop() {
    let mut ledger = started_ledger();
    ledger.append_wal_entries(Vec::new()).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(ledger.last_commit_id(), 0);
}

// ── WalTailer ───────────────────────────────────────────────────────────────

#[test]
fn tailer_passive_empty_ledger_from_nonzero_returns_zero() {
    let ledger = started_ledger();
    let mut tailer = ledger.wal_tailer();
    let mut buf = vec![0u8; 256];
    assert_eq!(tailer.tail(1, &mut buf), 0);
}

#[test]
fn tailer_from_zero_includes_segment_header() {
    let ledger = started_ledger();
    let mut tailer = ledger.wal_tailer();
    wait_until("segment header visible", || {
        let mut buf = vec![0u8; 256];
        tailer.tail(0, &mut buf) >= WAL_RECORD_SIZE as u32
    });

    tailer.reset();
    let mut buf = vec![0u8; 256];
    let n = tailer.tail(0, &mut buf) as usize;
    assert!(n >= WAL_RECORD_SIZE);
    assert_eq!(buf[0], WalEntryKind::SegmentHeader as u8);
}

#[test]
fn tailer_from_tx_id_skips_structural_records() {
    let ledger = started_ledger();
    let tx = deposit_client(&ledger, 1, 100);

    let mut tailer = ledger.wal_tailer();
    let mut buf = vec![0u8; 4096];
    let n = tailer.tail(tx, &mut buf) as usize;
    // Deposit → 1 metadata + 2 entries (debit + credit); structural skipped.
    assert_eq!(n, WAL_RECORD_SIZE * 3);
    assert_eq!(buf[0], WalEntryKind::TxMetadata as u8);
    assert_eq!(buf[WAL_RECORD_SIZE], WalEntryKind::TxEntry as u8);
    assert_eq!(buf[WAL_RECORD_SIZE * 2], WalEntryKind::TxEntry as u8);
}

#[test]
fn tailer_rounds_buffer_down_to_record_boundary() {
    let ledger = started_ledger();
    deposit_client(&ledger, 1, 10);
    let mut tailer = ledger.wal_tailer();

    let mut buf = vec![0u8; 79];
    assert_eq!(tailer.tail(1, &mut buf) as usize, WAL_RECORD_SIZE);

    tailer.reset();
    let mut tiny = vec![0u8; 39];
    assert_eq!(tailer.tail(1, &mut tiny), 0);

    let mut empty: [u8; 0] = [];
    assert_eq!(tailer.tail(0, &mut empty), 0);
}

/// Passive tail: ledger is fully written before tailing begins. Multiple
/// small buffers must stream the whole stream without re-emitting any record.
#[test]
fn tailer_passive_resumes_across_calls_without_duplicates() {
    let ledger = started_ledger();
    for _ in 0..5 {
        deposit_client(&ledger, 1, 10);
    }
    let mut tailer = ledger.wal_tailer();

    // Buffer holds exactly 2 records → force many resume steps.
    let mut buf = vec![0u8; WAL_RECORD_SIZE * 2];
    let mut collected: Vec<u64> = Vec::new();
    loop {
        let n = tailer.tail(1, &mut buf) as usize;
        if n == 0 {
            break;
        }
        for i in 0..(n / WAL_RECORD_SIZE) {
            collected.push(record_tx_id(&buf[i * WAL_RECORD_SIZE..]));
        }
    }

    // 5 deposits × 3 records (meta + debit + credit) = 15.
    assert_eq!(collected.len(), 15);
    // tx_ids must be ascending.
    let mut last = 0u64;
    for tx in &collected {
        assert!(*tx >= last);
        last = *tx;
    }
    assert_eq!(collected.first().copied(), Some(1));
    assert_eq!(collected.last().copied(), Some(5));
}

/// Active tail: writer keeps appending while tailer streams. Each call
/// should advance the cursor and surface newly-committed records without
/// rescanning the prefix.
#[test]
fn tailer_actively_sees_new_records_on_each_call() {
    let ledger = started_ledger();
    deposit_client(&ledger, 1, 10);
    let mut tailer = ledger.wal_tailer();

    let mut buf = vec![0u8; 4096];
    let first = tailer.tail(1, &mut buf) as usize;
    assert!(first > 0);

    // No new writes → subsequent call sees nothing.
    let none = tailer.tail(1, &mut buf) as usize;
    assert_eq!(none, 0);

    // Now write more; the tailer picks them up on the next call.
    deposit_client(&ledger, 1, 20);
    deposit_client(&ledger, 1, 30);
    let second = tailer.tail(1, &mut buf) as usize;
    assert!(second > 0);
    // The new bytes must only contain records with tx_id > first batch.
    for i in 0..(second / WAL_RECORD_SIZE) {
        assert!(record_tx_id(&buf[i * WAL_RECORD_SIZE..]) >= 2);
    }
}

/// Lag tail: the caller starts far behind the commit index and streams
/// everything in one call. All committed tx_ids must appear exactly once.
#[test]
fn tailer_from_lag_streams_full_history() {
    let ledger = started_ledger();
    for _ in 0..20 {
        deposit_client(&ledger, 1, 10);
    }
    let mut tailer = ledger.wal_tailer();

    let mut buf = vec![0u8; 40 * 200];
    let n = tailer.tail(1, &mut buf) as usize;
    assert_eq!(n, WAL_RECORD_SIZE * 3 * 20);

    let mut tx_ids: Vec<u64> = Vec::new();
    for i in 0..(n / WAL_RECORD_SIZE) {
        tx_ids.push(record_tx_id(&buf[i * WAL_RECORD_SIZE..]));
    }
    for expected in 1..=20 {
        assert!(tx_ids.contains(&expected), "missing tx_id {}", expected);
    }
}

/// Tail spans multiple segments after rotation. With a very low
/// `transaction_count_per_segment`, the writer rotates mid-stream; the
/// tailer must cross the boundary and stream sealed + active segments.
#[test]
fn tailer_crosses_segment_rotation() {
    let ledger = started_ledger_with_rotation(3);
    // 10 deposits → ~3 rotations at 3 tx per segment.
    for _ in 0..10 {
        deposit_client(&ledger, 1, 10);
    }
    wait_until("commit >= 10", || ledger.last_commit_id() >= 10);

    let mut tailer = ledger.wal_tailer();
    let mut buf = vec![0u8; 40 * 200];
    let n = tailer.tail(1, &mut buf) as usize;
    assert_eq!(n, WAL_RECORD_SIZE * 3 * 10);

    let mut last_tx = 0u64;
    for i in 0..(n / WAL_RECORD_SIZE) {
        let tx = record_tx_id(&buf[i * WAL_RECORD_SIZE..]);
        assert!(tx >= 1 && tx <= 10);
        assert!(tx >= last_tx);
        last_tx = tx;
    }
}

/// Tail while the writer is actively rotating between every call. Each
/// call must resume correctly even as earlier segments become sealed.
#[test]
fn tailer_actively_tails_while_rotating() {
    let ledger = started_ledger_with_rotation(2);
    let mut tailer = ledger.wal_tailer();

    let mut collected: Vec<u64> = Vec::new();
    let mut buf = vec![0u8; WAL_RECORD_SIZE * 4];

    for _ in 0..6 {
        deposit_client(&ledger, 1, 10);
        loop {
            let n = tailer.tail(1, &mut buf) as usize;
            if n == 0 {
                break;
            }
            for i in 0..(n / WAL_RECORD_SIZE) {
                collected.push(record_tx_id(&buf[i * WAL_RECORD_SIZE..]));
            }
        }
    }

    // 6 deposits × 3 records.
    assert_eq!(collected.len(), 18);
    // Order + no duplicate tx_ids in contiguous runs of 3.
    for chunk in collected.chunks(3) {
        assert!(chunk.iter().all(|&t| t == chunk[0]));
    }
    let mut last = 0u64;
    for chunk in collected.chunks(3) {
        assert!(chunk[0] > last);
        last = chunk[0];
    }
    assert_eq!(last, 6);
}

/// Changing `from_tx_id` between calls must re-seek rather than resume.
#[test]
fn tailer_different_from_tx_id_reseeks() {
    let ledger = started_ledger();
    for _ in 0..3 {
        deposit_client(&ledger, 1, 10);
    }
    let mut tailer = ledger.wal_tailer();

    let mut buf = vec![0u8; 4096];
    let n1 = tailer.tail(2, &mut buf) as usize; // skip tx_id=1
    // One submit = 3 records; tx_ids 2 and 3 → 6 records.
    assert_eq!(n1, WAL_RECORD_SIZE * 6);

    // Re-seek: ask from tx 1 again. Must restream everything.
    let n2 = tailer.tail(1, &mut buf) as usize;
    assert_eq!(n2, WAL_RECORD_SIZE * 9);
}

/// `reset()` clears the cursor so the next call starts from the beginning.
#[test]
fn tailer_reset_rewinds_to_start() {
    let ledger = started_ledger();
    deposit_client(&ledger, 1, 10);
    let mut tailer = ledger.wal_tailer();

    let mut buf = vec![0u8; 4096];
    let first = tailer.tail(1, &mut buf) as usize;
    assert!(first > 0);
    assert_eq!(tailer.tail(1, &mut buf), 0);

    tailer.reset();
    let after_reset = tailer.tail(1, &mut buf) as usize;
    assert_eq!(after_reset, first);
}
