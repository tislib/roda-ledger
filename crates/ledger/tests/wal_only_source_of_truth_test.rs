//! The WAL is the only source of truth: delete every derived artefact
//! (.seal/.crc markers, snapshots, indexes), redraw the segment boundaries by
//! merging raw `.bin` files, restart — and identical balances re-derive and the
//! merged segments re-seal with no tx lost. ~0.8s (79k tx, multi-segment merge).

use ledger::ledger::{Ledger, LedgerConfig};
use ledger::snapshot::{QueryKind, QueryRequest, QueryResponse};
use ledger::transactor::transaction::Operation;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::Duration;
use storage::StorageConfig;
use storage::entities::{EntryKind, WalEntry};

const TX_PER_SEGMENT: u64 = 10_000;
const DEPOSITS: u64 = 79_000;
const ACCOUNTS: u64 = 5; // deposits fan out across a few accounts (1..=5)
const SEALED_AFTER_PHASE1: u32 = 7; // (1 open + 79_000) / 10k → 7 sealed + active

fn unique_dir() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temp_wal_only_truth_{}", nanos)
}

fn config(dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.to_string(),
            transaction_count_per_segment: TX_PER_SEGMENT,
            snapshot_frequency: 2, // snapshot every other sealed segment
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(5),
        ..Default::default()
    }
}

// account/amount are pure functions of the deposit index, so phase 1 and phase 3
// agree on the expected ledger without threading extra state through the test.
fn deposit_account(i: u64) -> u64 {
    (i % ACCOUNTS) + 1
}
fn deposit_amount(i: u64) -> u64 {
    1 + (i % 9)
}

// `wal_NNNNNN.bin` — a sealed segment file (6-digit id). Mirrors the storage
// layer's own filename parser, which is private to that crate.
fn is_sealed_bin(name: &str) -> bool {
    name.strip_prefix("wal_")
        .and_then(|s| s.strip_suffix(".bin"))
        .is_some_and(|s| s.len() == 6 && s.chars().all(|c| c.is_ascii_digit()))
}

fn dir_names(dir: &str) -> Vec<String> {
    fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

fn seal_marker_count(dir: &str) -> usize {
    dir_names(dir)
        .iter()
        .filter(|n| n.starts_with("wal_") && n.ends_with(".seal"))
        .count()
}

fn sealed_bin_bytes(dir: &str, id: u32) -> u64 {
    fs::metadata(Path::new(dir).join(format!("wal_{:06}.bin", id)))
        .unwrap()
        .len()
}

// Keep only the raw WAL — sealed `wal_NNNNNN.bin` segments plus the active
// `wal.bin`. Everything else is derived and gets deleted.
fn keep_only_wal_files(dir: &str) {
    for entry in fs::read_dir(dir).unwrap().flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "wal.bin" || is_sealed_bin(&name) {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            fs::remove_dir_all(&path).unwrap();
        } else {
            fs::remove_file(&path).unwrap();
        }
    }
}

// Concatenate each group of sealed segments into one new segment, renumbered
// 1..=groups.len(). A sealed `.bin` is a pure 40-byte record stream ending on a
// tx boundary, so byte concatenation yields a valid WAL the recoverer replays.
fn merge_segments(dir: &str, groups: &[&[u32]]) {
    let bin = |id: u32| Path::new(dir).join(format!("wal_{:06}.bin", id));

    // Read every source first — output ids overlap the inputs.
    let mut merged: Vec<Vec<u8>> = Vec::new();
    let mut sources = BTreeSet::new();
    for group in groups {
        let mut buf = Vec::new();
        for &id in *group {
            buf.extend_from_slice(&fs::read(bin(id)).unwrap());
            sources.insert(id);
        }
        merged.push(buf);
    }
    for id in sources {
        fs::remove_file(bin(id)).unwrap();
    }
    for (idx, buf) in merged.into_iter().enumerate() {
        fs::write(bin(idx as u32 + 1), buf).unwrap();
    }
}

// Assert the committed tx is a deposit of `amount` into `account` (a Debit
// follower on that account — the credit side lands on SYSTEM(0)).
fn assert_is_deposit(ledger: &Ledger, tx_id: u64, account: u64, amount: u64) {
    let resp = ledger.query_block(QueryRequest {
        kind: QueryKind::GetTransaction { tx_id },
        respond: Box::new(|_| {}),
    });
    let result = match resp {
        QueryResponse::Transaction(Some(r)) => r,
        other => panic!("tx {tx_id} not found: {other:?}"),
    };
    let debit = result
        .entries
        .iter()
        .find_map(|e| match e {
            WalEntry::Entry(en) if en.account_id == account && en.kind == EntryKind::DEBIT => {
                Some(en)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("tx {tx_id} has no Debit entry for account {account}"));
    assert_eq!(debit.amount, amount, "tx {tx_id} deposit amount mismatch");
}

#[test]
fn wal_is_the_only_source_of_truth_after_merge_and_reseal() {
    let dir = unique_dir();
    let _ = fs::remove_dir_all(&dir);

    // Expected per-account balances, derived independently from the index.
    let mut expected = vec![0i64; (ACCOUNTS + 1) as usize];
    let mut total_deposited = 0i64;
    for i in 0..DEPOSITS {
        let amt = deposit_amount(i) as i64;
        expected[deposit_account(i) as usize] += amt;
        total_deposited += amt;
    }

    // ── Phase 1: build a 7-segment ledger, seal it, verify. ──────────────────
    {
        let mut ledger = Ledger::new(config(&dir));
        ledger.start().unwrap();
        ledger.open_accounts(100); // ids 1..=100 (tx 1)

        let mut last_id = 0;
        for i in 0..DEPOSITS {
            last_id = ledger.submit(Operation::Deposit {
                account: deposit_account(i),
                amount: deposit_amount(i),
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();

        // Really sealed: exactly 7 sealed segments, each with a .seal marker,
        // active wal.bin still present.
        assert_eq!(
            ledger.last_sealed_segment_id(),
            SEALED_AFTER_PHASE1,
            "phase 1 should seal exactly 7 segments"
        );
        assert_eq!(seal_marker_count(&dir), SEALED_AFTER_PHASE1 as usize);
        assert!(Path::new(&dir).join("wal.bin").exists());

        // Balances for a few accounts; the deposits really are deposits.
        for a in 1..=ACCOUNTS {
            assert_eq!(
                ledger.get_balance(a),
                expected[a as usize],
                "phase 1 balance mismatch for account {a}"
            );
        }
        // Double-entry: SYSTEM holds the negative; all balances sum to zero.
        assert_eq!(ledger.get_balance(0), -total_deposited);
        let sum: i64 = (0..=ACCOUNTS).map(|a| ledger.get_balance(a)).sum();
        assert_eq!(sum, 0, "phase 1 double-entry must net to zero");
        // An opened-but-unfunded account stays at zero.
        assert_eq!(ledger.get_balance(100), 0);

        // The last few committed transactions are deposits to the expected accounts.
        for k in 1..=ACCOUNTS {
            let i = DEPOSITS - k;
            assert_is_deposit(&ledger, i + 2, deposit_account(i), deposit_amount(i));
        }
        // ledger dropped here → clean shutdown (writes wal.stop)
    }

    // Snapshots were produced — proof they exist before we throw them away.
    assert!(
        dir_names(&dir).iter().any(|n| n.starts_with("snapshot_")),
        "phase 1 should have written at least one snapshot"
    );

    // ── Phase 2: keep only the raw WAL, then redraw segment boundaries. ──────
    keep_only_wal_files(&dir);
    {
        // Only the 7 sealed `.bin` + active `wal.bin` survive; no markers left.
        let names = dir_names(&dir);
        assert_eq!(names.len(), SEALED_AFTER_PHASE1 as usize + 1);
        assert!(names.contains(&"wal.bin".to_string()));
        assert_eq!(seal_marker_count(&dir), 0);
    }

    let bytes_before: u64 = (1..=SEALED_AFTER_PHASE1)
        .map(|id| sealed_bin_bytes(&dir, id))
        .sum();

    merge_segments(&dir, &[&[1, 2], &[3, 4, 5], &[6, 7]]);

    {
        // 3 merged sealed segments + active, still no markers, no bytes lost.
        let bins: Vec<_> = dir_names(&dir)
            .into_iter()
            .filter(|n| is_sealed_bin(n))
            .collect();
        assert_eq!(bins.len(), 3, "merge should leave 3 segments");
        assert_eq!(seal_marker_count(&dir), 0);
        let bytes_after: u64 = (1..=3).map(|id| sealed_bin_bytes(&dir, id)).sum();
        assert_eq!(bytes_before, bytes_after, "merge must not lose WAL bytes");
    }

    // ── Phase 3: restart from raw WAL alone; everything re-derives. ──────────
    {
        let mut ledger = Ledger::new(config(&dir));
        ledger.start().unwrap(); // recovery replays the WAL + re-seals merged segments

        // No tx lost: the highest committed id survives the merge.
        assert_eq!(ledger.last_commit_id(), 1 + DEPOSITS);

        // No balance lost: identical balances, double-entry still nets to zero.
        for a in 1..=ACCOUNTS {
            assert_eq!(
                ledger.get_balance(a),
                expected[a as usize],
                "phase 3 balance mismatch for account {a}"
            );
        }
        assert_eq!(ledger.get_balance(0), -total_deposited);
        let sum: i64 = (0..=ACCOUNTS).map(|a| ledger.get_balance(a)).sum();
        assert_eq!(sum, 0, "phase 3 double-entry must net to zero");
        assert_eq!(ledger.get_balance(100), 0);
    }

    let _ = fs::remove_dir_all(&dir);
}
