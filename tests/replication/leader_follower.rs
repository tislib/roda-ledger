use super::common::{
    apply_ok, config_for, make_append, start_ledger, wait_for_commit, TempDir, TEST_TERM,
};
use roda_ledger::entities::{TxMetadata, WalEntry, WalEntryKind};
use roda_ledger::replication::{validate_wal_bytes, Replication};
use roda_ledger::transaction::Operation;
use std::path::Path;
use std::time::Duration;

fn read_active_wal(data_dir: &str) -> Vec<u8> {
    std::fs::read(Path::new(data_dir).join("wal.bin")).expect("read wal.bin")
}

fn split_header(raw: &[u8]) -> (&[u8], &[u8]) {
    assert!(raw.len() >= 40, "raw wal too short");
    assert_eq!(
        raw[0],
        WalEntryKind::SegmentHeader as u8,
        "expected SegmentHeader as the first record"
    );
    raw.split_at(40)
}

fn tx_ids_in(data: &[u8]) -> Vec<u64> {
    let mut out = Vec::new();
    let mut off = 0;
    while off + 40 <= data.len() {
        if data[off] == WalEntryKind::TxMetadata as u8 {
            let m: TxMetadata = bytemuck::pod_read_unaligned(&data[off..off + 40]);
            out.push(m.tx_id);
        }
        off += 40;
    }
    out
}

fn run_leader(data_dir: &str, account: u64, n: u64, amount_each: u64) -> (u64, Vec<u8>) {
    let leader = start_ledger(config_for(data_dir, false));
    let mut last_tx_id = 0u64;
    for _ in 0..n {
        last_tx_id = leader.submit(Operation::Deposit {
            account,
            amount: amount_each,
            user_ref: 0,
        });
    }
    wait_for_commit(&leader, last_tx_id, Duration::from_secs(5));
    let commit = leader.last_commit_id();
    drop(leader);
    let raw = read_active_wal(data_dir);
    (commit, raw)
}

#[test]
fn leader_authored_bytes_validate_on_follower() {
    let leader_dir = TempDir::new("lf_validate_leader");
    let (commit, raw) = run_leader(&leader_dir.as_str(), 77, 5, 100);
    assert_eq!(commit, 5);

    let (_header, tx_tail) = split_header(&raw);
    let entries = validate_wal_bytes(tx_tail, 1, 5, TEST_TERM).expect("validator accepts");

    let tx_ids = tx_ids_in(tx_tail);
    assert_eq!(tx_ids, vec![1, 2, 3, 4, 5]);
    assert!(
        entries.iter().any(|e| matches!(e, WalEntry::Metadata(m) if m.tx_id == 5)),
        "validator should have returned a metadata for tx_id=5"
    );
}

#[test]
fn follower_wal_bytes_match_leader_wal_bytes() {
    let leader_dir = TempDir::new("lf_bytewise_leader");
    let (_commit, leader_raw) = run_leader(&leader_dir.as_str(), 1001, 8, 250);
    let (_leader_header, leader_tail) = split_header(&leader_raw);

    let follower_dir = TempDir::new("lf_bytewise_follower");
    {
        let follower = start_ledger(config_for(&follower_dir.as_str(), true));
        let replication = Replication::new(follower.ledger_context(), 2, TEST_TERM);

        apply_ok(&replication, make_append(1, 8, leader_tail), 0);
        wait_for_commit(&follower, 8, Duration::from_secs(5));
        drop(follower);
    }

    let follower_raw = read_active_wal(&follower_dir.as_str());
    let (_follower_header, follower_tail) = split_header(&follower_raw);

    assert_eq!(
        follower_tail.len(),
        leader_tail.len(),
        "tail byte lengths diverged (leader={}, follower={})",
        leader_tail.len(),
        follower_tail.len()
    );
    assert_eq!(
        follower_tail, leader_tail,
        "follower tx tail must match leader tx tail byte-for-byte"
    );
}

#[test]
fn follower_handles_multiple_rpcs_in_sequence() {
    let leader_dir = TempDir::new("lf_multi_leader");
    let (_commit, leader_raw) = run_leader(&leader_dir.as_str(), 9, 10, 3);
    let (_leader_header, leader_tail) = split_header(&leader_raw);

    let split_offset = split_point(leader_tail, 6);

    let (first_half, second_half) = leader_tail.split_at(split_offset);

    let follower_dir = TempDir::new("lf_multi_follower");
    {
        let follower = start_ledger(config_for(&follower_dir.as_str(), true));
        let replication = Replication::new(follower.ledger_context(), 2, TEST_TERM);

        apply_ok(&replication, make_append(1, 5, first_half), 0);
        wait_for_commit(&follower, 5, Duration::from_secs(5));

        apply_ok(&replication, make_append(6, 10, second_half), 5);
        wait_for_commit(&follower, 10, Duration::from_secs(5));
        drop(follower);
    }

    let follower_raw = read_active_wal(&follower_dir.as_str());
    let (_, follower_tail) = split_header(&follower_raw);
    assert_eq!(
        follower_tail, leader_tail,
        "follower tail must match leader tail even when shipped in multiple RPCs"
    );
}

fn split_point(data: &[u8], target_tx_id: u64) -> usize {
    let mut off = 0;
    while off + 40 <= data.len() {
        if data[off] == WalEntryKind::TxMetadata as u8 {
            let m: TxMetadata = bytemuck::pod_read_unaligned(&data[off..off + 40]);
            if m.tx_id == target_tx_id {
                return off;
            }
        }
        off += 40;
    }
    panic!("tx_id {} not found in range", target_tx_id);
}
