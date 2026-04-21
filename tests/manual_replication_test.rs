//! Manual leader → follower replication using `WalTailer` on the leader and
//! `append_wal_entries` on the follower (ADR-015 smoke test).

use roda_ledger::entities::SYSTEM_ACCOUNT_ID;
use roda_ledger::ledger::{Ledger, LedgerConfig, StorageConfig};
use roda_ledger::transaction::{Operation, WaitLevel};
use roda_ledger::wait_strategy::WaitStrategy;
use roda_ledger::wal_tail::decode_records;
use spdlog::Level;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

const BATCHES: u64 = 20;
const BATCH_SIZE: u64 = 5_000;
const ACCOUNT: u64 = 1;
const DEPOSIT_AMOUNT: u64 = 10;

/// Build a ledger rooted under `temp_{name}_{nanos}/`. `name` makes the
/// leader/follower directories human-readable in the temp dir.
fn named_ledger(name: &str, tx_per_segment: u64) -> Ledger {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut dir = std::env::current_dir().unwrap();
    dir.push(format!("temp_{}_{}", name, nanos));

    let cfg = LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.to_string_lossy().into_owned(),
            temporary: true,
            transaction_count_per_segment: tx_per_segment,
            snapshot_frequency: 2,
        },
        wait_strategy: WaitStrategy::Balanced,
        log_level: Level::Critical,
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::default()
    };
    let mut l = Ledger::new(cfg);
    l.start().expect("start");
    l
}

fn wait_for<F: FnMut() -> bool>(label: &str, timeout: Duration, mut f: F) {
    let deadline = Instant::now() + timeout;
    while !f() {
        if Instant::now() > deadline {
            panic!("timeout: {label}");
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn manual_replication_leader_to_follower() {
    // 1. Two ledgers, follower rotates every 20_000 transactions.
    let leader = named_ledger("leader", 10_000_000);
    let follower = named_ledger("follower", 20_000);

    // Share follower across threads; main thread only verifies, replication
    // thread drives append_wal_entries.
    let follower = Arc::new(Mutex::new(follower));
    let running = Arc::new(AtomicBool::new(true));

    // 2 + 3. Background replication: WalTailer on leader → decode → follower.
    let mut tailer = leader.wal_tailer();
    let fol_repl = follower.clone();
    let run = running.clone();
    let repl = thread::spawn(move || {
        // 4096 records (~160 KB) per read keeps the tailer cursor active
        // and amortises lock acquisitions on the follower mutex.
        let mut buf = vec![0u8; 40 * 4096];
        let mut idle_retries = 0u32;
        while run.load(Ordering::Relaxed) {
            let n = tailer.tail(1, &mut buf) as usize;
            if n == 0 {
                idle_retries = idle_retries.saturating_add(1);
                let sleep_us = (idle_retries.min(100) as u64) * 50;
                thread::sleep(Duration::from_micros(sleep_us));
                continue;
            }
            idle_retries = 0;
            let entries = decode_records(&buf[..n]);
            if !entries.is_empty() {
                fol_repl
                    .lock()
                    .unwrap()
                    .append_wal_entries(entries)
                    .expect("follower append");
            }
        }
        // Final drain once shutdown is requested so we don't lose records
        // committed between the last iteration and the flag flip.
        loop {
            let n = tailer.tail(1, &mut buf) as usize;
            if n == 0 {
                break;
            }
            let entries = decode_records(&buf[..n]);
            if !entries.is_empty() {
                fol_repl
                    .lock()
                    .unwrap()
                    .append_wal_entries(entries)
                    .expect("follower append (drain)");
            }
        }
    });

    // 4. Submit 20 portions of 5_000 deposits to the leader.
    let total_tx = BATCHES * BATCH_SIZE;
    let mut last_leader_tx = 0u64;
    for _ in 0..BATCHES {
        let ops: Vec<Operation> = (0..BATCH_SIZE)
            .map(|_| Operation::Deposit {
                account: ACCOUNT,
                amount: DEPOSIT_AMOUNT,
                user_ref: 0,
            })
            .collect();
        let results = leader.submit_batch_and_wait(ops, WaitLevel::Committed);
        last_leader_tx = results.last().unwrap().tx_id;
    }
    assert_eq!(last_leader_tx, total_tx);
    assert_eq!(leader.last_commit_id(), total_tx);

    // 5. Wait for follower commit + snapshot indexes to catch up.
    wait_for(
        "follower commit catches up",
        Duration::from_secs(60),
        || follower.lock().unwrap().last_commit_id() >= last_leader_tx,
    );
    wait_for(
        "follower snapshot catches up",
        Duration::from_secs(60),
        || follower.lock().unwrap().last_snapshot_id() >= last_leader_tx,
    );

    running.store(false, Ordering::Relaxed);
    repl.join().expect("repl thread");

    // ── verification ─────────────────────────────────────────────────────
    let fol = follower.lock().unwrap();
    assert_eq!(
        fol.last_commit_id(),
        leader.last_commit_id(),
        "commit index mismatch"
    );
    assert_eq!(
        fol.last_snapshot_id(),
        leader.last_snapshot_id(),
        "snapshot index mismatch"
    );

    // Deposit debits the user account and credits SYSTEM_ACCOUNT in this
    // ledger's accounting model. Balances must match on both sides.
    let leader_user = leader.get_balance(ACCOUNT);
    let fol_user = fol.get_balance(ACCOUNT);
    let leader_sys = leader.get_balance(SYSTEM_ACCOUNT_ID);
    let fol_sys = fol.get_balance(SYSTEM_ACCOUNT_ID);

    assert_eq!(leader_user, fol_user, "user account balance divergence");
    assert_eq!(leader_sys, fol_sys, "system account balance divergence");

    // Sanity: amounts are what we expect for N sequential deposits.
    let expected_abs = (total_tx * DEPOSIT_AMOUNT) as i64;
    assert_eq!(leader_user.abs(), expected_abs, "leader balance wrong");
    assert_eq!(leader_user + leader_sys, 0, "double-entry must sum to zero");
}
