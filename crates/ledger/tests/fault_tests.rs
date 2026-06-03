//! Surface tests for the `fault-injection`-gated `LedgerFaultInjector`.
//! See ADR-018. Run with `--features fault-injection`.
//!
//! Multi-axis backpressure scenarios live in `backpressure_test.rs`.

#![cfg(feature = "fault-injection")]

use ledger::fault::LedgerFaultInjector;
use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::Operation;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use storage::StorageConfig;

/// RAII guard that releases all stall axes on drop — keeps a
/// panicking test from wedging WAL threads (and the ensuing
/// `Ledger::drop` join) on a condvar.
struct FaultGuard(Arc<LedgerFaultInjector>);
impl Drop for FaultGuard {
    fn drop(&mut self) {
        self.0.unstuck_sync();
        self.0.unstuck_write();
        self.0.set_sync_delay(None);
        self.0.set_write_delay(None);
    }
}

fn make_config(dir: &str, queue_size: usize) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.to_string(),
            temporary: true,
            transaction_count_per_segment: 100_000,
            snapshot_frequency: u32::MAX,
        },
        queue_size,
        seal_check_internal: Duration::from_millis(10),
        disable_seal: true,
        ..Default::default()
    }
}

fn unique_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temp_fault_{}_{}", name, nanos)
}

/// Stuck `fdatasync` keeps `last_commit_id` / `last_snapshot_id` at
/// zero and leaves the account balance invisible to queries. Once
/// released, both indices advance and the balance materialises.
#[test]
fn stuck_sync_blocks_commit_and_snapshot() {
    let dir = unique_dir("stuck_commit");
    let mut ledger = Ledger::new(make_config(&dir, 1 << 14));
    let fault = ledger.fault_injector();
    let _guard = FaultGuard(fault.clone());

    // Stick BEFORE start so the first sync the committer attempts parks.
    fault.stick_sync();
    ledger.start().unwrap();

    let last_id = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });

    // Compute can finish (Transactor doesn't sync), but commit cannot.
    let deadline = Instant::now() + Duration::from_millis(500);
    while ledger.last_compute_id() < last_id && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        ledger.last_compute_id(),
        last_id,
        "transactor should compute even while sync is stuck"
    );

    // Give the WAL committer plenty of time to attempt a sync (and park).
    thread::sleep(Duration::from_millis(50));

    assert_eq!(
        ledger.last_commit_id(),
        0,
        "commit_id must not advance while fdatasync is parked"
    );
    assert_eq!(
        ledger.last_snapshot_id(),
        0,
        "snapshot_id must not advance while commit is stuck"
    );
    assert_eq!(
        ledger.get_balance(1),
        0,
        "balance must be invisible while snapshot is stuck"
    );

    // Release the sync — everything downstream catches up.
    fault.unstuck_sync();

    let deadline = Instant::now() + Duration::from_secs(2);
    while ledger.last_snapshot_id() < last_id && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        ledger.last_snapshot_id(),
        last_id,
        "snapshot_id must advance after unstuck"
    );
    assert_eq!(ledger.get_balance(1), 100);
}

/// Stuck `write_all` keeps `last_commit_id` / `last_snapshot_id` at
/// zero — same end-state as `stuck_sync_blocks_commit_and_snapshot`
/// but the stall originates one stage earlier (the WAL writer rather
/// than the committer). The point is that a *no-error* failure mode
/// still freezes commit advancement; nothing in the pipeline observes
/// the missing write.
#[test]
fn stuck_write_blocks_commit_and_snapshot() {
    let dir = unique_dir("stuck_write");
    let mut ledger = Ledger::new(make_config(&dir, 1 << 14));
    let fault = ledger.fault_injector();
    let _guard = FaultGuard(fault.clone());

    // Stick BEFORE start so the first `write_all` the WAL writer
    // attempts parks instead of returning. No error surfaces — the
    // call never returns at all.
    fault.stick_write();
    ledger.start().unwrap();

    let last_id = ledger.submit(Operation::Deposit {
        account: 2,
        amount: 50,
        user_ref: 0,
    });

    // With the write stalled, compute can still complete (transactor
    // doesn't write) but the WAL writer's `write_all` never returns,
    // so `last_written_tx_id` stays at zero and commit can't follow.
    let deadline = Instant::now() + Duration::from_millis(500);
    while ledger.last_compute_id() < last_id && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        ledger.last_compute_id(),
        last_id,
        "transactor should compute even while write is stuck"
    );

    // Soak so the WAL writer has had every chance to attempt a write.
    thread::sleep(Duration::from_millis(50));

    assert_eq!(
        ledger.last_commit_id(),
        0,
        "commit_id must not advance while write_all is parked"
    );
    assert_eq!(
        ledger.last_snapshot_id(),
        0,
        "snapshot_id must not advance while commit is frozen"
    );
    assert_eq!(
        ledger.get_balance(2),
        0,
        "balance must be invisible while snapshot is frozen"
    );

    // Release — everything downstream catches up.
    fault.unstuck_write();

    let deadline = Instant::now() + Duration::from_secs(2);
    while ledger.last_snapshot_id() < last_id && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        ledger.last_snapshot_id(),
        last_id,
        "snapshot_id must advance after unstuck"
    );
    assert_eq!(ledger.get_balance(2), 50);
}

/// While `fdatasync` is parked, the pipeline queues fill and `submit`
/// blocks. A background submitter cannot make unbounded progress; once
/// the fault is released, it completes.
#[test]
fn stuck_sync_creates_submit_backpressure() {
    let dir = unique_dir("stuck_backpressure");
    // Small queues so the pipeline fills after a few hundred submits.
    // Downstream slack the submitter accumulates before blocking is
    // roughly: seq→trans (queue_size) + wal_input (queue_size) +
    // WAL writer VecDeque (initial capacity = queue_size * 16).
    // For queue_size = 16 that's ~288; pick `total` comfortably above
    // so backpressure is unambiguous but cleanup stays cheap.
    let queue_size = 16usize;
    let total: u64 = 100_000;

    let mut ledger = Ledger::new(make_config(&dir, queue_size));
    let fault = ledger.fault_injector();
    let _guard = FaultGuard(fault.clone());
    fault.stick_sync();
    ledger.start().unwrap();

    let ledger = Arc::new(ledger);
    let submitted = Arc::new(AtomicU64::new(0));

    // Background submitter: pushes `total` deposits, blocks on backpressure.
    let h = {
        let ledger = ledger.clone();
        let submitted = submitted.clone();
        thread::spawn(move || {
            for _ in 0..total {
                ledger.submit(Operation::Deposit {
                    account: 7,
                    amount: 1,
                    user_ref: 0,
                });
                submitted.fetch_add(1, Ordering::Release);
            }
        })
    };

    // Wait long enough for the queues to saturate.
    thread::sleep(Duration::from_millis(100));
    let stuck_at = submitted.load(Ordering::Acquire);
    assert!(
        stuck_at < total,
        "submit thread should be blocked on backpressure, but completed all \
         {total} submits (queue_size={queue_size})"
    );
    assert!(!h.is_finished(), "submit thread must still be parked");

    // Confirm no transaction has committed while sync is stuck.
    assert_eq!(
        ledger.last_commit_id(),
        0,
        "no commit can land while sync is stuck"
    );

    // Release: submitter drains and the pipeline progresses.
    fault.unstuck_sync();

    h.join().expect("submit thread joined");
    assert_eq!(submitted.load(Ordering::Acquire), total);

    let deadline = Instant::now() + Duration::from_secs(10);
    while ledger.last_snapshot_id() < total && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(ledger.last_snapshot_id(), total);
    assert_eq!(ledger.get_balance(7), total as i64);
}
