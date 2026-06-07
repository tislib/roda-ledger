//! Backpressure tests using the `LedgerFaultInjector`. See ADR-018.
//!
//! Backpressure comes from the **disk writer** (ADR-021): the WAL frees a ring slot
//! only after it has *written* the batch to the segment, so a stalled/slow writer
//! holds the ring and back-pressures the transactor. `fdatasync` is deliberately
//! NOT a flow-control point — release happens before commit, so a stalled fsync
//! does not directly cap submission (the page cache + OS write-back absorb the
//! write-vs-sync gap). These tests therefore exercise only the write path.
//!
//! Each test parks the WAL write (stalled or slowed), drives a high-rate submitter,
//! and asserts two things:
//!
//! 1. **In-flight cap** — `submitted - last_snapshot_id` stays under `MAX_INFLIGHT`.
//!    Without backpressure the submitter would race to `SUBMIT_TOTAL` while the
//!    snapshot stage is wedged at 0.
//!
//! 2. **Submitter does not move** — over a second window (`OBSERVE`) the delta in
//!    `submitted` stays under `MAX_PROGRESS_IN_OBSERVE` — far below the unthrottled
//!    submitter's millions of ops/sec, proving `submit()` is held back by the writer.
//!
//! Pipeline slack is dominated by the inter-stage buffers: the sequencer→transactor
//! queue (`1024`, pipeline.rs) plus the transactor's batch buffer (same capacity),
//! ~`2k` entries, well under the runaway an un-back-pressured pipeline would reach.
//!
//! Run with `--features fault-injection`.

#![cfg(feature = "fault-injection")]

use ledger::fault::LedgerFaultInjector;
use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::Operation;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use storage::StorageConfig;

const RING_SIZE: usize = 16;
const SUBMIT_TOTAL: u64 = 100_000;
/// Bound on `submitted - snapshot`. Slack is the seq→transactor queue (1024) plus
/// the transactor's batch buffer (same capacity) ≈ 2k entries; `3_000` is a
/// comfortable margin for scheduling jitter while still rejecting an OOM-class
/// runaway (which would race toward `SUBMIT_TOTAL`).
const MAX_INFLIGHT: u64 = 3_000;
/// First wait: let the fault soak so the pipeline settles into its
/// steady-state. Picked long enough that an unthrottled pipeline
/// would have buffered ≫ `MAX_INFLIGHT` by now.
const SOAK: Duration = Duration::from_millis(150);
/// Second wait: sample submitter progress *after* the soak. We assert
/// the delta over this window stays under `MAX_PROGRESS_IN_OBSERVE`.
const OBSERVE: Duration = Duration::from_millis(100);
/// Slow-mode per-call delay — big enough to stall throughput well
/// below the unblocked submitter, small enough that release-and-drain
/// stays under the test timeout.
const SLOW: Duration = Duration::from_millis(20);

/// Upper bound on submitter progress during the `OBSERVE` window.
///
/// For *stall* faults: snapshot can't advance at all, so the
/// submitter is stuck on a full seq→trans queue — progress should be
/// roughly zero. For *slow* faults: the throttled write or sync
/// admits a small steady-state throughput; with `SLOW = 20ms` and
/// `OBSERVE = 100ms` that's at most ~5 batches of pipeline-slack
/// size, well under this cap. `5_000` is the same order of magnitude
/// as `MAX_INFLIGHT` and orders of magnitude below the ~10⁶ ops/sec
/// an unblocked submitter would achieve.
const MAX_PROGRESS_IN_OBSERVE: u64 = 5_000;

/// RAII guard that releases every fault axis on drop — keeps a
/// panicking test from wedging the WAL threads (and the ensuing
/// `Ledger::drop` join) on a condvar or a long sleep.
struct FaultGuard(Arc<LedgerFaultInjector>);
impl Drop for FaultGuard {
    fn drop(&mut self) {
        self.0.set_write_delay(None);
        self.0.set_sync_delay(None);
        self.0.unstuck_write();
        self.0.unstuck_sync();
    }
}

fn make_config(dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.to_string(),
            temporary: true,
            transaction_count_per_segment: 1_000_000,
            snapshot_frequency: u32::MAX,
        },
        ring_size: RING_SIZE,
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
    format!("temp_bp_{}_{}", name, nanos)
}

/// Drive a submitter under the supplied fault and assert the
/// backpressure invariants. `inject` runs after `start()`; `release`
/// runs after the assertions so cleanup mirrors a real "disk
/// recovered" handoff.
fn run_backpressure_scenario(
    name: &str,
    inject: impl FnOnce(&LedgerFaultInjector),
    release: impl FnOnce(&LedgerFaultInjector),
) {
    let dir = unique_dir(name);
    let mut ledger = Ledger::new(make_config(&dir));
    let fault = ledger.fault_injector();
    let _guard = FaultGuard(fault.clone());

    inject(&fault);
    ledger.start().unwrap();

    let ledger = Arc::new(ledger);
    let submitted = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let h = {
        let ledger = ledger.clone();
        let submitted = submitted.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            for _ in 0..SUBMIT_TOTAL {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                ledger.submit(Operation::Deposit {
                    account: 7,
                    amount: 1,
                    user_ref: 0,
                });
                submitted.fetch_add(1, Ordering::Release);
            }
        })
    };

    // Soak: let the pipeline settle into its steady-state under fault.
    thread::sleep(SOAK);

    // First sample — bound on overall in-flight from submitter's view.
    let submitted_a = submitted.load(Ordering::Acquire);
    let snap_a = ledger.last_snapshot_id();
    let in_flight_a = submitted_a.saturating_sub(snap_a);
    assert!(
        in_flight_a <= MAX_INFLIGHT,
        "[{name}] in-flight (submitted - snapshot) not bounded: \
         {in_flight_a} > {MAX_INFLIGHT} \
         (submitted={submitted_a}, snapshot={snap_a})",
    );
    assert!(
        submitted_a < SUBMIT_TOTAL,
        "[{name}] submitter completed all {SUBMIT_TOTAL} submits under fault — \
         backpressure missing (submitted={submitted_a})",
    );

    // Second sample — bound on submitter throughput during fault.
    thread::sleep(OBSERVE);
    let submitted_b = submitted.load(Ordering::Acquire);
    let progress = submitted_b - submitted_a;
    assert!(
        progress <= MAX_PROGRESS_IN_OBSERVE,
        "[{name}] submitter advanced {progress} ops in {:?} under fault — \
         submission is not being held back (cap={MAX_PROGRESS_IN_OBSERVE})",
        OBSERVE,
    );

    // Tear down — stop the submitter early so cleanup doesn't have to
    // process the remaining ~100k entries.
    stop.store(true, Ordering::Release);
    release(&fault);

    let join_deadline = Instant::now() + Duration::from_secs(5);
    while !h.is_finished() && Instant::now() < join_deadline {
        thread::sleep(Duration::from_millis(5));
    }
    h.join().expect("submit thread joined");
}

/// `before_write` parks on a condvar — `write_all` on the WAL file never completes.
/// The WAL holds the ring (release happens only after the write), the upstream
/// queues saturate, and `submit()` parks in its spin/yield retry.
#[test]
fn write_stall_caps_submission() {
    run_backpressure_scenario("write_stall", |f| f.stick_write(), |f| f.unstuck_write());
}

/// `before_write` sleeps for `SLOW` per call — the WAL writer makes
/// slow forward progress; sync follows it. In-flight reaches a
/// steady cap; submitter throughput is throttled.
#[test]
fn write_slow_caps_submission() {
    run_backpressure_scenario(
        "write_slow",
        |f| f.set_write_delay(Some(SLOW)),
        |f| f.set_write_delay(None),
    );
}
