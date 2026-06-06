//! Ledger fault injector — see ADR-018.
//!
//! Compiled only with the `fault-injection` feature. The injector is
//! installed on the `Storage`'s WAL-fault slot at `Ledger::new`; both
//! `Segment::write_pending_entries` and the WAL committer's
//! `Syncer::sync` consult it before touching disk.
//!
//! v1 surface: stall (park on condvar) and slow (sleep) on both the
//! write and sync axes. Error / corruption axes from ADR-018 land in
//! later PRs.

use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::time::Duration;
use storage::fault::WalFault;

/// Per-axis fault state. Encodes the (stuck, delay) pair both
/// `before_write` and `before_sync` consult.
#[derive(Default)]
struct AxisState {
    stuck: bool,
    delay: Option<Duration>,
}

/// Single-knob fault injector for the ledger's WAL IO. Constructed
/// inside `Ledger::new`; exposed to tests via `Ledger::fault_injector`.
///
/// One injector handles both write and sync axes — write faults park
/// the WAL writer thread, sync faults park the WAL committer thread.
/// They are independently controllable so a test can isolate which
/// pipeline stage the backpressure originates from.
pub struct LedgerFaultInjector {
    write: Mutex<AxisState>,
    write_cv: Condvar,
    sync: Mutex<AxisState>,
    sync_cv: Condvar,
}

impl LedgerFaultInjector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    // ── Sync axis ────────────────────────────────────────────────────

    /// Park subsequent `fdatasync` calls until `unstuck_sync` is
    /// invoked. Idempotent.
    pub fn stick_sync(&self) {
        self.sync.lock().stuck = true;
    }

    /// Release any thread parked in `before_sync` and let future
    /// syncs through. Idempotent.
    pub fn unstuck_sync(&self) {
        self.sync.lock().stuck = false;
        self.sync_cv.notify_all();
    }

    /// Insert a per-call sleep on every `fdatasync`. Pass `None` to
    /// clear. The sleep is read on every call so tests can ramp it
    /// without rebuilding the injector.
    pub fn set_sync_delay(&self, delay: Option<Duration>) {
        self.sync.lock().delay = delay;
    }

    // ── Write axis ───────────────────────────────────────────────────

    /// Park subsequent `write_all` calls until `unstuck_write` is
    /// invoked. Idempotent.
    pub fn stick_write(&self) {
        self.write.lock().stuck = true;
    }

    /// Release any thread parked in `before_write` and let future
    /// writes through. Idempotent.
    pub fn unstuck_write(&self) {
        self.write.lock().stuck = false;
        self.write_cv.notify_all();
    }

    /// Insert a per-call sleep on every `write_all`. Pass `None` to
    /// clear.
    pub fn set_write_delay(&self, delay: Option<Duration>) {
        self.write.lock().delay = delay;
    }
}

impl Default for LedgerFaultInjector {
    fn default() -> Self {
        Self {
            write: Mutex::new(AxisState::default()),
            write_cv: Condvar::new(),
            sync: Mutex::new(AxisState::default()),
            sync_cv: Condvar::new(),
        }
    }
}

impl WalFault for LedgerFaultInjector {
    fn before_write(&self, _buf_len: usize) {
        let mut guard = self.write.lock();
        while guard.stuck {
            self.write_cv.wait(&mut guard);
        }
        let delay = guard.delay;
        drop(guard);
        if let Some(d) = delay {
            std::thread::sleep(d);
        }
    }

    fn before_sync(&self) {
        let mut guard = self.sync.lock();
        while guard.stuck {
            self.sync_cv.wait(&mut guard);
        }
        let delay = guard.delay;
        drop(guard);
        if let Some(d) = delay {
            std::thread::sleep(d);
        }
    }
}
