//! Cluster-level fault injector (ADR-018).
//!
//! Compiled only with `--features fault-injection`. The injector
//! holds per-`FaultLevel` state and routes effects to the right
//! backing primitive:
//!
//! - `DISK_ACCESS` / `WAL_ACCESS` → [`ledger::fault::LedgerFaultInjector`]
//!   on the current `Ledger` via the `LedgerSlot`.
//! - All other buckets (`LEADER_PUSH`, `FOLLOWER_REPLICATION`,
//!   `MESSAGE`, `PARTITION`, `CONSENSUS_STATE`, `ELECTION_*`, `PING`)
//!   currently store their requested outcome in an in-memory map.
//!   The corresponding intercept hooks in `consensus/` and
//!   `handlers/` will read these slots once they're wired in
//!   follow-up PRs; for now they're observable via
//!   [`ClusterFaultInjector::snapshot`] (test-only helper).
//!
//! Stuck operations get an opaque `stuck_id` (caller-supplied or
//! auto-generated) that `UnstuckOperation` references.

use crate::ledger_slot::LedgerSlot;
use parking_lot::Mutex;
use proto::fault::{
    FaultLevel, FaultOutcome, Stuck, fault_level::Bucket, fault_outcome, write_corruption,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Owns the per-node fault state. Held as `Arc<ClusterFaultInjector>`
/// so the gRPC handler, the replication driver, and the consensus
/// loop can all read the same source of truth.
pub struct ClusterFaultInjector {
    ledger: Arc<LedgerSlot>,
    state: Mutex<State>,
    next_stuck_id: AtomicU64,
}

#[derive(Default)]
struct State {
    /// Active fault per level. Keys are `(bucket, peer_id)` —
    /// `peer_id` is ignored for node-wide buckets but kept in the
    /// key to make the table uniform.
    faults: HashMap<LevelKey, FaultOutcome>,
    /// `stuck_id` -> the level that parked it. Releasing routes
    /// back to whichever backing primitive owns the park.
    stuck: HashMap<String, LevelKey>,
    /// Clock offset (signed milliseconds) applied to wrapped
    /// `Instant::now()` reads. 0 = wall clock.
    clock_offset_ms: i64,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
struct LevelKey {
    bucket: Bucket,
    peer_id: u64,
}

impl LevelKey {
    fn from_level(level: &FaultLevel) -> Self {
        let bucket = Bucket::try_from(level.bucket).unwrap_or(Bucket::Unspecified);
        // Per-peer buckets keep `peer_id`; node-wide buckets are
        // normalised to 0 so the table doesn't grow per-caller.
        let peer_id = if bucket_uses_peer(bucket) {
            level.peer_id
        } else {
            0
        };
        Self { bucket, peer_id }
    }
}

fn bucket_uses_peer(b: Bucket) -> bool {
    matches!(
        b,
        Bucket::LeaderPush
            | Bucket::FollowerReplication
            | Bucket::Message
            | Bucket::Partition
            | Bucket::ElectionRequestVote
            | Bucket::ElectionHandshake
    )
}

impl ClusterFaultInjector {
    pub fn new(ledger: Arc<LedgerSlot>) -> Arc<Self> {
        Arc::new(Self {
            ledger,
            state: Mutex::new(State::default()),
            next_stuck_id: AtomicU64::new(1),
        })
    }

    /// Install or replace the fault on a single level. Returns the
    /// `assigned_stuck_id` when the outcome is `Stuck` with an empty
    /// caller-supplied id; otherwise empty.
    pub fn set_fault(
        &self,
        level: &FaultLevel,
        outcome: &FaultOutcome,
    ) -> Result<String, FaultError> {
        let key = LevelKey::from_level(level);
        validate_outcome(key.bucket, outcome)?;

        let assigned_id = self.apply_backing(key, outcome)?;
        let mut state = self.state.lock();
        state.faults.insert(key, outcome.clone());

        // Track stuck ids so UnstuckOperation can release them.
        if let Some(stuck_id) = stuck_id_of(outcome, &assigned_id) {
            state.stuck.insert(stuck_id, key);
        }
        Ok(assigned_id)
    }

    /// Remove the fault on a level. Returns true if there was one.
    pub fn clear_fault(&self, level: &FaultLevel) -> bool {
        let key = LevelKey::from_level(level);
        let mut state = self.state.lock();
        let had = state.faults.remove(&key).is_some();
        // Also release every parked op for this level — clearing a
        // rule semantically means "I'm done; release everyone".
        state.stuck.retain(|_, k| *k != key);
        drop(state);
        if had {
            self.release_backing(key);
        }
        had
    }

    /// Wipe every level. Returns `(cleared_levels, released_stuck_ops)`.
    pub fn clear_all(&self) -> (u32, u32) {
        let (rules, stuck) = {
            let mut state = self.state.lock();
            let r = state.faults.len() as u32;
            let s = state.stuck.len() as u32;
            state.faults.clear();
            state.stuck.clear();
            (r, s)
        };
        // Release every disk-axis park unconditionally — the ledger
        // injector's API is idempotent.
        if let Some(inj) = self.ledger_injector() {
            inj.unstuck_write();
            inj.unstuck_sync();
            inj.set_write_delay(None);
            inj.set_sync_delay(None);
        }
        (rules, stuck)
    }

    /// Release the operation parked under `stuck_id`. Returns true
    /// if the id was known.
    pub fn unstuck(&self, stuck_id: &str) -> bool {
        let key = {
            let mut state = self.state.lock();
            state.stuck.remove(stuck_id)
        };
        match key {
            Some(k) => {
                self.release_backing(k);
                true
            }
            None => false,
        }
    }

    /// Set the per-node clock offset.
    pub fn set_clock_skew(&self, offset_ms: i64) {
        self.state.lock().clock_offset_ms = offset_ms;
    }

    /// Current clock offset (for the wrapped `now()` read path).
    pub fn clock_offset(&self) -> i64 {
        self.state.lock().clock_offset_ms
    }

    /// Snapshot of installed faults — test helper. Production hooks
    /// don't need to copy the map; they look up the specific level
    /// they care about.
    pub fn snapshot(&self) -> Vec<(FaultLevel, FaultOutcome)> {
        self.state
            .lock()
            .faults
            .iter()
            .map(|(k, v)| {
                (
                    FaultLevel {
                        bucket: k.bucket as i32,
                        peer_id: k.peer_id,
                    },
                    v.clone(),
                )
            })
            .collect()
    }

    // ── internal: backing primitive routing ─────────────────────

    fn ledger_injector(&self) -> Option<Arc<ledger::fault::LedgerFaultInjector>> {
        Some(self.ledger.current().fault_injector())
    }

    /// Apply the outcome to whichever underlying primitive owns the
    /// effect. Returns the assigned stuck_id when the outcome is
    /// `Stuck` with an empty caller-supplied id.
    fn apply_backing(&self, key: LevelKey, outcome: &FaultOutcome) -> Result<String, FaultError> {
        let assigned_id = match outcome.kind.as_ref() {
            Some(fault_outcome::Kind::Stuck(Stuck { stuck_id })) if stuck_id.is_empty() => {
                let n = self.next_stuck_id.fetch_add(1, Ordering::AcqRel);
                format!("auto-{}-{}-{}", bucket_short(key.bucket), key.peer_id, n)
            }
            Some(fault_outcome::Kind::Stuck(Stuck { stuck_id })) => stuck_id.clone(),
            _ => String::new(),
        };

        // Route disk-affecting buckets to the LedgerFaultInjector.
        // Other buckets are stored in `state.faults` for now — the
        // consensus / replication intercept points will consult them
        // when those hooks land.
        if let Some(inj) = self.ledger_injector() {
            match (key.bucket, outcome.kind.as_ref()) {
                (Bucket::DiskAccess | Bucket::WalAccess, Some(fault_outcome::Kind::Stuck(_))) => {
                    if matches!(key.bucket, Bucket::WalAccess) {
                        inj.stick_write();
                    }
                    // DiskAccess covers term + vote which today share
                    // the WAL syncer (see ADR-018); park the sync
                    // axis to cover both.
                    inj.stick_sync();
                }
                (Bucket::WalAccess, Some(fault_outcome::Kind::Slow(s))) => {
                    inj.set_write_delay(Some(Duration::from_millis(s.delay_ms)));
                }
                (Bucket::DiskAccess, Some(fault_outcome::Kind::Slow(s))) => {
                    inj.set_sync_delay(Some(Duration::from_millis(s.delay_ms)));
                }
                (Bucket::WalAccess, Some(fault_outcome::Kind::Normal(_))) => {
                    // Mirror `Stuck`: WalAccess covers both axes.
                    inj.unstuck_write();
                    inj.unstuck_sync();
                    inj.set_write_delay(None);
                    inj.set_sync_delay(None);
                }
                (Bucket::DiskAccess, Some(fault_outcome::Kind::Normal(_))) => {
                    inj.unstuck_sync();
                    inj.set_sync_delay(None);
                }
                (Bucket::WalAccess | Bucket::DiskAccess, Some(fault_outcome::Kind::Error(_))) => {
                    // LedgerFaultInjector doesn't yet emit io::Error;
                    // tracked under ADR-018 "Open questions". Store
                    // the rule so a future PR can pick it up.
                    spdlog::warn!(
                        "fault: error injection requested on {:?} — \
                         stored but not yet enforced by LedgerFaultInjector",
                        key.bucket
                    );
                }
                (Bucket::WalAccess, Some(fault_outcome::Kind::Corruption(c))) => {
                    spdlog::warn!(
                        "fault: WriteCorruption requested ({:?}) — \
                         stored but not yet enforced",
                        c.kind
                    );
                }
                _ => {
                    // Non-disk bucket — state-only for now. Logged at
                    // debug so a curious operator can see what
                    // intercept points are still TODO.
                    spdlog::debug!(
                        "fault: {:?} (peer={}) installed but no live \
                         intercept hook is wired yet — fault state is \
                         observable via ClusterFaultInjector::snapshot",
                        key.bucket,
                        key.peer_id
                    );
                }
            }
        }
        Ok(assigned_id)
    }

    /// Release the backing primitive for a level (called on
    /// ClearFault and UnstuckOperation). Must mirror `apply_backing`:
    /// `WalAccess` parks both write AND sync, so releasing it has to
    /// wake both — otherwise the WAL committer sticks on sync forever.
    fn release_backing(&self, key: LevelKey) {
        if let Some(inj) = self.ledger_injector() {
            match key.bucket {
                Bucket::WalAccess => {
                    inj.unstuck_write();
                    inj.unstuck_sync();
                    inj.set_write_delay(None);
                    inj.set_sync_delay(None);
                }
                Bucket::DiskAccess => {
                    inj.unstuck_sync();
                    inj.set_sync_delay(None);
                }
                _ => {}
            }
        }
    }
}

fn stuck_id_of(outcome: &FaultOutcome, assigned: &str) -> Option<String> {
    match outcome.kind.as_ref()? {
        fault_outcome::Kind::Stuck(Stuck { stuck_id }) if !stuck_id.is_empty() => {
            Some(stuck_id.clone())
        }
        fault_outcome::Kind::Stuck(_) => Some(assigned.to_string()),
        _ => None,
    }
}

fn bucket_short(b: Bucket) -> &'static str {
    match b {
        Bucket::Unspecified => "unspec",
        Bucket::DiskAccess => "disk",
        Bucket::WalAccess => "wal",
        Bucket::LeaderPush => "lpush",
        Bucket::FollowerReplication => "frep",
        Bucket::Message => "msg",
        Bucket::Partition => "part",
        Bucket::ConsensusState => "cstate",
        Bucket::ElectionRequestVote => "elec-vote",
        Bucket::ElectionHandshake => "elec-hs",
        Bucket::Ping => "ping",
    }
}

/// Reject (bucket, outcome) combinations that don't make sense —
/// the proto schema can't enforce this so the injector does.
fn validate_outcome(bucket: Bucket, outcome: &FaultOutcome) -> Result<(), FaultError> {
    use fault_outcome::Kind;
    let Some(kind) = outcome.kind.as_ref() else {
        // No outcome set = treat as a clear-style no-op; let it
        // through and the apply_backing call below records the
        // empty state.
        return Ok(());
    };

    let ok = match kind {
        Kind::Normal(_) | Kind::Error(_) | Kind::Slow(_) | Kind::Stuck(_) => true,
        Kind::Corruption(_) => matches!(bucket, Bucket::WalAccess),
        Kind::Message(_) => matches!(
            bucket,
            Bucket::Message | Bucket::ElectionRequestVote | Bucket::ElectionHandshake
        ),
        Kind::Consensus(_) => matches!(bucket, Bucket::ConsensusState),
        Kind::Election(_) => matches!(
            bucket,
            Bucket::ElectionRequestVote | Bucket::ElectionHandshake
        ),
        Kind::Partition(_) => matches!(bucket, Bucket::Partition),
        Kind::Ping(_) => matches!(bucket, Bucket::Ping),
    };
    if ok {
        Ok(())
    } else {
        Err(FaultError::InvalidOutcome(bucket, kind_name(kind)))
    }
}

fn kind_name(k: &fault_outcome::Kind) -> &'static str {
    use fault_outcome::Kind;
    match k {
        Kind::Normal(_) => "Normal",
        Kind::Error(_) => "Error",
        Kind::Slow(_) => "Slow",
        Kind::Stuck(_) => "Stuck",
        Kind::Corruption(_) => "WriteCorruption",
        Kind::Message(_) => "Message",
        Kind::Consensus(_) => "Consensus",
        Kind::Election(_) => "Election",
        Kind::Partition(_) => "Partition",
        Kind::Ping(_) => "Ping",
    }
}

/// Errors callers (the gRPC handler) translate to `Status`.
#[derive(Debug)]
pub enum FaultError {
    InvalidOutcome(Bucket, &'static str),
    UnknownBucket,
    FileEventFailed(String),
}

impl std::fmt::Display for FaultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FaultError::InvalidOutcome(b, k) => {
                write!(f, "outcome `{}` not valid on bucket `{:?}`", k, b)
            }
            FaultError::UnknownBucket => write!(f, "FaultLevel.bucket is UNSPECIFIED"),
            FaultError::FileEventFailed(e) => write!(f, "file event failed: {}", e),
        }
    }
}

// Silence the unused-import lint when no corruption variant is
// matched — kept here so future PRs that wire WriteCorruption don't
// have to re-add the import.
#[allow(dead_code)]
fn _force_corruption_use(_w: write_corruption::Kind) {}
