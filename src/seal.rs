use crate::config::LedgerConfig;
use crate::entities::WalEntry;
use crate::pipeline::SealContext;
use crate::storage::FunctionSnapshotRecord;
use crate::storage::SegmentStaus::SEALED;
use crate::storage::{Segment, Storage};
use rustc_hash::FxHashMap;
use spdlog::{debug, error, warn};
use std::sync::Arc;
use std::thread::{JoinHandle, sleep};
use std::time::Duration;

pub struct Seal {
    runner: Option<SealRunner>, // will be taken out when start() is called
}

struct SealRunner {
    storage: Arc<Storage>,
    balances: Vec<i64>,
    /// Mirror of the WASM function registry observed through sealed
    /// segments. Same lifecycle as `balances`: updated inline as
    /// `FunctionRegistered` WAL records pass by, snapshotted alongside
    /// the balance snapshot at `snapshot_frequency`.
    ///
    /// Keyed by name → (version, crc32c). `crc32c == 0` means the
    /// function was unregistered; kept in the map so the snapshot
    /// preserves the audit trail.
    functions: FxHashMap<String, (u16, u32)>,
    seal_check_internal: Duration,
}

impl Seal {
    pub fn new(config: &LedgerConfig, storage: Arc<Storage>) -> Self {
        Self {
            runner: Some(SealRunner {
                storage,
                balances: vec![0; config.max_accounts],
                functions: FxHashMap::default(),
                seal_check_internal: config.seal_check_internal,
            }),
        }
    }

    pub fn start(&mut self, ctx: SealContext) -> std::io::Result<JoinHandle<()>> {
        if let Some(runner) = self.runner.take() {
            std::thread::Builder::new()
                .name("seal".to_string())
                .spawn(move || {
                    let mut r = runner;
                    r.run(ctx);
                })
        } else {
            Err(std::io::Error::other("Seal already started"))
        }
    }

    /// Pre-seal a segment during recovery. Returns the sealed segment id so the
    /// caller can publish it to the pipeline's seal index.
    pub fn recover_pre_seal(&mut self, segment: &mut Segment) -> std::io::Result<u32> {
        if let Some(mut runner) = self.runner.take() {
            let id = runner.process_seal(segment)?;
            self.runner = Some(runner);
            Ok(id)
        } else {
            Err(std::io::Error::other("Seal already started"))
        }
    }

    pub(crate) fn recover_balance(
        &mut self,
        account_id: usize,
        computed_balance: i64,
    ) -> std::io::Result<()> {
        if let Some(mut runner) = self.runner.take() {
            runner.balances[account_id] = computed_balance;
            self.runner = Some(runner);
            Ok(())
        } else {
            Err(std::io::Error::other("Seal already started"))
        }
    }

    /// Seed the Seal stage's function tracker during recovery so the
    /// next emitted function snapshot reflects state from before the
    /// last restart. Called once per record loaded from the function
    /// snapshot (via `Recover`), before WAL replay adds any tail records.
    pub(crate) fn recover_function(
        &mut self,
        name: String,
        version: u16,
        crc32c: u32,
    ) -> std::io::Result<()> {
        if let Some(mut runner) = self.runner.take() {
            runner.functions.insert(name, (version, crc32c));
            self.runner = Some(runner);
            Ok(())
        } else {
            Err(std::io::Error::other("Seal already started"))
        }
    }
}

impl SealRunner {
    fn run(&mut self, ctx: SealContext) {
        loop {
            // A long time nothing happened.
            if let Err(e) = self.seal_pending_segments(&ctx) {
                error!("Seal: failed to seal pending segments: {}", e);
                sleep(Duration::from_secs(1));
            }
            // Check before sleep to ensure that the last seal is done before shutting down
            if !ctx.is_running() {
                break;
            }
            sleep(self.seal_check_internal);
        }
    }

    fn seal_pending_segments(&mut self, ctx: &SealContext) -> std::io::Result<()> {
        let mut pending = self.storage.list_all_segments()?;
        for segment in pending.iter_mut() {
            if segment.status() == SEALED {
                continue;
            }
            let id = self.process_seal(segment)?;
            ctx.set_processed_index(id);
        }

        Ok(())
    }

    /// Seal `segment` and update the runner's balance buffer. Returns the
    /// sealed segment id; the caller is responsible for publishing it to the
    /// pipeline (either through `SealContext` or directly during recovery).
    pub fn process_seal(&mut self, segment: &mut Segment) -> std::io::Result<u32> {
        segment.load()?;
        segment.seal()?;

        // Build on-disk transaction and account index files (ADR-008).
        if let Err(e) = segment.build_indexes() {
            error!(
                "Seal: failed to build indexes for segment {}: {}",
                segment.id(),
                e
            );
        }

        // only the last seal can be taken snapshot
        // Load wal records and update both balances and the function
        // registry mirror from this segment's WAL.
        let mut seg_last_tx_id: u64 = 0;
        segment.visit_wal_records(|entry| match entry {
            WalEntry::Entry(e) => {
                let id = e.account_id as usize;
                if id < self.balances.len() {
                    self.balances[id] = e.computed_balance;
                } else {
                    warn!("Seal: account ID {} exceeds balance vector length", id);
                }
            }
            WalEntry::Metadata(m) => {
                if m.tx_id > seg_last_tx_id {
                    seg_last_tx_id = m.tx_id;
                }
            }
            // Mirror the WASM function registry through the sealed WAL.
            // Unregister records (crc32c == 0) are kept in the map so
            // the snapshot preserves the audit trail.
            WalEntry::FunctionRegistered(f) => {
                self.functions
                    .insert(f.name_str().to_string(), (f.version, f.crc32c));
            }
            _ => {}
        })?;

        // Conditionally write snapshots (balance + function)
        let snapshot_frequency = self.storage.config().snapshot_frequency;
        if snapshot_frequency > 0 && segment.id().is_multiple_of(snapshot_frequency) {
            let mut snapshot_records: Vec<(u64, i64)> = self
                .balances
                .iter()
                .enumerate()
                .filter_map(|(id, &bal)| {
                    if bal != 0 {
                        Some((id as u64, bal))
                    } else {
                        None
                    }
                })
                .collect();
            snapshot_records.sort_unstable_by_key(|(id, _)| *id);

            debug!("Seal: saving snapshot for WAL segment {}", segment.id());
            if let Err(e) = segment.save_snapshot(&snapshot_records[..]) {
                error!(
                    "Seal: failed to save snapshot for segment {}: {}",
                    segment.id(),
                    e
                );
            }

            // Function snapshot piggybacks on the same trigger. Written
            // unconditionally — even when the registry is empty — so
            // recovery can always jump straight to the latest snapshot
            // boundary instead of replaying WAL from segment 1.
            let mut fn_records: Vec<FunctionSnapshotRecord> = self
                .functions
                .iter()
                .map(|(name, (version, crc32c))| {
                    FunctionSnapshotRecord::new(name, *version, *crc32c)
                })
                .collect();
            // Deterministic ordering for reproducibility + diffing.
            fn_records.sort_unstable_by(|a, b| a.name_str().cmp(b.name_str()));

            debug!(
                "Seal: saving function snapshot for WAL segment {} ({} records)",
                segment.id(),
                fn_records.len()
            );
            if let Err(e) =
                self.storage
                    .save_function_snapshot(segment.id(), seg_last_tx_id, &fn_records)
            {
                error!(
                    "Seal: failed to save function snapshot for segment {}: {}",
                    segment.id(),
                    e
                );
            }
        } else if snapshot_frequency > 0 {
            debug!(
                "Seal: skipping snapshot for segment {} (frequency={})",
                segment.id(),
                snapshot_frequency
            );
        }

        Ok(segment.id())
    }
}
