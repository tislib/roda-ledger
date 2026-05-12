use crate::config::LedgerConfig;
use crate::pipeline::SealContext;
use spdlog::{debug, error, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::{JoinHandle, sleep};
use std::time::Duration;
use storage::SegmentStaus::SEALED;
use storage::entities::WalEntry;
use storage::{Segment, Storage};

pub struct Seal {
    runner: Option<SealRunner>, // will be taken out when start() is called
}

struct SealRunner {
    storage: Arc<Storage>,
    balances: Vec<i64>,
    /// Latest `(version, crc32c)` per function name. `crc32c == 0` means
    /// the name's most recent record is an unregister; the entry stays in
    /// the map so the function snapshot preserves the audit trail
    /// (internal.md §11.4).
    function_map: HashMap<String, (u16, u32)>,
    seal_check_internal: Duration,
}

impl Seal {
    pub fn new(config: &LedgerConfig, storage: Arc<Storage>) -> Self {
        Self {
            runner: Some(SealRunner {
                storage,
                balances: vec![0; config.max_accounts],
                function_map: HashMap::new(),
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

    /// Pre-seal a segment during recovery. Returns the sealed
    /// segment id so the caller can publish it to the pipeline's
    /// seal index.
    ///
    /// At recovery time, [`storage::Storage::truncate_wal_above`]
    /// has already enforced the watermark on disk: any tx above it has
    /// been physically removed. The segments visible at this point are
    /// CLOSED, will never be appended to, and are expected to be safe
    /// to seal. So if `process_seal`'s gate *fires* here — meaning a
    /// segment's last `tx_id` is still above `seal_watermark` after
    /// truncation supposedly ran — the cluster invariant has been
    /// broken. We surface that as an error rather than silently
    /// leaving an unsealable CLOSED segment behind.
    pub fn recover_pre_seal(
        &mut self,
        segment: &mut Segment,
        seal_watermark: u64,
    ) -> std::io::Result<u32> {
        if let Some(mut runner) = self.runner.take() {
            let result = runner.process_seal(segment, seal_watermark);
            self.runner = Some(runner);
            match result? {
                Some(id) => Ok(id),
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "recover_pre_seal: segment {} has last_tx > seal_watermark={} \
                         after truncate_wal_above ran. The on-disk watermark invariant \
                         is broken (ADR-0016 §10).",
                        segment.id(),
                        seal_watermark,
                    ),
                )),
            }
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

    /// Seed (or update) Seal's in-memory function map during recovery —
    /// from a function snapshot triple or a forward-replayed
    /// `FunctionRegistered` record. `crc32c == 0` means the name is in
    /// an unregistered state; the entry is still recorded so the next
    /// snapshot reflects the audit trail (internal.md §11.4).
    pub(crate) fn recover_function_record(
        &mut self,
        name: &str,
        version: u16,
        crc32c: u32,
    ) -> std::io::Result<()> {
        if let Some(mut runner) = self.runner.take() {
            runner
                .function_map
                .insert(name.to_string(), (version, crc32c));
            self.runner = Some(runner);
            Ok(())
        } else {
            Err(std::io::Error::other("Seal already started"))
        }
    }
}

impl SealRunner {
    fn run(&mut self, ctx: SealContext) {
        while ctx.is_running() {
            // A long time nothing happened.
            if let Err(e) = self.seal_pending_segments(&ctx) {
                error!("Seal: failed to seal pending segments: {}", e);
                sleep(Duration::from_secs(1));
            }
            sleep(self.seal_check_internal);
        }
    }

    fn seal_pending_segments(&mut self, ctx: &SealContext) -> std::io::Result<()> {
        // Snapshot the cluster gate once per pass — it advances
        // monotonically, so re-reading inside the loop would not
        // change the decision for an already-rejected segment.
        let seal_watermark = ctx.seal_watermark();

        let mut pending = self.storage.list_all_segments()?;
        for segment in pending.iter_mut() {
            if !ctx.is_running() {
                break;
            }
            if segment.status() == SEALED {
                continue;
            }
            match self.process_seal(segment, seal_watermark)? {
                Some(id) => ctx.set_processed_index(id),
                None => {
                    // Cluster gate blocked this segment. Later
                    // segments have strictly higher tx_ids, so they're
                    // blocked too — abandon the pass.
                    break;
                }
            }
        }

        Ok(())
    }

    /// Seal `segment` and update the runner's balance buffer.
    ///
    /// Returns `Some(id)` when the segment was actually sealed; the
    /// caller is responsible for publishing `id` to the pipeline
    /// (either via `SealContext` or directly during recovery).
    ///
    /// Returns `None` when the cluster seal-watermark gate
    /// (ADR-0016 §10) blocked the seal because `segment_last_tx >
    /// seal_watermark`. In that case the segment stays CLOSED (no
    /// `.crc` / `.seal` written) and balances are unchanged. The
    /// caller must NOT advance the seal index.
    ///
    /// The segment is loaded exactly once at the start of this method;
    /// the gate check then reads the last tx_id directly from the
    /// loaded WAL via `Segment::last_tx_id_in_wal_data` — no second
    /// load, no full forward scan.
    pub fn process_seal(
        &mut self,
        segment: &mut Segment,
        seal_watermark: u64,
    ) -> std::io::Result<Option<u32>> {
        segment.load()?;

        // ADR-0016 §10 cluster gate. The default `u64::MAX` (standalone
        // Ledger users, plain `start()`) short-circuits this check.
        if seal_watermark != u64::MAX {
            let last_tx = segment.last_tx_id_in_wal_data()?;
            if last_tx > seal_watermark {
                debug!(
                    "Seal: skipping segment {} (last_tx={} > seal_watermark={})",
                    segment.id(),
                    last_tx,
                    seal_watermark
                );
                return Ok(None);
            }
        }

        segment.seal()?;

        // Build on-disk transaction and account index files (ADR-008).
        if let Err(e) = segment.build_indexes() {
            error!(
                "Seal: failed to build indexes for segment {}: {}",
                segment.id(),
                e
            );
        }

        // Load WAL records and update balances + function map from this
        // segment's WAL. Function registrations are kept in the map even
        // when unregistered (crc32c == 0) so the next snapshot preserves
        // the audit trail (internal.md §11.4).
        segment.visit_wal_records(|entry| match entry {
            WalEntry::Entry(e) => {
                let id = e.account_id as usize;
                if id < self.balances.len() {
                    self.balances[id] = e.computed_balance;
                } else {
                    warn!("Seal: account ID {} exceeds balance vector length", id);
                }
            }
            WalEntry::FunctionRegistered(f) => {
                self.function_map
                    .insert(f.name_str().to_string(), (f.version, f.crc32c));
            }
            _ => {}
        })?;

        // Write the balance + function snapshots at the configured cadence.
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

            // Function snapshot — emitted even when empty (internal.md §20.7).
            let mut function_records: Vec<(String, u16, u32)> = self
                .function_map
                .iter()
                .map(|(name, (version, crc))| (name.clone(), *version, *crc))
                .collect();
            function_records.sort_unstable_by(|a, b| a.0.cmp(&b.0));

            debug!(
                "Seal: saving function snapshot for WAL segment {} ({} entries)",
                segment.id(),
                function_records.len()
            );
            if let Err(e) = segment.save_function_snapshot(&function_records[..]) {
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

        Ok(Some(segment.id()))
    }
}
