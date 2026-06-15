use crate::config::LedgerConfig;
use crate::pipeline::SealContext;
use crate::recover::ActiveSnapshot;
use crate::transactor::grow_capacity;
use crate::transactor::kv::KvKey;
use spdlog::{debug, error};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::{JoinHandle, sleep};
use std::time::Duration;
use storage::SegmentStaus::SEALED;
use storage::entities::WalEntry;
use storage::{Segment, Storage};

/// One-shot launcher for the seal thread. Holds exactly what `start` needs;
/// `start` consumes `self`, recovers the runner's baseline from `active_snapshot`,
/// spawns the thread, and drops `self` while the runner keeps running.
pub struct Seal<'a> {
    pub ctx: SealContext,
    pub active_snapshot: &'a ActiveSnapshot,
    pub storage: Arc<Storage>,
    pub config: &'a LedgerConfig,
}

impl Seal<'_> {
    pub fn start(self) -> std::io::Result<JoinHandle<()>> {
        let Seal {
            ctx,
            active_snapshot,
            storage,
            config,
        } = self;

        // Resume sealing forward only: every already-closed segment is treated as
        // done (never re-sealed/re-snapshotted from the recovered baseline). The
        // active segment is sealed when it finishes; nothing in the past is touched.
        ctx.set_processed_index(storage.last_segment_id().saturating_sub(1));

        let runner = SealRunner::recover(storage, config, active_snapshot);
        std::thread::Builder::new()
            .name("seal".to_string())
            .spawn(move || {
                let mut r = runner;
                r.run(ctx);
            })
    }
}

struct SealRunner {
    storage: Arc<Storage>,
    balances: Vec<i64>,
    /// Per-account flags (ADR-022), parallel to `balances`; written into each
    /// snapshot so PROGRAMMED buckets recover with their real status.
    flags: Vec<u64>,
    /// Parent→bucket links `(parent_id, type_id) → child_id`, written into the
    /// snapshot so links survive a snapshot-based restart.
    links: HashMap<(u64, u16), u64>,
    /// Account allocator high-water (ADR-022): bumped by `AccountOpened` records
    /// as segments seal, and written into each snapshot.
    next_account_id: u64,
    /// Geometric growth factor for `balances` (ADR-022), from config.
    resize_factor: f64,
    /// Latest `(version, crc32c)` per function name (`crc32c == 0` = unregistered;
    /// kept for the audit trail, internal.md §11.4).
    function_map: HashMap<String, (u16, u32)>,
    /// Programmable KV state (ADR-023): folded as segments seal and written into
    /// each snapshot so recovery loads it rather than replaying the whole WAL.
    kv: HashMap<KvKey, i64>,
    seal_check_internal: Duration,
}

impl SealRunner {
    /// Build the seal baseline from `active_snapshot`: balances/flags by id,
    /// links, allocator high-water, and the function map. The next snapshot the
    /// stage writes (for the active segment, once it closes) is computed from here.
    fn recover(storage: Arc<Storage>, config: &LedgerConfig, snap: &ActiveSnapshot) -> Self {
        let cap = (snap.next_account_id as usize).max(config.initial_account_size);
        let mut balances = vec![0i64; cap];
        let mut flags = vec![0u64; cap];
        for (&id, account) in &snap.accounts {
            balances[id as usize] = account.balance;
            flags[id as usize] = account.flags;
        }
        let mut links = HashMap::new();
        for &(parent_id, type_id, child_id) in &snap.links {
            links.insert((parent_id, type_id), child_id);
        }
        let mut function_map = HashMap::new();
        for f in &snap.functions {
            function_map.insert(f.name_str().to_string(), (f.version, f.crc32c));
        }
        Self {
            storage,
            balances,
            flags,
            links,
            next_account_id: snap.next_account_id,
            resize_factor: config.resize_factor,
            function_map,
            kv: snap.kv.clone(),
            seal_check_internal: config.seal_check_internal,
        }
    }

    /// Grow `balances` to cover `needed` ids (geometric, ADR-022).
    fn ensure_balance_capacity(&mut self, needed: usize) {
        if needed > self.balances.len() {
            let new_cap = grow_capacity(self.balances.len(), self.resize_factor, needed);
            self.balances.resize(new_cap, 0);
            self.flags.resize(new_cap, 0);
        }
    }

    fn run(&mut self, ctx: SealContext) {
        while ctx.is_running() {
            ctx.bump_seal_step_id();
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
        // Never re-seal the past: only segments after the last processed one.
        let last_processed = ctx.get_processed_index();

        let mut pending = self.storage.list_all_segments()?;
        for segment in pending.iter_mut() {
            if segment.id() <= last_processed || segment.status() == SEALED {
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

    /// Seal `segment`, fold its WAL into the baseline, and write a snapshot at the
    /// configured cadence (state as-of this segment).
    ///
    /// Returns `None` when the cluster seal-watermark gate (ADR-0016 §10) blocked
    /// the seal because `segment_last_tx > seal_watermark`; the segment stays
    /// CLOSED and the caller must NOT advance the seal index.
    fn process_seal(
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

        self.fold_segment(segment)?;

        // Write a snapshot at the configured cadence (state as-of this segment).
        let snapshot_frequency = self.storage.config().snapshot_frequency;
        if snapshot_frequency > 0 && segment.id().is_multiple_of(snapshot_frequency) {
            self.write_snapshots(segment);
        }

        Ok(Some(segment.id()))
    }

    /// Fold one segment's WAL into the in-memory baseline (balances/flags/links/
    /// next_account_id/function_map).
    fn fold_segment(&mut self, segment: &Segment) -> std::io::Result<()> {
        segment.visit_wal_records(|entry| match entry {
            WalEntry::Entry(e) => {
                let id = e.account_id as usize;
                self.ensure_balance_capacity(id + 1);
                self.balances[id] = e.computed_balance;
            }
            WalEntry::FunctionRegistered(f) => {
                self.function_map
                    .insert(f.name_str().to_string(), (f.version, f.crc32c));
            }
            WalEntry::AccountOpened(a) => {
                let end = a.begin_account_id + a.count as u64;
                self.next_account_id = self.next_account_id.max(end);
                self.ensure_balance_capacity(end as usize);
                for id in a.begin_account_id..end {
                    self.flags[id as usize] = a.flags;
                }
            }
            WalEntry::AccountLinked(a) => {
                self.links.insert((a.parent_id, a.type_id), a.child_id);
            }
            WalEntry::AccountFlagsUpdated(a) => {
                self.ensure_balance_capacity(a.account_id as usize + 1);
                self.flags[a.account_id as usize] = a.new_flags;
            }
            WalEntry::Kv(k) => {
                let key = KvKey::new(k.kv_scope, k.account_id, k.key);
                if k.value == 0 {
                    self.kv.remove(&key);
                } else {
                    self.kv.insert(key, k.value);
                }
            }
            _ => {}
        })
    }

    /// Write the balance + function snapshots for `segment` from the current
    /// baseline (state as-of this segment). Errors are logged, not propagated.
    fn write_snapshots(&self, segment: &Segment) {
        // Persist every existent account (flags != 0) plus any funded
        // account (balance != 0, e.g. SYSTEM): (id, balance, flags).
        let mut snapshot_records: Vec<(u64, i64, u64)> = self
            .flags
            .iter()
            .enumerate()
            .filter_map(|(id, &flags)| {
                let bal = self.balances[id];
                if flags != 0 || bal != 0 {
                    Some((id as u64, bal, flags))
                } else {
                    None
                }
            })
            .collect();
        snapshot_records.sort_unstable_by_key(|(id, _, _)| *id);

        let mut snapshot_links: Vec<(u64, u16, u64)> =
            self.links.iter().map(|(&(p, t), &c)| (p, t, c)).collect();
        snapshot_links.sort_unstable();

        debug!("Seal: saving snapshot for WAL segment {}", segment.id());
        if let Err(e) = segment.save_snapshot(
            self.next_account_id,
            &snapshot_records[..],
            &snapshot_links[..],
        ) {
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

        // KV snapshot (ADR-023) — the full programmable KV state as-of this segment.
        let mut kv_records: Vec<(u16, u64, [u32; 4], i64)> = self
            .kv
            .iter()
            .map(|(k, &v)| (k.kv_scope, k.account_id, k.key, v))
            .collect();
        kv_records.sort_unstable();
        if let Err(e) = segment.save_kv_snapshot(&kv_records[..]) {
            error!(
                "Seal: failed to save kv snapshot for segment {}: {}",
                segment.id(),
                e
            );
        }
    }
}
