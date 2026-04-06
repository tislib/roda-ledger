use crate::entities::WalEntry;
use crate::storage::SegmentStaus::SEALED;
use crate::storage::{Segment, Storage};
use spdlog::{debug, error, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::{JoinHandle, sleep};
use std::time::Duration;

pub struct Seal {
    last_sealed_id: Arc<AtomicU32>,
    runner: Option<SealRunner>, // will be taken out when start() is called
}

struct SealRunner {
    storage: Arc<Storage>,
    running: Arc<AtomicBool>,
    balances: Vec<i64>,
    last_sealed_id: Arc<AtomicU32>,
    seal_check_internal: Duration,
}

impl Seal {
    pub fn new(
        account_count: usize,
        storage: Arc<Storage>,
        running: Arc<AtomicBool>,
        seal_check_internal: Duration,
    ) -> Self {
        let last_sealed_id = Arc::new(AtomicU32::new(0));
        Self {
            last_sealed_id: last_sealed_id.clone(),
            runner: Some(SealRunner {
                storage: storage.clone(),
                running: running.clone(),
                balances: vec![0; account_count],
                last_sealed_id: last_sealed_id.clone(),
                seal_check_internal,
            }),
        }
    }

    pub fn last_sealed_segment_id(&self) -> u32 {
        self.last_sealed_id.load(Ordering::Acquire)
    }

    pub fn start(&mut self) -> std::io::Result<JoinHandle<()>> {
        if let Some(runner) = self.runner.take() {
            std::thread::Builder::new()
                .name("seal".to_string())
                .spawn(move || {
                    let mut r = runner;
                    r.run();
                })
        } else {
            Err(std::io::Error::other("Seal already started"))
        }
    }

    pub fn recover_pre_seal(&mut self, segment: &mut Segment) -> std::io::Result<()> {
        if let Some(mut runner) = self.runner.take() {
            runner.process_seal(segment)?;
            self.runner = Some(runner);
            Ok(())
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
}

impl SealRunner {
    fn run(&mut self) {
        loop {
            // A long time nothing happened.
            if let Err(e) = self.seal_pending_segments() {
                error!("Seal: failed to seal pending segments: {}", e);
                sleep(Duration::from_secs(1));
            }
            // Check before sleep to ensure that the last seal is done before shutting down
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            sleep(self.seal_check_internal);
        }
    }

    fn seal_pending_segments(&mut self) -> std::io::Result<()> {
        let mut pending = self.storage.list_all_segments()?;
        for segment in pending.iter_mut() {
            if segment.status() == SEALED {
                continue;
            }
            self.process_seal(segment)?;
        }

        Ok(())
    }

    pub fn process_seal(&mut self, segment: &mut Segment) -> std::io::Result<()> {
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

        // 3. Record the last sealed segment id
        self.last_sealed_id.store(segment.id(), Ordering::Release);

        // only the last seal can be taken snapshot
        // 4. Load wal records and update balances
        segment.visit_wal_records(|entry| {
            if let WalEntry::Entry(e) = entry {
                let id = e.account_id as usize;
                if id < self.balances.len() {
                    self.balances[id] = e.computed_balance;
                } else {
                    warn!("Seal: account ID {} exceeds balance vector length", id);
                }
            }
        })?;

        // 5. Conditionally write a snapshot
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
        } else if snapshot_frequency > 0 {
            debug!(
                "Seal: skipping snapshot for segment {} (frequency={})",
                segment.id(),
                snapshot_frequency
            );
        }

        Ok(())
    }
}
