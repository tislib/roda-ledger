use crate::balance::Balance;
use crate::entities::{EntryKind, WalEntry};
use crate::pipeline_mode::PipelineMode;
use arc_swap::ArcSwap;
use crossbeam_queue::ArrayQueue;
use spdlog::{debug, error, info};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default)]
pub struct Checkpoint {
    pub checkpoint_id: u64,
    pub last_transaction_id: u64,
    pub last_wal_position: u64,
}

pub struct SnapshotBalance {
    pub snapshot: AtomicI64,
    pub checkpoint: AtomicI64,
    pub checkpoint_id: AtomicU8,
}

pub struct Snapshot {
    inbound: Arc<ArrayQueue<WalEntry>>,
    balances: Arc<Vec<SnapshotBalance>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    checkpoint: Arc<ArcSwap<Checkpoint>>,
    checkpoint_requested: Arc<AtomicBool>,
    checkpoint_mode: Arc<AtomicBool>,
    current_checkpoint: Arc<AtomicU8>,
    location: Option<PathBuf>,
    in_memory: bool,
    snapshot_interval: Duration,
    pending_entries: AtomicU8,
    running: Arc<AtomicBool>,
    pipeline_mode: PipelineMode,
}

pub struct SnapshotRunner {
    inbound: Arc<ArrayQueue<WalEntry>>,
    balances: Arc<Vec<SnapshotBalance>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    checkpoint: Arc<ArcSwap<Checkpoint>>,
    checkpoint_requested: Arc<AtomicBool>,
    checkpoint_mode: Arc<AtomicBool>,
    current_checkpoint: Arc<AtomicU8>,
    pending_entries: u8,
    running: Arc<AtomicBool>,
    pipeline_mode: PipelineMode,
}

pub struct SnapshotStorer {
    balances: Arc<Vec<SnapshotBalance>>,
    checkpoint: Arc<ArcSwap<Checkpoint>>,
    checkpoint_requested: Arc<AtomicBool>,
    checkpoint_mode: Arc<AtomicBool>,
    current_checkpoint: Arc<AtomicU8>,
    location: Option<PathBuf>,
    in_memory: bool,
    snapshot_interval: Duration,
    running: Arc<AtomicBool>,
}

impl Snapshot {
    pub fn new(
        inbound: Arc<ArrayQueue<WalEntry>>,
        location: Option<&str>,
        in_memory: bool,
        snapshot_interval: Duration,
        running: Arc<AtomicBool>,
        max_accounts: usize,
        pipeline_mode: PipelineMode,
    ) -> Self {
        let location = if in_memory {
            None
        } else {
            let folder = location.unwrap_or("data");
            let path = std::path::Path::new(folder);
            if !path.exists() {
                let _ = std::fs::create_dir_all(path);
            }
            Some(path.to_path_buf())
        };

        let balances = (0..max_accounts)
            .map(|_| SnapshotBalance {
                snapshot: AtomicI64::new(0),
                checkpoint: AtomicI64::new(0),
                checkpoint_id: AtomicU8::new(0),
            })
            .collect();

        Self {
            inbound,
            balances: Arc::new(balances),
            last_processed_transaction_id: Arc::new(Default::default()),
            checkpoint: Arc::new(ArcSwap::new(Arc::new(Checkpoint::default()))),
            checkpoint_requested: Arc::new(AtomicBool::new(false)),
            checkpoint_mode: Arc::new(AtomicBool::new(false)),
            current_checkpoint: Arc::new(AtomicU8::new(0)),
            location,
            in_memory,
            snapshot_interval,
            pending_entries: AtomicU8::new(0),
            running,
            pipeline_mode,
        }
    }

    pub fn start(&self) -> Vec<JoinHandle<()>> {
        let runner = SnapshotRunner {
            inbound: self.inbound.clone(),
            balances: self.balances.clone(),
            last_processed_transaction_id: self.last_processed_transaction_id.clone(),
            checkpoint: self.checkpoint.clone(),
            checkpoint_requested: self.checkpoint_requested.clone(),
            checkpoint_mode: self.checkpoint_mode.clone(),
            current_checkpoint: self.current_checkpoint.clone(),
            pending_entries: 0,
            running: self.running.clone(),
            pipeline_mode: self.pipeline_mode,
        };
        let h1 = std::thread::Builder::new()
            .name("snapshot".to_string())
            .spawn(move || {
                let mut runner = runner;
                runner.run()
            })
            .unwrap();

        let storer = SnapshotStorer {
            balances: self.balances.clone(),
            checkpoint: self.checkpoint.clone(),
            checkpoint_requested: self.checkpoint_requested.clone(),
            checkpoint_mode: self.checkpoint_mode.clone(),
            current_checkpoint: self.current_checkpoint.clone(),
            location: self.location.clone(),
            in_memory: self.in_memory,
            snapshot_interval: self.snapshot_interval,
            running: self.running.clone(),
        };

        let h2 = std::thread::Builder::new()
            .name("snapshot_storer".to_string())
            .spawn(move || {
                let mut storer = storer;
                storer.run()
            })
            .unwrap();
        vec![h1, h2]
    }

    pub fn last_processed_transaction_id(&self) -> u64 {
        self.last_processed_transaction_id
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn last_wal_position(&self) -> u64 {
        self.checkpoint.load().last_wal_position
    }

    pub fn set_last_processed_transaction_id(&self, last_id: u64) {
        self.last_processed_transaction_id
            .store(last_id, Ordering::Relaxed);
    }

    pub fn reprocess_transaction(&self, wal_entry: WalEntry) {
        let tx_id = wal_entry.tx_id();
        match wal_entry {
            WalEntry::Metadata(m) => {
                self.pending_entries.store(m.entry_count, Ordering::Relaxed);
            }
            WalEntry::Entry(e) => {
                if let Some(sb) = self.balances.get(e.account_id as usize) {
                    let balance = sb.snapshot.load(Ordering::Acquire);
                    let new_balance = match e.kind {
                        EntryKind::Credit => balance.saturating_sub(e.amount as i64),
                        EntryKind::Debit => balance.saturating_add(e.amount as i64),
                    };
                    sb.snapshot.store(new_balance, Ordering::Release);
                    // During replay/reprocess, we just sync both
                    sb.checkpoint.store(new_balance, Ordering::Release);
                }

                let current = self.pending_entries.load(Ordering::Relaxed);
                if current > 0 {
                    self.pending_entries.store(current - 1, Ordering::Relaxed);
                }
            }
        }
        if self.pending_entries.load(Ordering::Relaxed) == 0 {
            self.last_processed_transaction_id
                .store(tx_id, Ordering::Relaxed);
        }
    }

    pub fn restore(&self) -> std::io::Result<()> {
        if self.in_memory || self.location.is_none() {
            return Ok(());
        }

        let location = self.location.as_ref().unwrap();
        let final_path = location.join("snapshot.bin");

        if !final_path.exists() {
            info!("No snapshot file found at {:?}", final_path);
            return Ok(());
        }

        info!("Restoring snapshot from {:?}", final_path);
        let mut file = std::fs::File::open(&final_path)?;
        let mut buf_u64 = [0u8; 8];

        file.read_exact(&mut buf_u64)?;
        let checkpoint_id = u64::from_le_bytes(buf_u64);

        file.read_exact(&mut buf_u64)?;
        let last_transaction_id = u64::from_le_bytes(buf_u64);

        file.read_exact(&mut buf_u64)?;
        let last_wal_position = u64::from_le_bytes(buf_u64);

        self.last_processed_transaction_id
            .store(last_transaction_id, Ordering::Relaxed);
        self.checkpoint.store(Arc::new(Checkpoint {
            checkpoint_id,
            last_transaction_id,
            last_wal_position,
        }));

        let mut balance_buf = [0u8; 8];

        loop {
            match file.read_exact(&mut buf_u64) {
                Ok(_) => {
                    let account_id = u64::from_le_bytes(buf_u64);
                    file.read_exact(&mut balance_buf)?;
                    let balance = i64::from_le_bytes(balance_buf);
                    if let Some(sb) = self.balances.get(account_id as usize) {
                        sb.snapshot.store(balance, Ordering::Release);
                        sb.checkpoint.store(balance, Ordering::Release);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    pub fn get_all_balances(&self) -> Vec<(u64, Balance)> {
        self.balances
            .iter()
            .enumerate()
            .filter_map(|(id, sb)| {
                let bal = sb.snapshot.load(Ordering::Acquire);
                if bal != 0 {
                    Some((id as u64, bal))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_balance(&self, account_id: u64) -> Balance {
        if let Some(sb) = self.balances.get(account_id as usize) {
            sb.snapshot.load(Ordering::Acquire)
        } else {
            0
        }
    }
}

impl SnapshotRunner {
    pub fn run(&mut self) {
        let mut last_transaction_id = self.last_processed_transaction_id.load(Ordering::Relaxed);
        let mut last_wal_position = self.checkpoint.load().last_wal_position;

        let mut retry_count = 0;
        while self.running.load(Ordering::Relaxed) {
            if let Some(wal_entry) = self.inbound.pop() {
                retry_count = 0;
                last_transaction_id = wal_entry.tx_id();
                last_wal_position += 33; // Each entry is 33 bytes (1 kind + 32 struct)

                match wal_entry {
                    WalEntry::Metadata(m) => {
                        self.pending_entries = m.entry_count;
                    }
                    WalEntry::Entry(e) => {
                        if let Some(sb) = self.balances.get(e.account_id as usize) {
                            let balance = sb.snapshot.load(Ordering::Acquire);
                            let new_balance = match e.kind {
                                EntryKind::Credit => balance.saturating_sub(e.amount as i64),
                                EntryKind::Debit => balance.saturating_add(e.amount as i64),
                            };
                            sb.snapshot.store(new_balance, Ordering::Release);

                            if !self.checkpoint_mode.load(Ordering::Acquire) {
                                // Normal mode: update checkpoint balance too
                                sb.checkpoint.store(new_balance, Ordering::Release);
                            } else {
                                // Checkpoint mode: track that we updated it
                                let current_cp = self.current_checkpoint.load(Ordering::Acquire);
                                sb.checkpoint_id.store(current_cp, Ordering::Release);
                            }
                        }
                        if self.pending_entries > 0 {
                            self.pending_entries -= 1;
                        }
                    }
                }
                if self.pending_entries == 0 {
                    self.last_processed_transaction_id
                        .store(last_transaction_id, Ordering::Relaxed);
                }
            } else {
                if self.checkpoint_requested.load(Ordering::Relaxed) && self.pending_entries == 0 {
                    let cp = self.checkpoint.load();
                    let new_cp_id = cp.checkpoint_id + 1;

                    // Increment current_checkpoint (wrapping u8)
                    let next_cp_tag = self
                        .current_checkpoint
                        .load(Ordering::Acquire)
                        .wrapping_add(1);
                    if next_cp_tag == 0 {
                        // Avoid 0 as it's the default state
                        self.current_checkpoint.store(1, Ordering::Release);
                    } else {
                        self.current_checkpoint
                            .store(next_cp_tag, Ordering::Release);
                    }

                    let new_checkpoint = Checkpoint {
                        checkpoint_id: new_cp_id,
                        last_transaction_id,
                        last_wal_position,
                    };
                    self.checkpoint.store(Arc::new(new_checkpoint));

                    self.checkpoint_mode.store(true, Ordering::Release);
                    self.checkpoint_requested.store(false, Ordering::Release);
                }
                self.pipeline_mode.wait_strategy(retry_count);
                retry_count += 1;
            }
        }
    }
}

impl SnapshotStorer {
    pub fn run(&mut self) {
        if self.in_memory || self.location.is_none() {
            return;
        }

        while self.running.load(Ordering::Relaxed) {
            let start = std::time::Instant::now();
            while std::time::Instant::now().duration_since(start) < self.snapshot_interval {
                std::thread::sleep(Duration::from_millis(100));
                if !self.running.load(Ordering::Relaxed) {
                    return;
                }
            }

            // 1. Request checkpoint
            debug!("Requesting snapshot checkpoint");
            self.checkpoint_requested.store(true, Ordering::Release);

            // 2. Wait for Runner to enter checkpoint mode
            while !self.checkpoint_mode.load(Ordering::Acquire) {
                if !self.running.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }

            let cp_tag = self.current_checkpoint.load(Ordering::Acquire);

            // 3. Save snapshot (reads from .checkpoint)
            info!("Saving snapshot checkpoint {}", cp_tag);
            if let Err(e) = self.save_snapshot() {
                error!("Failed to save snapshot: {}", e);
            }

            // 4. Exit checkpoint mode
            self.checkpoint_mode.store(false, Ordering::Release);

            // 5. Catch up: balances updated during checkpoint mode
            for sb in self.balances.iter() {
                if sb.checkpoint_id.load(Ordering::Acquire) == cp_tag {
                    // Update checkpoint value to current snapshot value
                    let val = sb.snapshot.load(Ordering::Acquire);
                    sb.checkpoint.store(val, Ordering::Release);
                    // Reset tag
                    sb.checkpoint_id.store(0, Ordering::Release);
                }
            }
        }
    }

    fn save_snapshot(&self) -> std::io::Result<()> {
        let location = self.location.as_ref().unwrap();
        let tmp_path = location.join("snapshot.bin.tmp");
        let final_path = location.join("snapshot.bin");

        let mut file = std::fs::File::create(&tmp_path)?;
        let cp = self.checkpoint.load();

        let mut buffer = Vec::with_capacity(1024 * 1024);
        buffer.extend_from_slice(&cp.checkpoint_id.to_le_bytes());
        buffer.extend_from_slice(&cp.last_transaction_id.to_le_bytes());
        buffer.extend_from_slice(&cp.last_wal_position.to_le_bytes());

        for (id, sb) in self.balances.iter().enumerate() {
            let bal = sb.checkpoint.load(Ordering::Acquire);
            if bal != 0 {
                buffer.extend_from_slice(&(id as u64).to_le_bytes());
                buffer.extend_from_slice(&bal.to_le_bytes());
            }

            if buffer.len() > 8 * 1024 * 1024 {
                file.write_all(&buffer)?;
                buffer.clear();
            }
        }

        if !buffer.is_empty() {
            file.write_all(&buffer)?;
        }

        file.sync_all()?;
        std::fs::rename(tmp_path, final_path)?;
        Ok(())
    }
}
