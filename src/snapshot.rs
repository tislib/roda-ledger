use crate::balance::Balance;
use crate::entities::{EntryKind, WalEntry};
use arc_swap::ArcSwap;
use crossbeam_queue::ArrayQueue;
use crossbeam_skiplist::SkipMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default)]
pub struct Checkpoint {
    pub checkpoint_id: u64,
    pub last_transaction_id: u64,
    pub last_wal_position: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct BalanceCheckpoint {
    pub account_id: u64,
    pub checkpoint: u64,
}

impl BalanceCheckpoint {
    pub fn new(account_id: u64, checkpoint: u64) -> Self {
        Self {
            account_id,
            checkpoint,
        }
    }

    pub fn range(account_id: u64) -> std::ops::Range<Self> {
        Self::new(account_id, 0)..Self::new(account_id + 1, 0)
    }
}

pub struct Snapshot {
    inbound: Arc<ArrayQueue<WalEntry>>,
    // Store as (account_id, checkpoint) -> Balance
    balances: Arc<SkipMap<BalanceCheckpoint, Balance>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    checkpoint: Arc<ArcSwap<Checkpoint>>,
    checkpoint_requested: Arc<AtomicBool>,
    location: Option<PathBuf>,
    in_memory: bool,
    snapshot_interval: Duration,
    pending_entries: AtomicU8,
    running: Arc<AtomicBool>,
}

pub struct SnapshotRunner {
    inbound: Arc<ArrayQueue<WalEntry>>,
    balances: Arc<SkipMap<BalanceCheckpoint, Balance>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    checkpoint: Arc<ArcSwap<Checkpoint>>,
    checkpoint_requested: Arc<AtomicBool>,
    pending_entries: u8,
    running: Arc<AtomicBool>,
}

pub struct SnapshotStorer {
    balances: Arc<SkipMap<BalanceCheckpoint, Balance>>,
    checkpoint: Arc<ArcSwap<Checkpoint>>,
    checkpoint_requested: Arc<AtomicBool>,
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

        Self {
            inbound,
            balances: Arc::new(SkipMap::new()),
            last_processed_transaction_id: Arc::new(Default::default()),
            checkpoint: Arc::new(ArcSwap::new(Arc::new(Checkpoint::default()))),
            checkpoint_requested: Arc::new(AtomicBool::new(false)),
            location,
            in_memory,
            snapshot_interval,
            pending_entries: AtomicU8::new(0),
            running,
        }
    }

    pub fn start(&self) -> Vec<JoinHandle<()>> {
        let runner = SnapshotRunner {
            inbound: self.inbound.clone(),
            balances: self.balances.clone(),
            last_processed_transaction_id: self.last_processed_transaction_id.clone(),
            checkpoint: self.checkpoint.clone(),
            checkpoint_requested: self.checkpoint_requested.clone(),
            pending_entries: 0,
            running: self.running.clone(),
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
                let checkpoint_id = self.checkpoint.load().checkpoint_id;
                let balance = self.get_balance(e.account_id);
                let new_balance = match e.kind {
                    EntryKind::Credit => balance.saturating_sub(e.amount as i64),
                    EntryKind::Debit => balance.saturating_add(e.amount as i64),
                };
                self.balances.insert(
                    BalanceCheckpoint::new(e.account_id, checkpoint_id),
                    new_balance,
                );
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
            return Ok(());
        }

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

        let balance_size = std::mem::size_of::<Balance>();
        let mut balance_buf = vec![0u8; balance_size];

        loop {
            match file.read_exact(&mut buf_u64) {
                Ok(_) => {
                    let account_id = u64::from_le_bytes(buf_u64);
                    file.read_exact(&mut balance_buf)?;
                    let balance: Balance = *bytemuck::from_bytes(&balance_buf);
                    self.balances
                        .insert(BalanceCheckpoint::new(account_id, checkpoint_id), balance);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    pub fn get_all_balances(&self) -> Vec<(u64, Balance)> {
        let mut results = Vec::new();
        let mut last_account_id = None;
        let mut last_balance = None;

        for entry in self.balances.iter() {
            let key = entry.key();
            if Some(key.account_id) == last_account_id {
                last_balance = Some(*entry.value());
            } else {
                if let (Some(id), Some(bal)) = (last_account_id, last_balance) {
                    results.push((id, bal));
                }
                last_account_id = Some(key.account_id);
                last_balance = Some(*entry.value());
            }
        }
        if let (Some(id), Some(bal)) = (last_account_id, last_balance) {
            results.push((id, bal));
        }
        results
    }

    pub fn get_balance(&self, account_id: u64) -> Balance {
        // Find the highest checkpoint for the account.
        // SkipMap keys are sorted by account_id ASC, then checkpoint ASC.
        // range((account_id, u64::MAX)..) would give entries starting from (account_id, MAX), which is wrong.
        // We need to look at entries for account_id and take the last one.
        // Or we can use range(BalanceCheckpoint::new(account_id, 0)..BalanceCheckpoint::new(account_id + 1, 0)) and take last.
        if let Some(entry) = self
            .balances
            .range(BalanceCheckpoint::range(account_id))
            .next_back()
        {
            return *entry.value();
        }
        0
    }
}

impl SnapshotRunner {
    pub fn run(&mut self) {
        let mut last_transaction_id = 0;
        let mut last_wal_position = self.checkpoint.load().last_wal_position;

        while self.running.load(Ordering::Relaxed) {
            if let Some(wal_entry) = self.inbound.pop() {
                last_transaction_id = wal_entry.tx_id();
                last_wal_position += 33; // Each entry is 33 bytes (1 kind + 32 struct)

                match wal_entry {
                    WalEntry::Metadata(m) => {
                        self.pending_entries = m.entry_count;
                    }
                    WalEntry::Entry(e) => {
                        let checkpoint_id = self.checkpoint.load().checkpoint_id;
                        let balance = if let Some(entry) = self
                            .balances
                            .range(BalanceCheckpoint::range(e.account_id))
                            .next_back()
                        {
                            *entry.value()
                        } else {
                            0
                        };
                        let new_balance = match e.kind {
                            EntryKind::Credit => balance.saturating_sub(e.amount as i64),
                            EntryKind::Debit => balance.saturating_add(e.amount as i64),
                        };
                        self.balances.insert(
                            BalanceCheckpoint::new(e.account_id, checkpoint_id),
                            new_balance,
                        );
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
                    let new_checkpoint = Checkpoint {
                        checkpoint_id: cp.checkpoint_id + 1,
                        last_transaction_id,
                        last_wal_position,
                    };
                    self.checkpoint.store(Arc::new(new_checkpoint));
                    self.checkpoint_requested.store(false, Ordering::Relaxed);
                }
                std::thread::yield_now();
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

            // Request checkpoint
            self.checkpoint_requested.store(true, Ordering::Relaxed);

            // Wait for checkpoint to be fulfilled
            while self.checkpoint_requested.load(Ordering::Relaxed) {
                if !self.running.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }

            let checkpoint = self.checkpoint.load();

            // Collect all balances up to current checkpoint_id
            let balances = self.collect_balances(checkpoint.checkpoint_id);

            if let Err(e) = self.save_snapshot(balances) {
                eprintln!("Failed to save snapshot: {}", e);
            }
        }
    }

    fn collect_balances(&self, up_to_checkpoint: u64) -> Vec<(u64, Balance)> {
        let mut results = Vec::new();
        let mut last_account_id = None;
        let mut last_balance = None;

        for entry in self.balances.iter() {
            let key = entry.key();
            if key.checkpoint > up_to_checkpoint {
                continue;
            }

            if Some(key.account_id) == last_account_id {
                last_balance = Some(*entry.value());
            } else {
                if let (Some(id), Some(bal)) = (last_account_id, last_balance) {
                    results.push((id, bal));
                }
                last_account_id = Some(key.account_id);
                last_balance = Some(*entry.value());
            }
        }
        if let (Some(id), Some(bal)) = (last_account_id, last_balance) {
            results.push((id, bal));
        }
        results
    }

    fn save_snapshot(&self, balances: Vec<(u64, Balance)>) -> std::io::Result<()> {
        let location = self.location.as_ref().unwrap();
        let tmp_path = location.join("snapshot.bin.tmp");
        let final_path = location.join("snapshot.bin");

        let mut file = std::fs::File::create(&tmp_path)?;

        let mut buffer =
            Vec::with_capacity(32 + balances.len() * (8 + std::mem::size_of::<Balance>()));

        // Write snapshot header: checkpoint_id, last_transaction_id, last_wal_position (each u64)
        let cp = self.checkpoint.load();
        buffer.extend_from_slice(&cp.checkpoint_id.to_le_bytes());
        buffer.extend_from_slice(&cp.last_transaction_id.to_le_bytes());
        buffer.extend_from_slice(&cp.last_wal_position.to_le_bytes());

        for (account_id, balance) in balances {
            buffer.extend_from_slice(&account_id.to_le_bytes());
            buffer.extend_from_slice(bytemuck::bytes_of(&balance));
        }

        file.write_all(&buffer)?;
        file.sync_all()?;
        std::fs::rename(tmp_path, final_path)?;
        Ok(())
    }
}
