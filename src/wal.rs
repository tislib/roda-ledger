use crate::balance::BalanceDataType;
use crate::transaction::{Transaction, TransactionDataType};
use crossbeam_queue::ArrayQueue;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

pub struct Wal<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    file_path: PathBuf,
    step: Arc<AtomicU64>,
}

pub struct WalRunner<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    file: Option<File>,
    file_path: PathBuf,
    buffer: Vec<Transaction<Data, BalanceData>>,
    last_retry: Option<Instant>,
    pub step: Arc<AtomicU64>,
}

impl<Data, BalanceData> Wal<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(
        inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
        outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
        ledger_folder: Option<&str>,
    ) -> Self {
        let folder = ledger_folder.unwrap_or("data");
        let path = Path::new(folder);
        if !path.exists() {
            let _ = std::fs::create_dir_all(path);
        }

        let file_path = path.join("wal.bin");

        Self {
            inbound,
            outbound,
            file_path,
            step: Arc::new(Default::default()),
        }
    }

    pub fn step(&self) -> u64 {
        self.step.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn start(&self) {
        let mut runner = self.create_runner();
        std::thread::Builder::new()
            .name("wal".to_string())
            .spawn(move || runner.run())
            .unwrap();
    }

    fn create_runner(&self) -> WalRunner<Data, BalanceData> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)
            .ok();

        let buffer_capacity = (128 * 1024) / std::mem::size_of::<Transaction<Data, BalanceData>>();

        WalRunner {
            inbound: self.inbound.clone(),
            outbound: self.outbound.clone(),
            file,
            file_path: self.file_path.clone(),
            buffer: Vec::with_capacity(buffer_capacity.max(1)),
            last_retry: None,
            step: self.step.clone(),
        }
    }
}

impl<Data, BalanceData> WalRunner<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn run(&mut self) {
        // Try to open file if not open
        if self.file.is_none() {
            if let Ok(file) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.file_path)
            {
                self.file = Some(file);
            } else {
                self.last_retry = Some(Instant::now());
                return;
            }
        }

        loop {
            // Check if we should retry
            if self
                .last_retry
                .is_some_and(|last| Instant::now().duration_since(last) < Duration::from_secs(1))
            {
                continue;
            }

            // Fill buffer if empty
            if self.buffer.is_empty() {
                let count = self.inbound.len().min(self.buffer.capacity());
                if count == 0 {
                    continue;
                }
                for _ in 0..count {
                    if let Some(tx) = self.inbound.pop() {
                        self.buffer.push(tx);
                    }
                }
            }

            if self.buffer.is_empty() {
                continue;
            }

            // Try to write buffer
            let bytes = bytemuck::cast_slice::<Transaction<Data, BalanceData>, u8>(&self.buffer);
            let file = self.file.as_mut().unwrap();
            if file.write_all(bytes).and_then(|_| file.flush()).is_ok() {
                // Success! Reset retry timer and move to outbound
                self.last_retry = None;
                let processed_count = self.buffer.len();
                for tx in self.buffer.drain(..) {
                    let mut tx = tx;
                    loop {
                        match self.outbound.push(tx) {
                            Ok(_) => break,
                            Err(returned_tx) => {
                                tx = returned_tx;
                                std::thread::yield_now();
                            }
                        }
                    }
                }
                self.step
                    .fetch_add(processed_count as u64, std::sync::atomic::Ordering::Relaxed);
            } else {
                self.last_retry = Some(Instant::now());
            }
            // If write failed, buffer remains full and will be retried in next tick (after 1s)
        }
    }
}
