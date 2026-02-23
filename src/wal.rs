use crate::balance::BalanceDataType;
use crate::transaction::{Transaction, TransactionDataType};
use crossbeam_queue::ArrayQueue;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct Wal<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    file_path: Option<PathBuf>,
    last_processed_transaction_id: Arc<AtomicU64>,
    in_memory: bool,
    running: Arc<AtomicBool>,
}

pub struct WalRunner<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    file: Option<File>,
    file_path: Option<PathBuf>,
    buffer: Vec<Transaction<Data, BalanceData>>,
    last_retry: Option<Instant>,
    last_processed_transaction_id: Arc<AtomicU64>,
    current_offset: u64,
    running: Arc<AtomicBool>,
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
        in_memory: bool,
        running: Arc<AtomicBool>,
    ) -> Self {
        let file_path = if in_memory {
            None
        } else {
            let folder = ledger_folder.unwrap_or("data");
            let path = Path::new(folder);
            if !path.exists() {
                let _ = std::fs::create_dir_all(path);
            }
            Some(path.join("wal.bin"))
        };

        Self {
            inbound,
            outbound,
            file_path,
            last_processed_transaction_id: Arc::new(Default::default()),
            in_memory,
            running,
        }
    }

    pub fn last_processed_transaction_id(&self) -> u64 {
        self.last_processed_transaction_id
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_records(&self, offset: u64) -> Vec<Transaction<Data, BalanceData>> {
        if self.in_memory || self.file_path.is_none() {
            return Vec::new();
        }

        let mut file = match File::open(self.file_path.as_ref().unwrap()) {
            Ok(f) => f,
            Err(_) => return Vec::new(),
        };

        if file.seek(SeekFrom::Start(offset)).is_err() {
            return Vec::new();
        }

        let mut records = Vec::new();
        let tx_size = std::mem::size_of::<Transaction<Data, BalanceData>>();
        let mut buf = vec![0u8; tx_size];

        loop {
            match file.read_exact(&mut buf) {
                Ok(_) => {
                    let tx: Transaction<Data, BalanceData> = *bytemuck::from_bytes(&buf);
                    records.push(tx);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(_) => break,
            }
        }
        records
    }

    pub fn set_last_processed_transaction_id(&self, last_id: u64) {
        self.last_processed_transaction_id
            .store(last_id, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn start(&self) -> JoinHandle<()> {
        let mut runner = self.create_runner();
        let in_memory = self.in_memory;
        std::thread::Builder::new()
            .name("wal".to_string())
            .spawn(move || {
                if in_memory {
                    runner.run_in_memory();
                } else {
                    runner.run();
                }
            })
            .unwrap()
    }

    fn create_runner(&self) -> WalRunner<Data, BalanceData> {
        let mut current_offset = 0;
        let file = if self.in_memory {
            None
        } else {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.file_path.as_ref().unwrap())
                .ok();

            if let Some(ref f) = file {
                current_offset = f.metadata().map(|m| m.len()).unwrap_or(0);
            }
            file
        };

        let buffer_capacity = (128 * 1024) / std::mem::size_of::<Transaction<Data, BalanceData>>();

        WalRunner {
            inbound: self.inbound.clone(),
            outbound: self.outbound.clone(),
            file,
            file_path: self.file_path.clone(),
            buffer: Vec::with_capacity(buffer_capacity.max(1)),
            last_retry: None,
            last_processed_transaction_id: self.last_processed_transaction_id.clone(),
            current_offset,
            running: self.running.clone(),
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
                .open(self.file_path.as_ref().unwrap())
            {
                self.current_offset = file.metadata().map(|m| m.len()).unwrap_or(0);
                self.file = Some(file);
            } else {
                self.last_retry = Some(Instant::now());
                return;
            }
        }

        while self.running.load(Ordering::Relaxed) {
            // Check if we should retry
            if self
                .last_retry
                .is_some_and(|last| Instant::now().duration_since(last) < Duration::from_secs(1))
            {
                std::thread::yield_now();
                continue;
            }

            // Fill buffer if empty
            if self.buffer.is_empty() {
                let count = self.inbound.len().min(self.buffer.capacity());
                if count == 0 {
                    std::thread::yield_now();
                    continue;
                }
                for _ in 0..count {
                    if let Some(tx) = self.inbound.pop() {
                        self.buffer.push(tx);
                    }
                }
            }

            if self.buffer.is_empty() {
                std::thread::yield_now();
                continue;
            }

            let tx_size = std::mem::size_of::<Transaction<Data, BalanceData>>() as u64;
            for (i, tx) in self.buffer.iter_mut().enumerate() {
                tx.wal_location = self.current_offset + (i as u64 * tx_size);
            }

            // Try to write buffer
            let success = {
                let bytes =
                    bytemuck::cast_slice::<Transaction<Data, BalanceData>, u8>(&self.buffer);
                let file = self.file.as_mut().unwrap();
                file.write_all(bytes).and_then(|_| file.flush()).is_ok()
            };

            if success {
                // Success! Reset retry timer and move to outbound
                self.last_retry = None;
                let processed_count = self.buffer.len();
                self.current_offset += processed_count as u64 * tx_size;
                let mut local_last_processed_transaction_id = 0;

                for tx in self.buffer.drain(..) {
                    let mut tx = tx;
                    loop {
                        match self.outbound.push(tx) {
                            Ok(_) => break,
                            Err(returned_tx) => {
                                tx = returned_tx;
                                if !self.running.load(Ordering::Relaxed) {
                                    return;
                                }
                                std::thread::yield_now();
                            }
                        }
                    }
                    local_last_processed_transaction_id = tx.id;
                }
                self.last_processed_transaction_id
                    .store(local_last_processed_transaction_id, Ordering::Relaxed);
            } else {
                self.last_retry = Some(Instant::now());
            }
            // If write failed, buffer remains full and will be retried in next tick (after 1s)
        }
    }

    pub fn run_in_memory(&mut self) {
        while self.running.load(Ordering::Relaxed) {
            if let Some(tx) = self.inbound.pop() {
                let mut tx = tx;
                loop {
                    match self.outbound.push(tx) {
                        Ok(_) => break,
                        Err(returned_tx) => {
                            tx = returned_tx;
                            if !self.running.load(Ordering::Relaxed) {
                                return;
                            }
                            std::thread::yield_now();
                        }
                    }
                }
                self.last_processed_transaction_id
                    .store(tx.id, Ordering::Relaxed);
            } else {
                std::thread::yield_now();
            }
        }
    }
}
