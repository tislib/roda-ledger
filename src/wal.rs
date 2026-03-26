use crate::entities::{TxEntry, TxMetadata, WalEntry, WalEntryKind};
use crossbeam_queue::ArrayQueue;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct Wal {
    inbound: Arc<ArrayQueue<WalEntry>>,
    outbound: Arc<ArrayQueue<WalEntry>>,
    file_path: Option<PathBuf>,
    last_processed_transaction_id: Arc<AtomicU64>,
    in_memory: bool,
    running: Arc<AtomicBool>,
}

pub struct WalRunner {
    inbound: Arc<ArrayQueue<WalEntry>>,
    outbound: Arc<ArrayQueue<WalEntry>>,
    file: Option<File>,
    file_path: Option<PathBuf>,
    buffer: Vec<WalEntry>,
    write_buffer: Vec<u8>,
    last_retry: Option<Instant>,
    last_processed_transaction_id: Arc<AtomicU64>,
    current_offset: u64,
    pending_entries: u8,
    running: Arc<AtomicBool>,
}

impl Wal {
    pub fn new(
        inbound: Arc<ArrayQueue<WalEntry>>,
        outbound: Arc<ArrayQueue<WalEntry>>,
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

    pub fn get_records(&self, offset: u64) -> Vec<WalEntry> {
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

        let mut reader = std::io::BufReader::with_capacity(128 * 1024, file);
        let mut records = Vec::new();

        loop {
            let (kind, size) = {
                let buffer = match reader.fill_buf() {
                    Ok(b) => {
                        if b.is_empty() {
                            break;
                        }
                        b
                    }
                    Err(_) => break,
                };

                let kind = buffer[0];
                let size = match kind {
                    k if k == WalEntryKind::TxMetadata as u8 => 32,
                    k if k == WalEntryKind::TxEntry as u8 => 32,
                    _ => break,
                };

                if buffer.len() < 1 + size {
                    // Not enough data for full record in buffer, need to read more
                    // We'll fall back to read_exact for this record
                    (kind, Some(size))
                } else {
                    // We have the full record in buffer
                    let record_data = &buffer[1..1 + size];
                    if kind == WalEntryKind::TxMetadata as u8 {
                        let metadata: TxMetadata = bytemuck::pod_read_unaligned(record_data);
                        records.push(WalEntry::Metadata(metadata));
                    } else {
                        let entry: TxEntry = bytemuck::pod_read_unaligned(record_data);
                        records.push(WalEntry::Entry(entry));
                    }
                    reader.consume(1 + size);
                    continue;
                }
            };

            // Fallback for when record spans across BufReader's internal buffer
            if let Some(size) = size {
                let mut full_record = vec![0u8; 1 + size];
                if reader.read_exact(&mut full_record).is_err() {
                    break;
                }
                let record_data = &full_record[1..];
                if kind == WalEntryKind::TxMetadata as u8 {
                    let metadata: TxMetadata = bytemuck::pod_read_unaligned(record_data);
                    records.push(WalEntry::Metadata(metadata));
                } else {
                    let entry: TxEntry = bytemuck::pod_read_unaligned(record_data);
                    records.push(WalEntry::Entry(entry));
                }
            } else {
                break;
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

    fn create_runner(&self) -> WalRunner {
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

        let buffer_capacity = (128 * 1024) / std::mem::size_of::<WalEntry>();

        WalRunner {
            inbound: self.inbound.clone(),
            outbound: self.outbound.clone(),
            file,
            file_path: self.file_path.clone(),
            buffer: Vec::with_capacity(buffer_capacity.max(1)),
            write_buffer: Vec::with_capacity(buffer_capacity.max(1) * 33),
            last_retry: None,
            last_processed_transaction_id: self.last_processed_transaction_id.clone(),
            current_offset,
            pending_entries: 0,
            running: self.running.clone(),
        }
    }
}

impl WalRunner {
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

            // Prepare data in write buffer
            self.write_buffer.clear();
            for entry in &self.buffer {
                self.write_buffer.push(entry.kind() as u8);
                match entry {
                    WalEntry::Metadata(m) => {
                        self.write_buffer.extend_from_slice(bytemuck::bytes_of(m))
                    }
                    WalEntry::Entry(e) => {
                        self.write_buffer.extend_from_slice(bytemuck::bytes_of(e))
                    }
                }
            }

            // Try to write buffer
            let success = if let Some(ref mut file) = self.file {
                match file.write_all(&self.write_buffer) {
                    Ok(_) => file.flush().is_ok(),
                    Err(_) => false,
                }
            } else {
                false
            };

            if success {
                // Success! Reset retry timer and move to outbound
                self.last_retry = None;
                self.current_offset += self.write_buffer.len() as u64;

                let mut local_last_id = self.last_processed_transaction_id.load(Ordering::Relaxed);

                for tx in self.buffer.drain(..) {
                    let tx_id = tx.tx_id();
                    match tx {
                        WalEntry::Metadata(m) => self.pending_entries = m.entry_count,
                        WalEntry::Entry(_) => {
                            if self.pending_entries > 0 {
                                self.pending_entries -= 1;
                            }
                        }
                    }

                    if self.pending_entries == 0 {
                        local_last_id = tx_id;
                    }

                    let mut tx = tx;
                    loop {
                        match self.outbound.push(tx) {
                            Ok(_) => break,
                            Err(returned_tx) => {
                                tx = returned_tx;
                                if !self.running.load(Ordering::Relaxed) {
                                    self.last_processed_transaction_id
                                        .store(local_last_id, Ordering::Relaxed);
                                    return;
                                }
                                std::thread::yield_now();
                            }
                        }
                    }
                }
                self.last_processed_transaction_id
                    .store(local_last_id, Ordering::Relaxed);
            } else {
                self.last_retry = Some(Instant::now());
            }
            // If write failed, buffer remains full and will be retried in next tick (after 1s)
        }
    }

    pub fn run_in_memory(&mut self) {
        while self.running.load(Ordering::Relaxed) {
            if let Some(tx) = self.inbound.pop() {
                let tx_id = tx.tx_id();
                match tx {
                    WalEntry::Metadata(m) => self.pending_entries = m.entry_count,
                    WalEntry::Entry(_) => {
                        if self.pending_entries > 0 {
                            self.pending_entries -= 1;
                        }
                    }
                }

                let mut tx = tx;
                loop {
                    match self.outbound.push(tx) {
                        Ok(_) => break,
                        Err(returned_tx) => {
                            tx = returned_tx;
                            if !self.running.load(Ordering::Relaxed) {
                                if self.pending_entries == 0 {
                                    self.last_processed_transaction_id
                                        .store(tx_id, Ordering::Relaxed);
                                }
                                return;
                            }
                            std::thread::yield_now();
                        }
                    }
                }
                if self.pending_entries == 0 {
                    self.last_processed_transaction_id
                        .store(tx_id, Ordering::Relaxed);
                }
            } else {
                std::thread::yield_now();
            }
        }
    }
}
