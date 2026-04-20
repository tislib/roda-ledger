use roda_ledger::config::{LedgerConfig, StorageConfig};
use roda_ledger::entities::{
    EntryKind, FailReason, TxEntry, TxMetadata, WalEntry, WalEntryKind,
};
use roda_ledger::ledger::Ledger;
use roda_ledger::replication::{AppendEntries, AppendOk, Replication};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const TEST_TERM: u64 = 1;

pub struct TempDir {
    pub path: PathBuf,
}

impl TempDir {
    pub fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = PathBuf::from(format!("temp_replication_{}_{}", tag, nanos));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    pub fn as_str(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub fn config_for(data_dir: &str, replication_mode: bool) -> LedgerConfig {
    let mut cfg = LedgerConfig {
        queue_size: 1 << 14,
        max_accounts: 1_000_000,
        storage: StorageConfig {
            data_dir: data_dir.to_string(),
            temporary: false,
            snapshot_frequency: 2,
            transaction_count_per_segment: 10_000_000,
        },
        seal_check_internal: Duration::from_millis(10),
        disable_seal: true,
        ..Default::default()
    };
    cfg.replication_mode = replication_mode;
    cfg
}

pub fn start_ledger(cfg: LedgerConfig) -> Arc<Ledger> {
    let mut ledger = Ledger::new(cfg);
    ledger.start().expect("ledger start");
    Arc::new(ledger)
}

pub fn build_tx(tx_id: u64, account_id: u64, amount: u64) -> Vec<u8> {
    let entry = TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::Credit,
        _pad0: [0; 6],
        tx_id,
        account_id,
        amount,
        computed_balance: amount as i64,
    };
    let mut meta = TxMetadata {
        entry_type: WalEntryKind::TxMetadata as u8,
        entry_count: 1,
        link_count: 0,
        fail_reason: FailReason::NONE,
        crc32c: 0,
        tx_id,
        timestamp: 0,
        user_ref: 0,
        tag: [0; 8],
    };
    let mut digest = crc32c::crc32c(bytemuck::bytes_of(&meta));
    digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&entry));
    meta.crc32c = digest;

    let mut out = Vec::with_capacity(80);
    out.extend_from_slice(bytemuck::bytes_of(&meta));
    out.extend_from_slice(bytemuck::bytes_of(&entry));
    out
}

pub fn build_range(first_tx_id: u64, count: u64, account_id: u64, amount_each: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity((count as usize) * 80);
    let mut running: i64 = 0;
    for i in 0..count {
        running += amount_each as i64;
        bytes.extend(build_tx_with_balance(
            first_tx_id + i,
            account_id,
            amount_each,
            running,
        ));
    }
    bytes
}

pub fn build_tx_with_balance(
    tx_id: u64,
    account_id: u64,
    amount: u64,
    computed_balance: i64,
) -> Vec<u8> {
    let entry = TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::Credit,
        _pad0: [0; 6],
        tx_id,
        account_id,
        amount,
        computed_balance,
    };
    let mut meta = TxMetadata {
        entry_type: WalEntryKind::TxMetadata as u8,
        entry_count: 1,
        link_count: 0,
        fail_reason: FailReason::NONE,
        crc32c: 0,
        tx_id,
        timestamp: 0,
        user_ref: 0,
        tag: [0; 8],
    };
    let mut digest = crc32c::crc32c(bytemuck::bytes_of(&meta));
    digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&entry));
    meta.crc32c = digest;

    let mut out = Vec::with_capacity(80);
    out.extend_from_slice(bytemuck::bytes_of(&meta));
    out.extend_from_slice(bytemuck::bytes_of(&entry));
    out
}

#[allow(dead_code)]
pub fn decode_range(data: &[u8]) -> Vec<WalEntry> {
    let mut out = Vec::with_capacity(data.len() / 40);
    let mut off = 0;
    while off + 40 <= data.len() {
        let kind = data[off];
        match kind {
            k if k == WalEntryKind::TxMetadata as u8 => {
                let m: TxMetadata = bytemuck::pod_read_unaligned(&data[off..off + 40]);
                out.push(WalEntry::Metadata(m));
            }
            k if k == WalEntryKind::TxEntry as u8 => {
                let e: TxEntry = bytemuck::pod_read_unaligned(&data[off..off + 40]);
                out.push(WalEntry::Entry(e));
            }
            _ => {}
        }
        off += 40;
    }
    out
}

pub fn make_append<'a>(
    from_tx_id: u64,
    to_tx_id: u64,
    wal_bytes: &'a [u8],
) -> AppendEntries<'a> {
    let (prev_tx_id, prev_term) = if from_tx_id <= 1 {
        (0, 0)
    } else {
        (from_tx_id - 1, TEST_TERM)
    };
    AppendEntries {
        term: TEST_TERM,
        prev_tx_id,
        prev_term,
        from_tx_id,
        to_tx_id,
        wal_bytes,
        leader_commit_tx_id: to_tx_id,
    }
}

pub fn apply_ok(replication: &Replication, req: AppendEntries<'_>, last_local_tx_id: u64) -> AppendOk {
    replication
        .process(req, last_local_tx_id)
        .expect("process should succeed on a well-formed request")
}

pub fn wait_for_commit(ledger: &Ledger, target: u64, timeout: Duration) {
    let start = Instant::now();
    while ledger.last_commit_id() < target {
        if start.elapsed() >= timeout {
            panic!(
                "timed out waiting for last_commit_id >= {} (saw {})",
                target,
                ledger.last_commit_id()
            );
        }
        std::thread::yield_now();
    }
}
