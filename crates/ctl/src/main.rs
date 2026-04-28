use std::path::PathBuf;
use std::process;

use clap::{Parser, Subcommand};
use roda_ledger::ctl::RodaCtl;

#[derive(Parser)]
#[command(name = "roda-ctl", about = "Offline operational tools for roda-ledger")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Read a WAL segment binary and write its contents as NDJSON
    Unpack {
        /// Path to WAL segment binary file
        segment: PathBuf,
        /// Output file path (default: stdout)
        #[arg(long)]
        out: Option<PathBuf>,
        /// Continue past CRC failures, marking corrupt records
        #[arg(long)]
        ignore_crc: bool,
    },
    /// Read NDJSON and write a WAL segment binary
    Pack {
        /// Input JSON file (use - for stdin)
        input: PathBuf,
        /// Output binary file path
        #[arg(long)]
        out: Option<PathBuf>,
        /// Skip structural validation
        #[arg(long)]
        no_validate: bool,
    },
    /// Verify integrity of WAL segments and snapshots
    Verify {
        /// Data directory path
        path: PathBuf,
        /// Verify only a single segment by ID
        #[arg(long)]
        segment: Option<u32>,
        /// Verify a range of segments (e.g. 1..10)
        #[arg(long)]
        range: Option<String>,
    },
    /// Seal a WAL segment (compute CRC sidecar + seal marker)
    Seal {
        /// Path to WAL segment binary file
        segment: PathBuf,
        /// Force reseal of already-sealed segments
        #[arg(long)]
        force: bool,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Unpack {
            segment,
            out,
            ignore_crc,
        } => RodaCtl::unpack(&segment, out.as_deref(), ignore_crc),
        Commands::Pack {
            input,
            out,
            no_validate,
        } => RodaCtl::pack(&input, out.as_deref(), no_validate),
        Commands::Verify {
            path,
            segment,
            range,
        } => {
            let range_parsed = range.as_deref().and_then(parse_range);
            match RodaCtl::verify(&path, segment, range_parsed) {
                Ok(report) => {
                    report.print(&mut std::io::stderr(), &path);
                    if report.all_ok() {
                        Ok(())
                    } else {
                        Err(roda_ledger::ctl::CtlError::new("verification failed"))
                    }
                }
                Err(e) => Err(e),
            }
        }
        Commands::Seal { segment, force } => RodaCtl::seal(&segment, force),
    };

    if let Err(e) = result {
        eprintln!("ERROR: {}", e);
        process::exit(1);
    }
}

fn parse_range(s: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = s.split("..").collect();
    if parts.len() != 2 {
        return None;
    }
    Some((parts[0].parse().ok()?, parts[1].parse().ok()?))
}

mod json;
mod pack;
mod seal;
mod unpack;
mod verify;

use std::io::Write;
use std::path::Path;

use storage::{Segment, Storage, StorageConfig};

/// Top-level entry point for all `roda-ctl` operations.
/// The binary creates this struct and dispatches commands to it.
pub struct RodaCtl;

impl RodaCtl {
    pub fn unpack(
        segment_path: &Path,
        out: Option<&Path>,
        ignore_crc: bool,
    ) -> Result<(), CtlError> {
        unpack::run(segment_path, out, ignore_crc)
    }

    pub fn pack(input: &Path, out: Option<&Path>, no_validate: bool) -> Result<(), CtlError> {
        pack::run(input, out, no_validate)
    }

    pub fn verify(
        path: &Path,
        segment_filter: Option<u32>,
        range: Option<(u32, u32)>,
    ) -> Result<VerifyReport, CtlError> {
        verify::run(path, segment_filter, range)
    }

    pub fn seal(segment_path: &Path, force: bool) -> Result<(), CtlError> {
        seal::run(segment_path, force)
    }
}

pub fn open_segment_from_path(path: &Path) -> Result<Segment, CtlError> {
    let mut data_dir = path
        .parent()
        .unwrap_or(Path::new("."))
        .to_string_lossy()
        .to_string();

    if data_dir.is_empty() {
        data_dir = ".".to_string();
    }

    let storage = make_storage(&data_dir)?;
    let segment = match path.file_name().and_then(|f| f.to_str()) {
        Some("wal.bin") => storage.active_segment()?,
        _ => {
            let segment_id = parse_segment_id_from_path(path).ok_or_else(|| {
                CtlError::new(format!(
                    "cannot parse segment ID from {:?} (expected wal_NNNNNN.bin)",
                    path.file_name().unwrap_or_default()
                ))
            })?;
            storage.segment(segment_id)?
        }
    };
    Ok(segment)
}

// ── Error type ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct CtlError {
    pub message: String,
}

impl CtlError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
        }
    }
}

impl std::fmt::Display for CtlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CtlError {}

impl From<std::io::Error> for CtlError {
    fn from(e: std::io::Error) -> Self {
        CtlError::new(e.to_string())
    }
}

// ── Verify report ────────────────────────────────────────────────────────────

pub struct VerifyReport {
    pub segments: Vec<SegmentReport>,
    pub active: Option<SegmentReport>,
    pub cross_errors: Vec<String>,
}

pub struct SegmentReport {
    pub filename: String,
    pub status: String,
    pub record_count: u64,
    pub first_tx_id: u64,
    pub last_tx_id: u64,
    pub ok: bool,
    pub errors: Vec<String>,
    pub snapshot: Option<SnapshotReport>,
}

pub struct SnapshotReport {
    pub filename: String,
    pub account_count: u64,
    pub ok: bool,
    pub errors: Vec<String>,
}

impl VerifyReport {
    pub fn all_ok(&self) -> bool {
        self.segments.iter().all(|s| s.ok)
            && self.active.as_ref().is_none_or(|a| a.ok)
            && self.cross_errors.is_empty()
    }

    /// Prints the human-readable report to the given writer.
    pub fn print(&self, w: &mut impl Write, dir: &Path) {
        let _ = writeln!(w, "Verifying {} ...\n", dir.display());

        for r in &self.segments {
            Self::print_segment(w, r);
        }
        if let Some(r) = &self.active {
            Self::print_segment(w, r);
        }
        for err in &self.cross_errors {
            let _ = writeln!(w, "  {}", err);
        }

        let total_seg = self.segments.len() + if self.active.is_some() { 1 } else { 0 };
        let total_snap = self
            .segments
            .iter()
            .filter(|s| s.snapshot.is_some())
            .count();
        let total_txs: u64 = self
            .segments
            .iter()
            .map(|r| {
                if r.last_tx_id >= r.first_tx_id && r.first_tx_id > 0 {
                    r.last_tx_id - r.first_tx_id + 1
                } else {
                    0
                }
            })
            .sum();
        let status = if self.all_ok() {
            "all OK"
        } else {
            "ERRORS FOUND"
        };
        let _ = writeln!(
            w,
            "\nSummary: {} segments, {} snapshots, {} transactions -- {}",
            total_seg,
            total_snap,
            fmt_num(total_txs),
            status,
        );
    }

    fn print_segment(w: &mut impl Write, r: &SegmentReport) {
        let result = if r.ok { "OK" } else { "FAIL" };
        if r.status == "CLOSED" {
            let _ = writeln!(
                w,
                "  {:<20} {:<9} (awaiting seal){}OK",
                r.filename,
                r.status,
                " ".repeat(32),
            );
        } else {
            let _ = writeln!(
                w,
                "  {:<20} {:<9} {} records   tx {}..{}   {}",
                r.filename,
                r.status,
                fmt_num(r.record_count),
                fmt_num(r.first_tx_id),
                fmt_num(r.last_tx_id),
                result,
            );
        }
        for err in &r.errors {
            let _ = writeln!(w, "    {}", err);
        }
        if let Some(snap) = &r.snapshot {
            let sr = if snap.ok { "OK" } else { "FAIL" };
            let _ = writeln!(
                w,
                "    {:<18} {} accounts{}{}",
                snap.filename,
                fmt_num(snap.account_count),
                " ".repeat(24),
                sr,
            );
            for err in &snap.errors {
                let _ = writeln!(w, "      {}", err);
            }
        }
    }
}

// ── Shared helpers ───────────────────────────────────────────────────────────

fn parse_segment_id_from_path(path: &Path) -> Option<u32> {
    parse_segment_id_from_name(path.file_name()?.to_str()?)
}

fn parse_segment_id_from_name(name: &str) -> Option<u32> {
    let s = name.strip_prefix("wal_")?.strip_suffix(".bin")?;
    if s.len() == 6 && s.chars().all(|c| c.is_ascii_digit()) {
        s.parse().ok()
    } else {
        None
    }
}

pub(crate) fn make_storage(data_dir: &str) -> Result<Storage, CtlError> {
    Storage::new(StorageConfig {
        data_dir: data_dir.to_string(),
        temporary: false,
        ..Default::default()
    })
        .map_err(|e| CtlError::new(format!("cannot open data directory: {}", e)))
}

fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}
