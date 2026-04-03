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
