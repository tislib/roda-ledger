use std::io::BufRead;
use std::path::Path;

use crate::entities::*;

use super::json::{compute_tx_crc, json_to_wal_entry};
use super::{CtlError, make_storage};

pub fn run(input: &Path, out: Option<&Path>, no_validate: bool) -> Result<(), CtlError> {
    let reader: Box<dyn BufRead> = if input == Path::new("-") {
        Box::new(std::io::stdin().lock())
    } else {
        Box::new(std::io::BufReader::new(
            std::fs::File::open(input)
                .map_err(|e| CtlError::new(format!("cannot open {:?}: {}", input, e)))?,
        ))
    };

    let mut entries: Vec<WalEntry> = Vec::new();
    for (i, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| CtlError::new(format!("line {}: {}", i + 1, e)))?;
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(&line)
            .map_err(|e| CtlError::new(format!("invalid JSON at line {}: {}", i + 1, e)))?;
        let entry = json_to_wal_entry(&value)
            .map_err(|e| CtlError::new(format!("invalid record at line {}: {}", i + 1, e)))?;
        entries.push(entry);
    }

    if entries.is_empty() {
        return Err(CtlError::new("no records found in input"));
    }

    if !no_validate {
        validate_structure(&entries)?;
    }

    recompute_crcs(&mut entries);

    let output = match out {
        Some(p) => p.to_path_buf(),
        None => {
            if input == Path::new("-") {
                return Err(CtlError::new("--out is required when reading from stdin"));
            }
            input.with_extension("bin")
        }
    };

    // Ensure output directory exists and is a valid storage directory
    let mut output_data_dir = output
        .parent()
        .unwrap_or(Path::new("."))
        .to_string_lossy()
        .to_string();
    if output_data_dir.is_empty() {
        output_data_dir = ".".to_string();
    }
    let _ = make_storage(&output_data_dir)?;

    // Write via a temporary Storage + active segment + append_entries
    let temp_dir =
        tempfile::tempdir().map_err(|e| CtlError::new(format!("cannot create temp dir: {}", e)))?;

    let storage = make_storage(&temp_dir.path().to_string_lossy())?;

    let mut segment = storage.active_segment()?;
    segment.write_entries(&entries);

    // Copy the written wal.bin to the desired output path
    let src = temp_dir.path().join("wal.bin");
    std::fs::copy(&src, &output)
        .map_err(|e| CtlError::new(format!("cannot write {:?}: {}", output, e)))?;

    eprintln!("Packed {} records to {}", entries.len(), output.display());
    Ok(())
}

fn validate_structure(entries: &[WalEntry]) -> Result<(), CtlError> {
    if !matches!(entries[0], WalEntry::SegmentHeader(_)) {
        return Err(CtlError::new("first record must be SegmentHeader"));
    }
    if !matches!(entries.last(), Some(WalEntry::SegmentSealed(_))) {
        return Err(CtlError::new("last record must be SegmentSealed"));
    }

    let header_id = match &entries[0] {
        WalEntry::SegmentHeader(h) => h.segment_id,
        _ => unreachable!(),
    };
    let (sealed_id, sealed_count) = match entries.last().unwrap() {
        WalEntry::SegmentSealed(s) => (s.segment_id, s.record_count),
        _ => unreachable!(),
    };
    if header_id != sealed_id {
        return Err(CtlError::new(format!(
            "segment_id mismatch: SegmentHeader={}, SegmentSealed={}",
            header_id, sealed_id
        )));
    }
    if sealed_count != entries.len() as u64 {
        return Err(CtlError::new(format!(
            "record_count mismatch: SegmentSealed says {}, actual {}",
            sealed_count,
            entries.len()
        )));
    }

    let mut last_tx_id: Option<u64> = None;
    for entry in entries {
        if let WalEntry::Metadata(m) = entry {
            if let Some(prev) = last_tx_id
                && m.tx_id <= prev
            {
                return Err(CtlError::new(format!(
                    "tx_id not monotonically increasing: {} after {}",
                    m.tx_id, prev
                )));
            }
            last_tx_id = Some(m.tx_id);
        }
    }
    Ok(())
}

fn recompute_crcs(entries: &mut [WalEntry]) {
    let mut i = 0;
    while i < entries.len() {
        if let WalEntry::Metadata(ref meta) = entries[i] {
            let count = meta.entry_count as usize;
            let mut tx_entries: Vec<TxEntry> = Vec::with_capacity(count);
            for j in 1..=count {
                if i + j < entries.len()
                    && let WalEntry::Entry(e) = &entries[i + j]
                {
                    tx_entries.push(*e);
                }
            }
            let mut m = *meta;
            m.crc32c = compute_tx_crc(&m, &tx_entries);
            entries[i] = WalEntry::Metadata(m);
            i += 1 + count;
        } else {
            i += 1;
        }
    }
}
