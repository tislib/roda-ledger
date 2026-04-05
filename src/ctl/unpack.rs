use std::io::{BufWriter, Write};
use std::path::Path;

use crate::entities::*;

use super::json::{
    compute_tx_crc_with_links, verify_tx_crc_with_links, wal_entry_to_json,
};
use super::{CtlError, open_segment_from_path};

pub fn run(segment_path: &Path, out: Option<&Path>, ignore_crc: bool) -> Result<(), CtlError> {
    let mut segment = open_segment_from_path(segment_path)?;
    segment.load()?;

    let out_writer: Box<dyn Write> = match out {
        Some(p) => Box::new(
            std::fs::File::create(p)
                .map_err(|e| CtlError::new(format!("cannot create {:?}: {}", p, e)))?,
        ),
        None => Box::new(std::io::stdout().lock()),
    };
    let mut writer = BufWriter::new(out_writer);

    let mut record_index = 0u64;
    let mut pending_meta: Option<(TxMetadata, u64)> = None;
    let mut pending_entries: Vec<TxEntry> = Vec::new();
    let mut pending_links: Vec<TxLink> = Vec::new();
    let mut pending_json: Vec<serde_json::Value> = Vec::new();
    let mut result: Result<(), CtlError> = Ok(());

    segment.visit_wal_records(|entry| {
        if result.is_err() {
            return;
        }

        match entry {
            WalEntry::Metadata(m) => {
                if let Some((prev, idx)) = pending_meta.take() {
                    if let Err(e) = flush_tx(
                        &prev,
                        &pending_entries,
                        &pending_links,
                        &pending_json,
                        idx,
                        ignore_crc,
                        &mut writer,
                    ) {
                        result = Err(e);
                        return;
                    }
                    pending_entries.clear();
                    pending_links.clear();
                    pending_json.clear();
                }

                if m.entry_count == 0 && m.link_count == 0 {
                    if !verify_tx_crc_with_links(m, &[], &[]) {
                        let msg = crc_error_msg(
                            record_index,
                            m.tx_id,
                            m.crc32c,
                            compute_tx_crc_with_links(m, &[], &[]),
                        );
                        if !ignore_crc {
                            result = Err(CtlError::new(msg));
                            return;
                        }
                        eprintln!("{}", msg);
                        let mut j = wal_entry_to_json(entry);
                        j["crc_error"] = serde_json::Value::Bool(true);
                        let _ = writeln!(writer, "{}", j);
                    } else {
                        let _ = writeln!(writer, "{}", wal_entry_to_json(entry));
                    }
                } else {
                    pending_meta = Some((*m, record_index));
                    pending_json.push(wal_entry_to_json(entry));
                }
            }
            WalEntry::Entry(e) => {
                if pending_meta.is_some() {
                    pending_entries.push(*e);
                    pending_json.push(wal_entry_to_json(entry));
                } else {
                    let _ = writeln!(writer, "{}", wal_entry_to_json(entry));
                }
            }
            WalEntry::Link(l) => {
                if pending_meta.is_some() {
                    pending_links.push(*l);
                    pending_json.push(wal_entry_to_json(entry));
                } else {
                    let _ = writeln!(writer, "{}", wal_entry_to_json(entry));
                }
            }
            WalEntry::SegmentHeader(_) | WalEntry::SegmentSealed(_) => {
                if let Some((prev, idx)) = pending_meta.take() {
                    if let Err(e) = flush_tx(
                        &prev,
                        &pending_entries,
                        &pending_links,
                        &pending_json,
                        idx,
                        ignore_crc,
                        &mut writer,
                    ) {
                        result = Err(e);
                        return;
                    }
                    pending_entries.clear();
                    pending_links.clear();
                    pending_json.clear();
                }
                let _ = writeln!(writer, "{}", wal_entry_to_json(entry));
            }
        }
        record_index += 1;
    })?;

    result?;

    if let Some((prev, idx)) = pending_meta.take() {
        flush_tx(
            &prev,
            &pending_entries,
            &pending_links,
            &pending_json,
            idx,
            ignore_crc,
            &mut writer,
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn flush_tx(
    meta: &TxMetadata,
    entries: &[TxEntry],
    links: &[TxLink],
    json_lines: &[serde_json::Value],
    record_index: u64,
    ignore_crc: bool,
    writer: &mut BufWriter<Box<dyn Write>>,
) -> Result<(), CtlError> {
    let ok = verify_tx_crc_with_links(meta, entries, links);
    if !ok {
        let msg = crc_error_msg(
            record_index,
            meta.tx_id,
            meta.crc32c,
            compute_tx_crc_with_links(meta, entries, links),
        );
        if !ignore_crc {
            return Err(CtlError::new(msg));
        }
        eprintln!("{}", msg);
    }
    for (i, j) in json_lines.iter().enumerate() {
        if !ok && i == 0 {
            let mut jc = j.clone();
            jc["crc_error"] = serde_json::Value::Bool(true);
            writeln!(writer, "{}", jc)?;
        } else {
            writeln!(writer, "{}", j)?;
        }
    }
    Ok(())
}

fn crc_error_msg(record: u64, tx_id: u64, expected: u32, actual: u32) -> String {
    format!(
        "CRC mismatch at record {} (tx_id {})\n  expected: {:#010x}\n  actual:   {:#010x}",
        record, tx_id, expected, actual
    )
}
