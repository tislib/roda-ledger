use crate::entities::TxLinkKind;
use crate::entities::*;
use crate::storage::{WAL_MAGIC, WAL_VERSION};

pub fn wal_entry_to_json(entry: &WalEntry) -> serde_json::Value {
    match entry {
        WalEntry::SegmentHeader(h) => serde_json::json!({
            "type": "SegmentHeader",
            "segment_id": h.segment_id,
            "version": h.version,
            "magic": format!("{:#010x}", h.magic),
            "first_tx_id": h.first_tx_id,
        }),
        WalEntry::Metadata(m) => serde_json::json!({
            "type": "TxMetadata",
            "tx_id": m.tx_id,
            "entry_count": m.entry_count,
            "link_count": m.link_count,
            "fail_reason": m.fail_reason.as_u8(),
            "crc32c": format!("{:#010x}", m.crc32c),
            "user_ref": m.user_ref,
            "timestamp": m.timestamp,
            "tag": hex_encode_tag(&m.tag),
        }),
        WalEntry::Entry(e) => serde_json::json!({
            "type": "TxEntry",
            "tx_id": e.tx_id,
            "account_id": e.account_id,
            "amount": e.amount,
            "kind": if e.kind == EntryKind::Credit { "Credit" } else { "Debit" },
            "computed_balance": e.computed_balance,
        }),
        WalEntry::SegmentSealed(s) => serde_json::json!({
            "type": "SegmentSealed",
            "segment_id": s.segment_id,
            "last_tx_id": s.last_tx_id,
            "record_count": s.record_count,
        }),
        WalEntry::Link(l) => serde_json::json!({
            "type": "TxLink",
            "kind": match l.kind() {
                TxLinkKind::Duplicate => "Duplicate",
                TxLinkKind::Reversal => "Reversal",
            },
            "tx_id": l.tx_id,
            "to_tx_id": l.to_tx_id,
        }),
    }
}

pub fn json_to_wal_entry(value: &serde_json::Value) -> Result<WalEntry, String> {
    let entry_type = value["type"].as_str().ok_or("missing 'type' field")?;

    match entry_type {
        "SegmentHeader" => {
            let segment_id = value["segment_id"].as_u64().ok_or("missing segment_id")? as u32;
            let version = value
                .get("version")
                .and_then(|v| v.as_u64())
                .unwrap_or(WAL_VERSION as u64) as u8;
            let magic = value
                .get("magic")
                .and_then(|v| v.as_str())
                .and_then(parse_hex_u32)
                .unwrap_or(WAL_MAGIC);
            let first_tx_id = value["first_tx_id"].as_u64().ok_or("missing first_tx_id")?;
            Ok(WalEntry::SegmentHeader(SegmentHeader {
                entry_type: WalEntryKind::SegmentHeader as u8,
                version,
                _pad0: [0; 2],
                magic,
                segment_id,
                _pad1: [0; 4],
                first_tx_id,
                _pad2: [0; 16],
            }))
        }
        "TxMetadata" => {
            let tx_id = value["tx_id"].as_u64().ok_or("missing tx_id")?;
            let entry_count = value["entry_count"].as_u64().ok_or("missing entry_count")? as u8;
            let link_count = value
                .get("link_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u8;
            let fail_reason = FailReason::from_u8(value["fail_reason"].as_u64().unwrap_or(0) as u8);
            let user_ref = value.get("user_ref").and_then(|v| v.as_u64()).unwrap_or(0);
            let timestamp = value.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
            let tag = value
                .get("tag")
                .and_then(|v| v.as_str())
                .map(hex_decode_tag)
                .unwrap_or([0u8; 8]);
            Ok(WalEntry::Metadata(TxMetadata {
                entry_type: WalEntryKind::TxMetadata as u8,
                entry_count,
                link_count,
                fail_reason,
                crc32c: 0,
                tx_id,
                timestamp,
                user_ref,
                tag,
            }))
        }
        "TxEntry" => {
            let tx_id = value["tx_id"].as_u64().ok_or("missing tx_id")?;
            let account_id = value["account_id"].as_u64().ok_or("missing account_id")?;
            let amount = value["amount"].as_u64().ok_or("missing amount")?;
            let kind = match value["kind"].as_str().ok_or("missing kind")? {
                "Credit" => EntryKind::Credit,
                "Debit" => EntryKind::Debit,
                other => return Err(format!("unknown kind: {}", other)),
            };
            let computed_balance = value["computed_balance"]
                .as_i64()
                .ok_or("missing computed_balance")?;
            Ok(WalEntry::Entry(TxEntry {
                entry_type: WalEntryKind::TxEntry as u8,
                kind,
                _pad0: [0; 6],
                tx_id,
                account_id,
                amount,
                computed_balance,
            }))
        }
        "SegmentSealed" => {
            let segment_id = value["segment_id"].as_u64().ok_or("missing segment_id")? as u32;
            let last_tx_id = value["last_tx_id"].as_u64().ok_or("missing last_tx_id")?;
            let record_count = value["record_count"]
                .as_u64()
                .ok_or("missing record_count")?;
            Ok(WalEntry::SegmentSealed(SegmentSealed {
                entry_type: WalEntryKind::SegmentSealed as u8,
                _pad0: [0; 3],
                segment_id,
                last_tx_id,
                record_count,
                _pad1: [0; 16],
            }))
        }
        "TxLink" => {
            let to_tx_id = value["to_tx_id"].as_u64().ok_or("missing to_tx_id")?;
            let tx_id = value["tx_id"].as_u64().ok_or("missing tx_id")?;
            let kind = match value["kind"].as_str().ok_or("missing kind")? {
                "Duplicate" => TxLinkKind::Duplicate,
                "Reversal" => TxLinkKind::Reversal,
                other => return Err(format!("unknown link kind: {}", other)),
            };
            Ok(WalEntry::Link(TxLink {
                entry_type: WalEntryKind::Link as u8,
                link_kind: kind as u8,
                _pad: [0; 6],
                tx_id,
                to_tx_id,
                _pad2: [0; 16],
            }))
        }
        other => Err(format!("unknown record type: {}", other)),
    }
}

pub fn compute_tx_crc(meta: &TxMetadata, entries: &[TxEntry]) -> u32 {
    compute_tx_crc_with_links(meta, entries, &[])
}

pub fn compute_tx_crc_with_links(meta: &TxMetadata, entries: &[TxEntry], links: &[TxLink]) -> u32 {
    let mut m = *meta;
    m.crc32c = 0;
    let mut digest = crc32c::crc32c(bytemuck::bytes_of(&m));
    for e in entries {
        digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(e));
    }
    for l in links {
        digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(l));
    }
    digest
}

pub fn verify_tx_crc_with_links(meta: &TxMetadata, entries: &[TxEntry], links: &[TxLink]) -> bool {
    compute_tx_crc_with_links(meta, entries, links) == meta.crc32c
}

fn hex_encode_tag(tag: &[u8; 8]) -> String {
    // Trim trailing null bytes and interpret as UTF-8; fall back to hex if invalid.
    let trimmed = match tag.iter().rposition(|&b| b != 0) {
        Some(last) => &tag[..=last],
        None => &tag[..0],
    };
    match std::str::from_utf8(trimmed) {
        Ok(s) => s.to_string(),
        Err(_) => format!(
            "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            tag[0], tag[1], tag[2], tag[3], tag[4], tag[5], tag[6], tag[7]
        ),
    }
}

fn hex_decode_tag(s: &str) -> [u8; 8] {
    let mut tag = [0u8; 8];
    // If it looks like a 16-char hex string (no non-hex chars other than those
    // in a plain UTF-8 tag), try hex first only when the string is exactly 16
    // hex digits; otherwise treat as a plain UTF-8 string padded with nulls.
    let is_hex16 = s.len() == 16 && s.chars().all(|c| c.is_ascii_hexdigit());
    if is_hex16 {
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            if i >= 8 {
                break;
            }
            if let Ok(cs) = std::str::from_utf8(chunk)
                && let Ok(b) = u8::from_str_radix(cs, 16)
            {
                tag[i] = b;
            }
        }
    } else {
        // Plain UTF-8 string — copy bytes, pad the rest with nulls.
        let bytes = s.as_bytes();
        let len = bytes.len().min(8);
        tag[..len].copy_from_slice(&bytes[..len]);
    }
    tag
}

fn parse_hex_u32(s: &str) -> Option<u32> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    u32::from_str_radix(s, 16).ok()
}
