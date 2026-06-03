use storage::entities::TxLinkKind;
use storage::entities::*;
use storage::entities::{decode_tag, encode_tag};
use storage::wal_serializer::serialize_wal_records;

/// Render a single `WalEntry` as inspection-friendly JSON.
///
/// `current_tx_id` is the `tx_id` of the most recent `TxMetadata` the
/// caller has seen in the stream. Storage no longer carries `tx_id`
/// on follower records (`TxEntry`/`TxLink`), so the renderer accepts
/// it from the caller to keep the JSON output self-describing.
pub fn wal_entry_to_json(entry: &WalEntry, current_tx_id: u64) -> serde_json::Value {
    match entry {
        WalEntry::Metadata(m) => serde_json::json!({
            "type": "TxMetadata",
            "tx_id": m.tx_id,
            "sub_item_count": m.sub_item_count,
            "fail_reason": m.fail_reason.as_u8(),
            "crc32c": format!("{:#010x}", m.crc32c),
            "user_ref": m.user_ref,
            "timestamp": m.timestamp,
            "tag": encode_tag(&m.tag),
        }),
        WalEntry::Entry(e) => serde_json::json!({
            "type": "TxEntry",
            "tx_id": current_tx_id,
            "account_id": e.account_id,
            "amount": e.amount,
            "kind": if e.kind == EntryKind::Credit { "Credit" } else { "Debit" },
            "computed_balance": e.computed_balance,
        }),
        WalEntry::Link(l) => serde_json::json!({
            "type": "TxLink",
            "kind": match l.kind() {
                TxLinkKind::Duplicate => "Duplicate",
                TxLinkKind::Reversal => "Reversal",
            },
            "tx_id": current_tx_id,
            "to_tx_id": l.to_tx_id,
        }),
        WalEntry::FunctionRegistered(f) => serde_json::json!({
            "type": "FunctionRegistered",
            "name": f.name_str(),
            "version": f.version,
            "crc32c": format!("{:#010x}", f.crc32c),
            "unregister": f.is_unregister(),
        }),
        WalEntry::Term(_) => serde_json::json!({ "type": "Term" }),
    }
}

pub fn json_to_wal_entry(value: &serde_json::Value) -> Result<WalEntry, String> {
    let entry_type = value["type"].as_str().ok_or("missing 'type' field")?;

    match entry_type {
        "TxMetadata" => {
            let tx_id = value["tx_id"].as_u64().ok_or("missing tx_id")?;
            let sub_item_count = value["sub_item_count"]
                .as_u64()
                .ok_or("missing sub_item_count")? as u16;
            let fail_reason = FailReason::from_u8(value["fail_reason"].as_u64().unwrap_or(0) as u8);
            let user_ref = value.get("user_ref").and_then(|v| v.as_u64()).unwrap_or(0);
            let timestamp = value.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
            let tag = value
                .get("tag")
                .and_then(|v| v.as_str())
                .map(decode_tag)
                .unwrap_or([0u8; 8]);
            Ok(WalEntry::Metadata(TxMetadata {
                entry_type: WalEntryKind::TxMetadata as u8,
                fail_reason,
                sub_item_count,
                crc32c: 0,
                tx_id,
                timestamp,
                user_ref,
                tag,
            }))
        }
        "TxEntry" => {
            // `tx_id` in the JSON is informational only — the storage
            // entity inherits it from the preceding TxMetadata.
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
                _pad1: [0; 8],
                account_id,
                amount,
                computed_balance,
            }))
        }
        "TxLink" => {
            let to_tx_id = value["to_tx_id"].as_u64().ok_or("missing to_tx_id")?;
            let kind = match value["kind"].as_str().ok_or("missing kind")? {
                "Duplicate" => TxLinkKind::Duplicate,
                "Reversal" => TxLinkKind::Reversal,
                other => return Err(format!("unknown link kind: {}", other)),
            };
            Ok(WalEntry::Link(TxLink {
                entry_type: WalEntryKind::Link as u8,
                link_kind: kind as u8,
                _pad: [0; 6],
                _pad1: [0; 8],
                to_tx_id,
                _pad2: [0; 16],
            }))
        }
        "FunctionRegistered" => {
            let name = value["name"].as_str().ok_or("missing name")?;
            let version = value["version"].as_u64().ok_or("missing version")? as u16;
            let crc32c = value
                .get("crc32c")
                .and_then(|v| v.as_str())
                .and_then(parse_hex_u32)
                .unwrap_or(0);
            Ok(WalEntry::FunctionRegistered(FunctionRegistered::new(
                name, version, crc32c,
            )))
        }
        other => Err(format!("unknown record type: {}", other)),
    }
}

pub fn compute_tx_crc(meta: &TxMetadata, sub_items: &[WalEntry]) -> u32 {
    let mut m = *meta;
    m.crc32c = 0;
    let mut digest = crc32c::crc32c(bytemuck::bytes_of(&m));
    for entry in sub_items {
        digest = crc32c::crc32c_append(digest, serialize_wal_records(entry));
    }
    digest
}

pub fn verify_tx_crc(meta: &TxMetadata, sub_items: &[WalEntry]) -> bool {
    compute_tx_crc(meta, sub_items) == meta.crc32c
}

fn parse_hex_u32(s: &str) -> Option<u32> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    u32::from_str_radix(s, 16).ok()
}
