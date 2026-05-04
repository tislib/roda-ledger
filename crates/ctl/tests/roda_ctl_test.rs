use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use storage::entities::*;
use storage::{WAL_MAGIC, WAL_VERSION};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn roda_ctl() -> Command {
    Command::new(env!("CARGO_BIN_EXE_roda-ctl"))
}

fn unique_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = PathBuf::from(format!("/tmp/roda_ctl_test_{}_{}", name, nanos));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn compute_tx_crc(meta: &TxMetadata, entries: &[TxEntry]) -> u32 {
    let mut m = *meta;
    m.crc32c = 0;
    let mut digest = crc32c::crc32c(bytemuck::bytes_of(&m));
    for e in entries {
        digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(e));
    }
    digest
}

fn create_test_segment(dir: &Path, segment_id: u32, tx_start: u64, num_txs: u32) -> PathBuf {
    let mut data = Vec::new();

    let header = SegmentHeader {
        entry_type: WalEntryKind::SegmentHeader as u8,
        version: WAL_VERSION,
        _pad0: [0; 2],
        magic: WAL_MAGIC,
        segment_id,
        _pad1: [0; 4],
        _pad2: [0; 24],
    };
    data.extend_from_slice(bytemuck::bytes_of(&header));

    let mut last_tx_id = tx_start;
    for i in 0..num_txs {
        let tx_id = tx_start + i as u64;
        last_tx_id = tx_id;

        // Real convention (transactor.rs): Debit → balance += amount, Credit → balance -= amount.
        // account_id=1 is the receiving side (Debit, balance grows positive).
        // account_id=0 is the sending side  (Credit, balance grows negative).
        let e1 = TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind: EntryKind::Debit,
            _pad0: [0; 6],
            tx_id,
            account_id: 1,
            amount: 100,
            computed_balance: 100 * (i as i64 + 1),
        };
        let e2 = TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind: EntryKind::Credit,
            _pad0: [0; 6],
            tx_id,
            account_id: 0,
            amount: 100,
            computed_balance: -(100 * (i as i64 + 1)),
        };
        let mut meta = TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            fail_reason: FailReason::NONE,
            sub_item_count: 2,
            crc32c: 0,
            tx_id,
            timestamp: 1_700_000_000 + i as u64,
            user_ref: 0,
            tag: [0; 8],
        };
        meta.crc32c = compute_tx_crc(&meta, &[e1, e2]);

        data.extend_from_slice(bytemuck::bytes_of(&meta));
        data.extend_from_slice(bytemuck::bytes_of(&e1));
        data.extend_from_slice(bytemuck::bytes_of(&e2));
    }

    let record_count = 1 + (num_txs as u64) * 3 + 1;
    let sealed = SegmentSealed {
        entry_type: WalEntryKind::SegmentSealed as u8,
        _pad0: [0; 3],
        segment_id,
        last_tx_id,
        record_count,
        _pad1: [0; 16],
    };
    data.extend_from_slice(bytemuck::bytes_of(&sealed));

    let path = dir.join(format!("wal_{:06}.bin", segment_id));
    fs::write(&path, &data).unwrap();
    path
}

fn seal_segment_files(dir: &Path, segment_id: u32) {
    let wal = fs::read(dir.join(format!("wal_{:06}.bin", segment_id))).unwrap();
    let crc = crc32c::crc32c(&wal);
    let mut sidecar = [0u8; 16];
    sidecar[0..4].copy_from_slice(&crc.to_le_bytes());
    sidecar[4..12].copy_from_slice(&(wal.len() as u64).to_le_bytes());
    sidecar[12..16].copy_from_slice(&WAL_MAGIC.to_le_bytes());
    fs::write(dir.join(format!("wal_{:06}.crc", segment_id)), sidecar).unwrap();
    fs::write(dir.join(format!("wal_{:06}.seal", segment_id)), []).unwrap();
}

// ── Unpack tests ─────────────────────────────────────────────────────────────

#[test]
fn unpack_produces_valid_ndjson() {
    let dir = unique_dir("unpack_ndjson");
    create_test_segment(&dir, 1, 1, 3);

    let out = roda_ctl()
        .args(["unpack", dir.join("wal_000001.bin").to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8(out.stdout).unwrap();
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert_eq!(lines.len(), 11); // 1 header + 3*(1+2) + 1 sealed

    for (i, l) in lines.iter().enumerate() {
        let _: serde_json::Value =
            serde_json::from_str(l).unwrap_or_else(|e| panic!("line {} not JSON: {}", i, e));
    }

    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["type"], "SegmentHeader");
    assert_eq!(first["segment_id"], 1);

    let last: serde_json::Value = serde_json::from_str(lines[10]).unwrap();
    assert_eq!(last["type"], "SegmentSealed");
    assert_eq!(last["record_count"], 11);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn unpack_detects_crc_mismatch() {
    let dir = unique_dir("unpack_crc");
    let wal = create_test_segment(&dir, 1, 1, 2);
    let mut data = fs::read(&wal).unwrap();
    data[104] ^= 0xFF; // corrupt TxEntry
    fs::write(&wal, &data).unwrap();

    let out = roda_ctl()
        .args(["unpack", wal.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("CRC mismatch"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn unpack_ignore_crc_continues() {
    let dir = unique_dir("unpack_ignore");
    let wal = create_test_segment(&dir, 1, 1, 2);
    let mut data = fs::read(&wal).unwrap();
    data[104] ^= 0xFF;
    fs::write(&wal, &data).unwrap();

    let out = roda_ctl()
        .args(["unpack", wal.to_str().unwrap(), "--ignore-crc"])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8(out.stdout).unwrap().contains("crc_error"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn unpack_to_file() {
    let dir = unique_dir("unpack_file");
    create_test_segment(&dir, 1, 1, 2);
    let outf = dir.join("out.json");

    let out = roda_ctl()
        .args([
            "unpack",
            dir.join("wal_000001.bin").to_str().unwrap(),
            "--out",
            outf.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(outf.exists());
    assert_eq!(fs::read_to_string(&outf).unwrap().trim().lines().count(), 8);

    let _ = fs::remove_dir_all(&dir);
}

// ── Pack tests ───────────────────────────────────────────────────────────────

#[test]
fn pack_round_trip() {
    let dir = unique_dir("pack_rt");
    let wal = create_test_segment(&dir, 1, 1, 3);
    let original = fs::read(&wal).unwrap();

    let json = dir.join("seg.json");
    roda_ctl()
        .args([
            "unpack",
            wal.to_str().unwrap(),
            "--out",
            json.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    let repacked_dir = unique_dir("pack_rt_out");
    let repacked = repacked_dir.join("wal_000001.bin");
    let out = roda_ctl()
        .args([
            "pack",
            json.to_str().unwrap(),
            "--out",
            repacked.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    assert_eq!(
        original,
        fs::read(&repacked).unwrap(),
        "binary differs after round-trip"
    );
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::remove_dir_all(&repacked_dir);
}

#[test]
fn pack_recomputes_crc() {
    let dir = unique_dir("pack_crc");
    create_test_segment(&dir, 1, 1, 2);
    let json = dir.join("seg.json");
    roda_ctl()
        .args([
            "unpack",
            dir.join("wal_000001.bin").to_str().unwrap(),
            "--out",
            json.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    // Corrupt crc32c values in JSON
    let content = fs::read_to_string(&json).unwrap();
    let mut lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();
    for line in &mut lines {
        if line.contains("\"TxMetadata\"")
            && let Ok(mut v) = serde_json::from_str::<serde_json::Value>(line)
        {
            v["crc32c"] = serde_json::Value::String("0xdeadbeef".into());
            *line = serde_json::to_string(&v).unwrap();
        }
    }
    fs::write(&json, lines.join("\n") + "\n").unwrap();

    let repacked_dir = unique_dir("pack_crc_out");
    let repacked = repacked_dir.join("wal_000001.bin");
    let out = roda_ctl()
        .args([
            "pack",
            json.to_str().unwrap(),
            "--out",
            repacked.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(out.status.success());

    // Repacked should pass CRC check
    let out = roda_ctl()
        .args(["unpack", repacked.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "repacked should have valid CRCs, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let _ = fs::remove_dir_all(&dir);
    let _ = fs::remove_dir_all(&repacked_dir);
}

#[test]
fn pack_validates_structure() {
    let dir = unique_dir("pack_validate");
    let json = dir.join("bad.json");
    fs::write(&json, r#"{"type":"TxMetadata","tx_id":1,"sub_item_count":0,"fail_reason":0,"crc32c":"0x00","user_ref":0,"timestamp":0,"tag":"0000000000000000"}
{"type":"SegmentSealed","segment_id":1,"last_tx_id":1,"record_count":2}
"#).unwrap();

    let out = roda_ctl()
        .args([
            "pack",
            json.to_str().unwrap(),
            "--out",
            dir.join("o.bin").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("first record must be SegmentHeader"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn pack_no_validate_skips() {
    let dir = unique_dir("pack_noval");
    let json = dir.join("bad.json");
    fs::write(&json, r#"{"type":"TxMetadata","tx_id":1,"sub_item_count":0,"fail_reason":0,"crc32c":"0x00","user_ref":0,"timestamp":0,"tag":"0000000000000000"}
"#).unwrap();

    let out = roda_ctl()
        .args([
            "pack",
            json.to_str().unwrap(),
            "--out",
            dir.join("o.bin").to_str().unwrap(),
            "--no-validate",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());

    let _ = fs::remove_dir_all(&dir);
}

// ── Verify tests ─────────────────────────────────────────────────────────────

#[test]
fn verify_succeeds_on_valid_data() {
    let dir = unique_dir("verify_ok");
    create_test_segment(&dir, 1, 1, 5);
    seal_segment_files(&dir, 1);
    create_test_segment(&dir, 2, 6, 5);
    seal_segment_files(&dir, 2);

    let out = roda_ctl()
        .args(["verify", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stderr).contains("all OK"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn verify_single_segment() {
    let dir = unique_dir("verify_single");
    create_test_segment(&dir, 1, 1, 5);
    seal_segment_files(&dir, 1);
    create_test_segment(&dir, 2, 6, 5);
    seal_segment_files(&dir, 2);

    let out = roda_ctl()
        .args(["verify", dir.to_str().unwrap(), "--segment", "1"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("wal_000001.bin"));
    assert!(!stderr.contains("wal_000002.bin"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn verify_detects_file_crc_corruption() {
    let dir = unique_dir("verify_fcrc");
    create_test_segment(&dir, 1, 1, 5);
    seal_segment_files(&dir, 1);

    let mut crc = fs::read(dir.join("wal_000001.crc")).unwrap();
    crc[0] ^= 0xFF;
    fs::write(dir.join("wal_000001.crc"), &crc).unwrap();

    let out = roda_ctl()
        .args(["verify", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn verify_detects_record_crc_corruption() {
    let dir = unique_dir("verify_rcrc");
    let wal = create_test_segment(&dir, 1, 1, 3);
    let mut data = fs::read(&wal).unwrap();
    data[104] ^= 0xFF;
    fs::write(&wal, &data).unwrap();
    seal_segment_files(&dir, 1); // file CRC matches corrupted data

    let out = roda_ctl()
        .args(["verify", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("CRC mismatch"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn verify_cross_segment_tx_gap() {
    let dir = unique_dir("verify_cross");
    create_test_segment(&dir, 1, 1, 5);
    seal_segment_files(&dir, 1);
    create_test_segment(&dir, 2, 10, 5);
    seal_segment_files(&dir, 2); // gap

    let out = roda_ctl()
        .args(["verify", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&out.stderr).contains("tx_id gap"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn verify_range_filter() {
    let dir = unique_dir("verify_range");
    create_test_segment(&dir, 1, 1, 3);
    seal_segment_files(&dir, 1);
    create_test_segment(&dir, 2, 4, 3);
    seal_segment_files(&dir, 2);
    create_test_segment(&dir, 3, 7, 3);
    seal_segment_files(&dir, 3);

    let out = roda_ctl()
        .args(["verify", dir.to_str().unwrap(), "--range", "1..2"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("wal_000001.bin"));
    assert!(stderr.contains("wal_000002.bin"));
    assert!(!stderr.contains("wal_000003.bin"));

    let _ = fs::remove_dir_all(&dir);
}

// ── Seal tests ───────────────────────────────────────────────────────────────

#[test]
fn seal_creates_sidecar_and_marker() {
    let dir = unique_dir("seal_basic");
    create_test_segment(&dir, 1, 1, 3);

    let out = roda_ctl()
        .args(["seal", dir.join("wal_000001.bin").to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(dir.join("wal_000001.crc").exists());
    assert!(dir.join("wal_000001.seal").exists());

    let crc_data = fs::read(dir.join("wal_000001.crc")).unwrap();
    assert_eq!(crc_data.len(), 16);
    assert_eq!(
        u32::from_le_bytes(crc_data[12..16].try_into().unwrap()),
        WAL_MAGIC
    );

    let wal = fs::read(dir.join("wal_000001.bin")).unwrap();
    assert_eq!(
        u32::from_le_bytes(crc_data[0..4].try_into().unwrap()),
        crc32c::crc32c(&wal)
    );
    assert_eq!(
        u64::from_le_bytes(crc_data[4..12].try_into().unwrap()),
        wal.len() as u64
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn seal_refuses_already_sealed() {
    let dir = unique_dir("seal_refuse");
    create_test_segment(&dir, 1, 1, 3);
    seal_segment_files(&dir, 1);

    let out = roda_ctl()
        .args(["seal", dir.join("wal_000001.bin").to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("already SEALED"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn seal_force_reseals() {
    let dir = unique_dir("seal_force");
    create_test_segment(&dir, 1, 1, 3);
    seal_segment_files(&dir, 1);
    let orig_crc = fs::read(dir.join("wal_000001.crc")).unwrap();

    let out = roda_ctl()
        .args([
            "seal",
            dir.join("wal_000001.bin").to_str().unwrap(),
            "--force",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(orig_crc, fs::read(dir.join("wal_000001.crc")).unwrap());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn seal_then_verify() {
    let dir = unique_dir("seal_verify");
    create_test_segment(&dir, 1, 1, 5);
    roda_ctl()
        .args(["seal", dir.join("wal_000001.bin").to_str().unwrap()])
        .output()
        .unwrap();

    let out = roda_ctl()
        .args(["verify", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("all OK"));

    let _ = fs::remove_dir_all(&dir);
}

// ── E2E workflow ─────────────────────────────────────────────────────────────

#[test]
fn full_unpack_pack_seal_verify() {
    let dir = unique_dir("e2e");
    create_test_segment(&dir, 1, 1, 5);
    seal_segment_files(&dir, 1);

    let json = dir.join("wal.json");
    roda_ctl()
        .args([
            "unpack",
            dir.join("wal_000001.bin").to_str().unwrap(),
            "--out",
            json.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    let new_bin = dir.join("new.bin");
    roda_ctl()
        .args([
            "pack",
            json.to_str().unwrap(),
            "--out",
            new_bin.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    fs::copy(&new_bin, dir.join("wal_000001.bin")).unwrap();
    let _ = fs::remove_file(dir.join("wal_000001.crc"));
    let _ = fs::remove_file(dir.join("wal_000001.seal"));

    roda_ctl()
        .args(["seal", dir.join("wal_000001.bin").to_str().unwrap()])
        .output()
        .unwrap();

    let out = roda_ctl()
        .args(["verify", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let _ = fs::remove_dir_all(&dir);
}
