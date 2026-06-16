//! Tests for the `tools` build helper. Run with `--features tools`.
//!
//! They shell out to `cargo` to compile a real wasm module, so they self-skip
//! when the `wasm32-unknown-unknown` target isn't installed (keeps CI green on
//! runners that don't have it).
#![cfg(feature = "tools")]

use roda_wasm_abi::tools::compile_to_wasm;
use std::process::Command;

/// True if the wasm target is installed (via rustup); skip the test otherwise.
fn wasm_target_installed() -> bool {
    Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).contains("wasm32-unknown-unknown"))
        .unwrap_or(false)
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

const TRANSFER: &str = r#"
    use roda_wasm_abi::{credit, debit, execute, Status};
    execute!(|p| {
        let amount = p.amount(1);
        debit(p.account(0), amount);
        credit(p.account(2), amount);
        Status::OK
    });
"#;

#[test]
fn compiles_a_module_to_wasm() {
    if !wasm_target_installed() {
        eprintln!("skipping: wasm32-unknown-unknown target not installed");
        return;
    }
    let wasm = compile_to_wasm(TRANSFER).expect("compile should succeed");

    // Valid wasm magic, and the ABI is wired: exports `execute`, imports `ledger`.
    assert_eq!(&wasm[..4], b"\0asm", "wasm magic header");
    assert!(contains(&wasm, b"execute"), "exports execute");
    assert!(contains(&wasm, b"ledger"), "imports the ledger host module");
}

#[test]
fn surfaces_compile_errors() {
    if !wasm_target_installed() {
        eprintln!("skipping: wasm32-unknown-unknown target not installed");
        return;
    }
    let err = compile_to_wasm("this is not valid rust").expect_err("should fail to compile");
    // cargo's stderr is surfaced in the error.
    assert!(!err.to_string().is_empty());
}
