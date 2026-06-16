//! Host-only build helper: compile guest-module source to a wasm binary.
//!
//! Spins up a throwaway Cargo project that depends on **this** crate by path (so
//! it builds against the local source, not a published release), runs
//! `cargo build --target wasm32-unknown-unknown`, returns the `.wasm` bytes, and
//! deletes the project. Useful for tests and tooling that need a real module
//! without checking a `.wasm` into the tree.
//!
//! Gated behind the `tools` feature and `#[cfg(not(target_arch = "wasm32"))]`,
//! so it shells out to `cargo` / uses `std` only on the host and can never leak
//! into a guest module.
//!
//! ```no_run
//! let wasm = roda_wasm_abi::tools::compile_to_wasm(r#"
//!     use roda_wasm_abi::{credit, debit, execute, Status};
//!     execute!(|p| {
//!         debit(p.account(0), p.amount(1));
//!         credit(p.account(2), p.amount(1));
//!         Status::OK
//!     });
//! "#).unwrap();
//! assert_eq!(&wasm[..4], b"\0asm");
//! ```

// The crate is `#![no_std]`, so std's prelude items aren't in scope here.
use std::format;
use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::string::String;
use std::sync::atomic::{AtomicU64, Ordering};
use std::vec::Vec;

/// Compile `lib_src` (a crate body that `use`s `roda_wasm_abi`) to a wasm
/// module, returning the `.wasm` bytes.
pub fn compile_to_wasm(lib_src: &str) -> io::Result<Vec<u8>> {
    compile_to_wasm_with_deps(lib_src, "")
}

/// Like [`compile_to_wasm`], with extra `[dependencies]` lines appended verbatim
/// to the generated `Cargo.toml` — e.g.
/// `r#"serde = { version = "1", default-features = false }"#`.
pub fn compile_to_wasm_with_deps(lib_src: &str, extra_deps: &str) -> io::Result<Vec<u8>> {
    let project = TempProject::new()?;
    project.write_manifest(extra_deps)?;
    project.write_src(lib_src)?;
    project.build()
}

/// A throwaway Cargo project; removes its directory on drop.
struct TempProject {
    dir: PathBuf,
}

impl TempProject {
    fn new() -> io::Result<Self> {
        // pid + a per-process counter keeps concurrent compiles isolated.
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("roda-wasm-gen-{}-{n}", std::process::id()));
        std::fs::create_dir_all(dir.join("src"))?;
        Ok(TempProject { dir })
    }

    fn write_manifest(&self, extra_deps: &str) -> io::Result<()> {
        // Depend on this crate by path → build against the local source.
        // `{:?}` quotes/escapes the path into a valid TOML string (Windows too).
        let abi_dir = env!("CARGO_MANIFEST_DIR");
        let manifest = format!(
            r#"[package]
name = "rodagen"
version = "0.0.0"
edition = "2021"

# Standalone so it is never pulled into a surrounding workspace.
[workspace]

[lib]
crate-type = ["cdylib"]

[dependencies]
roda-wasm-abi = {{ path = {abi_dir:?} }}
{extra_deps}

[profile.release]
opt-level = "z"
lto = true
panic = "abort"
strip = true
"#
        );
        std::fs::write(self.dir.join("Cargo.toml"), manifest)
    }

    fn write_src(&self, lib_src: &str) -> io::Result<()> {
        std::fs::write(self.dir.join("src/lib.rs"), lib_src)
    }

    fn build(&self) -> io::Result<Vec<u8>> {
        let out = Command::new("cargo")
            .current_dir(&self.dir)
            .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
            .output()?;
        if !out.status.success() {
            // cargo's real compile errors are on stderr — surface them.
            return Err(io::Error::other(
                String::from_utf8_lossy(&out.stderr).into_owned(),
            ));
        }
        std::fs::read(
            self.dir
                .join("target/wasm32-unknown-unknown/release/rodagen.wasm"),
        )
    }
}

impl Drop for TempProject {
    fn drop(&mut self) {
        // Best-effort cleanup.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
