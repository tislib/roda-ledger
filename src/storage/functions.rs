//! Disk storage for registered WASM functions (ADR-014).
//!
//! Layout:
//! ```text
//! {storage.data_dir()}/functions/{name}_v{N}.wasm
//! ```
//!
//! - Registration writes the binary to a new versioned file.
//! - Unregistration writes a 0-byte file (audit trail preserved, on-disk and
//!   WAL signals agreeing that the function is gone).
//!
//! Every function in this module operates on `&Storage` — the path
//! `{data_dir}/functions` is resolved internally via [`Storage::functions_dir`].
//! No caller ever passes a raw `&Path` or constructs the directory itself.
//! `Storage::new` ensures the directory exists at startup.
//!
//! **The `functions/` directory is reference data, not source of truth.** The
//! WAL `FunctionRegistered` records and the function snapshot are the
//! authoritative registry. Every read from this module already knows the
//! exact `(name, version)` it is looking for — there is no on-disk discovery
//! API here for that reason.

use crate::storage::Storage;
use std::fs;
use std::io;
use std::path::PathBuf;

/// Resolve `{storage}/functions/{name}_v{version}.wasm`.
fn function_path(storage: &Storage, name: &str, version: u16) -> PathBuf {
    storage.functions_dir().join(format!("{name}_v{version}.wasm"))
}

/// Atomically write a WASM binary under `{storage}/functions/{name}_v{version}.wasm`
/// (write to a temp file in the same directory, then rename). Returns the
/// CRC32C of the written bytes.
pub fn write_function(
    storage: &Storage,
    name: &str,
    version: u16,
    binary: &[u8],
) -> io::Result<u32> {
    // Storage::new already created the functions directory; defensive create
    // here keeps the function callable in tests that bypass the constructor.
    fs::create_dir_all(storage.functions_dir())?;
    let path = function_path(storage, name, version);
    let tmp = path.with_extension("wasm.tmp");
    fs::write(&tmp, binary)?;
    fs::rename(&tmp, &path)?;
    Ok(crc32c::crc32c(binary))
}

/// Read a WASM binary from `{storage}/functions/{name}_v{version}.wasm`.
pub fn read_function(storage: &Storage, name: &str, version: u16) -> io::Result<Vec<u8>> {
    fs::read(function_path(storage, name, version))
}

/// Truncate the on-disk binary to 0 bytes — do not delete, the audit
/// trail is preserved. Returns `Ok(())` if the file did not exist.
pub fn truncate_function(storage: &Storage, name: &str, version: u16) -> io::Result<()> {
    let path = function_path(storage, name, version);
    if !path.exists() {
        return Ok(());
    }
    fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use tempfile::tempdir;

    /// Build an isolated `Storage` rooted in a fresh tempdir for one test.
    fn temp_storage() -> (Storage, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let cfg = StorageConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            ..StorageConfig::default()
        };
        let storage = Storage::new(cfg).unwrap();
        (storage, dir)
    }

    #[test]
    fn write_read_roundtrip() {
        let (storage, _td) = temp_storage();
        let bytes = b"\x00asm\x01\x00\x00\x00".to_vec();
        let crc = write_function(&storage, "foo", 1, &bytes).unwrap();
        assert_eq!(crc, crc32c::crc32c(&bytes));
        let read = read_function(&storage, "foo", 1).unwrap();
        assert_eq!(read, bytes);
    }

    #[test]
    fn truncate_preserves_file_at_zero_bytes() {
        let (storage, _td) = temp_storage();
        write_function(&storage, "bar", 3, b"hello").unwrap();
        truncate_function(&storage, "bar", 3).unwrap();
        let meta = fs::metadata(function_path(&storage, "bar", 3)).unwrap();
        assert_eq!(meta.len(), 0);
    }

    #[test]
    fn truncate_missing_is_ok() {
        let (storage, _td) = temp_storage();
        truncate_function(&storage, "none", 1).unwrap();
    }

    #[test]
    fn function_path_is_under_storage_functions_dir() {
        let (storage, _td) = temp_storage();
        let p = function_path(&storage, "my_fn", 7);
        assert!(p.ends_with("functions/my_fn_v7.wasm"));
        assert!(p.starts_with(storage.data_dir()));
    }

    #[test]
    fn read_specific_version() {
        // Read API only needs (name, version) — no on-disk discovery; the
        // caller already knows which version it wants from the WAL or
        // function snapshot.
        let (storage, _td) = temp_storage();
        write_function(&storage, "fee", 1, b"v1").unwrap();
        write_function(&storage, "fee", 2, b"v2").unwrap();
        assert_eq!(read_function(&storage, "fee", 1).unwrap(), b"v1");
        assert_eq!(read_function(&storage, "fee", 2).unwrap(), b"v2");
    }
}
