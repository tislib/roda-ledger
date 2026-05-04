//! Binary entry point for the E2E suite.
//!
//! Each `async fn` scenario in `testing::e2e::{correctness, crash, wasm}`
//! is registered as a `libtest_mimic::Trial`. The runner mimics the
//! standard `cargo test` CLI — `--list`, `--exact`, `--nocapture`,
//! `--test-threads`, name filter — so existing scripts and CI flags
//! keep working unchanged.
//!
//! Each trial builds its own tokio runtime; the `multi_thread` flag at
//! registration time mirrors `#[tokio::test(flavor = "multi_thread")]`
//! behavior on the original tests.

use libtest_mimic::{Arguments, Failed, Trial};
use std::panic::AssertUnwindSafe;
use testing::e2e;

fn main() {
    let args = Arguments::from_args();

    let trials = vec![
        // ── correctness ──────────────────────────────────────────────────
        scenario(
            "correctness::deposit_committed_reflects_in_balance",
            false,
            || e2e::correctness::deposit_committed_reflects_in_balance(),
        ),
        scenario("correctness::matrix_transfer_grid", false, || {
            e2e::correctness::matrix_transfer_grid()
        }),
        scenario("correctness::matrix_concurrent_transfer_grid", true, || {
            e2e::correctness::matrix_concurrent_transfer_grid()
        }),
        // ── crash (multi_thread) ─────────────────────────────────────────
        scenario("crash::simple_crash_recovery", true, || {
            e2e::crash::simple_crash_recovery()
        }),
        scenario("crash::crash_in_the_middle", true, || {
            e2e::crash::crash_in_the_middle()
        }),
        // ── wasm ─────────────────────────────────────────────────────────
        scenario("wasm::wasm_runs_properly", false, || {
            e2e::wasm::wasm_runs_properly()
        }),
        scenario("wasm::wasm_new_version_runs_properly", false, || {
            e2e::wasm::wasm_new_version_runs_properly()
        }),
        scenario("wasm::wasm_loaded_after_restart", true, || {
            e2e::wasm::wasm_loaded_after_restart()
        }),
        scenario("wasm::wasm_loaded_after_crash", true, || {
            e2e::wasm::wasm_loaded_after_crash()
        }),
        scenario(
            "wasm::wasm_new_version_loaded_after_restart_and_crash",
            true,
            || e2e::wasm::wasm_new_version_loaded_after_restart_and_crash(),
        ),
        scenario("wasm::wasm_unregister_persists_across_crash", true, || {
            e2e::wasm::wasm_unregister_persists_across_crash()
        }),
    ];

    libtest_mimic::run(&args, trials).exit();
}

/// Wrap an `async fn() -> ()` scenario into a libtest-mimic `Trial`.
/// `multi_thread = true` selects the multi-thread tokio scheduler, mirroring
/// `#[tokio::test(flavor = "multi_thread")]` semantics.
fn scenario<F, Fut>(name: &'static str, multi_thread: bool, body: F) -> Trial
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    Trial::test(name, move || {
        let rt = if multi_thread {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
        } else {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
        }
        .map_err(|e| Failed::from(format!("runtime build: {e}")))?;

        match std::panic::catch_unwind(AssertUnwindSafe(|| rt.block_on(body()))) {
            Ok(()) => Ok(()),
            Err(panic) => Err(Failed::from(panic_message(panic))),
        }
    })
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<String>() {
        return s.clone();
    }
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    "scenario panicked (non-string payload)".into()
}
