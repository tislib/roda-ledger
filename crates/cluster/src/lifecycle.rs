//! Sync-Drop → async-await bridge for cooperative task teardown, plus
//! a helper that pins a gRPC server onto its own OS thread + dedicated
//! `current_thread` tokio runtime.
//!
//! The cluster's lifecycle objects (`ClusterHandles`, `StandaloneHandles`)
//! own task and thread handles that must be driven to completion when the
//! object is dropped. Drop is sync but the handles are async, so for tokio
//! tasks we park the current worker (`block_in_place`) and drive the
//! runtime until the handles finish — bounded by a 5-second timeout to
//! surface bugs rather than hang indefinitely.
//!
//! Single-thread (`current_thread`) runtimes can't run the cooperative
//! drain pattern: the runtime needs the calling thread to drive the
//! awaited tasks, but the calling thread is busy in Drop. We fall back
//! to `abort` for tokio handles there and log a warning so the test can
//! be migrated to `flavor = "multi_thread"`.
//!
//! gRPC servers run on **dedicated** OS threads (one per server), each
//! with its own `current_thread` tokio runtime. That isolates the
//! ledger-facing and peer-facing service loops from each other and from
//! the raft loop's runtime — see [`spawn_grpc_thread`]. Joining those
//! threads in Drop is plain `std::thread::JoinHandle::join()`; we wrap
//! it in `block_in_place` when the host runtime is multi-threaded so
//! the parked worker can be replaced while the join proceeds.

use std::future::Future;
use std::thread;
use std::time::Duration;
use tokio::runtime;
use tokio::task::JoinHandle;

const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Spawn an OS thread that hosts a private `current_thread` tokio
/// runtime and drives `fut` to completion. The returned
/// `thread::JoinHandle` exits when `fut` resolves — for our gRPC
/// servers, that's when `serve_with_shutdown` resolves after the
/// owning `Notify` fires.
///
/// `name` is applied to both the OS thread and the runtime's worker
/// for clean stack traces and `top -H` output.
pub(crate) fn spawn_grpc_thread<F>(name: &str, fut: F) -> std::io::Result<thread::JoinHandle<()>>
where
    F: Future<Output = ()> + Send + 'static,
{
    let owned_name = name.to_string();
    thread::Builder::new()
        .name(owned_name.clone())
        .spawn(move || {
            let rt = runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name(&owned_name)
                .build()
                .expect("build dedicated gRPC runtime");
            rt.block_on(fut);
        })
}

/// Join the dedicated gRPC server threads owned by a `*Handles` Drop.
/// Wrapped in `block_in_place` when the surrounding tokio runtime has
/// multiple workers so the parked worker can be replaced while the
/// join proceeds — important because in-flight gRPC handlers may be
/// awaiting oneshots driven by the raft loop on that same runtime.
///
/// On a `current_thread` runtime there is no other worker to swap in,
/// so we fall back to a plain `join()` and log a warning. The dedicated
/// gRPC runtime exits independently of the host runtime, so the join
/// itself does not deadlock — but a parked AppendEntries reply on a
/// stalled host runtime could keep the gRPC drain pinned. Tests that
/// drive the cluster lifecycle should use `flavor = "multi_thread"`.
pub(crate) fn join_grpc_threads(
    label: &'static str,
    threads: impl IntoIterator<Item = Option<thread::JoinHandle<()>>>,
) {
    let owned: Vec<thread::JoinHandle<()>> = threads.into_iter().flatten().collect();
    if owned.is_empty() {
        return;
    }

    let multi_thread = tokio::runtime::Handle::try_current()
        .map(|h| h.metrics().num_workers() > 1)
        .unwrap_or(false);

    let join_all = move || {
        for t in owned {
            if let Err(e) = t.join() {
                spdlog::warn!("{}: gRPC thread panicked: {:?}", label, e);
            }
        }
    };

    if multi_thread {
        tokio::task::block_in_place(join_all);
    } else {
        spdlog::warn!(
            "{}: Drop without a multi_thread runtime — gRPC thread join may stall \
             if a parked AppendEntries reply needs the host runtime to advance \
             (test should use #[tokio::test(flavor = \"multi_thread\")])",
            label
        );
        join_all();
    }
}

/// Drain tokio task handles cooperatively in Drop. Used for the raft
/// loop's `JoinHandle` — the gRPC servers no longer produce these (see
/// [`spawn_grpc_thread`] / [`join_grpc_threads`]).
pub(crate) fn drain_in_drop(
    label: &'static str,
    handles: impl IntoIterator<Item = Option<JoinHandle<()>>>,
) {
    let owned: Vec<JoinHandle<()>> = handles.into_iter().flatten().collect();
    if owned.is_empty() {
        return;
    }

    let runtime = match tokio::runtime::Handle::try_current() {
        Ok(h) => h,
        Err(_) => {
            for h in owned {
                h.abort();
            }
            return;
        }
    };

    if runtime.metrics().num_workers() <= 1 {
        spdlog::warn!(
            "{}: Drop on current_thread runtime — falling back to abort \
             (test should use #[tokio::test(flavor = \"multi_thread\")])",
            label
        );
        for h in owned {
            h.abort();
        }
        return;
    }

    let timed_out = tokio::task::block_in_place(|| {
        runtime.block_on(async {
            tokio::time::timeout(DRAIN_TIMEOUT, async {
                for h in owned {
                    let _ = h.await;
                }
            })
            .await
            .is_err()
        })
    });

    if timed_out {
        spdlog::warn!(
            "{}: cooperative drain exceeded {:?}; remaining tasks aborted by runtime",
            label,
            DRAIN_TIMEOUT
        );
    }
}
