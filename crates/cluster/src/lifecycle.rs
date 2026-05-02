//! Helpers that pin gRPC servers (and the raft loop) onto dedicated OS
//! threads with private `current_thread` tokio runtimes.
//!
//! Two flavours:
//!
//! - [`spawn_grpc_thread`] — a plain `current_thread` runtime. Used for
//!   the client-facing Ledger gRPC server, which is fully `Send` and
//!   has no `!Send` co-tenants.
//! - [`spawn_local_thread`] — adds a `tokio::task::LocalSet` on top of
//!   the runtime, so the future may `spawn_local` further `!Send`
//!   tasks. Used for the peer-facing Node gRPC thread, which co-hosts
//!   the raft loop (`Rc<RefCell<RaftNode>>`, hence `!Send`).
//!
//! Joining those threads in Drop is plain `std::thread::JoinHandle::join()`;
//! [`join_grpc_threads`] wraps it in `block_in_place` when the host
//! runtime is multi-threaded so the parked worker can be replaced while
//! the join proceeds.

use std::future::Future;
use std::thread;
use tokio::runtime;
use tokio::task::LocalSet;

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

/// Variant of [`spawn_grpc_thread`] that hosts a `tokio::task::LocalSet`
/// on the dedicated thread, so the future (and any tasks it
/// `spawn_local`s) can hold `!Send` state — `Rc<RefCell<…>>`,
/// `LocalSet`-only typestate, etc.
///
/// The future itself is built **inside** the new thread by the
/// caller-supplied closure, so neither it nor its captures need to be
/// `Send`. The closure is the only thing that crosses the thread
/// boundary, and only it must satisfy `Send + 'static`.
///
/// Used for the node-grpc thread, which co-hosts the peer-facing gRPC
/// server and the raft loop. The raft loop owns
/// `Rc<RefCell<RaftNode<…>>>` and is `!Send`; running it on this same
/// LocalSet keeps the raft state machine and the inbound gRPC handlers
/// on the same OS thread without needing any cross-runtime channel
/// hops between them.
pub(crate) fn spawn_local_thread<F, Fut>(
    name: &str,
    build: F,
) -> std::io::Result<thread::JoinHandle<()>>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let owned_name = name.to_string();
    thread::Builder::new()
        .name(owned_name.clone())
        .spawn(move || {
            let rt = runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name(&owned_name)
                .build()
                .expect("build dedicated thread runtime");
            let local = LocalSet::new();
            rt.block_on(local.run_until(build()));
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
                panic!("{}: gRPC thread panicked: {:?}", label, e);
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
