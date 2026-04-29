//! Sync-Drop → async-await bridge for cooperative task teardown.
//!
//! The cluster's lifecycle objects (`SupervisorHandles`, `StandaloneHandles`)
//! own `tokio::task::JoinHandle`s that must be driven to completion when the
//! object is dropped. Drop is sync but the handles are async, so we have to
//! park the current worker (`block_in_place`) and drive the runtime until the
//! handles finish — bounded by a 5-second timeout to surface bugs rather than
//! hang indefinitely.
//!
//! Single-thread (`current_thread`) runtimes can't run this pattern: the
//! runtime needs the calling thread to drive the awaited tasks, but the
//! calling thread is busy in Drop. We fall back to `abort` there and log a
//! warning so the test can be migrated to `flavor = "multi_thread"`.

use std::time::Duration;
use tokio::task::JoinHandle;

const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

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
