//! Test/bench helpers for driving the Transactor without a full `Ledger`.
//!
//! Benches link the library without `cfg(test)`, so these live on the normal
//! crate surface (hidden from docs) rather than under `#[cfg(test)]`.
#![doc(hidden)]

use crate::pipeline::{Pipeline, TransactorContext};
use crate::tx_ring::releaser::TxRingReleaser;
use crate::tx_ring::ring::TxRing;
use crate::tx_ring::writer::TxRingWriter;
use crate::wait_strategy::WaitStrategy;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use storage::entities::WalEntry;

/// Background thread that keeps a `TxRing` drained by advancing its releaser to
/// the writer's committed frontier, so a producer under benchmark never blocks
/// on a full ring. Stops and joins on drop.
pub struct RingDrain {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl RingDrain {
    fn spawn(ring: Arc<TxRing>, mut releaser: TxRingReleaser) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let stop = running.clone();
        let handle = std::thread::Builder::new()
            .name("ring_drain".to_string())
            .spawn(move || {
                while stop.load(Ordering::Relaxed) {
                    let write = ring.write_index();
                    if write != releaser.released() {
                        releaser.advance_to(write);
                    } else {
                        std::hint::spin_loop();
                    }
                }
                releaser.advance_to(ring.write_index());
            })
            .expect("spawn ring_drain");
        Self {
            running,
            handle: Some(handle),
        }
    }
}

impl Drop for RingDrain {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// A standalone `Pipeline` wired to a fresh ring, plus the ring's writer (for a
/// `Transactor`/`TransactorRunner`) and a background drain that keeps the ring
/// from filling. Get the context via `pipeline.transactor_context()`.
pub fn mock_pipeline(
    queue_size: usize,
    ring_size: usize,
    wait_strategy: WaitStrategy,
) -> (Arc<Pipeline>, TxRingWriter, RingDrain) {
    let (ring, writer, releaser) = TxRing::new(ring_size);
    let drain = RingDrain::spawn(ring.clone(), releaser);
    let pipeline = Pipeline::with_sizes(queue_size, wait_strategy, ring);
    (pipeline, writer, drain)
}

/// A minimal `TransactorContext` (`is_running() == true`) for unit tests that
/// only need the context surface — e.g. to call `ensure_capacity`.
pub fn mock_transactor_ctx() -> TransactorContext {
    let (ring, _writer, _releaser) = TxRing::new(2);
    Pipeline::with_sizes(8, WaitStrategy::LowLatency, ring).transactor_context()
}

/// A standalone `Pipeline` wired to a fresh ring, returning the ring's writer and
/// releaser **without** a drain — for benches that feed the ring directly and wire
/// the releaser into a real consumer stage (WAL/snapshot) that frees slots itself.
pub fn ring_pipeline(
    queue_size: usize,
    ring_size: usize,
    wait_strategy: WaitStrategy,
) -> (Arc<Pipeline>, TxRingWriter, TxRingReleaser) {
    let (ring, writer, releaser) = TxRing::new(ring_size);
    let pipeline = Pipeline::with_sizes(queue_size, wait_strategy, ring);
    (pipeline, writer, releaser)
}

/// Push one entry, reclaiming freed slots (blocking on the releaser) when the
/// granted window is exhausted, so a feeder never panics on a full ring.
pub fn ring_push(writer: &mut TxRingWriter, entry: WalEntry) {
    while writer.capacity() == 0 {
        writer.commit(); // publish so the consumer can free slots
        if writer.grant() > 0 {
            break;
        }
        std::hint::spin_loop();
    }
    writer.push(entry);
}
