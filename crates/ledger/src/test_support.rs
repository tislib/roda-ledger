//! Test/bench helpers for driving the Transactor without a full `Ledger`.
//!
//! Benches link the library without `cfg(test)`, so these live on the normal
//! crate surface (hidden from docs) rather than under `#[cfg(test)]`.
#![doc(hidden)]

use crate::pipeline::{Pipeline, TransactorContext};
use crate::tx_ring::reader::TxRingReader;
use crate::tx_ring::ring::TxRing;
use crate::tx_ring::writer::TxRingWriter;
use crate::wait_strategy::WaitStrategy;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use storage::entities::WalEntry;

/// Background thread that keeps the ring drained by advancing the reader's
/// release index to the writer's committed frontier, so a producer under
/// benchmark never blocks on a full ring. Stops and joins on drop.
pub struct RingDrain {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl RingDrain {
    fn spawn(mut reader: TxRingReader) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let stop = running.clone();
        let handle = std::thread::Builder::new()
            .name("ring_drain".to_string())
            .spawn(move || {
                while stop.load(Ordering::Relaxed) {
                    let write = reader.write_index();
                    if write != reader.released() {
                        reader.release_to(write);
                    } else {
                        std::hint::spin_loop();
                    }
                }
                let write = reader.write_index();
                reader.release_to(write);
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
    let (writer, reader) = TxRing::new(ring_size);
    let drain = RingDrain::spawn(reader);
    let pipeline = Pipeline::with_sizes(queue_size, wait_strategy);
    (pipeline, writer, drain)
}

/// A minimal `TransactorContext` (`is_running() == true`) for unit tests that
/// only need the context surface — e.g. to call `ensure_capacity`.
pub fn mock_transactor_ctx() -> TransactorContext {
    let (_writer, _reader) = TxRing::new(2);
    Pipeline::with_sizes(8, WaitStrategy::LowLatency).transactor_context()
}

/// A standalone `Pipeline` plus a fresh ring's writer and reader **without** a
/// drain — for benches that feed the ring directly and wire the reader into a
/// real consumer stage (WAL) that reads and frees slots itself.
pub fn ring_pipeline(
    queue_size: usize,
    ring_size: usize,
    wait_strategy: WaitStrategy,
) -> (Arc<Pipeline>, TxRingWriter, TxRingReader) {
    let (writer, reader) = TxRing::new(ring_size);
    let pipeline = Pipeline::with_sizes(queue_size, wait_strategy);
    (pipeline, writer, reader)
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
