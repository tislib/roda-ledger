use super::ring::TxRing;
use super::writer::TxRingWriter;
use bytemuck::Zeroable;
use std::sync::Arc;
use std::thread;
use storage::entities::{TxEntry, WalEntry};

// A distinct entry whose `tx_id` doubles as its logical position, so reads are checkable.
fn entry(tx_id: u64) -> WalEntry {
    WalEntry::Entry(TxEntry {
        tx_id,
        ..TxEntry::zeroed()
    })
}

// Collect the tx_ids the writer walks over its pending window after `skip`,
// reading each entry by reference (no copy).
fn pending_ids(w: &TxRingWriter, skip: usize) -> Vec<u64> {
    let mut got = Vec::new();
    w.walk_pending(skip, |e| {
        if let WalEntry::Entry(te) = e {
            got.push(te.tx_id);
        }
    });
    got
}

// Collect the tx_ids the writer walks over an explicit logical range.
fn range_ids(w: &TxRingWriter, start: usize, end: usize) -> Vec<u64> {
    let mut got = Vec::new();
    w.walk(start, end, |e| {
        if let WalEntry::Entry(te) = e {
            got.push(te.tx_id);
        }
    });
    got
}

#[test]
fn reserve_grants_all_free_space() {
    let (_ring, mut writer, _releaser) = TxRing::new(4);
    assert_eq!(writer.reserve(), 4);
    assert_eq!(writer.capacity(), 4);
}

#[test]
fn non_power_of_two_panics() {
    let result = std::panic::catch_unwind(|| TxRing::new(3));
    assert!(result.is_err());
}

#[test]
fn full_ring_grants_zero() {
    let (ring, mut writer, _releaser) = TxRing::new(4);
    writer.reserve();
    for i in 0..4 {
        writer.push(entry(i));
    }
    writer.commit();
    assert_eq!(ring.write_index(), 4);
    assert_eq!(ring.release_index(), 0);
    assert_eq!(ring.capacity(), 4);
    // Nothing released yet, so re-granting yields no space.
    assert_eq!(writer.reserve(), 0);
    assert_eq!(writer.capacity(), 0);
}

#[test]
fn release_then_reserve() {
    let (ring, mut writer, mut releaser) = TxRing::new(4);
    writer.reserve();
    for i in 0..4 {
        writer.push(entry(i));
    }
    assert_eq!(writer.capacity(), 0);
    // publish before releasing so the releaser stays within the write cursor
    writer.commit();
    releaser.advance_to(2);
    assert_eq!(releaser.released(), 2);
    assert_eq!(writer.reserve(), 2);
    assert_eq!(writer.capacity(), 2);
    writer.push(entry(4));
    writer.push(entry(5));
    drop(writer);
    assert_eq!(ring.write_index(), 6);
}

#[test]
fn reserve_grants_nothing_until_release() {
    let (_ring, mut writer, mut releaser) = TxRing::new(4);
    writer.reserve();
    for i in 0..4 {
        writer.push(entry(i));
    }
    writer.commit();
    assert_eq!(writer.reserve(), 0);
    assert_eq!(writer.reserve(), 0);
    releaser.advance_to(1);
    assert_eq!(writer.reserve(), 1);
}

#[test]
#[should_panic]
fn release_backward_panics() {
    let (_ring, mut writer, mut releaser) = TxRing::new(4);
    writer.reserve();
    for i in 0..4 {
        writer.push(entry(i));
    }
    writer.commit();
    releaser.advance_to(3);
    releaser.advance_to(1);
}

#[test]
#[should_panic]
fn release_past_write_panics() {
    let (_ring, mut writer, mut releaser) = TxRing::new(4);
    writer.reserve();
    writer.push(entry(0));
    writer.commit();
    releaser.advance_to(3);
}

#[test]
fn get_within_window_returns_value() {
    let (ring, mut writer, mut releaser) = TxRing::new(8);
    writer.reserve();
    for i in 0..6 {
        writer.push(entry(i));
    }
    writer.commit();
    releaser.advance_to(2); // live window is now [2, 6)
    for i in 2..6usize {
        assert_eq!(ring.get(i).tx_id(), i as u64);
    }
}

#[test]
#[should_panic]
fn get_at_write_index_panics() {
    // idx == write_index: slot not yet published, reading it is stale.
    let (ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    for i in 0..4 {
        writer.push(entry(i));
    }
    writer.commit();
    let _ = ring.get(4);
}

#[test]
#[should_panic]
fn get_below_release_index_panics() {
    // idx < release_index: slot is reclaimable, the writer may overwrite it.
    let (ring, mut writer, mut releaser) = TxRing::new(8);
    writer.reserve();
    for i in 0..4 {
        writer.push(entry(i));
    }
    writer.commit();
    releaser.advance_to(2);
    let _ = ring.get(1);
}

#[test]
fn multi_reader_copy_out() {
    const N: u64 = 1000;
    const CAP: usize = 1024;
    let (ring, mut writer, _releaser) = TxRing::new(CAP);

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let ring = Arc::clone(&ring);
            thread::spawn(move || {
                let mut local = 0usize;
                let mut seen = Vec::with_capacity(N as usize);
                while (seen.len() as u64) < N {
                    let w = ring.write_index();
                    while local < w {
                        seen.push(ring.get(local).tx_id());
                        local += 1;
                    }
                    std::hint::spin_loop();
                }
                seen
            })
        })
        .collect();

    writer.reserve();
    for i in 0..N {
        writer.push(entry(i));
    }
    drop(writer);

    let expected: Vec<u64> = (0..N).collect();
    for r in readers {
        assert_eq!(r.join().unwrap(), expected);
    }
}

#[test]
fn wrap_around() {
    const CAP: usize = 4;
    let (ring, mut writer, mut releaser) = TxRing::new(CAP);
    let rounds = 16usize;
    let mut produced = 0usize;
    let mut consumed = 0usize;
    for _ in 0..rounds {
        writer.reserve();
        writer.push(entry(produced as u64));
        writer.push(entry(produced as u64 + 1));
        produced += 2;
        writer.commit();

        let w = ring.write_index();
        while consumed < w {
            assert_eq!(ring.get(consumed).tx_id(), consumed as u64);
            consumed += 1;
        }
        releaser.advance_to(consumed);
    }
    assert_eq!(produced, rounds * 2);
    assert_eq!(ring.write_index(), rounds * 2);
    assert_eq!(releaser.released(), rounds * 2);
}

#[test]
fn walk_pending_reads_pushed_entries() {
    let (_ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    writer.push(entry(10));
    writer.push(entry(11));
    writer.push(entry(12));
    assert_eq!(writer.pending(), 3);
    assert_eq!(pending_ids(&writer, 0), vec![10, 11, 12]);
}

#[test]
fn walk_pending_skips_prefix() {
    let (_ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    for id in 10..13 {
        writer.push(entry(id));
    }
    assert_eq!(pending_ids(&writer, 1), vec![11, 12]);
    assert_eq!(pending_ids(&writer, 2), vec![12]);
    // skip == pending and skip > pending both walk nothing, never wrap.
    assert!(pending_ids(&writer, 3).is_empty());
    assert!(pending_ids(&writer, 99).is_empty());
}

#[test]
fn walk_pending_empty_when_idle() {
    let (_ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    assert_eq!(writer.pending(), 0);
    assert!(pending_ids(&writer, 0).is_empty());
}

#[test]
fn walk_explicit_range_is_half_open() {
    let (_ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    for id in 0..4 {
        writer.push(entry(id));
    }
    assert_eq!(range_ids(&writer, 0, 4), vec![0, 1, 2, 3]);
    assert_eq!(range_ids(&writer, 1, 3), vec![1, 2]);
    assert!(range_ids(&writer, 2, 2).is_empty());
}

#[test]
fn commit_empties_pending_window() {
    let (_ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    for id in 10..13 {
        writer.push(entry(id));
    }
    assert_eq!(writer.pending(), 3);
    writer.commit();
    assert_eq!(writer.pending(), 0);
    assert!(pending_ids(&writer, 0).is_empty());
}

#[test]
fn walk_pending_wraps_around_the_buffer() {
    const CAP: usize = 4;
    let (_ring, mut writer, mut releaser) = TxRing::new(CAP);
    writer.reserve();
    for id in 0..CAP as u64 {
        writer.push(entry(id));
    }
    writer.commit();
    releaser.advance_to(CAP); // free the whole buffer
    assert_eq!(writer.reserve(), CAP);

    // These land in physical slots 0,1,2 even though their logical indices wrapped.
    writer.push(entry(40));
    writer.push(entry(41));
    writer.push(entry(42));
    assert_eq!(writer.pending(), 3);
    assert_eq!(pending_ids(&writer, 0), vec![40, 41, 42]);
}

#[test]
fn rollback_moves_head_back_and_reuses_slots() {
    let (ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    for id in 10..14 {
        writer.push(entry(id));
    }
    assert_eq!(writer.pending(), 4);
    assert_eq!(writer.capacity(), 4);

    assert_eq!(writer.rollback(2), 2); // drop entry(12), entry(13)
    assert_eq!(writer.pending(), 2);
    assert_eq!(writer.capacity(), 6); // their slots are free again

    writer.push(entry(99)); // overwrites the slot entry(12) used
    assert_eq!(pending_ids(&writer, 0), vec![10, 11, 99]);

    writer.commit();
    assert_eq!(ring.write_index(), 3);
}

#[test]
fn rollback_clamps_to_pending_and_spares_committed() {
    let (_ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    writer.push(entry(10));
    writer.push(entry(11));
    writer.commit(); // [10, 11] are now committed, out of rollback's reach
    writer.push(entry(12));
    assert_eq!(writer.pending(), 1);

    assert_eq!(writer.rollback(5), 1); // clamped: only the uncommitted entry(12)
    assert_eq!(writer.pending(), 0);
}

#[test]
fn rollback_zero_is_a_noop() {
    let (_ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    for id in 10..13 {
        writer.push(entry(id));
    }
    assert_eq!(writer.rollback(0), 0);
    assert_eq!(writer.pending(), 3);
    assert_eq!(pending_ids(&writer, 0), vec![10, 11, 12]);
}

// Collect tx_ids a reader walks from `from` up to the published write_index.
fn walked_ids(ring: &TxRing, from: usize) -> Vec<u64> {
    let mut got = Vec::new();
    ring.walk_entries(from, |e| {
        got.push(e.tx_id());
        true
    });
    got
}

#[test]
fn walk_entries_reads_committed_from_index_to_write() {
    let (ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    for i in 0..5 {
        writer.push(entry(i));
    }
    writer.commit(); // write_index = 5
    assert_eq!(walked_ids(&ring, 0), vec![0, 1, 2, 3, 4]);
    assert_eq!(walked_ids(&ring, 2), vec![2, 3, 4]);
}

#[test]
fn walk_entries_skips_uncommitted_entries() {
    let (ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    for i in 0..3 {
        writer.push(entry(i));
    }
    writer.commit(); // publishes 0,1,2
    writer.push(entry(3)); // pushed but NOT committed
    // The reader only sees published entries, never the uncommitted tail.
    assert_eq!(walked_ids(&ring, 0), vec![0, 1, 2]);
}

#[test]
fn walk_entries_stops_when_handler_returns_false() {
    let (ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    for i in 0..5 {
        writer.push(entry(i));
    }
    writer.commit();
    let mut got = Vec::new();
    ring.walk_entries(0, |e| {
        got.push(e.tx_id());
        got.len() < 3 // stop after taking three
    });
    assert_eq!(got, vec![0, 1, 2]);
}

#[test]
fn walk_entries_empty_when_caught_up() {
    let (ring, mut writer, _releaser) = TxRing::new(8);
    writer.reserve();
    for i in 0..3 {
        writer.push(entry(i));
    }
    writer.commit(); // write_index = 3
    let mut count = 0;
    ring.walk_entries(3, |_| {
        count += 1;
        true
    });
    assert_eq!(count, 0);
}

#[test]
fn walk_entries_wraps_around_the_buffer() {
    const CAP: usize = 4;
    let (ring, mut writer, mut releaser) = TxRing::new(CAP);
    writer.reserve();
    for i in 0..CAP as u64 {
        writer.push(entry(i));
    }
    writer.commit();
    releaser.advance_to(CAP);
    writer.reserve();
    writer.push(entry(40));
    writer.push(entry(41));
    writer.commit(); // write_index = 6, live window [4, 6)
    // Logical indices 4,5 map to physical slots 0,1.
    assert_eq!(walked_ids(&ring, 4), vec![40, 41]);
}
