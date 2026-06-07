use super::reader::TxRingReader;
use super::ring::TxRing;
use super::writer::TxRingWriter;
use bytemuck::Zeroable;
use storage::entities::{TxEntry, WalEntry, WalEntryKind};

// A distinct entry whose `account_id` doubles as its logical position, so reads are checkable.
// `entry_type` must be stamped: the ring stores raw bytes and re-parses on read, so a
// zeroed discriminator (byte 0) would parse back as `TxMetadata`.
fn entry(id: u64) -> WalEntry {
    WalEntry::Entry(TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        account_id: id,
        ..TxEntry::zeroed()
    })
}

// Read back the position sentinel an `entry` carries.
fn id_of(e: &WalEntry) -> u64 {
    match e {
        WalEntry::Entry(te) => te.account_id,
        other => panic!("expected WalEntry::Entry, got {other:?}"),
    }
}

// Collect the position ids the writer walks over an explicit ring_index range.
fn range_ids(w: &TxRingWriter, start: usize, end: usize) -> Vec<u64> {
    let mut got = Vec::new();
    w.walk(start, end, |e| got.push(id_of(e)));
    got
}

// Collect the position ids the reader walks from `from` up to the published write_index.
fn walked_ids(reader: &TxRingReader, from: usize) -> Vec<u64> {
    let mut got = Vec::new();
    reader.walk(from, |e| {
        got.push(id_of(&e));
        true
    });
    got
}

#[test]
fn reserve_grants_all_free_space() {
    let (mut writer, _reader) = TxRing::new(4);
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
    let (mut writer, reader) = TxRing::new(4);
    writer.reserve();
    for i in 0..4 {
        writer.push(entry(i));
    }
    writer.commit();
    assert_eq!(reader.write_index(), 4);
    assert_eq!(reader.released(), 0);
    assert_eq!(reader.capacity(), 4);
    // Nothing released yet, so re-granting yields no space.
    assert_eq!(writer.reserve(), 0);
    assert_eq!(writer.capacity(), 0);
}

#[test]
fn release_then_reserve() {
    let (mut writer, mut reader) = TxRing::new(4);
    writer.reserve();
    for i in 0..4 {
        writer.push(entry(i));
    }
    assert_eq!(writer.capacity(), 0);
    // publish before releasing so the reader stays within the write cursor
    writer.commit();
    reader.release_to(2);
    assert_eq!(reader.released(), 2);
    assert_eq!(writer.reserve(), 2);
    assert_eq!(writer.capacity(), 2);
    writer.push(entry(4));
    writer.push(entry(5));
    drop(writer);
    assert_eq!(reader.write_index(), 6);
}

#[test]
fn reserve_grants_nothing_until_release() {
    let (mut writer, mut reader) = TxRing::new(4);
    writer.reserve();
    for i in 0..4 {
        writer.push(entry(i));
    }
    writer.commit();
    assert_eq!(writer.reserve(), 0);
    assert_eq!(writer.reserve(), 0);
    reader.release_to(1);
    assert_eq!(writer.reserve(), 1);
}

#[test]
#[should_panic]
fn release_backward_panics() {
    let (mut writer, mut reader) = TxRing::new(4);
    writer.reserve();
    for i in 0..4 {
        writer.push(entry(i));
    }
    writer.commit();
    reader.release_to(3);
    reader.release_to(1);
}

#[test]
#[should_panic]
fn release_past_write_panics() {
    let (mut writer, mut reader) = TxRing::new(4);
    writer.reserve();
    writer.push(entry(0));
    writer.commit();
    reader.release_to(3);
}

#[test]
fn get_returns_value_in_window() {
    let (mut writer, mut reader) = TxRing::new(8);
    writer.reserve();
    for i in 0..6 {
        writer.push(entry(i));
    }
    writer.commit();
    reader.release_to(2); // live window is now [2, 6)
    for i in 2..6usize {
        assert_eq!(id_of(&reader.get(i)), i as u64);
    }
}

#[test]
fn wrap_around() {
    const CAP: usize = 4;
    let (mut writer, mut reader) = TxRing::new(CAP);
    let rounds = 16usize;
    let mut produced = 0usize;
    let mut consumed = 0usize;
    for _ in 0..rounds {
        writer.reserve();
        writer.push(entry(produced as u64));
        writer.push(entry(produced as u64 + 1));
        produced += 2;
        writer.commit();

        let w = reader.write_index();
        while consumed < w {
            assert_eq!(id_of(&reader.get(consumed)), consumed as u64);
            consumed += 1;
        }
        reader.release_to(consumed);
    }
    assert_eq!(produced, rounds * 2);
    assert_eq!(reader.write_index(), rounds * 2);
    assert_eq!(reader.released(), rounds * 2);
}

#[test]
fn push_returns_the_ring_index_written() {
    let (mut writer, _reader) = TxRing::new(8);
    writer.reserve();
    assert_eq!(writer.push(entry(10)), 0);
    assert_eq!(writer.push(entry(11)), 1);
    assert_eq!(writer.push(entry(12)), 2);
    assert_eq!(writer.cursor(), 3);
    let head = writer.cursor();
    assert_eq!(range_ids(&writer, 0, head), vec![10, 11, 12]);
}

#[test]
fn walk_explicit_range_is_half_open() {
    let (mut writer, _reader) = TxRing::new(8);
    writer.reserve();
    for id in 0..4 {
        writer.push(entry(id));
    }
    assert_eq!(range_ids(&writer, 0, 4), vec![0, 1, 2, 3]);
    assert_eq!(range_ids(&writer, 1, 3), vec![1, 2]);
    assert!(range_ids(&writer, 2, 2).is_empty());
}

#[test]
fn walk_wraps_around_the_buffer() {
    const CAP: usize = 4;
    let (mut writer, mut reader) = TxRing::new(CAP);
    writer.reserve();
    for id in 0..CAP as u64 {
        writer.push(entry(id));
    }
    writer.commit();
    reader.release_to(CAP); // free the whole buffer
    assert_eq!(writer.reserve(), CAP);

    // Absolute ring_indexes 4,5,6 map to physical slots 0,1,2.
    assert_eq!(writer.push(entry(40)), 4);
    assert_eq!(writer.push(entry(41)), 5);
    assert_eq!(writer.push(entry(42)), 6);
    assert_eq!(range_ids(&writer, 4, 7), vec![40, 41, 42]);
}

#[test]
fn rollback_to_discards_tail_and_reuses_slots() {
    let (mut writer, reader) = TxRing::new(8);
    writer.reserve();
    for id in 10..14 {
        writer.push(entry(id)); // ring_index 0..4
    }
    assert_eq!(writer.cursor(), 4);
    assert_eq!(writer.capacity(), 4);

    writer.rollback_to(2); // drop entry(12), entry(13)
    assert_eq!(writer.cursor(), 2);
    assert_eq!(writer.capacity(), 6); // their slots are free again

    assert_eq!(writer.push(entry(99)), 2); // reuses ring_index 2
    assert_eq!(range_ids(&writer, 0, 3), vec![10, 11, 99]);

    writer.commit();
    assert_eq!(reader.write_index(), 3);
}

#[test]
fn rollback_to_commit_point_spares_committed_entries() {
    let (mut writer, _reader) = TxRing::new(8);
    writer.reserve();
    writer.push(entry(10));
    writer.push(entry(11));
    writer.commit(); // ring_index 0,1 committed, out of reach
    let committed_head = writer.cursor(); // 2
    writer.push(entry(12)); // ring_index 2, uncommitted
    assert_eq!(writer.cursor(), 3);

    writer.rollback_to(committed_head); // discard only the uncommitted entry(12)
    assert_eq!(writer.cursor(), 2);
}

#[test]
fn walk_reads_committed_from_index_to_write() {
    let (mut writer, reader) = TxRing::new(8);
    writer.reserve();
    for i in 0..5 {
        writer.push(entry(i));
    }
    writer.commit(); // write_index = 5
    assert_eq!(walked_ids(&reader, 0), vec![0, 1, 2, 3, 4]);
    assert_eq!(walked_ids(&reader, 2), vec![2, 3, 4]);
}

#[test]
fn walk_skips_uncommitted_entries() {
    let (mut writer, reader) = TxRing::new(8);
    writer.reserve();
    for i in 0..3 {
        writer.push(entry(i));
    }
    writer.commit(); // publishes 0,1,2
    writer.push(entry(3)); // pushed but NOT committed
    // The reader only sees published entries, never the uncommitted tail.
    assert_eq!(walked_ids(&reader, 0), vec![0, 1, 2]);
}

#[test]
fn walk_stops_when_handler_returns_false() {
    let (mut writer, reader) = TxRing::new(8);
    writer.reserve();
    for i in 0..5 {
        writer.push(entry(i));
    }
    writer.commit();
    let mut got = Vec::new();
    reader.walk(0, |e| {
        got.push(id_of(&e));
        got.len() < 3 // stop after taking three
    });
    assert_eq!(got, vec![0, 1, 2]);
}

#[test]
fn walk_empty_when_caught_up() {
    let (mut writer, reader) = TxRing::new(8);
    writer.reserve();
    for i in 0..3 {
        writer.push(entry(i));
    }
    writer.commit(); // write_index = 3
    let mut count = 0;
    reader.walk(3, |_| {
        count += 1;
        true
    });
    assert_eq!(count, 0);
}

#[test]
fn walk_wraps_around_the_buffer_on_read() {
    const CAP: usize = 4;
    let (mut writer, mut reader) = TxRing::new(CAP);
    writer.reserve();
    for i in 0..CAP as u64 {
        writer.push(entry(i));
    }
    writer.commit();
    reader.release_to(CAP);
    writer.reserve();
    writer.push(entry(40));
    writer.push(entry(41));
    writer.commit(); // write_index = 6, live window [4, 6)
    // Logical indices 4,5 map to physical slots 0,1.
    assert_eq!(walked_ids(&reader, 4), vec![40, 41]);
}
