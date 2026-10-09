//! Regressions for the bounded endpoint-reference segment lifetime (#1929).
//!
//! Endpoint references are the one scratch set a skewed leaf would hold
//! whole while the endpoint pass resolves it. These tests pin the segment
//! lifetime on small real files: capped physical segments, exact aggregate
//! counts and CRC traffic, one reclaimed segment per verified read, the
//! terminal destructive lifecycle, and the geometry guards that refuse a
//! malformed frame before its claimed length can drive an allocation.

use std::fs::OpenOptions;
use std::io::Write;

use super::*;

const WIDTH: usize = 4;
/// A block header plus three records: every full staging block fills one
/// segment exactly, so ten records rotate across four physical files.
const SEG_CAP: usize = HEADER + 3 * WIDTH;

/// u32 records staged through the ordinary Scatter path, whose buffers a
/// segmented set clamps to one segment's record-aligned payload.
fn scatter_values(partitions: &Partitions, scratch: &Scratch, values: &[u32]) {
    let mut scatter = Scatter::new(scratch, partitions, 256 << 10);
    for (index, value) in values.iter().enumerate() {
        scatter
            .push(index % partitions.len(), &value.to_le_bytes())
            .unwrap();
    }
    scatter.finish().unwrap();
}

/// Append one valid frame to a physical segment from the outside, the way a
/// corrupted or oversized file would grow.
fn append_frame(path: &Path, payload: &[u8]) {
    let mut block = Vec::with_capacity(HEADER + payload.len());
    block.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    block.extend_from_slice(&crc32c(payload).to_le_bytes());
    block.extend_from_slice(payload);
    OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(&block)
        .unwrap();
}

fn tree() -> (tempfile::TempDir, StableDirectory, Scratch) {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    (root, directory, scratch)
}

/// Ten u32 records, three to a segment: four physical files of 20, 20, 20
/// and 12 logical bytes, 72 in total.
fn four_segment_partition(scratch: &Scratch) -> Partitions {
    let partitions = Partitions::create_segmented(scratch, "refs", 1, WIDTH, SEG_CAP).unwrap();
    scatter_values(&partitions, scratch, &(0..10).collect::<Vec<_>>());
    partitions
}

#[test]
fn segmented_writes_split_past_three_segments_and_read_back_identically() {
    let (_root, _directory, scratch) = tree();
    let partitions = four_segment_partition(&scratch);
    assert_eq!(partitions.counts().unwrap(), vec![10]);
    let paths: Vec<_> = (0..4)
        .map(|segment| partitions.segment_path(0, segment))
        .collect();
    let lengths: Vec<_> = paths
        .iter()
        .map(|path| std::fs::metadata(path).unwrap().len())
        .collect();
    assert_eq!(lengths, vec![20, 20, 20, 12]);
    assert!(
        lengths.iter().all(|length| *length <= SEG_CAP as u64),
        "every physical segment holds at most its cap"
    );
    // One write, one read: the aggregate traffic counts every CRC frame.
    assert_eq!(partitions.written_bytes(), 72);
    assert_eq!(scratch.written_bytes(), 72);
    assert_eq!(scratch.occupied_bytes(), 72);
    assert_eq!(scratch.peak_occupied_bytes(), 72);
    let mut seen = Vec::new();
    partitions
        .read(&scratch, 0, |payload| {
            seen.extend(
                payload
                    .chunks_exact(WIDTH)
                    .map(|record| u32::from_le_bytes(record.try_into().expect("4 bytes"))),
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, (0..10).collect::<Vec<_>>());
    assert_eq!(partitions.read_bytes(), 72);
    // The read is non-destructive: a second pass sees the same records and
    // adds the same traffic.
    partitions.read(&scratch, 0, |_| Ok(())).unwrap();
    assert_eq!(partitions.read_bytes(), 144);
    // Cleanup covers every owned segment and releases every byte exactly once.
    partitions.reclaim(&scratch, 0).unwrap();
    assert!(paths.iter().all(|path| !path.exists()));
    assert_eq!(scratch.occupied_bytes(), 0);
    assert_eq!(scratch.peak_occupied_bytes(), 72);
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(
        scratch.occupied_bytes(),
        0,
        "a second reclaim releases nothing"
    );
    scratch.remove().unwrap();
}

#[test]
fn a_skewed_leaf_reclaims_each_verified_segment_before_reading_the_next() {
    let (_root, _directory, scratch) = tree();
    let partitions = four_segment_partition(&scratch);
    // One leaf deliberately holds every reference: the segment count and the
    // per-visit lifetime are what bound it, not the leaf's record count.
    let paths: Vec<_> = (0..4)
        .map(|segment| partitions.segment_path(0, segment))
        .collect();
    let cancel = AtomicBool::new(false);
    let mut visits = Vec::new();
    partitions
        .read_reclaiming(&scratch, 0, &cancel, |payload| {
            assert!(
                payload.len() <= SEG_CAP - HEADER,
                "one callback sees at most one segment's payload"
            );
            visits.push((
                payload.len() / WIDTH,
                (0..4)
                    .map(|segment| paths[segment].exists())
                    .collect::<Vec<_>>(),
                scratch.occupied_bytes(),
            ));
            Ok(())
        })
        .unwrap();
    // Visit v reads segment v: earlier segments are already reclaimed, the
    // current one is still on disk, and the live occupancy is exactly the
    // unread suffix. Delaying any reclaim fails these assertions.
    assert_eq!(
        visits,
        vec![
            (3, vec![true, true, true, true], 72),
            (3, vec![false, true, true, true], 52),
            (3, vec![false, false, true, true], 32),
            (1, vec![false, false, false, true], 12),
        ]
    );
    assert!(visits.iter().all(|(_, _, occupied)| *occupied <= 72));
    assert!(paths.iter().all(|path| !path.exists()));
    assert_eq!(scratch.occupied_bytes(), 0);
    assert_eq!(
        partitions.read_bytes(),
        72,
        "every verified frame is traffic"
    );
    // The read was terminal: the consumed partition takes no more work.
    let mut block = [0_u8; HEADER + WIDTH];
    let error = partitions.append(&scratch, 0, &mut block).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts appends"),
        "{error}"
    );
    let error = partitions.read(&scratch, 0, |_| Ok(())).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts reads"),
        "{error}"
    );
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts reads"),
        "{error}"
    );
    // An explicit reclaim after a whole success is a no-op, not a double
    // release.
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    scratch.remove().unwrap();
}

#[test]
fn a_corrupt_later_segment_fails_terminally_and_keeps_its_file() {
    let (_root, _directory, scratch) = tree();
    let partitions = four_segment_partition(&scratch);
    let paths: Vec<_> = (0..4)
        .map(|segment| partitions.segment_path(0, segment))
        .collect();
    let mut bytes = std::fs::read(&paths[1]).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(&paths[1], bytes).unwrap();
    let cancel = AtomicBool::new(false);
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(error.to_string().contains("CRC32C"), "{error}");
    // Segment 0 was verified and reclaimed first; the corrupt segment and
    // everything after it stayed.
    assert!(!paths[0].exists());
    assert!(paths[1].exists());
    assert!(paths[2].exists());
    assert!(paths[3].exists());
    assert_eq!(
        partitions.read_bytes(),
        20,
        "verified bytes are still traffic"
    );
    // A failed destructive read is terminal: nothing may reuse the rest.
    let mut block = [0_u8; HEADER + WIDTH];
    let error = partitions.append(&scratch, 0, &mut block).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts appends"),
        "{error}"
    );
    let error = partitions.read(&scratch, 0, |_| Ok(())).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts reads"),
        "{error}"
    );
    // Cleanup still accounts the remaining suffix exactly once.
    partitions.reclaim(&scratch, 0).unwrap();
    assert!(paths.iter().skip(1).all(|path| !path.exists()));
    assert_eq!(scratch.occupied_bytes(), 0);
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0, "cleanup never releases twice");
    scratch.remove().unwrap();
}

#[test]
fn a_callback_failure_after_an_earlier_reclaim_keeps_the_current_segment() {
    let (_root, _directory, scratch) = tree();
    let partitions = four_segment_partition(&scratch);
    let paths: Vec<_> = (0..4)
        .map(|segment| partitions.segment_path(0, segment))
        .collect();
    let cancel = AtomicBool::new(false);
    let mut visits = 0;
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_payload| {
            visits += 1;
            if visits == 3 {
                return Err(storage("boom"));
            }
            Ok(())
        })
        .unwrap_err();
    assert!(error.to_string().contains("boom"), "{error}");
    assert_eq!(visits, 3);
    // Two segments were verified and reclaimed before the failure; the
    // segment whose callback failed was never claimed as reclaimed.
    assert!(!paths[0].exists());
    assert!(!paths[1].exists());
    assert!(paths[2].exists());
    assert!(paths[3].exists());
    assert_eq!(scratch.occupied_bytes(), 32);
    // Cleanup removes the remaining suffix, and the partition stays failed.
    partitions.reclaim(&scratch, 0).unwrap();
    assert!(paths.iter().all(|path| !path.exists()));
    assert_eq!(scratch.occupied_bytes(), 0);
    let mut block = [0_u8; HEADER + WIDTH];
    let error = partitions.append(&scratch, 0, &mut block).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts appends"),
        "{error}"
    );
    scratch.remove().unwrap();
}

#[test]
fn a_missing_middle_segment_is_an_error_and_its_cleanup_is_explicit() {
    let (_root, _directory, scratch) = tree();
    let partitions = four_segment_partition(&scratch);
    let paths: Vec<_> = (0..4)
        .map(|segment| partitions.segment_path(0, segment))
        .collect();
    std::fs::remove_file(&paths[1]).unwrap();
    let cancel = AtomicBool::new(false);
    let result = partitions.read_reclaiming(&scratch, 0, &cancel, |_| Ok(()));
    assert!(result.is_err(), "a missing ordinal is never a clean end");
    assert!(!paths[0].exists(), "segment 0 was verified before the miss");
    assert!(paths[2].exists());
    assert!(paths[3].exists());
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts reads"),
        "the failed attempt is terminal: {error}"
    );
    // Cleanup deletes every remaining segment. Segment 1 was deleted outside
    // the set, so its length is unknown and its reservation stays charged:
    // conservative accounting, never a double release.
    partitions.reclaim(&scratch, 0).unwrap();
    assert!(!paths[2].exists());
    assert!(!paths[3].exists());
    assert_eq!(scratch.occupied_bytes(), 20);
    scratch.remove().unwrap();
}

#[test]
fn a_final_segment_truncated_to_zero_is_refused_before_its_unlink() {
    let (_root, _directory, scratch) = tree();
    let partitions = four_segment_partition(&scratch);
    let last = partitions.segment_path(0, 3);
    std::fs::write(&last, b"").unwrap();
    let cancel = AtomicBool::new(false);
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(error.to_string().contains("lost records"), "{error}");
    // The exact-count check ran before the last segment was reclaimed, so an
    // empty final segment can never pass as a consumed partition.
    assert!(last.exists(), "the unverified final segment stays on disk");
    assert_eq!(scratch.occupied_bytes(), 12);
    partitions.reclaim(&scratch, 0).unwrap();
    assert!(!last.exists());
    assert_eq!(scratch.occupied_bytes(), 0);
    scratch.remove().unwrap();
}

#[test]
fn a_final_segment_truncated_at_a_frame_boundary_still_loses_no_evidence() {
    let (_root, _directory, scratch) = tree();
    // Staging four records per block under a ten-record segment cap makes
    // the final segment hold two frames: 21 records land as 48, 48 and 36
    // bytes, the last segment carrying a 24-byte and a 12-byte frame.
    let partitions =
        Partitions::create_segmented(scratch, "refs", 1, WIDTH, HEADER + 10 * WIDTH).unwrap();
    let mut scatter = Scatter::new(scratch, &partitions, 4 * WIDTH);
    for value in 0..21_u32 {
        scatter.push(0, &value.to_le_bytes()).unwrap();
    }
    scatter.finish().unwrap();
    let paths: Vec<_> = (0..3)
        .map(|segment| partitions.segment_path(0, segment))
        .collect();
    let lengths: Vec<_> = paths
        .iter()
        .map(|path| std::fs::metadata(path).unwrap().len())
        .collect();
    assert_eq!(lengths, vec![48, 48, 36]);
    // Drop the final frame exactly: a clean end, one whole record short.
    let mut bytes = std::fs::read(&paths[2]).unwrap();
    bytes.truncate(24);
    std::fs::write(&paths[2], bytes).unwrap();
    let cancel = AtomicBool::new(false);
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(
        error.to_string().contains("lost records"),
        "a cleanly readable final segment is still short its records: {error}"
    );
    assert!(paths[2].exists());
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    scratch.remove().unwrap();
}

#[test]
fn an_overshoot_is_refused_before_the_block_reaches_the_callback() {
    let (_root, _directory, scratch) = tree();
    // Eight records fill a 40-byte segment; ten records leave a two-record
    // final segment that an outside frame pushes past the scattered count.
    let partitions =
        Partitions::create_segmented(scratch, "refs", 1, WIDTH, HEADER + 8 * WIDTH).unwrap();
    scatter_values(&partitions, scratch, &(0..10).collect::<Vec<_>>());
    let last = partitions.segment_path(0, 1);
    assert_eq!(std::fs::metadata(&last).unwrap().len(), 16);
    append_frame(&last, &7_u32.to_le_bytes());
    let cancel = AtomicBool::new(false);
    let mut visits = 0;
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| {
            visits += 1;
            Ok(())
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("more records than were scattered"),
        "{error}"
    );
    assert_eq!(
        visits, 2,
        "the overshooting frame never reached the callback"
    );
    assert!(last.exists(), "the unverified final segment stays on disk");
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    scratch.remove().unwrap();
}

#[test]
fn a_frame_claiming_more_than_the_admitted_length_is_refused_before_resize() {
    let (_root, _directory, scratch) = tree();
    let partitions =
        Partitions::create_segmented(scratch, "refs", 1, WIDTH, HEADER + 6 * WIDTH).unwrap();
    // A 24-byte file whose header claims a 4096-byte payload: with the
    // geometry guard removed, this mutation grows the payload buffer before
    // any length check can refuse it.
    let path = partitions.path(0);
    let mut crafted = Vec::new();
    crafted.extend_from_slice(&4096_u32.to_le_bytes());
    crafted.extend_from_slice(&0_u32.to_le_bytes());
    crafted.extend_from_slice(&[0_u8; 16]);
    std::fs::write(path, &crafted).unwrap();
    let mut payload = Vec::new();
    let mut reader = BlockReader::open_segment(&scratch, path, WIDTH, HEADER + 6 * WIDTH).unwrap();
    let error = reader.next_bounded_block(&mut payload).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("claims more payload than its admitted length holds"),
        "{error}"
    );
    assert_eq!(
        payload.capacity(),
        0,
        "nothing was allocated behind the claim"
    );
    // The same corruption through the destructive read: typed error, no
    // reclaim, terminal failure.
    let cancel = AtomicBool::new(false);
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("claims more payload than its admitted length holds"),
        "{error}"
    );
    assert!(path.exists());
    let error = partitions.read(&scratch, 0, |_| Ok(())).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts reads"),
        "{error}"
    );
    scratch.remove().unwrap();
}

#[test]
fn a_segment_longer_than_its_cap_or_a_misaligned_frame_is_typed() {
    let (_root, _directory, scratch) = tree();
    let partitions =
        Partitions::create_segmented(scratch, "refs", 1, WIDTH, HEADER + 6 * WIDTH).unwrap();
    let path = partitions.path(0);
    // Two valid 12-byte-payload frames exceed the 32-byte cap: the segment
    // is refused at open, before a single frame is honored.
    append_frame(path, &[1_u8; 12]);
    append_frame(path, &[2_u8; 12]);
    let cancel = AtomicBool::new(false);
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("longer than its partition's segment cap"),
        "{error}"
    );
    assert!(path.exists());
    scratch.remove().unwrap();

    let (_root, _directory, scratch) = tree();
    let partitions =
        Partitions::create_segmented(scratch, "refs", 1, WIDTH, HEADER + 6 * WIDTH).unwrap();
    let path = partitions.path(0);
    // A valid CRC over six bytes that are not whole records.
    let payload = [7_u8; 6];
    let mut crafted = Vec::new();
    crafted.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    crafted.extend_from_slice(&crc32c(&payload).to_le_bytes());
    crafted.extend_from_slice(&payload);
    std::fs::write(path, &crafted).unwrap();
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(error.to_string().contains("partial record"), "{error}");
    assert!(path.exists());
    scratch.remove().unwrap();
}

#[test]
fn an_empty_segmented_partition_consumes_its_initial_segment() {
    let (_root, _directory, scratch) = tree();
    let partitions = Partitions::create_segmented(scratch, "refs", 1, WIDTH, SEG_CAP).unwrap();
    assert_eq!(partitions.counts().unwrap(), vec![0]);
    let cancel = AtomicBool::new(false);
    partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap();
    // The empty initial segment is valid without any frame, and its file is
    // still reclaimed by the successful pass.
    assert!(!partitions.path(0).exists());
    assert_eq!(scratch.occupied_bytes(), 0);
    let mut block = [0_u8; HEADER + WIDTH];
    let error = partitions.append(&scratch, 0, &mut block).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts appends"),
        "{error}"
    );
    scratch.remove().unwrap();
}

#[test]
fn a_segment_cap_must_hold_a_header_and_one_record() {
    let (_root, _directory, scratch) = tree();
    let error =
        Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER + WIDTH - 1).unwrap_err();
    assert!(error.to_string().contains("cap must hold"), "{error}");
    let error = Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER - 1).unwrap_err();
    assert!(error.to_string().contains("cap must hold"), "{error}");
    let error = Partitions::create_segmented(&scratch, "refs", 1, 0, 64).unwrap_err();
    assert!(
        error.to_string().contains("positive record width"),
        "{error}"
    );
    assert!(
        !scratch.file("refs-000000.blocks").exists(),
        "a refused set creates no files"
    );
    // The exact boundary holds one record per segment.
    let partitions =
        Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER + WIDTH).unwrap();
    scatter_values(&partitions, &scratch, &[1, 2, 3]);
    let lengths: Vec<_> = (0..3)
        .map(|segment| {
            std::fs::metadata(partitions.segment_path(0, segment))
                .unwrap()
                .len()
        })
        .collect();
    assert_eq!(lengths, vec![12, 12, 12]);
    let cancel = AtomicBool::new(false);
    let mut seen = Vec::new();
    partitions
        .read_reclaiming(&scratch, 0, &cancel, |payload| {
            seen.push(u32::from_le_bytes(payload.try_into().expect("4 bytes")));
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, vec![1, 2, 3]);
    assert_eq!(scratch.occupied_bytes(), 0);
    scratch.remove().unwrap();
}

#[test]
fn concurrent_writers_to_one_segmented_partition_keep_every_record() {
    let (_root, _directory, scratch) = tree();
    let partitions = Partitions::create_segmented(&scratch, "refs", 1, WIDTH, SEG_CAP).unwrap();
    std::thread::scope(|scope| {
        for thread in 0_u32..2 {
            let scratch = &scratch;
            let partitions = &partitions;
            scope.spawn(move || {
                let mut scatter = Scatter::new(scratch, partitions, 256 << 10);
                for offset in 0..30_u32 {
                    scatter
                        .push(0, &(thread * 1000 + offset).to_le_bytes())
                        .unwrap();
                }
                scatter.finish().unwrap();
            });
        }
    });
    assert_eq!(partitions.counts().unwrap(), vec![60]);
    let mut segment = 0;
    while partitions.segment_path(0, segment).exists() {
        let length = std::fs::metadata(partitions.segment_path(0, segment))
            .unwrap()
            .len();
        assert!(
            length <= SEG_CAP as u64,
            "segment {segment} is {length} bytes"
        );
        segment += 1;
    }
    assert!(
        segment > 3,
        "concurrent writes rotated across {segment} segments"
    );
    let mut seen = Vec::new();
    partitions
        .read(&scratch, 0, |payload| {
            seen.extend(
                payload
                    .chunks_exact(WIDTH)
                    .map(|record| u32::from_le_bytes(record.try_into().expect("4 bytes"))),
            );
            Ok(())
        })
        .unwrap();
    seen.sort_unstable();
    let mut expected: Vec<_> = (0_u32..2)
        .flat_map(|thread| (0..30_u32).map(move |offset| thread * 1000 + offset))
        .collect();
    expected.sort_unstable();
    assert_eq!(
        seen, expected,
        "interleaved appends lost or duplicated nothing"
    );
    let cancel = AtomicBool::new(false);
    partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    scratch.remove().unwrap();
}

#[test]
fn an_unsegmented_read_reclaiming_verifies_the_count_then_reclaims_once() {
    let (_root, _directory, scratch) = tree();
    let partitions = Partitions::create(&scratch, "plain", 1, WIDTH).unwrap();
    scatter_values(&partitions, &scratch, &[5, 6, 7, 8, 9]);
    let cancel = AtomicBool::new(false);
    partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap();
    assert!(!partitions.path(0).exists());
    assert_eq!(scratch.occupied_bytes(), 0);

    // A clean read whose frame count overshoots the scattered count is
    // refused on the aggregate, and the file stays for the teardown.
    let (_root, _directory, scratch) = tree();
    let partitions = Partitions::create(&scratch, "plain", 1, WIDTH).unwrap();
    scatter_values(&partitions, &scratch, &[1]);
    append_frame(partitions.path(0), &42_u32.to_le_bytes());
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(error.to_string().contains("lost records"), "{error}");
    assert!(partitions.path(0).exists());
    scratch.remove().unwrap();
}
