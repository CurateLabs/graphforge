//! Regressions for the bounded endpoint-reference segment lifetime (#1929).
//!
//! Endpoint references are the one scratch set a skewed leaf would hold
//! whole while the endpoint pass resolves it. These tests pin the segment
//! lifetime on small real files: capped physical segments, exact aggregate
//! counts and CRC traffic, one reclaimed segment per verified read, real
//! counted output whose peak proves the input/output overlap bound, the
//! terminal destructive lifecycle, exclusive initial creation, serialized
//! idempotent cleanup, per-frame cancellation, and the geometry and
//! overflow guards that refuse a malformed claim before it can allocate or
//! mutate.

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

/// Refuse a segmented constructor and return its typed error, without
/// requiring a `Debug` impl on the constructed set.
fn refused(build: impl FnOnce() -> Result<Partitions, GfError>) -> GfError {
    match build() {
        Ok(_) => panic!("the segmented constructor was expected to refuse"),
        Err(error) => error,
    }
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
    // The endpoint pass produces output while the skewed leaf still holds
    // input: every callback emits one 8-byte output record per resolved
    // input record in a real 16-byte CRC frame, so the peak pins the actual
    // input/output overlap, not just the input lifetime.
    let outputs = Partitions::create(&scratch, "resolved", 1, 8).unwrap();
    // One leaf deliberately holds every reference: the segment count and the
    // per-visit lifetime are what bound it, not the leaf's record count.
    let paths: Vec<_> = (0..4)
        .map(|segment| partitions.segment_path(0, segment))
        .collect();
    let cancel = AtomicBool::new(false);
    let mut out = Scatter::new(&scratch, &outputs, 8);
    let mut visits = Vec::new();
    partitions
        .read_reclaiming(&scratch, 0, &cancel, |payload| {
            assert!(
                payload.len() <= SEG_CAP - HEADER,
                "one callback sees at most one segment's payload"
            );
            for record in payload.chunks_exact(WIDTH) {
                let value = u32::from_le_bytes(record.try_into().expect("4 bytes"));
                out.push(0, &(u64::from(value) * 3).to_le_bytes()).unwrap();
            }
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
    // current one is still on disk, and the live occupancy is the unread
    // input plus every output frame written so far. Delaying any reclaim
    // fails these assertions.
    assert_eq!(
        visits,
        vec![
            (3, vec![true, true, true, true], 120),
            (3, vec![false, true, true, true], 148),
            (3, vec![false, false, true, true], 176),
            (1, vec![false, false, false, true], 172),
        ]
    );
    assert!(paths.iter().all(|path| !path.exists()));
    // Ten resolved records: 160 bytes of real output. The peak is the third
    // visit: every output byte plus the one input segment still unread at
    // that moment (160 + 20). An implementation that delays every unlink to
    // the end of the pass instead peaks at 232 and fails this bound.
    assert_eq!(outputs.counts().unwrap(), vec![10]);
    assert_eq!(outputs.written_bytes(), 160);
    assert_eq!(
        partitions.read_bytes(),
        72,
        "every verified frame is traffic"
    );
    assert_eq!(scratch.peak_occupied_bytes(), 176);
    assert!(
        scratch.peak_occupied_bytes() <= outputs.written_bytes() + 20,
        "the output never overlaps more than one unread input segment"
    );
    assert_eq!(scratch.occupied_bytes(), 160);
    outputs.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
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
    drop(out);
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
    let error = partitions.read(&scratch, 0, |_| Ok(())).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts reads"),
        "a cleaned-up failure never returns to Writing: {error}"
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
    // The zero-length file leaves with its own actual bytes, so its
    // conservative reservation stays charged: never a double release.
    assert_eq!(scratch.occupied_bytes(), 12);
    scratch.remove().unwrap();
}

#[test]
fn a_final_segment_truncated_at_a_frame_boundary_still_loses_no_evidence() {
    let (_root, _directory, scratch) = tree();
    // Staging four records per block under a ten-record segment cap makes
    // the final segment hold two frames: 21 records land as 48, 48 and 36
    // bytes, the last segment carrying a 24-byte and a 12-byte frame.
    let partitions =
        Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER + 10 * WIDTH).unwrap();
    let mut scatter = Scatter::new(&scratch, &partitions, 4 * WIDTH);
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
    assert!(!paths[2].exists());
    // Reclaiming the truncated file releases its actual 24 bytes; the 12
    // bytes the outside truncation destroyed stay conservatively charged.
    assert_eq!(scratch.occupied_bytes(), 12);
    drop(scatter);
    scratch.remove().unwrap();
}

#[test]
fn an_overshoot_is_refused_before_the_block_reaches_the_callback() {
    let (_root, _directory, scratch) = tree();
    // Eight records fill a 40-byte segment; ten records leave a two-record
    // final segment that an outside frame pushes past the scattered count.
    let partitions =
        Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER + 8 * WIDTH).unwrap();
    scatter_values(&partitions, &scratch, &(0..10).collect::<Vec<_>>());
    let last = partitions.segment_path(0, 1);
    assert_eq!(std::fs::metadata(&last).unwrap().len(), 16);
    // The outside frame is real tree occupancy: reserve it the way a writer
    // would have, so the file it lands in can be released exactly once.
    scratch.occupy((HEADER + WIDTH) as u64).unwrap();
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
    // The first segment left with its bytes; the oversized final file keeps
    // the whole tree's charge until it is reclaimed.
    assert_eq!(scratch.occupied_bytes(), 28);
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    scratch.remove().unwrap();
}

#[test]
fn a_frame_claiming_more_than_the_admitted_length_is_refused_before_resize() {
    let (_root, _directory, scratch) = tree();
    let partitions =
        Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER + 6 * WIDTH).unwrap();
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
    let error = {
        let mut reader =
            BlockReader::open_segment(&scratch, path, WIDTH, HEADER + 6 * WIDTH).unwrap();
        reader.next_bounded_block(&mut payload).unwrap_err()
    };
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
fn a_generic_reader_refuses_a_claim_beyond_its_file_before_any_resize() {
    let (_root, _directory, scratch) = tree();
    let path = scratch.file("plain.blocks");
    // The same small corrupt file through the plain generic open: the opened
    // handle's own length bounds the claim before any allocation.
    let mut crafted = Vec::new();
    crafted.extend_from_slice(&4096_u32.to_le_bytes());
    crafted.extend_from_slice(&0_u32.to_le_bytes());
    crafted.extend_from_slice(&[0_u8; 16]);
    std::fs::write(&path, &crafted).unwrap();
    let mut payload = Vec::new();
    let error = {
        let mut reader = BlockReader::open(&scratch, &path).unwrap();
        reader.next_block(&mut payload).unwrap_err()
    };
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
    scratch.remove().unwrap();
}

#[test]
fn an_ordinary_reader_keeps_its_empty_frame_semantics() {
    let (_root, _directory, scratch) = tree();
    let path = scratch.file("plain.blocks");
    let mut frame = Vec::new();
    frame.extend_from_slice(&0_u32.to_le_bytes());
    frame.extend_from_slice(&crc32c(b"").to_le_bytes());
    std::fs::write(&path, &frame).unwrap();
    let mut payload = Vec::new();
    let (first, second) = {
        let mut reader = BlockReader::open(&scratch, &path).unwrap();
        let first = reader.next_block(&mut payload).unwrap();
        let second = reader.next_block(&mut payload).unwrap();
        (first, second)
    };
    assert!(first, "an ordinary empty frame is still a block");
    assert!(payload.is_empty());
    assert!(!second, "and the file then ends cleanly");
    scratch.remove().unwrap();
}

#[test]
fn a_segment_longer_than_its_cap_or_a_misaligned_frame_is_typed() {
    let (_root, _directory, scratch) = tree();
    let partitions =
        Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER + 6 * WIDTH).unwrap();
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
        Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER + 6 * WIDTH).unwrap();
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
    let partitions = Partitions::create_segmented(&scratch, "refs", 1, WIDTH, SEG_CAP).unwrap();
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
        refused(|| Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER + WIDTH - 1));
    assert!(error.to_string().contains("cap must hold"), "{error}");
    let error = refused(|| Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER - 1));
    assert!(error.to_string().contains("cap must hold"), "{error}");
    let error = refused(|| Partitions::create_segmented(&scratch, "refs", 1, 0, 64));
    assert!(
        error.to_string().contains("positive record width"),
        "{error}"
    );
    // A width whose header-plus-record floor cannot even be computed is
    // refused by the checked arithmetic, never by a wrap.
    let error =
        refused(|| Partitions::create_segmented(&scratch, "refs", 1, usize::MAX, usize::MAX));
    assert!(error.to_string().contains("cap must hold"), "{error}");
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
fn an_existing_initial_segment_is_refused_and_left_untouched() {
    let (_root, _directory, scratch) = tree();
    let sentinel = scratch.file("refs-000000.blocks");
    std::fs::write(&sentinel, b"another owner's bytes").unwrap();
    let error = refused(|| Partitions::create_segmented(&scratch, "refs", 1, WIDTH, SEG_CAP));
    assert!(error.to_string().contains("exists"), "{error}");
    assert_eq!(
        std::fs::read(&sentinel).unwrap(),
        b"another owner's bytes",
        "the exclusive initial create never truncates another owner's file"
    );
    scratch.remove().unwrap();
}

#[test]
fn concurrent_cleanups_serialize_and_release_every_byte_once() {
    let (_root, _directory, scratch) = tree();
    let partitions = four_segment_partition(&scratch);
    let paths: Vec<_> = (0..4)
        .map(|segment| partitions.segment_path(0, segment))
        .collect();
    let results = std::thread::scope(|scope| {
        let scratch = &scratch;
        let partitions = &partitions;
        (0..4)
            .map(|_| scope.spawn(move || partitions.reclaim(scratch, 0)))
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(
        results.iter().all(|result| result.is_ok()),
        "no concurrent cleanup reports a spurious miss: {results:?}"
    );
    assert!(paths.iter().all(|path| !path.exists()));
    assert_eq!(scratch.occupied_bytes(), 0, "cleanup never releases twice");
    // The set is consumed: no more work, and a further cleanup is a no-op.
    let mut block = [0_u8; HEADER + WIDTH];
    let error = partitions.append(&scratch, 0, &mut block).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts appends"),
        "{error}"
    );
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    scratch.remove().unwrap();
}

#[test]
fn a_cleanup_attempt_inside_a_running_destructive_read_is_refused() {
    let (_root, _directory, scratch) = tree();
    let partitions = four_segment_partition(&scratch);
    let cancel = AtomicBool::new(false);
    let mut refusal = None;
    partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| {
            // The running reader owns the remaining segments: cleanup is
            // refused, never interleaved with the pass.
            refusal = Some(partitions.reclaim(&scratch, 0).unwrap_err().to_string());
            Ok(())
        })
        .unwrap();
    let refusal = refusal.expect("every visit attempted a cleanup");
    assert!(refusal.contains("owns its remaining segments"), "{refusal}");
    // The read still consumed the whole partition: every segment gone, and
    // an explicit reclaim afterwards is a no-op.
    assert!((0..4).all(|segment| !partitions.segment_path(0, segment).exists()));
    assert_eq!(scratch.occupied_bytes(), 0);
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    scratch.remove().unwrap();
}

#[test]
fn a_cancellation_mid_segment_stops_the_second_callback_and_reclaims_nothing() {
    let (_root, _directory, scratch) = tree();
    // Two 4-record frames land in one 48-byte segment: the token must stop
    // the pass between its frames, not only between segments.
    let partitions =
        Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER + 10 * WIDTH).unwrap();
    let mut scatter = Scatter::new(&scratch, &partitions, 4 * WIDTH);
    for value in 0..8_u32 {
        scatter.push(0, &value.to_le_bytes()).unwrap();
    }
    scatter.finish().unwrap();
    let path = partitions.segment_path(0, 0);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 48);
    let cancel = AtomicBool::new(false);
    let mut visits = 0;
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_payload| {
            visits += 1;
            cancel.store(true, Ordering::Release);
            Ok(())
        })
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert_eq!(visits, 1, "no callback runs after the token is set");
    assert!(path.exists(), "a cancelled read reclaims nothing");
    assert_eq!(scratch.occupied_bytes(), 48);
    // The cancelled pass is terminal.
    let mut block = [0_u8; HEADER + WIDTH];
    let error = partitions.append(&scratch, 0, &mut block).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts appends"),
        "{error}"
    );
    drop(scatter);
    scratch.remove().unwrap();

    // A token observed before an empty final segment's unlink refuses that
    // unlink too.
    let (_root, _directory, scratch) = tree();
    let partitions = Partitions::create_segmented(&scratch, "refs", 1, WIDTH, SEG_CAP).unwrap();
    let cancel = AtomicBool::new(true);
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert!(
        partitions.path(0).exists(),
        "even an empty final segment is not unlinked once cancelled"
    );
    scratch.remove().unwrap();
}

#[test]
fn refused_appends_never_mutate_files_counters_or_lifecycle() {
    let (_root, _directory, scratch) = tree();
    let partitions = Partitions::create_segmented(&scratch, "refs", 1, WIDTH, SEG_CAP).unwrap();
    let first = partitions.path(0).to_path_buf();
    let occupied = scratch.occupied_bytes();
    let written = partitions.written_bytes();

    // Shorter than a header.
    let mut short = [0_u8; 4];
    let error = partitions.append(&scratch, 0, &mut short).unwrap_err();
    assert!(
        error.to_string().contains("shorter than its header"),
        "{error}"
    );

    // A partial record on a segmented partition is typed, not a debug assert.
    let mut partial = vec![0_u8; HEADER + 6];
    let error = partitions.append(&scratch, 0, &mut partial).unwrap_err();
    assert!(error.to_string().contains("partial record"), "{error}");

    // An aggregate record-count overflow is refused before any byte moves:
    // this child test fabricates the extreme count through the parent's
    // private state.
    partitions.state[0].lock().unwrap().records = u64::MAX;
    let mut block = vec![0_u8; HEADER + WIDTH];
    let error = partitions.append(&scratch, 0, &mut block).unwrap_err();
    assert!(
        error.to_string().contains("record count overflowed"),
        "{error}"
    );

    // A segment ordinal overflow is refused before the next file is created.
    {
        let mut progress = partitions.state[0].lock().unwrap();
        progress.records = 0;
        progress.segment = u64::MAX;
        progress.segment_bytes = (SEG_CAP - HEADER - WIDTH + 1) as u64;
    }
    let error = partitions.append(&scratch, 0, &mut block).unwrap_err();
    assert!(error.to_string().contains("ordinal overflowed"), "{error}");
    {
        let mut progress = partitions.state[0].lock().unwrap();
        progress.segment = 0;
        progress.segment_bytes = 0;
    }

    // Nothing moved: no bytes, no counter, no file, no terminal state.
    assert_eq!(scratch.occupied_bytes(), occupied);
    assert_eq!(partitions.written_bytes(), written);
    assert_eq!(partitions.counts().unwrap(), vec![0]);
    assert_eq!(std::fs::metadata(&first).unwrap().len(), 0);
    assert!(!partitions.segment_path(0, 1).exists());
    scatter_values(&partitions, &scratch, &[42]);
    assert_eq!(
        partitions.counts().unwrap(),
        vec![1],
        "the partition still accepts a good append"
    );
    scratch.remove().unwrap();
}

#[test]
fn a_non_destructive_read_validates_counts_and_restores_writing_on_success() {
    let (_root, _directory, scratch) = tree();
    // Twelve records staged four to a frame land as 48- and 24-byte
    // segments.
    let partitions =
        Partitions::create_segmented(&scratch, "refs", 1, WIDTH, HEADER + 10 * WIDTH).unwrap();
    let mut scatter = Scatter::new(&scratch, &partitions, 4 * WIDTH);
    for value in 0..12_u32 {
        scatter.push(0, &value.to_le_bytes()).unwrap();
    }
    scatter.finish().unwrap();
    let paths: Vec<_> = (0..2)
        .map(|segment| partitions.segment_path(0, segment))
        .collect();
    let lengths: Vec<_> = paths
        .iter()
        .map(|path| std::fs::metadata(path).unwrap().len())
        .collect();
    assert_eq!(lengths, vec![48, 24]);
    assert_eq!(scratch.occupied_bytes(), 72);

    // A verified non-destructive pass restores Writing: the partition
    // accepts an append again and the counters keep accumulating.
    partitions.read(&scratch, 0, |_| Ok(())).unwrap();
    assert_eq!(partitions.read_bytes(), 72);
    scatter_values(&partitions, &scratch, &[100]);
    assert_eq!(partitions.counts().unwrap(), vec![13]);
    assert_eq!(std::fs::metadata(&paths[1]).unwrap().len(), 36);

    // Dropping the last frame makes the aggregate count short: the read
    // refuses, terminalizes, and leaves every file for the explicit cleanup.
    let mut bytes = std::fs::read(&paths[1]).unwrap();
    bytes.truncate(24);
    std::fs::write(&paths[1], bytes).unwrap();
    let error = partitions.read(&scratch, 0, |_| Ok(())).unwrap_err();
    assert!(error.to_string().contains("lost records"), "{error}");
    assert!(paths.iter().all(|path| path.exists()));
    let mut block = [0_u8; HEADER + WIDTH];
    let error = partitions.append(&scratch, 0, &mut block).unwrap_err();
    assert!(
        error.to_string().contains("no longer accepts appends"),
        "{error}"
    );

    // Cleanup releases the files' actual bytes; the bytes the outside
    // truncation destroyed stay conservatively charged.
    partitions.reclaim(&scratch, 0).unwrap();
    assert!(paths.iter().all(|path| !path.exists()));
    assert_eq!(scratch.occupied_bytes(), 12);
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), 12, "cleanup never releases twice");
    drop(scatter);
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
    // refused before the callback, and the file stays for the teardown.
    let (_root, _directory, scratch) = tree();
    let partitions = Partitions::create(&scratch, "plain", 1, WIDTH).unwrap();
    scatter_values(&partitions, &scratch, &[1]);
    append_frame(partitions.path(0), &42_u32.to_le_bytes());
    let error = partitions
        .read_reclaiming(&scratch, 0, &cancel, |_| Ok(()))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("more records than were scattered"),
        "{error}"
    );
    assert!(partitions.path(0).exists());
    scratch.remove().unwrap();
}
