use super::*;

#[test]
fn crc32c_matches_the_published_check_value() {
    // RFC 3720 appendix B.4 and the usual "123456789" check value.
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    assert_eq!(crc32c(&[0_u8; 32]), 0x8A91_36AA);
    assert_eq!(crc32c(&[0xff_u8; 32]), 0x62A8_AB43);
    assert_eq!(crc32c(b""), 0);
    let ascending = (0_u8..32).collect::<Vec<_>>();
    assert_eq!(crc32c(&ascending), 0x46DD_794E);
}

#[test]
fn a_flipped_byte_is_caught_when_a_block_is_read_back() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let partitions = Partitions::create(&scratch, "p", 2, 4).unwrap();
    let mut scatter = Scatter::new(&scratch, &partitions, 16);
    for value in 0_u32..10 {
        scatter
            .push((value % 2) as usize, &value.to_le_bytes())
            .unwrap();
    }
    scatter.finish().unwrap();
    assert_eq!(partitions.counts().unwrap(), vec![5, 5]);
    let mut seen = 0;
    partitions
        .read(&scratch, 0, |payload| {
            seen += payload.len() / 4;
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, 5);
    let mut bytes = std::fs::read(partitions.path(0)).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(partitions.path(0), bytes).unwrap();
    let error = partitions.read(&scratch, 0, |_| Ok(())).unwrap_err();
    assert!(error.to_string().contains("CRC32C"), "{error}");
    scratch.remove().unwrap();
    assert!(!root.path().join(SCRATCH_DIRECTORY).exists());
}

#[test]
fn dropping_scratch_deletes_the_tree() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    std::fs::write(scratch.file("x"), b"x").unwrap();
    drop(scratch);
    assert!(!root.path().join(SCRATCH_DIRECTORY).exists());
}

#[test]
fn occupancy_falls_as_files_are_reclaimed_and_the_peak_stays_behind() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let partitions = Partitions::create(&scratch, "p", 2, 4).unwrap();
    let mut scatter = Scatter::new(&scratch, &partitions, 16);
    for value in 0_u32..10 {
        scatter
            .push((value % 2) as usize, &value.to_le_bytes())
            .unwrap();
    }
    scatter.finish().unwrap();
    assert_eq!(scratch.occupied_bytes(), scratch.written_bytes());
    assert_eq!(scratch.peak_occupied_bytes(), scratch.written_bytes());
    partitions.reclaim(&scratch, 0).unwrap();
    let live = scratch.occupied_bytes();
    assert!(live > 0 && live < scratch.written_bytes());
    // A file is reclaimed once: the second call removes nothing more.
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), live);
    assert!(!partitions.path(0).exists());
    partitions.reclaim(&scratch, 1).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    assert_eq!(scratch.peak_occupied_bytes(), scratch.written_bytes());
    assert!(scratch.peak_occupied_bytes() > live);
    scratch.remove().unwrap();
    assert!(!root.path().join(SCRATCH_DIRECTORY).exists());
}

#[test]
fn an_appender_writes_blocks_that_reclaim_to_zero() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let path = scratch.file("appender.blocks");
    let mut appender = Appender::create(&scratch, &path, 16).unwrap();
    for value in 0_u32..5 {
        appender.push(&value.to_le_bytes()).unwrap();
    }
    appender.finish().unwrap();
    // Four records flush as one block; the fifth stays buffered until the
    // finish flushes it as the last block.
    assert_eq!(scratch.occupied_bytes(), (2 * HEADER + 16 + 4) as u64);
    assert_eq!(scratch.written_bytes(), scratch.occupied_bytes());
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        scratch.occupied_bytes()
    );
    scratch.reclaim_file(&path).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    assert_eq!(scratch.peak_occupied_bytes(), (2 * HEADER + 20) as u64);
    scratch.remove().unwrap();
}

#[test]
fn a_failed_append_reserves_before_its_write_and_keeps_the_charge() {
    // The partition file is removed so the append fails on open, before any
    // byte moves. The block must already be reserved then: charging after
    // the write would leave the tracker empty.
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let partitions = Partitions::create(&scratch, "p", 1, 4).unwrap();
    std::fs::remove_file(partitions.path(0)).unwrap();
    let mut scatter = Scatter::new(&scratch, &partitions, 16);
    let mut failed = false;
    for value in 0_u32..4 {
        if scatter.push(0, &value.to_le_bytes()).is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed, "the append must fail on the missing file");
    assert_eq!(partitions.written_bytes(), 0);
    assert_eq!(scratch.written_bytes(), 0);
    let reserved = scratch.occupied_bytes();
    assert!(reserved > 0);
    assert_eq!(scratch.peak_occupied_bytes(), reserved);
    scratch.remove().unwrap();
}

#[test]
fn a_reservation_overlapping_a_live_file_sets_the_peak_before_its_write() {
    // Deterministic stand-in for two workers: while file 0 is live, another
    // writer reserves one block. Reserving before the write keeps the peak
    // at the real overlap even though the live file is reclaimed while the
    // reservation is outstanding; a charge after the write could land after
    // that reclaim and miss the overlap.
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let partitions = Partitions::create(&scratch, "p", 2, 4).unwrap();
    let mut scatter = Scatter::new(&scratch, &partitions, 16);
    for value in 0_u32..8 {
        scatter
            .push((value % 2) as usize, &value.to_le_bytes())
            .unwrap();
    }
    scatter.finish().unwrap();
    // Each file holds one block: 8 header bytes + 4 records of 4 bytes.
    let live = scratch.occupied_bytes();
    assert_eq!(live, 2 * (HEADER + 16) as u64);
    let block = (HEADER + 16) as u64;
    scratch.occupy(block).unwrap();
    assert_eq!(scratch.peak_occupied_bytes(), live + block);
    partitions.reclaim(&scratch, 0).unwrap();
    assert_eq!(scratch.occupied_bytes(), live);
    assert_eq!(scratch.peak_occupied_bytes(), live + block);
    scratch.release(block).unwrap();
    partitions.reclaim(&scratch, 1).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    assert_eq!(scratch.peak_occupied_bytes(), live + block);
    scratch.remove().unwrap();
}

#[test]
fn occupancy_overflow_is_refused_rather_than_saturated() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    scratch.occupy(u64::MAX).unwrap();
    let error = scratch.occupy(1).unwrap_err();
    assert!(error.to_string().contains("overflow"), "{error}");
    assert_eq!(scratch.occupied_bytes(), u64::MAX);
    scratch.remove().unwrap();
}
