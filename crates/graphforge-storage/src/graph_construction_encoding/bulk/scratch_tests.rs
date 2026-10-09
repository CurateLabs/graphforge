
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
