use super::*;

#[test]
fn cache_release_never_runs_before_successful_synchronization() {
    use std::cell::RefCell;

    let calls = RefCell::new(Vec::new());
    synchronize_before_release(
        || {
            calls.borrow_mut().push("sync");
            Ok(())
        },
        || {
            calls.borrow_mut().push("release");
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(*calls.borrow(), ["sync", "release"]);

    calls.borrow_mut().clear();
    let error = synchronize_before_release(
        || {
            calls.borrow_mut().push("sync");
            Err(io::Error::other("sync failed"))
        },
        || {
            calls.borrow_mut().push("release");
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "sync failed");
    assert_eq!(*calls.borrow(), ["sync"]);
}

#[test]
fn durable_cache_writer_preserves_bytes_and_reports_platform_semantics() {
    use std::io::Read as _;
    use std::num::NonZeroU64;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("bounded");
    let file = File::create(&path).unwrap();
    let mut writer =
        DurableFileCacheWriter::with_window_bytes(file, NonZeroU64::new(8).unwrap()).unwrap();
    writer.write_all(b"0123456789abcdefghij").unwrap();

    #[cfg(target_os = "linux")]
    {
        let evidence = writer.evidence();
        assert_eq!(evidence.sync_operations, 2);
        assert_eq!(evidence.release_operations, 2);
        assert_eq!(evidence.released_bytes, 16);
        assert_eq!(evidence.peak_window_bytes, 8);
    }
    #[cfg(not(target_os = "linux"))]
    assert_eq!(writer.evidence(), FileCacheReleaseEvidence::default());

    writer.sync_all_and_release().unwrap();
    let evidence = writer.evidence();
    assert_eq!(
        evidence.sync_operations,
        if cfg!(target_os = "linux") { 3 } else { 1 }
    );
    assert!(evidence.peak_window_bytes <= 8 || !cfg!(target_os = "linux"));
    #[cfg(target_os = "linux")]
    {
        assert_eq!(evidence.release_operations, 3);
        assert_eq!(evidence.unsupported_operations, 0);
        assert_eq!(evidence.released_bytes, 20);
    }
    #[cfg(not(target_os = "linux"))]
    {
        assert_eq!(evidence.release_operations, 0);
        assert_eq!(evidence.unsupported_operations, 1);
        assert_eq!(evidence.released_bytes, 0);
        assert_eq!(evidence.peak_window_bytes, 20);
    }

    drop(writer);
    let mut bytes = Vec::new();
    File::open(path).unwrap().read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"0123456789abcdefghij");
}

#[test]
fn shared_cache_budget_covers_every_construction_operation_topology() {
    for (operation, active_streams) in [
        ("copy", 2_usize),
        ("v3-index", 3),
        ("v4-projections", 2),
        ("node-encoding", 5),
        ("edge-encoding", 4),
        ("merge-two-way", 3),
        ("merge-fan-in", 33),
    ] {
        let window = cache_release_window_for_streams(active_streams).unwrap();
        let aggregate = window
            .get()
            .checked_mul(u64::try_from(active_streams).unwrap())
            .unwrap();
        assert!(window.get() > 0, "{operation}");
        assert!(
            aggregate <= DEFAULT_CACHE_RELEASE_WINDOW_BYTES,
            "{operation}: {aggregate}"
        );
    }
    assert!(cache_release_window_for_streams(0).is_err());
    assert!(
        cache_release_window_for_streams(
            usize::try_from(DEFAULT_CACHE_RELEASE_WINDOW_BYTES).unwrap() + 1
        )
        .is_err()
    );
}

#[test]
fn operation_budget_validation_uses_opened_reader_and_writer_configuration() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let output = root.path().join("output");
    std::fs::write(&source, b"source").unwrap();
    let window = cache_release_window_for_streams(2).unwrap();
    let reader = FileCacheReleasingReader::with_window_bytes(
        File::open(source).unwrap(),
        window,
        FileCacheReleaseTracker::default(),
    )
    .unwrap();
    let writer =
        DurableFileCacheWriter::with_window_bytes(File::create(output).unwrap(), window).unwrap();

    assert_eq!(
        validate_cache_release_operation_windows(&[reader.window_bytes(), writer.window_bytes(),])
            .unwrap(),
        DEFAULT_CACHE_RELEASE_WINDOW_BYTES
    );
    assert!(
        validate_cache_release_operation_windows(&[
            NonZeroU64::new(DEFAULT_CACHE_RELEASE_WINDOW_BYTES).unwrap(),
            writer.window_bytes(),
        ])
        .is_err()
    );
}

#[test]
fn durable_cache_writer_respects_current_offset() {
    use std::io::{Read as _, Seek as _, SeekFrom};
    use std::num::NonZeroU64;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("offset");
    std::fs::write(&path, b"prefix-xxxxxxx").unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(7)).unwrap();
    let mut writer =
        DurableFileCacheWriter::with_window_bytes(file, NonZeroU64::new(3).unwrap()).unwrap();
    writer.write_all(b"changed").unwrap();
    writer.sync_all_and_release().unwrap();
    drop(writer);

    let mut actual = Vec::new();
    File::open(path).unwrap().read_to_end(&mut actual).unwrap();
    assert_eq!(actual, b"prefix-changed");
}

#[test]
fn durable_cache_writer_sync_failure_precedes_next_write_consumption() {
    use std::io::Read as _;
    use std::num::NonZeroU64;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("sync-failure");
    let mut writer = DurableFileCacheWriter::with_window_bytes(
        File::create(&path).unwrap(),
        NonZeroU64::new(8).unwrap(),
    )
    .unwrap();
    writer.write_all(b"12345678").unwrap();
    inject_cache_sync_failure();
    #[cfg(target_os = "linux")]
    {
        assert_eq!(
            writer.write(b"9").unwrap_err().to_string(),
            "injected cache-sync failure"
        );
        assert_eq!(writer.write(b"9").unwrap(), 1);
        writer.sync_all_and_release().unwrap();
    }
    #[cfg(not(target_os = "linux"))]
    {
        assert_eq!(writer.write(b"9").unwrap(), 1);
        assert_eq!(
            writer.sync_all_and_release().unwrap_err().to_string(),
            "injected cache-sync failure"
        );
        writer.sync_all_and_release().unwrap();
    }
    drop(writer);

    let mut actual = Vec::new();
    File::open(path).unwrap().read_to_end(&mut actual).unwrap();
    assert_eq!(actual, b"123456789");
}

#[test]
fn durable_cache_writer_surfaces_advice_failure_after_durability() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("writer-advice-failure");
    let mut writer = DurableFileCacheWriter::new(File::create(&path).unwrap()).unwrap();
    writer.write_all(b"durable").unwrap();
    inject_cache_release_failure();
    assert_eq!(
        writer.sync_all_and_release().unwrap_err().to_string(),
        "injected cache-release failure"
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"durable");
    writer.sync_all_and_release().unwrap();
}

#[test]
fn cache_releasing_reader_releases_completed_and_partial_windows_on_drop() {
    use std::io::Read as _;
    use std::num::NonZeroU64;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("drop-reader");
    std::fs::write(&path, b"0123456789abcdefghij").unwrap();
    let tracker = FileCacheReleaseTracker::default();
    let mut reader = FileCacheReleasingReader::with_window_bytes(
        File::open(path).unwrap(),
        NonZeroU64::new(8).unwrap(),
        tracker.clone(),
    )
    .unwrap();
    let mut bytes = [0_u8; 9];
    reader.read_exact(&mut bytes).unwrap();
    drop(reader);
    tracker.check_error().unwrap();

    let evidence = tracker.evidence();
    #[cfg(target_os = "linux")]
    {
        assert_eq!(evidence.release_operations, 2);
        assert_eq!(evidence.released_bytes, 9);
        assert_eq!(evidence.peak_window_bytes, 8);
    }
    #[cfg(not(target_os = "linux"))]
    {
        assert_eq!(evidence.unsupported_operations, 1);
        assert_eq!(evidence.peak_window_bytes, 9);
    }
}

#[test]
fn cache_releasing_reader_empty_reads_preserve_remaining_data_and_evidence() {
    use std::io::Read as _;
    use std::num::NonZeroU64;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("empty-read");
    let expected = b"0123456789abcdef";
    std::fs::write(&path, expected).unwrap();

    for prefix_length in [0, 3, 8] {
        let tracker = FileCacheReleaseTracker::default();
        let mut reader = FileCacheReleasingReader::with_window_bytes(
            File::open(&path).unwrap(),
            NonZeroU64::new(8).unwrap(),
            tracker.clone(),
        )
        .unwrap();
        let mut actual = vec![0; prefix_length];
        reader.read_exact(&mut actual).unwrap();
        let before_empty_read = tracker.evidence();
        assert_eq!(reader.read(&mut []).unwrap(), 0);
        assert_eq!(tracker.evidence(), before_empty_read);
        reader.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, expected, "prefix length {prefix_length}");
        reader.finish().unwrap();
    }
}

#[test]
fn cache_releasing_reader_real_syscalls_cover_two_windows_and_partial_tail() {
    use std::io::Read as _;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("large-sparse-reader");
    let length = DEFAULT_CACHE_RELEASE_WINDOW_BYTES
        .checked_mul(2)
        .unwrap()
        .checked_add(1_048_593)
        .unwrap();
    let file = File::create(&path).unwrap();
    file.set_len(length).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let mut reader = FileCacheReleasingReader::new(File::open(path).unwrap()).unwrap();
    let mut buffer = vec![0_u8; 1 << 20];
    let mut read = 0_u64;
    loop {
        let count = reader.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        read += count as u64;
    }
    let evidence = reader.finish().unwrap();
    assert_eq!(read, length);
    #[cfg(target_os = "linux")]
    {
        assert_eq!(evidence.release_operations, 3);
        assert_eq!(evidence.released_bytes, length);
        assert_eq!(
            evidence.peak_window_bytes,
            DEFAULT_CACHE_RELEASE_WINDOW_BYTES
        );
    }
    #[cfg(not(target_os = "linux"))]
    {
        assert_eq!(evidence.unsupported_operations, 1);
        assert_eq!(evidence.peak_window_bytes, length);
    }
}

#[test]
fn cache_releasing_reader_surfaces_injected_advice_failure() {
    use std::io::Read as _;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("advice-failure");
    std::fs::write(&path, b"consumed").unwrap();
    let mut reader = FileCacheReleasingReader::new(File::open(path).unwrap()).unwrap();
    let mut bytes = [0_u8; 8];
    assert_eq!(reader.read(&mut bytes).unwrap(), 8);
    inject_cache_release_failure();
    assert_eq!(
        reader.finish().unwrap_err().to_string(),
        "injected cache-release failure"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn injected_advice_failures_after_completed_windows_preserve_prior_evidence() {
    use std::io::{Read as _, Write as _};
    use std::num::NonZeroU64;

    let root = tempfile::tempdir().unwrap();
    let written_path = root.path().join("writer-spanning-failure");
    let mut writer = DurableFileCacheWriter::with_window_bytes(
        File::create(&written_path).unwrap(),
        NonZeroU64::new(8).unwrap(),
    )
    .unwrap();
    writer.write_all(b"0123456789abcdef").unwrap();
    assert_eq!(writer.evidence().release_operations, 1);
    inject_cache_release_failure();
    assert_eq!(
        writer.write(b"x").unwrap_err().to_string(),
        "injected cache-release failure"
    );
    assert_eq!(writer.evidence().released_bytes, 8);
    writer.write_all(b"x").unwrap();
    writer.sync_all_and_release().unwrap();
    assert_eq!(std::fs::read(&written_path).unwrap(), b"0123456789abcdefx");

    let read_path = root.path().join("reader-spanning-failure");
    std::fs::write(&read_path, b"0123456789abcdefghijklmnop").unwrap();
    let tracker = FileCacheReleaseTracker::default();
    let mut reader = FileCacheReleasingReader::with_window_bytes(
        File::open(read_path).unwrap(),
        NonZeroU64::new(8).unwrap(),
        tracker.clone(),
    )
    .unwrap();
    let mut prefix = [0_u8; 16];
    reader.read_exact(&mut prefix).unwrap();
    assert_eq!(tracker.evidence().release_operations, 1);
    inject_cache_release_failure();
    assert_eq!(
        reader.read(&mut [0_u8; 1]).unwrap_err().to_string(),
        "injected cache-release failure"
    );
    assert_eq!(tracker.evidence().released_bytes, 8);
    reader.finish().unwrap();
}
