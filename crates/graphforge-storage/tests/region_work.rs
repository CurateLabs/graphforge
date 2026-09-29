//! One test per process: process-wide observations include worker threads.
use graphforge_filesystem::ObservedSync;
use graphforge_storage::concurrency_attribution::{ObservedSha256, RegionCapture, RegionScope};
use sha2::{Digest, Sha256};
use std::io::Write;

#[test]
fn work_observation_counts_worker_hashes_writes_and_barriers_and_reconciles() {
    let file = tempfile::tempfile().unwrap();
    let capture = RegionCapture::start("import_command");
    {
        let _stage = RegionScope::named("stage+seal");
        let mut hasher = ObservedSha256::new();
        hasher.update(b"hello");
        let _child = RegionScope::named("append_nodes");
        std::thread::spawn(move || {
            let mut file = file;
            file.write_all(b"worker payload").unwrap();
            file.observed_sync_all().unwrap();
            assert_eq!(
                ObservedSha256::digest(b"worker payload"),
                Sha256::digest(b"worker payload")
            );
        })
        .join()
        .unwrap();
        assert_eq!(hasher.finalize(), Sha256::digest(b"hello"));
    }
    let snapshot = capture.finish();
    assert!(snapshot.complete);
    let root = &snapshot.regions["import_command"].inclusive;
    let child = &snapshot.regions["import_command/stage+seal/append_nodes"].inclusive;
    assert_eq!(root.hashed_bytes, Some(19));
    assert_eq!(child.hashed_bytes, Some(14));
    assert_eq!(root.fsync_calls, Some(1));
    assert_eq!(child.fsync_calls, Some(1));
    assert!(root.hash_elapsed_ns.unwrap() > 0);
    assert!(root.fsync_elapsed_ns.unwrap() > 0);
    #[cfg(target_os = "linux")]
    {
        // Assertion diagnostics may use write syscalls too. The observed
        // worker payload must be present even though no worker scopes exist.
        assert!(root.written_bytes.unwrap() >= 14);
        assert!(child.written_bytes.unwrap() >= 14);
    }
    for field in [
        "wall_ns",
        "hashed_bytes",
        "written_bytes",
        "hash_elapsed_ns",
        "fsync_calls",
        "fsync_elapsed_ns",
    ] {
        let value = serde_json::to_value(&snapshot).unwrap();
        if let Some(inclusive) = value["regions"]["import_command"]["inclusive"][field].as_u64() {
            let residuals: u64 = value["regions"]
                .as_object()
                .unwrap()
                .values()
                .map(|row| row["residual"][field].as_u64().unwrap())
                .sum();
            assert_eq!(inclusive, residuals, "{field}");
        }
    }
    // A later capture observes no historical work. Nested captures never
    // reset counters, and without a capture hashing retains its old behavior.
    assert_eq!(
        ObservedSha256::digest(b"outside"),
        Sha256::digest(b"outside")
    );
    let idle = RegionCapture::start("import_command").finish();
    assert_eq!(
        idle.regions["import_command"].inclusive.hashed_bytes,
        Some(0)
    );
    assert_eq!(
        idle.regions["import_command"].inclusive.fsync_calls,
        Some(0)
    );
}
