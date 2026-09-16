use super::*;
use crate::graph_object_store::AuthenticatedGraphFile;
use crate::graph_object_store::BEFORE_OBJECT_LINK;
use crate::graph_object_store::BTreeMap;
use crate::graph_object_store::File;
use crate::graph_object_store::GRAPH_OBJECTS_DIR;
use crate::graph_object_store::GraphManifestState;
#[cfg(windows)]
use crate::graph_object_store::OpenOptions;
use crate::graph_object_store::PathBuf;
use crate::graph_object_store::SHA256_DIR;
use crate::graph_object_store::Sha256;
#[cfg(windows)]
use crate::graph_object_store::StableDirectory;
use crate::graph_object_store::TEMP_DIR;
use crate::graph_object_store::append_authenticated_graph_files_v2;
use crate::graph_object_store::begin_graph_object_publication;
use crate::graph_object_store::corrupt_sealed_graph_object_for_test;
#[cfg(windows)]
use crate::graph_object_store::gc_graph_objects;
use crate::graph_object_store::graph_object_path;
use crate::graph_object_store::hex_digest;
use crate::graph_object_store::open_graph_object_by_digest;
use crate::graph_object_store::read_graph_object;
use crate::graph_object_store::tests::assert_injected_error;
use crate::graph_object_store::tests::inject_returned_error;
use crate::graph_object_store::verify_file;
use crate::graph_object_store::verify_graph_object;

#[test]
fn allocation_observed_concurrent_cas_winner_keeps_real_temporary_peak() {
    let root = tempfile::tempdir().unwrap();
    let operation = crate::StorageAllocationOperation::default();
    let winner_operation = operation.clone();
    let winner_root = root.path().to_path_buf();
    let payload = vec![8_u8; 16384];
    let winner_payload = payload.clone();
    BEFORE_OBJECT_LINK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let mut winner = begin_graph_object_publication(&winner_root).unwrap();
            winner.set_allocation_operation(Some(winner_operation));
            install_graph_object_bytes_with_lease(&winner, &winner_payload).unwrap();
        }));
    });
    let mut loser = begin_graph_object_publication(root.path()).unwrap();
    loser.set_allocation_operation(Some(operation.clone()));
    let (digest, evidence) = install_graph_object_bytes_with_lease(&loser, &payload).unwrap();
    assert!(evidence.reused_existing);
    assert!(evidence.attempted_install);
    let final_file = File::open(graph_object_path(root.path(), &digest).unwrap()).unwrap();
    let allocated = graphforge_filesystem::file_space_usage(&final_file)
        .unwrap()
        .allocated_bytes;
    assert!(allocated > 0);
    assert_eq!(operation.totals().unwrap(), (allocated, 2 * allocated));
    assert_eq!(
        fs::read_dir(root.path().join(GRAPH_OBJECTS_DIR).join(TEMP_DIR))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn allocation_observed_cas_install_reuse_and_returned_errors_match_real_union() {
    for boundary in [
        None,
        Some("install:temp-sealed"),
        Some("install:final-linked"),
        Some("install:temp-unlinked"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let operation = crate::StorageAllocationOperation::default();
        let mut lease = begin_graph_object_publication(root.path()).unwrap();
        lease.set_allocation_operation(Some(operation.clone()));
        let payload = vec![9_u8; 16384];
        inject_returned_error(boundary);
        let result = install_graph_object_bytes_with_lease(&lease, &payload);
        inject_returned_error(None);
        assert_eq!(result.is_err(), boundary.is_some());
        let mut identities = BTreeMap::new();
        for directory in [
            root.path().join(GRAPH_OBJECTS_DIR).join(TEMP_DIR),
            root.path().join(GRAPH_OBJECTS_DIR).join(SHA256_DIR),
        ] {
            // Only the bounded test fixture is walked after the install returned.
            let mut pending = vec![directory.to_path_buf()];
            while let Some(path) = pending.pop() {
                for entry in fs::read_dir(path).unwrap() {
                    let entry = entry.unwrap();
                    if entry.file_type().unwrap().is_dir() {
                        pending.push(entry.path());
                        continue;
                    }
                    let file = File::open(entry.path()).unwrap();
                    let id = graphforge_filesystem::file_identity(&file).unwrap();
                    identities.insert(
                        (id.volume_serial, id.file_id),
                        graphforge_filesystem::file_space_usage(&file)
                            .unwrap()
                            .allocated_bytes,
                    );
                }
            }
        }
        let actual: u64 = identities.values().sum();
        assert!(actual > 0);
        assert_eq!(operation.totals().unwrap(), (actual, actual));
        if boundary.is_none() {
            let before = operation.snapshot().unwrap();
            let (_, reused) = install_graph_object_bytes_with_lease(&lease, &payload).unwrap();
            assert!(reused.reused_existing);
            assert_eq!(operation.snapshot().unwrap(), before);
        }
    }
}

#[test]
fn install_crash_boundaries_leave_only_valid_or_recoverable_unreferenced_state() {
    let boundaries = [
        "install:temp-sealed",
        "install:final-linked",
        "install:bucket-synced",
        "install:temp-unlinked",
        "append:before-manifest-reference",
    ];
    for boundary in boundaries {
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let relative_path = PathBuf::from("boundary.parquet");
        let payload = vec![0x5a_u8; 4096];
        fs::write(workspace.path().join(&relative_path), &payload).unwrap();
        let digest = hex_digest(Sha256::digest(&payload).into());
        let sealed = [AuthenticatedGraphFile {
            relative_path,
            byte_length: payload.len() as u64,
            content_sha256: digest.clone(),
        }];
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let mut failed_state = GraphManifestState::empty();

        inject_returned_error(Some(boundary));
        let error = append_authenticated_graph_files_v2(
            &lease,
            workspace.path(),
            &mut failed_state,
            &sealed,
            &[],
        )
        .unwrap_err();
        inject_returned_error(None);
        assert_injected_error(error, boundary);
        assert!(
            failed_state.root().is_none(),
            "{boundary} exposed a manifest reference"
        );

        let object = graph_object_path(container.path(), &digest).unwrap();
        let recoverable_temporary_exists = container
            .path()
            .join(GRAPH_OBJECTS_DIR)
            .join(TEMP_DIR)
            .read_dir()
            .unwrap()
            .next()
            .is_some();
        if object.exists() {
            verify_graph_object(container.path(), &digest, payload.len() as u64).unwrap();
        } else {
            assert!(
                recoverable_temporary_exists,
                "{boundary} lost both the valid object and recoverable temporary"
            );
        }

        let mut retry_state = GraphManifestState::empty();
        append_authenticated_graph_files_v2(
            &lease,
            workspace.path(),
            &mut retry_state,
            &sealed,
            &[],
        )
        .unwrap();
        assert!(retry_state.root().is_some());
        verify_graph_object(container.path(), &digest, payload.len() as u64).unwrap();
    }
    inject_returned_error(None);
}

#[cfg(unix)]
#[test]
fn reuse_seals_the_authenticated_descriptor_not_a_replaced_path() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let payload = b"descriptor-bound payload";
    let (digest, _) = install_graph_object_bytes(root.path(), payload).unwrap();
    let path = graph_object_path(root.path(), &digest).unwrap();
    let displaced = path.with_extension("displaced");
    let descriptor = File::open(&path).unwrap();

    fs::rename(&path, &displaced).unwrap();
    fs::set_permissions(&displaced, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&path, b"hostile replacement").unwrap();
    verify_and_seal_graph_object(
        &descriptor,
        &digest,
        payload.len() as u64,
        &displaced,
        root.path(),
    )
    .unwrap();

    assert!(fs::metadata(&displaced).unwrap().permissions().readonly());
    assert!(!fs::metadata(&path).unwrap().permissions().readonly());
    assert!(open_graph_object_by_digest(root.path(), &digest, payload.len() as u64).is_err());
}

#[cfg(unix)]
#[test]
fn reuse_seals_same_inode_before_digest_authentication() {
    let root = tempfile::tempdir().unwrap();
    let payload = b"seal-before-authentication";
    let digest = hex_digest(Sha256::digest(payload).into());
    let path = root.path().join("candidate");
    fs::write(&path, payload).unwrap();
    let descriptor = File::open(&path).unwrap();

    seal_graph_object(&descriptor, &path, root.path()).unwrap();
    assert!(fs::OpenOptions::new().write(true).open(&path).is_err());
    verify_file(
        descriptor.try_clone().unwrap(),
        &digest,
        payload.len() as u64,
        root.path(),
    )
    .unwrap();
    assert!(descriptor.metadata().unwrap().permissions().readonly());
}

#[cfg(unix)]
#[test]
fn fresh_write_descriptor_can_be_sealed_without_losing_named_identity() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("fresh-object");
    let mut descriptor = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    descriptor.write_all(b"fresh payload").unwrap();
    descriptor.sync_all().unwrap();
    let identity = graphforge_filesystem::file_identity(&descriptor).unwrap();

    seal_graph_object(&descriptor, &path, root.path()).unwrap();

    assert_eq!(
        graphforge_filesystem::path_identity(&path).unwrap(),
        identity
    );
    assert!(descriptor.metadata().unwrap().permissions().readonly());
}

#[cfg(windows)]
#[test]
fn fresh_cas_transition_excludes_writers_before_and_after_seal() {
    use std::os::windows::fs::OpenOptionsExt as _;
    use std::process::Command;

    const HELPER: &str = "GRAPHFORGE_CAS_WRITER_EXCLUSION_HELPER";
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    if let Some(path) = std::env::var_os(HELPER) {
        let writer = OpenOptions::new()
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .open(path);
        assert!(writer.is_err(), "child unexpectedly acquired CAS writer");
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let name = std::ffi::OsStr::new("temporary");
    let path = root.path().join(name);
    let payload = b"sealed only after exclusive read admission";
    let mut writer = directory.create_cas_child_file(name).unwrap();
    writer.write_all(payload).unwrap();

    let assert_child_writer_denied = || {
        let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "graph_object_store::installation::tests::fresh_cas_transition_excludes_writers_before_and_after_seal",
                    "--nocapture",
                ])
                .env(HELPER, &path)
                .status()
                .unwrap();
        assert!(status.success());
    };
    assert_child_writer_denied();
    let sealed = directory.seal_cas_child_file(name, writer).unwrap();
    assert_child_writer_denied();
    assert_eq!(
        graphforge_filesystem::file_identity(&sealed.into_file()).unwrap(),
        graphforge_filesystem::path_identity(&path).unwrap()
    );
}

#[cfg(windows)]
#[test]
fn ordinary_owner_can_cleanup_fresh_cas_temp_and_gc_sealed_object() {
    let root = tempfile::tempdir().unwrap();
    let payload = b"owner-only Windows CAS cleanup";

    let (digest, evidence) = install_graph_object_bytes(root.path(), payload).unwrap();
    assert!(!evidence.reused_existing);
    let object = graph_object_path(root.path(), &digest).unwrap();
    assert!(object.exists());

    // Installation must remove the sealed temporary hard link using the
    // same owner capability available to a restricted, non-admin process.
    let lease = begin_graph_object_publication(root.path()).unwrap();
    assert!(lease.cas.tmp.child_names().unwrap().is_empty());
    drop(lease);

    // GC uses that same narrow capability on the canonical sealed name.
    let reclaimed =
        gc_graph_objects(root.path(), &[], crate::GraphManifestLimits::default()).unwrap();
    assert_eq!(reclaimed.objects_removed, 1);
    assert_eq!(reclaimed.bytes_removed, payload.len() as u64);
    assert!(!object.exists());
}

#[cfg(windows)]
#[test]
fn reuse_rejects_planted_unsealed_object_without_mutating_it() {
    let root = tempfile::tempdir().unwrap();
    let payload = b"planted writable object";
    let digest = hex_digest(Sha256::digest(payload).into());
    let lease = begin_graph_object_publication(root.path()).unwrap();
    let bucket = lease.cas.digest_bucket(&digest, true).unwrap();
    let name = std::ffi::OsStr::new(&digest[2..]);
    let path = graph_object_path(root.path(), &digest).unwrap();
    fs::write(&path, payload).unwrap();
    assert!(!fs::metadata(&path).unwrap().permissions().readonly());
    assert!(bucket.open_cas_child_file(name).is_err());
    drop(lease);

    assert!(install_graph_object_bytes(root.path(), payload).is_err());
    assert_eq!(fs::read(&path).unwrap(), payload);
    assert!(!fs::metadata(&path).unwrap().permissions().readonly());
}

#[cfg(windows)]
#[test]
fn authenticated_released_readonly_object_is_adopted_canonically() {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    let root = tempfile::tempdir().unwrap();
    let payload = b"released readonly inherited-dacl object";
    let digest = hex_digest(Sha256::digest(payload).into());
    let lease = begin_graph_object_publication(root.path()).unwrap();
    let _bucket = lease.cas.digest_bucket(&digest, true).unwrap();
    let path = graph_object_path(root.path(), &digest).unwrap();
    fs::write(&path, payload).unwrap();
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&path, permissions).unwrap();
    drop(lease);

    let (_, evidence) = install_graph_object_bytes(root.path(), payload).unwrap();
    assert!(evidence.reused_existing);
    let writer = OpenOptions::new()
        .write(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(&path);
    assert!(writer.is_err(), "canonical adoption retained write access");
}

#[test]
fn concurrent_winner_reuse_retains_the_losing_install_work() {
    for file_backed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        let payload = b"concurrent payload";
        let digest = hex_digest(Sha256::digest(payload).into());
        let winner_root = root.path().to_path_buf();
        BEFORE_OBJECT_LINK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let (_, winner) = install_graph_object_bytes(&winner_root, payload).unwrap();
                assert!(!winner.reused_existing);
                assert!(winner.attempted_install);
                assert_eq!(winner.fsync_calls, 3);
            }));
        });
        let loser = if file_backed {
            let path = source.path().join("payload");
            fs::write(&path, payload).unwrap();
            install_graph_object_file(root.path(), &path, &digest, payload.len() as u64).unwrap()
        } else {
            install_graph_object_bytes(root.path(), payload).unwrap().1
        };
        assert!(loser.reused_existing);
        assert!(loser.attempted_install);
        assert_eq!(loser.bytes_installed, 0);
        assert_eq!(loser.write_bytes, payload.len() as u64);
        assert_eq!(loser.write_calls, 1);
        assert_eq!(loser.file_fsync_calls, 1);
        assert_eq!(loser.directory_fsync_calls, 2);
        assert_eq!(loser.fsync_calls, 3);
        // Windows authenticates the protected sealed handle after closing
        // the writable handle, in addition to a file source-copy pass.
        let read_passes = 2 + u64::from(cfg!(windows) && file_backed);
        assert_eq!(loser.read_calls, read_passes);
        assert_eq!(loser.bytes_hashed, read_passes * payload.len() as u64);
        assert_eq!(
            read_graph_object(root.path(), &digest, payload.len() as u64).unwrap(),
            payload
        );
        let (_, early_reuse) = install_graph_object_bytes(root.path(), payload).unwrap();
        assert!(early_reuse.reused_existing);
        assert!(!early_reuse.attempted_install);
        assert_eq!(early_reuse.write_bytes, 0);
        assert_eq!(early_reuse.fsync_calls, 0);
        assert!(
            root.path()
                .join(GRAPH_OBJECTS_DIR)
                .join(TEMP_DIR)
                .read_dir()
                .unwrap()
                .next()
                .is_none()
        );
    }
}

#[test]
fn file_install_receipts_count_actual_cache_window_synchronizations() {
    let window = graphforge_filesystem::cache_release_window_for_streams(2)
        .unwrap()
        .get();
    assert_eq!(window, 32 * 1024 * 1024);
    for bytes in [window - 1, window, window + 1] {
        let root = tempfile::tempdir().unwrap();
        let source_root = tempfile::tempdir().unwrap();
        let source = source_root.path().join("source");
        let payload = vec![0x5a_u8; usize::try_from(bytes).unwrap()];
        fs::write(&source, &payload).unwrap();
        let digest = hex_digest(Sha256::digest(&payload).into());

        let installed = install_graph_object_file(root.path(), &source, &digest, bytes).unwrap();
        let rollovers = u64::from(cfg!(target_os = "linux") && bytes > window);
        assert_eq!(
            installed.fsync_calls,
            3 + rollovers,
            "payload bytes {bytes}"
        );
        assert!(!installed.reused_existing);
        assert_eq!(installed.file_fsync_calls, 1 + rollovers);
        assert_eq!(installed.directory_fsync_calls, 2);
        assert_eq!(installed.bytes_installed, bytes);
        assert_eq!(
            read_graph_object(root.path(), &digest, bytes).unwrap(),
            payload
        );

        let reused = install_graph_object_file(root.path(), &source, &digest, bytes).unwrap();
        assert!(reused.reused_existing);
        assert_eq!(reused.fsync_calls, 0);
        assert_eq!(reused.file_fsync_calls, 0);
        assert_eq!(reused.directory_fsync_calls, 0);
        assert_eq!(reused.write_bytes, 0);
        assert!(
            root.path()
                .join(GRAPH_OBJECTS_DIR)
                .join(TEMP_DIR)
                .read_dir()
                .unwrap()
                .next()
                .is_none()
        );
    }
}

#[test]
fn installs_once_reuses_exact_object_and_rejects_tampering() {
    let root = tempfile::tempdir().unwrap();
    let (digest, first) = install_graph_object_bytes(root.path(), b"payload").unwrap();
    assert_eq!(first.bytes_hashed, 7);
    assert_eq!(first.bytes_installed, 7);
    assert!(!first.reused_existing);
    assert_eq!(first.read_calls, 1);
    assert_eq!(first.write_bytes, 7);
    assert_eq!(first.write_calls, 1);
    assert_eq!(first.fsync_calls, 3);
    assert!(
        root.path()
            .join(GRAPH_OBJECTS_DIR)
            .join(TEMP_DIR)
            .read_dir()
            .unwrap()
            .next()
            .is_none(),
        "fresh installation retained its sealed temporary alias"
    );
    let (_, second) = install_graph_object_bytes(root.path(), b"payload").unwrap();
    assert!(second.reused_existing);
    assert_eq!(second.read_calls, 1);
    assert_eq!(second.write_bytes, 0);
    assert_eq!(second.write_calls, 0);
    assert_eq!(second.fsync_calls, 0);
    assert_eq!(
        read_graph_object(root.path(), &digest, 7).unwrap(),
        b"payload"
    );

    corrupt_sealed_graph_object_for_test(
        &graph_object_path(root.path(), &digest).unwrap(),
        b"corrupt",
    );
    assert!(install_graph_object_bytes(root.path(), b"payload").is_err());
}

#[cfg(windows)]
#[test]
fn windows_fresh_sealed_install_removes_its_temporary_alias() {
    let root = tempfile::tempdir().unwrap();
    install_graph_object_bytes(root.path(), b"windows sealed payload").unwrap();

    assert!(
        root.path()
            .join(GRAPH_OBJECTS_DIR)
            .join(TEMP_DIR)
            .read_dir()
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn rejects_unsafe_digest_and_source_mismatch() {
    let root = tempfile::tempdir().unwrap();
    assert!(graph_object_path(root.path(), "../escape").is_err());
    let source = root.path().join("source");
    fs::write(&source, b"payload").unwrap();
    assert!(install_graph_object_file(root.path(), &source, &"0".repeat(64), 7).is_err());
    assert!(
        install_graph_object_file(
            root.path(),
            &source,
            &hex_digest(Sha256::digest(b"payload").into()),
            8
        )
        .is_err()
    );
}

#[test]
fn cas_copy_isolated_from_preexisting_writable_source_descriptor() {
    let container = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let relative_path = PathBuf::from("owned.parquet");
    let payload = vec![9_u8; 4096];
    let workspace_path = workspace.path().join(&relative_path);
    fs::write(&workspace_path, &payload).unwrap();
    let mut hostile = std::fs::OpenOptions::new()
        .write(true)
        .open(&workspace_path)
        .unwrap();
    let sealed = [AuthenticatedGraphFile {
        relative_path,
        byte_length: payload.len() as u64,
        content_sha256: hex_digest(Sha256::digest(&payload).into()),
    }];
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let mut state = GraphManifestState::empty();
    append_authenticated_graph_files_v2(&lease, workspace.path(), &mut state, &sealed, &[])
        .unwrap();
    assert!(workspace_path.exists());
    hostile.rewind().unwrap();
    hostile.write_all(&vec![0x44; payload.len()]).unwrap();
    hostile.sync_all().unwrap();
    verify_graph_object(
        container.path(),
        &sealed[0].content_sha256,
        payload.len() as u64,
    )
    .unwrap();
}
