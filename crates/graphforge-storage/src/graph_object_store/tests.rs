use super::*;
use crate::graph_object_store::ACTIVE_DIR;
use crate::graph_object_store::BTreeSet;
use crate::graph_object_store::GRAPH_OBJECTS_DIR;
use crate::graph_object_store::GfError;
#[cfg(unix)]
use crate::graph_object_store::GraphFilesInventory;
use crate::graph_object_store::LIFECYCLE_LOCK;
use crate::graph_object_store::ProjectErrorCode;
use crate::graph_object_store::RETURNED_ERROR_BOUNDARY;
use crate::graph_object_store::ReadOnlyCasRoot;
use crate::graph_object_store::SHA256_DIR;
use crate::graph_object_store::Sha256;
#[cfg(unix)]
use crate::graph_object_store::TEMP_DIR;
use crate::graph_object_store::begin_graph_object_gc;
use crate::graph_object_store::begin_graph_object_publication;
use crate::graph_object_store::gc_graph_objects;
use crate::graph_object_store::graph_object_path;
use crate::graph_object_store::graph_object_publication_is_live;
use crate::graph_object_store::hex_digest;
use crate::graph_object_store::install_graph_object_bytes;
#[cfg(unix)]
use crate::graph_object_store::materialize_graph_objects;
use crate::graph_object_store::open_graph_object_by_digest;
use crate::graph_object_store::read_graph_object;
use crate::graph_object_store::read_graph_object_by_digest;
use crate::graph_object_store::try_begin_graph_object_gc;
use crate::graph_object_store::verify_graph_object;

pub(super) fn assert_injected_error(error: GfError, boundary: &str) {
    match error {
        GfError::Storage(message) => assert_eq!(
            message,
            format!("injected graph object returned error at {boundary}")
        ),
        other => panic!("unexpected injected error: {other}"),
    }
}

pub(super) fn inject_returned_error(boundary: Option<&str>) {
    RETURNED_ERROR_BOUNDARY.with(|current| {
        *current.borrow_mut() = boundary.map(str::to_owned);
    });
}

/// Forces the same-filesystem CAS install fast path to behave as if the
/// source were on a different filesystem, so tests can exercise the
/// byte-copy fallback deterministically.
pub(super) fn force_move_install_ineligible(ineligible: bool) {
    crate::graph_object_store::FORCE_MOVE_INSTALL_INELIGIBLE.with(|current| {
        current.set(ineligible);
    });
}

#[test]
fn returned_errors_release_every_cas_lock_and_pending_lease_boundary() {
    let root = tempfile::tempdir().unwrap();
    drop(begin_graph_object_publication(root.path()).unwrap());

    #[cfg(unix)]
    let reading_boundaries = [
        "reading:objects-lock",
        "reading:lifecycle-lock",
        "reading:revalidate",
    ]
    .as_slice();
    #[cfg(not(unix))]
    let reading_boundaries = ["reading:lifecycle-lock", "reading:revalidate"].as_slice();
    for &boundary in reading_boundaries {
        inject_returned_error(Some(boundary));
        let error = ReadOnlyCasRoot::open(root.path()).err().unwrap();
        assert_injected_error(error, boundary);
        inject_returned_error(None);
        drop(try_begin_graph_object_gc(root.path()).unwrap().unwrap());
    }

    #[cfg(unix)]
    let gc_boundaries = ["gc:objects-lock", "gc:lifecycle-lock", "gc:revalidate"].as_slice();
    #[cfg(not(unix))]
    let gc_boundaries = ["gc:lifecycle-lock", "gc:revalidate"].as_slice();
    for &boundary in gc_boundaries {
        inject_returned_error(Some(boundary));
        let error = begin_graph_object_gc(root.path()).err().unwrap();
        assert_injected_error(error, boundary);
        inject_returned_error(None);
        drop(try_begin_graph_object_gc(root.path()).unwrap().unwrap());
    }

    #[cfg(unix)]
    let try_gc_boundaries = [
        "try-gc:objects-lock",
        "try-gc:lifecycle-lock",
        "try-gc:revalidate",
    ]
    .as_slice();
    #[cfg(not(unix))]
    let try_gc_boundaries = ["try-gc:lifecycle-lock", "try-gc:revalidate"].as_slice();
    for &boundary in try_gc_boundaries {
        inject_returned_error(Some(boundary));
        let error = try_begin_graph_object_gc(root.path()).err().unwrap();
        assert_injected_error(error, boundary);
        inject_returned_error(None);
        drop(try_begin_graph_object_gc(root.path()).unwrap().unwrap());
    }

    #[cfg(unix)]
    let publication_boundaries = [
        "publication:objects-lock",
        "publication:lifecycle-lock",
        "publication:revalidate",
        "publication:lease-create",
        "publication:lease-identity",
        "publication:lease-lock",
        "publication:lease-sync",
        "publication:active-sync",
    ]
    .as_slice();
    #[cfg(not(unix))]
    let publication_boundaries = [
        "publication:lifecycle-lock",
        "publication:revalidate",
        "publication:lease-create",
        "publication:lease-identity",
        "publication:lease-lock",
        "publication:lease-sync",
        "publication:active-sync",
    ]
    .as_slice();
    for &boundary in publication_boundaries {
        inject_returned_error(Some(boundary));
        let error = begin_graph_object_publication(root.path()).err().unwrap();
        assert_injected_error(error, boundary);
        inject_returned_error(None);
        let residue_count = fs::read_dir(root.path().join(GRAPH_OBJECTS_DIR).join(ACTIVE_DIR))
            .unwrap()
            .count();
        assert_eq!(
            residue_count,
            usize::from(boundary == "publication:lease-create")
        );
        assert!(!graph_object_publication_is_live(root.path()).unwrap());
        assert_eq!(
            fs::read_dir(root.path().join(GRAPH_OBJECTS_DIR).join(ACTIVE_DIR))
                .unwrap()
                .count(),
            0
        );
        drop(begin_graph_object_publication(root.path()).unwrap());
        drop(try_begin_graph_object_gc(root.path()).unwrap().unwrap());
        drop(ReadOnlyCasRoot::open(root.path()).unwrap());
    }
}

#[test]
fn pure_reads_require_only_existing_digest_namespace_and_never_create() {
    let root = tempfile::tempdir().unwrap();
    let payload = b"read-only payload";
    let digest = hex_digest(Sha256::digest(payload).into());
    let objects = root.path().join(GRAPH_OBJECTS_DIR);
    let sha256 = objects.join(SHA256_DIR);
    let bucket = sha256.join(&digest[..2]);
    fs::create_dir_all(&bucket).unwrap();
    fs::write(bucket.join(&digest[2..]), payload).unwrap();
    fs::write(objects.join(LIFECYCLE_LOCK), b"").unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(bucket.join(&digest[2..]), fs::Permissions::from_mode(0o400)).unwrap();
        fs::set_permissions(
            objects.join(LIFECYCLE_LOCK),
            fs::Permissions::from_mode(0o400),
        )
        .unwrap();
        fs::set_permissions(&bucket, fs::Permissions::from_mode(0o500)).unwrap();
        fs::set_permissions(&sha256, fs::Permissions::from_mode(0o500)).unwrap();
        fs::set_permissions(&objects, fs::Permissions::from_mode(0o500)).unwrap();
    }

    let namespace_before = fs::read_dir(&objects)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();
    let digest_namespace_before = fs::read_dir(&sha256)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();
    let bucket_namespace_before = fs::read_dir(&bucket)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        read_graph_object(root.path(), &digest, payload.len() as u64).unwrap(),
        payload
    );
    verify_graph_object(root.path(), &digest, payload.len() as u64).unwrap();
    assert_eq!(
        read_graph_object_by_digest(root.path(), &digest, 1024).unwrap(),
        payload
    );
    let namespace_after = fs::read_dir(&objects)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();
    assert_eq!(namespace_after, namespace_before);
    assert_eq!(
        namespace_after,
        BTreeSet::from([SHA256_DIR.into(), LIFECYCLE_LOCK.into()])
    );
    assert_eq!(
        fs::read_dir(&sha256)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>(),
        digest_namespace_before
    );
    assert_eq!(
        fs::read_dir(&bucket)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>(),
        bucket_namespace_before
    );
    assert_eq!(fs::read(bucket.join(&digest[2..])).unwrap(), payload);

    #[cfg(windows)]
    {
        let lifecycle = objects.join(LIFECYCLE_LOCK);
        let object = bucket.join(&digest[2..]);
        let mut permissions = fs::metadata(&lifecycle).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&lifecycle, permissions).unwrap();
        let mut permissions = fs::metadata(&object).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&object, permissions).unwrap();
        verify_graph_object(root.path(), &digest, payload.len() as u64).unwrap();
        let mut permissions = fs::metadata(&lifecycle).unwrap().permissions();
        permissions.set_readonly(false);
        fs::set_permissions(&lifecycle, permissions).unwrap();
        let mut permissions = fs::metadata(&object).unwrap().permissions();
        permissions.set_readonly(false);
        fs::set_permissions(&object, permissions).unwrap();
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let target_owner = tempfile::tempdir().unwrap();
        let inventory = GraphFilesInventory {
            format: "graphforge-graph-files".into(),
            format_version: 1,
            files: vec![crate::GraphFileEntry {
                relative_path: "payload.bin".into(),
                byte_length: payload.len() as u64,
                content_sha256: digest.clone(),
                role: crate::GraphFileRole::Other,
            }],
            file_count: 1,
            total_byte_length: payload.len() as u64,
        };
        assert!(
            materialize_graph_objects(
                root.path(),
                &inventory,
                &target_owner.path().join("readonly-target")
            )
            .is_err()
        );
        assert_eq!(
            fs::read_dir(&objects)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<BTreeSet<_>>(),
            namespace_before
        );
        fs::set_permissions(&objects, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(
            objects.join(LIFECYCLE_LOCK),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        fs::set_permissions(&sha256, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&bucket, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(bucket.join(&digest[2..]), fs::Permissions::from_mode(0o600)).unwrap();
        materialize_graph_objects(
            root.path(),
            &inventory,
            &target_owner.path().join("writable-target"),
        )
        .unwrap();
        assert!(objects.join(TEMP_DIR).is_dir());
        assert!(objects.join(ACTIVE_DIR).is_dir());
        assert!(objects.join(LIFECYCLE_LOCK).is_file());
    }

    let missing = tempfile::tempdir().unwrap();
    assert!(read_graph_object(missing.path(), &digest, payload.len() as u64).is_err());
    assert!(!missing.path().join(GRAPH_OBJECTS_DIR).exists());
}

#[test]
fn read_only_guard_pins_cas_against_gc() {
    let root = tempfile::tempdir().unwrap();
    let lease = begin_graph_object_publication(root.path()).unwrap();
    drop(lease);
    let reader = ReadOnlyCasRoot::open(root.path()).unwrap();
    assert!(matches!(try_begin_graph_object_gc(root.path()), Ok(None)));
    drop(reader);
    assert!(matches!(
        try_begin_graph_object_gc(root.path()),
        Ok(Some(_))
    ));
}

#[test]
fn read_only_lifecycle_rejects_multiple_links() {
    let root = tempfile::tempdir().unwrap();
    let objects = root.path().join(GRAPH_OBJECTS_DIR);
    fs::create_dir_all(objects.join(SHA256_DIR)).unwrap();
    let outside = root.path().join("outside");
    fs::write(&outside, b"").unwrap();
    fs::hard_link(&outside, objects.join(LIFECYCLE_LOCK)).unwrap();
    assert!(ReadOnlyCasRoot::open(root.path()).is_err());
}

#[cfg(unix)]
#[test]
fn read_only_lifecycle_rejects_links_fifos_and_sockets_without_blocking() {
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener;
    use std::process::Command;

    let prepare = || {
        let root = tempfile::tempdir().unwrap();
        let objects = root.path().join(GRAPH_OBJECTS_DIR);
        fs::create_dir_all(objects.join(SHA256_DIR)).unwrap();
        (root, objects)
    };

    const FIFO_HELPER: &str = "GRAPHFORGE_READ_ONLY_CAS_FIFO_HELPER";
    if std::env::var_os(FIFO_HELPER).is_some() {
        let (root, objects) = prepare();
        assert!(
            Command::new("mkfifo")
                .arg(objects.join(LIFECYCLE_LOCK))
                .status()
                .unwrap()
                .success()
        );
        assert!(ReadOnlyCasRoot::open(root.path()).is_err());
        return;
    }

    let (root, objects) = prepare();
    let outside = root.path().join("outside");
    fs::write(&outside, b"").unwrap();
    symlink(&outside, objects.join(LIFECYCLE_LOCK)).unwrap();
    assert!(ReadOnlyCasRoot::open(root.path()).is_err());

    let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "graph_object_store::tests::read_only_lifecycle_rejects_links_fifos_and_sockets_without_blocking",
                "--nocapture",
            ])
            .env(FIFO_HELPER, "1")
            .spawn()
            .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("read-only lifecycle FIFO open blocked");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    let (root, objects) = prepare();
    let _socket = UnixListener::bind(objects.join(LIFECYCLE_LOCK)).unwrap();
    assert!(ReadOnlyCasRoot::open(root.path()).is_err());
}

#[cfg(unix)]
#[test]
fn lifecycle_lock_rejects_symlink_and_pathname_substitution() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let object_root = root.path().join(GRAPH_OBJECTS_DIR);
    fs::create_dir_all(&object_root).unwrap();
    let outside = root.path().join("outside.lock");
    fs::write(&outside, b"").unwrap();
    symlink(&outside, object_root.join(LIFECYCLE_LOCK)).unwrap();
    assert!(begin_graph_object_publication(root.path()).is_err());

    fs::remove_file(object_root.join(LIFECYCLE_LOCK)).unwrap();
    let lease = begin_graph_object_publication(root.path()).unwrap();
    let displaced = object_root.join("displaced.lock");
    fs::rename(object_root.join(LIFECYCLE_LOCK), &displaced).unwrap();
    fs::write(object_root.join(LIFECYCLE_LOCK), b"").unwrap();

    assert!(lease.revalidate_for_publish().is_err());
    assert!(matches!(try_begin_graph_object_gc(root.path()), Ok(None)));

    let other_root = tempfile::tempdir().unwrap();
    let lease = begin_graph_object_publication(other_root.path()).unwrap();
    let object_root = other_root.path().join(GRAPH_OBJECTS_DIR);
    fs::rename(&object_root, other_root.path().join("displaced-objects")).unwrap();
    fs::create_dir(&object_root).unwrap();
    assert!(lease.revalidate_for_publish().is_err());
    assert!(matches!(
        try_begin_graph_object_gc(other_root.path()),
        Ok(Some(_))
    ));
    assert!(lease.revalidate_for_publish().is_err());
}

#[cfg(windows)]
#[test]
fn windows_lifecycle_handles_deny_coordination_path_replacement() {
    let root = tempfile::tempdir().unwrap();
    let lease = begin_graph_object_publication(root.path()).unwrap();
    let lifecycle = root.path().join(GRAPH_OBJECTS_DIR).join(LIFECYCLE_LOCK);
    let replacement = lifecycle.with_extension("replacement");
    assert!(fs::rename(&lifecycle, replacement).is_err());
    lease.revalidate_for_publish().unwrap();
}

#[test]
fn publication_and_gc_lifecycles_are_mutually_exclusive() {
    use std::sync::mpsc::{self, TryRecvError};
    use std::time::Duration;

    let root = tempfile::tempdir().unwrap();
    let publication = begin_graph_object_publication(root.path()).unwrap();
    assert!(matches!(
        gc_graph_objects(root.path(), &[], crate::GraphManifestLimits::default()),
        Err(GfError::Project {
            code: ProjectErrorCode::WriterBusy,
            ..
        })
    ));
    let path = root.path().to_path_buf();
    let (gc_tx, gc_rx) = mpsc::channel();
    let gc_thread = std::thread::spawn(move || {
        let guard = begin_graph_object_gc(&path).unwrap();
        gc_tx.send(guard).unwrap();
    });
    assert!(matches!(gc_rx.try_recv(), Err(TryRecvError::Empty)));
    drop(publication);
    let gc = gc_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    gc_thread.join().unwrap();

    let path = root.path().to_path_buf();
    let (publish_tx, publish_rx) = mpsc::channel();
    let publish_thread = std::thread::spawn(move || {
        let lease = begin_graph_object_publication(&path).unwrap();
        publish_tx.send(lease).unwrap();
    });
    assert!(matches!(publish_rx.try_recv(), Err(TryRecvError::Empty)));
    drop(gc);
    let publication = publish_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    publish_thread.join().unwrap();
    drop(publication);
}

#[test]
fn authenticated_reader_keeps_cas_sealed_through_descriptor_consumption() {
    let root = tempfile::tempdir().unwrap();
    let payload = b"authenticated payload";
    let (digest, _) = install_graph_object_bytes(root.path(), payload).unwrap();
    let mut reader =
        open_graph_object_by_digest(root.path(), &digest, payload.len() as u64).unwrap();
    assert_eq!(reader.len(), payload.len() as u64);
    let path = graph_object_path(root.path(), &digest).unwrap();

    let mutation = fs::OpenOptions::new().write(true).open(&path);
    assert!(
        mutation.is_err(),
        "sealed CAS object accepted in-place mutation"
    );

    let mut decoded = Vec::new();
    reader.read_to_end(&mut decoded).unwrap();
    assert_eq!(decoded, payload);
}

#[test]
fn publication_lease_blocks_sweep_probe_and_stale_residue_is_reclaimed() {
    let root = tempfile::tempdir().unwrap();
    let lease = begin_graph_object_publication(root.path()).unwrap();
    assert!(graph_object_publication_is_live(root.path()).unwrap());
    let active = root.path().join(GRAPH_OBJECTS_DIR).join(ACTIVE_DIR);
    drop(lease);
    let stale = active.join("00000000-0000-0000-0000-000000000000.lock");
    fs::write(&stale, []).unwrap();
    assert!(!graph_object_publication_is_live(root.path()).unwrap());
    assert!(!stale.exists());
}

#[test]
fn publication_lease_probe_fails_closed_on_noncanonical_residue() {
    let root = tempfile::tempdir().unwrap();
    let active = root.path().join(GRAPH_OBJECTS_DIR).join(ACTIVE_DIR);
    fs::create_dir_all(&active).unwrap();
    let hostile = active.join("caller-owned");
    fs::write(&hostile, b"preserve").unwrap();
    assert!(graph_object_publication_is_live(root.path()).is_err());
    assert_eq!(fs::read(&hostile).unwrap(), b"preserve");
}
