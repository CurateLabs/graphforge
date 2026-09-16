use super::*;
use std::io::Write as _;

#[test]
fn retained_directory_adoption_checks_handle_and_named_identity() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let first_identity = path_identity(first.path()).unwrap();
    let wrong_handle = open_directory_handle(second.path()).unwrap();

    // The named directory is genuine and matches the claimed identity;
    // only the retained handle is wrong. Named-only validation would pass.
    let failure = StableDirectory::from_retained_handle(
        first.path().to_path_buf(),
        wrong_handle,
        first_identity,
    )
    .unwrap_err();
    assert_eq!(failure.stage(), DirectoryValidationStage::IdentityChanged);
    assert_eq!(failure.into_io_error().kind(), io::ErrorKind::Other);

    let retained = StableDirectory::from_retained_handle(
        first.path().to_path_buf(),
        open_directory_handle(first.path()).unwrap(),
        first_identity,
    )
    .unwrap();
    retained.revalidate_named_detailed().unwrap();
    retained.try_clone().unwrap().revalidate_named().unwrap();
    assert_eq!(file_identity(retained.as_file()).unwrap(), first_identity);
    assert_eq!(
        file_identity(&retained.into_handle().into_file()).unwrap(),
        first_identity
    );
}

#[test]
fn retained_directory_adoption_rejects_regular_handle_and_missing_name() {
    let root = tempfile::tempdir().unwrap();
    let identity = path_identity(root.path()).unwrap();
    let regular = root.path().join("regular");
    std::fs::write(&regular, b"unchanged").unwrap();
    let failure = StableDirectory::from_retained_handle(
        root.path().to_path_buf(),
        OpenedDirectoryHandle {
            file: File::open(&regular).unwrap(),
        },
        identity,
    )
    .unwrap_err();
    assert_eq!(failure.stage(), DirectoryValidationStage::IdentityChanged);
    assert_eq!(std::fs::read(regular).unwrap(), b"unchanged");

    let failure = StableDirectory::from_retained_handle(
        root.path().join("missing"),
        open_directory_handle(root.path()).unwrap(),
        identity,
    )
    .unwrap_err();
    assert_eq!(failure.stage(), DirectoryValidationStage::NamedMetadata);
    assert_eq!(failure.into_io_error().kind(), io::ErrorKind::NotFound);
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn mark_sparse(file: &File) {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    let mut returned = 0;
    // SAFETY: `file` retains a live file handle; this control code has no
    // input or output buffer, and `returned` remains live for the call.
    let succeeded = unsafe {
        DeviceIoControl(
            file.as_raw_handle(),
            FSCTL_SET_SPARSE,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            0,
            &raw mut returned,
            std::ptr::null_mut(),
        )
    };
    assert_ne!(succeeded, 0, "{}", io::Error::last_os_error());
}

#[cfg(unix)]
fn mark_sparse(_file: &File) {}

#[cfg(any(unix, windows))]
#[test]
fn retained_handle_reports_sparse_logical_and_allocated_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sparse.bin");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    mark_sparse(&file);
    file.set_len(64 * 1024 * 1024).unwrap();
    file.sync_all().unwrap();

    let usage = file_space_usage(&file).unwrap();
    assert_eq!(usage.logical_bytes, 64 * 1024 * 1024);
    assert!(
        usage.allocated_bytes < usage.logical_bytes,
        "sparse allocation must be physical, not a logical-length proxy: {usage:?}"
    );
}

#[cfg(any(unix, windows))]
#[test]
fn retained_hard_link_handles_share_identity_and_space_usage() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.bin");
    let alias_path = directory.path().join("alias.bin");
    let source = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&source_path)
        .unwrap();
    mark_sparse(&source);
    source.set_len(32 * 1024 * 1024).unwrap();
    source.sync_all().unwrap();
    std::fs::hard_link(&source_path, &alias_path).unwrap();
    let alias = File::open(&alias_path).unwrap();

    assert_eq!(
        file_identity(&source).unwrap(),
        file_identity(&alias).unwrap()
    );
    assert_eq!(
        file_space_usage(&source).unwrap(),
        file_space_usage(&alias).unwrap()
    );

    std::fs::remove_file(&source_path).unwrap();
    assert_eq!(
        file_identity(&source).unwrap(),
        file_identity(&alias).unwrap()
    );
    assert_eq!(
        file_space_usage(&source).unwrap(),
        file_space_usage(&alias).unwrap()
    );
}

#[cfg(unix)]
const FIFO_CHILD_ENV: &str = "GRAPHFORGE_FILESYSTEM_FIFO_CHILD";

#[cfg(unix)]
const FIFO_ROOT_ENV: &str = "GRAPHFORGE_FILESYSTEM_FIFO_ROOT";

pub(super) fn directory_handle(path: &Path) -> File {
    #[cfg(unix)]
    return File::open(path).unwrap();

    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        const FILE_SHARE_READ: u32 = 0x0000_0001;
        const FILE_SHARE_WRITE: u32 = 0x0000_0002;
        return std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)
            .unwrap();
    }
}

#[test]
fn concurrent_open_or_create_has_one_stable_named_identity() {
    let root = tempfile::tempdir().unwrap();
    let root = std::sync::Arc::new(root);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(9));
    let workers = (0..8)
        .map(|_| {
            let root = std::sync::Arc::clone(&root);
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                let directory = StableDirectory::open(root.path()).unwrap();
                barrier.wait();
                let file = directory
                    .open_or_create_child_file(OsStr::new("lifecycle.lock"))
                    .unwrap();
                file_identity(&file).unwrap()
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let identities = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert!(identities.windows(2).all(|pair| pair[0] == pair[1]));
    assert_eq!(
        file_link_count(&File::open(root.path().join("lifecycle.lock")).unwrap()).unwrap(),
        1
    );
}

#[cfg(unix)]
#[test]
fn peerless_fifo_child() {
    let Ok(operation) = std::env::var(FIFO_CHILD_ENV) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(FIFO_ROOT_ENV).expect("FIFO test root"));
    let stable = StableDirectory::open(&root).unwrap();
    let fifo_identity = path_identity(&root.join("fifo")).unwrap();
    let regular_identity = path_identity(&root.join("regular")).unwrap();
    let result = match operation.as_str() {
        "open" => stable.open_child_file(OsStr::new("fifo")).map(drop),
        "open-or-create" => stable
            .open_or_create_child_file(OsStr::new("fifo"))
            .map(drop),
        "unlink" => stable.unlink_child_if_identity(OsStr::new("fifo"), fifo_identity),
        "replace-source" => {
            stable.replace_child(OsStr::new("fifo"), fifo_identity, OsStr::new("regular"))
        }
        "replace-target" => {
            stable.replace_child(OsStr::new("regular"), regular_identity, OsStr::new("fifo"))
        }
        "authenticated-source" => stable.replace_authenticated_child(
            OsStr::new("fifo"),
            fifo_identity,
            OsStr::new("regular"),
            regular_identity,
        ),
        "authenticated-target" => stable.replace_authenticated_child(
            OsStr::new("regular"),
            regular_identity,
            OsStr::new("fifo"),
            fifo_identity,
        ),
        "native-replace-source" => replace_file(
            &directory_handle(&root),
            OsStr::new("fifo"),
            OsStr::new("regular"),
        )
        .map_err(|error| io::Error::other(error.to_string())),
        "native-replace-target" => replace_file(
            &directory_handle(&root),
            OsStr::new("regular"),
            OsStr::new("fifo"),
        )
        .map_err(|error| io::Error::other(error.to_string())),
        "native-install-source" => install_new_file(
            &directory_handle(&root),
            OsStr::new("fifo"),
            OsStr::new("absent"),
        ),
        other => panic!("unknown FIFO operation {other}"),
    };
    assert!(
        result.is_err(),
        "peerless FIFO must fail closed: {operation}"
    );
}

#[cfg(unix)]
#[test]
fn every_regular_child_operation_rejects_peerless_fifo_without_blocking() {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    for operation in [
        "open",
        "open-or-create",
        "unlink",
        "replace-source",
        "replace-target",
        "native-replace-source",
        "native-replace-target",
        "native-install-source",
        "authenticated-source",
        "authenticated-target",
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("regular"), b"regular").unwrap();
        let status = Command::new("mkfifo")
            .arg(root.path().join("fifo"))
            .status()
            .unwrap();
        assert!(status.success(), "mkfifo failed for {operation}");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "tests::peerless_fifo_child", "--nocapture"])
            .env(FIFO_CHILD_ENV, operation)
            .env(FIFO_ROOT_ENV, root.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let started = Instant::now();
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "FIFO child failed for {operation}: {status}"
                );
                break;
            }
            if started.elapsed() >= Duration::from_secs(2) {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("regular-child operation blocked on peerless FIFO: {operation}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[cfg(unix)]
#[test]
fn regular_child_operations_reject_symlinks_and_unix_sockets() {
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixDatagram;

    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("regular"), b"regular").unwrap();
    symlink("regular", root.path().join("linked")).unwrap();
    let _socket = UnixDatagram::bind(root.path().join("socket")).unwrap();
    let stable = StableDirectory::open(root.path()).unwrap();

    for special in ["linked", "socket"] {
        assert!(stable.open_child_file(OsStr::new(special)).is_err());
        assert!(
            stable
                .open_or_create_child_file(OsStr::new(special))
                .is_err()
        );
        let identity = path_identity(&root.path().join(special)).unwrap();
        assert!(
            stable
                .unlink_child_if_identity(OsStr::new(special), identity)
                .is_err()
        );
        assert!(root.path().join(special).exists());
    }
}

#[test]
fn replacement_changes_exact_bytes_and_consumes_source() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let target = directory.path().join("target");
    std::fs::write(&source, b"new").unwrap();
    std::fs::write(&target, b"old").unwrap();
    let handle = directory_handle(directory.path());
    replace_file(&handle, OsStr::new("source"), OsStr::new("target")).unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"new");
    assert!(!source.exists());
}

#[test]
fn no_replace_rename_preserves_file_and_directory_destinations() {
    for directory in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let target = root.path().join("target");
        let contents = |path: &Path| {
            if directory {
                path.join("payload")
            } else {
                path.to_path_buf()
            }
        };
        if directory {
            std::fs::create_dir(&source).unwrap();
            std::fs::create_dir(&target).unwrap();
        }
        std::fs::write(contents(&source), b"source").unwrap();
        std::fs::write(contents(&target), b"sentinel").unwrap();
        assert_eq!(
            rename_no_replace(&source, &target).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(std::fs::read(contents(&source)).unwrap(), b"source");
        assert_eq!(std::fs::read(contents(&target)).unwrap(), b"sentinel");
        let absent = root.path().join("absent");
        rename_no_replace(&source, &absent).unwrap();
        assert!(!source.exists());
        assert_eq!(std::fs::read(contents(&absent)).unwrap(), b"source");
    }
}

#[test]
fn no_replace_rename_concurrent_publish_has_one_winner() {
    use std::sync::{Arc, Barrier};
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target");
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for name in ["first", "second"] {
        let source = root.path().join(name);
        std::fs::write(&source, name.as_bytes()).unwrap();
        let target = target.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            (source.clone(), rename_no_replace(&source, &target))
        }));
    }
    barrier.wait();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results.iter().filter(|(_, result)| result.is_ok()).count(),
        1
    );
    for (source, result) in results {
        if let Err(error) = result {
            assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
            assert_eq!(
                std::fs::read(&source).unwrap(),
                source.file_name().unwrap().to_str().unwrap().as_bytes()
            );
        } else {
            assert!(!source.exists());
            assert_eq!(
                std::fs::read(&target).unwrap(),
                source.file_name().unwrap().to_str().unwrap().as_bytes()
            );
        }
    }
}

#[test]
fn new_install_never_replaces_an_existing_entry() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let target = directory.path().join("target");
    std::fs::write(&source, b"new").unwrap();
    let handle = directory_handle(directory.path());
    install_new_file(&handle, OsStr::new("source"), OsStr::new("target")).unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"new");

    let second = directory.path().join("second");
    std::fs::write(&second, b"other").unwrap();
    let error = install_new_file(&handle, OsStr::new("second"), OsStr::new("target")).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&target).unwrap(), b"new");
    assert_eq!(std::fs::read(&second).unwrap(), b"other");
}

#[cfg(windows)]
#[test]
fn concurrent_no_replace_install_has_exactly_one_winner() {
    use std::sync::{Arc, Barrier};

    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first");
    let second = directory.path().join("second");
    let target = directory.path().join("target");
    std::fs::write(&first, b"first").unwrap();
    std::fs::write(&second, b"second").unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let mut contenders = Vec::new();
    for name in ["first", "second"] {
        let path = directory.path().to_path_buf();
        let barrier = Arc::clone(&barrier);
        contenders.push(std::thread::spawn(move || {
            let handle = directory_handle(&path);
            barrier.wait();
            install_new_file(&handle, OsStr::new(name), OsStr::new("target"))
        }));
    }
    barrier.wait();
    let results = contenders
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| {
                result
                    .as_ref()
                    .is_err_and(|error| error.kind() == io::ErrorKind::AlreadyExists)
            })
            .count(),
        1
    );
    let target_bytes = std::fs::read(&target).unwrap();
    assert!(target_bytes == b"first" || target_bytes == b"second");
    let loser = if target_bytes == b"first" {
        second
    } else {
        first
    };
    assert!(loser.exists());
}

#[test]
fn hard_linked_inputs_are_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let alias = directory.path().join("alias");
    let target = directory.path().join("target");
    std::fs::write(&source, b"new").unwrap();
    std::fs::hard_link(&source, &alias).unwrap();
    std::fs::write(&target, b"old").unwrap();
    assert!(matches!(
        replace_file(
            &directory_handle(directory.path()),
            OsStr::new("source"),
            OsStr::new("target")
        ),
        Err(ReplaceFileError::NotReplaced(_))
    ));
    assert_eq!(std::fs::read(&target).unwrap(), b"old");
}

#[test]
fn private_directory_is_created_without_inheriting_public_access() {
    let parent = tempfile::tempdir().unwrap();
    let directory = parent.path().join("private");
    create_private_directory(&directory).unwrap();
    assert!(directory.is_dir());
    let identity = path_identity(&directory).unwrap();
    assert_ne!(identity.file_id, [0; 16]);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            std::fs::metadata(directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
}

#[cfg(unix)]
#[test]
fn stable_directory_fails_closed_after_named_child_substitution() {
    let root = tempfile::tempdir().unwrap();
    let stable_root = StableDirectory::open(root.path()).unwrap();
    let child = stable_root
        .create_child_directory(OsStr::new("objects"))
        .unwrap();
    let file = child.create_child_file(OsStr::new("payload")).unwrap();
    drop(file);
    let displaced = root.path().join("displaced");
    std::fs::rename(root.path().join("objects"), &displaced).unwrap();
    std::fs::create_dir(root.path().join("objects")).unwrap();

    assert!(child.revalidate_named().is_err());
    assert!(child.open_child_file(OsStr::new("payload")).is_err());
    assert_eq!(std::fs::read(displaced.join("payload")).unwrap(), b"");
}

#[cfg(unix)]
#[test]
fn stable_directory_rejects_symlink_child() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), root.path().join("objects")).unwrap();
    let stable_root = StableDirectory::open(root.path()).unwrap();
    assert!(
        stable_root
            .open_child_directory(OsStr::new("objects"))
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn descriptor_relative_visit_skips_symlinks_and_rejects_root_replacement() {
    use std::os::unix::fs::symlink;

    let parent = tempfile::tempdir().unwrap();
    let owned = parent.path().join("owned");
    let outside = parent.path().join("outside");
    std::fs::create_dir(&owned).unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(owned.join("inside"), b"inside").unwrap();
    std::fs::write(outside.join("secret"), b"secret").unwrap();
    symlink(&outside, owned.join("escape")).unwrap();
    let stable = StableDirectory::open(&owned).unwrap();
    let mut remaining = 16;
    let mut lengths = Vec::new();
    stable
        .visit_regular_files(&mut remaining, &mut |file| {
            lengths.push(file.metadata()?.len());
            Ok(())
        })
        .unwrap();
    assert_eq!(lengths, [6]);

    std::fs::rename(&owned, parent.path().join("displaced")).unwrap();
    std::fs::create_dir(&owned).unwrap();
    std::fs::write(owned.join("replacement"), b"replacement").unwrap();
    let mut visited = 0;
    assert!(
        stable
            .visit_regular_files(&mut remaining, &mut |_| {
                visited += 1;
                Ok(())
            })
            .is_err()
    );
    assert_eq!(visited, 0);
}

#[cfg(unix)]
#[test]
fn stable_directory_enumerates_and_links_only_retained_regular_source() {
    use std::io::Write as _;

    let root = tempfile::tempdir().unwrap();
    let stable = StableDirectory::open(root.path()).unwrap();
    let source_dir = stable.create_child_directory(OsStr::new("source")).unwrap();
    let destination = stable
        .create_child_directory(OsStr::new("destination"))
        .unwrap();
    let mut source = source_dir.create_child_file(OsStr::new("payload")).unwrap();
    source.write_all(b"payload").unwrap();
    let identity = file_identity(&source).unwrap();
    assert_eq!(
        source_dir.child_names().unwrap(),
        [std::ffi::OsString::from("payload")]
    );
    let (installed, installed_identity) = source_dir
        .link_child_into(
            OsStr::new("payload"),
            &source,
            identity,
            &destination,
            OsStr::new("copy"),
        )
        .unwrap();
    assert_eq!(installed_identity, identity);
    assert_eq!(file_identity(&installed).unwrap(), identity);

    std::fs::rename(
        root.path().join("source/payload"),
        root.path().join("source/old"),
    )
    .unwrap();
    std::fs::write(root.path().join("source/payload"), b"replacement").unwrap();
    assert!(
        source_dir
            .link_child_into(
                OsStr::new("payload"),
                &source,
                identity,
                &destination,
                OsStr::new("bad")
            )
            .is_err()
    );
    assert!(!root.path().join("destination/bad").exists());
}

#[test]
fn stable_directory_rejects_non_child_names_and_identity_mismatch_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let stable = StableDirectory::open(root.path()).unwrap();
    assert!(stable.open_child_file(OsStr::new("../escape")).is_err());
    let file = stable.create_child_file(OsStr::new("payload")).unwrap();
    let identity = file_identity(&file).unwrap();
    drop(file);
    std::fs::rename(root.path().join("payload"), root.path().join("old")).unwrap();
    std::fs::write(root.path().join("payload"), b"replacement").unwrap();
    assert!(
        stable
            .unlink_child_if_identity(OsStr::new("payload"), identity)
            .is_err()
    );
    assert_eq!(
        std::fs::read(root.path().join("payload")).unwrap(),
        b"replacement"
    );
}

#[test]
fn authenticated_replacement_preserves_shared_payload_and_open_snapshot() {
    use std::io::Read as _;
    let root = tempfile::tempdir().unwrap();
    let stable = StableDirectory::open(root.path()).unwrap();
    std::fs::write(root.path().join("target"), b"old payload").unwrap();
    std::fs::hard_link(root.path().join("target"), root.path().join("cas")).unwrap();
    std::fs::write(root.path().join("temporary"), b"new payload").unwrap();
    let mut permissions = std::fs::metadata(root.path().join("cas"))
        .unwrap()
        .permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(root.path().join("cas"), permissions).unwrap();
    // Match Parquet readers: File::open shares deletion on Windows.
    // Retained authentication capabilities intentionally deny deletion.
    let mut snapshot = File::open(root.path().join("target")).unwrap();
    let prior = file_identity(&snapshot).unwrap();
    let staged = path_identity(&root.path().join("temporary")).unwrap();
    assert!(
        stable
            .replace_child(OsStr::new("temporary"), staged, OsStr::new("target"))
            .is_err()
    );
    stable
        .replace_authenticated_child(OsStr::new("temporary"), staged, OsStr::new("target"), prior)
        .unwrap();
    stable.sync().unwrap();
    let mut old = Vec::new();
    snapshot.read_to_end(&mut old).unwrap();
    assert_eq!(old, b"old payload");
    assert_eq!(
        std::fs::read(root.path().join("cas")).unwrap(),
        b"old payload"
    );
    assert_eq!(path_identity(&root.path().join("cas")).unwrap(), prior);
    assert_eq!(
        std::fs::read(root.path().join("target")).unwrap(),
        b"new payload"
    );
    assert_eq!(path_identity(&root.path().join("target")).unwrap(), staged);
    assert!(!root.path().join("temporary").exists());
    assert!(
        std::fs::metadata(root.path().join("cas"))
            .unwrap()
            .permissions()
            .readonly()
    );
}

#[cfg(windows)]
#[test]
fn authenticated_replacement_respects_retained_authentication_guard() {
    let root = tempfile::tempdir().unwrap();
    let stable = StableDirectory::open(root.path()).unwrap();
    std::fs::write(root.path().join("target"), b"old").unwrap();
    std::fs::hard_link(root.path().join("target"), root.path().join("cas")).unwrap();
    std::fs::write(root.path().join("temporary"), b"new").unwrap();
    let guard = stable.open_child_file(OsStr::new("target")).unwrap();
    let prior = file_identity(&guard).unwrap();
    let staged = path_identity(&root.path().join("temporary")).unwrap();
    assert!(
        stable
            .replace_authenticated_child(
                OsStr::new("temporary"),
                staged,
                OsStr::new("target"),
                prior
            )
            .is_err()
    );
    assert_eq!(std::fs::read(root.path().join("target")).unwrap(), b"old");
    assert_eq!(
        std::fs::read(root.path().join("temporary")).unwrap(),
        b"new"
    );
    drop(guard);
    stable
        .replace_authenticated_child(OsStr::new("temporary"), staged, OsStr::new("target"), prior)
        .unwrap();
    assert_eq!(std::fs::read(root.path().join("target")).unwrap(), b"new");
    assert_eq!(std::fs::read(root.path().join("cas")).unwrap(), b"old");
}

#[cfg(unix)]
#[test]
fn authenticated_replacement_rejects_symlinks_and_directories() {
    let root = tempfile::tempdir().unwrap();
    let stable = StableDirectory::open(root.path()).unwrap();
    std::fs::write(root.path().join("regular"), b"payload").unwrap();
    std::os::unix::fs::symlink("regular", root.path().join("symlink")).unwrap();
    std::fs::create_dir(root.path().join("directory")).unwrap();
    let regular = path_identity(&root.path().join("regular")).unwrap();
    for name in ["symlink", "directory"] {
        let invalid = path_identity(&root.path().join(name)).unwrap();
        assert!(
            stable
                .replace_authenticated_child(
                    OsStr::new(name),
                    invalid,
                    OsStr::new("regular"),
                    regular
                )
                .is_err()
        );
        assert!(
            stable
                .replace_authenticated_child(
                    OsStr::new("regular"),
                    regular,
                    OsStr::new(name),
                    invalid
                )
                .is_err()
        );
    }
    assert_eq!(
        std::fs::read(root.path().join("regular")).unwrap(),
        b"payload"
    );
}

#[test]
fn authenticated_replacement_rejects_substitution_and_shared_source() {
    let root = tempfile::tempdir().unwrap();
    let stable = StableDirectory::open(root.path()).unwrap();
    std::fs::write(root.path().join("target"), b"old").unwrap();
    std::fs::write(root.path().join("temporary"), b"new").unwrap();
    std::fs::write(root.path().join("other"), b"other").unwrap();
    let prior = path_identity(&root.path().join("target")).unwrap();
    let staged = path_identity(&root.path().join("temporary")).unwrap();
    let other = path_identity(&root.path().join("other")).unwrap();
    for (source, target) in [(other, prior), (staged, other)] {
        assert!(
            stable
                .replace_authenticated_child(
                    OsStr::new("temporary"),
                    source,
                    OsStr::new("target"),
                    target
                )
                .is_err()
        );
    }
    std::fs::hard_link(root.path().join("temporary"), root.path().join("alias")).unwrap();
    assert!(
        stable
            .replace_authenticated_child(
                OsStr::new("temporary"),
                staged,
                OsStr::new("target"),
                prior
            )
            .is_err()
    );
    assert_eq!(std::fs::read(root.path().join("target")).unwrap(), b"old");
    assert_eq!(
        std::fs::read(root.path().join("temporary")).unwrap(),
        b"new"
    );
}

#[test]
fn stable_directory_rejects_replaced_atomic_temporary_child() {
    use std::io::Write as _;

    let root = tempfile::tempdir().unwrap();
    let stable = StableDirectory::open(root.path()).unwrap();
    let mut temporary = stable
        .create_replaceable_child_file(OsStr::new("temporary"))
        .unwrap();
    temporary.write_all(b"authenticated").unwrap();
    temporary.sync_all().unwrap();
    let expected = file_identity(&temporary).unwrap();
    drop(temporary);
    std::fs::rename(root.path().join("temporary"), root.path().join("original")).unwrap();
    std::fs::write(root.path().join("temporary"), b"substitute").unwrap();

    assert!(
        stable
            .replace_child(OsStr::new("temporary"), expected, OsStr::new("CURRENT"))
            .is_err()
    );
    assert_eq!(
        std::fs::read(root.path().join("temporary")).unwrap(),
        b"substitute"
    );
    assert_eq!(
        std::fs::read(root.path().join("original")).unwrap(),
        b"authenticated"
    );
    assert!(!root.path().join("CURRENT").exists());
}

#[test]
fn unpublished_artifact_guard_tracks_rename_commit_and_safe_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();

    {
        let mut guard = directory
            .create_unpublished_replaceable_child(OsStr::new("drop-temp"))
            .unwrap();
        let mut file = guard.take_file().unwrap();
        file.write_all(b"temporary").unwrap();
        file.sync_all().unwrap();
        drop(file);
    }
    assert!(!root.path().join("drop-temp").exists());

    {
        let mut guard = directory
            .create_unpublished_replaceable_child(OsStr::new("rename-temp"))
            .unwrap();
        let mut file = guard.take_file().unwrap();
        file.write_all(b"renamed").unwrap();
        file.sync_all().unwrap();
        drop(file);
        guard.install_child(OsStr::new("renamed-final")).unwrap();
        guard.sync_parent().unwrap();
    }
    assert!(!root.path().join("rename-temp").exists());
    assert!(!root.path().join("renamed-final").exists());

    {
        let mut guard = directory
            .create_unpublished_replaceable_child(OsStr::new("commit-temp"))
            .unwrap();
        let mut file = guard.take_file().unwrap();
        file.write_all(b"committed").unwrap();
        file.sync_all().unwrap();
        drop(file);
        guard.install_child(OsStr::new("committed-final")).unwrap();
        guard.sync_parent().unwrap();
        guard.commit().unwrap();
    }
    assert_eq!(
        std::fs::read(root.path().join("committed-final")).unwrap(),
        b"committed"
    );

    std::fs::write(root.path().join("occupied"), b"original").unwrap();
    {
        let mut guard = directory
            .create_unpublished_replaceable_child(OsStr::new("failed-install-temp"))
            .unwrap();
        let file = guard.take_file().unwrap();
        drop(file);
        assert!(guard.install_child(OsStr::new("occupied")).is_err());
    }
    assert_eq!(
        std::fs::read(root.path().join("occupied")).unwrap(),
        b"original"
    );
    assert!(!root.path().join("failed-install-temp").exists());
}

#[cfg(windows)]
#[test]
fn stable_directory_sync_uses_an_identity_checked_write_handle() {
    let root = tempfile::tempdir().unwrap();
    let stable = StableDirectory::open(root.path()).unwrap();
    stable.sync().unwrap();
}
