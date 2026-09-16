use super::super::tests::project;
use super::super::*;
use super::*;

#[test]
fn allocation_observed_atomic_replacement_preserves_coexistence_and_cleanup() {
    for stable in [false, true] {
        for fail in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("CURRENT");
            std::fs::write(&path, vec![1_u8; 8192]).unwrap();
            let old = File::open(&path).unwrap();
            let old_bytes = graphforge_filesystem::file_space_usage(&old)
                .unwrap()
                .allocated_bytes;
            let operation = crate::StorageAllocationOperation::default();
            operation.replace_file_at(&path, &old).unwrap();
            drop(old);
            let coexistence = std::cell::Cell::new(0);
            let after_write = || {
                let (current, peak) = operation.totals().unwrap();
                assert!(current > old_bytes);
                assert_eq!(current, peak);
                coexistence.set(current);
                if fail {
                    Err(std::io::Error::other("before replacement"))
                } else {
                    Ok(())
                }
            };
            let payload = vec![2_u8; 16384];
            let result = if stable {
                let directory = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
                publish_atomic_bytes_in(
                    &directory,
                    &path,
                    std::ffi::OsStr::new("CURRENT"),
                    &payload,
                    after_write,
                    || Ok(()),
                    || Ok(()),
                    Some(&operation),
                )
            } else {
                publish_atomic_bytes_with_allocation(
                    &path,
                    &payload,
                    after_write,
                    || Ok(()),
                    || Ok(()),
                    Some(&operation),
                )
            };
            assert_eq!(result.is_err(), fail);
            let actual = File::open(&path).unwrap();
            let actual_bytes = graphforge_filesystem::file_space_usage(&actual)
                .unwrap()
                .allocated_bytes;
            assert_eq!(
                operation.totals().unwrap(),
                (actual_bytes, coexistence.get())
            );
            assert_eq!(
                std::fs::read(&path).unwrap(),
                if fail { vec![1_u8; 8192] } else { payload }
            );
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        }
    }
}

#[cfg(windows)]
#[test]
fn windows_directory_sync_uses_write_capable_handle() {
    let root = tempfile::tempdir().unwrap();

    sync_directory(root.path()).unwrap();
}

#[test]
fn journal_decode_and_atomic_temp_cleanup_matrix_is_fail_closed() {
    let root = project();
    let journal = root.path().join(TRANSACTIONS_DIR).join("malformed.json");
    std::fs::create_dir_all(journal.parent().unwrap()).unwrap();
    for bytes in [
        b"not-json".as_slice(),
        br#"{"journal_version":999}"#,
        br#"{"journal_version":1,"transaction_uuid":"bad"}"#,
    ] {
        std::fs::write(&journal, bytes).unwrap();
        assert_eq!(
            read_journal(&journal).unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );
        assert_eq!(std::fs::read(&journal).unwrap(), bytes);
    }

    let unrelated = root.path().join("metadata.json");
    assert!(!cleanup_atomicwrite_temp(&unrelated).unwrap());
    let empty = root.path().join(".atomicwriteabc123");
    std::fs::create_dir(&empty).unwrap();
    assert!(cleanup_atomicwrite_temp(&empty).unwrap());
    assert!(!empty.exists());

    let populated = root.path().join(".atomicwritedef456");
    std::fs::create_dir(&populated).unwrap();
    std::fs::write(populated.join("tmpfile.tmp"), b"abandoned").unwrap();
    assert!(cleanup_atomicwrite_temp(&populated).unwrap());
    assert!(!populated.exists());

    let hostile = root.path().join(".atomicwriteghi789");
    std::fs::create_dir(&hostile).unwrap();
    std::fs::write(hostile.join("unexpected"), b"caller bytes").unwrap();
    assert!(!cleanup_atomicwrite_temp(&hostile).unwrap());
    assert_eq!(
        std::fs::read(hostile.join("unexpected")).unwrap(),
        b"caller bytes"
    );

    let native_temp = root.path().join(format!(
        ".graphforge-atomic-{}.tmp",
        hex_digest(Sha256::digest(b"CURRENT").into())
    ));
    std::fs::write(&native_temp, b"abandoned").unwrap();
    assert!(cleanup_atomicwrite_temp(&native_temp).unwrap());
    assert!(!native_temp.exists());
}

#[test]
fn atomic_bytes_install_and_replace_use_one_bounded_native_temp() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("CURRENT");

    publish_atomic_bytes(&target, b"first\n", || Ok(()), || Ok(()), || Ok(())).unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"first\n");
    publish_atomic_bytes(&target, b"second\n", || Ok(()), || Ok(()), || Ok(())).unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"second\n");
    assert!(atomic_temp_names(root.path()).is_empty());
}

#[test]
fn concurrent_atomic_replace_of_the_same_target_does_not_share_a_temp() {
    use std::sync::{Arc, Barrier};

    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("CURRENT");
    publish_atomic_bytes(&target, b"seed\n", || Ok(()), || Ok(()), || Ok(())).unwrap();

    for _ in 0..32 {
        let barrier = Arc::new(Barrier::new(2));
        let mut joins = Vec::new();
        for payload in [b"left\n" as &[u8], b"right\n"] {
            let path = target.clone();
            let barrier = Arc::clone(&barrier);
            joins.push(std::thread::spawn(move || {
                barrier.wait();
                publish_atomic_bytes(&path, payload, || Ok(()), || Ok(()), || Ok(()))
            }));
        }
        let results: Vec<_> = joins
            .into_iter()
            .map(|thread| thread.join().expect("publisher thread"))
            .collect();
        assert!(
            results.iter().all(Result::is_ok),
            "concurrent CURRENT replace must not fail with a shared temp: {results:?}"
        );
        let published = std::fs::read(&target).unwrap();
        assert!(
            published == b"left\n" || published == b"right\n",
            "{}",
            String::from_utf8_lossy(&published)
        );
    }
    assert!(atomic_temp_names(root.path()).is_empty());
}

#[test]
fn cleanup_preserves_a_kernel_leased_atomic_temp() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("CURRENT");
    publish_atomic_bytes(
        &target,
        b"published\n",
        || {
            let temps = atomic_temp_names(root.path());
            assert_eq!(temps.len(), 1);
            let temp = root.path().join(&temps[0]);
            assert!(cleanup_atomicwrite_temp(&temp).unwrap());
            assert!(temp.exists(), "live publisher temp must not be removed");
            Ok(())
        },
        || Ok(()),
        || Ok(()),
    )
    .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"published\n");
    assert!(atomic_temp_names(root.path()).is_empty());
}

#[test]
fn cross_process_cleanup_preserves_a_kernel_leased_atomic_temp() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("CURRENT");
    publish_atomic_bytes(
        &target,
        b"published\n",
        || {
            let temps = atomic_temp_names(root.path());
            assert_eq!(temps.len(), 1);
            let temp = root.path().join(&temps[0]);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("project_publication::control::tests::atomic_temp_cleanup_child")
                .arg("--exact")
                .env("GRAPHFORGE_ATOMIC_TEMP_CLEANUP_CHILD", &temp)
                .status()
                .unwrap();
            assert!(status.success());
            assert!(temp.exists(), "child process must preserve live temp");
            Ok(())
        },
        || Ok(()),
        || Ok(()),
    )
    .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"published\n");
    assert!(atomic_temp_names(root.path()).is_empty());
}

#[test]
fn atomic_temp_cleanup_child() {
    let Ok(path) = std::env::var("GRAPHFORGE_ATOMIC_TEMP_CLEANUP_CHILD") else {
        return;
    };
    let path = Path::new(&path);
    assert!(cleanup_atomicwrite_temp(path).unwrap());
    assert!(path.exists(), "leased temp was removed by child process");
}

#[test]
fn atomic_temp_cleanup_rejects_near_misses_links_and_multiple_entries() {
    let root = project();
    for name in [
        ".atomicwrite",
        ".atomicwrite12345",
        ".atomicwrite1234567",
        ".atomicwrite12-456",
        "atomicwrite123456",
    ] {
        let path = root.path().join(name);
        std::fs::create_dir(&path).unwrap();
        assert!(!cleanup_atomicwrite_temp(&path).unwrap());
        assert!(path.exists());
    }

    let regular = root.path().join(".atomicwriteabc001");
    std::fs::write(&regular, b"caller").unwrap();
    assert!(!cleanup_atomicwrite_temp(&regular).unwrap());
    assert_eq!(std::fs::read(&regular).unwrap(), b"caller");

    let multiple = root.path().join(".atomicwriteabc002");
    std::fs::create_dir(&multiple).unwrap();
    std::fs::write(multiple.join("tmpfile.tmp"), b"temporary").unwrap();
    std::fs::write(multiple.join("second"), b"caller").unwrap();
    assert!(!cleanup_atomicwrite_temp(&multiple).unwrap());
    assert_eq!(std::fs::read(multiple.join("second")).unwrap(), b"caller");

    let wrong_entry = root.path().join(".atomicwriteabc003");
    std::fs::create_dir(&wrong_entry).unwrap();
    std::fs::write(wrong_entry.join("not-temp"), b"caller").unwrap();
    assert!(!cleanup_atomicwrite_temp(&wrong_entry).unwrap());
    assert_eq!(
        std::fs::read(wrong_entry.join("not-temp")).unwrap(),
        b"caller"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let linked_dir = root.path().join(".atomicwriteabc004");
        symlink(root.path(), &linked_dir).unwrap();
        assert!(!cleanup_atomicwrite_temp(&linked_dir).unwrap());
        assert!(
            linked_dir
                .symlink_metadata()
                .unwrap()
                .file_type()
                .is_symlink()
        );

        let linked_entry = root.path().join(".atomicwriteabc005");
        std::fs::create_dir(&linked_entry).unwrap();
        symlink(
            root.path().join(CURRENT_FILE),
            linked_entry.join("tmpfile.tmp"),
        )
        .unwrap();
        assert!(!cleanup_atomicwrite_temp(&linked_entry).unwrap());
        assert!(
            linked_entry
                .join("tmpfile.tmp")
                .symlink_metadata()
                .unwrap()
                .file_type()
                .is_symlink()
        );

        let hardlinked_entry = root.path().join(".atomicwriteabc006");
        std::fs::create_dir(&hardlinked_entry).unwrap();
        let owned = root.path().join("hardlink-owner");
        std::fs::write(&owned, b"caller").unwrap();
        std::fs::hard_link(&owned, hardlinked_entry.join("tmpfile.tmp")).unwrap();
        assert!(!cleanup_atomicwrite_temp(&hardlinked_entry).unwrap());
        assert_eq!(std::fs::read(&owned).unwrap(), b"caller");
    }
}

fn atomic_temp_names(root: &Path) -> Vec<String> {
    std::fs::read_dir(root)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".graphforge-atomic-"))
        .collect()
}
