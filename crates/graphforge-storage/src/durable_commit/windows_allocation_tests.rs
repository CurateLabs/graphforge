use graphforge_filesystem::StableDirectory;
use std::ffi::OsStr;

#[test]
fn windows_immutable_link_refreshes_native_allocation_and_preserves_reused_temporary() {
    use crate::durable_commit::{SealedArtifact, install_immutable, seal_file_witness};
    use graphforge_filesystem::{FileIdentity, file_identity, file_link_count, file_space_usage};
    use std::cell::Cell;
    use std::io::Write as _;

    fn prepare(
        parent: &StableDirectory,
        name: &OsStr,
        operation: &crate::StorageAllocationOperation,
    ) -> (SealedArtifact, FileIdentity, u64) {
        // A resident-sized control object and CAS-sized names reproduce the
        // NTFS allocation boundary without changing its content or length.
        let mut writer = parent.create_cas_child_file(name).unwrap();
        writer.write_all(&[0x5a; 536]).unwrap();
        let witness = seal_file_witness(writer.as_file()).unwrap();
        let file = parent
            .seal_cas_child_file(name, writer)
            .unwrap()
            .into_file();
        let identity = file_identity(&file).unwrap();
        let allocated = file_space_usage(&file).unwrap().allocated_bytes;
        operation
            .replace_file_at(&parent.path().join(name), &file)
            .unwrap();
        let sealed =
            SealedArtifact::adopt_sealed(parent, name, file, witness, Some(operation)).unwrap();
        (sealed, identity, allocated)
    }

    let root = tempfile::tempdir().unwrap();
    let temporary_root = root.path().join("tmp");
    let destination_root = root.path().join("sha256").join("aa");
    std::fs::create_dir_all(&temporary_root).unwrap();
    std::fs::create_dir_all(&destination_root).unwrap();
    let temporary = StableDirectory::open(&temporary_root).unwrap();
    let destination = StableDirectory::open(&destination_root).unwrap();
    let source_name = OsStr::new("00000000-0000-0000-0000-000000000001");
    let target_name = OsStr::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let target = destination_root.join(target_name);
    let operation = crate::StorageAllocationOperation::default();
    let (sealed, identity, before_link) = prepare(&temporary, source_name, &operation);
    let before_install = operation.snapshot().unwrap();
    let after_link = Cell::new(0);
    let (installed, installed_identity, reused) = install_immutable(
        sealed,
        &destination,
        target_name,
        |_, _| panic!("fresh destination must not have a winner"),
        |reused, file| {
            assert!(!reused);
            assert_eq!(file_identity(file).unwrap(), identity);
            assert_eq!(file_link_count(file).unwrap(), 2);
            let usage = file_space_usage(file).unwrap();
            assert_eq!(usage.logical_bytes, 536);
            eprintln!(
                "native immutable link allocation: before={before_link} after={}",
                usage.allocated_bytes
            );
            assert!(
                usage.allocated_bytes > before_link,
                "fixture must exercise real native allocation growth"
            );
            // The old ordering registered the destination against this stale
            // temporary owner. Prove that the actual measured transition
            // triggers the strict refusal, rather than accepting a no-op link.
            let mut stale = before_install.clone();
            let error = stale
                .replace_owner(
                    crate::StorageAllocationOperation::file_owner(&target).unwrap(),
                    &std::collections::BTreeMap::from([(
                        crate::storage_attribution::native_identity_key(
                            identity.volume_serial,
                            &identity.file_id,
                        ),
                        usage.allocated_bytes,
                    )]),
                )
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("active identity allocation changed")
            );
            after_link.set(usage.allocated_bytes);
            assert_eq!(
                operation.totals().unwrap(),
                (usage.allocated_bytes, usage.allocated_bytes)
            );
            operation
                .replace_file_at(&target, file)
                .map_err(std::io::Error::other)
        },
        |_, _| Ok(()),
        || Ok(()),
    )
    .unwrap();
    assert!(!reused);
    assert_eq!(installed_identity, identity);
    assert_eq!(file_link_count(&installed).unwrap(), 1);
    assert!(!temporary_root.join(source_name).exists());
    let retained = file_space_usage(&installed).unwrap().allocated_bytes;
    assert_eq!(retained, after_link.get());
    assert_eq!(operation.totals().unwrap(), (retained, retained));

    // A concurrent existing winner belongs to another identity. Its unchanged
    // allocation and the losing temporary must both remain charged until unlink.
    let losing_name = OsStr::new("00000000-0000-0000-0000-000000000002");
    let (losing, losing_identity, losing_bytes) = prepare(&temporary, losing_name, &operation);
    assert_ne!(losing_identity, identity);
    let overlapping = retained + losing_bytes;
    assert_eq!(operation.totals().unwrap(), (overlapping, overlapping));
    let (_, winner_identity, reused) = install_immutable(
        losing,
        &destination,
        target_name,
        |file, winner_identity| {
            assert_eq!(winner_identity, identity);
            assert_eq!(file_identity(file).unwrap(), identity);
            assert_eq!(file_space_usage(file).unwrap().allocated_bytes, retained);
            Ok(())
        },
        |reused, file| {
            assert!(reused);
            assert_eq!(operation.totals().unwrap(), (overlapping, overlapping));
            operation
                .replace_file_at(&target, file)
                .map_err(std::io::Error::other)
        },
        |_, _| Ok(()),
        || Ok(()),
    )
    .unwrap();
    assert!(reused);
    assert_eq!(winner_identity, identity);
    assert!(!temporary_root.join(losing_name).exists());
    assert_eq!(operation.totals().unwrap(), (retained, overlapping));
    assert_eq!(std::fs::read_dir(&temporary_root).unwrap().count(), 0);
}
