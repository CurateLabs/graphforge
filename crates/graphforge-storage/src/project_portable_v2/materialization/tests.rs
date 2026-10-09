use super::super::tests::{package, tar_entry};
use super::*;
use crate::project_portable_v2::walk;
use std::sync::atomic::Ordering;

#[test]
fn observed_materialization_failure_and_cancel_cleanup_retry_both_forms() {
    let source = package();
    let bundle = bundle_from_expanded(source.path());
    for input in [source.path(), bundle.path()] {
        for cancel in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let destination = root.path().join("materialized");
            let operation = crate::StorageAllocationOperation::default();
            let cancelled = AtomicBool::new(false);
            let mut injected = false;
            let result = materialize_verified_portable_v2_observed(
                input,
                &destination,
                PortableV2Limits::default(),
                Some(&cancelled),
                |path, file| {
                    match file {
                        Some(file) => {
                            operation.replace_file_at(path, file).unwrap();
                            if !injected && file.metadata().unwrap().len() > 0 {
                                injected = true;
                                if cancel {
                                    cancelled.store(true, Ordering::Relaxed);
                                } else {
                                    return Err(PortableV2Error::new(
                                        PortableV2ErrorCode::Io,
                                        "injected observed write failure",
                                    ));
                                }
                            }
                        }
                        None => {
                            assert!(!path.exists());
                            operation.remove_file_at(path).unwrap();
                        }
                    }
                    Ok(())
                },
                true,
            );
            assert!(injected);
            let error = result.err().expect("injected materialization must fail");
            if !cancel {
                assert_eq!(error.code, PortableV2ErrorCode::Io);
            }
            assert!(!destination.exists());
            assert_eq!(operation.totals().unwrap().0, 0);
            assert!(operation.totals().unwrap().1 > 0);
            cancelled.store(false, Ordering::Relaxed);
            materialize_verified_portable_v2_observed(
                input,
                &destination,
                PortableV2Limits::default(),
                Some(&cancelled),
                |path, file| {
                    match file {
                        Some(file) => operation.replace_file_at(path, file).unwrap(),
                        None => operation.remove_file_at(path).unwrap(),
                    }
                    Ok(())
                },
                true,
            )
            .unwrap();
            let actual = crate::StorageAllocationOperation::from_paths(&[destination]).unwrap();
            assert_eq!(operation.totals().unwrap().0, actual.totals().unwrap().0);
        }
    }
}

#[test]
fn materialization_reports_actual_bounded_payload_reads() {
    use graphforge_core::hash_observation::operation::Capture;
    let source = package();
    let bundle = bundle_from_expanded(source.path());
    let limits = PortableV2Limits {
        copy_buffer_bytes: 4,
        ..Default::default()
    };
    for input in [source.path(), bundle.path()] {
        let parent = tempfile::tempdir().unwrap();
        let destination = parent.path().join("materialized");
        let capture = Capture::start();
        let materialized = materialize_verified_portable_v2_observed(
            input,
            &destination,
            limits,
            None,
            |_, _| Ok(()),
            false,
        )
        .unwrap();
        let observed = capture.snapshot();
        drop(capture);
        assert_eq!(materialized.application_read_bytes, 2);
        assert_eq!(materialized.application_read_operations, 1);
        assert_eq!(
            fs::read(destination.join("data/components/ontology/core-ontology/ontology.json"))
                .unwrap(),
            b"{}"
        );
        let transport = if input.is_file() {
            fs::metadata(input).unwrap().len()
        } else {
            0
        };
        assert_eq!(
            observed.portable_authentication_sha256_bytes,
            materialized.report.payload_bytes + transport
        );
        assert_eq!(observed.artifact_payload_sha256_bytes, 0);
        assert_eq!(observed.unclassified_sha256_bytes, 0);
    }
}

#[test]
fn import_authenticates_consumed_bytes_when_source_is_changed_and_restored() {
    let relative = "data/components/ontology/core-ontology/ontology.json";
    for bundled in [false, true] {
        let source = package();
        let bundle = bundle_from_expanded(source.path());
        let input = if bundled {
            bundle.path()
        } else {
            source.path()
        };
        let changed = if bundled {
            bundle.path().to_owned()
        } else {
            source.path().join(relative)
        };
        let original = fs::read(&changed).unwrap();
        let modified = fs::metadata(&changed).unwrap().modified().unwrap();
        let offset = if bundled {
            original
                .windows(relative.len())
                .position(|bytes| bytes == relative.as_bytes())
                .unwrap()
                + 512
        } else {
            0
        };
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("materialized");
        let mut injected = false;
        let mut restored = false;
        let result = materialize_verified_portable_v2_observed(
            input,
            &destination,
            PortableV2Limits {
                copy_buffer_bytes: 1,
                ..Default::default()
            },
            None,
            |_, file| {
                let Some(file) = file else {
                    return Ok(());
                };
                match file.metadata().unwrap().len() {
                    1 if !injected => {
                        let mut bytes = original.clone();
                        bytes[offset + 1] = b']';
                        fs::write(&changed, bytes).unwrap();
                        injected = true;
                    }
                    2 if injected && !restored => {
                        fs::write(&changed, &original).unwrap();
                        fs::OpenOptions::new()
                            .write(true)
                            .open(&changed)
                            .unwrap()
                            .set_modified(modified)
                            .unwrap();
                        restored = true;
                    }
                    _ => {}
                }
                Ok(())
            },
            false,
        );
        assert!(injected && restored);
        assert_eq!(fs::read(changed).unwrap(), original);
        assert_eq!(
            result
                .err()
                .expect("copied corruption must be refused")
                .code,
            PortableV2ErrorCode::DigestMismatch
        );
        assert!(!destination.exists());
    }
}

#[test]
fn materialization_refuses_corruption_of_written_output_with_unchanged_source() {
    let source = package();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("materialized");
    let mut corrupted = false;
    let result = materialize_verified_portable_v2_observed(
        source.path(),
        &destination,
        PortableV2Limits::default(),
        None,
        |path, file| {
            if !corrupted && file.is_some_and(|file| file.metadata().unwrap().len() == 2) {
                fs::write(path, b"[]").unwrap();
                corrupted = true;
            }
            Ok(())
        },
        false,
    );
    assert!(corrupted);
    assert_eq!(
        result
            .err()
            .expect("physical staged corruption must be refused")
            .code,
        PortableV2ErrorCode::ConcurrentMutation
    );
    assert!(!destination.exists());
    assert_eq!(
        fs::read(
            source
                .path()
                .join("data/components/ontology/core-ontology/ontology.json")
        )
        .unwrap(),
        b"{}"
    );
}

fn bundle_from_expanded(source: &Path) -> tempfile::NamedTempFile {
    let mut paths = Vec::new();
    walk(
        source,
        source,
        &mut paths,
        PortableV2Limits::default(),
        None,
    )
    .unwrap();
    paths.sort();
    let mut bytes = Vec::new();
    for path in paths {
        bytes.extend(tar_entry(&path, &fs::read(source.join(&path)).unwrap()));
    }
    bytes.extend([0_u8; 1024]);
    let bundle = tempfile::NamedTempFile::new().unwrap();
    fs::write(bundle.path(), bytes).unwrap();
    bundle
}

#[test]
fn source_changes_during_materialization_release_routes_and_allow_retry() {
    for mutation in ["append-expanded", "remove-expanded", "append-bundle"] {
        let source = package();
        let bundle = bundle_from_expanded(source.path());
        let input = if mutation == "append-bundle" {
            bundle.path()
        } else {
            source.path()
        };
        let payload = source
            .path()
            .join("data/components/ontology/core-ontology/ontology.json");
        let changed = if mutation == "append-bundle" {
            bundle.path()
        } else {
            payload.as_path()
        };
        let original = fs::read(changed).unwrap();
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("materialized");
        let allocation = crate::StorageAllocationOperation::default();
        let mut injected = false;
        let result = materialize_verified_portable_v2_observed(
            input,
            &destination,
            PortableV2Limits::default(),
            None,
            |path, file| {
                match file {
                    Some(file) => {
                        allocation.replace_file_at(path, file).unwrap();
                        if !injected && file.metadata().unwrap().len() > 0 {
                            injected = true;
                            if mutation == "remove-expanded" {
                                fs::remove_file(changed).unwrap();
                            } else {
                                fs::OpenOptions::new()
                                    .append(true)
                                    .open(changed)
                                    .unwrap()
                                    .write_all(b"changed")
                                    .unwrap();
                            }
                        }
                    }
                    None => {
                        assert!(!path.exists());
                        allocation.remove_file_at(path).unwrap();
                    }
                }
                Ok(())
            },
            true,
        );
        assert!(injected);
        let error = result.err().expect("changed source must be refused");
        assert_eq!(error.code, PortableV2ErrorCode::ConcurrentMutation);
        assert!(!destination.exists());
        assert_eq!(allocation.totals().unwrap().0, 0);
        assert!(allocation.totals().unwrap().1 > 0);
        fs::write(changed, &original).unwrap();
        materialize_verified_portable_v2_observed(
            input,
            &destination,
            PortableV2Limits::default(),
            None,
            |path, file| {
                match file {
                    Some(file) => allocation.replace_file_at(path, file).unwrap(),
                    None => allocation.remove_file_at(path).unwrap(),
                }
                Ok(())
            },
            true,
        )
        .unwrap();
        assert_eq!(
            fs::read(destination.join("data/components/ontology/core-ontology/ontology.json"))
                .unwrap(),
            b"{}"
        );
        let actual = crate::StorageAllocationOperation::from_paths(&[destination.clone()]).unwrap();
        assert_eq!(allocation.totals().unwrap().0, actual.totals().unwrap().0);
        let error = materialize_verified_portable_v2_observed(
            input,
            &destination,
            PortableV2Limits::default(),
            None,
            |_, _| panic!("existing destination must be refused before observing writes"),
            true,
        )
        .err()
        .unwrap();
        assert_eq!(error.code, PortableV2ErrorCode::Io);
        assert_eq!(
            fs::read(destination.join("data/components/ontology/core-ontology/ontology.json"))
                .unwrap(),
            b"{}"
        );
    }
}

fn captured_ontology() -> (tempfile::TempDir, std::path::PathBuf, MaterializedCapture) {
    let package = package();
    let owner = tempfile::tempdir().unwrap();
    let stage = owner.path().join("materialized");
    let mut materialized = materialize_verified_portable_v2_observed(
        package.path(),
        &stage,
        PortableV2Limits::default(),
        None,
        |_, _| Ok(()),
        false,
    )
    .unwrap();
    let relative = "data/components/ontology/core-ontology/ontology.json";
    let capture = materialized.captures.remove(relative).unwrap();
    (owner, stage.join(relative), capture)
}

#[test]
fn portable_captured_cas_refuses_consumed_mutation_restored_before_revalidation() {
    let (_owner, path, capture) = captured_ontology();
    let target = tempfile::tempdir().unwrap();
    crate::open_or_initialize_project(target.path()).unwrap();
    let current = fs::read(target.path().join("CURRENT")).unwrap();
    let before = fs::metadata(&path).unwrap();
    let identity = graphforge_filesystem::path_identity(&path).unwrap();
    let source = capture.open_source(&path).unwrap();
    let lease = crate::begin_graph_object_publication(target.path()).unwrap();
    let hook_path = path.clone();
    let injected = std::rc::Rc::new(std::cell::Cell::new(false));
    let restored = std::rc::Rc::new(std::cell::Cell::new(false));
    let hook_injected = injected.clone();
    let hook_restored = restored.clone();
    crate::graph_object_store::set_captured_copy_hook(Some(Box::new(move |phase| {
        if phase == "before_read" && !hook_injected.get() {
            fs::write(&hook_path, b"[]").unwrap();
            hook_injected.set(true);
        } else if phase == "after_read" && hook_injected.get() && !hook_restored.get() {
            fs::write(&hook_path, b"{}").unwrap();
            fs::OpenOptions::new()
                .write(true)
                .open(&hook_path)
                .unwrap()
                .set_modified(before.modified().unwrap())
                .unwrap();
            hook_restored.set(true);
        }
    })));
    let observed = graphforge_core::hash_observation::operation::Capture::start();
    let result = crate::graph_object_store::install_captured_portable_source_with_lease(
        &lease,
        &source,
        &mut || false,
    );
    let work = observed.snapshot();
    drop(observed);
    crate::graph_object_store::set_captured_copy_hook(None);
    assert!(injected.get() && restored.get());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("checksum or length changed during copy"));
    assert_eq!(work.artifact_payload_sha256_bytes, 0);
    assert_eq!(work.unclassified_sha256_bytes, 0);
    assert!(work.checksum_bytes >= 2);
    assert_eq!(fs::read(&path).unwrap(), b"{}");
    assert_eq!(
        graphforge_filesystem::path_identity(&path).unwrap(),
        identity
    );
    assert_eq!(fs::read(target.path().join("CURRENT")).unwrap(), current);
}

#[test]
fn research_captured_cas_counts_real_bytes_reuses_authority_and_refuses_corruption() {
    let (owner, path, capture) = captured_ontology();
    let stage = owner.path().join("materialized");
    let digest = hex(&capture.digest);
    let relative = format!("data/components/research/research-content/{digest}");
    let research_path = stage.join(&relative);
    fs::create_dir_all(research_path.parent().unwrap()).unwrap();
    fs::rename(path, &research_path).unwrap();
    let objects = BTreeMap::from([(digest.clone(), 2)]);
    let captures = BTreeMap::from([(relative, capture)]);
    let target = tempfile::tempdir().unwrap();
    crate::open_or_initialize_project(target.path()).unwrap();
    let lease = crate::begin_graph_object_publication(target.path()).unwrap();
    let observed = graphforge_core::hash_observation::operation::Capture::start();
    for attempt in 0..2 {
        let before = observed.snapshot();
        super::super::research::install_captured_with_lease(
            &stage, &lease, &objects, &captures, None,
        )
        .unwrap();
        let after = observed.snapshot();
        let checksum_bytes = after.checksum_bytes - before.checksum_bytes;
        if attempt == 0 {
            #[cfg(unix)]
            assert_eq!(
                checksum_bytes, 2,
                "authenticate the actual copied bytes once"
            );
            // Windows must close the writable temporary and authenticate its
            // exact-identity sealed reader before publishing the CAS name.
            #[cfg(windows)]
            assert_eq!(checksum_bytes, 4, "copy and sealed-reader authentication");
        } else {
            assert_eq!(
                checksum_bytes, 2,
                "same-lease reuse checks the retained inode"
            );
        }
    }
    let work = observed.snapshot();
    drop(observed);
    assert_eq!(work.artifact_payload_sha256_bytes, 0);
    assert_eq!(work.portable_authentication_sha256_bytes, 0);
    assert_eq!(work.unclassified_sha256_bytes, 0);
    #[cfg(unix)]
    assert_eq!(work.checksum_bytes, 4);
    #[cfg(windows)]
    assert_eq!(work.checksum_bytes, 6);
    eprintln!("research actual captured install/reuse bytes: {work:?}");
    assert_eq!(
        crate::read_graph_object_by_digest(target.path(), &digest, 2).unwrap(),
        b"{}"
    );
    let unknown = crate::begin_graph_object_publication(target.path()).unwrap();
    let observed = graphforge_core::hash_observation::operation::Capture::start();
    super::super::research::install_captured_with_lease(
        &stage, &unknown, &objects, &captures, None,
    )
    .unwrap();
    let work = observed.snapshot();
    drop(observed);
    assert_eq!(
        work.artifact_payload_sha256_bytes, 2,
        "unknown lease must authenticate existing CAS bytes"
    );
    let refused = tempfile::tempdir().unwrap();
    crate::open_or_initialize_project(refused.path()).unwrap();
    let current = fs::read(refused.path().join("CURRENT")).unwrap();
    let lease = crate::begin_graph_object_publication(refused.path()).unwrap();
    fs::write(research_path, b"[]").unwrap();
    assert!(super::super::research::install_captured_with_lease(
        &stage, &lease, &objects, &captures, None
    )
    .is_err());
    assert_eq!(fs::read(refused.path().join("CURRENT")).unwrap(), current);
}
