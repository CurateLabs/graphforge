use super::super::tests::{package, tar_entry};
use super::*;
use std::sync::atomic::Ordering;

#[test]
fn observed_materialization_failure_and_cancel_cleanup_retry_both_forms() {
    let source = package();
    let mut paths = Vec::new();
    walk(
        source.path(),
        source.path(),
        &mut paths,
        PortableV2Limits::default(),
        None,
    )
    .unwrap();
    paths.sort();
    let mut bytes = Vec::new();
    for path in paths {
        bytes.extend(tar_entry(
            &path,
            &fs::read(source.path().join(&path)).unwrap(),
        ));
    }
    bytes.extend([0_u8; 1024]);
    let bundle = tempfile::NamedTempFile::new().unwrap();
    fs::write(bundle.path(), bytes).unwrap();
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
    let parent = tempfile::tempdir().unwrap();
    let input_path = parent.path().join("input");
    fs::write(&input_path, vec![7_u8; 10]).unwrap();
    let mut input = File::open(input_path).unwrap();
    let mut output = Vec::new();
    let (bytes, operations) =
        copy_exact_materialized(&mut input, &mut output, 10, 4, None).unwrap();
    assert_eq!((bytes, operations), (10, 3));
    assert_eq!(output, vec![7_u8; 10]);
}
