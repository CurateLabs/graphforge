use super::*;

#[test]
fn returned_error_child() {
    let Ok(package) = std::env::var("GF_IMPORT_RETURNED_PACKAGE") else {
        return;
    };
    let target = std::env::var("GF_IMPORT_RETURNED_TARGET").unwrap();
    let operation =
        Uuid::parse_str(&std::env::var("GF_IMPORT_RETURNED_OPERATION").unwrap()).unwrap();
    let generation =
        Uuid::parse_str(&std::env::var("GF_IMPORT_RETURNED_GENERATION").unwrap()).unwrap();
    let committed = std::env::var("GF_IMPORT_RETURNED_COMMITTED").unwrap() == "true";
    let error = import_complete_portable_v2(
        package,
        &target,
        operation,
        generation,
        &supported(),
        PortableV2Limits::default(),
        None,
    )
    .unwrap_err();
    assert_eq!(error.committed_import.is_some(), committed, "{error:?}");
    if let Some(receipt) = error.committed_import {
        assert_eq!(receipt.operation_uuid, operation);
        assert_eq!(receipt.generation_uuid, generation);
        let current = crate::resolve_project_generation(&target).unwrap();
        assert_eq!(current.generation_uuid(), generation);
        assert_eq!(
            receipt.generation_manifest_sha256,
            current.manifest_sha256()
        );
        let durable = crate::published_project_transaction(Path::new(&target), operation)
            .unwrap()
            .unwrap();
        assert_eq!(
            durable.generation_manifest_sha256,
            receipt.generation_manifest_sha256
        );
    }
}

#[test]
fn returned_publication_and_reopen_errors_preserve_commit_evidence_and_retry() {
    let (_package_owner, package) = composition_package();
    let different_source = tempfile::tempdir().unwrap();
    let different_generation = crate::open_or_initialize_project(different_source.path()).unwrap();
    let different_package = different_source.path().join("different.gfproject");
    let export_limits = crate::PortableV2ExportLimits::default();
    let plan = crate::plan_complete_portable_v2(&different_generation, export_limits).unwrap();
    crate::export_complete_portable_v2(
        &plan,
        &different_package,
        crate::PortableV2Output::Expanded,
        export_limits,
        &AtomicBool::new(false),
        |_| {},
    )
    .unwrap();
    for (failpoint, committed) in [
        ("project.before_current_replace.error", false),
        ("project.after_current_replace.error", true),
        ("portable_import.before_reopen.error", true),
    ] {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("imported");
        let old = crate::open_or_initialize_project(&target)
            .unwrap()
            .generation_uuid();
        let operation = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "project_portable_v2_import::tests::returned_errors::returned_error_child",
                "--nocapture",
            ])
            .env("GF_IMPORT_RETURNED_PACKAGE", &package)
            .env("GF_IMPORT_RETURNED_TARGET", &target)
            .env("GF_IMPORT_RETURNED_OPERATION", operation.to_string())
            .env("GF_IMPORT_RETURNED_GENERATION", generation.to_string())
            .env("GF_IMPORT_RETURNED_COMMITTED", committed.to_string())
            .env("GRAPHFORGE_PROJECT_FAILPOINTS", COOKIE)
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
                operation.to_string(),
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
            .status()
            .unwrap();
        assert!(status.success(), "{failpoint}: {status}");
        assert_eq!(
            crate::resolve_project_generation(&target)
                .unwrap()
                .generation_uuid(),
            if committed { generation } else { old }
        );
        let conflict = import(&different_package, &target, operation, generation).unwrap_err();
        assert_eq!(conflict.code, PortableV2ErrorCode::ConcurrentMutation);
        assert!(conflict.committed_import.is_none());
        assert_eq!(
            crate::resolve_project_generation(&target)
                .unwrap()
                .generation_uuid(),
            if committed { generation } else { old }
        );
        let retry = import(&package, &target, operation, generation).unwrap();
        assert_eq!(retry.publication.generation_uuid, generation);
        assert_eq!(retry.publication.idempotent_replay, committed);
        assert!(retry.materialized_cleanup.parent_sync_confirmed);
        let exact = import(&package, &target, operation, generation).unwrap();
        assert!(exact.publication.idempotent_replay);
        assert_eq!(
            exact.publication.generation_manifest_sha256,
            retry.publication.generation_manifest_sha256
        );
        let conflict = import(&package, &target, operation, Uuid::new_v4()).unwrap_err();
        assert!(conflict.committed_import.is_none());
        assert_eq!(
            crate::resolve_project_generation(&target)
                .unwrap()
                .generation_uuid(),
            generation
        );
    }
}

fn import(
    package: &Path,
    target: &Path,
    operation: Uuid,
    generation: Uuid,
) -> Result<PortableV2ImportReceipt, PortableV2Error> {
    import_complete_portable_v2(
        package,
        target,
        operation,
        generation,
        &supported(),
        PortableV2Limits::default(),
        None,
    )
}
