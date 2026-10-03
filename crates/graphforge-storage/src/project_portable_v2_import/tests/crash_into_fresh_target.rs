//! An interrupted import into a target that did not exist yet retries alone.

use super::*;

#[test]
fn retries_without_external_recovery() {
    let source_project = tempfile::tempdir().unwrap();
    let source_generation = crate::open_or_initialize_project(source_project.path()).unwrap();
    let package_parent = tempfile::tempdir().unwrap();
    let package = package_parent.path().join("complete.gfproject");
    let limits = crate::PortableV2ExportLimits::default();
    let plan = crate::plan_complete_portable_v2(&source_generation, limits).unwrap();
    crate::export_complete_portable_v2(
        &plan,
        &package,
        crate::PortableV2Output::Expanded,
        limits,
        &AtomicBool::new(false),
        |_| {},
    )
    .unwrap();
    for failpoint in [
        "portable_import.after_owner",
        "project.after_writer_lock",
        "project.after_manifest_fsync",
        "project.after_current_replace",
    ] {
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("project");
        let transaction = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(HELPER)
            .arg("--nocapture")
            .env("GRAPHFORGE_PORTABLE_V2_CRASH_PACKAGE", &package)
            .env("GRAPHFORGE_PORTABLE_V2_CRASH_TARGET", &target)
            .env(
                "GRAPHFORGE_PORTABLE_V2_CRASH_TRANSACTION",
                transaction.to_string(),
            )
            .env(
                "GRAPHFORGE_PORTABLE_V2_CRASH_GENERATION",
                generation.to_string(),
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINTS", COOKIE)
            .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
        let receipt = import_complete_portable_v2(
            &package,
            &target,
            transaction,
            generation,
            &supported(),
            PortableV2Limits::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("retry after {failpoint} failed: {error:?}"));
        assert_eq!(receipt.publication.generation_uuid, generation);
        assert_eq!(
            crate::resolve_project_generation(&target)
                .unwrap()
                .generation_uuid(),
            generation
        );
    }
}
