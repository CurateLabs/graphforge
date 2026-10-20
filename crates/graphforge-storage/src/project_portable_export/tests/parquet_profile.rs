//! Portable verification and import refuse a correctly hashed package whose
//! Parquet pages use an encoding outside the funded profile, in both package
//! forms. GraphForge's own export would not write such a package, so these
//! tests assemble the representations from the plan without the writer's
//! self-verification (`write_test_representations`).

use super::*;
use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;

#[path = "../../../tests/support/hostile_parquet.rs"]
mod hostile;

const GRAPH_FILE: &str = "data/components/graph-data/graph-tree/a.parquet";

/// A committed generation whose graph tree holds `a.parquet` with `bytes`.
fn generation_with_graph_file(bytes: &[u8]) -> (tempfile::TempDir, ResolvedProjectGeneration) {
    let project = tempfile::tempdir().unwrap();
    let parent = open_or_initialize_project(project.path()).unwrap();
    let tree = tempfile::tempdir().unwrap();
    fs::write(tree.path().join("a.parquet"), bytes).unwrap();
    let (_, inventory) = crate::capture_graph_files(tree.path()).unwrap();
    let mut participants = crate::empty_workspace_participants().unwrap();
    participants.insert(0, inventory);
    let request = crate::ProjectGenerationRequest {
        transaction_uuid: Uuid::new_v4(),
        generation_uuid: Uuid::new_v4(),
        capabilities: vec![
            crate::ProjectCapability {
                capability_id: "graph".into(),
                capability_version: 1,
            },
            crate::ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let crate::ProjectStageOutcome::Staged(staged) =
        crate::stage_project_generation_with_graph_tree(
            project.path(),
            &request,
            Some(tree.path()),
        )
        .unwrap()
    else {
        panic!("fresh graph generation replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    drop(parent);
    let generation = crate::resolve_project_generation(project.path()).unwrap();
    (project, generation)
}

fn verify(package: &Path) -> Result<crate::PortableReport, PortableError> {
    crate::verify_portable(package, PortableMode::Full, PortableLimits::default(), None)
}

fn import(
    package: &Path,
    target: &Path,
    generation: &ResolvedProjectGeneration,
) -> Result<(), PortableError> {
    let supported = generation
        .capabilities()
        .into_iter()
        .map(|capability| crate::ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect::<Vec<_>>();
    crate::import_complete_portable(
        package,
        target,
        Uuid::new_v4(),
        Uuid::new_v4(),
        &supported,
        PortableLimits::default(),
        None,
    )
    .map(|_| ())
}

#[test]
fn unfunded_parquet_is_refused_by_verify_and_import_in_both_package_forms() {
    let delta = hostile::string_file(
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        WriterVersion::PARQUET_1_0,
        64,
    );
    let cases = [
        ("delta length", delta.clone()),
        (
            "delta byte array",
            hostile::string_file(Encoding::DELTA_BYTE_ARRAY, WriterVersion::PARQUET_1_0, 64),
        ),
        (
            "delta length v2 pages",
            hostile::string_file(
                Encoding::DELTA_LENGTH_BYTE_ARRAY,
                WriterVersion::PARQUET_2_0,
                64,
            ),
        ),
        (
            "footer declares plain",
            hostile::declaring(&delta, &[Encoding::PLAIN, Encoding::RLE]),
        ),
    ];
    for (name, bytes) in cases {
        let (_project, generation) = generation_with_graph_file(&bytes);
        let plan = plan_complete_portable(&generation, PortableExportLimits::default()).unwrap();
        let outputs = tempfile::tempdir().unwrap();
        let (expanded, bundle) = write_test_representations(&plan, outputs.path());
        for package in [&expanded, &bundle] {
            let error = verify(package).unwrap_err();
            assert_eq!(
                error.code,
                PortableErrorCode::LimitExceeded,
                "{name}: {error}"
            );
            assert_eq!(error.entry.as_deref(), Some(GRAPH_FILE), "{name}");
            assert!(
                error
                    .to_string()
                    .ends_with("outside the funded decoder profile"),
                "{name}: {error}"
            );
            let target = outputs.path().join("imported");
            let error = import(package, &target, &generation).unwrap_err();
            assert_eq!(
                error.code,
                PortableErrorCode::LimitExceeded,
                "{name}: {error}"
            );
            assert!(
                !target.exists(),
                "{name}: a refused import leaves no target"
            );
        }
    }
}

#[test]
fn funded_parquet_in_both_package_forms_still_verifies() {
    let funded = hostile::string_file(Encoding::PLAIN, WriterVersion::PARQUET_1_0, 64);
    let (_project, generation) = generation_with_graph_file(&funded);
    let plan = plan_complete_portable(&generation, PortableExportLimits::default()).unwrap();
    let outputs = tempfile::tempdir().unwrap();
    let (expanded, bundle) = write_test_representations(&plan, outputs.path());
    for package in [&expanded, &bundle] {
        verify(package).unwrap();
    }
}
