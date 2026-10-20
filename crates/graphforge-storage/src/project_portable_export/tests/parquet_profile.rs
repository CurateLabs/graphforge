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
    generation_with_graph_files(&[("a.parquet", bytes)])
}

/// A committed generation whose graph tree holds each `(relative path, bytes)`.
pub(super) fn generation_with_graph_files(
    files: &[(&str, &[u8])],
) -> (tempfile::TempDir, ResolvedProjectGeneration) {
    let project = tempfile::tempdir().unwrap();
    let parent = open_or_initialize_project(project.path()).unwrap();
    let tree = tempfile::tempdir().unwrap();
    for (relative, bytes) in files {
        let path = tree.path().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
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
    verify_with(package, PortableLimits::default())
}

fn verify_with(
    package: &Path,
    limits: PortableLimits,
) -> Result<crate::PortableReport, PortableError> {
    crate::verify_portable(package, PortableMode::Full, limits, None)
}

fn stream(bundle: &Path, limits: PortableLimits) -> Result<crate::PortableReport, PortableError> {
    let mut reader = std::io::BufReader::new(fs::File::open(bundle).unwrap());
    crate::verify_portable_stream(&mut reader, None, limits, None)
}

/// Stream `bundle` through a fresh scratch directory and require that nothing
/// is left in it, whether the verdict is a report or a refusal.
fn stream_in_scratch(
    bundle: &Path,
    limits: PortableLimits,
) -> Result<crate::PortableReport, PortableError> {
    let scratch = tempfile::tempdir().unwrap();
    let mut reader = std::io::BufReader::new(fs::File::open(bundle).unwrap());
    let verdict = crate::verify_portable_stream(&mut reader, Some(scratch.path()), limits, None);
    assert_eq!(
        fs::read_dir(scratch.path()).unwrap().count(),
        0,
        "the stream left files in its scratch directory"
    );
    verdict
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
            if package == &bundle {
                // The same bytes from a reader that cannot be read back are
                // walked as they stream past, with the same refusal.
                let streamed = stream(&bundle, PortableLimits::default()).unwrap_err();
                assert_eq!(streamed.code, error.code, "{name}: {streamed}");
                assert_eq!(streamed.entry, error.entry, "{name}");
                assert_eq!(streamed.to_string(), error.to_string(), "{name}");
            }
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
    let file = verify(&bundle).unwrap();
    let streamed = stream(&bundle, PortableLimits::default()).unwrap();
    assert_eq!(streamed.source, crate::PortableSource::Stream);
    assert_eq!(streamed.package_digest, file.package_digest);
    assert_eq!(streamed.payload_bytes, file.payload_bytes);
}

#[test]
fn a_parquet_entry_over_the_in_memory_threshold_gets_the_file_verdict_from_a_stream() {
    let funded = hostile::string_file(Encoding::PLAIN, WriterVersion::PARQUET_1_0, 64);
    let unfunded = hostile::string_file(
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        WriterVersion::PARQUET_1_0,
        64,
    );
    // A threshold below both entries sends them through the scratch file.
    let spilling = PortableLimits {
        max_parquet_walk_bytes: 256,
        ..PortableLimits::default()
    };
    assert!(funded.len() as u64 > spilling.max_parquet_walk_bytes);
    assert!(unfunded.len() as u64 > spilling.max_parquet_walk_bytes);
    for (bytes, valid) in [(funded, true), (unfunded, false)] {
        let (_project, generation) = generation_with_graph_file(&bytes);
        let plan = plan_complete_portable(&generation, PortableExportLimits::default()).unwrap();
        let outputs = tempfile::tempdir().unwrap();
        let (_, bundle) = write_test_representations(&plan, outputs.path());
        let file = verify_with(&bundle, spilling);
        let spilled = stream_in_scratch(&bundle, spilling);
        let in_memory = stream_in_scratch(&bundle, PortableLimits::default());
        if valid {
            let (file, spilled, in_memory) = (file.unwrap(), spilled.unwrap(), in_memory.unwrap());
            for report in [&spilled, &in_memory] {
                assert_eq!(report.source, crate::PortableSource::Stream);
                assert_eq!(report.package_digest, file.package_digest);
                assert_eq!(report.payload_bytes, file.payload_bytes);
                assert_eq!(report.transport_digest, file.transport_digest);
            }
        } else {
            let (file, spilled, in_memory) = (
                file.unwrap_err(),
                spilled.unwrap_err(),
                in_memory.unwrap_err(),
            );
            assert_eq!(file.code, PortableErrorCode::LimitExceeded);
            for error in [&spilled, &in_memory] {
                assert_eq!(error.code, file.code);
                assert_eq!(error.entry.as_deref(), Some(GRAPH_FILE));
                assert_eq!(error.to_string(), file.to_string());
            }
        }
    }
}
