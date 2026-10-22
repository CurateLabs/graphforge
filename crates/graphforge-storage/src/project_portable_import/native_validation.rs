//! Native owner validation occurs only in private directories, before admission.
use super::{
    PortableError, PortableErrorCode, PortableImportProgress, PortableImportReceipt,
    PortableLimits, ProjectCapability, Uuid,
};
use crate::{
    ResolvedProjectGeneration,
    research_versions::{ResearchRegistry, ResearchVersionRecord},
};
use std::{collections::BTreeMap, path::Path, sync::atomic::AtomicBool};

/// Native domain owner callback; the temporary generation must not escape the call.
pub type NativeResearchValidator<'a> = dyn FnMut(
        &ResolvedProjectGeneration,
        &ResearchVersionRecord,
        &ResearchRegistry,
    ) -> Result<(), graphforge_core::GfError>
    + 'a;

/// Verify native historical content without admitting or mutating any destination.
///
/// The package is read once, in canonical order, and each research Version is
/// staged in a private directory created inside `scratch` (the system
/// temporary directory when `scratch` is `None`), validated and removed before
/// the next one is read, so the disk it needs is bounded by one Version
/// (`PortableLimits::max_research_version_bytes`), not by the package. A caller
/// that knows where the package will be imported passes a directory on that
/// filesystem, so verification needs no space anywhere else. `source` is a
/// bundle file or an expanded directory.
pub fn validate_research_package(
    source: &Path,
    scratch: Option<&Path>,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    validator: &mut NativeResearchValidator<'_>,
) -> Result<graphforge_core::portable::PortableReport, PortableError> {
    crate::project_portable::research::stream::validate_research_path(
        source,
        scratch,
        limits,
        cancelled,
        Some(validator),
    )
}

/// [`validate_research_package`] for a canonical bundle read once from a
/// sequential reader. The verdict equals [`validate_research_package`] on the
/// same bytes in a file, and the report's `source` is `stream`. A package
/// without research Versions writes nothing.
pub fn validate_research_package_stream(
    source: &mut dyn std::io::Read,
    scratch: Option<&Path>,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    validator: &mut NativeResearchValidator<'_>,
) -> Result<graphforge_core::portable::PortableReport, PortableError> {
    crate::project_portable::research::stream::scan_stream(
        source,
        limits,
        cancelled,
        scratch,
        Some(validator),
    )
}

/// Validate every Version of a package staged whole, as an import does.
pub(super) fn validate(
    stage: &Path,
    registry: &ResearchRegistry,
    objects: &crate::project_portable::research::Objects,
    captures: &BTreeMap<String, crate::project_portable::MaterializedCapture>,
    cancelled: Option<&AtomicBool>,
    validator: Option<&mut NativeResearchValidator<'_>>,
) -> Result<(), PortableError> {
    let validator = validator.ok_or_else(invalid)?;
    for (id, version) in &registry.versions {
        check_cancel(cancelled)?;
        let version_objects: crate::project_portable::research::Objects = objects
            .range((*id, String::new())..)
            .take_while(|((owner, _), _)| owner == id)
            .map(|(key, held)| (key.clone(), *held))
            .collect();
        validate_version(
            stage,
            registry,
            version,
            &version_objects,
            captures,
            cancelled,
            validator,
        )?;
    }
    check_cancel(cancelled)
}

/// Run the native owner over one Version staged under `stage`.
///
/// The Version's objects are installed into a private object store, and its
/// historical Project is opened there, both beside the stage, on the
/// filesystem the caller already admitted and never in the system temporary
/// directory. Both are removed when this returns. Evidence that only
/// evidence needs is authenticated by the scan and is not read.
pub(crate) fn validate_version(
    stage: &Path,
    registry: &ResearchRegistry,
    version: &ResearchVersionRecord,
    objects: &crate::project_portable::research::Objects,
    captures: &BTreeMap<String, crate::project_portable::MaterializedCapture>,
    cancelled: Option<&AtomicBool>,
    validator: &mut NativeResearchValidator<'_>,
) -> Result<(), PortableError> {
    let scratch = stage.parent().unwrap_or_else(|| Path::new("."));
    let source = private_directory_in(scratch)?;
    crate::open_or_initialize_ephemeral_project(source.path()).map_err(|_| invalid())?;
    let lease = crate::begin_graph_object_publication(source.path()).map_err(|_| invalid())?;
    let needed: crate::project_portable::research::Objects = objects
        .iter()
        .filter(|(_, held)| !held.evidence_only)
        .map(|(key, held)| (key.clone(), *held))
        .collect();
    crate::project_portable::research::install_captured_with_lease(
        stage, &lease, &needed, captures, cancelled,
    )?;
    check_cancel(cancelled)?;
    let target = private_directory_in(scratch)?;
    let generation = crate::research_versions::materialize_prepared_research_version(
        source.path(),
        version,
        target.path(),
    )
    .map_err(|error| classify_native_failure(&error))?;
    validator(&generation, version, registry).map_err(|error| classify_native_failure(&error))?;
    check_cancel(cancelled)
}

/// A native-owner failure over the package's content is the archive's
/// incompatibility; a storage failure keeps the storage class (`Io`).
fn classify_native_failure(error: &graphforge_core::GfError) -> PortableError {
    match error {
        graphforge_core::GfError::Storage(_) => PortableError::new(
            PortableErrorCode::Io,
            "research validation cannot read its storage",
        ),
        _ => invalid(),
    }
}

fn private_directory_in(parent: &Path) -> Result<tempfile::TempDir, PortableError> {
    tempfile::Builder::new()
        .prefix(".graphforge-research-validation-")
        .tempdir_in(parent)
        .map_err(|error| {
            PortableError::new(
                PortableErrorCode::Io,
                "cannot create research validation scratch",
            )
            .with_cause(error.kind().to_string())
        })
}

fn invalid() -> PortableError {
    PortableError::new(
        PortableErrorCode::Incompatible,
        "research archive requires valid native historical domains and commitments",
    )
}

fn check_cancel(cancelled: Option<&AtomicBool>) -> Result<(), PortableError> {
    if cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed)) {
        return Err(PortableError::new(
            PortableErrorCode::Cancelled,
            "research validation cancelled",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PortableExportLimits, PortableOutput};

    /// A real exported research package staged the way the importer stages it
    /// before `validate`: verified, privately materialized with captures, and
    /// re-admitted over the stage by the research semantic validation.
    struct Staged {
        owner: tempfile::TempDir,
        registry: ResearchRegistry,
        objects: crate::project_portable::research::Objects,
        captures: BTreeMap<String, crate::project_portable::MaterializedCapture>,
    }

    impl Staged {
        fn stage(&self) -> std::path::PathBuf {
            self.owner.path().join("materialized")
        }
    }

    fn staged_research_package() -> Staged {
        let package = crate::project_portable::research_tests::package();
        let project = tempfile::tempdir().unwrap();
        // The parent graph generation every real export carries beside research.
        crate::open_or_initialize_project(project.path()).unwrap();
        let tree = tempfile::tempdir().unwrap();
        std::fs::write(tree.path().join("a.parquet"), b"graph-a").unwrap();
        std::fs::create_dir(tree.path().join("properties")).unwrap();
        let property_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            crate::schemas::uuid_field("node_uuid"),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));
        let properties = arrow::record_batch::RecordBatch::try_new(
            property_schema,
            vec![
                std::sync::Arc::new(
                    arrow::array::FixedSizeBinaryArray::try_from_iter(std::iter::once(
                        graphforge_core::uuid::new_v7().into_bytes(),
                    ))
                    .unwrap(),
                ),
                std::sync::Arc::new(arrow::array::StringArray::from(vec!["person"])),
            ],
        )
        .unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(tree.path().join("properties/Person.parquet")).unwrap(),
            properties.schema(),
            None,
        )
        .unwrap();
        writer.write(&properties).unwrap();
        writer.close().unwrap();
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
            panic!("fresh parent generation");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        // Archive the saved-queries object, then republish with the registry
        // participant, as the research exporter plans it.
        crate::graph_object_store::install_graph_object_bytes(
            project.path(),
            &package.object_bytes,
        )
        .unwrap();
        let parent = crate::resolve_project_generation(project.path()).unwrap();
        let mut participants: Vec<crate::ProjectParticipant> = parent
            .participant_snapshots()
            .unwrap()
            .into_iter()
            .map(|snapshot| crate::ProjectParticipant {
                capability_id: snapshot.capability_id,
                capability_version: snapshot.capability_version,
                record_family_id: snapshot.record_family_id,
                record_version: snapshot.record_version,
                encoding: crate::ProjectParticipantEncoding::Json,
                schema_fingerprint: snapshot.schema_fingerprint,
                row_count: snapshot.row_count,
                bytes: snapshot.bytes,
            })
            .collect();
        participants.push(package.registry.participant().unwrap());
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                crate::ProjectCapability {
                    capability_id: "research".into(),
                    capability_version: crate::research_versions::RESEARCH_VERSION,
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
                Some(&parent.graph_tree_root()),
            )
            .unwrap()
        else {
            panic!("archive generation");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        let generation = crate::resolve_project_generation(project.path()).unwrap();
        let plan =
            crate::plan_complete_portable(&generation, PortableExportLimits::default()).unwrap();
        let output = tempfile::tempdir().unwrap();
        let bundle = output.path().join("research.gfpb");
        let cancelled = std::sync::atomic::AtomicBool::new(false);
        crate::export_complete_portable(
            &plan,
            &bundle,
            PortableOutput::Bundle,
            PortableExportLimits::default(),
            &cancelled,
            |_| {},
        )
        .unwrap();
        let owner = tempfile::tempdir().unwrap();
        let stage = owner.path().join("materialized");
        let materialized = crate::project_portable::materialize_verified_portable_observed(
            &bundle,
            &stage,
            PortableLimits::default(),
            None,
            |_, _| Ok(()),
            false,
        )
        .unwrap();
        let (registry, objects) = crate::project_portable::research::validate_stage(
            &stage,
            &materialized.report,
            PortableLimits::default(),
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(registry, package.registry);
        Staged {
            owner,
            registry,
            objects,
            captures: materialized.captures,
        }
    }

    fn validate_with(
        mut validator: impl FnMut(
            &crate::ResolvedProjectGeneration,
            &ResearchVersionRecord,
            &ResearchRegistry,
        ) -> Result<(), graphforge_core::GfError>,
    ) -> Result<(), PortableError> {
        let staged = staged_research_package();
        validate(
            &staged.stage(),
            &staged.registry,
            &staged.objects,
            &staged.captures,
            None,
            Some(&mut validator),
        )
    }

    #[test]
    fn research_without_native_owner_is_refused_before_materialization() {
        let absent = std::path::Path::new("/nonexistent-research-validation-stage");
        let error = validate(
            absent,
            &ResearchRegistry::default(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Incompatible);
    }

    #[test]
    fn the_staged_package_reaches_the_native_owner() {
        validate_with(|_, _, _| Ok(())).unwrap();
    }

    #[test]
    fn a_native_owner_storage_failure_keeps_the_storage_class() {
        let error =
            validate_with(|_, _, _| Err(graphforge_core::GfError::Storage("injected".into())))
                .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Io);
    }

    #[test]
    fn a_native_owner_content_failure_stays_incompatible() {
        let error =
            validate_with(|_, _, _| Err(graphforge_core::GfError::Validation("injected".into())))
                .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Incompatible);
    }
}

/// Import with native owner validation of every archived research Version.
#[allow(clippy::too_many_arguments)]
pub fn import_complete_portable_with_native_validation(
    source: impl AsRef<Path>,
    target: impl AsRef<Path>,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    supported_capabilities: &[ProjectCapability],
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    progress: impl FnMut(PortableImportProgress),
    allocation: Option<&crate::StorageAllocationOperation>,
    validator: &mut NativeResearchValidator<'_>,
) -> Result<PortableImportReceipt, PortableError> {
    super::import_complete_portable_native(
        source,
        target,
        transaction_uuid,
        generation_uuid,
        supported_capabilities,
        limits,
        cancelled,
        progress,
        allocation,
        Some(validator),
        super::space::filesystem_available_space,
    )
}
