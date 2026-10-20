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
/// The package is materialized in a private directory created inside
/// `scratch`, or in the system temporary directory when `scratch` is `None`.
/// A caller that knows where the package will be imported passes a directory
/// on that filesystem, so verification needs no space anywhere else.
pub fn validate_research_package(
    source: &Path,
    scratch: Option<&Path>,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    validator: &mut NativeResearchValidator<'_>,
) -> Result<graphforge_core::portable::PortableReport, PortableError> {
    let owner = match scratch {
        Some(directory) => private_directory_in(directory)?,
        None => private_directory_in(&std::env::temp_dir())?,
    };
    let stage = owner.path().join("verified");
    let materialized = crate::project_portable::materialize_verified_portable_observed(
        source,
        &stage,
        limits,
        cancelled,
        |_, _| Ok(()),
        false,
    )?;
    let report = materialized.report;
    if let Some((registry, objects)) =
        crate::project_portable::research::validate_stage(&stage, &report, limits, cancelled)?
    {
        validate(
            &stage,
            &registry,
            &objects,
            &materialized.captures,
            cancelled,
            Some(validator),
        )?;
    }
    Ok(report)
}

/// [`validate_research_package`] for a canonical bundle read once from a
/// sequential reader.
///
/// The scan stages only the research files, `data/components/research/**`,
/// because they are all native validation reads; the other components are
/// authenticated and discarded. A package without research files writes
/// nothing. The verdict equals [`validate_research_package`] on the same bytes
/// in a file, and the report's `source` is `stream`.
pub fn validate_research_package_stream(
    source: &mut dyn std::io::Read,
    scratch: Option<&Path>,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    validator: &mut NativeResearchValidator<'_>,
) -> Result<graphforge_core::portable::PortableReport, PortableError> {
    let scan = crate::project_portable::scan_stream(source, limits, cancelled, scratch)?;
    let report = scan.report;
    if report.research_interchange {
        let (stage, captures) = scan.spill.authenticated(cancelled)?;
        if let Some((registry, objects)) =
            crate::project_portable::research::validate_stage(stage, &report, limits, cancelled)?
        {
            validate(
                stage,
                &registry,
                &objects,
                captures,
                cancelled,
                Some(validator),
            )?;
        }
    }
    Ok(report)
}

pub(super) fn validate(
    stage: &Path,
    registry: &ResearchRegistry,
    objects: &BTreeMap<(Uuid, String), u64>,
    captures: &BTreeMap<String, crate::project_portable::MaterializedCapture>,
    cancelled: Option<&AtomicBool>,
    validator: Option<&mut NativeResearchValidator<'_>>,
) -> Result<(), PortableError> {
    let validator = validator.ok_or_else(invalid)?;
    // Historical views live beside the stage, on the filesystem the import
    // already admitted, never in the system temporary directory.
    let scratch = stage.parent().unwrap_or_else(|| Path::new("."));
    let source = private_directory_in(scratch)?;
    crate::open_or_initialize_ephemeral_project(source.path()).map_err(|_| invalid())?;
    let lease = crate::begin_graph_object_publication(source.path()).map_err(|_| invalid())?;
    crate::project_portable::research::install_captured_with_lease(
        stage, &lease, objects, captures, cancelled,
    )?;
    for version in registry.versions.values() {
        check_cancel(cancelled)?;
        let target = private_directory_in(scratch)?;
        let generation = crate::research_versions::materialize_prepared_research_version(
            source.path(),
            version,
            target.path(),
        )
        .map_err(|_| invalid())?;
        validator(&generation, version, registry).map_err(|_| invalid())?;
    }
    check_cancel(cancelled)
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
    )
}
