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
    .map_err(classify_native_failure)?;
    validator(&generation, version, registry).map_err(classify_native_failure)?;
    check_cancel(cancelled)
}

/// A native-owner failure over the package's content is the archive's
/// incompatibility; a storage failure keeps the storage class (`Io`).
fn classify_native_failure(error: graphforge_core::GfError) -> PortableError {
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
        super::space::filesystem_available_space,
    )
}
