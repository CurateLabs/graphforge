//! Native owner validation occurs only in private directories, before admission.
use super::{
    PortableV2Error, PortableV2ErrorCode, PortableV2ImportProgress, PortableV2ImportReceipt,
    PortableV2Limits, ProjectCapability, Uuid,
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
pub fn validate_research_package(
    source: &Path,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
    validator: &mut NativeResearchValidator<'_>,
) -> Result<graphforge_core::portable::PortableV2Report, PortableV2Error> {
    let owner = tempfile::tempdir().map_err(|_| invalid())?;
    let stage = owner.path().join("verified");
    let report = crate::project_portable_v2::materialize_verified_portable_v2(
        source, &stage, limits, cancelled,
    )?;
    if let Some((registry, objects)) =
        crate::project_portable_v2::research::validate_stage(&stage, &report, limits, cancelled)?
    {
        validate(&stage, &registry, &objects, cancelled, Some(validator))?;
    }
    Ok(report)
}

pub(super) fn validate(
    stage: &Path,
    registry: &ResearchRegistry,
    objects: &BTreeMap<String, u64>,
    cancelled: Option<&AtomicBool>,
    validator: Option<&mut NativeResearchValidator<'_>>,
) -> Result<(), PortableV2Error> {
    let validator = validator.ok_or_else(invalid)?;
    let source = tempfile::tempdir().map_err(|_| invalid())?;
    crate::open_or_initialize_ephemeral_project(source.path()).map_err(|_| invalid())?;
    let _lease = crate::begin_graph_object_publication(source.path()).map_err(|_| invalid())?;
    crate::project_portable_v2::research::install(stage, source.path(), objects)?;
    for version in registry.versions.values() {
        check_cancel(cancelled)?;
        let target = tempfile::tempdir().map_err(|_| invalid())?;
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

fn invalid() -> PortableV2Error {
    PortableV2Error::new(
        PortableV2ErrorCode::Incompatible,
        "research archive requires valid native historical domains and commitments",
    )
}

fn check_cancel(cancelled: Option<&AtomicBool>) -> Result<(), PortableV2Error> {
    if cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed)) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Cancelled,
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
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
    }
}

/// Import with native owner validation of every archived research Version.
#[allow(clippy::too_many_arguments)]
pub fn import_complete_portable_v2_with_native_validation(
    source: impl AsRef<Path>,
    target: impl AsRef<Path>,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    supported_capabilities: &[ProjectCapability],
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
    progress: impl FnMut(PortableV2ImportProgress),
    allocation: Option<&crate::StorageAllocationOperation>,
    validator: &mut NativeResearchValidator<'_>,
) -> Result<PortableV2ImportReceipt, PortableV2Error> {
    super::import_complete_portable_v2_native(
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
