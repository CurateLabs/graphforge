//! Research component semantic admission before any destination publication.
use super::{PortableV2Error, PortableV2ErrorCode, PortableV2Limits};
use crate::research_versions::{ResearchRegistry, interchange::portable};
use std::{collections::BTreeMap, path::Path, sync::atomic::AtomicBool};

const PREFIX: &str = "data/components/research/research-content/";

type ResearchArchive = (ResearchRegistry, BTreeMap<String, u64>);

pub(crate) fn validate_stage(
    stage: &Path,
    report: &graphforge_core::portable::PortableV2Report,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<Option<ResearchArchive>, PortableV2Error> {
    if !report.research_interchange {
        return Ok(None);
    }
    let registry_id = crate::project_portable_v2_export::planning::portable_participant_id(
        "research", "registry",
    );
    let descriptors: Vec<_> = report
        .research_entries
        .iter()
        .filter(|entry| entry.component_id == registry_id)
        .collect();
    if descriptors.len() != 1 {
        return Err(invalid());
    }
    let descriptor = descriptors[0];
    let bytes = super::read_bounded_file(
        &stage.join(&descriptor.path),
        crate::research_versions::MAX_REGISTRY_BYTES as u64,
        &descriptor.path,
    )?;
    let registry = portable::portable_registry(&bytes).map_err(|_| invalid())?;
    let mut provided = BTreeMap::new();
    for file in report
        .research_entries
        .iter()
        .filter(|entry| entry.component_id != registry_id)
    {
        if file.component_id != "research-content"
            || file.path != format!("{PREFIX}{}", file.sha256)
            || provided.insert(file.sha256.clone(), file.length).is_some()
        {
            return Err(invalid());
        }
    }
    let required = portable::portable_objects(&registry, |digest, bound| {
        super::check_cancel(cancelled).map_err(|_| {
            graphforge_core::GfError::Validation("research verification cancelled".into())
        })?;
        if provided
            .get(digest)
            .is_none_or(|length| *length > bound.min(limits.max_entry_bytes))
        {
            return Err(graphforge_core::GfError::Validation(
                "research object is missing or oversized".into(),
            ));
        }
        super::read_bounded_file(
            &stage.join(format!("{PREFIX}{digest}")),
            bound.min(limits.max_entry_bytes),
            "research object",
        )
        .map_err(|_| graphforge_core::GfError::Validation("research object unavailable".into()))
    })
    .map_err(|_| super::check_cancel(cancelled).err().unwrap_or_else(invalid))?;
    if required.len() != provided.len()
        || required.iter().any(|(digest, length)| {
            provided
                .get(digest)
                .is_none_or(|actual| length.is_some_and(|expected| expected != *actual))
        })
    {
        return Err(invalid());
    }
    super::check_cancel(cancelled)?;
    Ok(Some((registry, provided)))
}

pub(crate) fn install(
    stage: &Path,
    target: &Path,
    objects: &BTreeMap<String, u64>,
) -> Result<(), PortableV2Error> {
    for (digest, length) in objects {
        crate::graph_object_store::install_graph_object_file(
            target,
            &stage.join(format!("{PREFIX}{digest}")),
            digest,
            *length,
        )
        .map_err(|_| invalid())?;
    }
    Ok(())
}

fn invalid() -> PortableV2Error {
    PortableV2Error::new(
        PortableV2ErrorCode::Incompatible,
        "research component identity, schema, provenance or selected closure is invalid",
    )
}
