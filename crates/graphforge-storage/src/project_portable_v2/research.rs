//! Research component semantic admission before any destination publication.
use super::{PortableV2Error, PortableV2ErrorCode, PortableV2Limits};
use crate::research_versions::{interchange::portable, ResearchRegistry};
use std::{collections::BTreeMap, path::Path, sync::atomic::AtomicBool};

const PREFIX: &str = "data/components/research/research-content/";

type ResearchArchive = (ResearchRegistry, BTreeMap<String, u64>);

pub(crate) fn validate_stage(
    stage: &Path,
    report: &graphforge_core::portable::PortableV2Report,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<Option<ResearchArchive>, PortableV2Error> {
    validate_with_reader(report, limits, cancelled, |path, bound| {
        super::read_bounded_file(&stage.join(path), bound, path)
    })
}

pub(super) fn validate_with_reader(
    report: &graphforge_core::portable::PortableV2Report,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
    mut read: impl FnMut(&str, u64) -> Result<Vec<u8>, PortableV2Error>,
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
    let bytes = read(
        &descriptor.path,
        crate::research_versions::MAX_REGISTRY_BYTES as u64,
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
    let required = portable::portable_objects_admitted(
        &registry,
        |digest| {
            provided.get(digest).copied().ok_or_else(|| {
                graphforge_core::GfError::Validation("research object is missing".into())
            })
        },
        |digest, bound| {
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
            read(
                &format!("{PREFIX}{digest}"),
                bound.min(limits.max_entry_bytes),
            )
            .map_err(|_| graphforge_core::GfError::Validation("research object unavailable".into()))
        },
    )
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

pub(crate) fn install_captured_with_lease(
    stage: &Path,
    lease: &crate::GraphObjectPublicationLease,
    objects: &BTreeMap<String, u64>,
    captures: &BTreeMap<String, super::MaterializedCapture>,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableV2Error> {
    for (digest, length) in objects {
        super::check_cancel(cancelled)?;
        let relative = format!("{PREFIX}{digest}");
        let capture = captures.get(&relative).ok_or_else(invalid)?;
        let source = capture.open_source(&stage.join(relative))?;
        if source.content_sha256() != digest || source.bytes() != *length {
            return Err(invalid());
        }
        crate::graph_object_store::install_captured_portable_source_with_lease(
            lease,
            &source,
            &mut || cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed)),
        )
        .map_err(|_| {
            super::check_cancel(cancelled).err().unwrap_or_else(|| {
                PortableV2Error::new(
                    PortableV2ErrorCode::ConcurrentMutation,
                    "captured research object installation refused",
                )
            })
        })?;
    }
    Ok(())
}

fn invalid() -> PortableV2Error {
    PortableV2Error::new(
        PortableV2ErrorCode::Incompatible,
        "research component identity, schema, provenance or selected closure is invalid",
    )
}
