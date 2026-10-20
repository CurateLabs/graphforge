//! Research component semantic admission before any destination publication.
use super::{PortableError, PortableErrorCode, PortableLimits};
use crate::research_versions::{ResearchRegistry, interchange::portable};
use std::{collections::BTreeMap, path::Path, sync::atomic::AtomicBool};
use uuid::Uuid;

const PREFIX: &str = "data/components/research/research-retained/";

/// The registry and every retained object with its length, keyed by the owning Version.
type ResearchArchive = (ResearchRegistry, BTreeMap<(Uuid, String), u64>);

fn retained_path(version: &Uuid, digest: &str) -> String {
    format!("{PREFIX}{version}/{digest}")
}

/// Inverse of `retained_path`; only the canonical lowercase spelling is accepted.
fn retained_key(path: &str) -> Option<(Uuid, String)> {
    let (version, digest) = path.strip_prefix(PREFIX)?.split_once('/')?;
    let version: Uuid = version.parse().ok()?;
    (retained_path(&version, digest) == path).then(|| (version, digest.to_owned()))
}

pub(crate) fn validate_stage(
    stage: &Path,
    report: &graphforge_core::portable::PortableReport,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<Option<ResearchArchive>, PortableError> {
    validate_with_reader(report, limits, cancelled, |path, bound| {
        super::read_bounded_file(&stage.join(path), bound, path)
    })
}

pub(super) fn validate_with_reader(
    report: &graphforge_core::portable::PortableReport,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    mut read: impl FnMut(&str, u64) -> Result<Vec<u8>, PortableError>,
) -> Result<Option<ResearchArchive>, PortableError> {
    if !report.research_interchange {
        return Ok(None);
    }
    let registry_id =
        crate::project_portable_export::planning::portable_participant_id("research", "registry");
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
    let mut provided: BTreeMap<(Uuid, String), u64> = BTreeMap::new();
    for file in report
        .research_entries
        .iter()
        .filter(|entry| entry.component_id != registry_id)
    {
        let key = retained_key(&file.path).ok_or_else(invalid)?;
        if file.component_id != "research-content"
            || key.1 != file.sha256
            || !registry.versions.contains_key(&key.0)
            || provided.insert(key, file.length).is_some()
        {
            return Err(invalid());
        }
    }
    // Every Version carries its own complete closure; a shared object appears once per Version.
    for (id, version) in &registry.versions {
        let length_of = |digest: &str| {
            provided
                .get(&(*id, digest.to_owned()))
                .copied()
                .ok_or_else(|| {
                    graphforge_core::GfError::Validation("research object is missing".into())
                })
        };
        let required = portable::version_objects_admitted(version, length_of, |digest, bound| {
            super::check_cancel(cancelled).map_err(|_| {
                graphforge_core::GfError::Validation("research verification cancelled".into())
            })?;
            if provided
                .get(&(*id, digest.to_owned()))
                .is_none_or(|length| *length > bound.min(limits.max_entry_bytes))
            {
                return Err(graphforge_core::GfError::Validation(
                    "research object is missing or oversized".into(),
                ));
            }
            read(
                &retained_path(id, digest),
                bound.min(limits.max_entry_bytes),
            )
            .map_err(|_| graphforge_core::GfError::Validation("research object unavailable".into()))
        })
        .map_err(|_| super::check_cancel(cancelled).err().unwrap_or_else(invalid))?;
        let held = provided
            .range((*id, String::new())..)
            .take_while(|((v, _), _)| v == id);
        if held.clone().count() != required.len()
            || required.iter().any(|(digest, length)| {
                provided
                    .get(&(*id, digest.clone()))
                    .is_none_or(|actual| length.is_some_and(|expected| expected != *actual))
            })
        {
            return Err(invalid());
        }
    }
    super::check_cancel(cancelled)?;
    Ok(Some((registry, provided)))
}

pub(crate) fn install_captured_with_lease(
    stage: &Path,
    lease: &crate::GraphObjectPublicationLease,
    objects: &BTreeMap<(Uuid, String), u64>,
    captures: &BTreeMap<String, super::MaterializedCapture>,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableError> {
    // A shared object is carried once per Version but installed once.
    let mut installed = std::collections::BTreeSet::new();
    for ((version, digest), length) in objects {
        super::check_cancel(cancelled)?;
        if !installed.insert(digest) {
            continue;
        }
        let relative = retained_path(version, digest);
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
                PortableError::new(
                    PortableErrorCode::ConcurrentMutation,
                    "captured research object installation refused",
                )
            })
        })?;
    }
    Ok(())
}

fn invalid() -> PortableError {
    PortableError::new(
        PortableErrorCode::Incompatible,
        "research component identity, schema, provenance or selected closure is invalid",
    )
}
