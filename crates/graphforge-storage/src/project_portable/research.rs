//! Research component semantic admission before any destination publication.
use super::{PortableError, PortableErrorCode, PortableLimits};
use crate::research_versions::{ResearchRegistry, ResearchVersionRecord, interchange::portable};
use std::{collections::BTreeMap, path::Path, sync::atomic::AtomicBool};
use uuid::Uuid;

pub(crate) mod stream;

/// Every file of the research component lives here. Research validation reads
/// no other path, so a bundle read in one pass stages exactly this prefix.
pub(super) const NAMESPACE: &str = "data/components/research/";
/// Objects a Version's participants need (and evidence that is also one of
/// them) sit under `research-retained/<version>/<sha256>`. A reader stages them.
const RETAINED: &str = "data/components/research/research-retained/";
/// Objects only a Version's local evidence needs sit under
/// `research-retained-evidence/<version>/<sha256>`. Native validation never
/// reads their bytes, so a reader authenticates them as they stream and never
/// stages them; the path says so before the Version's closure is known. The
/// directory sorts after the registry and before `research-retained/`
/// (`-` < `/`), so the registry is still the first research entry.
const EVIDENCE: &str = "data/components/research/research-retained-evidence/";

/// True for a path whose bytes only evidence needs. No decoder reads them, so
/// nothing walks their Parquet pages.
pub(super) fn is_evidence_only(path: &str) -> bool {
    path.starts_with(EVIDENCE)
}

/// One retained object of a Version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Held {
    pub(crate) length: u64,
    /// The object lives under `research-retained-evidence/`: only evidence needs it.
    pub(crate) evidence_only: bool,
}

/// Every retained object, keyed by the owning Version and digest.
pub(crate) type Objects = BTreeMap<(Uuid, String), Held>;

/// The registry and every retained object.
type ResearchArchive = (ResearchRegistry, Objects);

pub(crate) fn object_path(version: &Uuid, digest: &str, evidence_only: bool) -> String {
    let prefix = if evidence_only { EVIDENCE } else { RETAINED };
    format!("{prefix}{version}/{digest}")
}

/// Inverse of [`object_path`]; only the canonical lowercase spelling is accepted.
fn object_key(path: &str) -> Option<(Uuid, String, bool)> {
    [(RETAINED, false), (EVIDENCE, true)]
        .into_iter()
        .find_map(|(prefix, evidence_only)| {
            let (version, digest) = path.strip_prefix(prefix)?.split_once('/')?;
            let version: Uuid = version.parse().ok()?;
            (object_path(&version, digest, evidence_only) == path)
                .then(|| (version, digest.to_owned(), evidence_only))
        })
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
    let bytes = read(
        &registry_path(report)?,
        crate::research_versions::MAX_REGISTRY_BYTES as u64,
    )?;
    let registry = portable::portable_registry(&bytes).map_err(|_| invalid())?;
    let provided = provided_by_report(report, &registry)?;
    for (id, version) in &registry.versions {
        check_version(id, version, &provided, limits, cancelled, &mut read)?;
    }
    super::check_cancel(cancelled)?;
    Ok(Some((registry, provided)))
}

fn registry_id() -> String {
    crate::project_portable_export::planning::portable_participant_id("research", "registry")
}

/// The registry's path, or a refusal unless the manifest names exactly one
/// registry file and it is in its fixed place, which precedes every retained
/// object in canonical order whatever else the package holds.
fn registry_path(
    report: &graphforge_core::portable::PortableReport,
) -> Result<String, PortableError> {
    let registry_id = registry_id();
    let mut descriptors = report
        .research_entries
        .iter()
        .filter(|entry| entry.component_id == registry_id);
    let path = format!("{NAMESPACE}{registry_id}/participant.json");
    match (descriptors.next(), descriptors.next()) {
        (Some(descriptor), None) if descriptor.path == path => Ok(path),
        _ => Err(invalid()),
    }
}

/// Every retained object the manifest names, keyed by owning Version; each must
/// sit in a registry Version's directory under its digest, once.
fn provided_by_report(
    report: &graphforge_core::portable::PortableReport,
    registry: &ResearchRegistry,
) -> Result<Objects, PortableError> {
    let registry_id = registry_id();
    let mut provided = Objects::new();
    for file in report
        .research_entries
        .iter()
        .filter(|entry| entry.component_id != registry_id)
    {
        let (version, digest, evidence_only) = object_key(&file.path).ok_or_else(invalid)?;
        let held = Held {
            length: file.length,
            evidence_only,
        };
        if file.component_id != "research-content"
            || digest != file.sha256
            || !registry.versions.contains_key(&version)
            || provided.insert((version, digest), held).is_some()
        {
            return Err(invalid());
        }
    }
    Ok(provided)
}

/// One Version's closure against the objects `provided` for it: every object it
/// needs is present with its exact length and nothing else is carried. An object
/// under `research-retained-evidence/` must be evidence and no participant, graph file or
/// manifest node. Every Version carries its own complete closure; a shared
/// object appears once per Version.
fn check_version(
    id: &Uuid,
    version: &ResearchVersionRecord,
    provided: &Objects,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    read: &mut impl FnMut(&str, u64) -> Result<Vec<u8>, PortableError>,
) -> Result<(), PortableError> {
    let length_of = |digest: &str| {
        provided
            .get(&(*id, digest.to_owned()))
            .map(|held| held.length)
            .ok_or_else(|| {
                graphforge_core::GfError::Validation("research object is missing".into())
            })
    };
    let core = portable::core_objects_admitted(version, length_of, |digest, bound| {
        super::check_cancel(cancelled).map_err(|_| {
            graphforge_core::GfError::Validation("research verification cancelled".into())
        })?;
        if provided
            .get(&(*id, digest.to_owned()))
            .is_none_or(|held| held.length > bound.min(limits.max_entry_bytes))
        {
            return Err(graphforge_core::GfError::Validation(
                "research object is missing or oversized".into(),
            ));
        }
        read(
            &object_path(id, digest, false),
            bound.min(limits.max_entry_bytes),
        )
        .map_err(|_| graphforge_core::GfError::Validation("research object unavailable".into()))
    })
    .map_err(|_| super::check_cancel(cancelled).err().unwrap_or_else(invalid))?;
    let evidence = portable::local_evidence(version);
    let mut required: BTreeMap<&String, Option<u64>> = core.iter().map(|(d, l)| (d, *l)).collect();
    for (digest, length) in &evidence {
        if required
            .insert(digest, Some(*length))
            .flatten()
            .is_some_and(|old| old != *length)
        {
            return Err(invalid());
        }
    }
    let mut held = 0;
    for ((owner, digest), object) in provided.range((*id, String::new())..) {
        if owner != id {
            break;
        }
        held += 1;
        let placed =
            !object.evidence_only || (evidence.contains_key(digest) && !core.contains_key(digest));
        if !placed
            || required
                .get(digest)
                .is_none_or(|length| length.is_some_and(|l| l != object.length))
        {
            return Err(invalid());
        }
    }
    if held != required.len() {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn install_captured_with_lease(
    stage: &Path,
    lease: &crate::GraphObjectPublicationLease,
    objects: &Objects,
    captures: &BTreeMap<String, super::MaterializedCapture>,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableError> {
    // A shared object is carried once per Version but installed once.
    let mut installed = std::collections::BTreeSet::new();
    for ((version, digest), held) in objects {
        super::check_cancel(cancelled)?;
        if !installed.insert(digest) {
            continue;
        }
        let relative = object_path(version, digest, held.evidence_only);
        let capture = captures.get(&relative).ok_or_else(invalid)?;
        let source = capture.open_source(&stage.join(relative))?;
        if source.content_sha256() != digest || source.bytes() != held.length {
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
