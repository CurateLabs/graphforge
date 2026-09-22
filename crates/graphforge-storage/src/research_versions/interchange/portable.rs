//! Closed portable registry admission and exact CAS closure accounting.
use super::super::{BTreeMap, BTreeSet, Digest, GfError, ResearchRegistry, Sha256, hex, invalid};

pub(crate) fn portable_registry(bytes: &[u8]) -> Result<ResearchRegistry, GfError> {
    if bytes.len() > super::super::MAX_REGISTRY_BYTES {
        return Err(invalid("portable research registry exceeds its bound"));
    }
    let registry: ResearchRegistry = serde_json::from_slice(bytes)
        .map_err(|_| invalid("malformed portable research registry"))?;
    registry.validate()?;
    if super::super::json(&registry)? != bytes || registry.interchange.len() != 1 {
        return Err(invalid(
            "research export requires a canonical selected interchange archive",
        ));
    }
    let archive = registry.interchange.values().next().expect("one archive");
    if archive.registry()? != registry {
        return Err(invalid(
            "portable research cannot carry operational heads or unrelated history",
        ));
    }
    Ok(registry)
}

/// Authenticate participant control bytes and derive the exact exported object set.
/// File contents are also covered by the outer verifier's streaming digest checks.
pub(crate) fn portable_objects(
    registry: &ResearchRegistry,
    mut read: impl FnMut(&str, u64) -> Result<Vec<u8>, GfError>,
) -> Result<BTreeMap<String, Option<u64>>, GfError> {
    let mut objects = BTreeMap::new();
    for version in registry.versions.values() {
        for participant in &version.content.participants {
            let digest = hex(&participant.content_sha256);
            let bytes = read(&digest, 256 * 1024 * 1024)?;
            if hex(&Sha256::digest(&bytes).into()) != digest {
                return Err(invalid("portable research participant identity conflicts"));
            }
            if participant.key.capability == "workspace"
                && participant.key.family == "configuration"
            {
                let value = serde_json::from_slice(&bytes)
                    .map_err(|_| invalid("invalid research settings"))?;
                crate::project_portable_v2_selection::validate_setting_value(None, &value)
                    .map_err(|_| {
                        invalid(
                            "secret-bearing or host-specific research settings are not portable",
                        )
                    })?;
            }
            add(&mut objects, digest, Some(bytes.len() as u64))?;
            if participant.key.capability == "graph" && participant.key.family == "files" {
                let files = match crate::graph_files::decode_versioned_graph_files_participant(
                    participant.record_version,
                    &bytes,
                )? {
                    crate::GraphFilesParticipant::V1(inventory) => inventory.files,
                    crate::GraphFilesParticipant::V2(root) => {
                        crate::resolve_graph_manifest(
                            &root,
                            crate::GraphManifestLimits::default(),
                            |digest| {
                                let bytes = read(
                                    digest,
                                    crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                                )?;
                                add(&mut objects, digest.to_owned(), Some(bytes.len() as u64))?;
                                Ok(bytes)
                            },
                        )?
                        .0
                    }
                };
                for file in files {
                    add(&mut objects, file.content_sha256, Some(file.byte_length))?;
                }
            }
        }
        for evidence in &version.content.evidence {
            if let super::super::ResearchEvidenceReference::Local {
                sha256,
                byte_length,
                ..
            } = evidence
            {
                add(&mut objects, hex(sha256), Some(*byte_length))?;
            }
        }
    }
    Ok(objects)
}

fn add(
    objects: &mut BTreeMap<String, Option<u64>>,
    digest: String,
    length: Option<u64>,
) -> Result<(), GfError> {
    if objects
        .insert(digest, length)
        .is_some_and(|old| old != length)
    {
        return Err(invalid("research object has conflicting lengths"));
    }
    Ok(())
}

pub(crate) fn object_inventory(
    generation: &crate::ResolvedProjectGeneration,
) -> Result<BTreeSet<String>, GfError> {
    let snapshot = generation
        .participant_snapshot("research", "registry")?
        .ok_or_else(|| invalid("portable research registry is unavailable"))?;
    let registry = portable_registry(&snapshot.bytes)?;
    let objects = portable_objects(&registry, |digest, limit| {
        crate::read_graph_object_by_digest(generation.container_root(), digest, limit)
    })?;
    for (digest, length) in &objects {
        if let Some(length) = length {
            crate::verify_graph_object(generation.container_root(), digest, *length)?;
        }
    }
    Ok(objects.into_keys().collect())
}
