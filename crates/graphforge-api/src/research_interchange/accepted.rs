//! Preserve exact selected acceptance mappings with independently narrowed proof roots.
use crate::branches::{baseline, fields};
use crate::{CancellationToken, GfError, GraphForge, ResearchFieldIdentity};
use graphforge_storage::research_versions::{
    PreparedResearchContent, ResearchAcceptedMapping, ResearchRegistry,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub(super) struct Proofs {
    pub accepted: BTreeMap<Uuid, ResearchAcceptedMapping>,
    pub exports: BTreeMap<Uuid, Uuid>,
    pub prepared: Vec<PreparedResearchContent>,
}

pub(super) fn prepare(
    owner: &GraphForge,
    registry: &ResearchRegistry,
    selected_uuid: Uuid,
    selected: &GraphForge,
    cancel: &CancellationToken,
) -> Result<Proofs, GfError> {
    let accepted = selected_mappings(registry, selected, cancel)?;
    let mut grouped: BTreeMap<Uuid, Vec<_>> = BTreeMap::new();
    for mapping in accepted.values() {
        grouped
            .entry(mapping.proof_version_uuid)
            .or_default()
            .push(mapping);
    }
    let mut exports = BTreeMap::new();
    let mut prepared = Vec::new();
    for (original, mappings) in grouped {
        cancel.checkpoint()?;
        let available = if registry.versions.contains_key(&original) {
            original
        } else {
            registry
                .interchange
                .values()
                .find_map(|a| a.proof_exports.get(&original).copied())
                .filter(|id| registry.versions.contains_key(id))
                .ok_or_else(|| {
                    GfError::Validation("selected acceptance proof is unavailable".into())
                })?
        };
        let version = &registry.versions[&available];
        let graph = crate::research_versions::materialize_version(owner, version)?;
        let source_fields = fields::read(&graph, cancel)?;
        let units: BTreeSet<_> = mappings
            .iter()
            .map(|m| {
                (
                    m.unit.object_kind.clone(),
                    m.unit.object_uuid,
                    m.unit.field.clone(),
                )
            })
            .collect();
        let members = proof_members(&units, &source_fields);
        let identity = proof_identity(selected_uuid, original, &units)?;
        let frozen = owner.freeze_slice(
            &crate::SliceRequest {
                request_uuid: identity,
                source: crate::SliceSource::Version {
                    version_uuid: available,
                },
                selector: crate::SliceSelector::Direct { members },
                include: crate::SliceMembers::default(),
                exclude: crate::SliceMembers::default(),
                limits: crate::SliceLimits::default(),
            },
            cancel,
        )?;
        let ipc = encode_slice(frozen)?;
        let reuse = identity == available;
        let (mut proof, _) = super::projection::prepare(
            owner,
            available,
            &super::ResearchExportProjection {
                version_uuid: if reuse { Uuid::now_v7() } else { identity },
                frozen_ipc: ipc,
                fields: units
                    .into_iter()
                    .map(|(object_kind, object_uuid, field)| ResearchFieldIdentity {
                        object_kind,
                        object_uuid,
                        field,
                    })
                    .collect(),
                created_at: version.created_at,
            },
            cancel,
        )?;
        proof.version.content.source_version = Some(original);
        if reuse {
            if version.content.source_version != Some(original)
                || version.content.participants != proof.version.content.participants
                || version.content.evidence != proof.version.content.evidence
                || version.content.required_versions != proof.version.content.required_versions
                || registry.identities.get(&available) != Some(&version.identity_sha256()?)
            {
                return Err(GfError::Validation(
                    "retained acceptance proof differs from its exact selected content closure"
                        .into(),
                ));
            }
            exports.insert(original, available);
        } else {
            exports.insert(original, proof.version.version_uuid);
            prepared.push(proof);
        }
    }
    Ok(Proofs {
        accepted,
        exports,
        prepared,
    })
}

fn selected_mappings(
    registry: &ResearchRegistry,
    selected: &GraphForge,
    cancel: &CancellationToken,
) -> Result<BTreeMap<Uuid, ResearchAcceptedMapping>, GfError> {
    let baseline = baseline::read(selected)?;
    let values = fields::read(selected, cancel)?;
    let mut accepted = BTreeMap::new();
    let all = registry.proposals.accepted.values().chain(
        registry
            .interchange
            .values()
            .flat_map(|a| a.accepted.values()),
    );
    for mapping in all {
        let key = (
            mapping.unit.object_kind.clone(),
            mapping.unit.object_uuid,
            mapping.unit.field.clone(),
        );
        let selected = baseline
            .get(&key)
            .is_some_and(|row| row.contribution == mapping.contribution_uuid)
            || (baseline.is_empty()
                && values.get(&key).copied() == mapping.value_sha256
                && values.contains_key(&key));
        if selected
            && accepted
                .insert(mapping.mapping_uuid, mapping.clone())
                .is_some_and(|old| old != *mapping)
        {
            return Err(GfError::Validation(
                "conflicting accepted research provenance".into(),
            ));
        }
    }
    Ok(accepted)
}

fn encode_slice(frozen: crate::ExecutionResult) -> Result<Vec<u8>, GfError> {
    let mut ipc = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &frozen.schema)
            .map_err(|e| GfError::Validation(e.to_string()))?;
        for batch in frozen.batches {
            writer
                .write(&batch)
                .map_err(|e| GfError::Validation(e.to_string()))?;
        }
        writer
            .finish()
            .map_err(|e| GfError::Validation(e.to_string()))?;
    }
    Ok(ipc)
}

fn proof_members(
    units: &BTreeSet<fields::Key>,
    source_fields: &fields::Fields,
) -> crate::SliceMembers {
    let mut members = crate::SliceMembers::default();
    for (kind, id, _) in units {
        if !source_fields.contains_key(&(kind.clone(), *id, "$object".into())) {
            continue;
        }
        match kind.as_str() {
            "node" => &mut members.nodes,
            "edge" => &mut members.edges,
            "assertion" => &mut members.assertions,
            "source" => &mut members.sources,
            "artifact" => &mut members.artifacts,
            _ => continue,
        }
        .insert(*id);
    }
    members
}

fn proof_identity(
    selected_uuid: Uuid,
    original: Uuid,
    units: &BTreeSet<fields::Key>,
) -> Result<Uuid, GfError> {
    let mut hash = Sha256::new();
    hash.update(b"graphforge-interchange-accepted-proof/1");
    hash.update(selected_uuid.as_bytes());
    hash.update(original.as_bytes());
    hash.update(
        serde_json::to_vec(units)
            .map_err(|_| GfError::Validation("invalid selected proof".into()))?,
    );
    Ok(graphforge_core::canonical::uuid_v8(hash.finalize().into()))
}
