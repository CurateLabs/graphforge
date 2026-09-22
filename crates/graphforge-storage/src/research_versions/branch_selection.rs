//! Prepare selected immutable Branch state without publishing an intermediate head.
use super::{
    BTreeSet, Digest, GfError, Path, PreparedBranchContent, RegisterResearchVersion,
    ResearchGraphSelection, ResearchParticipantCommitment, ResearchParticipantKey,
    ResearchVersionRecord, Sha256, cancelled, history, invalid, projection, read_research_registry,
    retained_content,
};
use std::sync::atomic::AtomicBool;

/// Prepare a whole retained Version or an owner-selected graph/domain closure.
/// `spec.selection` names unchanged participants; replacement participants must
/// be produced and validated by domain owners. This operation publishes no head.
pub fn prepare_branch_selection(
    root: &Path,
    spec: &RegisterResearchVersion,
    current_origin: Option<&RegisterResearchVersion>,
    graph: Option<&ResearchGraphSelection>,
    replacements: &[crate::ProjectParticipant],
    cancellation: &AtomicBool,
) -> Result<PreparedBranchContent, GfError> {
    cancelled(cancellation)?;
    let current = crate::resolve_project_generation(root)?;
    let mut registry = read_research_registry(&current)?;
    if let Some(origin) = current_origin {
        super::branches::stage_origin(root, &mut registry, origin)?;
    }
    let origin_id = spec
        .source_version
        .ok_or_else(|| invalid("Branch source Version is required"))?;
    let origin = registry
        .versions
        .get(&origin_id)
        .cloned()
        .ok_or_else(|| invalid("Branch source Version is unavailable"))?;
    if origin.content.generation_uuid != spec.source_generation_uuid {
        return Err(invalid("Branch source generation differs from its Version"));
    }
    let lease = crate::begin_graph_object_publication(root)?;
    let mut version = if let Some(graph) = graph {
        let id = projection::register(root, &mut registry, spec, graph)?;
        registry.versions.remove(&id).expect("prepared projection")
    } else {
        if spec.selection.is_some()
            || !replacements.is_empty()
            || spec.evidence != origin.content.evidence
        {
            return Err(invalid(
                "whole Branch preparation requires complete Version context",
            ));
        }
        retained_content::compact(root, &mut registry, &BTreeSet::from([origin_id]))?;
        let mut content = origin.content;
        content.source_version = Some(origin_id);
        content
            .required_versions
            .clone_from(&spec.required_versions);
        ResearchVersionRecord {
            version_uuid: spec.version_uuid,
            context_uuid: spec.context_uuid,
            label: spec.label.clone(),
            description: spec.description.clone(),
            created_at: spec.created_at,
            content,
        }
    };
    replace_domains(root, &mut version, replacements, cancellation)?;
    retained_content::inspect(root, &version, None)?;
    cancelled(cancellation)?;
    Ok(PreparedBranchContent {
        version,
        _lease: lease,
    })
}

/// Replace domain-owner commitments on an already projected graph, preserving
/// its immutable graph CAS and publication lease. Never publishes CURRENT.
pub fn replace_prepared_branch_domains(
    root: &Path,
    prepared: &mut PreparedBranchContent,
    keep: &BTreeSet<ResearchParticipantKey>,
    replacements: &[crate::ProjectParticipant],
    cancellation: &AtomicBool,
) -> Result<(), GfError> {
    cancelled(cancellation)?;
    prepared
        .version
        .content
        .participants
        .retain(|p| p.key.capability == "graph" || keep.contains(&p.key));
    replace_domains(root, &mut prepared.version, replacements, cancellation)?;
    retained_content::inspect(root, &prepared.version, None)?;
    cancelled(cancellation)
}

fn replace_domains(
    root: &Path,
    version: &mut ResearchVersionRecord,
    replacements: &[crate::ProjectParticipant],
    cancellation: &AtomicBool,
) -> Result<(), GfError> {
    let mut keys = BTreeSet::new();
    let mut bytes = 0_usize;
    for participant in replacements {
        cancelled(cancellation)?;
        let key = ResearchParticipantKey {
            capability: participant.capability_id.clone(),
            family: participant.record_family_id.clone(),
        };
        if history(&key) || key.capability == "graph" || !keys.insert(key.clone()) {
            return Err(invalid(
                "Branch domain replacement contains history, graph or duplicate records",
            ));
        }
        bytes = bytes
            .checked_add(participant.bytes.len())
            .ok_or_else(|| invalid("Branch domain bytes overflow"))?;
        if bytes > 256 * 1024 * 1024 {
            return Err(super::error(
                super::ProjectErrorCode::ResourceLimit,
                "Branch domain replacement byte limit exceeded",
            ));
        }
        crate::graph_object_store::install_graph_object_bytes(root, &participant.bytes)?;
        version.content.participants.retain(|p| p.key != key);
        version
            .content
            .participants
            .push(ResearchParticipantCommitment {
                key,
                capability_version: participant.capability_version,
                record_version: participant.record_version,
                encoding: match participant.encoding {
                    crate::ProjectParticipantEncoding::Json => "json",
                    crate::ProjectParticipantEncoding::Parquet => "parquet",
                    crate::ProjectParticipantEncoding::Arrow => "arrow",
                }
                .into(),
                schema_sha256: participant.schema_fingerprint,
                row_count: participant.row_count,
                content_sha256: Sha256::digest(&participant.bytes).into(),
            });
    }
    version
        .content
        .participants
        .sort_by(|a, b| a.key.cmp(&b.key));
    Ok(())
}
