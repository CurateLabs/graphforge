//! Pinned native endpoint materialization and owner-validated field baselines.
use super::{ResearchComparisonAuthority, ResearchComparisonEndpoint};
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::{
        baseline,
        fields::{self, Fields, Objects},
    },
};
use graphforge_storage::{
    ResolvedProjectGeneration,
    research_versions::{ResearchRegistry, ResearchVersionRecord},
};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(super) struct State {
    pub fields: Fields,
    pub baseline: BTreeMap<fields::Key, baseline::Row>,
    pub version: Option<Uuid>,
    pub generation: Uuid,
    pub context: Uuid,
    pub authority_context: (u8, Uuid),
    pub branch: Option<Uuid>,
    pub parent_branch: Option<Uuid>,
    pub missing: Vec<(String, Uuid, String)>,
    pub suppressed: Objects,
}
pub(super) fn load(
    owner: &GraphForge,
    current: &ResolvedProjectGeneration,
    registry: &ResearchRegistry,
    endpoint: &ResearchComparisonEndpoint,
    selected: Option<&Objects>,
    cancel: &CancellationToken,
) -> Result<State, GfError> {
    cancel.checkpoint()?;
    let version = match endpoint {
        ResearchComparisonEndpoint::Project => None,
        ResearchComparisonEndpoint::Version { version_uuid } => Some(
            registry
                .versions
                .get(version_uuid)
                .ok_or_else(unavailable)?,
        ),
        ResearchComparisonEndpoint::Branch { branch_uuid } => Some(
            registry
                .heads
                .get(branch_uuid)
                .and_then(|id| registry.versions.get(id))
                .ok_or_else(unavailable)?,
        ),
    };
    let graph = version.map_or_else(
        || pinned_project(owner, current),
        |v| crate::research_versions::materialize_version(owner, v),
    )?;
    let expanded = selected
        .map(|objects| super::scope::dependencies(&graph, objects, cancel))
        .transpose()?;
    let selected = expanded.as_ref();
    let fields = fields::read_selected(&graph, selected, cancel)?;
    let branch = version.and_then(|v| registry.branches.get(&v.context_uuid));
    let mut baseline = if let Some(branch) = branch {
        crate::branches::baseline_context::read(
            owner,
            &graph,
            branch.branch_uuid,
            version.expect("Branch has a Version").version_uuid,
            registry,
            cancel,
        )?
    } else {
        baseline::read(&graph)?
    };
    if let Some(selected) = selected {
        baseline.retain(|key, _| {
            key.0.starts_with("ontology") || selected.contains(&(key.0.clone(), key.1))
        });
    }
    let suppressed = local_suppression(
        &graph,
        current,
        registry,
        branch.map(|b| b.branch_uuid),
        &fields,
        &baseline,
    )?;
    let missing = dependencies(&graph, registry, version, selected)?;
    Ok(State {
        fields,
        baseline,
        version: version.map(|v| v.version_uuid),
        generation: version.map_or(current.generation_uuid(), |v| v.content.generation_uuid),
        context: version.map_or(
            crate::research_claims::authority::project_uuid(current)?,
            |v| v.context_uuid,
        ),
        authority_context: match (branch, version) {
            (Some(branch), _) => (1, branch.branch_uuid),
            (None, Some(version)) if version.content.source_version.is_some() => {
                (2, version.context_uuid)
            }
            _ => (0, crate::research_claims::authority::project_uuid(current)?),
        },
        branch: branch.map(|b| b.branch_uuid),
        parent_branch: branch.and_then(|b| b.parent_branch_uuid),
        missing,
        suppressed,
    })
}
fn pinned_project(
    owner: &GraphForge,
    generation: &ResolvedProjectGeneration,
) -> Result<GraphForge, GfError> {
    GraphForge::open_resolved_with_options(
        generation.container_root().to_path_buf(),
        generation.clone(),
        true,
        owner.write_options.clone(),
        owner.resource_policy.clone(),
        graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
            generation.generation_uuid(),
        ),
    )
}
fn dependencies(
    graph: &GraphForge,
    registry: &ResearchRegistry,
    version: Option<&ResearchVersionRecord>,
    selected: Option<&Objects>,
) -> Result<Vec<(String, Uuid, String)>, GfError> {
    use graphforge_storage::research_versions::ResearchEvidenceReference;
    let mut missing = Vec::new();
    let project_evidence;
    let evidence = if let Some(version) = version {
        &version.content.evidence
    } else {
        project_evidence =
            crate::research_versions::complete_evidence(&graph.generation_for_read()?)?;
        &project_evidence
    };
    {
        for evidence in evidence {
            let artifact_uuid = match evidence {
                ResearchEvidenceReference::ExternalOnly { artifact_uuid, .. }
                | ResearchEvidenceReference::Unverifiable { artifact_uuid }
                | ResearchEvidenceReference::Local { artifact_uuid, .. } => artifact_uuid,
            };
            if selected.is_some_and(|s| !s.contains(&("artifact".into(), *artifact_uuid))) {
                continue;
            }
            match evidence {
                ResearchEvidenceReference::ExternalOnly { artifact_uuid, .. } => {
                    missing.push(("artifact".into(), *artifact_uuid, "external_only".into()));
                }
                ResearchEvidenceReference::Unverifiable { artifact_uuid } => {
                    missing.push(("artifact".into(), *artifact_uuid, "unverifiable".into()));
                }
                ResearchEvidenceReference::Local { .. } => {}
            }
        }
    }
    if let Some(snapshot) = graph
        .generation_for_read()?
        .participant_snapshot("workspace", "branch_references")?
    {
        let rows: serde_json::Value = serde_json::from_slice(&snapshot.bytes)
            .map_err(|_| super::invalid("invalid native reference history"))?;
        for row in rows
            .as_array()
            .ok_or_else(|| super::invalid("invalid native reference history"))?
        {
            let reference = row
                .get("reference_uuid")
                .and_then(|v| v.as_str())
                .and_then(|v| Uuid::parse_str(v).ok())
                .ok_or_else(|| super::invalid("invalid reference identity"))?;
            if selected.is_some_and(|s| !s.contains(&("reference".into(), reference))) {
                continue;
            }
            let id = row
                .get("source_version_uuid")
                .and_then(|v| v.as_str())
                .and_then(|v| Uuid::parse_str(v).ok())
                .ok_or_else(|| super::invalid("invalid reference Version"))?;
            if !registry.versions.contains_key(&id) {
                missing.push((
                    "version".into(),
                    id,
                    "reference_payload_not_retained".into(),
                ));
            }
        }
    }
    Ok(missing)
}
pub(super) fn canonical(
    state: &mut State,
    current: &ResolvedProjectGeneration,
    query: Option<&ResearchComparisonAuthority>,
    selected: Option<&Objects>,
) -> Result<(), GfError> {
    use graphforge_knowledge::research::ResearchDecisionKind;
    use sha2::{Digest, Sha256};
    let Some(query) = query else { return Ok(()) };
    let scope =
        crate::research_claims::authority::resolve(current, &query.context, query.community_uuid)?;
    let ledger = crate::research_claims::ledger::read_decisions(current)?;
    let end = ledger.events().last().map_or(0, |e| e.sequence);
    let cutoff = query.through_sequence.unwrap_or(end);
    if cutoff > end {
        return Err(super::invalid(
            "canonical comparison sequence is not yet available",
        ));
    }
    let mut latest = BTreeMap::new();
    for event in ledger
        .events()
        .iter()
        .filter(|e| e.authority == scope && e.sequence <= cutoff)
    {
        if selected
            .is_some_and(|s| !s.contains(&(event.subject_kind.as_str().into(), event.subject_uuid)))
        {
            continue;
        }
        if event.kind != ResearchDecisionKind::Integrate {
            latest.insert(
                (event.subject_kind.as_str().to_owned(), event.subject_uuid),
                event.kind,
            );
        }
    }
    for ((kind, id), decision) in latest {
        if decision == ResearchDecisionKind::Promote {
            state.fields.insert(
                (kind, id, "$canonical".into()),
                Sha256::digest(b"explicit-promotion/1").into(),
            );
        }
    }
    Ok(())
}
fn unavailable() -> GfError {
    GfError::Api {
        code: graphforge_core::ApiErrorCode::ResultNotRetained,
        message:
            "comparison endpoint or selected base is not retained; choose an available Version"
                .into(),
    }
}

fn local_suppression(
    graph: &GraphForge,
    current: &ResolvedProjectGeneration,
    registry: &ResearchRegistry,
    branch: Option<Uuid>,
    fields: &Fields,
    baseline: &BTreeMap<fields::Key, baseline::Row>,
) -> Result<Objects, GfError> {
    let mut result = Objects::new();
    for (key, row) in baseline {
        if key.2 == "$object" && !row.baseline.is_empty() && !fields.contains_key(key) {
            result.insert((key.0.clone(), key.1));
        }
    }
    let mut contexts = std::collections::HashSet::new();
    if let Some(branch) = branch {
        let mut next = Some(branch);
        while let Some(id) = next {
            if !contexts.insert(id) {
                return Err(super::invalid("cyclic research ancestry"));
            }
            next = registry
                .branches
                .get(&id)
                .and_then(|b| b.parent_branch_uuid);
        }
    } else {
        contexts.insert(crate::research_claims::authority::project_uuid(current)?);
    }
    let suppressions =
        crate::research_claims::ledger::read_suppressions(&graph.generation_for_read()?)?;
    let mut active = std::collections::BTreeSet::new();
    let mut inherited = std::collections::BTreeSet::new();
    for row in suppressions
        .events()
        .iter()
        .filter(|r| contexts.contains(&r.context_uuid))
    {
        active.insert(row.assertion_uuid);
        let key = (
            "assertion".into(),
            row.assertion_uuid,
            format!("$claim_suppressions:{}", row.suppression_uuid),
        );
        if baseline.get(&key).is_some_and(|b| !b.baseline.is_empty()) {
            inherited.insert(row.assertion_uuid);
        }
    }
    result.extend(
        active
            .difference(&inherited)
            .map(|id| ("assertion".into(), *id)),
    );
    Ok(result)
}
