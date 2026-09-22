//! One pinned three-way review; original origin and incorporated baselines differ.
use super::{PreviewResearchUpstreamRequest, ResearchUpstreamScope, invalid};
use crate::{
    CancellationToken, GfError, GraphForge, ResearchComparisonEndpoint,
    branches::fields,
    research_comparison::{delta, state},
};
use graphforge_storage::{
    ResolvedProjectGeneration,
    research_versions::{ResearchBranchRecord, ResearchRegistry},
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub(super) struct Preview {
    pub current: ResolvedProjectGeneration,
    pub registry: ResearchRegistry,
    pub branch: ResearchBranchRecord,
    pub local: state::State,
    pub upstream: state::State,
    pub upstream_graph: GraphForge,
    pub rows: Vec<delta::Row>,
    pub requirements: BTreeMap<fields::Key, super::dependencies::Requirements>,
    pub digest: [u8; 32],
}

pub(super) fn load(
    owner: &GraphForge,
    request: &PreviewResearchUpstreamRequest,
    cancel: &CancellationToken,
) -> Result<Preview, GfError> {
    cancel.checkpoint()?;
    crate::research_claims::authority::require_owner(owner)?;
    let current =
        graphforge_storage::resolve_project_generation(owner.resolved_generation.container_root())?;
    let registry = graphforge_storage::research_versions::read_research_registry(&current)?;
    let branch = registry
        .branches
        .get(&request.branch_uuid)
        .cloned()
        .ok_or_else(|| invalid("upstream review Branch is unavailable"))?;
    let local = state::load(
        owner,
        &current,
        &registry,
        &ResearchComparisonEndpoint::Branch {
            branch_uuid: request.branch_uuid,
        },
        None,
        cancel,
    )?;
    let mut objects: fields::Objects = local
        .fields
        .keys()
        .chain(local.baseline.keys())
        .map(|key| (key.0.clone(), key.1))
        .collect();
    if let ResearchUpstreamScope::Fields { fields } = &request.scope {
        let unique: BTreeSet<_> = fields.iter().collect();
        if fields.is_empty() || fields.len() > 256 || fields.len() != unique.len() {
            return Err(invalid(
                "upstream field selection requires 1..256 distinct fields",
            ));
        }
        objects.extend(
            fields
                .iter()
                .map(|field| (field.object_kind.clone(), field.object_uuid)),
        );
    }
    let endpoint = branch
        .parent_branch_uuid
        .map_or(ResearchComparisonEndpoint::Project, |branch_uuid| {
            ResearchComparisonEndpoint::Branch { branch_uuid }
        });
    let mut upstream = state::load(
        owner,
        &current,
        &registry,
        &endpoint,
        Some(&objects),
        cancel,
    )?;
    let upstream_graph = if let Some(version) = upstream.version {
        crate::research_versions::materialize_version(owner, &registry.versions[&version])?
    } else {
        state::pinned_project(owner, &current)?
    };
    let existing: fields::Objects = upstream
        .fields
        .keys()
        .filter(|key| key.2 == "$object")
        .map(|key| (key.0.clone(), key.1))
        .collect();
    let closure = crate::slices::branch::dependency_objects(&upstream_graph, &existing, cancel)?;
    if closure.iter().any(|object| !objects.contains(object)) {
        objects.extend(closure);
        upstream = state::load(
            owner,
            &current,
            &registry,
            &endpoint,
            Some(&objects),
            cancel,
        )?;
    }
    bounds(&local, &upstream)?;
    let rows = delta::compare(&local, &upstream, &BTreeMap::new(), cancel)?
        .into_iter()
        .filter(|row| selected(&request.scope, row))
        .collect::<Vec<_>>();
    let requirements =
        super::dependencies::inspect(&upstream_graph, &local, &upstream, &rows, cancel)?;
    let mut digest = Sha256::new();
    digest.update(b"graphforge-upstream-preview/1");
    digest.update(
        serde_json::to_vec(&(
            request,
            current.generation_uuid(),
            branch.base_version_uuid,
            local.version,
            upstream.version,
            upstream.generation,
            &rows,
            requirements.iter().collect::<Vec<_>>(),
        ))
        .map_err(|_| invalid("invalid upstream preview"))?,
    );
    Ok(Preview {
        current,
        registry,
        branch,
        local,
        upstream,
        upstream_graph,
        rows,
        requirements,
        digest: digest.finalize().into(),
    })
}

fn selected(scope: &ResearchUpstreamScope, row: &delta::Row) -> bool {
    if row.change == "dependency_unavailable" {
        return true;
    }
    match scope {
        ResearchUpstreamScope::Branch => true,
        ResearchUpstreamScope::Sources => matches!(row.key.0.as_str(), "source" | "artifact"),
        ResearchUpstreamScope::Ontology => row.key.0.starts_with("ontology"),
        ResearchUpstreamScope::Fields { fields } => fields.iter().any(|field| {
            field.object_kind == row.key.0
                && field.object_uuid == row.key.1
                && field.field == row.key.2
        }),
    }
}
fn bounds(local: &state::State, upstream: &state::State) -> Result<(), GfError> {
    let mut count = 0usize;
    let mut bytes = 0usize;
    for key in local
        .fields
        .keys()
        .chain(local.baseline.keys())
        .chain(upstream.fields.keys())
    {
        count = count.saturating_add(1);
        bytes = bytes.saturating_add(1024 + key.0.len() + key.2.len());
        if count > 40_000 || bytes > 64 * 1024 * 1024 {
            return Err(GfError::Api {
                code: graphforge_core::ApiErrorCode::ResourceLimit,
                message: "upstream review exceeds 40000 fields or 64 MiB working state".into(),
            });
        }
    }
    Ok(())
}

pub(super) fn hex(value: &[u8; 32]) -> String {
    use std::fmt::Write;
    value
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            write!(text, "{byte:02x}").expect("String");
            text
        })
}
pub(super) fn key(unit: &crate::ResearchFieldIdentity) -> fields::Key {
    (
        unit.object_kind.clone(),
        unit.object_uuid,
        unit.field.clone(),
    )
}
pub(super) fn identity(operation: Uuid, role: &str) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-upstream-operation/1");
    digest.update(operation.as_bytes());
    digest.update(role.as_bytes());
    graphforge_core::canonical::uuid_v8(digest.finalize().into())
}
