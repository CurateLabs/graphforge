//! Pin destination authority once and commit the exact native review preview.
use super::{PreviewResearchProposalRequest, invalid};
use crate::{CancellationToken, ExecutionResult, GfError, GraphForge, branches::fields};
use graphforge_storage::research_versions::{
    ResearchProposalDestination, ResearchProposalRecord, ResearchRegistry, read_research_registry,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use uuid::Uuid;

pub(super) struct Preview {
    pub generation: Uuid,
    pub registry: ResearchRegistry,
    pub proposal: ResearchProposalRecord,
    pub source: GraphForge,
    pub destination: GraphForge,
    pub rows: Vec<Row>,
    pub digest: [u8; 32],
}

#[derive(Serialize)]
pub(super) struct Row {
    pub item_uuid: Uuid,
    pub review_baseline: Option<[u8; 32]>,
    pub destination_value: Option<[u8; 32]>,
    pub conflict: bool,
    pub already_accepted: bool,
    pub required_items: BTreeSet<Uuid>,
    pub unavailable: Vec<String>,
    pub evidence_gaps: Vec<(Uuid, String)>,
}

impl GraphForge {
    /// Preview exact selected research, conflicts and dependencies without mutation.
    pub fn preview_research_proposal(
        &self,
        request: &PreviewResearchProposalRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionResult, GfError> {
        super::output::preview(&load(self, request.proposal_uuid, cancellation)?)
    }
}

pub(super) fn load(
    owner: &GraphForge,
    proposal_uuid: Uuid,
    cancellation: &CancellationToken,
) -> Result<Preview, GfError> {
    cancellation.checkpoint()?;
    crate::research_claims::authority::require_owner(owner)?;
    let current =
        graphforge_storage::resolve_project_generation(owner.resolved_generation.container_root())?;
    let registry = read_research_registry(&current)?;
    let proposal = registry
        .proposals
        .proposals
        .get(&proposal_uuid)
        .cloned()
        .ok_or_else(|| invalid("Proposal is unavailable"))?;
    if registry.proposals.released.contains_key(&proposal_uuid) {
        return Err(invalid(
            "Proposal payload was explicitly released; submit a new proposal",
        ));
    }
    let payload = registry
        .versions
        .get(&proposal.payload_version_uuid)
        .ok_or_else(|| invalid("frozen Proposal payload is unavailable"))?;
    let source = crate::research_versions::materialize_version(owner, payload)?;
    let destination = load_destination(owner, &current, &registry, &proposal)?;
    let objects: fields::Objects = proposal
        .items
        .iter()
        .map(|item| (item.unit.object_kind.clone(), item.unit.object_uuid))
        .collect();
    let objects = crate::slices::branch::dependency_objects(&source, &objects, cancellation)?;
    let proposed = fields::read(&source, cancellation)?;
    let before = fields::read_selected(&destination, Some(&objects), cancellation)?;
    let mut rows = Vec::new();
    let mut remaining_bytes = 8 * 1024 * 1024;
    for item in &proposal.items {
        cancellation.checkpoint()?;
        let key = (
            item.unit.object_kind.clone(),
            item.unit.object_uuid,
            item.unit.field.clone(),
        );
        if proposed.get(&key).copied() != item.value_sha256 {
            return Err(invalid(
                "frozen Proposal selected value differs from submission",
            ));
        }
        let destination_value = before.get(&key).copied();
        let already_accepted = registry.proposals.accepted.values().any(|mapping| {
            mapping.destination == proposal.destination
                && mapping.unit == item.unit
                && mapping.contribution_uuid == item.contribution_uuid
                && mapping.value_sha256 == item.value_sha256
        });
        let prior_acceptance = registry
            .proposals
            .accepted
            .values()
            .filter(|mapping| {
                mapping.destination == proposal.destination
                    && mapping.unit == item.unit
                    && mapping.contribution_uuid == item.contribution_uuid
            })
            .max_by_key(|mapping| registry.proposals.reviews[&mapping.operation_uuid].sequence);
        let review_baseline =
            prior_acceptance.map_or(item.baseline_sha256, |mapping| mapping.value_sha256);
        let mut row = Row {
            item_uuid: item.item_uuid,
            destination_value,
            review_baseline,
            conflict: destination_value != review_baseline
                && destination_value != item.value_sha256,
            already_accepted,
            required_items: item.required_items.clone(),
            unavailable: vec![],
            evidence_gaps: vec![],
        };
        super::dependencies::Context {
            proposal: &proposal,
            source: &source,
            proposed: &proposed,
            before: &before,
            cancellation,
            evidence: &payload.content.evidence,
        }
        .resolve(item, &mut row, &mut remaining_bytes)?;
        rows.push(row);
    }
    let mut hash = Sha256::new();
    hash.update(b"graphforge-proposal-preview/1");
    hash.update(current.generation_uuid().as_bytes());
    hash.update(
        serde_json::to_vec(&(&proposal, &rows)).map_err(|_| invalid("invalid review preview"))?,
    );
    Ok(Preview {
        generation: current.generation_uuid(),
        registry,
        proposal,
        source,
        destination,
        rows,
        digest: hash.finalize().into(),
    })
}

fn load_destination(
    owner: &GraphForge,
    current: &graphforge_storage::ResolvedProjectGeneration,
    registry: &ResearchRegistry,
    proposal: &ResearchProposalRecord,
) -> Result<GraphForge, GfError> {
    match proposal.destination {
        ResearchProposalDestination::Branch { branch_uuid } => {
            let version = registry
                .heads
                .get(&branch_uuid)
                .and_then(|id| registry.versions.get(id))
                .ok_or_else(|| invalid("Proposal destination Branch is unavailable"))?;
            crate::research_versions::materialize_version(owner, version)
        }
        ResearchProposalDestination::Project { project_uuid } => {
            if crate::research_claims::authority::project_uuid(current)? != project_uuid {
                return Err(invalid("Proposal destination Project authority differs"));
            }
            GraphForge::open_resolved_with_options(
                current.container_root().to_path_buf(),
                current.clone(),
                true,
                owner.write_options.clone(),
                owner.resource_policy.clone(),
                graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
                    current.generation_uuid(),
                ),
            )
        }
    }
}
