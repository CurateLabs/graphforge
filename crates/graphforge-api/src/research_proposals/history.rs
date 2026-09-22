//! Bounded Arrow views of restore-independent review and accepted provenance.
use super::{ResearchProposalHistoryDetail, ResearchProposalHistoryRequest, invalid, output::hex};
use crate::{CancellationToken, ExecutionResult, GfError, GraphForge, branches::fields};
use graphforge_storage::research_versions::{
    ResearchProposalDecision, ResearchProposalDestination, ResearchProposalItem,
    ResearchProposalRecord, ResearchRegistry,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

type Row = BTreeMap<&'static str, String>;
const COLUMNS: &[&str] = &[
    "proposal_uuid",
    "source_branch_uuid",
    "source_version_uuid",
    "item_uuid",
    "object_kind",
    "object_uuid",
    "field",
    "contribution_uuid",
    "review_state",
    "branch_state",
    "destination_kind",
    "destination_uuid",
    "destination_version_uuid",
    "proof_version_uuid",
    "operation_uuid",
    "actor_uuid",
    "sequence",
    "decision",
    "explanation",
    "policy",
    "value_sha256",
    "generation_uuid",
    "next",
];

impl GraphForge {
    /// Inspect immutable proposal item, review or accepted mapping history as Arrow.
    /// Released payloads do not remove receipts, decisions or contribution mappings.
    pub fn research_proposal_history(
        &self,
        request: &ResearchProposalHistoryRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionResult, GfError> {
        cancellation.checkpoint()?;
        crate::research_claims::authority::require_owner(self)?;
        if !(1..=1000).contains(&request.page_size)
            || request.after.as_ref().is_some_and(|s| s.len() > 128)
        {
            return Err(invalid("invalid Proposal history bounds"));
        }
        let current = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        let registry = graphforge_storage::research_versions::read_research_registry(&current)?;
        let proposal = registry
            .proposals
            .proposals
            .get(&request.proposal_uuid)
            .ok_or_else(|| invalid("Proposal history is unavailable"))?;
        let mut query = request.clone();
        query.after = None;
        let mut hash = Sha256::new();
        hash.update(b"graphforge-proposal-history-page/1");
        hash.update(current.generation_uuid().as_bytes());
        hash.update(serde_json::to_vec(&query).map_err(|_| invalid("invalid history request"))?);
        let token = hex(&hash.finalize().into());
        let offset = request
            .after
            .as_ref()
            .map(|after| {
                let (commitment, offset) = after.split_once(':').ok_or_else(stale)?;
                let offset = offset.parse::<usize>().map_err(|_| stale())?;
                if commitment != token || offset > 4096 * 256 {
                    return Err(stale());
                }
                Ok(offset)
            })
            .transpose()?
            .unwrap_or(0);
        let mut page = Page {
            rows: Vec::new(),
            offset,
            seen: 0,
            limit: request.page_size,
            bytes: 0,
        };
        collect_rows(
            self,
            request.detail,
            &registry,
            proposal,
            &mut page,
            cancellation,
        )?;
        let next = (page.rows.len() > request.page_size)
            .then(|| format!("{token}:{}", offset + request.page_size));
        page.rows.truncate(request.page_size);
        for row in &mut page.rows {
            row.insert("generation_uuid", current.generation_uuid().to_string());
            if let Some(next) = &next {
                row.insert("next", next.clone());
            }
        }
        arrow(&page.rows)
    }
}

struct Page {
    rows: Vec<Row>,
    offset: usize,
    seen: usize,
    limit: usize,
    bytes: usize,
}
impl Page {
    fn push(&mut self, row: Row) -> Result<bool, GfError> {
        self.seen += 1;
        if self.seen <= self.offset {
            return Ok(true);
        }
        self.bytes = self
            .bytes
            .saturating_add(row.values().map(String::len).sum::<usize>() + 1024);
        if self.bytes > 8 * 1024 * 1024 {
            return Err(GfError::Project {
                code: graphforge_core::ProjectErrorCode::ResourceLimit,
                message: "Proposal history page exceeds 8 MiB".into(),
            });
        }
        self.rows.push(row);
        Ok(self.rows.len() <= self.limit)
    }
}

fn matching<'a>(
    registry: &'a ResearchRegistry,
    proposal: &'a ResearchProposalRecord,
    item: &'a ResearchProposalItem,
) -> impl Iterator<Item = &'a graphforge_storage::research_versions::ResearchAcceptedMapping> {
    registry.proposals.accepted.values().filter(move |mapping| {
        mapping.destination == proposal.destination
            && mapping.unit == item.unit
            && mapping.contribution_uuid == item.contribution_uuid
            && mapping.value_sha256 == item.value_sha256
    })
}

fn base(proposal: &ResearchProposalRecord, item: &ResearchProposalItem) -> Row {
    let (kind, destination) = match proposal.destination {
        ResearchProposalDestination::Project { project_uuid } => ("project", project_uuid),
        ResearchProposalDestination::Branch { branch_uuid } => ("branch", branch_uuid),
    };
    let mut row = Row::from([
        ("proposal_uuid", proposal.proposal_uuid.to_string()),
        (
            "source_branch_uuid",
            proposal.source_branch_uuid.to_string(),
        ),
        (
            "source_version_uuid",
            proposal.source_version_uuid.to_string(),
        ),
        ("item_uuid", item.item_uuid.to_string()),
        ("object_kind", item.unit.object_kind.clone()),
        ("object_uuid", item.unit.object_uuid.to_string()),
        ("field", item.unit.field.clone()),
        ("contribution_uuid", item.contribution_uuid.to_string()),
        ("destination_kind", kind.into()),
        ("destination_uuid", destination.to_string()),
    ]);
    if let Some(value) = item.value_sha256 {
        row.insert("value_sha256", hex(&value));
    }
    row
}
fn decision_name(decision: ResearchProposalDecision) -> &'static str {
    match decision {
        ResearchProposalDecision::Accept => "accepted",
        ResearchProposalDecision::Reject => "rejected",
        ResearchProposalDecision::Defer => "deferred",
    }
}
fn stale() -> GfError {
    GfError::Api {
        code: graphforge_core::ApiErrorCode::PageSnapshotGone,
        message: "Proposal history continuation is stale; restart at current history".into(),
    }
}
fn arrow(rows: &[Row]) -> Result<ExecutionResult, GfError> {
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(
        COLUMNS
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = COLUMNS
        .iter()
        .map(|name| {
            Arc::new(
                rows.iter()
                    .map(|row| row.get(name).map(String::as_str))
                    .collect::<StringArray>(),
            ) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|error| GfError::Validation(error.to_string()))?;
    Ok(crate::knowledge::assertion_result(batch))
}

fn collect_rows(
    owner: &GraphForge,
    detail: ResearchProposalHistoryDetail,
    registry: &ResearchRegistry,
    proposal: &ResearchProposalRecord,
    page: &mut Page,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    match detail {
        ResearchProposalHistoryDetail::Items => {
            item_rows(owner, registry, proposal, page, cancellation)?;
        }
        ResearchProposalHistoryDetail::Reviews => {
            let mut reviews: Vec<_> = registry
                .proposals
                .reviews
                .values()
                .filter(|review| review.proposal_uuid == proposal.proposal_uuid)
                .collect();
            reviews.sort_by_key(|review| review.sequence);
            'reviews: for review in reviews {
                for item in &proposal.items {
                    cancellation.checkpoint()?;
                    let mut row = base(proposal, item);
                    row.insert("operation_uuid", review.operation_uuid.to_string());
                    row.insert("actor_uuid", review.actor_uuid.to_string());
                    row.insert("sequence", review.sequence.to_string());
                    row.insert(
                        "decision",
                        decision_name(review.decisions[&item.item_uuid]).into(),
                    );
                    row.insert("explanation", review.explanation.clone());
                    row.insert("policy", review.policy.clone());
                    if let Some(id) = review.destination_version_uuid {
                        row.insert("destination_version_uuid", id.to_string());
                    }
                    if !page.push(row)? {
                        break 'reviews;
                    }
                }
            }
        }
        ResearchProposalHistoryDetail::Accepted => {
            for item in &proposal.items {
                cancellation.checkpoint()?;
                for mapping in matching(registry, proposal, item) {
                    let mut row = base(proposal, item);
                    row.insert("source_branch_uuid", mapping.source_branch_uuid.to_string());
                    row.insert(
                        "source_version_uuid",
                        mapping.source_version_uuid.to_string(),
                    );
                    row.insert(
                        "destination_version_uuid",
                        mapping.destination_version_uuid.to_string(),
                    );
                    row.insert("proof_version_uuid", mapping.proof_version_uuid.to_string());
                    row.insert("operation_uuid", mapping.operation_uuid.to_string());
                    row.insert("review_state", "accepted".into());
                    if !page.push(row)? {
                        break;
                    }
                }
                if page.rows.len() > page.limit {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn item_rows(
    owner: &GraphForge,
    registry: &ResearchRegistry,
    proposal: &ResearchProposalRecord,
    page: &mut Page,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let version = registry
        .heads
        .get(&proposal.source_branch_uuid)
        .and_then(|id| registry.versions.get(id))
        .ok_or_else(|| invalid("Proposal source Branch head is unavailable"))?;
    let view = crate::research_versions::materialize_version(owner, version)?;
    let objects = proposal
        .items
        .iter()
        .map(|item| (item.unit.object_kind.clone(), item.unit.object_uuid))
        .collect();
    let current_fields = fields::read_selected(&view, Some(&objects), cancellation)?;
    let latest = registry
        .proposals
        .reviews
        .values()
        .filter(|review| review.proposal_uuid == proposal.proposal_uuid)
        .max_by_key(|review| review.sequence);
    for item in &proposal.items {
        cancellation.checkpoint()?;
        let mut row = base(proposal, item);
        let accepted = matching(registry, proposal, item).next().is_some();
        let decision = latest.and_then(|review| review.decisions.get(&item.item_uuid));
        let state = if accepted {
            "accepted"
        } else if registry
            .proposals
            .released
            .contains_key(&proposal.proposal_uuid)
        {
            "superseded"
        } else {
            decision.copied().map_or("proposed", decision_name)
        };
        row.insert("review_state", state.into());
        let key = (
            item.unit.object_kind.clone(),
            item.unit.object_uuid,
            item.unit.field.clone(),
        );
        row.insert(
            "branch_state",
            if current_fields.get(&key).copied() == item.value_sha256 {
                state
            } else {
                "superseded"
            }
            .into(),
        );
        if let Some(review) = latest {
            row.insert("operation_uuid", review.operation_uuid.to_string());
            row.insert("actor_uuid", review.actor_uuid.to_string());
            row.insert("sequence", review.sequence.to_string());
        }
        if !page.push(row)? {
            break;
        }
    }
    Ok(())
}
