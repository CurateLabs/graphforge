//! Restore-independent decisions for reviewed immediate-upstream incorporation.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// Exact native field identity in one reviewed upstream update.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchUpstreamField {
    /// Native object kind, distinct from a runtime catalog identity.
    pub object_kind: String,
    /// Stable native identity.
    pub object_uuid: Uuid,
    /// Native semantic field name.
    pub field: String,
}

/// Persisted resolution; immutable assertion payloads belong to the knowledge owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchUpstreamResolutionRecord {
    /// Publish the exact reviewed upstream value.
    AdoptUpstream,
    /// Preserve local state and advance its reviewed upstream baseline.
    KeepLocal,
    /// Publish the owner's admitted representation containing both values.
    RetainBoth,
    /// Preserve local state and publish this immutable explanatory assertion.
    Explain {
        /// Native claim identity created in the same Branch Version.
        assertion_uuid: Uuid,
    },
}

/// Exact comparison and result commitments for one reviewed field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchUpstreamFieldReview {
    /// Reviewed semantic field.
    pub unit: ResearchUpstreamField,
    /// Previous independently incorporated upstream value.
    pub baseline_sha256: Option<[u8; 32]>,
    /// Local value before publication.
    pub local_sha256: Option<[u8; 32]>,
    /// Reviewed immediate-upstream value; None denotes deletion.
    pub upstream_sha256: Option<[u8; 32]>,
    /// Published local value; KeepLocal need not equal upstream.
    pub result_sha256: Option<[u8; 32]>,
    /// Analyst's explicit resolution or compatible automatic adoption.
    pub resolution: ResearchUpstreamResolutionRecord,
}

/// One permanent reviewed update, committed atomically with the new Branch head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchUpstreamReview {
    /// Contiguous native history order; caller timestamps do not order decisions.
    pub sequence: u64,
    /// Durable replay operation.
    pub operation_uuid: Uuid,
    /// Branch whose head advances.
    pub branch_uuid: Uuid,
    /// Immutable original base, which this operation never changes.
    pub original_base_version_uuid: Uuid,
    /// Exact local head reviewed before publication.
    pub prior_version_uuid: Uuid,
    /// Exact immediate-upstream Version identity; genealogy alone does not root payload.
    pub upstream_version_uuid: Uuid,
    /// Installed Branch Version, whose identity outlives optional payload retention.
    pub version_uuid: Uuid,
    /// Shared Project generation reviewed before publication.
    pub preview_generation_uuid: Uuid,
    /// Commitment to exact reviewed scope and values.
    pub preview_sha256: [u8; 32],
    /// At most 256 distinct field resolutions.
    pub fields: Vec<ResearchUpstreamFieldReview>,
    /// Explicit external/unverifiable evidence acknowledgement.
    pub acknowledged_evidence: BTreeSet<Uuid>,
    /// Recorded actor, not remote authentication.
    pub actor_uuid: Uuid,
    /// UTC microseconds.
    pub created_at: i64,
    /// Bounded human review explanation.
    pub explanation: String,
}

/// Project-lifetime history; restoration cannot erase a retained operation's decisions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchUpstreamHistory {
    /// Immutable reviews indexed by their publication identity.
    pub reviews: BTreeMap<Uuid, ResearchUpstreamReview>,
}

pub(super) fn validate(registry: &super::ResearchRegistry) -> Result<(), super::GfError> {
    let reviews = &registry.upstream.reviews;
    if reviews.len() > super::MAX_RECEIPTS {
        return Err(super::error(
            super::ProjectErrorCode::ResourceLimit,
            "upstream review history capacity exceeded; review and replay evidence does not expire",
        ));
    }
    let sequences: BTreeSet<_> = reviews.values().map(|review| review.sequence).collect();
    if sequences != (1..=reviews.len() as u64).collect() {
        return Err(super::invalid("upstream review sequence is not contiguous"));
    }
    for (id, review) in reviews {
        let branch = registry
            .branches
            .get(&review.branch_uuid)
            .ok_or_else(|| super::invalid("upstream review Branch is unavailable"))?;
        if id.is_nil()
            || *id != review.operation_uuid
            || review.actor_uuid.is_nil()
            || review.preview_generation_uuid.is_nil()
            || review.explanation.len() > 4096
            || review.acknowledged_evidence.len() > 256
            || review.fields.is_empty()
            || review.fields.len() > 256
            || review.original_base_version_uuid != branch.base_version_uuid
            || [
                review.prior_version_uuid,
                review.upstream_version_uuid,
                review.version_uuid,
            ]
            .iter()
            .any(|id| !registry.identities.contains_key(id))
            || registry
                .receipts
                .get(id)
                .is_none_or(|receipt| receipt.version_uuid != Some(review.version_uuid))
        {
            return Err(super::invalid(
                "invalid upstream review identity, history, receipt or bounds",
            ));
        }
        let mut units = BTreeSet::new();
        for field in &review.fields {
            if field.unit.object_uuid.is_nil()
                || field.unit.object_kind.is_empty()
                || field.unit.object_kind.len() > 64
                || field.unit.field.is_empty()
                || field.unit.field.len() > 4096
                || !units.insert(&field.unit)
            {
                return Err(super::invalid(
                    "invalid or duplicate upstream reviewed field",
                ));
            }
            let valid = match field.resolution {
                ResearchUpstreamResolutionRecord::AdoptUpstream => {
                    field.result_sha256 == field.upstream_sha256
                }
                ResearchUpstreamResolutionRecord::KeepLocal => {
                    field.result_sha256 == field.local_sha256
                }
                ResearchUpstreamResolutionRecord::Explain { assertion_uuid } => {
                    !assertion_uuid.is_nil() && field.result_sha256 == field.local_sha256
                }
                ResearchUpstreamResolutionRecord::RetainBoth => {
                    field.local_sha256.is_some()
                        && field.upstream_sha256.is_some()
                        && field.result_sha256.is_some()
                }
            };
            if !valid {
                return Err(super::invalid(
                    "upstream resolution disagrees with its published value",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn preserve(
    before: &super::ResearchRegistry,
    after: &super::ResearchRegistry,
    parent: &crate::ResolvedProjectGeneration,
    candidate_generation: Uuid,
    origin_witness: Option<&super::RegisterResearchVersion>,
) -> Result<(), super::GfError> {
    if before
        .upstream
        .reviews
        .iter()
        .any(|(id, review)| after.upstream.reviews.get(id) != Some(review))
    {
        return Err(super::invalid(
            "publication cannot erase or rewrite upstream review history",
        ));
    }
    let appended: Vec<_> = after
        .upstream
        .reviews
        .values()
        .filter(|review| !before.upstream.reviews.contains_key(&review.operation_uuid))
        .collect();
    if appended.len() > 1 {
        return Err(super::invalid(
            "one publication may append only one upstream review",
        ));
    }
    for review in appended {
        let receipt = after
            .receipts
            .get(&review.operation_uuid)
            .ok_or_else(|| super::invalid("new upstream review lacks its receipt"))?;
        let branch = before
            .branches
            .get(&review.branch_uuid)
            .ok_or_else(|| super::invalid("upstream review requires an existing Branch"))?;
        match (branch.parent_branch_uuid, origin_witness) {
            (Some(_), None) => {}
            (None, Some(origin)) => {
                if origin.version_uuid != review.upstream_version_uuid
                    || origin.context_uuid != branch.project_uuid
                    || origin.source_generation_uuid != parent.generation_uuid()
                    || origin.selection.is_some()
                    || origin.source_version.is_some()
                    || !origin.required_versions.is_empty()
                    || before.identities.contains_key(&origin.version_uuid)
                {
                    return Err(super::invalid(
                        "upstream review requires a fresh complete parent Project capture",
                    ));
                }
                let captured = super::capture(parent.container_root(), origin, before)?;
                if after.identities.get(&origin.version_uuid)
                    != Some(&super::identity_digest(&captured)?)
                {
                    return Err(super::invalid(
                        "upstream Project origin commitment differs from the reviewed parent",
                    ));
                }
            }
            _ => {
                return Err(super::invalid(
                    "upstream review origin differs from its immediate parent",
                ));
            }
        }
        if review.preview_generation_uuid != parent.generation_uuid()
            || receipt.generation_uuid != candidate_generation
            || before.receipts.contains_key(&review.operation_uuid)
            || before.identities.contains_key(&review.version_uuid)
            || receipt.intent_sha256.is_none()
            || before.heads.get(&review.branch_uuid) != Some(&review.prior_version_uuid)
            || after.heads.get(&review.branch_uuid) != Some(&review.version_uuid)
            || branch.parent_branch_uuid.is_some_and(|parent| {
                before.heads.get(&parent) != Some(&review.upstream_version_uuid)
            })
        {
            return Err(super::invalid(
                "new upstream review does not describe this publication transition",
            ));
        }
    }
    Ok(())
}
