//! Native requests keep destination authority separate from source provenance.
use graphforge_knowledge::research::{ResearchDecisionKind, ResearchSubjectKind};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Explicit native research context within the open owning Project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchContext {
    /// The owning Project's current research.
    Project,
    /// An independently evolving native Branch.
    Branch {
        /// Stable native Branch identity.
        branch_uuid: Uuid,
    },
}
/// One explicit integration, promotion or revocation; never inferred from status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchDecisionInput {
    /// Immutable decision identity, UUIDv7.
    pub decision_uuid: Uuid,
    /// Typed node, relationship or assertion identity.
    pub subject_kind: ResearchSubjectKind,
    /// Public subject UUID.
    pub subject_uuid: Uuid,
    /// Explicit destination decision.
    pub kind: ResearchDecisionKind,
    /// Exact retained source Version as provenance, if applicable.
    pub source_version_uuid: Option<Uuid>,
}
/// Atomically append explicit decisions in one destination authority scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordResearchDecisionsRequest {
    /// Stable operation identity for exact retry.
    pub operation_uuid: Uuid,
    /// Required authoritative CURRENT for first publication.
    pub expected_generation_uuid: Uuid,
    /// Explicit destination context; Project identity is native-derived.
    pub context: ResearchContext,
    /// Optional community scope; metadata, not access enforcement.
    pub community_uuid: Option<Uuid>,
    /// Creating analyst or agent identity.
    pub creator_uuid: Uuid,
    /// Caller-recorded UTC microseconds; publication sequence orders history.
    pub recorded_at: i64,
    /// Ordered explicit decisions, bounded to 256 per publication.
    pub decisions: Vec<ResearchDecisionInput>,
}

/// Atomic creation of an immutable assertion and its research classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateResearchClaimRequest {
    /// Stable publication identity.
    pub operation_uuid: Uuid,
    /// Required CURRENT before first publication.
    pub expected_generation_uuid: Uuid,
    /// New immutable assertion identity, UUIDv7.
    pub assertion_uuid: Uuid,
    /// Exact immutable claim bytes.
    pub claim: String,
    /// Existing native graph references.
    pub graph_refs: Vec<crate::AssertionGraphRefInput>,
    /// Explicit interpretation category, independent of supported/canonical status.
    pub category: graphforge_knowledge::research::ResearchCategory,
    /// Analyst/agent producing the assertion.
    pub creator_uuid: Uuid,
    /// Existing producer run; mandatory for machine extraction.
    pub run_uuid: Option<Uuid>,
    /// UTC creation time recorded with the immutable assertion.
    pub created_at: i64,
}

/// Append one immutable explicit relation between existing claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelateResearchClaimsRequest {
    /// Stable publication identity.
    pub operation_uuid: Uuid,
    /// Required CURRENT before first publication.
    pub expected_generation_uuid: Uuid,
    /// Exact relation; producing provenance must exist in this research context.
    pub relation: graphforge_knowledge::research::ClaimRelationRecord,
}

/// New immutable research claim payload for a Branch publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchClaimDraft {
    /// New assertion UUIDv7.
    pub assertion_uuid: Uuid,
    /// Exact claim text.
    pub claim: String,
    /// Existing native graph references.
    pub graph_refs: Vec<crate::AssertionGraphRefInput>,
    /// Explicit research category.
    pub category: graphforge_knowledge::research::ResearchCategory,
    /// Existing producer run, mandatory for machine extraction.
    pub run_uuid: Option<Uuid>,
}
/// One bounded Branch-local research change; all intermediate preparation is private.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchClaimChange {
    /// Create a local interpretation without canonical promotion.
    Create {
        /// Immutable new claim payload.
        claim: ResearchClaimDraft,
    },
    /// Append a native disputed status and decision rationale.
    Challenge {
        /// Existing assertion to challenge in this Branch only.
        assertion_uuid: Uuid,
        /// New status event UUIDv7.
        status_event_uuid: Uuid,
        /// New native reasoning UUIDv7.
        reasoning_uuid: Uuid,
        /// Plain-text decision rationale.
        rationale: String,
        /// Existing producing provenance event.
        provenance_uuid: Uuid,
    },
    /// Append an explicit relation between existing claims.
    Relate {
        /// Native immutable relation record.
        relation: graphforge_knowledge::research::ClaimRelationRecord,
    },
    /// Replace/revise by adding an immutable successor and native supersession history.
    Revise {
        /// Existing prior assertion, whose bytes remain unchanged.
        prior_assertion_uuid: Uuid,
        /// New immutable successor.
        claim: ResearchClaimDraft,
        /// Native supersession UUIDv7.
        supersession_uuid: Uuid,
        /// Paired superseded status UUIDv7.
        status_event_uuid: Uuid,
        /// New decision-rationale UUIDv7.
        reasoning_uuid: Uuid,
        /// Plain-text explanation of the revision.
        rationale: String,
        /// Existing producing provenance event.
        provenance_uuid: Uuid,
        /// Explicit claim relation UUIDv7.
        relation_uuid: Uuid,
    },
    /// Hide the scoped knowledge view without deleting assertion or graph bytes.
    Suppress {
        /// New immutable event UUIDv7.
        suppression_uuid: Uuid,
        /// Existing assertion.
        assertion_uuid: Uuid,
        /// Existing producing provenance event.
        provenance_uuid: Uuid,
    },
}
/// One atomic Branch Version publication for contextual knowledge changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeResearchBranchClaimRequest {
    /// Stable final publication identity.
    pub operation_uuid: Uuid,
    /// Required owning Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Exact destination Branch.
    pub branch_uuid: Uuid,
    /// Fresh immutable resulting Version.
    pub version_uuid: Uuid,
    /// Creating analyst/agent metadata.
    pub creator_uuid: Uuid,
    /// UTC creation time.
    pub created_at: i64,
    /// One bounded explicit change.
    pub change: ResearchClaimChange,
}

/// Explicit scoped knowledge inspection; ordinary graph queries never use this filter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectResearchClaimsRequest {
    /// Exact research context.
    pub context: ResearchContext,
    /// Exact community authority, if any.
    pub community_uuid: Option<Uuid>,
    /// Include hidden claims for inspection; default active view omits them.
    pub include_suppressed: bool,
}
/// Existing immutable owner history to inspect in a research context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchClaimHistoryKind {
    /// Frozen research classifications.
    Classification,
    /// Explicit directed claim relations.
    Relations,
    /// Immutable scoped knowledge suppression events.
    Suppressions,
    /// Existing native epistemic status events.
    Status,
    /// Existing native reasoning history.
    Reasoning,
    /// Existing native evidence links.
    Evidence,
}
/// Explicit native claim history request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchClaimHistoryRequest {
    /// Exact research context.
    pub context: ResearchContext,
    /// Existing data owner to inspect.
    pub family: ResearchClaimHistoryKind,
    /// Optional exact assertion filter; relations match either endpoint.
    pub assertion_uuid: Option<Uuid>,
}

/// Shared native transport contract for current canonical choices or decision history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchAuthorityQuery {
    /// Explicit owning Project or Branch context.
    pub context: ResearchContext,
    /// Exact community scope, if any.
    pub community_uuid: Option<Uuid>,
}
