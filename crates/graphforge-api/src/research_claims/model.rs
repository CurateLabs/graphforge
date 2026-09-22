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
