//! Explicit native selection and resolution for reviewed upstream incorporation.
use crate::ResearchFieldIdentity;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Which upstream changes the analyst chooses to inspect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchUpstreamScope {
    /// Existing Branch membership and its required context; never an implicit expansion.
    Branch,
    /// Sources and Artifacts already in Branch scope, with dependency consequences.
    Sources,
    /// Native ontology changes required by this Branch's context.
    Ontology,
    /// Exact fields, including explicitly selected additions outside existing membership.
    Fields {
        /// Native identities from research inspection, at most 256.
        fields: Vec<ResearchFieldIdentity>,
    },
}

/// Read-only review of the Branch's exact immediate parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewResearchUpstreamRequest {
    /// Stable Branch; opening it does not advance research.
    pub branch_uuid: Uuid,
    /// Explicit review scope.
    pub scope: ResearchUpstreamScope,
}

/// Explicit treatment of one reviewed upstream field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchUpstreamResolution {
    /// Incorporate the upstream value or deletion.
    AdoptUpstream,
    /// Preserve the local value while recording the reviewed upstream baseline.
    KeepLocal,
    /// Concatenate existing list values only when native property and ontology owners admit the result.
    /// Scalars require another resolution; list order and repetitions are preserved.
    RetainBoth,
    /// Preserve the local value with an explicit immutable explanatory assertion.
    Explain {
        /// Exact explanatory research claim to publish in the same Branch update.
        claim: crate::ResearchClaimDraft,
    },
}

/// One selected native field and its explicit resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchUpstreamDecision {
    /// Exact field from the preview.
    pub unit: ResearchFieldIdentity,
    /// Resolution; incompatible retain-both remains unresolved.
    pub resolution: ResearchUpstreamResolution,
}

/// Apply all compatible changes or an explicitly reviewed subset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchUpstreamSelection {
    /// Adopt only compatible previewed upstream changes whose dependencies are selected.
    AllCompatible,
    /// Individually reviewed changes; other incorporated baselines remain unchanged.
    Selected {
        /// At most 256 unique native fields.
        decisions: Vec<ResearchUpstreamDecision>,
    },
}

/// Publish one reviewed upstream update through the owning Project authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateResearchBranchRequest {
    /// Stable operation identity; exact retry recovers its original durable receipt.
    pub operation_uuid: Uuid,
    /// Owning Project CURRENT from the preview.
    pub expected_generation_uuid: Uuid,
    /// Fresh immutable local Version.
    pub version_uuid: Uuid,
    /// Exact preview scope and Branch.
    pub preview: PreviewResearchUpstreamRequest,
    /// Native commitment to base/local/upstream identities and review rows.
    pub preview_sha256: [u8; 32],
    /// Selected resolutions or all compatible changes.
    pub selection: ResearchUpstreamSelection,
    /// Explicit acknowledgement of external-only or unverifiable evidence context.
    pub acknowledge_evidence: std::collections::BTreeSet<Uuid>,
    /// Recorded actor, not remote authentication.
    pub actor_uuid: Uuid,
    /// UTC microseconds.
    pub created_at: i64,
    /// Bounded review explanation.
    pub explanation: String,
}

/// Bounded inspection of permanent upstream decisions for one Branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchUpstreamHistoryRequest {
    /// Branch whose reviews are inspected.
    pub branch_uuid: Uuid,
    /// Number of field decisions per page, 1..1000.
    pub page_size: usize,
    /// Opaque generation-bound continuation from the previous Arrow page.
    pub after: Option<String>,
}
