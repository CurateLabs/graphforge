//! Bridge-set document types and bounded predicate vocabulary.

use serde::{Deserialize, Serialize};

use crate::composition::{ActivationMode, BridgeSetId, OntologyModuleId, QualifiedSymbol};

/// Bounded predicate set for bridge assertions (contract + #838 AC).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgePredicate {
    /// Governed equivalence between same-kind endpoints.
    Equivalent,
    /// Directional relatedness (source related-to target).
    Related,
    /// Directional broader mapping (source broader than target).
    Broader,
    /// Directional narrower mapping (source narrower than target).
    Narrower,
    /// Governed disjointness (same-kind endpoints must not overlap).
    Disjoint,
    /// Property or relation mapping (kind-compatible maps_to).
    MapsTo,
    /// Evidence linkage (typically entity/claim surfaces).
    EvidenceFor,
}

impl BridgePredicate {
    /// Stable wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Equivalent => "equivalent",
            Self::Related => "related",
            Self::Broader => "broader",
            Self::Narrower => "narrower",
            Self::Disjoint => "disjoint",
            Self::MapsTo => "maps_to",
            Self::EvidenceFor => "evidence_for",
        }
    }

    /// Whether the predicate is symmetric (direction still recorded for provenance).
    #[must_use]
    pub fn is_symmetric(self) -> bool {
        matches!(self, Self::Equivalent | Self::Disjoint)
    }
}

/// How a mapping was produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MappingMethod {
    /// Explicitly authored by a human curator.
    Authored,
    /// Suggested by tooling; non-authoritative until adopted.
    Suggested,
    /// Inferred by tooling; non-authoritative until adopted.
    Inferred,
}

impl MappingMethod {
    /// Stable wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Authored => "authored",
            Self::Suggested => "suggested",
            Self::Inferred => "inferred",
        }
    }

    /// Suggested/inferred mappings cannot become durable authority without adoption.
    #[must_use]
    pub fn is_authoritative(self) -> bool {
        matches!(self, Self::Authored)
    }
}

/// Optional confidence for suggested/inferred mappings.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MappingConfidence {
    /// Closed interval \[0.0, 1.0\].
    pub value: f64,
}

/// Provenance recorded on every authoritative assertion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BridgeProvenance {
    /// Production method.
    pub method: MappingMethod,
    /// Optional confidence (required for suggested/inferred when present).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<MappingConfidence>,
    /// Human justification (required for adopted authoritative mappings).
    pub justification: String,
    /// Evidence references (URIs or opaque evidence IDs); path-free.
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

/// Optional shared semantic surface hint (never mandatory).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedSurfaceHint {
    /// Provenance vocabulary.
    Provenance,
    /// Evidence / claim surface.
    Evidence,
    /// Research action vocabulary.
    ResearchActions,
    /// Time / place commons.
    TimePlace,
    /// Common relation vocabulary.
    CommonRelations,
}

impl SharedSurfaceHint {
    /// Stable wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Provenance => "provenance",
            Self::Evidence => "evidence",
            Self::ResearchActions => "research_actions",
            Self::TimePlace => "time_place",
            Self::CommonRelations => "common_relations",
        }
    }
}

/// Lifecycle status for a bridge-set record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeLifecycleStatus {
    /// Staged, not validated.
    Candidate,
    /// Validated but not durable authority.
    Validated,
    /// Durable bridge inventory authority.
    Adopted,
    /// Replaced by a newer exact bridge version.
    Superseded,
    /// Explicitly removed from authority.
    Removed,
}

impl BridgeLifecycleStatus {
    /// Stable wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Validated => "validated",
            Self::Adopted => "adopted",
            Self::Superseded => "superseded",
            Self::Removed => "removed",
        }
    }
}

/// One directed (or symmetric) mapping between exact qualified endpoints.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BridgeAssertion {
    /// Source endpoint (exact qualified symbol).
    pub source: QualifiedSymbol,
    /// Target endpoint (exact qualified symbol).
    pub target: QualifiedSymbol,
    /// Bounded predicate.
    pub predicate: BridgePredicate,
    /// Direction flag retained even for symmetric predicates (source→target).
    #[serde(default = "default_true")]
    pub directional: bool,
    /// Provenance / evidence.
    pub provenance: BridgeProvenance,
    /// Optional validity interval start as opaque NFC string (ISO-8601 recommended).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<String>,
    /// Optional validity interval end as opaque NFC string (ISO-8601 recommended).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to: Option<String>,
}

fn default_true() -> bool {
    true
}

/// Canonical bridge-set document (digest domain: `graphforge-ontology-bridge/1`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BridgeDocument {
    /// Globally unique NFC-normalized bridge URI.
    pub bridge_id: String,
    /// Opaque NFC authored version.
    pub authored_version: String,
    /// Exact source module constraints (must appear in known inventory).
    pub source_modules: Vec<OntologyModuleId>,
    /// Exact target module constraints.
    pub target_modules: Vec<OntologyModuleId>,
    /// Optional bridge-set dependencies on other exact bridge identities.
    #[serde(default)]
    pub dependencies: Vec<BridgeSetId>,
    /// Optional shared surface hints (never required for validity).
    #[serde(default)]
    pub shared_surfaces: Vec<SharedSurfaceHint>,
    /// Mapping assertions.
    pub assertions: Vec<BridgeAssertion>,
    /// Optional enforcement override for this bridge when activated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enforcement: Option<ActivationMode>,
}
