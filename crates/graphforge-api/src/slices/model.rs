//! Slice input contracts. Selections do not create independent research state.
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Explicit research source. Current selection is dynamic; Version selection is exact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SliceSource {
    /// Evaluate against one pinned CURRENT generation.
    Current,
    /// Evaluate exact retained immutable research; never follow the live parent.
    Version {
        /// Immutable Version identity, distinct from storage generation identity.
        version_uuid: Uuid,
    },
}

/// Explicit active membership, separate from required dependencies and boundaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SliceMembers {
    /// Selected graph nodes.
    pub nodes: BTreeSet<Uuid>,
    /// Selected graph relationships; endpoints become dependencies unless selected.
    pub edges: BTreeSet<Uuid>,
    /// Selected research Sources.
    pub sources: BTreeSet<Uuid>,
    /// Selected immutable Artifacts.
    pub artifacts: BTreeSet<Uuid>,
    /// Selected immutable assertions.
    pub assertions: BTreeSet<Uuid>,
}

/// Direction used by bounded native traversal selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SliceDirection {
    /// Follow outgoing relationships.
    Outgoing,
    /// Follow incoming relationships.
    Incoming,
    /// Follow either endpoint.
    #[default]
    Both,
}

/// One native selection rule. Query output must identify node_uuid or edge_uuid.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SliceSelector {
    /// Exact identities supplied by the caller.
    Direct {
        /// Explicit active membership.
        members: SliceMembers,
    },
    /// Native property equality with a separately bound scalar parameter.
    Filter {
        /// Required node label.
        label: String,
        /// Property to compare.
        property: String,
        /// JSON scalar equality value; objects and arrays are refused.
        equals: serde_json::Value,
    },
    /// Native read-only Cypher selection; no mutation is allowed.
    Query {
        /// Query returning canonical UUID identity columns.
        query: String,
    },
    /// Existing Rust text-search path, with a finite hit count.
    Search {
        /// Required node label.
        label: String,
        /// Text search expression.
        text: String,
        /// Maximum selected search hits.
        limit: u32,
    },
    /// Deterministic breadth-first selection with one canonical inclusion path.
    Traverse {
        /// Explicit seed node identities.
        seeds: BTreeSet<Uuid>,
        /// Relationship direction.
        direction: SliceDirection,
        /// Maximum hop count, including zero for seeds only.
        max_depth: u32,
        /// Empty means all relationship types.
        relationship_types: BTreeSet<String>,
    },
}

/// Caller-selected resource bounds, each constrained by a native hard ceiling.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SliceLimits {
    /// Maximum decoded topology/ledger rows plus emitted selector rows.
    /// This is not a count of physical query-operator work.
    pub scanned_rows: u32,
    /// Maximum explicitly active objects.
    pub selected_objects: u32,
    /// Maximum separate outside references.
    pub boundary_references: u32,
    /// Maximum required dependency references.
    pub dependencies: u32,
    /// Conservative working-set budget for selection collections.
    pub working_bytes: u64,
    /// Maximum Arrow response or frozen membership capsule size.
    pub response_bytes: u64,
}

impl Default for SliceLimits {
    fn default() -> Self {
        Self {
            scanned_rows: 1_000_000,
            selected_objects: 100_000,
            boundary_references: 100_000,
            dependencies: 100_000,
            working_bytes: 64 * 1024 * 1024,
            response_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Read-only Slice evaluation with explicit inclusion and contraction adjustments.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SliceRequest {
    /// Caller identity used to bind continuation to this exact request.
    pub request_uuid: Uuid,
    /// Explicit dynamic or immutable source.
    pub source: SliceSource,
    /// Native selection rule.
    pub selector: SliceSelector,
    /// Explicit additions; required dependencies never become active implicitly.
    #[serde(default)]
    pub include: SliceMembers,
    /// Explicit removals from active membership.
    #[serde(default)]
    pub exclude: SliceMembers,
    /// Bounded evaluation and output.
    #[serde(default)]
    pub limits: SliceLimits,
}

/// Separately inspectable data-bearing Slice result families.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlicePageKind {
    /// Explicitly active objects only.
    Included,
    /// Outside references, without importing their content.
    Boundary,
    /// One deterministic rule/path explanation per active object.
    Explanations,
    /// Required context, separate from active membership.
    Dependencies,
    /// Typed counts, including node labels and research object families.
    Counts,
}

/// Explicit frozen membership revision; changing historical authority is opt-in.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SliceRevisionRequest {
    /// New immutable selection request identity.
    pub request_uuid: Uuid,
    /// Add exact objects from the chosen historical authority.
    #[serde(default)]
    pub include: SliceMembers,
    /// Remove objects from active membership, preserving required context.
    #[serde(default)]
    pub exclude: SliceMembers,
    /// A separately retained Version for deliberate outside-history expansion.
    /// None keeps the original Version and never follows CURRENT.
    pub source_version: Option<Uuid>,
}
