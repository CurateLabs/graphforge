//! GraphForge core — public error type, span, and shared value model.
//!
//! This is the **foundation crate**: every other `gf-*` crate depends on it. The
//! `GraphForge` engine facade lives in the top-level `gf-api` crate (it needs the
//! pipeline crates, which depend on `gf-core` — so it cannot live here without a
//! cycle; see #716).
#![forbid(unsafe_code)]

pub mod algorithms;
pub mod canonical;
pub mod embedding_options;
pub mod manifest;
pub mod uuid;

use std::{fmt, sync::Arc};

// ---------------------------------------------------------------------------
// Span
// ---------------------------------------------------------------------------

/// Byte-offset range in the original source text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct Span {
    /// Start byte offset (inclusive).
    pub start: usize,
    /// End byte offset (exclusive).
    pub end: usize,
}

impl Span {
    /// Create a new span.
    #[must_use]
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

// ---------------------------------------------------------------------------
// TypeId
// ---------------------------------------------------------------------------

/// Opaque integer identifier for any ontology type (entity, relation, or property).
///
/// IDs are assigned at compile time by the ontology compiler and are stable
/// for the lifetime of a loaded ontology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TypeId(pub u32);

/// Opaque integer identifier for a property type.
///
/// Distinct from [`TypeId`] (which identifies entity/relation types) so that
/// the type system prevents accidental interchangeability.  IDs are assigned
/// at compile time by the ontology compiler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PropId(pub u32);

/// Serialisation format of an ontology definition file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OntologyFormat {
    /// YAML (`.yaml` or `.yml`).
    Yaml,
    /// JSON (`.json`).
    Json,
}

/// How the binder resolves unknown labels, relation types, and property names.
///
/// Serialises as a lowercase string (`"exploratory"`, `"advisory"`,
/// `"strict"`).  Lives in `gf-core` so that the project manifest
/// ([`manifest::ProjectManifest`]) and the binder (`gf-ir`) share one
/// definition; `gf-ir` re-exports it as `gf_ir::OntologyMode`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum OntologyMode {
    /// No ontology required; the runtime catalog auto-assigns integer IDs for
    /// every observed label/type.
    #[default]
    Exploratory,
    /// Ontology present; violations produce warnings, not errors.
    Advisory,
    /// Ontology required; violations produce a bind error.
    Strict,
}

// ---------------------------------------------------------------------------
// GfError — public error enum
// ---------------------------------------------------------------------------

/// All errors produced by GraphForge.
///
/// Variant names correspond 1-to-1 with the Python exception hierarchy in
/// `graphforge.exceptions` so that binding layers can map them without
/// string-matching.
#[derive(thiserror::Error, Debug)]
pub enum GfError {
    /// Feature exists in the API but has not been implemented yet.
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),

    /// The Cypher parser rejected the input.
    #[error("parse error at {span}: {msg}")]
    Parse {
        /// Human-readable description of the parse failure.
        msg: String,
        /// Location of the bad token in the source.
        span: Span,
    },

    /// The binder rejected the query (e.g. undeclared variable, strict-mode
    /// unknown label). Carries the source span of the *first* error so callers
    /// can point at the offending token; `msg` lists every binder error. Shares
    /// the `PlanError` fault domain with [`GfError::Plan`] in the binding layers
    /// — it is the span-rich sibling for binder-phase failures.
    #[error("bind error at {span}: {msg}")]
    Bind {
        /// Human-readable description (all binder errors, joined with `; `).
        msg: String,
        /// Source location of the first offending token.
        span: Span,
    },

    /// The binder or query planner could not produce a valid plan.
    #[error("plan error: {0}")]
    Plan(String),

    /// A runtime fault occurred during query execution.
    #[error("execution error: {0}")]
    Execution(String),

    /// A redacted configured-provider invocation failed.
    #[error("provider error: class={class} provider={provider} model={model}")]
    Provider {
        /// Stable provider failure class.
        class: String,
        /// Normalized non-secret provider identifier.
        provider: String,
        /// Non-secret model identifier.
        model: String,
    },

    /// A storage I/O operation failed.
    #[error("storage error: {0}")]
    Storage(String),

    /// The project container cannot be resolved safely.
    #[error("{code}: {message}")]
    Project {
        /// Stable public error code.
        code: ProjectErrorCode,
        /// Safe diagnostic without participant contents or unrestricted paths.
        message: String,
    },

    /// A structured M20/M21 public API failure.
    #[error("{code}: {message}")]
    Api {
        /// Stable closed public error code.
        code: ApiErrorCode,
        /// Safe diagnostic without record contents.
        message: String,
    },

    /// An operation was invalid for the current instance lifecycle state.
    #[error("lifecycle error: {0}")]
    Lifecycle(String),

    /// Input failed validation at the API boundary.
    #[error("validation error: {0}")]
    Validation(String),

    /// An ontology file could not be loaded or applied.
    #[error("ontology error: {0}")]
    Ontology(String),
}

impl GfError {
    /// Return the stable public error code for this failure.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::NotImplemented(_) => "GF_NOT_IMPLEMENTED",
            Self::Parse { .. } => "GF_PARSE",
            Self::Bind { .. } | Self::Plan(_) => "GF_PLAN",
            Self::Execution(_) | Self::Provider { .. } => "GF_EXECUTION",
            Self::Storage(_) => "GF_IO",
            Self::Project { code, .. } => code.as_str(),
            Self::Api { code, .. } => code.as_str(),
            Self::Lifecycle(_) => "GF_LIFECYCLE",
            Self::Validation(_) => "GF_VALIDATION",
            Self::Ontology(_) => "GF_ONTOLOGY",
        }
    }
}

/// Stable non-project M20/M21 API error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiErrorCode {
    /// Requested UUID does not exist in the selected capability snapshot.
    NotFound,
    /// Operation was cancelled at a deterministic checkpoint.
    Cancelled,
    /// Request exceeded a registered bounded-resource limit.
    ResourceLimit,
    /// Page token is malformed or incompatible with the method.
    PageInvalid,
    /// Page token names a generation that is no longer the selected snapshot.
    PageSnapshotGone,
    /// Persisted Arrow schema or registered fingerprint is incompatible.
    SchemaMismatch,
    /// Caller supplied an unknown argument.
    UnknownArgument,
    /// Explicit projection policy could not resolve an epistemic ambiguity.
    AmbiguousProjection,
    /// An identity was reused for different immutable content.
    IdentityConflict,
    /// Canonical fingerprint collision was detected.
    FingerprintCollision,
    /// A completed run did not retain its result rows.
    ResultNotRetained,
}

impl ApiErrorCode {
    /// Frozen external spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "GF_NOT_FOUND",
            Self::Cancelled => "GF_CANCELLED",
            Self::ResourceLimit => "GF_RESOURCE_LIMIT",
            Self::PageInvalid => "GF_PAGE_INVALID",
            Self::PageSnapshotGone => "GF_PAGE_SNAPSHOT_GONE",
            Self::SchemaMismatch => "GF_SCHEMA_MISMATCH",
            Self::UnknownArgument => "GF_UNKNOWN_ARGUMENT",
            Self::AmbiguousProjection => "GF_AMBIGUOUS_PROJECTION",
            Self::IdentityConflict => "GF_IDENTITY_CONFLICT",
            Self::FingerprintCollision => "GF_FINGERPRINT_COLLISION",
            Self::ResultNotRetained => "GF_RESULT_NOT_RETAINED",
        }
    }
}

impl fmt::Display for ApiErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Stable project-format and project-generation error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectErrorCode {
    /// The root is not the current supported project format.
    UnsupportedProjectFormat,
    /// The v1 container has no committed generation.
    ProjectUninitialized,
    /// The commit pointer or selected generation is internally inconsistent.
    ProjectCorrupt,
    /// The project filesystem cannot provide the required atomic semantics.
    UnsupportedFilesystem,
    /// Another process currently owns the project writer lock.
    WriterBusy,
    /// A transaction UUID was reused with different immutable inputs.
    TransactionConflict,
    /// A generation failed before or after its commit point.
    PublicationFailed,
    /// A capability ID/version is not implemented by this binary.
    UnsupportedCapabilityVersion,
    /// A capability-specific operation was requested before enablement.
    CapabilityDisabled,
    /// A transaction failed before publication.
    TransactionFailed,
    /// The named checkpoint already exists.
    CheckpointExists,
    /// The named checkpoint does not exist.
    CheckpointNotFound,
    /// Checkpoint registry state is unsafe or inconsistent.
    CheckpointRegistryCorrupt,
    /// A mutation was attempted through an immutable checkpoint view.
    ReadOnlyView,
    /// A bounded checkpoint resource limit was exceeded.
    ResourceLimit,
}

impl ProjectErrorCode {
    /// Return the frozen public error-code spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedProjectFormat => "GF_UNSUPPORTED_PROJECT_FORMAT",
            Self::ProjectUninitialized => "GF_PROJECT_UNINITIALIZED",
            Self::ProjectCorrupt => "GF_PROJECT_CORRUPT",
            Self::UnsupportedFilesystem => "GF_UNSUPPORTED_FILESYSTEM",
            Self::WriterBusy => "GF_WRITER_BUSY",
            Self::TransactionConflict => "GF_IDEMPOTENCY_CONFLICT",
            Self::PublicationFailed => "GF_PUBLICATION_FAILED",
            Self::UnsupportedCapabilityVersion => "GF_UNSUPPORTED_CAPABILITY_VERSION",
            Self::CapabilityDisabled => "GF_CAPABILITY_DISABLED",
            Self::TransactionFailed => "GF_TRANSACTION_FAILED",
            Self::CheckpointExists => "GF_CHECKPOINT_EXISTS",
            Self::CheckpointNotFound => "GF_CHECKPOINT_NOT_FOUND",
            Self::CheckpointRegistryCorrupt => "GF_CHECKPOINT_REGISTRY_CORRUPT",
            Self::ReadOnlyView => "GF_READ_ONLY_VIEW",
            Self::ResourceLimit => "GF_RESOURCE_LIMIT",
        }
    }
}

impl fmt::Display for ProjectErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// PropValue — minimal property value type
// ---------------------------------------------------------------------------

/// A graph property value.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum PropValue {
    /// Null / missing value.
    Null,
    /// Boolean.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit float.
    Float(f64),
    /// UTF-8 string.
    Str(String),
    /// Ordered list.
    List(Vec<PropValue>),
}

impl fmt::Display for PropValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::Int(i) => write!(f, "{i}"),
            Self::Float(fl) => write!(f, "{fl}"),
            Self::Str(s) => write!(f, "{s}"),
            Self::List(l) => {
                write!(f, "[")?;
                for (i, v) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// NodeHandle / EdgeHandle
// ---------------------------------------------------------------------------

/// Opaque graph-instance identity used to reject handles from another graph.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct GraphIdentity(Arc<()>);

impl GraphIdentity {
    /// Create a fresh graph-instance identity.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl fmt::Debug for GraphIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GraphIdentity(..)")
    }
}

/// Opaque UUID handle to a node created by one GraphForge instance.
#[derive(Debug, Clone)]
pub struct NodeHandle {
    /// Stable public node identity.
    pub uuid: ::uuid::Uuid,
    /// Primary label.
    pub label: String,
    owner: GraphIdentity,
}

impl NodeHandle {
    /// Construct a handle owned by `owner`.
    #[doc(hidden)]
    #[must_use]
    pub fn new(uuid: ::uuid::Uuid, label: impl Into<String>, owner: GraphIdentity) -> Self {
        Self {
            uuid,
            label: label.into(),
            owner,
        }
    }

    /// Whether this handle belongs to the supplied graph instance.
    #[doc(hidden)]
    #[must_use]
    pub fn belongs_to(&self, owner: &GraphIdentity) -> bool {
        Arc::ptr_eq(&self.owner.0, &owner.0)
    }
}

impl PartialEq for NodeHandle {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}

impl Eq for NodeHandle {}

impl fmt::Display for NodeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(uuid={})", self.label, self.uuid)
    }
}

/// Typed public node identity accepted by path algorithms.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeSelector {
    /// Stable UUID identity.
    Uuid(::uuid::Uuid),
    /// A UUID handle owned by one graph instance.
    Handle(NodeHandle),
    /// A unique node selected by one typed property match within a label.
    Match {
        /// Required node label.
        label: String,
        /// Required property name.
        property: String,
        /// Exact property value.
        value: PropValue,
    },
}

impl NodeSelector {
    /// Parse a canonical UUID selector with a structured validation error.
    pub fn uuid(value: &str) -> Result<Self, GfError> {
        ::uuid::Uuid::parse_str(value)
            .map(Self::Uuid)
            .map_err(|_| GfError::Validation(format!("invalid node UUID {value:?}")))
    }
}

/// Opaque handle to an edge created via `GraphForge::add_edge`.
#[derive(Debug, Clone)]
pub struct EdgeHandle {
    /// Stable public edge identity.
    pub uuid: ::uuid::Uuid,
    /// Relationship type.
    pub rel_type: String,
}

impl EdgeHandle {
    /// Construct a UUID-backed edge handle.
    #[doc(hidden)]
    #[must_use]
    pub fn new(uuid: ::uuid::Uuid, rel_type: impl Into<String>) -> Self {
        Self {
            uuid,
            rel_type: rel_type.into(),
        }
    }
}

impl PartialEq for EdgeHandle {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}

impl Eq for EdgeHandle {}

impl fmt::Display for EdgeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(uuid={})", self.rel_type, self.uuid)
    }
}

// ---------------------------------------------------------------------------
// Option structs for analyst verbs and find
// ---------------------------------------------------------------------------

/// Options for `GraphForge::rank`.
#[derive(Debug, Clone)]
pub struct RankOptions {
    /// Algorithm name (e.g. `"pagerank"`).
    pub by: algorithms::RankAlgorithm,
    /// Optional relationship type filter.
    pub via: Option<String>,
    /// Whether to treat edges as directed.
    pub directed: bool,
    /// Optional property name to write scores back to nodes.
    pub write_property: Option<String>,
}

impl Default for RankOptions {
    fn default() -> Self {
        Self {
            by: algorithms::RankAlgorithm::default(),
            via: None,
            directed: true,
            write_property: None,
        }
    }
}

/// Options for `GraphForge::cluster`.
#[derive(Debug, Clone, Default)]
pub struct ClusterOptions {
    /// Algorithm name (e.g. `"louvain"`).
    pub by: algorithms::ClusterAlgorithm,
    /// Node property containing the feature vector for vector clustering.
    pub vector_property: Option<String>,
    /// Optional relationship type filter.
    pub via: Option<String>,
    /// Whether to treat edges as directed.
    pub directed: bool,
    /// Optional property name to write community IDs back to nodes.
    pub write_property: Option<String>,
}

/// Options for `GraphForge::find`.
#[derive(Debug, Clone)]
pub struct FindOptions {
    /// Optional text query.
    pub query: Option<String>,
    /// Node label filter.
    pub label: Option<String>,
    /// Optional dense vector for similarity search.
    pub vector: Option<Vec<f32>>,
    /// Optional existing graph node whose vector is read from the selected space.
    pub similar_to: Option<NodeSelector>,
    /// Optional text embedded with the selected space's compatible provider contract.
    pub semantic_query: Option<String>,
    /// Maximum number of results to return.
    pub limit: usize,
    /// Vector space identifier (e.g. `"sbert"`).
    pub space: Option<String>,
    /// Explicitly allow the last complete substantially stale generation.
    pub force_stale: bool,
}

impl Default for FindOptions {
    fn default() -> Self {
        Self {
            query: None,
            label: None,
            vector: None,
            similar_to: None,
            semantic_query: None,
            limit: 10,
            space: None,
            force_stale: false,
        }
    }
}

/// Options for `GraphForge::paths`.
#[derive(Debug, Clone)]
pub struct PathsOptions {
    /// Algorithm name (e.g. `"dijkstra"`, `"bfs"`, `"max_flow"`).
    pub by: algorithms::PathAlgorithm,
    /// Optional relationship type filter.
    pub via: Option<String>,
    /// Whether to treat edges as directed.
    pub directed: bool,
    /// Number of paths to return (e.g. Yen's k-shortest).
    pub k: usize,
    /// Optional edge-weight property name.
    pub weight: Option<String>,
    /// Optional graph-native capacity property for flow algorithms.
    pub capacity_property: Option<String>,
    /// Required graph-native unit-cost property for min-cost flow algorithms.
    pub cost_property: Option<String>,
    /// Optional node property containing an A* heuristic estimate.
    pub heuristic: Option<String>,
    /// Maximum number of edge transitions for random-walk paths.
    pub walk_length: Option<usize>,
    /// Seed for reproducible random-walk paths.
    pub seed: Option<u64>,
    /// Canonical resolved terminal UUIDs for explicit multi-terminal algorithms.
    pub terminal_uuids: Vec<[u8; 16]>,
    /// Graph-native node property containing prizes for prize-collecting Steiner trees.
    pub prize_property: Option<String>,
}

impl Default for PathsOptions {
    fn default() -> Self {
        Self {
            by: algorithms::PathAlgorithm::Bfs,
            via: None,
            directed: true,
            k: 1,
            weight: None,
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }
}

/// Options for `GraphForge::analyze`.
#[derive(Debug, Clone)]
pub struct AnalyzeOptions {
    /// Algorithm name (e.g. `"minimum_spanning_tree"`, `"is_dag"`).
    pub by: algorithms::AnalyzeAlgorithm,
    /// Optional relationship type filter.
    pub via: Option<String>,
    /// Whether to treat edges as directed.
    pub directed: bool,
    /// Optional edge-weight property name.
    pub weight: Option<String>,
    /// Requested result count for analyses that enumerate multiple results.
    pub k: Option<usize>,
    /// Optional node property that identifies a graph partition.
    pub partition_property: Option<String>,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self {
            by: algorithms::AnalyzeAlgorithm::IsDag,
            via: None,
            directed: true,
            weight: None,
            k: None,
            partition_property: None,
        }
    }
}

/// Options for `GraphForge::similar`.
#[derive(Debug, Clone)]
pub struct SimilarOptions {
    /// Algorithm name (e.g. `"node_similarity"`, `"knn"`, `"cosine"`).
    pub by: algorithms::SimilarAlgorithm,
    /// Number of neighbours to return.
    pub k: usize,
    /// Optional vector property for vector-based similarity.
    pub vector_property: Option<String>,
    /// Optional relationship type filter.
    pub via: Option<String>,
}

impl Default for SimilarOptions {
    fn default() -> Self {
        Self {
            by: algorithms::SimilarAlgorithm::default(),
            k: 10,
            vector_property: None,
            via: None,
        }
    }
}

// ---------------------------------------------------------------------------
// ExplainStage
// ---------------------------------------------------------------------------

/// Which compiler stage to inspect with `gf_cypher::explain_stage`.
///
/// Use `gf_cypher::explain_stage(cypher, stage)` directly.
///
/// `GraphForge::explain` is a stub that returns [`GfError::NotImplemented`] —
/// the implementation lives in `gf_cypher` to avoid circular crate dependencies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainStage {
    /// Pretty-printed JSON of the parsed [`gf_ast::AstQuery`].
    Ast,
    /// Bound AST after name resolution.
    ///
    /// **Deferred** — the binder produces a `GraphPlan` directly and does not
    /// annotate the `AstQuery`.  This variant returns [`GfError::NotImplemented`]
    /// until a future milestone adds a separate annotation pass.
    BoundAst,
    /// Serialised [`gf_ir::GraphPlan`] produced by the binder.
    ///
    /// Runs in [`gf_ir::OntologyMode::Exploratory`]; all unknown labels and
    /// relation types are auto-interned by the [`gf_ir::RuntimeCatalog`].
    GraphIr,
    /// DataFusion logical plan (not yet implemented).
    LogicalPlan,
    /// DataFusion physical plan (not yet implemented).
    PhysicalPlan,
}

// ---------------------------------------------------------------------------
// GraphForge — public facade
// ---------------------------------------------------------------------------
//
// The `GraphForge` engine facade and its interim `RecordBatch` result type live
// in the top-level `gf-api` crate, not here: `gf-core` is the foundation crate
// that every other crate depends on, so it cannot depend on the pipeline crates
// (`gf-cypher`/`gf-rel`/`gf-exec`) the facade needs without a dependency cycle.
// See #716 / #583. `gf-core` keeps the shared value types below
// ([`GfError`], [`OntologyMode`], [`NodeHandle`], [`RankOptions`], …) that the
// facade composes.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_display() {
        assert_eq!(Span::new(0, 5).to_string(), "0..5");
    }

    #[test]
    fn gf_error_not_implemented() {
        let e = GfError::NotImplemented("execute");
        assert!(e.to_string().contains("execute"));
    }

    #[test]
    fn node_handle_display() {
        let owner = GraphIdentity::new();
        let uuid = ::uuid::Uuid::from_bytes([1; 16]);
        let h = NodeHandle::new(uuid, "Person", owner.clone());
        assert!(h.to_string().contains("Person"));
        assert!(h.to_string().contains(&uuid.to_string()));
        assert!(h.belongs_to(&owner));
        assert!(!h.belongs_to(&GraphIdentity::new()));
        assert_eq!(h, NodeHandle::new(uuid, "Other", GraphIdentity::new()));
    }

    #[test]
    fn edge_handle_identity_and_display_are_uuid_based() {
        let uuid = ::uuid::Uuid::from_bytes([2; 16]);
        let handle = EdgeHandle::new(uuid, "KNOWS");
        assert_eq!(handle.uuid, uuid);
        assert_eq!(handle.rel_type, "KNOWS");
        assert_eq!(handle, EdgeHandle::new(uuid, "OTHER"));
        assert_ne!(
            handle,
            EdgeHandle::new(::uuid::Uuid::from_bytes([3; 16]), "KNOWS"),
        );
        assert_eq!(handle.to_string(), format!("KNOWS(uuid={uuid})"));
        assert!(!handle.to_string().starts_with("Edge(id="));
    }

    #[test]
    fn paths_options_default_to_the_canonical_bfs_contract() {
        let options = PathsOptions::default();
        assert_eq!(options.by, algorithms::PathAlgorithm::Bfs);
        assert_eq!(options.via, None);
        assert!(options.directed);
        assert_eq!(options.k, 1);
        assert_eq!(options.weight, None);
        assert!(options.terminal_uuids.is_empty());
        assert_eq!(options.prize_property, None);
    }

    #[test]
    fn analyze_options_default_to_the_canonical_is_dag_contract() {
        let options = AnalyzeOptions::default();
        assert_eq!(options.by, algorithms::AnalyzeAlgorithm::IsDag);
        assert_eq!(options.via, None);
        assert!(options.directed);
    }

    #[test]
    fn find_options_default_to_no_query_or_stale_override() {
        let options = FindOptions::default();
        assert_eq!(options.query, None);
        assert_eq!(options.label, None);
        assert_eq!(options.vector, None);
        assert_eq!(options.similar_to, None);
        assert_eq!(options.semantic_query, None);
        assert_eq!(options.limit, 10);
        assert_eq!(options.space, None);
        assert!(!options.force_stale);
    }
}
