//! GraphForge shared identities, values, options, and facade errors.
//!
//! Compiler, execution, storage, and domain crates consume these shared contracts.
//! Independent utility crates can define their own types without depending on core.
//! The public engine facade lives in `graphforge-api`, above the pipeline crates.
//! Stage and transport crates retain dedicated error types; relevant boundaries
//! classify errors for the facade and bindings.
#![forbid(unsafe_code)]

pub mod algorithms;
pub mod canonical;
pub mod embedding_options;
pub mod identifier;
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
/// `"strict"`).  Lives in `graphforge-core` so that the project manifest
/// ([`manifest::ProjectManifest`]) and the binder (`graphforge-ir`) share one
/// definition; `graphforge-ir` re-exports it as `graphforge_ir::OntologyMode`.
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

/// Shared error classifications used by the engine facade and bindings.
///
/// Compiler stages, I/O, discovery, and other subsystems also expose dedicated
/// error types. Boundary adapters classify them as needed; [`GfError::code`]
/// supplies stable public codes rather than a one-to-one variant/exception map.
#[derive(thiserror::Error, Debug, Clone)]
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
    /// can point at the offending token; `msg` lists every binder error.
    ///
    /// Publicly classified with [`GfError::Parse`] as `GF_PARSE` / `ParseError`
    /// — semantic query-structure failures share the parse fault domain.
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

    /// A structured knowledge/epistemic public API failure.
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

mod error_conversion;

impl GfError {
    /// Return the stable public error code for this failure.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::NotImplemented(_) => "GF_NOT_IMPLEMENTED",
            Self::Parse { .. } | Self::Bind { .. } => "GF_PARSE",
            Self::Plan(_) => "GF_PLAN",
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

/// Stable non-project knowledge/epistemic API error codes.
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
    /// A staged write cannot be applied to the latest committed generation.
    WriteConflict,
    /// Compatible contention exceeded the caller's bounded rebase attempts.
    RebaseExhausted,
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
            Self::WriteConflict => "GF_WRITE_CONFLICT",
            Self::RebaseExhausted => "GF_REBASE_EXHAUSTED",
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

/// Consumer-neutral temporal values accepted at GraphForge data boundaries.
///
/// Calendar and wall-clock components remain separate: durations are never
/// collapsed into elapsed nanoseconds, and zone-bearing datetimes retain both
/// the observed UTC offset and optional IANA zone identifier.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TemporalValue {
    /// Calendar duration with independent months, days, seconds, and nanoseconds.
    Duration {
        /// Calendar months.
        months: i64,
        /// Calendar days.
        days: i64,
        /// Whole elapsed seconds below the calendar components.
        seconds: i64,
        /// Sub-second nanoseconds.
        nanos: i64,
    },
    /// UTC instant as microseconds from the Unix epoch.
    UtcDateTime {
        /// Signed microseconds from the Unix epoch.
        epoch_micros: i64,
    },
    /// Calendar date as days from the Unix epoch.
    Date {
        /// Signed days from the Unix epoch.
        epoch_days: i64,
    },
    /// Local date and time without an offset or zone.
    LocalDateTime {
        /// Signed days from the Unix epoch.
        epoch_days: i64,
        /// Nanoseconds since local midnight.
        nanos: i64,
    },
    /// Local wall-clock time without an offset.
    LocalTime {
        /// Nanoseconds since local midnight.
        nanos: i64,
    },
    /// Wall-clock time with its explicit UTC offset in seconds.
    OffsetTime {
        /// Nanoseconds since local midnight.
        nanos: i64,
        /// Signed UTC offset in seconds.
        offset_seconds: i32,
    },
    /// Date and time with an explicit offset and optional IANA zone identity.
    ZonedDateTime {
        /// Signed days from the Unix epoch in the represented local date.
        epoch_days: i64,
        /// Nanoseconds since local midnight.
        nanos: i64,
        /// Signed observed UTC offset in seconds.
        offset_seconds: i32,
        /// Optional IANA zone identity; `None` means offset-only.
        zone: Option<String>,
    },
}

impl TemporalValue {
    /// Validate the canonical ranges shared by scalar and bulk ingestion.
    pub fn validate(&self) -> Result<(), GfError> {
        const NANOS_PER_DAY: i64 = 86_400_000_000_000;
        const MAX_OFFSET_SECONDS: i32 = 18 * 60 * 60;
        let validate_time = |nanos: i64| {
            if (0..NANOS_PER_DAY).contains(&nanos) {
                Ok(())
            } else {
                Err(GfError::Validation(
                    "temporal nanoseconds must be within one day".into(),
                ))
            }
        };
        let validate_offset = |offset: i32| {
            if (-MAX_OFFSET_SECONDS..=MAX_OFFSET_SECONDS).contains(&offset) {
                Ok(())
            } else {
                Err(GfError::Validation(
                    "temporal UTC offset must be within plus or minus 18 hours".into(),
                ))
            }
        };
        match self {
            Self::Duration { nanos, .. } if !(-999_999_999..=999_999_999).contains(nanos) => {
                Err(GfError::Validation(
                    "duration nanoseconds must be between -999999999 and 999999999".into(),
                ))
            }
            Self::LocalDateTime { nanos, .. } | Self::LocalTime { nanos } => validate_time(*nanos),
            Self::OffsetTime {
                nanos,
                offset_seconds,
            } => {
                validate_time(*nanos)?;
                validate_offset(*offset_seconds)
            }
            Self::ZonedDateTime {
                nanos,
                offset_seconds,
                zone,
                ..
            } => {
                validate_time(*nanos)?;
                validate_offset(*offset_seconds)?;
                if let Some(zone) = zone
                    && (zone.is_empty() || zone.len() > 255 || zone.chars().any(char::is_control))
                {
                    return Err(GfError::Validation(
                        "temporal zone must be nonempty, control-free UTF-8 up to 255 bytes".into(),
                    ));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Coordinate reference systems certified by GraphForge's spatial v1 profile.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SpatialCrs {
    /// WGS 84 longitude/latitude in canonical x/y order.
    Epsg4326,
    /// Web Mercator easting/northing in canonical x/y order.
    Epsg3857,
    /// Standards-valid CRS identifier preserved for interchange only.
    Preserved(String),
}

impl serde::Serialize for SpatialCrs {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            Self::Epsg4326 => "EPSG:4326",
            Self::Epsg3857 => "EPSG:3857",
            Self::Preserved(value) => value,
        })
    }
}

impl<'de> serde::Deserialize<'de> for SpatialCrs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "EPSG:4326" => Self::Epsg4326,
            "EPSG:3857" => Self::Epsg3857,
            _ => Self::Preserved(value),
        })
    }
}

/// Homogeneous geometry kinds in GraphForge's spatial v1 profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpatialGeometryType {
    /// One x/y coordinate.
    Point,
    /// One ordered sequence of vertices.
    LineString,
    /// One polygon represented as ordered rings.
    Polygon,
    /// A collection of points.
    MultiPoint,
    /// A collection of line strings.
    MultiLineString,
    /// A collection of polygons.
    MultiPolygon,
}

/// Complete homogeneous spatial property type.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SpatialType {
    /// Homogeneous geometry kind.
    pub geometry: SpatialGeometryType,
    /// Coordinate reference system for every coordinate.
    pub crs: SpatialCrs,
}

/// Canonical f64 coordinate payload for one spatial property value.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SpatialCoordinates {
    /// Point coordinate.
    Point([f64; 2]),
    /// Line-string vertices.
    LineString(Vec<[f64; 2]>),
    /// Polygon rings and their vertices.
    Polygon(Vec<Vec<[f64; 2]>>),
    /// Multi-point coordinates.
    MultiPoint(Vec<[f64; 2]>),
    /// Multi-line-string vertices.
    MultiLineString(Vec<Vec<[f64; 2]>>),
    /// Multi-polygon rings and vertices.
    MultiPolygon(Vec<Vec<Vec<[f64; 2]>>>),
}

/// One canonical typed spatial property value.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SpatialValue {
    /// Geometry kind and CRS.
    pub spatial_type: SpatialType,
    /// Coordinates matching `spatial_type.geometry`.
    pub coordinates: SpatialCoordinates,
    /// Original extension name when the value is preserved-only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension_name: Option<String>,
    /// Original extension metadata JSON when the value is preserved-only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension_metadata: Option<String>,
}

impl SpatialValue {
    /// Whether this value is preserved for interchange but not certified for computation.
    #[must_use]
    pub fn is_preserved_only(&self) -> bool {
        matches!(self.spatial_type.crs, SpatialCrs::Preserved(_))
            || self.extension_name.is_some()
            || self.extension_metadata.is_some()
    }

    /// Validate the explicit envelope required for preserved-only values.
    pub fn validate_interchange_profile(&self) -> Result<(), &'static str> {
        if !self.is_preserved_only() {
            return Ok(());
        }
        let (Some(name), Some(metadata)) = (&self.extension_name, &self.extension_metadata) else {
            return Err(
                "preserved-only spatial values require extension_name and extension_metadata",
            );
        };
        if name.is_empty() {
            return Err("preserved-only spatial extension_name must not be empty");
        }
        let trimmed = metadata.trim();
        let valid_metadata = trimmed.starts_with('{')
            && trimmed.ends_with('}')
            && trimmed.contains("\"crs\"")
            && trimmed.contains(':');
        if !valid_metadata {
            return Err("preserved-only spatial extension_metadata must contain a CRS");
        }
        Ok(())
    }
}

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
    /// Typed temporal value with consumer-neutral Arrow semantics.
    Temporal(TemporalValue),
    /// Canonical typed spatial value.
    Spatial(SpatialValue),
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
            Self::Temporal(value) => write!(f, "temporal({value:?})"),
            Self::Spatial(value) => write!(
                f,
                "spatial({:?}, {:?})",
                value.spatial_type, value.coordinates
            ),
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

/// Which compiler stage to inspect with `graphforge_cypher::explain_stage`.
///
/// Use `graphforge_cypher::explain_stage(cypher, stage)` directly.
///
/// `GraphForge::explain` is a stub that returns [`GfError::NotImplemented`] —
/// the implementation lives in `graphforge_cypher` to avoid circular crate dependencies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainStage {
    /// Pretty-printed JSON of the parsed [`graphforge_ast::AstQuery`].
    Ast,
    /// Bound AST after name resolution.
    ///
    /// **Deferred** — the binder produces a `GraphPlan` directly and does not
    /// annotate the `AstQuery`.  This variant returns [`GfError::NotImplemented`]
    /// until a future milestone adds a separate annotation pass.
    BoundAst,
    /// Serialised [`graphforge_ir::GraphPlan`] produced by the binder.
    ///
    /// Runs in [`graphforge_ir::OntologyMode::Exploratory`]; all unknown labels and
    /// relation types are auto-interned by the [`graphforge_ir::RuntimeCatalog`].
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
// in the top-level `graphforge-api` crate, not here: `graphforge-core` is the foundation crate
// that every other crate depends on, so it cannot depend on the pipeline crates
// (`graphforge-cypher`/`graphforge-rel`/`graphforge-exec`) the facade needs without a dependency cycle.
// See #716 / #583. `graphforge-core` keeps the shared value types below
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

    #[test]
    fn stable_error_code_enums_cover_every_public_variant() {
        let api = [
            (ApiErrorCode::NotFound, "GF_NOT_FOUND"),
            (ApiErrorCode::Cancelled, "GF_CANCELLED"),
            (ApiErrorCode::ResourceLimit, "GF_RESOURCE_LIMIT"),
            (ApiErrorCode::PageInvalid, "GF_PAGE_INVALID"),
            (ApiErrorCode::PageSnapshotGone, "GF_PAGE_SNAPSHOT_GONE"),
            (ApiErrorCode::SchemaMismatch, "GF_SCHEMA_MISMATCH"),
            (ApiErrorCode::UnknownArgument, "GF_UNKNOWN_ARGUMENT"),
            (ApiErrorCode::AmbiguousProjection, "GF_AMBIGUOUS_PROJECTION"),
            (ApiErrorCode::IdentityConflict, "GF_IDENTITY_CONFLICT"),
            (
                ApiErrorCode::FingerprintCollision,
                "GF_FINGERPRINT_COLLISION",
            ),
            (ApiErrorCode::ResultNotRetained, "GF_RESULT_NOT_RETAINED"),
        ];
        for (code, spelling) in api {
            assert_eq!(code.as_str(), spelling);
            assert_eq!(code.to_string(), spelling);
        }

        let project = [
            (
                ProjectErrorCode::UnsupportedProjectFormat,
                "GF_UNSUPPORTED_PROJECT_FORMAT",
            ),
            (
                ProjectErrorCode::ProjectUninitialized,
                "GF_PROJECT_UNINITIALIZED",
            ),
            (ProjectErrorCode::ProjectCorrupt, "GF_PROJECT_CORRUPT"),
            (
                ProjectErrorCode::UnsupportedFilesystem,
                "GF_UNSUPPORTED_FILESYSTEM",
            ),
            (ProjectErrorCode::WriterBusy, "GF_WRITER_BUSY"),
            (ProjectErrorCode::WriteConflict, "GF_WRITE_CONFLICT"),
            (ProjectErrorCode::RebaseExhausted, "GF_REBASE_EXHAUSTED"),
            (
                ProjectErrorCode::TransactionConflict,
                "GF_IDEMPOTENCY_CONFLICT",
            ),
            (ProjectErrorCode::PublicationFailed, "GF_PUBLICATION_FAILED"),
            (
                ProjectErrorCode::UnsupportedCapabilityVersion,
                "GF_UNSUPPORTED_CAPABILITY_VERSION",
            ),
            (
                ProjectErrorCode::CapabilityDisabled,
                "GF_CAPABILITY_DISABLED",
            ),
            (ProjectErrorCode::TransactionFailed, "GF_TRANSACTION_FAILED"),
            (ProjectErrorCode::CheckpointExists, "GF_CHECKPOINT_EXISTS"),
            (
                ProjectErrorCode::CheckpointNotFound,
                "GF_CHECKPOINT_NOT_FOUND",
            ),
            (
                ProjectErrorCode::CheckpointRegistryCorrupt,
                "GF_CHECKPOINT_REGISTRY_CORRUPT",
            ),
            (ProjectErrorCode::ReadOnlyView, "GF_READ_ONLY_VIEW"),
            (ProjectErrorCode::ResourceLimit, "GF_RESOURCE_LIMIT"),
        ];
        for (code, spelling) in project {
            assert_eq!(code.as_str(), spelling);
            assert_eq!(code.to_string(), spelling);
        }
    }

    #[test]
    fn public_value_display_and_selector_validation_cover_all_shapes() {
        let values = [
            (PropValue::Null, "null"),
            (PropValue::Bool(true), "true"),
            (PropValue::Int(-7), "-7"),
            (PropValue::Float(1.5), "1.5"),
            (PropValue::Str("x".into()), "x"),
            (
                PropValue::List(vec![PropValue::Int(1), PropValue::Null]),
                "[1, null]",
            ),
        ];
        for (value, rendered) in values {
            assert_eq!(value.to_string(), rendered);
        }
        assert!(matches!(
            NodeSelector::uuid("00000000-0000-0000-0000-000000000001"),
            Ok(NodeSelector::Uuid(_))
        ));
        assert!(matches!(
            NodeSelector::uuid("not-a-uuid"),
            Err(GfError::Validation(_))
        ));
        assert_eq!(format!("{:?}", GraphIdentity::new()), "GraphIdentity(..)");
    }

    #[test]
    fn every_gf_error_fault_domain_has_a_stable_code() {
        let span = Span::new(1, 2);
        let errors = [
            (GfError::NotImplemented("x"), "GF_NOT_IMPLEMENTED"),
            (
                GfError::Parse {
                    msg: "x".into(),
                    span,
                },
                "GF_PARSE",
            ),
            (
                GfError::Bind {
                    msg: "x".into(),
                    span,
                },
                "GF_PARSE",
            ),
            (GfError::Plan("x".into()), "GF_PLAN"),
            (GfError::Execution("x".into()), "GF_EXECUTION"),
            (
                GfError::Provider {
                    class: "c".into(),
                    provider: "p".into(),
                    model: "m".into(),
                },
                "GF_EXECUTION",
            ),
            (GfError::Storage("x".into()), "GF_IO"),
            (
                GfError::Project {
                    code: ProjectErrorCode::ProjectCorrupt,
                    message: "x".into(),
                },
                "GF_PROJECT_CORRUPT",
            ),
            (
                GfError::Api {
                    code: ApiErrorCode::NotFound,
                    message: "x".into(),
                },
                "GF_NOT_FOUND",
            ),
            (GfError::Lifecycle("x".into()), "GF_LIFECYCLE"),
            (GfError::Validation("x".into()), "GF_VALIDATION"),
            (GfError::Ontology("x".into()), "GF_ONTOLOGY"),
        ];
        for (error, code) in errors {
            assert_eq!(error.code(), code);
        }
    }
}
