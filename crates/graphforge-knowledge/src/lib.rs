//! Immutable UUID-referenced analytical knowledge.
//!
//! This crate owns knowledge records, validation, canonical fingerprints,
//! deterministic ordering, and Arrow schemas. It deliberately has no storage,
//! graph, execution, or provenance dependency.
#![forbid(unsafe_code)]

mod belief_projection;
mod hypothesis;
mod reasoning;
mod status;
mod supersession;
mod valid_time;

pub use hypothesis::{
    HYPOTHESIS_GROUP_CONTRACT_VERSION, HYPOTHESIS_GROUP_SCHEMA, HYPOTHESIS_KEY_POLICY_VERSION,
    HYPOTHESIS_MEMBERSHIP_CONTRACT_VERSION, HYPOTHESIS_MEMBERSHIP_SCHEMA,
    HYPOTHESIS_SELECTION_CONTRACT_VERSION, HYPOTHESIS_SELECTION_SCHEMA,
    HYPOTHESIS_STATE_POLICY_VERSION, HypothesisGroup, HypothesisLedger, HypothesisMembershipAction,
    HypothesisMembershipEvent, HypothesisSelectionEvent, MAX_HYPOTHESIS_QUESTION_KEY_BYTES,
};
pub use reasoning::{
    EPISTEMIC_CAPABILITY_VERSION, MAX_REASONING_CONTENT_BYTES,
    REASONING_CONTENT_FORMAT_REGISTRY_VERSION, REASONING_CONTRACT_VERSION,
    REASONING_KIND_REGISTRY_VERSION, REASONING_SCHEMA, ReasoningContentFormat, ReasoningKind,
    ReasoningLedger, ReasoningRecord,
};
pub use status::{
    ASSERTION_STATUS_CONTRACT_VERSION, ASSERTION_STATUS_REGISTRY_VERSION, ASSERTION_STATUS_SCHEMA,
    AssertionStatus, AssertionStatusEvent, AssertionStatusLedger,
};
pub use supersession::{
    ASSERTION_SUPERSESSION_CONTRACT_VERSION, ASSERTION_SUPERSESSION_POLICY_VERSION,
    ASSERTION_SUPERSESSION_SCHEMA, AssertionSupersession, AssertionSupersessionLedger,
};
pub use valid_time::{
    ASSERTION_VALIDITY_CONTRACT_VERSION, ASSERTION_VALIDITY_POLICY_VERSION,
    ASSERTION_VALIDITY_SCHEMA, AssertionValidityEvent, AssertionValidityLedger,
    VALID_TIME_CAPABILITY_VERSION,
};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float64Array, StringArray,
    TimestampMicrosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalError, CanonicalWriter, fingerprint,
};
use uuid::{Uuid, Version};

/// Knowledge capability contract implemented by this crate.
pub const KNOWLEDGE_CAPABILITY_VERSION: u32 = 1;
/// Assertion record contract.
pub const ASSERTION_CONTRACT_VERSION: u32 = 1;
/// Assertion-to-graph reference record contract.
pub const ASSERTION_GRAPH_REF_CONTRACT_VERSION: u32 = 1;
/// Confidence-assessment record contract.
pub const CONFIDENCE_ASSESSMENT_CONTRACT_VERSION: u32 = 1;
/// Confidence-input snapshot record contract.
pub const CONFIDENCE_INPUT_CONTRACT_VERSION: u32 = 1;
/// Evidence-link record contract.
pub const EVIDENCE_LINK_CONTRACT_VERSION: u32 = 1;
/// Algorithm-run identity record contract.
pub const ALGORITHM_RUN_CONTRACT_VERSION: u32 = 1;
/// Algorithm-run lifecycle event contract.
pub const ALGORITHM_RUN_EVENT_CONTRACT_VERSION: u32 = 1;
/// Closed confidence-policy registry version.
pub const CONFIDENCE_POLICY_REGISTRY_VERSION: u32 = 1;
/// Closed graph-object-kind registry version.
pub const GRAPH_OBJECT_KIND_REGISTRY_VERSION: u32 = 1;
/// Closed assertion-role registry version.
pub const ASSERTION_GRAPH_ROLE_REGISTRY_VERSION: u32 = 1;
/// Closed evidence source-kind registry version.
pub const EVIDENCE_SOURCE_KIND_REGISTRY_VERSION: u32 = 1;
/// Closed evidence role registry version.
pub const EVIDENCE_ROLE_REGISTRY_VERSION: u32 = 1;
/// Closed algorithm-run lifecycle registry version.
pub const ALGORITHM_RUN_STATE_REGISTRY_VERSION: u32 = 1;
/// Per-participant row bound.
pub const MAX_KNOWLEDGE_ROWS: usize = 1_000_000;

/// Authoritative `knowledge/assertions.parquet` schema.
pub static ASSERTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("assertion_uuid", false),
        Field::new("claim", DataType::Utf8, false),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

/// Authoritative `knowledge/assertion_graph_refs.parquet` schema.
pub static ASSERTION_GRAPH_REF_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("assertion_uuid", false),
        uuid_field("graph_uuid", false),
        Field::new("graph_kind", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("ordinal", DataType::UInt32, false),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

/// Authoritative `knowledge/confidence_assessments.parquet` schema.
pub static CONFIDENCE_ASSESSMENT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("confidence_uuid", false),
        uuid_field("assertion_uuid", false),
        Field::new("policy", DataType::Utf8, false),
        Field::new("policy_version", DataType::UInt32, false),
        Field::new("value", DataType::Float64, true),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

/// Authoritative `knowledge/confidence_inputs.parquet` schema.
pub static CONFIDENCE_INPUT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("confidence_uuid", false),
        uuid_field("input_confidence_uuid", false),
        Field::new("input_value", DataType::Float64, true),
        Field::new("ordinal", DataType::UInt32, false),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

/// Authoritative `knowledge/evidence.parquet` schema.
pub static EVIDENCE_LINK_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("evidence_uuid", false),
        uuid_field("assertion_uuid", false),
        uuid_field("source_uuid", false),
        Field::new("source_kind", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("weight", DataType::Float64, true),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

/// Authoritative `knowledge/algorithm_runs.parquet` schema.
pub static ALGORITHM_RUN_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("run_uuid", false),
        Field::new("algorithm", DataType::Utf8, false),
        Field::new("algorithm_version", DataType::UInt32, false),
        Field::new("descriptor_version", DataType::UInt32, false),
        Field::new("descriptor", DataType::Binary, false),
        Field::new(
            "projection_fingerprint",
            DataType::FixedSizeBinary(32),
            false,
        ),
        uuid_field("provenance_uuid", false),
        Field::new(
            "started_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

/// Authoritative `knowledge/algorithm_run_events.parquet` schema.
pub static ALGORITHM_RUN_EVENT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("event_uuid", false),
        uuid_field("run_uuid", false),
        Field::new("state", DataType::Utf8, false),
        Field::new("result_fingerprint", DataType::FixedSizeBinary(32), true),
        Field::new("error_code", DataType::Utf8, true),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        uuid_field("provenance_uuid", false),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static ASSERTION_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"assertion/1|assertion_uuid:fixed[16]:required|claim:utf8:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered assertion schema is within canonical bounds")
});

static ASSERTION_GRAPH_REF_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
            CanonicalDomain::Schema,
            CANONICAL_CONTRACT_VERSION,
            b"assertion_graph_ref/1|assertion_uuid:fixed[16]:required|graph_uuid:fixed[16]:required|graph_kind:utf8:required|role:utf8:required|ordinal:u32:required|contract_version:u32:required",
        )
        .expect("registered assertion graph-ref schema is within canonical bounds")
});

static CONFIDENCE_ASSESSMENT_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"confidence_assessment/1|confidence_uuid:fixed[16]:required|assertion_uuid:fixed[16]:required|policy:utf8:required|policy_version:u32:required|value:f64:nullable|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered confidence-assessment schema is within canonical bounds")
});

static CONFIDENCE_INPUT_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"confidence_input/1|confidence_uuid:fixed[16]:required|input_confidence_uuid:fixed[16]:required|input_value:f64:nullable|ordinal:u32:required|contract_version:u32:required",
    )
    .expect("registered confidence-input schema is within canonical bounds")
});

static EVIDENCE_LINK_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"evidence_link/1|evidence_uuid:fixed[16]:required|assertion_uuid:fixed[16]:required|source_uuid:fixed[16]:required|source_kind:utf8:required|role:utf8:required|weight:f64:nullable|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered evidence-link schema is within canonical bounds")
});

static ALGORITHM_RUN_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"algorithm_run/1|run_uuid:fixed[16]:required|algorithm:utf8:required|algorithm_version:u32:required|descriptor_version:u32:required|descriptor:binary:required|projection_fingerprint:fixed[32]:required|provenance_uuid:fixed[16]:required|started_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered algorithm-run schema is within canonical bounds")
});

static ALGORITHM_RUN_EVENT_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"algorithm_run_event/1|event_uuid:fixed[16]:required|run_uuid:fixed[16]:required|state:utf8:required|result_fingerprint:fixed[32]:nullable|error_code:utf8:nullable|recorded_at:timestamp_us_utc:required|provenance_uuid:fixed[16]:required|contract_version:u32:required",
    )
    .expect("registered algorithm-run-event schema is within canonical bounds")
});

fn uuid_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), nullable)
}

/// Closed graph UUID kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphObjectKind {
    /// Public node UUID.
    Node,
    /// Public edge UUID.
    Edge,
}

impl GraphObjectKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "edge",
        }
    }

    fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "node" => Ok(Self::Node),
            "edge" => Ok(Self::Edge),
            _ => Err(invalid("graph_kind", "unknown closed value")),
        }
    }
}

/// Closed assertion-to-graph role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssertionGraphRole {
    /// Claim subject.
    Subject,
    /// Claim object.
    Object,
    /// Context needed to interpret the claim.
    Context,
}

impl AssertionGraphRole {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Subject => "subject",
            Self::Object => "object",
            Self::Context => "context",
        }
    }

    fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "subject" => Ok(Self::Subject),
            "object" => Ok(Self::Object),
            "context" => Ok(Self::Context),
            _ => Err(invalid("role", "unknown closed value")),
        }
    }
}

/// One immutable analytical assertion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assertion {
    /// Caller-supplied UUIDv7 identity and idempotency key.
    pub assertion_uuid: Uuid,
    /// Exact validated UTF-8 claim bytes.
    pub claim: String,
    /// Producing provenance event.
    pub provenance_uuid: Uuid,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
    /// Assertion record contract.
    pub contract_version: u32,
}

impl Assertion {
    /// Construct one assertion record.
    pub fn new(
        assertion_uuid: Uuid,
        claim: String,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        require_v7(assertion_uuid, "assertion_uuid")?;
        require_uuid(provenance_uuid, "provenance_uuid")?;
        validate_claim(&claim)?;
        Ok(Self {
            assertion_uuid,
            claim,
            provenance_uuid,
            recorded_at_micros,
            contract_version: ASSERTION_CONTRACT_VERSION,
        })
    }
}

/// One immutable assertion-to-graph UUID reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssertionGraphRef {
    /// Owning assertion.
    pub assertion_uuid: Uuid,
    /// Referenced public graph UUID.
    pub graph_uuid: Uuid,
    /// Node or edge.
    pub graph_kind: GraphObjectKind,
    /// Subject/object/context role.
    pub role: AssertionGraphRole,
    /// Caller-significant contiguous position within the role.
    pub ordinal: u32,
    /// Reference record contract.
    pub contract_version: u32,
}

impl AssertionGraphRef {
    /// Construct one graph reference.
    pub fn new(
        assertion_uuid: Uuid,
        graph_uuid: Uuid,
        graph_kind: GraphObjectKind,
        role: AssertionGraphRole,
        ordinal: u32,
    ) -> Result<Self, KnowledgeError> {
        require_v7(assertion_uuid, "assertion_uuid")?;
        require_uuid(graph_uuid, "graph_uuid")?;
        Ok(Self {
            assertion_uuid,
            graph_uuid,
            graph_kind,
            role,
            ordinal,
            contract_version: ASSERTION_GRAPH_REF_CONTRACT_VERSION,
        })
    }
}

/// Validated immutable assertion participant content.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AssertionLedger {
    /// Assertions ordered by `(recorded_at, assertion_uuid)`.
    pub assertions: Vec<Assertion>,
    /// References ordered by assertion and the public reference sort key.
    pub graph_refs: Vec<AssertionGraphRef>,
}

impl AssertionLedger {
    /// Validate, sort, and construct assertion content.
    pub fn new(
        mut assertions: Vec<Assertion>,
        mut graph_refs: Vec<AssertionGraphRef>,
    ) -> Result<Self, KnowledgeError> {
        validate_rows(&assertions, &graph_refs)?;
        let times = assertions
            .iter()
            .map(|row| (row.assertion_uuid, row.recorded_at_micros))
            .collect::<HashMap<_, _>>();
        assertions.sort_by_key(|row| (row.recorded_at_micros, row.assertion_uuid));
        graph_refs.sort_by_key(|row| {
            (
                times[&row.assertion_uuid],
                row.assertion_uuid,
                role_order(row.role),
                row.ordinal,
                kind_order(row.graph_kind),
                row.graph_uuid,
            )
        });
        Ok(Self {
            assertions,
            graph_refs,
        })
    }

    /// Merge a staged assertion set idempotently.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut assertions = self.assertions.clone();
        let mut refs = self.graph_refs.clone();
        let mut by_id = assertions
            .iter()
            .cloned()
            .map(|row| (row.assertion_uuid, row))
            .collect::<HashMap<_, _>>();
        for row in &staged.assertions {
            if let Some(existing) = by_id.get(&row.assertion_uuid)
                && (existing != row
                    || refs_for(&refs, row.assertion_uuid)
                        != refs_for(&staged.graph_refs, row.assertion_uuid))
            {
                return Err(KnowledgeError::Conflict("assertion_uuid"));
            }
            if by_id.insert(row.assertion_uuid, row.clone()).is_none() {
                assertions.push(row.clone());
                refs.extend(
                    staged
                        .graph_refs
                        .iter()
                        .filter(|reference| reference.assertion_uuid == row.assertion_uuid)
                        .cloned(),
                );
            }
        }
        Self::new(assertions, refs)
    }

    /// Canonical assertion fingerprint over exact claim bytes and sorted refs.
    pub fn assertion_fingerprint(&self, assertion_uuid: Uuid) -> Result<[u8; 32], KnowledgeError> {
        let assertion = self
            .assertions
            .iter()
            .find(|row| row.assertion_uuid == assertion_uuid)
            .ok_or(KnowledgeError::Dangling("assertion_uuid"))?;
        let refs = refs_for(&self.graph_refs, assertion_uuid);
        let mut writer = CanonicalWriter::new();
        writer.raw(b"GFAS")?;
        writer.u32(ASSERTION_CONTRACT_VERSION)?;
        writer.text(&assertion.claim)?;
        writer.u64(
            u64::try_from(refs.len()).map_err(|_| KnowledgeError::Limit {
                participant: "assertion_graph_refs",
                observed: refs.len(),
                limit: MAX_KNOWLEDGE_ROWS,
            })?,
        )?;
        for reference in refs {
            writer.text(reference.role.as_str())?;
            writer.u32(reference.ordinal)?;
            writer.text(reference.graph_kind.as_str())?;
            writer.raw(reference.graph_uuid.as_bytes())?;
        }
        Ok(fingerprint(
            CanonicalDomain::Assertion,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )?)
    }

    /// Build the authoritative assertion Arrow batch.
    pub fn assertion_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        assertion_batch(&self.assertions)
    }

    /// Build the authoritative assertion graph-reference Arrow batch.
    pub fn graph_ref_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        graph_ref_batch(&self.graph_refs)
    }

    /// Decode authoritative Arrow batches and re-run every invariant.
    pub fn from_batches(
        assertion_batches: &[RecordBatch],
        graph_ref_batches: &[RecordBatch],
    ) -> Result<Self, KnowledgeError> {
        let mut assertions = Vec::new();
        for batch in assertion_batches {
            require_schema(batch, &ASSERTION_SCHEMA, "assertion.schema")?;
            let ids = fixed_column(batch, "assertion_uuid")?;
            let claims = string_column(batch, "claim")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                assertions.push(Assertion {
                    assertion_uuid: uuid_at(ids, row, "assertion_uuid")?,
                    claim: required_text(claims, row, "claim")?.to_owned(),
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        let mut refs = Vec::new();
        for batch in graph_ref_batches {
            require_schema(
                batch,
                &ASSERTION_GRAPH_REF_SCHEMA,
                "assertion_graph_ref.schema",
            )?;
            let assertions_col = fixed_column(batch, "assertion_uuid")?;
            let graph_ids = fixed_column(batch, "graph_uuid")?;
            let kinds = string_column(batch, "graph_kind")?;
            let roles = string_column(batch, "role")?;
            let ordinals = u32_column(batch, "ordinal")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                refs.push(AssertionGraphRef {
                    assertion_uuid: uuid_at(assertions_col, row, "assertion_uuid")?,
                    graph_uuid: uuid_at(graph_ids, row, "graph_uuid")?,
                    graph_kind: GraphObjectKind::parse(required_text(kinds, row, "graph_kind")?)?,
                    role: AssertionGraphRole::parse(required_text(roles, row, "role")?)?,
                    ordinal: required_u32(ordinals, row, "ordinal")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        Self::new(assertions, refs)
    }
}

/// Closed evidence source kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSourceKind {
    /// Caller-managed document identity.
    Document,
    /// Caller-managed observation identity.
    Observation,
    /// Existing graph node identity.
    GraphNode,
    /// Existing graph edge identity.
    GraphEdge,
}

impl EvidenceSourceKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Observation => "observation",
            Self::GraphNode => "graph_node",
            Self::GraphEdge => "graph_edge",
        }
    }

    fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "document" => Ok(Self::Document),
            "observation" => Ok(Self::Observation),
            "graph_node" => Ok(Self::GraphNode),
            "graph_edge" => Ok(Self::GraphEdge),
            _ => Err(invalid("source_kind", "unknown closed value")),
        }
    }
}

/// Closed relationship between evidence and an assertion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceRole {
    /// Evidence supports the assertion.
    Supports,
    /// Evidence contradicts the assertion.
    Contradicts,
    /// Evidence supplies interpretation context.
    Context,
}

impl EvidenceRole {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supports => "supports",
            Self::Contradicts => "contradicts",
            Self::Context => "context",
        }
    }

    fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "supports" => Ok(Self::Supports),
            "contradicts" => Ok(Self::Contradicts),
            "context" => Ok(Self::Context),
            _ => Err(invalid("role", "unknown closed value")),
        }
    }
}

/// One immutable evidence link.
#[derive(Clone, Debug, PartialEq)]
pub struct EvidenceLink {
    /// UUIDv7 identity and idempotency key.
    pub evidence_uuid: Uuid,
    /// Existing immutable assertion.
    pub assertion_uuid: Uuid,
    /// Caller-managed source identity.
    pub source_uuid: Uuid,
    /// Closed source kind.
    pub source_kind: EvidenceSourceKind,
    /// Closed relationship to the assertion.
    pub role: EvidenceRole,
    /// Optional finite metadata weight in `[0, 1]`.
    pub weight: Option<f64>,
    /// Provenance event identity.
    pub provenance_uuid: Uuid,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
    /// Evidence-link record contract.
    pub contract_version: u32,
}

impl EvidenceLink {
    /// Construct one validated immutable evidence link.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        evidence_uuid: Uuid,
        assertion_uuid: Uuid,
        source_uuid: Uuid,
        source_kind: EvidenceSourceKind,
        role: EvidenceRole,
        weight: Option<f64>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        require_v7(evidence_uuid, "evidence_uuid")?;
        require_v7(assertion_uuid, "assertion_uuid")?;
        require_uuid(source_uuid, "source_uuid")?;
        require_uuid(provenance_uuid, "provenance_uuid")?;
        validate_confidence(weight, "weight")?;
        let weight = weight.map(normalize_zero);
        Ok(Self {
            evidence_uuid,
            assertion_uuid,
            source_uuid,
            source_kind,
            role,
            weight,
            provenance_uuid,
            recorded_at_micros,
            contract_version: EVIDENCE_LINK_CONTRACT_VERSION,
        })
    }
}

/// Validated immutable evidence participant content.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EvidenceLedger {
    /// Links ordered by `(recorded_at, evidence_uuid)`.
    pub links: Vec<EvidenceLink>,
}

impl EvidenceLedger {
    /// Validate, sort, and construct evidence content.
    pub fn new(mut links: Vec<EvidenceLink>) -> Result<Self, KnowledgeError> {
        if links.len() > MAX_KNOWLEDGE_ROWS {
            return Err(KnowledgeError::Limit {
                participant: "evidence",
                observed: links.len(),
                limit: MAX_KNOWLEDGE_ROWS,
            });
        }
        let mut ids = HashSet::new();
        for link in &links {
            require_v7(link.evidence_uuid, "evidence_uuid")?;
            require_v7(link.assertion_uuid, "assertion_uuid")?;
            require_uuid(link.source_uuid, "source_uuid")?;
            require_uuid(link.provenance_uuid, "provenance_uuid")?;
            if link.contract_version != EVIDENCE_LINK_CONTRACT_VERSION {
                return Err(invalid("contract_version", "unsupported evidence version"));
            }
            if !ids.insert(link.evidence_uuid) {
                return Err(KnowledgeError::Duplicate("evidence_uuid"));
            }
            if let Some(weight) = link.weight {
                validate_confidence(Some(weight), "weight")?;
            }
        }
        links.sort_by_key(|row| (row.recorded_at_micros, row.evidence_uuid));
        Ok(Self { links })
    }

    /// Merge staged links idempotently.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut links = self.links.clone();
        for row in &staged.links {
            if let Some(existing) = links
                .iter()
                .find(|existing| existing.evidence_uuid == row.evidence_uuid)
            {
                if existing != row {
                    return Err(KnowledgeError::Conflict("evidence_uuid"));
                }
            } else {
                links.push(row.clone());
            }
        }
        Self::new(links)
    }

    /// Canonical fingerprint over normalized immutable content.
    pub fn evidence_fingerprint(&self, evidence_uuid: Uuid) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .links
            .iter()
            .find(|row| row.evidence_uuid == evidence_uuid)
            .ok_or(KnowledgeError::Dangling("evidence_uuid"))?;
        let mut writer = CanonicalWriter::new();
        writer.raw(b"GFEV")?;
        writer.u32(EVIDENCE_LINK_CONTRACT_VERSION)?;
        writer.raw(row.assertion_uuid.as_bytes())?;
        writer.raw(row.source_uuid.as_bytes())?;
        writer.text(row.source_kind.as_str())?;
        writer.text(row.role.as_str())?;
        canonical_optional_f64(&mut writer, row.weight)?;
        Ok(fingerprint(
            CanonicalDomain::EvidenceLink,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )?)
    }

    /// Build the authoritative evidence Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        evidence_batch(&self.links)
    }

    /// Decode authoritative Arrow batches and re-run every invariant.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut links = Vec::new();
        for batch in batches {
            require_schema(batch, &EVIDENCE_LINK_SCHEMA, "evidence.schema")?;
            let ids = fixed_column(batch, "evidence_uuid")?;
            let assertions = fixed_column(batch, "assertion_uuid")?;
            let sources = fixed_column(batch, "source_uuid")?;
            let kinds = string_column(batch, "source_kind")?;
            let roles = string_column(batch, "role")?;
            let weights = f64_column(batch, "weight")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                links.push(EvidenceLink {
                    evidence_uuid: uuid_at(ids, row, "evidence_uuid")?,
                    assertion_uuid: uuid_at(assertions, row, "assertion_uuid")?,
                    source_uuid: uuid_at(sources, row, "source_uuid")?,
                    source_kind: EvidenceSourceKind::parse(required_text(
                        kinds,
                        row,
                        "source_kind",
                    )?)?,
                    role: EvidenceRole::parse(required_text(roles, row, "role")?)?,
                    weight: optional_f64(weights, row),
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        Self::new(links)
    }
}

/// Closed append-only algorithm-run lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmRunState {
    /// Identity was durably published before dispatch.
    Started,
    /// Dispatch returned a canonical Arrow result.
    Completed,
    /// Dispatch returned a structured failure.
    Failed,
    /// Cancellation was observed at a deterministic checkpoint.
    Cancelled,
    /// Reopen found a published start without a terminal event.
    Interrupted,
}

impl AlgorithmRunState {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "started" => Ok(Self::Started),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(invalid("state", "unknown closed value")),
        }
    }

    /// Whether this state closes a run.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Started)
    }
}

/// Immutable identity for one recorded M18 invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlgorithmRun {
    /// Caller-supplied UUIDv7 run identity.
    pub run_uuid: Uuid,
    /// Closed public M18 algorithm name.
    pub algorithm: String,
    /// Algorithm contract version.
    pub algorithm_version: u32,
    /// Neutral descriptor contract version.
    pub descriptor_version: u32,
    /// Exact canonical descriptor bytes.
    pub descriptor: Vec<u8>,
    /// Exact resolved graph projection fingerprint.
    pub projection_fingerprint: [u8; 32],
    /// Provenance event that published the run identity.
    pub provenance_uuid: Uuid,
    /// Durable start transaction time.
    pub started_at_micros: i64,
    /// Run-record contract version.
    pub contract_version: u32,
}

impl AlgorithmRun {
    /// Construct one immutable run identity.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_uuid: Uuid,
        algorithm: String,
        algorithm_version: u32,
        descriptor_version: u32,
        descriptor: Vec<u8>,
        projection_fingerprint: [u8; 32],
        provenance_uuid: Uuid,
        started_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        let row = Self {
            run_uuid,
            algorithm,
            algorithm_version,
            descriptor_version,
            descriptor,
            projection_fingerprint,
            provenance_uuid,
            started_at_micros,
            contract_version: ALGORITHM_RUN_CONTRACT_VERSION,
        };
        validate_algorithm_run(&row)?;
        Ok(row)
    }
}

/// One immutable event in a recorded algorithm lifecycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlgorithmRunEvent {
    /// Deterministic event identity.
    pub event_uuid: Uuid,
    /// Owning run identity.
    pub run_uuid: Uuid,
    /// Closed lifecycle state.
    pub state: AlgorithmRunState,
    /// Canonical Arrow fingerprint for a completed result.
    pub result_fingerprint: Option<[u8; 32]>,
    /// Sanitized stable error code for non-success terminal states.
    pub error_code: Option<String>,
    /// Durable transaction time.
    pub recorded_at_micros: i64,
    /// Provenance event for this lifecycle transition.
    pub provenance_uuid: Uuid,
    /// Lifecycle-event contract version.
    pub contract_version: u32,
}

impl AlgorithmRunEvent {
    /// Construct one validated lifecycle event.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_uuid: Uuid,
        run_uuid: Uuid,
        state: AlgorithmRunState,
        result_fingerprint: Option<[u8; 32]>,
        error_code: Option<String>,
        recorded_at_micros: i64,
        provenance_uuid: Uuid,
    ) -> Result<Self, KnowledgeError> {
        let row = Self {
            event_uuid,
            run_uuid,
            state,
            result_fingerprint,
            error_code,
            recorded_at_micros,
            provenance_uuid,
            contract_version: ALGORITHM_RUN_EVENT_CONTRACT_VERSION,
        };
        validate_algorithm_run_event(&row)?;
        Ok(row)
    }
}

/// Validated immutable run identities and append-only lifecycle events.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AlgorithmRunLedger {
    /// Run identities ordered by `(started_at, run_uuid)`.
    pub runs: Vec<AlgorithmRun>,
    /// Events ordered by `(recorded_at, event_uuid)`.
    pub events: Vec<AlgorithmRunEvent>,
}

impl AlgorithmRunLedger {
    /// Validate and normalize complete run tables.
    pub fn new(
        mut runs: Vec<AlgorithmRun>,
        mut events: Vec<AlgorithmRunEvent>,
    ) -> Result<Self, KnowledgeError> {
        runs.sort_by_key(|row| (row.started_at_micros, row.run_uuid));
        events.sort_by_key(|row| (row.recorded_at_micros, row.event_uuid));
        validate_algorithm_run_rows(&runs, &events)?;
        Ok(Self { runs, events })
    }

    /// Merge immutable identities and events, rejecting conflicting reuse.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut runs = self.runs.clone();
        for row in &staged.runs {
            match runs.iter().find(|current| current.run_uuid == row.run_uuid) {
                Some(current) if current == row => {}
                Some(_) => return Err(KnowledgeError::Conflict("run_uuid")),
                None => runs.push(row.clone()),
            }
        }
        let mut events = self.events.clone();
        for row in &staged.events {
            match events
                .iter()
                .find(|current| current.event_uuid == row.event_uuid)
            {
                Some(current) if current == row => {}
                Some(_) => return Err(KnowledgeError::Conflict("event_uuid")),
                None => events.push(row.clone()),
            }
        }
        Self::new(runs, events)
    }

    /// Locate one run.
    #[must_use]
    pub fn run(&self, run_uuid: Uuid) -> Option<&AlgorithmRun> {
        self.runs.iter().find(|row| row.run_uuid == run_uuid)
    }

    /// Return lifecycle events for one run in canonical order.
    #[must_use]
    pub fn events_for(&self, run_uuid: Uuid) -> Vec<AlgorithmRunEvent> {
        self.events
            .iter()
            .filter(|row| row.run_uuid == run_uuid)
            .cloned()
            .collect()
    }

    /// Return the terminal event, when one exists.
    #[must_use]
    pub fn terminal_event(&self, run_uuid: Uuid) -> Option<&AlgorithmRunEvent> {
        self.events
            .iter()
            .find(|row| row.run_uuid == run_uuid && row.state.is_terminal())
    }

    /// Encode the authoritative run table.
    pub fn run_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        algorithm_run_batch(&self.runs)
    }

    /// Encode the authoritative event table.
    pub fn event_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        algorithm_run_event_batch(&self.events)
    }

    /// Decode, validate, and normalize persisted tables.
    pub fn from_batches(
        run_batches: &[RecordBatch],
        event_batches: &[RecordBatch],
    ) -> Result<Self, KnowledgeError> {
        let mut runs = Vec::new();
        for batch in run_batches {
            require_schema(batch, &ALGORITHM_RUN_SCHEMA, "algorithm_runs")?;
            let ids = fixed_column(batch, "run_uuid")?;
            let algorithms = string_column(batch, "algorithm")?;
            let algorithm_versions = u32_column(batch, "algorithm_version")?;
            let descriptor_versions = u32_column(batch, "descriptor_version")?;
            let descriptors = binary_column(batch, "descriptor")?;
            let projections = fixed_column(batch, "projection_fingerprint")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let started = timestamp_column(batch, "started_at")?;
            let contracts = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                runs.push(AlgorithmRun {
                    run_uuid: uuid_at(ids, row, "run_uuid")?,
                    algorithm: required_text(algorithms, row, "algorithm")?.to_owned(),
                    algorithm_version: required_u32(algorithm_versions, row, "algorithm_version")?,
                    descriptor_version: required_u32(
                        descriptor_versions,
                        row,
                        "descriptor_version",
                    )?,
                    descriptor: required_binary(descriptors, row, "descriptor")?.to_vec(),
                    projection_fingerprint: fixed_32_at(
                        projections,
                        row,
                        "projection_fingerprint",
                    )?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    started_at_micros: required_i64(started, row, "started_at")?,
                    contract_version: required_u32(contracts, row, "contract_version")?,
                });
            }
        }
        let mut events = Vec::new();
        for batch in event_batches {
            require_schema(batch, &ALGORITHM_RUN_EVENT_SCHEMA, "algorithm_run_events")?;
            let ids = fixed_column(batch, "event_uuid")?;
            let runs_column = fixed_column(batch, "run_uuid")?;
            let states = string_column(batch, "state")?;
            let results = fixed_column(batch, "result_fingerprint")?;
            let errors = string_column(batch, "error_code")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let contracts = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                events.push(AlgorithmRunEvent {
                    event_uuid: uuid_at(ids, row, "event_uuid")?,
                    run_uuid: uuid_at(runs_column, row, "run_uuid")?,
                    state: AlgorithmRunState::parse(required_text(states, row, "state")?)?,
                    result_fingerprint: optional_fixed_32(results, row, "result_fingerprint")?,
                    error_code: optional_text(errors, row),
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    contract_version: required_u32(contracts, row, "contract_version")?,
                });
            }
        }
        Self::new(runs, events)
    }
}

/// Closed confidence policy registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfidencePolicy {
    /// Caller supplies the assessment value.
    Explicit,
    /// Minimum of all available, non-null requested inputs; null if any is unavailable.
    ConservativeMin,
}

impl ConfidencePolicy {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::ConservativeMin => "conservative_min",
        }
    }

    fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "explicit" => Ok(Self::Explicit),
            "conservative_min" => Ok(Self::ConservativeMin),
            _ => Err(invalid("policy", "unknown closed value")),
        }
    }
}

/// One immutable confidence assessment.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfidenceAssessment {
    /// Caller-supplied UUIDv7 identity and idempotency key.
    pub confidence_uuid: Uuid,
    /// Assertion being assessed.
    pub assertion_uuid: Uuid,
    /// Closed policy.
    pub policy: ConfidencePolicy,
    /// Policy contract version.
    pub policy_version: u32,
    /// Result in `[0, 1]`, or null when conservative inputs are incomplete.
    pub value: Option<f64>,
    /// Producing provenance event.
    pub provenance_uuid: Uuid,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
    /// Assessment record contract.
    pub contract_version: u32,
}

impl ConfidenceAssessment {
    /// Construct one validated assessment.
    pub fn new(
        confidence_uuid: Uuid,
        assertion_uuid: Uuid,
        policy: ConfidencePolicy,
        value: Option<f64>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        require_v7(confidence_uuid, "confidence_uuid")?;
        require_v7(assertion_uuid, "assertion_uuid")?;
        require_uuid(provenance_uuid, "provenance_uuid")?;
        validate_confidence(value, "value")?;
        Ok(Self {
            confidence_uuid,
            assertion_uuid,
            policy,
            policy_version: 1,
            value: value.map(normalize_zero),
            provenance_uuid,
            recorded_at_micros,
            contract_version: CONFIDENCE_ASSESSMENT_CONTRACT_VERSION,
        })
    }
}

/// Immutable snapshot of one requested confidence input.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfidenceInput {
    /// Owning assessment.
    pub confidence_uuid: Uuid,
    /// Requested immutable assessment identity.
    pub input_confidence_uuid: Uuid,
    /// Value observed at assessment time; null means absent or null.
    pub input_value: Option<f64>,
    /// UUID-normalized position.
    pub ordinal: u32,
    /// Input record contract.
    pub contract_version: u32,
}

impl ConfidenceInput {
    /// Construct one validated snapshot input.
    pub fn new(
        confidence_uuid: Uuid,
        input_confidence_uuid: Uuid,
        input_value: Option<f64>,
        ordinal: u32,
    ) -> Result<Self, KnowledgeError> {
        require_v7(confidence_uuid, "confidence_uuid")?;
        require_v7(input_confidence_uuid, "input_confidence_uuid")?;
        validate_confidence(input_value, "input_value")?;
        Ok(Self {
            confidence_uuid,
            input_confidence_uuid,
            input_value: input_value.map(normalize_zero),
            ordinal,
            contract_version: CONFIDENCE_INPUT_CONTRACT_VERSION,
        })
    }
}

/// Validated append-only confidence participant content.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConfidenceLedger {
    /// Assessments ordered by `(recorded_at, confidence_uuid)`.
    pub assessments: Vec<ConfidenceAssessment>,
    /// Inputs ordered by assessment then `(ordinal, input_confidence_uuid)`.
    pub inputs: Vec<ConfidenceInput>,
}

impl ConfidenceLedger {
    /// Validate, sort, and construct confidence content.
    pub fn new(
        mut assessments: Vec<ConfidenceAssessment>,
        mut inputs: Vec<ConfidenceInput>,
    ) -> Result<Self, KnowledgeError> {
        inputs.sort_by_key(|row| (row.confidence_uuid, row.ordinal, row.input_confidence_uuid));
        validate_confidence_rows(&assessments, &inputs)?;
        let times = assessments
            .iter()
            .map(|row| (row.confidence_uuid, row.recorded_at_micros))
            .collect::<HashMap<_, _>>();
        assessments.sort_by_key(|row| (row.recorded_at_micros, row.confidence_uuid));
        inputs.sort_by_key(|row| {
            (
                times[&row.confidence_uuid],
                row.confidence_uuid,
                row.ordinal,
                row.input_confidence_uuid,
            )
        });
        Ok(Self {
            assessments,
            inputs,
        })
    }

    /// Evaluate and stage an explicit assessment.
    pub fn explicit(
        confidence_uuid: Uuid,
        assertion_uuid: Uuid,
        value: f64,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        Self::new(
            vec![ConfidenceAssessment::new(
                confidence_uuid,
                assertion_uuid,
                ConfidencePolicy::Explicit,
                Some(value),
                provenance_uuid,
                recorded_at_micros,
            )?],
            vec![],
        )
    }

    /// Evaluate `conservative_min@1` and persist the normalized requested-input snapshot.
    pub fn conservative_min(
        &self,
        confidence_uuid: Uuid,
        assertion_uuid: Uuid,
        mut requested: Vec<Uuid>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        requested.sort_unstable();
        if requested.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(KnowledgeError::Duplicate("input_confidence_uuid"));
        }
        let values = self
            .assessments
            .iter()
            .map(|row| (row.confidence_uuid, row.value))
            .collect::<HashMap<_, _>>();
        let mut minimum = None;
        let mut complete = !requested.is_empty();
        let mut inputs = Vec::with_capacity(requested.len());
        for (ordinal, input_uuid) in requested.into_iter().enumerate() {
            require_v7(input_uuid, "input_confidence_uuid")?;
            let observed = values.get(&input_uuid).copied().flatten();
            if let Some(value) = observed {
                minimum = Some(minimum.map_or(value, |current: f64| current.min(value)));
            } else {
                complete = false;
            }
            inputs.push(ConfidenceInput::new(
                confidence_uuid,
                input_uuid,
                observed,
                u32::try_from(ordinal).map_err(|_| KnowledgeError::Limit {
                    participant: "confidence_inputs",
                    observed: ordinal,
                    limit: u32::MAX as usize,
                })?,
            )?);
        }
        Self::new(
            vec![ConfidenceAssessment::new(
                confidence_uuid,
                assertion_uuid,
                ConfidencePolicy::ConservativeMin,
                complete.then_some(minimum).flatten(),
                provenance_uuid,
                recorded_at_micros,
            )?],
            inputs,
        )
    }

    /// Merge staged content idempotently.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut assessments = self.assessments.clone();
        let mut inputs = self.inputs.clone();
        for row in &staged.assessments {
            if let Some(existing) = assessments
                .iter()
                .find(|existing| existing.confidence_uuid == row.confidence_uuid)
            {
                if existing != row
                    || inputs_for(&inputs, row.confidence_uuid)
                        != inputs_for(&staged.inputs, row.confidence_uuid)
                {
                    return Err(KnowledgeError::Conflict("confidence_uuid"));
                }
            } else {
                assessments.push(row.clone());
                inputs.extend(
                    staged
                        .inputs
                        .iter()
                        .filter(|input| input.confidence_uuid == row.confidence_uuid)
                        .cloned(),
                );
            }
        }
        Self::new(assessments, inputs)
    }

    /// Canonical assessment fingerprint over policy, normalized value, and input snapshot.
    pub fn assessment_fingerprint(
        &self,
        confidence_uuid: Uuid,
    ) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .assessments
            .iter()
            .find(|row| row.confidence_uuid == confidence_uuid)
            .ok_or(KnowledgeError::Dangling("confidence_uuid"))?;
        let inputs = inputs_for(&self.inputs, confidence_uuid);
        let mut writer = CanonicalWriter::new();
        writer.raw(b"GFCA")?;
        writer.u32(CONFIDENCE_ASSESSMENT_CONTRACT_VERSION)?;
        writer.raw(row.assertion_uuid.as_bytes())?;
        writer.text(row.policy.as_str())?;
        writer.u32(row.policy_version)?;
        canonical_optional_f64(&mut writer, row.value)?;
        writer.u64(inputs.len() as u64)?;
        for input in inputs {
            writer.raw(input.input_confidence_uuid.as_bytes())?;
            canonical_optional_f64(&mut writer, input.input_value)?;
            writer.u32(input.ordinal)?;
        }
        Ok(fingerprint(
            CanonicalDomain::ConfidenceAssessment,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )?)
    }

    /// Build the authoritative assessment Arrow batch.
    pub fn assessment_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        confidence_assessment_batch(&self.assessments)
    }

    /// Build the authoritative input Arrow batch.
    pub fn input_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        confidence_input_batch(&self.inputs)
    }

    /// Decode authoritative Arrow batches and re-run every invariant.
    pub fn from_batches(
        assessment_batches: &[RecordBatch],
        input_batches: &[RecordBatch],
    ) -> Result<Self, KnowledgeError> {
        let mut assessments = Vec::new();
        for batch in assessment_batches {
            require_schema(batch, &CONFIDENCE_ASSESSMENT_SCHEMA, "confidence.schema")?;
            let ids = fixed_column(batch, "confidence_uuid")?;
            let assertions = fixed_column(batch, "assertion_uuid")?;
            let policies = string_column(batch, "policy")?;
            let policy_versions = u32_column(batch, "policy_version")?;
            let values = f64_column(batch, "value")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                assessments.push(ConfidenceAssessment {
                    confidence_uuid: uuid_at(ids, row, "confidence_uuid")?,
                    assertion_uuid: uuid_at(assertions, row, "assertion_uuid")?,
                    policy: ConfidencePolicy::parse(required_text(policies, row, "policy")?)?,
                    policy_version: required_u32(policy_versions, row, "policy_version")?,
                    value: optional_f64(values, row),
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        let mut inputs = Vec::new();
        for batch in input_batches {
            require_schema(batch, &CONFIDENCE_INPUT_SCHEMA, "confidence_input.schema")?;
            let owners = fixed_column(batch, "confidence_uuid")?;
            let ids = fixed_column(batch, "input_confidence_uuid")?;
            let values = f64_column(batch, "input_value")?;
            let ordinals = u32_column(batch, "ordinal")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                inputs.push(ConfidenceInput {
                    confidence_uuid: uuid_at(owners, row, "confidence_uuid")?,
                    input_confidence_uuid: uuid_at(ids, row, "input_confidence_uuid")?,
                    input_value: optional_f64(values, row),
                    ordinal: required_u32(ordinals, row, "ordinal")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        Self::new(assessments, inputs)
    }
}

/// Authoritative registry entry for one knowledge record family.
#[derive(Clone, Debug)]
pub struct SchemaRegistryEntry {
    /// Stable capability ID.
    pub capability_id: &'static str,
    /// Capability contract version.
    pub capability_version: u32,
    /// Stable record-family ID.
    pub record_family: &'static str,
    /// Record contract version.
    pub record_version: u32,
    /// Exact Arrow schema.
    pub schema: SchemaRef,
    /// Canonical schema fingerprint.
    pub schema_fingerprint: [u8; 32],
    /// Closed enum registries used by this family.
    pub enum_registry_versions: &'static [(&'static str, u32)],
    /// Canonical persisted sort key.
    pub sort_key: &'static [&'static str],
    /// Logical fields that uniquely identify one record for checkpoint diffs.
    pub diff_identity_fields: &'static [&'static str],
    /// Logical UUID field surfaced as `record_uuid`, when this family owns one.
    pub diff_record_uuid_field: Option<&'static str>,
    /// Fingerprint domain.
    pub fingerprint_domain: CanonicalDomain,
    /// Owning crate.
    pub owner: &'static str,
    /// Implementation issue.
    pub implementation_issue: u64,
    /// Maximum accepted rows.
    pub max_rows: usize,
}

impl SchemaRegistryEntry {
    /// Domain for owner-declared logical identity projections in checkpoint diffs.
    #[must_use]
    pub const fn diff_identity_fingerprint_domain(&self) -> CanonicalDomain {
        CanonicalDomain::ArrowResult
    }

    /// Domain for owner-canonical whole-record checkpoint fingerprints.
    #[must_use]
    pub const fn diff_record_fingerprint_domain(&self) -> CanonicalDomain {
        self.fingerprint_domain
    }
}

/// Return the authoritative assertion schema registry.
#[must_use]
pub fn schema_registry() -> Vec<SchemaRegistryEntry> {
    let mut entries = base_schema_registry_entries();
    entries.extend(algorithm_run_schema_entries());
    entries.push(reasoning::schema_registry_entry());
    entries.push(status::schema_registry_entry());
    entries.push(supersession::schema_registry_entry());
    entries.extend(hypothesis::schema_registry_entries());
    entries.push(valid_time::schema_registry_entry());
    entries.push(belief_projection::schema_registry_entry());
    entries
}

fn base_schema_registry_entries() -> Vec<SchemaRegistryEntry> {
    vec![
        SchemaRegistryEntry {
            capability_id: "knowledge",
            capability_version: KNOWLEDGE_CAPABILITY_VERSION,
            record_family: "assertions",
            record_version: ASSERTION_CONTRACT_VERSION,
            schema: Arc::clone(&ASSERTION_SCHEMA),
            schema_fingerprint: *ASSERTION_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[],
            sort_key: &["recorded_at", "assertion_uuid"],
            diff_identity_fields: &["assertion_uuid"],
            diff_record_uuid_field: Some("assertion_uuid"),
            fingerprint_domain: CanonicalDomain::Assertion,
            owner: "graphforge-knowledge",
            implementation_issue: 2411,
            max_rows: MAX_KNOWLEDGE_ROWS,
        },
        SchemaRegistryEntry {
            capability_id: "knowledge",
            capability_version: KNOWLEDGE_CAPABILITY_VERSION,
            record_family: "assertion_graph_refs",
            record_version: ASSERTION_GRAPH_REF_CONTRACT_VERSION,
            schema: Arc::clone(&ASSERTION_GRAPH_REF_SCHEMA),
            schema_fingerprint: *ASSERTION_GRAPH_REF_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[
                ("graph_kind", GRAPH_OBJECT_KIND_REGISTRY_VERSION),
                ("role", ASSERTION_GRAPH_ROLE_REGISTRY_VERSION),
            ],
            sort_key: &[
                "assertion_uuid",
                "role",
                "ordinal",
                "graph_kind",
                "graph_uuid",
            ],
            diff_identity_fields: &["assertion_uuid", "graph_uuid", "role", "ordinal"],
            diff_record_uuid_field: None,
            fingerprint_domain: CanonicalDomain::Assertion,
            owner: "graphforge-knowledge",
            implementation_issue: 2411,
            max_rows: MAX_KNOWLEDGE_ROWS,
        },
        SchemaRegistryEntry {
            capability_id: "knowledge",
            capability_version: KNOWLEDGE_CAPABILITY_VERSION,
            record_family: "confidence_assessments",
            record_version: CONFIDENCE_ASSESSMENT_CONTRACT_VERSION,
            schema: Arc::clone(&CONFIDENCE_ASSESSMENT_SCHEMA),
            schema_fingerprint: *CONFIDENCE_ASSESSMENT_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[("confidence_policy", CONFIDENCE_POLICY_REGISTRY_VERSION)],
            sort_key: &["recorded_at", "confidence_uuid"],
            diff_identity_fields: &["confidence_uuid"],
            diff_record_uuid_field: Some("confidence_uuid"),
            fingerprint_domain: CanonicalDomain::ConfidenceAssessment,
            owner: "graphforge-knowledge",
            implementation_issue: 774,
            max_rows: MAX_KNOWLEDGE_ROWS,
        },
        SchemaRegistryEntry {
            capability_id: "knowledge",
            capability_version: KNOWLEDGE_CAPABILITY_VERSION,
            record_family: "confidence_inputs",
            record_version: CONFIDENCE_INPUT_CONTRACT_VERSION,
            schema: Arc::clone(&CONFIDENCE_INPUT_SCHEMA),
            schema_fingerprint: *CONFIDENCE_INPUT_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[],
            sort_key: &["confidence_uuid", "ordinal", "input_confidence_uuid"],
            diff_identity_fields: &["confidence_uuid", "input_confidence_uuid"],
            diff_record_uuid_field: None,
            fingerprint_domain: CanonicalDomain::ConfidenceAssessment,
            owner: "graphforge-knowledge",
            implementation_issue: 774,
            max_rows: MAX_KNOWLEDGE_ROWS,
        },
        SchemaRegistryEntry {
            capability_id: "knowledge",
            capability_version: KNOWLEDGE_CAPABILITY_VERSION,
            record_family: "evidence",
            record_version: EVIDENCE_LINK_CONTRACT_VERSION,
            schema: Arc::clone(&EVIDENCE_LINK_SCHEMA),
            schema_fingerprint: *EVIDENCE_LINK_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[
                (
                    "evidence_source_kind",
                    EVIDENCE_SOURCE_KIND_REGISTRY_VERSION,
                ),
                ("evidence_role", EVIDENCE_ROLE_REGISTRY_VERSION),
            ],
            sort_key: &["recorded_at", "evidence_uuid"],
            diff_identity_fields: &["evidence_uuid"],
            diff_record_uuid_field: Some("evidence_uuid"),
            fingerprint_domain: CanonicalDomain::EvidenceLink,
            owner: "graphforge-knowledge",
            implementation_issue: 775,
            max_rows: MAX_KNOWLEDGE_ROWS,
        },
    ]
}

fn algorithm_run_schema_entries() -> [SchemaRegistryEntry; 2] {
    [
        SchemaRegistryEntry {
            capability_id: "knowledge",
            capability_version: KNOWLEDGE_CAPABILITY_VERSION,
            record_family: "algorithm_runs",
            record_version: ALGORITHM_RUN_CONTRACT_VERSION,
            schema: Arc::clone(&ALGORITHM_RUN_SCHEMA),
            schema_fingerprint: *ALGORITHM_RUN_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[],
            sort_key: &["started_at", "run_uuid"],
            diff_identity_fields: &["run_uuid"],
            diff_record_uuid_field: Some("run_uuid"),
            fingerprint_domain: CanonicalDomain::InvocationDescriptor,
            owner: "graphforge-knowledge",
            implementation_issue: 2003,
            max_rows: MAX_KNOWLEDGE_ROWS,
        },
        SchemaRegistryEntry {
            capability_id: "knowledge",
            capability_version: KNOWLEDGE_CAPABILITY_VERSION,
            record_family: "algorithm_run_events",
            record_version: ALGORITHM_RUN_EVENT_CONTRACT_VERSION,
            schema: Arc::clone(&ALGORITHM_RUN_EVENT_SCHEMA),
            schema_fingerprint: *ALGORITHM_RUN_EVENT_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[(
                "algorithm_run_state",
                ALGORITHM_RUN_STATE_REGISTRY_VERSION,
            )],
            sort_key: &["recorded_at", "event_uuid"],
            diff_identity_fields: &["event_uuid"],
            diff_record_uuid_field: Some("event_uuid"),
            fingerprint_domain: CanonicalDomain::ArrowResult,
            owner: "graphforge-knowledge",
            implementation_issue: 2003,
            max_rows: MAX_KNOWLEDGE_ROWS,
        },
    ]
}

/// Structured knowledge-domain failures.
#[derive(thiserror::Error, Debug)]
pub enum KnowledgeError {
    /// Invalid record value or derived identity.
    #[error("invalid knowledge {field}: {message}")]
    Invalid {
        /// Safe field name.
        field: &'static str,
        /// Safe failure summary.
        message: &'static str,
    },
    /// Participant row limit exceeded.
    #[error("knowledge {participant} row limit exceeded: observed {observed}, limit {limit}")]
    Limit {
        /// Safe participant name.
        participant: &'static str,
        /// Observed rows.
        observed: usize,
        /// Maximum rows.
        limit: usize,
    },
    /// Duplicate identity in one participant.
    #[error("duplicate knowledge identity: {0}")]
    Duplicate(&'static str),
    /// A required assertion or graph UUID is absent.
    #[error("dangling knowledge reference: {0}")]
    Dangling(&'static str),
    /// Idempotency identity was reused for different content.
    #[error("knowledge idempotency conflict: {0}")]
    Conflict(&'static str),
    /// A transaction identity was reused for different immutable content.
    #[error("knowledge transaction conflict: {0}")]
    TransactionConflict(&'static str),
    /// Shared canonicalization failure.
    #[error(transparent)]
    Canonical(#[from] CanonicalError),
    /// Arrow construction failure.
    #[error("knowledge Arrow failure: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

impl KnowledgeError {
    /// Stable public error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Invalid { .. } => "GF_KNOWLEDGE_INVALID",
            Self::Limit { .. } => "GF_RESOURCE_LIMIT",
            Self::Duplicate(_) => "GF_KNOWLEDGE_DUPLICATE",
            Self::Dangling(_) => "GF_KNOWLEDGE_DANGLING",
            Self::Conflict(_) => "GF_IDEMPOTENCY_CONFLICT",
            Self::TransactionConflict(_) => "GF_TRANSACTION_CONFLICT",
            Self::Canonical(error) => error.code(),
            Self::Arrow(_) => "GF_SCHEMA_MISMATCH",
        }
    }
}

fn validate_rows(
    assertions: &[Assertion],
    refs: &[AssertionGraphRef],
) -> Result<(), KnowledgeError> {
    check_limit("assertions", assertions.len())?;
    check_limit("assertion_graph_refs", refs.len())?;
    let mut assertion_ids = HashSet::with_capacity(assertions.len());
    for assertion in assertions {
        if assertion.contract_version != ASSERTION_CONTRACT_VERSION {
            return Err(invalid("assertion.contract_version", "unsupported version"));
        }
        require_v7(assertion.assertion_uuid, "assertion_uuid")?;
        require_uuid(assertion.provenance_uuid, "provenance_uuid")?;
        validate_claim(&assertion.claim)?;
        if !assertion_ids.insert(assertion.assertion_uuid) {
            return Err(KnowledgeError::Duplicate("assertion_uuid"));
        }
    }
    let mut tuples = HashSet::with_capacity(refs.len());
    let mut role_ordinals: HashMap<(Uuid, AssertionGraphRole), Vec<u32>> = HashMap::new();
    let mut covered_assertions = HashSet::with_capacity(assertions.len());
    for reference in refs {
        if reference.contract_version != ASSERTION_GRAPH_REF_CONTRACT_VERSION {
            return Err(invalid(
                "assertion_graph_ref.contract_version",
                "unsupported version",
            ));
        }
        require_v7(reference.assertion_uuid, "assertion_uuid")?;
        require_uuid(reference.graph_uuid, "graph_uuid")?;
        if !assertion_ids.contains(&reference.assertion_uuid) {
            return Err(KnowledgeError::Dangling("assertion_uuid"));
        }
        if !tuples.insert((
            reference.assertion_uuid,
            reference.graph_uuid,
            reference.role,
            reference.ordinal,
        )) {
            return Err(KnowledgeError::Duplicate(
                "assertion_uuid/graph_uuid/role/ordinal",
            ));
        }
        role_ordinals
            .entry((reference.assertion_uuid, reference.role))
            .or_default()
            .push(reference.ordinal);
        covered_assertions.insert(reference.assertion_uuid);
    }
    for assertion_uuid in assertion_ids {
        if !covered_assertions.contains(&assertion_uuid) {
            return Err(KnowledgeError::Dangling("assertion.graph_refs"));
        }
    }
    for ordinals in role_ordinals.values_mut() {
        ordinals.sort_unstable();
        if ordinals
            .iter()
            .enumerate()
            .any(|(expected, actual)| usize::try_from(*actual) != Ok(expected))
        {
            return Err(invalid("ordinal", "must be contiguous from zero per role"));
        }
    }
    Ok(())
}

fn assertion_batch(rows: &[Assertion]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.assertion_uuid.as_bytes())?;
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&ASSERTION_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.claim.as_str()),
            )),
            Arc::new(provenance.finish()),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|row| row.recorded_at_micros),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

fn graph_ref_batch(rows: &[AssertionGraphRef]) -> Result<RecordBatch, KnowledgeError> {
    let mut assertions = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut graph_ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        assertions.append_value(row.assertion_uuid.as_bytes())?;
        graph_ids.append_value(row.graph_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&ASSERTION_GRAPH_REF_SCHEMA),
        vec![
            Arc::new(assertions.finish()),
            Arc::new(graph_ids.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.graph_kind.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.role.as_str()),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

fn confidence_assessment_batch(
    rows: &[ConfidenceAssessment],
) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut assertions = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.confidence_uuid.as_bytes())?;
        assertions.append_value(row.assertion_uuid.as_bytes())?;
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&CONFIDENCE_ASSESSMENT_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(assertions.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.policy.as_str()),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.policy_version),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.value).collect::<Vec<_>>(),
            )),
            Arc::new(provenance.finish()),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|row| row.recorded_at_micros),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

fn confidence_input_batch(rows: &[ConfidenceInput]) -> Result<RecordBatch, KnowledgeError> {
    let mut owners = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        owners.append_value(row.confidence_uuid.as_bytes())?;
        ids.append_value(row.input_confidence_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&CONFIDENCE_INPUT_SCHEMA),
        vec![
            Arc::new(owners.finish()),
            Arc::new(ids.finish()),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.input_value).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

fn evidence_batch(rows: &[EvidenceLink]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut assertions = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.evidence_uuid.as_bytes())?;
        assertions.append_value(row.assertion_uuid.as_bytes())?;
        sources.append_value(row.source_uuid.as_bytes())?;
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&EVIDENCE_LINK_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(assertions.finish()),
            Arc::new(sources.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.source_kind.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.role.as_str()),
            )),
            Arc::new(rows.iter().map(|row| row.weight).collect::<Float64Array>()),
            Arc::new(provenance.finish()),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|row| row.recorded_at_micros),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

fn validate_algorithm_run(row: &AlgorithmRun) -> Result<(), KnowledgeError> {
    require_v7(row.run_uuid, "run_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    if row.algorithm.is_empty()
        || row.algorithm.len() as u64 > graphforge_core::canonical::MAX_CANONICAL_TEXT_BYTES
    {
        return Err(invalid("algorithm", "must be bounded non-empty UTF-8"));
    }
    if row.algorithm_version != 1 {
        return Err(invalid("algorithm_version", "unsupported version"));
    }
    if row.descriptor_version != 1 {
        return Err(invalid("descriptor_version", "unsupported version"));
    }
    if row.descriptor.is_empty()
        || row.descriptor.len() as u64 > graphforge_core::canonical::MAX_CANONICAL_BINARY_BYTES
    {
        return Err(invalid("descriptor", "must be bounded and non-empty"));
    }
    if row.contract_version != ALGORITHM_RUN_CONTRACT_VERSION {
        return Err(invalid(
            "contract_version",
            "unsupported algorithm-run version",
        ));
    }
    Ok(())
}

fn validate_algorithm_run_event(row: &AlgorithmRunEvent) -> Result<(), KnowledgeError> {
    require_uuid(row.event_uuid, "event_uuid")?;
    require_v7(row.run_uuid, "run_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    if row.contract_version != ALGORITHM_RUN_EVENT_CONTRACT_VERSION {
        return Err(invalid(
            "contract_version",
            "unsupported algorithm-run-event version",
        ));
    }
    if row.error_code.as_ref().is_some_and(|code| {
        code.is_empty()
            || code.len() as u64 > graphforge_core::canonical::MAX_CANONICAL_TEXT_BYTES
            || !code.starts_with("GF_")
    }) {
        return Err(invalid("error_code", "must be a bounded stable GF_ code"));
    }
    match row.state {
        AlgorithmRunState::Started => {
            if row.result_fingerprint.is_some() || row.error_code.is_some() {
                return Err(invalid("state", "started has no terminal payload"));
            }
        }
        AlgorithmRunState::Completed => {
            if row.result_fingerprint.is_none() || row.error_code.is_some() {
                return Err(invalid(
                    "state",
                    "completed requires only a result fingerprint",
                ));
            }
        }
        AlgorithmRunState::Failed
        | AlgorithmRunState::Cancelled
        | AlgorithmRunState::Interrupted => {
            if row.result_fingerprint.is_some() || row.error_code.is_none() {
                return Err(invalid(
                    "state",
                    "non-success terminal requires only an error code",
                ));
            }
        }
    }
    Ok(())
}

fn validate_algorithm_run_rows(
    runs: &[AlgorithmRun],
    events: &[AlgorithmRunEvent],
) -> Result<(), KnowledgeError> {
    check_limit("algorithm_runs", runs.len())?;
    check_limit("algorithm_run_events", events.len())?;
    let mut run_ids = HashSet::with_capacity(runs.len());
    let mut run_index = HashMap::with_capacity(runs.len());
    for row in runs {
        validate_algorithm_run(row)?;
        if !run_ids.insert(row.run_uuid) {
            return Err(KnowledgeError::Duplicate("run_uuid"));
        }
        run_index.insert(row.run_uuid, row);
    }
    let mut event_ids = HashSet::with_capacity(events.len());
    let mut per_run: HashMap<Uuid, (usize, usize)> = HashMap::new();
    for row in events {
        validate_algorithm_run_event(row)?;
        if !event_ids.insert(row.event_uuid) {
            return Err(KnowledgeError::Duplicate("event_uuid"));
        }
        let run = run_index
            .get(&row.run_uuid)
            .ok_or(KnowledgeError::Dangling("run_uuid"))?;
        if row.recorded_at_micros < run.started_at_micros {
            return Err(invalid("recorded_at", "event precedes run start"));
        }
        let counts = per_run.entry(row.run_uuid).or_default();
        if row.state == AlgorithmRunState::Started {
            counts.0 += 1;
            if row.recorded_at_micros != run.started_at_micros
                || row.provenance_uuid != run.provenance_uuid
            {
                return Err(invalid(
                    "started",
                    "start event must match immutable run identity",
                ));
            }
        } else {
            counts.1 += 1;
        }
    }
    for run in runs {
        let (started, terminal) = per_run.get(&run.run_uuid).copied().unwrap_or_default();
        if started != 1 {
            return Err(invalid("started", "run requires exactly one start event"));
        }
        if terminal > 1 {
            return Err(invalid(
                "terminal",
                "run permits at most one terminal event",
            ));
        }
    }
    Ok(())
}

fn algorithm_run_batch(rows: &[AlgorithmRun]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut projections = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.run_uuid.as_bytes())?;
        projections.append_value(row.projection_fingerprint)?;
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&ALGORITHM_RUN_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.algorithm.as_str()),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.algorithm_version),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.descriptor_version),
            )),
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| row.descriptor.as_slice()),
            )),
            Arc::new(projections.finish()),
            Arc::new(provenance.finish()),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|row| row.started_at_micros),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

fn algorithm_run_event_batch(rows: &[AlgorithmRunEvent]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut runs = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut results = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.event_uuid.as_bytes())?;
        runs.append_value(row.run_uuid.as_bytes())?;
        match row.result_fingerprint {
            Some(value) => results.append_value(value)?,
            None => results.append_null(),
        }
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&ALGORITHM_RUN_EVENT_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(runs.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.state.as_str()),
            )),
            Arc::new(results.finish()),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.error_code.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|row| row.recorded_at_micros),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(provenance.finish()),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

fn validate_confidence_rows(
    assessments: &[ConfidenceAssessment],
    inputs: &[ConfidenceInput],
) -> Result<(), KnowledgeError> {
    check_limit("confidence_assessments", assessments.len())?;
    check_limit("confidence_inputs", inputs.len())?;
    let mut ids = HashSet::with_capacity(assessments.len());
    let mut policies = HashMap::with_capacity(assessments.len());
    for row in assessments {
        require_v7(row.confidence_uuid, "confidence_uuid")?;
        require_v7(row.assertion_uuid, "assertion_uuid")?;
        require_uuid(row.provenance_uuid, "provenance_uuid")?;
        validate_confidence(row.value, "value")?;
        if row.policy_version != 1 {
            return Err(invalid("policy_version", "unsupported version"));
        }
        if row.contract_version != CONFIDENCE_ASSESSMENT_CONTRACT_VERSION {
            return Err(invalid(
                "confidence.contract_version",
                "unsupported version",
            ));
        }
        if !ids.insert(row.confidence_uuid) {
            return Err(KnowledgeError::Duplicate("confidence_uuid"));
        }
        policies.insert(row.confidence_uuid, row.policy);
    }
    let mut input_ids = HashSet::with_capacity(inputs.len());
    let mut normalized_inputs: HashMap<Uuid, Vec<(u32, Uuid, Option<f64>)>> = HashMap::new();
    for input in inputs {
        require_v7(input.confidence_uuid, "confidence_uuid")?;
        require_v7(input.input_confidence_uuid, "input_confidence_uuid")?;
        validate_confidence(input.input_value, "input_value")?;
        if input.contract_version != CONFIDENCE_INPUT_CONTRACT_VERSION {
            return Err(invalid(
                "confidence_input.contract_version",
                "unsupported version",
            ));
        }
        if !ids.contains(&input.confidence_uuid) {
            return Err(KnowledgeError::Dangling("confidence_uuid"));
        }
        if !input_ids.insert((input.confidence_uuid, input.input_confidence_uuid)) {
            return Err(KnowledgeError::Duplicate("input_confidence_uuid"));
        }
        normalized_inputs
            .entry(input.confidence_uuid)
            .or_default()
            .push((
                input.ordinal,
                input.input_confidence_uuid,
                input.input_value,
            ));
    }
    for (confidence_uuid, policy) in policies {
        let assessment = assessments
            .iter()
            .find(|row| row.confidence_uuid == confidence_uuid)
            .expect("validated assessment identity");
        let values = normalized_inputs.entry(confidence_uuid).or_default();
        values.sort_by_key(|(ordinal, _, _)| *ordinal);
        if values
            .iter()
            .enumerate()
            .any(|(expected, (actual, _, _))| usize::try_from(*actual) != Ok(expected))
        {
            return Err(invalid("ordinal", "must be contiguous from zero"));
        }
        if values.windows(2).any(|pair| pair[0].1 >= pair[1].1) {
            return Err(invalid(
                "input_confidence_uuid",
                "must be unique UUID-normalized order",
            ));
        }
        validate_policy_snapshot(assessment, policy, values)?;
    }
    Ok(())
}

fn validate_policy_snapshot(
    assessment: &ConfidenceAssessment,
    policy: ConfidencePolicy,
    values: &[(u32, Uuid, Option<f64>)],
) -> Result<(), KnowledgeError> {
    match policy {
        ConfidencePolicy::Explicit => {
            if !values.is_empty() {
                return Err(invalid(
                    "confidence_inputs",
                    "explicit policy has no inputs",
                ));
            }
            if assessment.value.is_none() {
                return Err(invalid("value", "explicit policy requires a value"));
            }
        }
        ConfidencePolicy::ConservativeMin => {
            let expected =
                if values.is_empty() || values.iter().any(|(_, _, value)| value.is_none()) {
                    None
                } else {
                    values
                        .iter()
                        .filter_map(|(_, _, value)| *value)
                        .reduce(f64::min)
                };
            if assessment.value != expected {
                return Err(invalid(
                    "value",
                    "does not match conservative_min input snapshot",
                ));
            }
        }
    }
    Ok(())
}

fn inputs_for(rows: &[ConfidenceInput], confidence_uuid: Uuid) -> Vec<ConfidenceInput> {
    let mut inputs = rows
        .iter()
        .filter(|row| row.confidence_uuid == confidence_uuid)
        .cloned()
        .collect::<Vec<_>>();
    inputs.sort_by_key(|row| (row.ordinal, row.input_confidence_uuid));
    inputs
}

fn validate_confidence(value: Option<f64>, field: &'static str) -> Result<(), KnowledgeError> {
    if value.is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value)) {
        Err(invalid(field, "must be finite and in [0,1]"))
    } else {
        Ok(())
    }
}

fn normalize_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

fn canonical_optional_f64(
    writer: &mut CanonicalWriter,
    value: Option<f64>,
) -> Result<(), KnowledgeError> {
    match value {
        None => writer.u8(0)?,
        Some(value) => {
            writer.u8(1)?;
            writer.u64(normalize_zero(value).to_bits())?;
        }
    }
    Ok(())
}

fn refs_for(rows: &[AssertionGraphRef], assertion_uuid: Uuid) -> Vec<AssertionGraphRef> {
    let mut refs = rows
        .iter()
        .filter(|row| row.assertion_uuid == assertion_uuid)
        .cloned()
        .collect::<Vec<_>>();
    refs.sort_by_key(|row| {
        (
            role_order(row.role),
            row.ordinal,
            kind_order(row.graph_kind),
            row.graph_uuid,
        )
    });
    refs
}

const fn role_order(role: AssertionGraphRole) -> u8 {
    match role {
        AssertionGraphRole::Subject => 0,
        AssertionGraphRole::Object => 1,
        AssertionGraphRole::Context => 2,
    }
}

const fn kind_order(kind: GraphObjectKind) -> u8 {
    match kind {
        GraphObjectKind::Node => 0,
        GraphObjectKind::Edge => 1,
    }
}

fn validate_claim(claim: &str) -> Result<(), KnowledgeError> {
    if claim.is_empty() {
        return Err(invalid("claim", "must not be empty"));
    }
    if claim.len() as u64 > graphforge_core::canonical::MAX_CANONICAL_TEXT_BYTES {
        return Err(invalid("claim", "exceeds canonical UTF-8 limit"));
    }
    Ok(())
}

fn require_uuid(value: Uuid, field: &'static str) -> Result<(), KnowledgeError> {
    if value.is_nil() {
        Err(invalid(field, "must not be nil"))
    } else {
        Ok(())
    }
}

fn require_v7(value: Uuid, field: &'static str) -> Result<(), KnowledgeError> {
    require_uuid(value, field)?;
    if value.get_version() != Some(Version::SortRand) {
        return Err(invalid(field, "must be UUIDv7"));
    }
    Ok(())
}

fn check_limit(participant: &'static str, observed: usize) -> Result<(), KnowledgeError> {
    if observed > MAX_KNOWLEDGE_ROWS {
        Err(KnowledgeError::Limit {
            participant,
            observed,
            limit: MAX_KNOWLEDGE_ROWS,
        })
    } else {
        Ok(())
    }
}

const fn invalid(field: &'static str, message: &'static str) -> KnowledgeError {
    KnowledgeError::Invalid { field, message }
}

fn require_schema(
    batch: &RecordBatch,
    expected: &SchemaRef,
    field: &'static str,
) -> Result<(), KnowledgeError> {
    if batch.schema().as_ref() == expected.as_ref() {
        Ok(())
    } else {
        Err(invalid(field, "schema mismatch"))
    }
}

fn fixed_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a FixedSizeBinaryArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "missing or wrong Arrow type"))
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a StringArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "missing or wrong Arrow type"))
}

fn binary_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a BinaryArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "missing or wrong Arrow type"))
}

fn timestamp_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a TimestampMicrosecondArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "missing or wrong Arrow type"))
}

fn u32_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a UInt32Array, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "missing or wrong Arrow type"))
}

fn f64_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a Float64Array, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "missing or wrong Arrow type"))
}

fn optional_f64(array: &Float64Array, row: usize) -> Option<f64> {
    (!array.is_null(row)).then(|| normalize_zero(array.value(row)))
}

fn uuid_at(
    array: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<Uuid, KnowledgeError> {
    if array.is_null(row) {
        return Err(invalid(field, "must not be null"));
    }
    Uuid::from_slice(array.value(row)).map_err(|_| invalid(field, "malformed UUID"))
}

fn fixed_32_at(
    array: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<[u8; 32], KnowledgeError> {
    if array.is_null(row) || array.value_length() != 32 {
        return Err(invalid(field, "must be a 32-byte value"));
    }
    Ok(array
        .value(row)
        .try_into()
        .expect("validated fixed-size binary width"))
}

fn optional_fixed_32(
    array: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<Option<[u8; 32]>, KnowledgeError> {
    if array.is_null(row) {
        Ok(None)
    } else {
        fixed_32_at(array, row, field).map(Some)
    }
}

fn required_text<'a>(
    array: &'a StringArray,
    row: usize,
    field: &'static str,
) -> Result<&'a str, KnowledgeError> {
    if array.is_null(row) {
        Err(invalid(field, "must not be null"))
    } else {
        Ok(array.value(row))
    }
}

fn optional_text(array: &StringArray, row: usize) -> Option<String> {
    (!array.is_null(row)).then(|| array.value(row).to_owned())
}

fn required_binary<'a>(
    array: &'a BinaryArray,
    row: usize,
    field: &'static str,
) -> Result<&'a [u8], KnowledgeError> {
    if array.is_null(row) {
        Err(invalid(field, "must not be null"))
    } else {
        Ok(array.value(row))
    }
}

fn required_i64(
    array: &TimestampMicrosecondArray,
    row: usize,
    field: &'static str,
) -> Result<i64, KnowledgeError> {
    if array.is_null(row) {
        Err(invalid(field, "must not be null"))
    } else {
        Ok(array.value(row))
    }
}

fn required_u32(
    array: &UInt32Array,
    row: usize,
    field: &'static str,
) -> Result<u32, KnowledgeError> {
    if array.is_null(row) {
        Err(invalid(field, "must not be null"))
    } else {
        Ok(array.value(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn fixture() -> AssertionLedger {
        let assertion_uuid = uuid7(1);
        AssertionLedger::new(
            vec![Assertion::new(assertion_uuid, "exact claim".into(), uuid7(2), 10).unwrap()],
            vec![
                AssertionGraphRef::new(
                    assertion_uuid,
                    uuid7(3),
                    GraphObjectKind::Node,
                    AssertionGraphRole::Subject,
                    0,
                )
                .unwrap(),
                AssertionGraphRef::new(
                    assertion_uuid,
                    uuid7(4),
                    GraphObjectKind::Edge,
                    AssertionGraphRole::Context,
                    0,
                )
                .unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn exact_claim_and_sorted_refs_have_stable_fingerprint() {
        let first = fixture();
        let mut reversed = first.graph_refs.clone();
        reversed.reverse();
        let second = AssertionLedger::new(first.assertions.clone(), reversed).unwrap();
        let fingerprint = first.assertion_fingerprint(uuid7(1)).unwrap();
        assert_eq!(fingerprint, second.assertion_fingerprint(uuid7(1)).unwrap());
        let encoded = fingerprint
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        // This locks the Assertion domain plus GFAS framing. A digest change is
        // a contract break and requires a version bump, never a golden refresh.
        assert_eq!(
            encoded,
            "6024d4f2e22b35d5850f6a2f1c9f0cf77d28ab1e7ba83342416a1e7660f7b4bb"
        );
    }

    #[test]
    fn arrow_round_trip_preserves_exact_records_across_chunking() {
        let ledger = fixture();
        let assertions = ledger.assertion_batch().unwrap();
        let refs = ledger.graph_ref_batch().unwrap();
        assert_eq!(
            AssertionLedger::from_batches(
                &[assertions.slice(0, 0), assertions.slice(0, 1)],
                &[refs.slice(0, 1), refs.slice(1, 1)],
            )
            .unwrap(),
            ledger
        );
    }

    #[test]
    fn invalid_claims_refs_and_ordinals_fail_structurally() {
        let assertion_uuid = uuid7(1);
        let assertion = Assertion::new(assertion_uuid, "claim".into(), uuid7(2), 10).unwrap();
        assert!(AssertionLedger::new(vec![assertion.clone()], vec![]).is_err());
        assert!(matches!(
            Assertion::new(assertion_uuid, String::new(), uuid7(2), 10),
            Err(KnowledgeError::Invalid { field: "claim", .. })
        ));
        let oversized =
            "x".repeat(graphforge_core::canonical::MAX_CANONICAL_TEXT_BYTES as usize + 1);
        assert!(matches!(
            Assertion::new(assertion_uuid, oversized, uuid7(2), 10),
            Err(KnowledgeError::Invalid { field: "claim", .. })
        ));

        let non_contiguous = AssertionGraphRef::new(
            assertion_uuid,
            uuid7(3),
            GraphObjectKind::Node,
            AssertionGraphRole::Subject,
            1,
        )
        .unwrap();
        assert!(matches!(
            AssertionLedger::new(vec![assertion.clone()], vec![non_contiguous]),
            Err(KnowledgeError::Invalid {
                field: "ordinal",
                ..
            })
        ));

        let duplicate = AssertionGraphRef::new(
            assertion_uuid,
            uuid7(3),
            GraphObjectKind::Node,
            AssertionGraphRole::Subject,
            0,
        )
        .unwrap();
        assert!(matches!(
            AssertionLedger::new(vec![assertion], vec![duplicate.clone(), duplicate]),
            Err(KnowledgeError::Duplicate(
                "assertion_uuid/graph_uuid/role/ordinal"
            ))
        ));
    }

    #[test]
    fn closed_values_fail_during_arrow_decode() {
        let ledger = fixture();
        let assertions = ledger.assertion_batch().unwrap();
        let refs = ledger.graph_ref_batch().unwrap();
        let bad_kind = RecordBatch::try_new(
            Arc::clone(&ASSERTION_GRAPH_REF_SCHEMA),
            vec![
                Arc::clone(refs.column(0)),
                Arc::clone(refs.column(1)),
                Arc::new(StringArray::from(vec!["vertex", "edge"])),
                Arc::clone(refs.column(3)),
                Arc::clone(refs.column(4)),
                Arc::clone(refs.column(5)),
            ],
        )
        .unwrap();
        assert!(matches!(
            AssertionLedger::from_batches(&[assertions.clone()], &[bad_kind]),
            Err(KnowledgeError::Invalid {
                field: "graph_kind",
                ..
            })
        ));

        let bad_role = RecordBatch::try_new(
            Arc::clone(&ASSERTION_GRAPH_REF_SCHEMA),
            vec![
                Arc::clone(refs.column(0)),
                Arc::clone(refs.column(1)),
                Arc::clone(refs.column(2)),
                Arc::new(StringArray::from(vec!["target", "context"])),
                Arc::clone(refs.column(4)),
                Arc::clone(refs.column(5)),
            ],
        )
        .unwrap();
        assert!(matches!(
            AssertionLedger::from_batches(&[assertions], &[bad_role]),
            Err(KnowledgeError::Invalid { field: "role", .. })
        ));
    }

    #[test]
    fn m20_schema_registry_excludes_every_m21_field() {
        let fields = schema_registry()
            .into_iter()
            .filter(|entry| entry.capability_id == "knowledge")
            .flat_map(|entry| {
                entry
                    .schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        for deferred in [
            "status",
            "confidence",
            "evidence",
            "hypothesis_uuid",
            "reasoning",
            "supersedes_uuid",
            "valid_from",
            "valid_to",
        ] {
            assert!(!fields.contains(deferred));
        }
    }

    #[test]
    fn schema_registry_owns_record_diff_identity_contracts() {
        let identities = schema_registry()
            .into_iter()
            .map(|entry| {
                for field in entry.diff_identity_fields {
                    assert!(entry.schema.field_with_name(field).is_ok());
                }
                if let Some(field) = entry.diff_record_uuid_field {
                    assert!(entry.diff_identity_fields.contains(&field));
                    assert_eq!(
                        entry.schema.field_with_name(field).unwrap().data_type(),
                        &DataType::FixedSizeBinary(16)
                    );
                }
                (
                    entry.record_family,
                    entry.diff_identity_fields,
                    entry.diff_record_uuid_field,
                )
            })
            .collect::<Vec<_>>();

        assert!(identities.contains(&(
            "assertions",
            &["assertion_uuid"][..],
            Some("assertion_uuid")
        )));
        assert!(identities.contains(&(
            "assertion_graph_refs",
            &["assertion_uuid", "graph_uuid", "role", "ordinal"][..],
            None
        )));
        assert!(identities.contains(&(
            "confidence_inputs",
            &["confidence_uuid", "input_confidence_uuid"][..],
            None
        )));
    }

    #[test]
    fn explicit_confidence_round_trips_and_normalizes_negative_zero() {
        let ledger = ConfidenceLedger::explicit(uuid7(10), uuid7(1), -0.0, uuid7(2), 20).unwrap();
        assert_eq!(
            ledger.assessments[0].value.unwrap().to_bits(),
            0.0f64.to_bits()
        );
        assert!(ledger.inputs.is_empty());
        let assessments = ledger.assessment_batch().unwrap();
        let inputs = ledger.input_batch().unwrap();
        assert_eq!(
            ConfidenceLedger::from_batches(&[assessments], &[inputs]).unwrap(),
            ledger
        );
    }

    #[test]
    fn conservative_min_is_uuid_normalized_and_snapshots_missing_values() {
        let first = ConfidenceLedger::explicit(uuid7(10), uuid7(1), 0.8, uuid7(2), 10).unwrap();
        let second = ConfidenceLedger::explicit(uuid7(11), uuid7(1), 0.3, uuid7(3), 11).unwrap();
        let existing = first.merge(&second).unwrap();
        let staged = existing
            .conservative_min(
                uuid7(20),
                uuid7(1),
                vec![uuid7(12), uuid7(11), uuid7(10)],
                uuid7(4),
                20,
            )
            .unwrap();
        assert_eq!(staged.assessments[0].value, None);
        assert_eq!(
            staged
                .inputs
                .iter()
                .map(|row| row.input_confidence_uuid)
                .collect::<Vec<_>>(),
            vec![uuid7(10), uuid7(11), uuid7(12)]
        );
        assert_eq!(
            staged
                .inputs
                .iter()
                .map(|row| row.input_value)
                .collect::<Vec<_>>(),
            vec![Some(0.8), Some(0.3), None]
        );

        let complete = existing
            .conservative_min(
                uuid7(21),
                uuid7(1),
                vec![uuid7(11), uuid7(10)],
                uuid7(5),
                21,
            )
            .unwrap();
        assert_eq!(complete.assessments[0].value, Some(0.3));
        assert_eq!(
            staged.assessment_fingerprint(uuid7(20)).unwrap(),
            existing
                .conservative_min(
                    uuid7(20),
                    uuid7(1),
                    vec![uuid7(10), uuid7(12), uuid7(11)],
                    uuid7(4),
                    20,
                )
                .unwrap()
                .assessment_fingerprint(uuid7(20))
                .unwrap()
        );
        let encoded = staged
            .assessment_fingerprint(uuid7(20))
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        // Locks `graphforge/confidence-assessment` plus GFCA framing.
        assert_eq!(
            encoded,
            "052104e7e9573852f29255e5f4d7942bd0f409a00b8cda80aba2d09112e40bbb"
        );
    }

    #[test]
    fn confidence_idempotency_and_validation_are_fail_closed() {
        for invalid_value in [f64::NAN, f64::INFINITY, -0.01, 1.01] {
            assert!(matches!(
                ConfidenceLedger::explicit(uuid7(10), uuid7(1), invalid_value, uuid7(2), 20),
                Err(KnowledgeError::Invalid { field: "value", .. })
            ));
        }
        let existing = ConfidenceLedger::explicit(uuid7(10), uuid7(1), 0.8, uuid7(2), 20).unwrap();
        assert_eq!(
            existing.merge(&existing).unwrap().assessments.len(),
            existing.assessments.len()
        );
        let conflict = ConfidenceLedger::explicit(uuid7(10), uuid7(1), 0.7, uuid7(2), 20).unwrap();
        assert!(matches!(
            existing.merge(&conflict),
            Err(KnowledgeError::Conflict("confidence_uuid"))
        ));
        assert!(matches!(
            existing.conservative_min(
                uuid7(20),
                uuid7(1),
                vec![uuid7(10), uuid7(10)],
                uuid7(2),
                20,
            ),
            Err(KnowledgeError::Duplicate("input_confidence_uuid"))
        ));
    }

    #[test]
    fn confidence_arrow_decode_rejects_schema_and_closed_policy_drift() {
        let ledger = ConfidenceLedger::explicit(uuid7(10), uuid7(1), 0.8, uuid7(2), 20).unwrap();
        let batch = ledger.assessment_batch().unwrap();
        let bad = RecordBatch::try_new(
            Arc::clone(&CONFIDENCE_ASSESSMENT_SCHEMA),
            vec![
                Arc::clone(batch.column(0)),
                Arc::clone(batch.column(1)),
                Arc::new(StringArray::from(vec!["average"])),
                Arc::clone(batch.column(3)),
                Arc::clone(batch.column(4)),
                Arc::clone(batch.column(5)),
                Arc::clone(batch.column(6)),
                Arc::clone(batch.column(7)),
            ],
        )
        .unwrap();
        assert!(matches!(
            ConfidenceLedger::from_batches(&[bad], &[]),
            Err(KnowledgeError::Invalid {
                field: "policy",
                ..
            })
        ));
    }

    #[test]
    fn evidence_round_trips_orders_fingerprints_and_merges_idempotently() {
        let later = EvidenceLink::new(
            uuid7(31),
            uuid7(1),
            uuid7(41),
            EvidenceSourceKind::Observation,
            EvidenceRole::Contradicts,
            Some(0.25),
            uuid7(51),
            20,
        )
        .unwrap();
        let earlier = EvidenceLink::new(
            uuid7(30),
            uuid7(1),
            uuid7(40),
            EvidenceSourceKind::Document,
            EvidenceRole::Supports,
            Some(-0.0),
            uuid7(50),
            10,
        )
        .unwrap();
        let ledger = EvidenceLedger::new(vec![later, earlier]).unwrap();
        assert_eq!(ledger.links[0].evidence_uuid, uuid7(30));
        assert_eq!(ledger.links[0].weight.unwrap().to_bits(), 0.0f64.to_bits());
        assert_eq!(
            EvidenceLedger::from_batches(&[ledger.batch().unwrap()]).unwrap(),
            ledger
        );
        assert_eq!(ledger.merge(&ledger).unwrap(), ledger);
        let encoded = ledger
            .evidence_fingerprint(uuid7(30))
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            encoded,
            "363146c1fc94d57cc02c098a7187d753d85a7b10ebd715ecbae76e85e1d3fd5e"
        );
    }

    #[test]
    fn evidence_validation_and_conflicting_identity_are_fail_closed() {
        for weight in [f64::NAN, f64::INFINITY, -0.01, 1.01] {
            assert!(matches!(
                EvidenceLink::new(
                    uuid7(30),
                    uuid7(1),
                    uuid7(40),
                    EvidenceSourceKind::Document,
                    EvidenceRole::Supports,
                    Some(weight),
                    uuid7(50),
                    10,
                ),
                Err(KnowledgeError::Invalid {
                    field: "weight",
                    ..
                })
            ));
        }
        let first = EvidenceLedger::new(vec![
            EvidenceLink::new(
                uuid7(30),
                uuid7(1),
                uuid7(40),
                EvidenceSourceKind::Document,
                EvidenceRole::Supports,
                None,
                uuid7(50),
                10,
            )
            .unwrap(),
        ])
        .unwrap();
        let conflict = EvidenceLedger::new(vec![
            EvidenceLink::new(
                uuid7(30),
                uuid7(1),
                uuid7(41),
                EvidenceSourceKind::Observation,
                EvidenceRole::Context,
                None,
                uuid7(50),
                10,
            )
            .unwrap(),
        ])
        .unwrap();
        assert!(matches!(
            first.merge(&conflict),
            Err(KnowledgeError::Conflict("evidence_uuid"))
        ));
    }

    #[test]
    fn algorithm_run_lifecycle_round_trips_and_rejects_second_terminal() {
        assert_eq!(
            ALGORITHM_RUN_SCHEMA_FINGERPRINT
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "ff52080371c5956aa9bc8b0cf9c1022e2c3271d576d43ab57f4b75eb33cf64ed"
        );
        assert_eq!(
            ALGORITHM_RUN_EVENT_SCHEMA_FINGERPRINT
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "f055828f73440cca1bd2ea746cb4599d52a736229d2a4df8ddec70972ceef0aa"
        );
        let run = AlgorithmRun::new(
            uuid7(40),
            "rank.degree".into(),
            1,
            1,
            b"canonical descriptor".to_vec(),
            [7; 32],
            uuid7(41),
            10,
        )
        .unwrap();
        let started = AlgorithmRunEvent::new(
            uuid7(42),
            run.run_uuid,
            AlgorithmRunState::Started,
            None,
            None,
            10,
            run.provenance_uuid,
        )
        .unwrap();
        let completed = AlgorithmRunEvent::new(
            uuid7(43),
            run.run_uuid,
            AlgorithmRunState::Completed,
            Some([8; 32]),
            None,
            11,
            uuid7(44),
        )
        .unwrap();
        let ledger = AlgorithmRunLedger::new(vec![run.clone()], vec![completed, started]).unwrap();
        assert_eq!(
            AlgorithmRunLedger::from_batches(
                &[ledger.run_batch().unwrap()],
                &[ledger.event_batch().unwrap()],
            )
            .unwrap(),
            ledger
        );
        let failed = AlgorithmRunEvent::new(
            uuid7(45),
            run.run_uuid,
            AlgorithmRunState::Failed,
            None,
            Some("GF_EXECUTION".into()),
            12,
            uuid7(46),
        )
        .unwrap();
        let mut events = ledger.events.clone();
        events.push(failed);
        assert!(matches!(
            AlgorithmRunLedger::new(ledger.runs.clone(), events),
            Err(KnowledgeError::Invalid {
                field: "terminal",
                ..
            })
        ));
    }

    #[test]
    fn closed_domain_vocabularies_round_trip_and_reject_unknown_tokens() {
        for value in [GraphObjectKind::Node, GraphObjectKind::Edge] {
            assert_eq!(GraphObjectKind::parse(value.as_str()).unwrap(), value);
        }
        assert!(GraphObjectKind::parse("vertex").is_err());
        for value in [
            AssertionGraphRole::Subject,
            AssertionGraphRole::Object,
            AssertionGraphRole::Context,
        ] {
            assert_eq!(AssertionGraphRole::parse(value.as_str()).unwrap(), value);
        }
        assert!(AssertionGraphRole::parse("target").is_err());
        for value in [
            EvidenceSourceKind::Document,
            EvidenceSourceKind::Observation,
            EvidenceSourceKind::GraphNode,
            EvidenceSourceKind::GraphEdge,
        ] {
            assert_eq!(EvidenceSourceKind::parse(value.as_str()).unwrap(), value);
        }
        assert!(EvidenceSourceKind::parse("web").is_err());
        for value in [
            EvidenceRole::Supports,
            EvidenceRole::Contradicts,
            EvidenceRole::Context,
        ] {
            assert_eq!(EvidenceRole::parse(value.as_str()).unwrap(), value);
        }
        assert!(EvidenceRole::parse("proves").is_err());
        for value in [
            ConfidencePolicy::Explicit,
            ConfidencePolicy::ConservativeMin,
        ] {
            assert_eq!(ConfidencePolicy::parse(value.as_str()).unwrap(), value);
        }
        assert!(ConfidencePolicy::parse("average").is_err());
        for value in [
            AlgorithmRunState::Started,
            AlgorithmRunState::Completed,
            AlgorithmRunState::Failed,
            AlgorithmRunState::Cancelled,
            AlgorithmRunState::Interrupted,
        ] {
            assert_eq!(AlgorithmRunState::parse(value.as_str()).unwrap(), value);
        }
        assert!(!AlgorithmRunState::Started.is_terminal());
        assert!(AlgorithmRunState::Completed.is_terminal());
        assert!(AlgorithmRunState::parse("running").is_err());
    }

    #[test]
    fn knowledge_error_codes_are_closed_and_exact() {
        let cases = [
            (invalid("field", "bad"), "GF_KNOWLEDGE_INVALID"),
            (
                KnowledgeError::Limit {
                    participant: "assertions",
                    observed: 2,
                    limit: 1,
                },
                "GF_RESOURCE_LIMIT",
            ),
            (KnowledgeError::Duplicate("id"), "GF_KNOWLEDGE_DUPLICATE"),
            (KnowledgeError::Dangling("id"), "GF_KNOWLEDGE_DANGLING"),
            (KnowledgeError::Conflict("id"), "GF_IDEMPOTENCY_CONFLICT"),
            (
                KnowledgeError::TransactionConflict("id"),
                "GF_TRANSACTION_CONFLICT",
            ),
            (
                KnowledgeError::Arrow(arrow::error::ArrowError::SchemaError("bad".into())),
                "GF_SCHEMA_MISMATCH",
            ),
        ];
        for (error, code) in cases {
            assert_eq!(error.code(), code);
            assert!(!error.to_string().is_empty());
        }
    }
}
pub use belief_projection::{
    ALGORITHM_INTERPRETATION_ATTACHMENT_SCHEMA, BELIEF_PROJECTION_ATTACHMENT_CONTRACT_VERSION,
    BeliefProjectionAttachment, BeliefProjectionAttachmentLedger,
};
