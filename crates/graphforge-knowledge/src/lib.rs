//! Immutable UUID-referenced analytical knowledge.
//!
//! This crate owns knowledge records, validation, canonical fingerprints,
//! deterministic ordering, and Arrow schemas. It deliberately has no storage,
//! graph, execution, or provenance dependency.
#![forbid(unsafe_code)]

mod algorithm_run;
mod belief_projection;
mod confidence;
mod hypothesis;
mod reasoning;
mod status;
mod supersession;
mod valid_time;

pub use algorithm_run::{AlgorithmRun, AlgorithmRunEvent, AlgorithmRunLedger, AlgorithmRunState};
pub use confidence::{ConfidenceAssessment, ConfidenceInput, ConfidenceLedger, ConfidencePolicy};

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

    pub(super) fn uuid7(seed: u8) -> Uuid {
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
    fn knowledge_schema_registry_excludes_every_epistemic_field() {
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
                KnowledgeError::Canonical(CanonicalError::Malformed("bad")),
                "GF_CANONICAL_INVALID",
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

    #[test]
    fn assertion_and_algorithm_run_merges_cover_idempotent_append_and_conflict_paths() {
        let base = fixture();
        assert_eq!(base.merge(&base).unwrap(), base);
        let second_id = uuid7(20);
        let second = AssertionLedger::new(
            vec![Assertion::new(second_id, "second".into(), uuid7(21), 20).unwrap()],
            vec![
                AssertionGraphRef::new(
                    second_id,
                    uuid7(22),
                    GraphObjectKind::Node,
                    AssertionGraphRole::Subject,
                    0,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let merged = base.merge(&second).unwrap();
        assert_eq!(merged.assertions.len(), 2);
        let conflicting = AssertionLedger::new(
            vec![Assertion::new(uuid7(1), "different".into(), uuid7(2), 10).unwrap()],
            base.graph_refs.clone(),
        )
        .unwrap();
        assert!(matches!(
            base.merge(&conflicting),
            Err(KnowledgeError::Conflict("assertion_uuid"))
        ));

        let run = AlgorithmRun::new(
            uuid7(40),
            "pagerank".into(),
            1,
            1,
            vec![1],
            [2; 32],
            uuid7(41),
            100,
        )
        .unwrap();
        let started = AlgorithmRunEvent::new(
            uuid7(42),
            run.run_uuid,
            AlgorithmRunState::Started,
            None,
            None,
            100,
            run.provenance_uuid,
        )
        .unwrap();
        let initial = AlgorithmRunLedger::new(vec![run.clone()], vec![started.clone()]).unwrap();
        assert_eq!(initial.run(run.run_uuid), Some(&run));
        assert_eq!(initial.events_for(run.run_uuid), vec![started.clone()]);
        assert!(initial.terminal_event(run.run_uuid).is_none());
        assert_eq!(initial.merge(&initial).unwrap(), initial);

        let completed = AlgorithmRunEvent::new(
            uuid7(43),
            run.run_uuid,
            AlgorithmRunState::Completed,
            Some([3; 32]),
            None,
            101,
            uuid7(44),
        )
        .unwrap();
        let staged =
            AlgorithmRunLedger::new(vec![run.clone()], vec![started, completed.clone()]).unwrap();
        let merged = initial.merge(&staged).unwrap();
        assert_eq!(merged.terminal_event(run.run_uuid), Some(&completed));

        let conflicting_run = AlgorithmRun {
            algorithm: "hits".into(),
            ..run.clone()
        };
        let conflict = AlgorithmRunLedger {
            runs: vec![conflicting_run],
            events: vec![],
        };
        assert!(matches!(
            initial.merge(&conflict),
            Err(KnowledgeError::Conflict("run_uuid"))
        ));
        let conflicting_event = AlgorithmRunEvent {
            error_code: Some("GF_EXECUTION".into()),
            state: AlgorithmRunState::Failed,
            ..completed
        };
        let conflict = AlgorithmRunLedger {
            runs: vec![],
            events: vec![conflicting_event],
        };
        assert!(matches!(
            merged.merge(&conflict),
            Err(KnowledgeError::Conflict("event_uuid"))
        ));
    }
}
pub use belief_projection::{
    ALGORITHM_INTERPRETATION_ATTACHMENT_SCHEMA, BELIEF_PROJECTION_ATTACHMENT_CONTRACT_VERSION,
    BeliefProjectionAttachment, BeliefProjectionAttachmentLedger,
};
