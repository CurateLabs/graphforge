//! Immutable GraphForge provenance events and UUID-referenced lineage.
//!
//! This crate is the semantic owner of the M20 provenance capability. It owns
//! record validation, closed value registries, canonical bytes and identities,
//! deterministic ordering, and authoritative Arrow schemas. It does not open
//! project files or depend on graph execution/storage crates.
#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray, TimestampMicrosecondArray,
    UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalError, CanonicalWriter, fingerprint,
    uuid_v8,
};
use uuid::Uuid;

/// Provenance capability contract implemented by this crate.
pub const PROVENANCE_CAPABILITY_VERSION: u32 = 1;
/// Event record contract.
pub const PROVENANCE_EVENT_CONTRACT_VERSION: u32 = 1;
/// Lineage record contract.
pub const LINEAGE_CONTRACT_VERSION: u32 = 1;
/// Closed event-kind registry version.
pub const EVENT_KIND_REGISTRY_VERSION: u32 = 5;
/// Closed subject-kind registry version.
pub const SUBJECT_KIND_REGISTRY_VERSION: u32 = 1;
/// Closed lineage-role registry version.
pub const LINEAGE_ROLE_REGISTRY_VERSION: u32 = 1;
/// Per-participant row bound used by validation before allocation/persistence.
pub const MAX_PROVENANCE_ROWS: usize = 1_000_000;

/// Authoritative `provenance/events.parquet` schema.
pub static PROVENANCE_EVENT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("provenance_uuid", false),
        uuid_field("operation_uuid", false),
        Field::new("event_kind", DataType::Utf8, false),
        uuid_field("actor_uuid", true),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

/// Authoritative `provenance/lineage.parquet` schema.
pub static LINEAGE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("lineage_uuid", false),
        uuid_field("provenance_uuid", false),
        uuid_field("subject_uuid", false),
        Field::new("subject_kind", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("ordinal", DataType::UInt32, false),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static PROVENANCE_EVENT_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"provenance_event/1|provenance_uuid:fixed[16]:required|operation_uuid:fixed[16]:required|event_kind:utf8:required|actor_uuid:fixed[16]:nullable|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered provenance event schema is within canonical bounds")
});

static LINEAGE_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"lineage/1|lineage_uuid:fixed[16]:required|provenance_uuid:fixed[16]:required|subject_uuid:fixed[16]:required|subject_kind:utf8:required|role:utf8:required|ordinal:u32:required|contract_version:u32:required",
    )
    .expect("registered lineage schema is within canonical bounds")
});

fn uuid_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), nullable)
}

/// Closed M20 provenance-event registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// Cypher or construction API created a node.
    CreateNode,
    /// Cypher or construction API created an edge.
    CreateEdge,
    /// MERGE created at least one graph object.
    MergeCreate,
    /// MERGE matched and performed no create.
    MergeMatchedNoop,
    /// SET changed or assigned a property.
    SetProperty,
    /// REMOVE removed a property.
    RemoveProperty,
    /// SET added a label.
    AddLabel,
    /// REMOVE removed a label.
    RemoveLabel,
    /// DELETE removed an unconnected graph object.
    Delete,
    /// DETACH DELETE removed a node and its incident edges.
    DetachDelete,
    /// Ontology inference materialized a persisted graph fact.
    OntologyInference,
    /// An immutable analytical assertion was created.
    CreateAssertion,
    /// An immutable confidence assessment was recorded.
    AssessConfidence,
    /// An immutable evidence link was recorded.
    RecordEvidence,
    /// An immutable algorithm-run lifecycle transition was recorded.
    RecordAlgorithmRun,
    /// An M21 interpretation attachment was appended to a completed run.
    RecordBeliefProjectionAttachment,
}

impl EventKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreateNode => "create_node",
            Self::CreateEdge => "create_edge",
            Self::MergeCreate => "merge_create",
            Self::MergeMatchedNoop => "merge_matched_noop",
            Self::SetProperty => "set_property",
            Self::RemoveProperty => "remove_property",
            Self::AddLabel => "add_label",
            Self::RemoveLabel => "remove_label",
            Self::Delete => "delete",
            Self::DetachDelete => "detach_delete",
            Self::OntologyInference => "ontology_inference",
            Self::CreateAssertion => "create_assertion",
            Self::AssessConfidence => "assess_confidence",
            Self::RecordEvidence => "record_evidence",
            Self::RecordAlgorithmRun => "record_algorithm_run",
            Self::RecordBeliefProjectionAttachment => "record_belief_projection_attachment",
        }
    }

    fn parse(value: &str) -> Result<Self, ProvenanceError> {
        match value {
            "create_node" => Ok(Self::CreateNode),
            "create_edge" => Ok(Self::CreateEdge),
            "merge_create" => Ok(Self::MergeCreate),
            "merge_matched_noop" => Ok(Self::MergeMatchedNoop),
            "set_property" => Ok(Self::SetProperty),
            "remove_property" => Ok(Self::RemoveProperty),
            "add_label" => Ok(Self::AddLabel),
            "remove_label" => Ok(Self::RemoveLabel),
            "delete" => Ok(Self::Delete),
            "detach_delete" => Ok(Self::DetachDelete),
            "ontology_inference" => Ok(Self::OntologyInference),
            "create_assertion" => Ok(Self::CreateAssertion),
            "assess_confidence" => Ok(Self::AssessConfidence),
            "record_evidence" => Ok(Self::RecordEvidence),
            "record_algorithm_run" => Ok(Self::RecordAlgorithmRun),
            "record_belief_projection_attachment" => Ok(Self::RecordBeliefProjectionAttachment),
            _ => Err(invalid("event_kind", "unknown closed value")),
        }
    }
}

/// Closed UUID subject registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubjectKind {
    /// Public graph node UUID.
    Node,
    /// Public graph edge UUID.
    Edge,
    /// Immutable assertion UUID.
    Assertion,
    /// Evidence-link UUID.
    EvidenceLink,
    /// Confidence-assessment UUID.
    ConfidenceAssessment,
    /// Algorithm-run UUID.
    AlgorithmRun,
    /// M21 interpretation attachment UUID.
    BeliefProjectionAttachment,
}

impl SubjectKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "edge",
            Self::Assertion => "assertion",
            Self::EvidenceLink => "evidence_link",
            Self::ConfidenceAssessment => "confidence_assessment",
            Self::AlgorithmRun => "algorithm_run",
            Self::BeliefProjectionAttachment => "belief_projection_attachment",
        }
    }

    fn parse(value: &str) -> Result<Self, ProvenanceError> {
        match value {
            "node" => Ok(Self::Node),
            "edge" => Ok(Self::Edge),
            "assertion" => Ok(Self::Assertion),
            "evidence_link" => Ok(Self::EvidenceLink),
            "confidence_assessment" => Ok(Self::ConfidenceAssessment),
            "algorithm_run" => Ok(Self::AlgorithmRun),
            "belief_projection_attachment" => Ok(Self::BeliefProjectionAttachment),
            _ => Err(invalid("subject_kind", "unknown closed value")),
        }
    }
}

/// Closed lineage direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineageRole {
    /// Object consumed by the operation.
    Input,
    /// Object produced or changed by the operation.
    Output,
}

impl LineageRole {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
        }
    }

    fn parse(value: &str) -> Result<Self, ProvenanceError> {
        match value {
            "input" => Ok(Self::Input),
            "output" => Ok(Self::Output),
            _ => Err(invalid("role", "unknown closed value")),
        }
    }
}

/// One immutable provenance event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvenanceEvent {
    /// Deterministic UUIDv8 derived from the canonical event.
    pub provenance_uuid: Uuid,
    /// Caller transaction/idempotency identity.
    pub operation_uuid: Uuid,
    /// Closed event kind.
    pub event_kind: EventKind,
    /// Optional analyst/agent identity.
    pub actor_uuid: Option<Uuid>,
    /// Injected UTC transaction timestamp in microseconds.
    pub recorded_at_micros: i64,
    /// Event record contract.
    pub contract_version: u32,
}

impl ProvenanceEvent {
    /// Build and identify one canonical event.
    ///
    /// # Errors
    /// Rejects nil operation/actor UUIDs or canonical encoding failures.
    pub fn new(
        operation_uuid: Uuid,
        event_kind: EventKind,
        actor_uuid: Option<Uuid>,
        recorded_at_micros: i64,
    ) -> Result<Self, ProvenanceError> {
        require_uuid(operation_uuid, "operation_uuid")?;
        if let Some(actor_uuid) = actor_uuid {
            require_uuid(actor_uuid, "actor_uuid")?;
        }
        let canonical =
            event_canonical_bytes(operation_uuid, event_kind, actor_uuid, recorded_at_micros)?;
        let provenance_uuid = uuid_v8(fingerprint(
            CanonicalDomain::ProvenanceEvent,
            CANONICAL_CONTRACT_VERSION,
            &canonical,
        )?);
        Ok(Self {
            provenance_uuid,
            operation_uuid,
            event_kind,
            actor_uuid,
            recorded_at_micros,
            contract_version: PROVENANCE_EVENT_CONTRACT_VERSION,
        })
    }

    /// Canonical bytes used for fingerprints and idempotency comparison.
    ///
    /// # Errors
    /// Returns a canonical encoding failure if shared bounds are exceeded.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ProvenanceError> {
        Ok(event_canonical_bytes(
            self.operation_uuid,
            self.event_kind,
            self.actor_uuid,
            self.recorded_at_micros,
        )?)
    }

    /// Full domain-separated event fingerprint.
    ///
    /// # Errors
    /// Returns a canonical encoding failure if shared bounds are exceeded.
    pub fn fingerprint(&self) -> Result<[u8; 32], ProvenanceError> {
        Ok(fingerprint(
            CanonicalDomain::ProvenanceEvent,
            CANONICAL_CONTRACT_VERSION,
            &self.canonical_bytes()?,
        )?)
    }
}

/// One immutable event-to-subject lineage row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineageRecord {
    /// Deterministic UUIDv8 derived from the canonical lineage row.
    pub lineage_uuid: Uuid,
    /// Owning provenance event.
    pub provenance_uuid: Uuid,
    /// Referenced public graph/knowledge object.
    pub subject_uuid: Uuid,
    /// Closed subject kind.
    pub subject_kind: SubjectKind,
    /// Input/output role.
    pub role: LineageRole,
    /// Deterministic position within the role.
    pub ordinal: u32,
    /// Lineage record contract.
    pub contract_version: u32,
}

impl LineageRecord {
    /// Build and identify one canonical lineage row.
    ///
    /// # Errors
    /// Rejects nil UUIDs or canonical encoding failures.
    pub fn new(
        provenance_uuid: Uuid,
        subject_uuid: Uuid,
        subject_kind: SubjectKind,
        role: LineageRole,
        ordinal: u32,
    ) -> Result<Self, ProvenanceError> {
        require_uuid(provenance_uuid, "provenance_uuid")?;
        require_uuid(subject_uuid, "subject_uuid")?;
        let canonical =
            lineage_canonical_bytes(provenance_uuid, subject_uuid, subject_kind, role, ordinal)?;
        let lineage_uuid = uuid_v8(fingerprint(
            CanonicalDomain::Lineage,
            CANONICAL_CONTRACT_VERSION,
            &canonical,
        )?);
        Ok(Self {
            lineage_uuid,
            provenance_uuid,
            subject_uuid,
            subject_kind,
            role,
            ordinal,
            contract_version: LINEAGE_CONTRACT_VERSION,
        })
    }

    /// Canonical bytes used for fingerprints and idempotency comparison.
    ///
    /// # Errors
    /// Returns a canonical encoding failure if shared bounds are exceeded.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ProvenanceError> {
        Ok(lineage_canonical_bytes(
            self.provenance_uuid,
            self.subject_uuid,
            self.subject_kind,
            self.role,
            self.ordinal,
        )?)
    }

    /// Full domain-separated lineage fingerprint.
    ///
    /// # Errors
    /// Returns a canonical encoding failure if shared bounds are exceeded.
    pub fn fingerprint(&self) -> Result<[u8; 32], ProvenanceError> {
        Ok(fingerprint(
            CanonicalDomain::Lineage,
            CANONICAL_CONTRACT_VERSION,
            &self.canonical_bytes()?,
        )?)
    }
}

/// Validated immutable provenance participant content.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProvenanceLedger {
    /// Events ordered by `(recorded_at, provenance_uuid)`.
    pub events: Vec<ProvenanceEvent>,
    /// Lineage ordered by event time and public history sort key.
    pub lineage: Vec<LineageRecord>,
}

impl ProvenanceLedger {
    /// Validate, sort, and construct ledger content.
    ///
    /// # Errors
    /// Rejects row limits, invalid versions/identities, duplicate IDs, dangling
    /// lineage, duplicate role ordinals, and non-canonical derived UUIDs.
    pub fn new(
        mut events: Vec<ProvenanceEvent>,
        mut lineage: Vec<LineageRecord>,
    ) -> Result<Self, ProvenanceError> {
        validate_rows(&events, &lineage)?;
        let times = events
            .iter()
            .map(|event| (event.provenance_uuid, event.recorded_at_micros))
            .collect::<HashMap<_, _>>();
        events.sort_by_key(|event| (event.recorded_at_micros, event.provenance_uuid));
        lineage.sort_by_key(|row| {
            (
                times[&row.provenance_uuid],
                row.provenance_uuid,
                role_order(row.role),
                row.ordinal,
                row.subject_uuid,
            )
        });
        Ok(Self { events, lineage })
    }

    /// Merge a staged ledger idempotently into an existing ledger.
    ///
    /// # Errors
    /// Identical operation event-sets are idempotent. Reuse of an operation
    /// UUID with a different complete event-set, or reuse of an event, lineage,
    /// or role/ordinal identity with different canonical content conflicts.
    pub fn merge(&self, staged: &Self) -> Result<Self, ProvenanceError> {
        let mut events = self.events.clone();
        let mut by_event = events
            .iter()
            .cloned()
            .map(|event| (event.provenance_uuid, event))
            .collect::<HashMap<_, _>>();
        let existing_operations = events_by_operation(&events);
        let staged_operations = events_by_operation(&staged.events);
        for (operation_uuid, staged_events) in &staged_operations {
            if let Some(existing_events) = existing_operations.get(operation_uuid)
                && (existing_events != staged_events
                    || operation_lineage(&self.lineage, existing_events)
                        != operation_lineage(&staged.lineage, staged_events))
            {
                return Err(ProvenanceError::Conflict("operation_uuid"));
            }
        }
        for event in &staged.events {
            if let Some(existing) = by_event.get(&event.provenance_uuid)
                && existing != event
            {
                return Err(ProvenanceError::Conflict("provenance_uuid"));
            }
            if by_event
                .insert(event.provenance_uuid, event.clone())
                .is_none()
            {
                events.push(event.clone());
            }
        }

        let mut lineage = self.lineage.clone();
        let mut by_lineage = lineage
            .iter()
            .cloned()
            .map(|row| (row.lineage_uuid, row))
            .collect::<HashMap<_, _>>();
        for row in &staged.lineage {
            if let Some(existing) = by_lineage.get(&row.lineage_uuid)
                && existing != row
            {
                return Err(ProvenanceError::Conflict("lineage_uuid"));
            }
            if by_lineage.insert(row.lineage_uuid, row.clone()).is_none() {
                lineage.push(row.clone());
            }
        }
        Self::new(events, lineage)
    }

    /// Build the authoritative event Arrow batch.
    ///
    /// # Errors
    /// Returns a structured Arrow construction failure.
    pub fn event_batch(&self) -> Result<RecordBatch, ProvenanceError> {
        event_batch(&self.events)
    }

    /// Build the authoritative lineage Arrow batch.
    ///
    /// # Errors
    /// Returns a structured Arrow construction failure.
    pub fn lineage_batch(&self) -> Result<RecordBatch, ProvenanceError> {
        lineage_batch(&self.lineage)
    }

    /// Decode authoritative Arrow batches and re-run every domain invariant.
    ///
    /// # Errors
    /// Rejects schema drift, nulls, malformed UUIDs, unknown closed values,
    /// unsupported versions, duplicates, dangling references, and row limits.
    pub fn from_batches(
        event_batches: &[RecordBatch],
        lineage_batches: &[RecordBatch],
    ) -> Result<Self, ProvenanceError> {
        let mut events = Vec::new();
        for batch in event_batches {
            require_schema(batch, &PROVENANCE_EVENT_SCHEMA, "event.schema")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let operations = fixed_column(batch, "operation_uuid")?;
            let kinds = string_column(batch, "event_kind")?;
            let actors = fixed_column(batch, "actor_uuid")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                let actor_uuid = if actors.is_null(row) {
                    None
                } else {
                    Some(uuid_at(actors, row, "actor_uuid")?)
                };
                events.push(ProvenanceEvent {
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    operation_uuid: uuid_at(operations, row, "operation_uuid")?,
                    event_kind: EventKind::parse(required_text(kinds, row, "event_kind")?)?,
                    actor_uuid,
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }

        let mut lineage = Vec::new();
        for batch in lineage_batches {
            require_schema(batch, &LINEAGE_SCHEMA, "lineage.schema")?;
            let lineage_ids = fixed_column(batch, "lineage_uuid")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let subjects = fixed_column(batch, "subject_uuid")?;
            let subject_kinds = string_column(batch, "subject_kind")?;
            let roles = string_column(batch, "role")?;
            let ordinals = u32_column(batch, "ordinal")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                lineage.push(LineageRecord {
                    lineage_uuid: uuid_at(lineage_ids, row, "lineage_uuid")?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    subject_uuid: uuid_at(subjects, row, "subject_uuid")?,
                    subject_kind: SubjectKind::parse(required_text(
                        subject_kinds,
                        row,
                        "subject_kind",
                    )?)?,
                    role: LineageRole::parse(required_text(roles, row, "role")?)?,
                    ordinal: required_u32(ordinals, row, "ordinal")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        Self::new(events, lineage)
    }
}

/// Authoritative registry entry for one provenance record family.
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
    /// Closed enum-registry version used by this record family.
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

/// Return the sole authoritative provenance schema registry.
#[must_use]
pub fn schema_registry() -> Vec<SchemaRegistryEntry> {
    vec![
        SchemaRegistryEntry {
            capability_id: "provenance",
            capability_version: PROVENANCE_CAPABILITY_VERSION,
            record_family: "events",
            record_version: PROVENANCE_EVENT_CONTRACT_VERSION,
            schema: Arc::clone(&PROVENANCE_EVENT_SCHEMA),
            schema_fingerprint: *PROVENANCE_EVENT_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[("event_kind", EVENT_KIND_REGISTRY_VERSION)],
            sort_key: &["recorded_at", "provenance_uuid"],
            diff_identity_fields: &["provenance_uuid"],
            diff_record_uuid_field: Some("provenance_uuid"),
            fingerprint_domain: CanonicalDomain::ProvenanceEvent,
            owner: "graphforge-provenance",
            implementation_issue: 773,
            max_rows: MAX_PROVENANCE_ROWS,
        },
        SchemaRegistryEntry {
            capability_id: "provenance",
            capability_version: PROVENANCE_CAPABILITY_VERSION,
            record_family: "lineage",
            record_version: LINEAGE_CONTRACT_VERSION,
            schema: Arc::clone(&LINEAGE_SCHEMA),
            schema_fingerprint: *LINEAGE_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[
                ("subject_kind", SUBJECT_KIND_REGISTRY_VERSION),
                ("role", LINEAGE_ROLE_REGISTRY_VERSION),
            ],
            sort_key: &[
                "recorded_at",
                "provenance_uuid",
                "role",
                "ordinal",
                "subject_uuid",
            ],
            diff_identity_fields: &["lineage_uuid"],
            diff_record_uuid_field: Some("lineage_uuid"),
            fingerprint_domain: CanonicalDomain::Lineage,
            owner: "graphforge-provenance",
            implementation_issue: 773,
            max_rows: MAX_PROVENANCE_ROWS,
        },
    ]
}

/// Structured domain failures.
#[derive(thiserror::Error, Debug)]
pub enum ProvenanceError {
    /// Invalid record value or derived identity.
    #[error("invalid provenance {field}: {message}")]
    Invalid {
        /// Safe field name.
        field: &'static str,
        /// Safe failure summary.
        message: &'static str,
    },
    /// Participant row limit exceeded.
    #[error("provenance {participant} row limit exceeded: observed {observed}, limit {limit}")]
    Limit {
        /// Safe participant name.
        participant: &'static str,
        /// Observed rows.
        observed: usize,
        /// Maximum rows.
        limit: usize,
    },
    /// Duplicate identity in one staged participant.
    #[error("duplicate provenance identity: {0}")]
    Duplicate(&'static str),
    /// Lineage references an event absent from the same participant.
    #[error("dangling provenance reference: {0}")]
    Dangling(&'static str),
    /// Idempotency identity was reused for different content.
    #[error("provenance idempotency conflict: {0}")]
    Conflict(&'static str),
    /// Shared canonicalization failure.
    #[error(transparent)]
    Canonical(#[from] CanonicalError),
    /// Arrow construction failure.
    #[error("provenance Arrow failure: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

impl ProvenanceError {
    /// Stable public error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Invalid { .. } => "GF_PROVENANCE_INVALID",
            Self::Limit { .. } => "GF_RESOURCE_LIMIT",
            Self::Duplicate(_) => "GF_PROVENANCE_DUPLICATE",
            Self::Dangling(_) => "GF_PROVENANCE_DANGLING",
            Self::Conflict(_) => "GF_IDEMPOTENCY_CONFLICT",
            Self::Canonical(error) => error.code(),
            Self::Arrow(_) => "GF_SCHEMA_MISMATCH",
        }
    }
}

fn event_canonical_bytes(
    operation_uuid: Uuid,
    event_kind: EventKind,
    actor_uuid: Option<Uuid>,
    recorded_at_micros: i64,
) -> Result<Vec<u8>, CanonicalError> {
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFPE")?;
    writer.u32(PROVENANCE_EVENT_CONTRACT_VERSION)?;
    writer.raw(operation_uuid.as_bytes())?;
    writer.text(event_kind.as_str())?;
    match actor_uuid {
        Some(actor_uuid) => {
            writer.u8(1)?;
            writer.raw(actor_uuid.as_bytes())?;
        }
        None => writer.u8(0)?,
    }
    writer.i64(recorded_at_micros)?;
    Ok(writer.finish())
}

fn lineage_canonical_bytes(
    provenance_uuid: Uuid,
    subject_uuid: Uuid,
    subject_kind: SubjectKind,
    role: LineageRole,
    ordinal: u32,
) -> Result<Vec<u8>, CanonicalError> {
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFPL")?;
    writer.u32(LINEAGE_CONTRACT_VERSION)?;
    writer.raw(provenance_uuid.as_bytes())?;
    writer.raw(subject_uuid.as_bytes())?;
    writer.text(subject_kind.as_str())?;
    writer.text(role.as_str())?;
    writer.u32(ordinal)?;
    Ok(writer.finish())
}

fn validate_rows(
    events: &[ProvenanceEvent],
    lineage: &[LineageRecord],
) -> Result<(), ProvenanceError> {
    check_limit("events", events.len())?;
    check_limit("lineage", lineage.len())?;
    let mut event_ids = HashSet::with_capacity(events.len());
    let mut operation_kinds = HashSet::with_capacity(events.len());
    for event in events {
        if event.contract_version != PROVENANCE_EVENT_CONTRACT_VERSION {
            return Err(ProvenanceError::Invalid {
                field: "event.contract_version",
                message: "unsupported version",
            });
        }
        require_uuid(event.provenance_uuid, "provenance_uuid")?;
        require_uuid(event.operation_uuid, "operation_uuid")?;
        if let Some(actor_uuid) = event.actor_uuid {
            require_uuid(actor_uuid, "actor_uuid")?;
        }
        if !event_ids.insert(event.provenance_uuid) {
            return Err(ProvenanceError::Duplicate("provenance_uuid"));
        }
        if !operation_kinds.insert((event.operation_uuid, event.event_kind)) {
            return Err(ProvenanceError::Duplicate("operation_uuid/event_kind"));
        }
        let expected = ProvenanceEvent::new(
            event.operation_uuid,
            event.event_kind,
            event.actor_uuid,
            event.recorded_at_micros,
        )?;
        if expected.provenance_uuid != event.provenance_uuid {
            return Err(ProvenanceError::Invalid {
                field: "provenance_uuid",
                message: "does not match canonical event",
            });
        }
    }

    let mut lineage_ids = HashSet::with_capacity(lineage.len());
    let mut positions = HashSet::with_capacity(lineage.len());
    for row in lineage {
        if row.contract_version != LINEAGE_CONTRACT_VERSION {
            return Err(ProvenanceError::Invalid {
                field: "lineage.contract_version",
                message: "unsupported version",
            });
        }
        require_uuid(row.lineage_uuid, "lineage_uuid")?;
        require_uuid(row.subject_uuid, "subject_uuid")?;
        if !event_ids.contains(&row.provenance_uuid) {
            return Err(ProvenanceError::Dangling("provenance_uuid"));
        }
        if !lineage_ids.insert(row.lineage_uuid) {
            return Err(ProvenanceError::Duplicate("lineage_uuid"));
        }
        if !positions.insert((row.provenance_uuid, row.role, row.ordinal)) {
            return Err(ProvenanceError::Duplicate("role/ordinal"));
        }
        let expected = LineageRecord::new(
            row.provenance_uuid,
            row.subject_uuid,
            row.subject_kind,
            row.role,
            row.ordinal,
        )?;
        if expected.lineage_uuid != row.lineage_uuid {
            return Err(ProvenanceError::Invalid {
                field: "lineage_uuid",
                message: "does not match canonical lineage",
            });
        }
    }
    Ok(())
}

type OperationEventIdentity = (Uuid, EventKind, Option<Uuid>, i64, u32);

fn events_by_operation(events: &[ProvenanceEvent]) -> HashMap<Uuid, Vec<OperationEventIdentity>> {
    let mut grouped = HashMap::<_, Vec<_>>::new();
    for event in events {
        grouped.entry(event.operation_uuid).or_default().push((
            event.provenance_uuid,
            event.event_kind,
            event.actor_uuid,
            event.recorded_at_micros,
            event.contract_version,
        ));
    }
    for operation_events in grouped.values_mut() {
        operation_events.sort_by_key(|event| event.0);
    }
    grouped
}

fn operation_lineage(
    lineage: &[LineageRecord],
    events: &[OperationEventIdentity],
) -> Vec<(Uuid, SubjectKind, LineageRole, u32, Uuid, u32)> {
    let event_ids = events.iter().map(|event| event.0).collect::<HashSet<_>>();
    let mut rows = lineage
        .iter()
        .filter(|row| event_ids.contains(&row.provenance_uuid))
        .map(|row| {
            (
                row.lineage_uuid,
                row.subject_kind,
                row.role,
                row.ordinal,
                row.subject_uuid,
                row.contract_version,
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|row| (row.0, row.3, row.4));
    rows
}

fn check_limit(participant: &'static str, observed: usize) -> Result<(), ProvenanceError> {
    if observed > MAX_PROVENANCE_ROWS {
        Err(ProvenanceError::Limit {
            participant,
            observed,
            limit: MAX_PROVENANCE_ROWS,
        })
    } else {
        Ok(())
    }
}

fn require_uuid(value: Uuid, field: &'static str) -> Result<(), ProvenanceError> {
    if value.is_nil() {
        Err(ProvenanceError::Invalid {
            field,
            message: "nil UUID is forbidden",
        })
    } else {
        Ok(())
    }
}

const fn role_order(role: LineageRole) -> u8 {
    match role {
        LineageRole::Input => 0,
        LineageRole::Output => 1,
    }
}

fn event_batch(events: &[ProvenanceEvent]) -> Result<RecordBatch, ProvenanceError> {
    let provenance = fixed_uuid(events.iter().map(|event| event.provenance_uuid))?;
    let operation = fixed_uuid(events.iter().map(|event| event.operation_uuid))?;
    let kinds = StringArray::from_iter_values(events.iter().map(|event| event.event_kind.as_str()));
    let mut actor = FixedSizeBinaryBuilder::with_capacity(events.len(), 16);
    for event in events {
        match event.actor_uuid {
            Some(value) => actor.append_value(value.as_bytes())?,
            None => actor.append_null(),
        }
    }
    let recorded_at = TimestampMicrosecondArray::from(
        events
            .iter()
            .map(|event| event.recorded_at_micros)
            .collect::<Vec<_>>(),
    )
    .with_timezone("UTC");
    let versions = UInt32Array::from_iter_values(events.iter().map(|event| event.contract_version));
    Ok(RecordBatch::try_new(
        Arc::clone(&PROVENANCE_EVENT_SCHEMA),
        vec![
            Arc::new(provenance),
            Arc::new(operation),
            Arc::new(kinds),
            Arc::new(actor.finish()),
            Arc::new(recorded_at),
            Arc::new(versions),
        ],
    )?)
}

fn lineage_batch(rows: &[LineageRecord]) -> Result<RecordBatch, ProvenanceError> {
    let lineage = fixed_uuid(rows.iter().map(|row| row.lineage_uuid))?;
    let provenance = fixed_uuid(rows.iter().map(|row| row.provenance_uuid))?;
    let subjects = fixed_uuid(rows.iter().map(|row| row.subject_uuid))?;
    let subject_kinds =
        StringArray::from_iter_values(rows.iter().map(|row| row.subject_kind.as_str()));
    let roles = StringArray::from_iter_values(rows.iter().map(|row| row.role.as_str()));
    let ordinals = UInt32Array::from_iter_values(rows.iter().map(|row| row.ordinal));
    let versions = UInt32Array::from_iter_values(rows.iter().map(|row| row.contract_version));
    Ok(RecordBatch::try_new(
        Arc::clone(&LINEAGE_SCHEMA),
        vec![
            Arc::new(lineage),
            Arc::new(provenance),
            Arc::new(subjects),
            Arc::new(subject_kinds),
            Arc::new(roles),
            Arc::new(ordinals),
            Arc::new(versions),
        ],
    )?)
}

fn fixed_uuid(
    values: impl IntoIterator<Item = Uuid>,
) -> Result<FixedSizeBinaryArray, arrow::error::ArrowError> {
    let values = values.into_iter();
    let (lower, _) = values.size_hint();
    let mut builder = FixedSizeBinaryBuilder::with_capacity(lower, 16);
    for value in values {
        builder.append_value(value.as_bytes())?;
    }
    Ok(builder.finish())
}

fn require_schema(
    batch: &RecordBatch,
    expected: &SchemaRef,
    field: &'static str,
) -> Result<(), ProvenanceError> {
    if batch.schema().as_ref() == expected.as_ref() {
        Ok(())
    } else {
        Err(invalid(field, "schema mismatch"))
    }
}

fn fixed_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a FixedSizeBinaryArray, ProvenanceError> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a StringArray, ProvenanceError> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn timestamp_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a TimestampMicrosecondArray, ProvenanceError> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<TimestampMicrosecondArray>())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn u32_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a UInt32Array, ProvenanceError> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn uuid_at(
    array: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<Uuid, ProvenanceError> {
    if array.is_null(row) {
        return Err(invalid(field, "null is forbidden"));
    }
    Uuid::from_slice(array.value(row)).map_err(|_| invalid(field, "invalid UUID bytes"))
}

fn required_text<'a>(
    array: &'a StringArray,
    row: usize,
    field: &'static str,
) -> Result<&'a str, ProvenanceError> {
    if array.is_null(row) {
        Err(invalid(field, "null is forbidden"))
    } else {
        Ok(array.value(row))
    }
}

fn required_i64(
    array: &TimestampMicrosecondArray,
    row: usize,
    field: &'static str,
) -> Result<i64, ProvenanceError> {
    if array.is_null(row) {
        Err(invalid(field, "null is forbidden"))
    } else {
        Ok(array.value(row))
    }
}

fn required_u32(
    array: &UInt32Array,
    row: usize,
    field: &'static str,
) -> Result<u32, ProvenanceError> {
    if array.is_null(row) {
        Err(invalid(field, "null is forbidden"))
    } else {
        Ok(array.value(row))
    }
}

const fn invalid(field: &'static str, message: &'static str) -> ProvenanceError {
    ProvenanceError::Invalid { field, message }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, FixedSizeBinaryArray, StringArray};

    use super::*;

    fn uuid(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn fixture() -> (ProvenanceEvent, Vec<LineageRecord>) {
        let event =
            ProvenanceEvent::new(uuid(1), EventKind::CreateEdge, Some(uuid(2)), 123).unwrap();
        let rows = vec![
            LineageRecord::new(
                event.provenance_uuid,
                uuid(4),
                SubjectKind::Edge,
                LineageRole::Output,
                0,
            )
            .unwrap(),
            LineageRecord::new(
                event.provenance_uuid,
                uuid(3),
                SubjectKind::Node,
                LineageRole::Input,
                0,
            )
            .unwrap(),
        ];
        (event, rows)
    }

    #[test]
    fn canonical_ids_and_bytes_are_stable() {
        let (event, rows) = fixture();
        assert_eq!(
            event.provenance_uuid.to_string(),
            "1255afb8-f9f5-806e-8086-f79f9ab73376"
        );
        assert_eq!(
            rows[0].lineage_uuid.to_string(),
            "b2fbd21f-798c-8105-babf-0a1af2d64ea9"
        );
        assert_eq!(event, event.clone());
        assert_eq!(event.fingerprint().unwrap(), event.fingerprint().unwrap());
    }

    #[test]
    fn ledger_orders_history_and_shapes_authoritative_arrow() {
        let (event, mut rows) = fixture();
        rows.reverse();
        let ledger = ProvenanceLedger::new(vec![event.clone()], rows).unwrap();
        assert_eq!(ledger.lineage[0].role, LineageRole::Input);
        assert_eq!(ledger.lineage[1].role, LineageRole::Output);

        let events = ledger.event_batch().unwrap();
        assert_eq!(events.schema(), *PROVENANCE_EVENT_SCHEMA);
        assert_eq!(events.num_rows(), 1);
        assert_eq!(
            events
                .column_by_name("event_kind")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "create_edge"
        );
        assert!(events.column_by_name("actor_uuid").unwrap().is_valid(0));

        let lineage = ledger.lineage_batch().unwrap();
        assert_eq!(lineage.schema(), *LINEAGE_SCHEMA);
        assert_eq!(lineage.num_rows(), 2);
        assert_eq!(
            lineage
                .column_by_name("subject_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            uuid(3).as_bytes()
        );
        assert_eq!(
            ProvenanceLedger::from_batches(&[events], &[lineage]).unwrap(),
            ledger
        );
    }

    #[test]
    fn validation_rejects_nil_tampered_duplicate_and_dangling_rows() {
        assert_eq!(
            ProvenanceEvent::new(Uuid::nil(), EventKind::CreateNode, None, 0)
                .unwrap_err()
                .code(),
            "GF_PROVENANCE_INVALID"
        );

        let (event, rows) = fixture();
        let mut tampered = event.clone();
        tampered.provenance_uuid = uuid(99);
        assert_eq!(
            ProvenanceLedger::new(vec![tampered], vec![])
                .unwrap_err()
                .code(),
            "GF_PROVENANCE_INVALID"
        );
        assert_eq!(
            ProvenanceLedger::new(vec![event.clone(), event.clone()], vec![])
                .unwrap_err()
                .code(),
            "GF_PROVENANCE_DUPLICATE"
        );
        assert_eq!(
            ProvenanceLedger::new(vec![], rows).unwrap_err().code(),
            "GF_PROVENANCE_DANGLING"
        );
    }

    #[test]
    fn identical_merge_is_idempotent_and_conflicts_are_atomic() {
        let (event, rows) = fixture();
        let ledger = ProvenanceLedger::new(vec![event.clone()], rows.clone()).unwrap();
        assert_eq!(ledger.merge(&ledger).unwrap(), ledger);

        let conflicting =
            ProvenanceEvent::new(event.operation_uuid, EventKind::Delete, None, 124).unwrap();
        let staged = ProvenanceLedger::new(vec![conflicting], vec![]).unwrap();
        assert_eq!(
            ledger.merge(&staged).unwrap_err().code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        let conflicting_lineage = ProvenanceLedger::new(
            vec![event.clone()],
            vec![
                rows[1].clone(),
                LineageRecord::new(
                    event.provenance_uuid,
                    uuid(99),
                    SubjectKind::Edge,
                    LineageRole::Output,
                    0,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        assert_eq!(
            ledger.merge(&conflicting_lineage).unwrap_err().code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert_eq!(ledger.events.len(), 1);
        assert_eq!(ledger.lineage.len(), 2);
    }

    #[test]
    fn one_operation_can_record_distinct_composite_mutation_kinds() {
        let operation_uuid = uuid(10);
        let node_event =
            ProvenanceEvent::new(operation_uuid, EventKind::CreateNode, None, 456).unwrap();
        let edge_event =
            ProvenanceEvent::new(operation_uuid, EventKind::CreateEdge, None, 456).unwrap();
        let ledger =
            ProvenanceLedger::new(vec![edge_event.clone(), node_event.clone()], vec![]).unwrap();

        assert_eq!(ledger.events.len(), 2);
        assert_eq!(ledger.merge(&ledger).unwrap(), ledger);

        let partial_retry = ProvenanceLedger::new(vec![node_event], vec![]).unwrap();
        assert_eq!(
            ledger.merge(&partial_retry).unwrap_err().code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );

        let duplicate_kind =
            ProvenanceEvent::new(operation_uuid, EventKind::CreateNode, Some(uuid(11)), 456)
                .unwrap();
        assert_eq!(
            ProvenanceLedger::new(
                vec![edge_event, duplicate_kind.clone(), duplicate_kind],
                vec![]
            )
            .unwrap_err()
            .code(),
            "GF_PROVENANCE_DUPLICATE"
        );
    }

    #[test]
    fn registry_has_one_owner_and_exact_frozen_schemas() {
        let registry = schema_registry();
        assert_eq!(registry.len(), 2);
        assert!(registry.iter().all(|entry| {
            entry.owner == "graphforge-provenance"
                && entry.capability_id == "provenance"
                && entry.capability_version == 1
                && entry.implementation_issue == 773
        }));
        assert_eq!(registry[0].diff_identity_fields, &["provenance_uuid"]);
        assert_eq!(registry[0].diff_record_uuid_field, Some("provenance_uuid"));
        assert_eq!(registry[1].diff_identity_fields, &["lineage_uuid"]);
        assert_eq!(registry[1].diff_record_uuid_field, Some("lineage_uuid"));
        assert_eq!(
            registry[0]
                .schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            [
                "provenance_uuid",
                "operation_uuid",
                "event_kind",
                "actor_uuid",
                "recorded_at",
                "contract_version"
            ]
        );
        assert_eq!(
            registry[1]
                .schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            [
                "lineage_uuid",
                "provenance_uuid",
                "subject_uuid",
                "subject_kind",
                "role",
                "ordinal",
                "contract_version"
            ]
        );
    }

    #[test]
    fn role_ordinals_are_unique_within_each_event() {
        let (event, mut rows) = fixture();
        rows.push(
            LineageRecord::new(
                event.provenance_uuid,
                uuid(5),
                SubjectKind::Node,
                LineageRole::Input,
                0,
            )
            .unwrap(),
        );
        assert_eq!(
            ProvenanceLedger::new(vec![event], rows).unwrap_err().code(),
            "GF_PROVENANCE_DUPLICATE"
        );
    }
}
