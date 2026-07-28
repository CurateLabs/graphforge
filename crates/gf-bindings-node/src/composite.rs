//! Thin Node conversion for composite graph + knowledge publication.
//!
//! Converts caller-supplied JS request objects into the frozen Rust
//! [`CompositeTransactionRequest`] and returns the canonical Arrow IPC receipt.
//! Validation, staging, publication, recovery, and idempotency stay in Rust.

use std::collections::HashMap;

use gf_api::{
    ApiErrorCode, Assertion, AssertionGraphRef, AssertionGraphRole, AssertionStatusEvent,
    AssertionSupersession, AssertionValidityEvent, COMPOSITE_TRANSACTION_CONTRACT_VERSION,
    CompositeGraphMutation, CompositeKnowledgeParticipants, CompositeTransactionRequest,
    ConfidenceAssessment, ConfidenceInput, ConfidencePolicy, EventKind, EvidenceLink, EvidenceRole,
    EvidenceSourceKind, GfError, GraphObjectKind, HypothesisGroup, HypothesisMembershipAction,
    HypothesisMembershipEvent, HypothesisSelectionEvent, KnowledgeError, LineageRecord,
    LineageRole, ProjectErrorCode, PropValue, ProvenanceError, ProvenanceEvent,
    ReasoningContentFormat, ReasoningKind, ReasoningRecord, SubjectKind, WriteContext,
};
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;
use uuid::Uuid;

use crate::error::to_napi_err;
use crate::{
    Result, assertion_status, canonical_operation_id, napi_validation, optional_uuid,
    props_from_map, record_batch_to_ipc,
};

/// Thin Node request for one composite graph + knowledge publication.
#[napi(object)]
pub struct CompositeTransactionInput {
    /// Request vocabulary contract version (must be `1`).
    pub contract_version: u32,
    /// Caller operation / idempotency UUID.
    pub operation_uuid: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
    /// Explicit graph mutations in caller order.
    pub graph_mutations: Vec<CompositeGraphMutationInput>,
    /// Explicit M20/M21 participants; omitted families are empty.
    pub knowledge: Option<CompositeKnowledgeInput>,
}

/// One explicit graph mutation. `kind` selects the variant.
#[napi(object)]
pub struct CompositeGraphMutationInput {
    /// `create_node`, `create_edge`, `delete_node`, `delete_edge`,
    /// `set_node_property`, `remove_node_property`, `set_edge_property`,
    /// or `remove_edge_property`.
    pub kind: String,
    /// Caller-supplied node UUID when the kind targets a node.
    pub node_uuid: Option<String>,
    /// Caller-supplied edge UUID when the kind targets an edge.
    pub edge_uuid: Option<String>,
    /// Initial node label for `create_node`.
    pub label: Option<String>,
    /// Relationship type for `create_edge`.
    pub rel_type: Option<String>,
    /// Source node UUID for `create_edge`.
    pub source_uuid: Option<String>,
    /// Target node UUID for `create_edge`.
    pub target_uuid: Option<String>,
    /// Property name for set/remove property kinds.
    pub property: Option<String>,
    /// Property value for set-property kinds.
    pub value: Option<serde_json::Value>,
    /// Initial properties for create kinds.
    pub properties: Option<HashMap<String, serde_json::Value>>,
}

/// Explicit knowledge participant vectors. Absent vectors are empty.
#[napi(object)]
#[derive(Default)]
pub struct CompositeKnowledgeInput {
    /// Provenance events.
    pub provenance_events: Option<Vec<CompositeProvenanceEventInput>>,
    /// Provenance lineage rows.
    pub lineage: Option<Vec<CompositeLineageInput>>,
    /// Immutable assertions.
    pub assertions: Option<Vec<CompositeAssertionInput>>,
    /// Assertion-to-graph references.
    pub assertion_graph_refs: Option<Vec<CompositeAssertionGraphRefInput>>,
    /// Confidence assessments.
    pub confidence_assessments: Option<Vec<CompositeConfidenceAssessmentInput>>,
    /// Confidence input snapshots.
    pub confidence_inputs: Option<Vec<CompositeConfidenceInputRow>>,
    /// Evidence links.
    pub evidence: Option<Vec<CompositeEvidenceInput>>,
    /// Reasoning records.
    pub reasoning: Option<Vec<CompositeReasoningInput>>,
    /// Assertion status events.
    pub assertion_status: Option<Vec<CompositeAssertionStatusInput>>,
    /// Assertion supersession relations.
    pub assertion_supersessions: Option<Vec<CompositeAssertionSupersessionInput>>,
    /// Hypothesis groups.
    pub hypothesis_groups: Option<Vec<CompositeHypothesisGroupInput>>,
    /// Hypothesis membership events.
    pub hypothesis_membership: Option<Vec<CompositeHypothesisMembershipInput>>,
    /// Hypothesis selection events.
    pub hypothesis_selection: Option<Vec<CompositeHypothesisSelectionInput>>,
    /// Assertion validity events.
    pub assertion_validity: Option<Vec<CompositeAssertionValidityInput>>,
}

/// Thin provenance-event construction fields.
#[napi(object)]
pub struct CompositeProvenanceEventInput {
    /// Operation UUID used to derive the event identity.
    pub operation_uuid: String,
    /// Closed event kind spelling.
    pub event_kind: String,
    /// Optional actor UUID.
    pub actor_uuid: Option<String>,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Thin lineage-row construction fields.
#[napi(object)]
pub struct CompositeLineageInput {
    /// Owning provenance event UUID.
    pub provenance_uuid: String,
    /// Referenced subject UUID.
    pub subject_uuid: String,
    /// Closed subject kind.
    pub subject_kind: String,
    /// `input` or `output`.
    pub role: String,
    /// Position within the role.
    pub ordinal: u32,
}

/// Thin assertion construction fields.
#[napi(object)]
pub struct CompositeAssertionInput {
    /// Caller-supplied assertion UUID.
    pub assertion_uuid: String,
    /// Exact claim text.
    pub claim: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Thin assertion-graph-ref construction fields.
#[napi(object)]
pub struct CompositeAssertionGraphRefInput {
    /// Owning assertion UUID.
    pub assertion_uuid: String,
    /// Referenced graph UUID.
    pub graph_uuid: String,
    /// `node` or `edge`.
    pub graph_kind: String,
    /// `subject`, `object`, or `context`.
    pub role: String,
    /// Position within the role.
    pub ordinal: u32,
}

/// Thin confidence-assessment construction fields.
#[napi(object)]
pub struct CompositeConfidenceAssessmentInput {
    /// Caller-supplied confidence UUID.
    pub confidence_uuid: String,
    /// Assessed assertion UUID.
    pub assertion_uuid: String,
    /// `explicit` or `conservative_min`.
    pub policy: String,
    /// Required by `explicit`.
    pub value: Option<f64>,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Thin confidence-input snapshot fields.
#[napi(object)]
pub struct CompositeConfidenceInputRow {
    /// Owning assessment UUID.
    pub confidence_uuid: String,
    /// Requested input assessment UUID.
    pub input_confidence_uuid: String,
    /// Observed value, or absent/null.
    pub input_value: Option<f64>,
    /// Position within the assessment.
    pub ordinal: u32,
}

/// Thin evidence-link construction fields.
#[napi(object)]
pub struct CompositeEvidenceInput {
    /// Caller-supplied evidence UUID.
    pub evidence_uuid: String,
    /// Linked assertion UUID.
    pub assertion_uuid: String,
    /// Source identity UUID.
    pub source_uuid: String,
    /// Closed source kind.
    pub source_kind: String,
    /// Closed evidence role.
    pub role: String,
    /// Optional weight in `[0, 1]`.
    pub weight: Option<f64>,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Thin reasoning-record construction fields.
#[napi(object)]
pub struct CompositeReasoningInput {
    /// Caller-supplied reasoning UUID.
    pub reasoning_uuid: String,
    /// Linked assertion UUID.
    pub assertion_uuid: String,
    /// Closed reasoning kind.
    pub kind: String,
    /// Closed content media type.
    pub content_format: String,
    /// Exact content bytes.
    pub content: Buffer,
    /// Optional prior reasoning UUID amended by this record.
    pub supersedes_reasoning_uuid: Option<String>,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Thin assertion-status construction fields.
#[napi(object)]
pub struct CompositeAssertionStatusInput {
    /// Caller-supplied status-event UUID.
    pub status_event_uuid: String,
    /// Linked assertion UUID.
    pub assertion_uuid: String,
    /// Closed status spelling.
    pub status: String,
    /// Optional confidence UUID.
    pub confidence_uuid: Option<String>,
    /// Optional reasoning UUID.
    pub reasoning_uuid: Option<String>,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Thin assertion-supersession construction fields.
#[napi(object)]
pub struct CompositeAssertionSupersessionInput {
    /// Caller-supplied supersession UUID.
    pub supersession_uuid: String,
    /// Prior assertion UUID.
    pub prior_assertion_uuid: String,
    /// Replacement assertion UUID.
    pub replacement_assertion_uuid: String,
    /// Paired superseded status-event UUID.
    pub status_event_uuid: String,
    /// Reasoning UUID.
    pub reasoning_uuid: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Thin hypothesis-group construction fields.
#[napi(object)]
pub struct CompositeHypothesisGroupInput {
    /// Caller-supplied group UUID.
    pub group_uuid: String,
    /// Canonical question key.
    pub question_key: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Thin hypothesis-membership construction fields.
#[napi(object)]
pub struct CompositeHypothesisMembershipInput {
    /// Caller-supplied membership-event UUID.
    pub membership_event_uuid: String,
    /// Publication operation UUID.
    pub operation_uuid: String,
    /// Group UUID.
    pub group_uuid: String,
    /// Assertion UUID.
    pub assertion_uuid: String,
    /// `added` or `removed`.
    pub action: String,
    /// Reasoning UUID.
    pub reasoning_uuid: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Thin hypothesis-selection construction fields.
#[napi(object)]
pub struct CompositeHypothesisSelectionInput {
    /// Caller-supplied selection-event UUID.
    pub selection_event_uuid: String,
    /// Publication operation UUID.
    pub operation_uuid: String,
    /// Group UUID.
    pub group_uuid: String,
    /// Selected assertion, or absent to clear.
    pub selected_assertion_uuid: Option<String>,
    /// Reasoning UUID.
    pub reasoning_uuid: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Thin assertion-validity construction fields.
#[napi(object)]
pub struct CompositeAssertionValidityInput {
    /// Caller-supplied validity-event UUID.
    pub validity_event_uuid: String,
    /// Linked assertion UUID.
    pub assertion_uuid: String,
    /// Inclusive lower bound, or unbounded.
    pub valid_from_micros: Option<i64>,
    /// Exclusive upper bound, or unbounded.
    pub valid_to_micros: Option<i64>,
    /// Optional reasoning UUID.
    pub reasoning_uuid: Option<String>,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
}

/// Convert one Node composite request and publish through Rust.
pub(crate) fn publish_composite_transaction(
    graph: &gf_api::GraphForge,
    request: CompositeTransactionInput,
) -> Result<Buffer> {
    let converted = convert_request(request)?;
    let receipt = graph
        .publish_composite_transaction(converted)
        .map_err(|error| to_napi_err(&error))?;
    record_batch_to_ipc(&receipt)
        .map(Buffer::from)
        .map_err(|error| to_napi_err(&error))
}

fn convert_request(request: CompositeTransactionInput) -> Result<CompositeTransactionRequest> {
    if request.contract_version != COMPOSITE_TRANSACTION_CONTRACT_VERSION {
        return Err(to_napi_err(&GfError::Validation(
            "composite request has an unsupported contract version".into(),
        )));
    }
    let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
    let actor_uuid = optional_uuid(request.actor_uuid.as_deref())?;
    let graph_mutations = request
        .graph_mutations
        .into_iter()
        .map(convert_graph_mutation)
        .collect::<Result<Vec<_>>>()?;
    let knowledge = convert_knowledge(request.knowledge.unwrap_or_default())?;
    Ok(CompositeTransactionRequest {
        contract_version: request.contract_version,
        context: WriteContext {
            operation_uuid,
            actor_uuid,
        },
        graph_mutations,
        knowledge,
    })
}

fn convert_graph_mutation(value: CompositeGraphMutationInput) -> Result<CompositeGraphMutation> {
    match value.kind.as_str() {
        "create_node" => Ok(CompositeGraphMutation::CreateNode {
            node_uuid: require_uuid(value.node_uuid.as_deref(), "node_uuid")?,
            label: value
                .label
                .ok_or_else(|| napi_validation("create_node requires label"))?,
            properties: props_from_map(value.properties)?,
        }),
        "create_edge" => Ok(CompositeGraphMutation::CreateEdge {
            edge_uuid: require_uuid(value.edge_uuid.as_deref(), "edge_uuid")?,
            rel_type: value
                .rel_type
                .ok_or_else(|| napi_validation("create_edge requires rel_type"))?,
            source_uuid: require_uuid(value.source_uuid.as_deref(), "source_uuid")?,
            target_uuid: require_uuid(value.target_uuid.as_deref(), "target_uuid")?,
            properties: props_from_map(value.properties)?,
        }),
        "delete_node" => Ok(CompositeGraphMutation::DeleteNode {
            node_uuid: require_uuid(value.node_uuid.as_deref(), "node_uuid")?,
        }),
        "delete_edge" => Ok(CompositeGraphMutation::DeleteEdge {
            edge_uuid: require_uuid(value.edge_uuid.as_deref(), "edge_uuid")?,
        }),
        "set_node_property" => Ok(CompositeGraphMutation::SetNodeProperty {
            node_uuid: require_uuid(value.node_uuid.as_deref(), "node_uuid")?,
            property: value
                .property
                .ok_or_else(|| napi_validation("set_node_property requires property"))?,
            value: require_prop_value(value.value)?,
        }),
        "remove_node_property" => Ok(CompositeGraphMutation::RemoveNodeProperty {
            node_uuid: require_uuid(value.node_uuid.as_deref(), "node_uuid")?,
            property: value
                .property
                .ok_or_else(|| napi_validation("remove_node_property requires property"))?,
        }),
        "set_edge_property" => Ok(CompositeGraphMutation::SetEdgeProperty {
            edge_uuid: require_uuid(value.edge_uuid.as_deref(), "edge_uuid")?,
            property: value
                .property
                .ok_or_else(|| napi_validation("set_edge_property requires property"))?,
            value: require_prop_value(value.value)?,
        }),
        "remove_edge_property" => Ok(CompositeGraphMutation::RemoveEdgeProperty {
            edge_uuid: require_uuid(value.edge_uuid.as_deref(), "edge_uuid")?,
            property: value
                .property
                .ok_or_else(|| napi_validation("remove_edge_property requires property"))?,
        }),
        _ => Err(napi_validation("unknown composite graph mutation kind")),
    }
}

fn convert_knowledge(value: CompositeKnowledgeInput) -> Result<CompositeKnowledgeParticipants> {
    Ok(CompositeKnowledgeParticipants {
        provenance_events: value
            .provenance_events
            .unwrap_or_default()
            .into_iter()
            .map(convert_provenance_event)
            .collect::<Result<_>>()?,
        lineage: value
            .lineage
            .unwrap_or_default()
            .into_iter()
            .map(convert_lineage)
            .collect::<Result<_>>()?,
        assertions: value
            .assertions
            .unwrap_or_default()
            .into_iter()
            .map(convert_assertion)
            .collect::<Result<_>>()?,
        assertion_graph_refs: value
            .assertion_graph_refs
            .unwrap_or_default()
            .into_iter()
            .map(convert_assertion_graph_ref)
            .collect::<Result<_>>()?,
        confidence_assessments: value
            .confidence_assessments
            .unwrap_or_default()
            .into_iter()
            .map(convert_confidence_assessment)
            .collect::<Result<_>>()?,
        confidence_inputs: value
            .confidence_inputs
            .unwrap_or_default()
            .into_iter()
            .map(convert_confidence_input)
            .collect::<Result<_>>()?,
        evidence: value
            .evidence
            .unwrap_or_default()
            .into_iter()
            .map(convert_evidence)
            .collect::<Result<_>>()?,
        reasoning: value
            .reasoning
            .unwrap_or_default()
            .into_iter()
            .map(convert_reasoning)
            .collect::<Result<_>>()?,
        assertion_status: value
            .assertion_status
            .unwrap_or_default()
            .into_iter()
            .map(convert_assertion_status)
            .collect::<Result<_>>()?,
        assertion_supersessions: value
            .assertion_supersessions
            .unwrap_or_default()
            .into_iter()
            .map(convert_assertion_supersession)
            .collect::<Result<_>>()?,
        hypothesis_groups: value
            .hypothesis_groups
            .unwrap_or_default()
            .into_iter()
            .map(convert_hypothesis_group)
            .collect::<Result<_>>()?,
        hypothesis_membership: value
            .hypothesis_membership
            .unwrap_or_default()
            .into_iter()
            .map(convert_hypothesis_membership)
            .collect::<Result<_>>()?,
        hypothesis_selection: value
            .hypothesis_selection
            .unwrap_or_default()
            .into_iter()
            .map(convert_hypothesis_selection)
            .collect::<Result<_>>()?,
        assertion_validity: value
            .assertion_validity
            .unwrap_or_default()
            .into_iter()
            .map(convert_assertion_validity)
            .collect::<Result<_>>()?,
    })
}

fn map_provenance_error(error: ProvenanceError) -> GfError {
    let message = error.to_string();
    match error {
        ProvenanceError::Conflict(_) => GfError::Project {
            code: ProjectErrorCode::TransactionConflict,
            message,
        },
        ProvenanceError::Limit { .. } => GfError::Api {
            code: ApiErrorCode::ResourceLimit,
            message,
        },
        ProvenanceError::Invalid { .. }
        | ProvenanceError::Duplicate(_)
        | ProvenanceError::Dangling(_)
        | ProvenanceError::Arrow(_) => GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message,
        },
        ProvenanceError::Canonical(_) => GfError::Validation(message),
    }
}

fn map_knowledge_error(error: KnowledgeError) -> GfError {
    let message = error.to_string();
    match error {
        KnowledgeError::Conflict(_) | KnowledgeError::TransactionConflict(_) => GfError::Project {
            code: ProjectErrorCode::TransactionConflict,
            message,
        },
        KnowledgeError::Limit { .. } => GfError::Api {
            code: ApiErrorCode::ResourceLimit,
            message,
        },
        KnowledgeError::Dangling(_) => GfError::Api {
            code: ApiErrorCode::NotFound,
            message,
        },
        KnowledgeError::Invalid { .. }
        | KnowledgeError::Duplicate(_)
        | KnowledgeError::Canonical(_) => GfError::Validation(message),
        KnowledgeError::Arrow(_) => GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message,
        },
    }
}

fn convert_provenance_event(value: CompositeProvenanceEventInput) -> Result<ProvenanceEvent> {
    ProvenanceEvent::new(
        require_uuid(Some(value.operation_uuid.as_str()), "operation_uuid")?,
        event_kind(&value.event_kind)?,
        optional_uuid(value.actor_uuid.as_deref())?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_provenance_error(error)))
}

fn convert_lineage(value: CompositeLineageInput) -> Result<LineageRecord> {
    LineageRecord::new(
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        require_uuid(Some(value.subject_uuid.as_str()), "subject_uuid")?,
        subject_kind(&value.subject_kind)?,
        lineage_role(&value.role)?,
        value.ordinal,
    )
    .map_err(|error| to_napi_err(&map_provenance_error(error)))
}

fn convert_assertion(value: CompositeAssertionInput) -> Result<Assertion> {
    Assertion::new(
        require_uuid(Some(value.assertion_uuid.as_str()), "assertion_uuid")?,
        value.claim,
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_assertion_graph_ref(
    value: CompositeAssertionGraphRefInput,
) -> Result<AssertionGraphRef> {
    AssertionGraphRef::new(
        require_uuid(Some(value.assertion_uuid.as_str()), "assertion_uuid")?,
        require_uuid(Some(value.graph_uuid.as_str()), "graph_uuid")?,
        graph_kind(&value.graph_kind)?,
        assertion_graph_role(&value.role)?,
        value.ordinal,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_confidence_assessment(
    value: CompositeConfidenceAssessmentInput,
) -> Result<ConfidenceAssessment> {
    ConfidenceAssessment::new(
        require_uuid(Some(value.confidence_uuid.as_str()), "confidence_uuid")?,
        require_uuid(Some(value.assertion_uuid.as_str()), "assertion_uuid")?,
        confidence_policy(&value.policy)?,
        value.value,
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_confidence_input(value: CompositeConfidenceInputRow) -> Result<ConfidenceInput> {
    ConfidenceInput::new(
        require_uuid(Some(value.confidence_uuid.as_str()), "confidence_uuid")?,
        require_uuid(
            Some(value.input_confidence_uuid.as_str()),
            "input_confidence_uuid",
        )?,
        value.input_value,
        value.ordinal,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_evidence(value: CompositeEvidenceInput) -> Result<EvidenceLink> {
    EvidenceLink::new(
        require_uuid(Some(value.evidence_uuid.as_str()), "evidence_uuid")?,
        require_uuid(Some(value.assertion_uuid.as_str()), "assertion_uuid")?,
        require_uuid(Some(value.source_uuid.as_str()), "source_uuid")?,
        evidence_source_kind(&value.source_kind)?,
        evidence_role(&value.role)?,
        value.weight,
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_reasoning(value: CompositeReasoningInput) -> Result<ReasoningRecord> {
    ReasoningRecord::new(
        require_uuid(Some(value.reasoning_uuid.as_str()), "reasoning_uuid")?,
        require_uuid(Some(value.assertion_uuid.as_str()), "assertion_uuid")?,
        reasoning_kind(&value.kind)?,
        reasoning_content_format(&value.content_format)?,
        value.content.to_vec(),
        optional_uuid(value.supersedes_reasoning_uuid.as_deref())?,
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_assertion_status(value: CompositeAssertionStatusInput) -> Result<AssertionStatusEvent> {
    AssertionStatusEvent::new(
        require_uuid(Some(value.status_event_uuid.as_str()), "status_event_uuid")?,
        require_uuid(Some(value.assertion_uuid.as_str()), "assertion_uuid")?,
        assertion_status(&value.status)?,
        optional_uuid(value.confidence_uuid.as_deref())?,
        optional_uuid(value.reasoning_uuid.as_deref())?,
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_assertion_supersession(
    value: CompositeAssertionSupersessionInput,
) -> Result<AssertionSupersession> {
    AssertionSupersession::new(
        require_uuid(Some(value.supersession_uuid.as_str()), "supersession_uuid")?,
        require_uuid(
            Some(value.prior_assertion_uuid.as_str()),
            "prior_assertion_uuid",
        )?,
        require_uuid(
            Some(value.replacement_assertion_uuid.as_str()),
            "replacement_assertion_uuid",
        )?,
        require_uuid(Some(value.status_event_uuid.as_str()), "status_event_uuid")?,
        require_uuid(Some(value.reasoning_uuid.as_str()), "reasoning_uuid")?,
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_hypothesis_group(value: CompositeHypothesisGroupInput) -> Result<HypothesisGroup> {
    HypothesisGroup::new(
        require_uuid(Some(value.group_uuid.as_str()), "group_uuid")?,
        value.question_key,
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_hypothesis_membership(
    value: CompositeHypothesisMembershipInput,
) -> Result<HypothesisMembershipEvent> {
    let action = match value.action.as_str() {
        "added" => HypothesisMembershipAction::Added,
        "removed" => HypothesisMembershipAction::Removed,
        _ => return Err(napi_validation("action must be 'added' or 'removed'")),
    };
    HypothesisMembershipEvent::new(
        require_uuid(
            Some(value.membership_event_uuid.as_str()),
            "membership_event_uuid",
        )?,
        require_uuid(Some(value.operation_uuid.as_str()), "operation_uuid")?,
        require_uuid(Some(value.group_uuid.as_str()), "group_uuid")?,
        require_uuid(Some(value.assertion_uuid.as_str()), "assertion_uuid")?,
        action,
        require_uuid(Some(value.reasoning_uuid.as_str()), "reasoning_uuid")?,
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_hypothesis_selection(
    value: CompositeHypothesisSelectionInput,
) -> Result<HypothesisSelectionEvent> {
    HypothesisSelectionEvent::new(
        require_uuid(
            Some(value.selection_event_uuid.as_str()),
            "selection_event_uuid",
        )?,
        require_uuid(Some(value.operation_uuid.as_str()), "operation_uuid")?,
        require_uuid(Some(value.group_uuid.as_str()), "group_uuid")?,
        optional_uuid(value.selected_assertion_uuid.as_deref())?,
        require_uuid(Some(value.reasoning_uuid.as_str()), "reasoning_uuid")?,
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn convert_assertion_validity(
    value: CompositeAssertionValidityInput,
) -> Result<AssertionValidityEvent> {
    AssertionValidityEvent::new(
        require_uuid(
            Some(value.validity_event_uuid.as_str()),
            "validity_event_uuid",
        )?,
        require_uuid(Some(value.assertion_uuid.as_str()), "assertion_uuid")?,
        value.valid_from_micros,
        value.valid_to_micros,
        optional_uuid(value.reasoning_uuid.as_deref())?,
        require_uuid(Some(value.provenance_uuid.as_str()), "provenance_uuid")?,
        value.recorded_at_micros,
    )
    .map_err(|error| to_napi_err(&map_knowledge_error(error)))
}

fn require_uuid(value: Option<&str>, field: &'static str) -> Result<Uuid> {
    let Some(value) = value else {
        return Err(to_napi_err(&GfError::Validation(format!(
            "{field} is required"
        ))));
    };
    Ok(canonical_operation_id(value)?.0)
}

fn require_prop_value(value: Option<serde_json::Value>) -> Result<PropValue> {
    let Some(value) = value else {
        return Err(napi_validation("property value is required"));
    };
    let mut map = HashMap::new();
    map.insert("value".to_owned(), value);
    let mut converted = props_from_map(Some(map))?;
    converted
        .remove("value")
        .ok_or_else(|| napi_validation("property value is required"))
}

fn event_kind(value: &str) -> Result<EventKind> {
    match value {
        "create_node" => Ok(EventKind::CreateNode),
        "create_edge" => Ok(EventKind::CreateEdge),
        "merge_create" => Ok(EventKind::MergeCreate),
        "merge_matched_noop" => Ok(EventKind::MergeMatchedNoop),
        "set_property" => Ok(EventKind::SetProperty),
        "remove_property" => Ok(EventKind::RemoveProperty),
        "add_label" => Ok(EventKind::AddLabel),
        "remove_label" => Ok(EventKind::RemoveLabel),
        "delete" => Ok(EventKind::Delete),
        "detach_delete" => Ok(EventKind::DetachDelete),
        "ontology_inference" => Ok(EventKind::OntologyInference),
        "create_assertion" => Ok(EventKind::CreateAssertion),
        "assess_confidence" => Ok(EventKind::AssessConfidence),
        "record_evidence" => Ok(EventKind::RecordEvidence),
        "record_algorithm_run" => Ok(EventKind::RecordAlgorithmRun),
        "record_belief_projection_attachment" => Ok(EventKind::RecordBeliefProjectionAttachment),
        _ => Err(napi_validation("unknown provenance event_kind")),
    }
}

fn subject_kind(value: &str) -> Result<SubjectKind> {
    match value {
        "node" => Ok(SubjectKind::Node),
        "edge" => Ok(SubjectKind::Edge),
        "assertion" => Ok(SubjectKind::Assertion),
        "evidence_link" => Ok(SubjectKind::EvidenceLink),
        "confidence_assessment" => Ok(SubjectKind::ConfidenceAssessment),
        "algorithm_run" => Ok(SubjectKind::AlgorithmRun),
        "belief_projection_attachment" => Ok(SubjectKind::BeliefProjectionAttachment),
        _ => Err(napi_validation("unknown lineage subject_kind")),
    }
}

fn lineage_role(value: &str) -> Result<LineageRole> {
    match value {
        "input" => Ok(LineageRole::Input),
        "output" => Ok(LineageRole::Output),
        _ => Err(napi_validation("unknown lineage role")),
    }
}

fn graph_kind(value: &str) -> Result<GraphObjectKind> {
    match value {
        "node" => Ok(GraphObjectKind::Node),
        "edge" => Ok(GraphObjectKind::Edge),
        _ => Err(napi_validation("unknown graph_kind")),
    }
}

fn assertion_graph_role(value: &str) -> Result<AssertionGraphRole> {
    match value {
        "subject" => Ok(AssertionGraphRole::Subject),
        "object" => Ok(AssertionGraphRole::Object),
        "context" => Ok(AssertionGraphRole::Context),
        _ => Err(napi_validation("unknown assertion graph role")),
    }
}

fn confidence_policy(value: &str) -> Result<ConfidencePolicy> {
    match value {
        "explicit" => Ok(ConfidencePolicy::Explicit),
        "conservative_min" => Ok(ConfidencePolicy::ConservativeMin),
        _ => Err(napi_validation("unknown confidence policy")),
    }
}

fn evidence_source_kind(value: &str) -> Result<EvidenceSourceKind> {
    match value {
        "document" => Ok(EvidenceSourceKind::Document),
        "observation" => Ok(EvidenceSourceKind::Observation),
        "graph_node" => Ok(EvidenceSourceKind::GraphNode),
        "graph_edge" => Ok(EvidenceSourceKind::GraphEdge),
        _ => Err(napi_validation("unknown evidence source_kind")),
    }
}

fn evidence_role(value: &str) -> Result<EvidenceRole> {
    match value {
        "supports" => Ok(EvidenceRole::Supports),
        "contradicts" => Ok(EvidenceRole::Contradicts),
        "context" => Ok(EvidenceRole::Context),
        _ => Err(napi_validation("unknown evidence role")),
    }
}

fn reasoning_kind(value: &str) -> Result<ReasoningKind> {
    match value {
        "evidence_interpretation" => Ok(ReasoningKind::EvidenceInterpretation),
        "logical_inference" => Ok(ReasoningKind::LogicalInference),
        "methodological_note" => Ok(ReasoningKind::MethodologicalNote),
        "decision_rationale" => Ok(ReasoningKind::DecisionRationale),
        _ => Err(napi_validation("unknown reasoning kind")),
    }
}

fn reasoning_content_format(value: &str) -> Result<ReasoningContentFormat> {
    match value {
        "text/plain" => Ok(ReasoningContentFormat::TextPlain),
        "text/markdown" => Ok(ReasoningContentFormat::TextMarkdown),
        "application/json" => Ok(ReasoningContentFormat::ApplicationJson),
        _ => Err(napi_validation("unknown reasoning content format")),
    }
}
