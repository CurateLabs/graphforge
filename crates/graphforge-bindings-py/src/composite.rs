//! Thin Python → Rust conversion for composite transactions (#2590).
//!
//! Conversion only. Validation, staging, publication, recovery, and idempotency
//! remain entirely in Rust via [`graphforge_api::GraphForge::publish_composite_transaction`].

use std::collections::HashMap;

use graphforge_api::{
    Assertion, AssertionGraphRef, AssertionGraphRole, AssertionStatus, AssertionStatusEvent,
    AssertionSupersession, AssertionValidityEvent, COMPOSITE_TRANSACTION_CONTRACT_VERSION,
    CompositeGraphMutation, CompositeKnowledgeParticipants, CompositeTransactionRequest,
    ConfidenceAssessment, ConfidenceInput, ConfidencePolicy, EventKind, EvidenceLink, EvidenceRole,
    EvidenceSourceKind, GfError, GraphObjectKind, HypothesisGroup, HypothesisMembershipAction,
    HypothesisMembershipEvent, HypothesisSelectionEvent, LineageRecord, LineageRole, OperationId,
    PropValue, ProvenanceEvent, ReasoningContentFormat, ReasoningKind, ReasoningRecord,
    SubjectKind, WriteContext,
};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::{canonical_operation_id, props_from_dict, py_to_prop_value, to_pyerr};

fn participant_err(error: impl std::fmt::Display) -> GfError {
    GfError::Validation(format!("invalid composite participant: {error}"))
}

fn dict_field<'py>(value: &Bound<'py, PyDict>, name: &str) -> PyResult<Bound<'py, PyAny>> {
    value
        .get_item(name)?
        .ok_or_else(|| PyTypeError::new_err(format!("composite entry requires {name}")))
}

fn optional_dict_field<'py>(
    value: &Bound<'py, PyDict>,
    name: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(value.get_item(name)?.filter(|item| !item.is_none()))
}

fn required_uuid(py: Python<'_>, value: &Bound<'_, PyDict>, name: &str) -> PyResult<uuid::Uuid> {
    let text = dict_field(value, name)?.extract::<String>()?;
    Ok(canonical_operation_id(&text)
        .map_err(|error| to_pyerr(py, &error))?
        .0)
}

fn optional_uuid(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
    name: &str,
) -> PyResult<Option<uuid::Uuid>> {
    optional_dict_field(value, name)?
        .map(|item| {
            let text = item.extract::<String>()?;
            Ok(canonical_operation_id(&text)
                .map_err(|error| to_pyerr(py, &error))?
                .0)
        })
        .transpose()
}

fn required_i64(value: &Bound<'_, PyDict>, name: &str) -> PyResult<i64> {
    dict_field(value, name)?.extract::<i64>()
}

fn optional_i64(value: &Bound<'_, PyDict>, name: &str) -> PyResult<Option<i64>> {
    optional_dict_field(value, name)?
        .map(|item| item.extract::<i64>())
        .transpose()
}

fn required_string(value: &Bound<'_, PyDict>, name: &str) -> PyResult<String> {
    dict_field(value, name)?.extract::<String>()
}

fn optional_f64(value: &Bound<'_, PyDict>, name: &str) -> PyResult<Option<f64>> {
    optional_dict_field(value, name)?
        .map(|item| item.extract::<f64>())
        .transpose()
}

fn event_kind(value: &str) -> Result<EventKind, GfError> {
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
        _ => Err(GfError::Validation(format!(
            "unknown composite provenance event_kind {value:?}"
        ))),
    }
}

fn subject_kind(value: &str) -> Result<SubjectKind, GfError> {
    match value {
        "node" => Ok(SubjectKind::Node),
        "edge" => Ok(SubjectKind::Edge),
        "assertion" => Ok(SubjectKind::Assertion),
        "evidence_link" => Ok(SubjectKind::EvidenceLink),
        "confidence_assessment" => Ok(SubjectKind::ConfidenceAssessment),
        "algorithm_run" => Ok(SubjectKind::AlgorithmRun),
        "belief_projection_attachment" => Ok(SubjectKind::BeliefProjectionAttachment),
        _ => Err(GfError::Validation(format!(
            "unknown composite lineage subject_kind {value:?}"
        ))),
    }
}

fn lineage_role(value: &str) -> Result<LineageRole, GfError> {
    match value {
        "input" => Ok(LineageRole::Input),
        "output" => Ok(LineageRole::Output),
        _ => Err(GfError::Validation(format!(
            "unknown composite lineage role {value:?}"
        ))),
    }
}

fn graph_object_kind(value: &str) -> Result<GraphObjectKind, GfError> {
    match value {
        "node" => Ok(GraphObjectKind::Node),
        "edge" => Ok(GraphObjectKind::Edge),
        _ => Err(GfError::Validation(format!(
            "unknown composite graph_kind {value:?}"
        ))),
    }
}

fn graph_role(value: &str) -> Result<AssertionGraphRole, GfError> {
    match value {
        "subject" => Ok(AssertionGraphRole::Subject),
        "object" => Ok(AssertionGraphRole::Object),
        "context" => Ok(AssertionGraphRole::Context),
        _ => Err(GfError::Validation(format!(
            "unknown composite assertion graph role {value:?}"
        ))),
    }
}

fn confidence_policy(value: &str) -> Result<ConfidencePolicy, GfError> {
    match value {
        "explicit" => Ok(ConfidencePolicy::Explicit),
        "conservative_min" => Ok(ConfidencePolicy::ConservativeMin),
        _ => Err(GfError::Validation(format!(
            "unknown composite confidence policy {value:?}"
        ))),
    }
}

fn evidence_source_kind(value: &str) -> Result<EvidenceSourceKind, GfError> {
    match value {
        "document" => Ok(EvidenceSourceKind::Document),
        "observation" => Ok(EvidenceSourceKind::Observation),
        "graph_node" => Ok(EvidenceSourceKind::GraphNode),
        "graph_edge" => Ok(EvidenceSourceKind::GraphEdge),
        _ => Err(GfError::Validation(format!(
            "unknown composite evidence source_kind {value:?}"
        ))),
    }
}

fn evidence_role(value: &str) -> Result<EvidenceRole, GfError> {
    match value {
        "supports" => Ok(EvidenceRole::Supports),
        "contradicts" => Ok(EvidenceRole::Contradicts),
        "context" => Ok(EvidenceRole::Context),
        _ => Err(GfError::Validation(format!(
            "unknown composite evidence role {value:?}"
        ))),
    }
}

fn reasoning_kind(value: &str) -> Result<ReasoningKind, GfError> {
    match value {
        "evidence_interpretation" => Ok(ReasoningKind::EvidenceInterpretation),
        "logical_inference" => Ok(ReasoningKind::LogicalInference),
        "methodological_note" => Ok(ReasoningKind::MethodologicalNote),
        "decision_rationale" => Ok(ReasoningKind::DecisionRationale),
        _ => Err(GfError::Validation(format!(
            "unknown composite reasoning kind {value:?}"
        ))),
    }
}

fn reasoning_content_format(value: &str) -> Result<ReasoningContentFormat, GfError> {
    match value {
        "text/plain" => Ok(ReasoningContentFormat::TextPlain),
        "text/markdown" => Ok(ReasoningContentFormat::TextMarkdown),
        "application/json" => Ok(ReasoningContentFormat::ApplicationJson),
        _ => Err(GfError::Validation(format!(
            "unknown composite reasoning content_format {value:?}"
        ))),
    }
}

fn membership_action(value: &str) -> Result<HypothesisMembershipAction, GfError> {
    match value {
        "added" => Ok(HypothesisMembershipAction::Added),
        "removed" => Ok(HypothesisMembershipAction::Removed),
        _ => Err(GfError::Validation(format!(
            "unknown composite hypothesis membership action {value:?}"
        ))),
    }
}

fn assertion_status_value(value: &str) -> Result<AssertionStatus, GfError> {
    match value {
        "hypothesis" => Ok(AssertionStatus::Hypothesis),
        "supported" => Ok(AssertionStatus::Supported),
        "refuted" => Ok(AssertionStatus::Refuted),
        "disputed" => Ok(AssertionStatus::Disputed),
        "retracted" => Ok(AssertionStatus::Retracted),
        "superseded" => Ok(AssertionStatus::Superseded),
        _ => Err(GfError::Validation("unknown assertion status".into())),
    }
}

fn mutation_properties(
    _py: Python<'_>,
    value: &Bound<'_, PyDict>,
) -> PyResult<HashMap<String, PropValue>> {
    match optional_dict_field(value, "properties")? {
        None => Ok(HashMap::new()),
        Some(item) => {
            let props = item.cast::<PyDict>().map_err(|_| {
                PyTypeError::new_err("composite mutation properties must be a dict")
            })?;
            props_from_dict(Some(props))
        }
    }
}

fn py_graph_mutation(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<CompositeGraphMutation> {
    let value = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("graph_mutations entries must be dictionaries"))?;
    let kind = match optional_dict_field(value, "kind")? {
        Some(item) => item.extract::<String>()?,
        None => required_string(value, "op")?,
    };
    match kind.as_str() {
        "create_node" => Ok(CompositeGraphMutation::CreateNode {
            node_uuid: required_uuid(py, value, "node_uuid")?,
            label: required_string(value, "label")?,
            properties: mutation_properties(py, value)?,
        }),
        "create_edge" => Ok(CompositeGraphMutation::CreateEdge {
            edge_uuid: required_uuid(py, value, "edge_uuid")?,
            rel_type: required_string(value, "rel_type")?,
            source_uuid: required_uuid(py, value, "source_uuid")?,
            target_uuid: required_uuid(py, value, "target_uuid")?,
            properties: mutation_properties(py, value)?,
        }),
        "delete_node" => Ok(CompositeGraphMutation::DeleteNode {
            node_uuid: required_uuid(py, value, "node_uuid")?,
        }),
        "delete_edge" => Ok(CompositeGraphMutation::DeleteEdge {
            edge_uuid: required_uuid(py, value, "edge_uuid")?,
        }),
        "set_node_property" => Ok(CompositeGraphMutation::SetNodeProperty {
            node_uuid: required_uuid(py, value, "node_uuid")?,
            property: required_string(value, "property")?,
            value: py_to_prop_value(&dict_field(value, "value")?)?,
        }),
        "remove_node_property" => Ok(CompositeGraphMutation::RemoveNodeProperty {
            node_uuid: required_uuid(py, value, "node_uuid")?,
            property: required_string(value, "property")?,
        }),
        "set_edge_property" => Ok(CompositeGraphMutation::SetEdgeProperty {
            edge_uuid: required_uuid(py, value, "edge_uuid")?,
            property: required_string(value, "property")?,
            value: py_to_prop_value(&dict_field(value, "value")?)?,
        }),
        "remove_edge_property" => Ok(CompositeGraphMutation::RemoveEdgeProperty {
            edge_uuid: required_uuid(py, value, "edge_uuid")?,
            property: required_string(value, "property")?,
        }),
        other => Err(PyTypeError::new_err(format!(
            "unknown composite graph mutation kind {other:?}"
        ))),
    }
}

fn py_list_of_dicts<'py>(
    knowledge: &Bound<'py, PyDict>,
    name: &str,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    match optional_dict_field(knowledge, name)? {
        None => Ok(Vec::new()),
        Some(item) => {
            let list = item.cast::<PyList>().map_err(|_| {
                PyTypeError::new_err(format!("composite knowledge.{name} must be a list"))
            })?;
            let mut rows = Vec::with_capacity(list.len());
            for entry in list.iter() {
                let dict = entry.cast::<PyDict>().map_err(|_| {
                    PyTypeError::new_err(format!(
                        "composite knowledge.{name} entries must be dictionaries"
                    ))
                })?;
                rows.push(dict.clone());
            }
            Ok(rows)
        }
    }
}

fn py_provenance_event(py: Python<'_>, value: &Bound<'_, PyDict>) -> PyResult<ProvenanceEvent> {
    let operation_uuid = required_uuid(py, value, "operation_uuid")?;
    let kind = event_kind(&required_string(value, "event_kind")?).map_err(|e| to_pyerr(py, &e))?;
    let actor_uuid = optional_uuid(py, value, "actor_uuid")?;
    let recorded_at_micros = required_i64(value, "recorded_at_micros")?;
    let event = ProvenanceEvent::new(operation_uuid, kind, actor_uuid, recorded_at_micros)
        .map_err(|error| to_pyerr(py, &participant_err(error)))?;
    if let Some(expected) = optional_uuid(py, value, "provenance_uuid")?
        && expected != event.provenance_uuid
    {
        return Err(to_pyerr(
            py,
            &GfError::Validation(
                "composite provenance_uuid does not match the derived Rust identity".into(),
            ),
        ));
    }
    Ok(event)
}

fn py_lineage(py: Python<'_>, value: &Bound<'_, PyDict>) -> PyResult<LineageRecord> {
    LineageRecord::new(
        required_uuid(py, value, "provenance_uuid")?,
        required_uuid(py, value, "subject_uuid")?,
        subject_kind(&required_string(value, "subject_kind")?).map_err(|e| to_pyerr(py, &e))?,
        lineage_role(&required_string(value, "role")?).map_err(|e| to_pyerr(py, &e))?,
        dict_field(value, "ordinal")?.extract::<u32>()?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_assertion(py: Python<'_>, value: &Bound<'_, PyDict>) -> PyResult<Assertion> {
    Assertion::new(
        required_uuid(py, value, "assertion_uuid")?,
        required_string(value, "claim")?,
        required_uuid(py, value, "provenance_uuid")?,
        required_i64(value, "recorded_at_micros")?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_assertion_graph_ref_row(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
) -> PyResult<AssertionGraphRef> {
    AssertionGraphRef::new(
        required_uuid(py, value, "assertion_uuid")?,
        required_uuid(py, value, "graph_uuid")?,
        graph_object_kind(&required_string(value, "graph_kind")?).map_err(|e| to_pyerr(py, &e))?,
        graph_role(&required_string(value, "role")?).map_err(|e| to_pyerr(py, &e))?,
        dict_field(value, "ordinal")?.extract::<u32>()?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_confidence_assessment(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
) -> PyResult<ConfidenceAssessment> {
    ConfidenceAssessment::new(
        required_uuid(py, value, "confidence_uuid")?,
        required_uuid(py, value, "assertion_uuid")?,
        confidence_policy(&required_string(value, "policy")?).map_err(|e| to_pyerr(py, &e))?,
        optional_f64(value, "value")?,
        required_uuid(py, value, "provenance_uuid")?,
        required_i64(value, "recorded_at_micros")?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_confidence_input(py: Python<'_>, value: &Bound<'_, PyDict>) -> PyResult<ConfidenceInput> {
    ConfidenceInput::new(
        required_uuid(py, value, "confidence_uuid")?,
        required_uuid(py, value, "input_confidence_uuid")?,
        optional_f64(value, "input_value")?,
        dict_field(value, "ordinal")?.extract::<u32>()?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_evidence_link(py: Python<'_>, value: &Bound<'_, PyDict>) -> PyResult<EvidenceLink> {
    EvidenceLink::new(
        required_uuid(py, value, "evidence_uuid")?,
        required_uuid(py, value, "assertion_uuid")?,
        required_uuid(py, value, "source_uuid")?,
        evidence_source_kind(&required_string(value, "source_kind")?)
            .map_err(|e| to_pyerr(py, &e))?,
        evidence_role(&required_string(value, "role")?).map_err(|e| to_pyerr(py, &e))?,
        optional_f64(value, "weight")?,
        required_uuid(py, value, "provenance_uuid")?,
        required_i64(value, "recorded_at_micros")?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_reasoning(py: Python<'_>, value: &Bound<'_, PyDict>) -> PyResult<ReasoningRecord> {
    let content_item = dict_field(value, "content")?;
    let content = if let Ok(text) = content_item.extract::<String>() {
        text.into_bytes()
    } else if let Ok(bytes) = content_item.extract::<Vec<u8>>() {
        bytes
    } else {
        return Err(PyTypeError::new_err(
            "composite reasoning content must be str or bytes",
        ));
    };
    ReasoningRecord::new(
        required_uuid(py, value, "reasoning_uuid")?,
        required_uuid(py, value, "assertion_uuid")?,
        reasoning_kind(&required_string(value, "kind")?).map_err(|e| to_pyerr(py, &e))?,
        reasoning_content_format(&required_string(value, "content_format")?)
            .map_err(|e| to_pyerr(py, &e))?,
        content,
        optional_uuid(py, value, "supersedes_reasoning_uuid")?,
        required_uuid(py, value, "provenance_uuid")?,
        required_i64(value, "recorded_at_micros")?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_assertion_status_event(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
) -> PyResult<AssertionStatusEvent> {
    AssertionStatusEvent::new(
        required_uuid(py, value, "status_event_uuid")?,
        required_uuid(py, value, "assertion_uuid")?,
        assertion_status_value(&required_string(value, "status")?).map_err(|e| to_pyerr(py, &e))?,
        optional_uuid(py, value, "confidence_uuid")?,
        optional_uuid(py, value, "reasoning_uuid")?,
        required_uuid(py, value, "provenance_uuid")?,
        required_i64(value, "recorded_at_micros")?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_assertion_supersession(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
) -> PyResult<AssertionSupersession> {
    AssertionSupersession::new(
        required_uuid(py, value, "supersession_uuid")?,
        required_uuid(py, value, "prior_assertion_uuid")?,
        required_uuid(py, value, "replacement_assertion_uuid")?,
        required_uuid(py, value, "status_event_uuid")?,
        required_uuid(py, value, "reasoning_uuid")?,
        required_uuid(py, value, "provenance_uuid")?,
        required_i64(value, "recorded_at_micros")?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_hypothesis_group(py: Python<'_>, value: &Bound<'_, PyDict>) -> PyResult<HypothesisGroup> {
    HypothesisGroup::new(
        required_uuid(py, value, "group_uuid")?,
        required_string(value, "question_key")?,
        required_uuid(py, value, "provenance_uuid")?,
        required_i64(value, "recorded_at_micros")?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_hypothesis_membership(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
) -> PyResult<HypothesisMembershipEvent> {
    HypothesisMembershipEvent::new(
        required_uuid(py, value, "membership_event_uuid")?,
        required_uuid(py, value, "operation_uuid")?,
        required_uuid(py, value, "group_uuid")?,
        required_uuid(py, value, "assertion_uuid")?,
        membership_action(&required_string(value, "action")?).map_err(|e| to_pyerr(py, &e))?,
        required_uuid(py, value, "reasoning_uuid")?,
        required_uuid(py, value, "provenance_uuid")?,
        required_i64(value, "recorded_at_micros")?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_hypothesis_selection(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
) -> PyResult<HypothesisSelectionEvent> {
    HypothesisSelectionEvent::new(
        required_uuid(py, value, "selection_event_uuid")?,
        required_uuid(py, value, "operation_uuid")?,
        required_uuid(py, value, "group_uuid")?,
        optional_uuid(py, value, "selected_assertion_uuid")?,
        required_uuid(py, value, "reasoning_uuid")?,
        required_uuid(py, value, "provenance_uuid")?,
        required_i64(value, "recorded_at_micros")?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn py_assertion_validity(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
) -> PyResult<AssertionValidityEvent> {
    AssertionValidityEvent::new(
        required_uuid(py, value, "validity_event_uuid")?,
        required_uuid(py, value, "assertion_uuid")?,
        optional_i64(value, "valid_from")?,
        optional_i64(value, "valid_to")?,
        optional_uuid(py, value, "reasoning_uuid")?,
        required_uuid(py, value, "provenance_uuid")?,
        required_i64(value, "recorded_at_micros")?,
    )
    .map_err(|error| to_pyerr(py, &participant_err(error)))
}

fn map_knowledge_rows<T>(
    py: Python<'_>,
    knowledge: &Bound<'_, PyDict>,
    name: &str,
    convert: fn(Python<'_>, &Bound<'_, PyDict>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    py_list_of_dicts(knowledge, name)?
        .into_iter()
        .map(|row| convert(py, &row))
        .collect()
}

fn py_knowledge_participants(
    py: Python<'_>,
    knowledge: Option<&Bound<'_, PyDict>>,
) -> PyResult<CompositeKnowledgeParticipants> {
    let Some(knowledge) = knowledge else {
        return Ok(CompositeKnowledgeParticipants::default());
    };
    Ok(CompositeKnowledgeParticipants {
        provenance_events: map_knowledge_rows(
            py,
            knowledge,
            "provenance_events",
            py_provenance_event,
        )?,
        lineage: map_knowledge_rows(py, knowledge, "lineage", py_lineage)?,
        assertions: map_knowledge_rows(py, knowledge, "assertions", py_assertion)?,
        assertion_graph_refs: map_knowledge_rows(
            py,
            knowledge,
            "assertion_graph_refs",
            py_assertion_graph_ref_row,
        )?,
        confidence_assessments: map_knowledge_rows(
            py,
            knowledge,
            "confidence_assessments",
            py_confidence_assessment,
        )?,
        confidence_inputs: map_knowledge_rows(
            py,
            knowledge,
            "confidence_inputs",
            py_confidence_input,
        )?,
        evidence: map_knowledge_rows(py, knowledge, "evidence", py_evidence_link)?,
        reasoning: map_knowledge_rows(py, knowledge, "reasoning", py_reasoning)?,
        assertion_status: map_knowledge_rows(
            py,
            knowledge,
            "assertion_status",
            py_assertion_status_event,
        )?,
        assertion_supersessions: map_knowledge_rows(
            py,
            knowledge,
            "assertion_supersessions",
            py_assertion_supersession,
        )?,
        hypothesis_groups: map_knowledge_rows(
            py,
            knowledge,
            "hypothesis_groups",
            py_hypothesis_group,
        )?,
        hypothesis_membership: map_knowledge_rows(
            py,
            knowledge,
            "hypothesis_membership",
            py_hypothesis_membership,
        )?,
        hypothesis_selection: map_knowledge_rows(
            py,
            knowledge,
            "hypothesis_selection",
            py_hypothesis_selection,
        )?,
        assertion_validity: map_knowledge_rows(
            py,
            knowledge,
            "assertion_validity",
            py_assertion_validity,
        )?,
    })
}

/// Convert one Python composite request into the frozen Rust contract.
pub(crate) fn py_composite_request(
    py: Python<'_>,
    operation_uuid: &str,
    graph_mutations: &Bound<'_, PyList>,
    knowledge: Option<&Bound<'_, PyDict>>,
    actor_uuid: Option<&str>,
    contract_version: u32,
) -> PyResult<CompositeTransactionRequest> {
    let operation_uuid =
        canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
    let actor_uuid = actor_uuid
        .map(canonical_operation_id)
        .transpose()
        .map_err(|error| to_pyerr(py, &error))?
        .map(|OperationId(uuid)| uuid);
    let graph_mutations = graph_mutations
        .iter()
        .map(|entry| py_graph_mutation(py, &entry))
        .collect::<PyResult<Vec<_>>>()?;
    let knowledge = py_knowledge_participants(py, knowledge)?;
    let version = if contract_version == 0 {
        COMPOSITE_TRANSACTION_CONTRACT_VERSION
    } else {
        contract_version
    };
    Ok(CompositeTransactionRequest {
        contract_version: version,
        context: WriteContext {
            operation_uuid,
            actor_uuid,
        },
        graph_mutations,
        knowledge,
    })
}

/// Derive the Rust-owned provenance identity for explicit composite participants.
#[pyfunction]
#[pyo3(signature = (operation_uuid, event_kind, recorded_at_micros, actor_uuid=None))]
pub(crate) fn composite_provenance_uuid(
    py: Python<'_>,
    operation_uuid: &str,
    event_kind: &str,
    recorded_at_micros: i64,
    actor_uuid: Option<&str>,
) -> PyResult<String> {
    let operation_uuid =
        canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
    let actor_uuid = actor_uuid
        .map(canonical_operation_id)
        .transpose()
        .map_err(|error| to_pyerr(py, &error))?
        .map(|OperationId(uuid)| uuid);
    let kind = self::event_kind(event_kind).map_err(|error| to_pyerr(py, &error))?;
    let event = ProvenanceEvent::new(operation_uuid.0, kind, actor_uuid, recorded_at_micros)
        .map_err(|error| to_pyerr(py, &participant_err(error)))?;
    Ok(event.provenance_uuid.to_string())
}
