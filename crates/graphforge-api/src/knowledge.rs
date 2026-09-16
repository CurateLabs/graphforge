//! `graphforge-api` orchestration for immutable UUID-referenced assertions.

mod assertions;
mod ledger;
mod supporting;
use ledger::assertion_evidence_publication_participants;
use ledger::assertion_publication_participants;
pub(crate) use ledger::assertion_result;
use ledger::assertion_status_bundle_participants;
pub(crate) use ledger::concat_or_empty;
use ledger::confidence_publication_participants;
pub(crate) use ledger::empty_epistemic_participants;
pub(crate) use ledger::empty_participants;
pub(crate) use ledger::encode_confidence_ledger;
pub(crate) use ledger::encode_evidence_ledger;
pub(crate) use ledger::encode_ledger;
pub(crate) use ledger::encode_reasoning_ledger;
pub(crate) use ledger::encode_status_ledger;
pub(crate) use ledger::encode_supersession_ledger;
use ledger::evidence_publication_participants;
pub(crate) use ledger::knowledge_generation_uuid;
use ledger::merged_assertion_evidence_provenance;
use ledger::merged_confidence_provenance;
use ledger::merged_evidence_provenance;
use ledger::merged_provenance;
pub(crate) use ledger::participant;
pub(crate) use ledger::read_confidence_ledger;
pub(crate) use ledger::read_evidence_ledger;
pub(crate) use ledger::read_ledger;
pub(crate) use ledger::read_parquet;
pub(crate) use ledger::read_reasoning_ledger;
pub(crate) use ledger::read_status_ledger;
pub(crate) use ledger::read_supersession_ledger;
use ledger::reasoning_publication_participants;
pub(crate) use ledger::require_participant_contract;
pub(crate) use ledger::snapshot_to_participant;
use ledger::status_publication_participants;
use ledger::supersession_publication_participants;
pub(crate) use ledger::with_next_token;

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};
use graphforge_knowledge::{
    ASSERTION_STATUS_SCHEMA, ASSERTION_SUPERSESSION_SCHEMA, Assertion, AssertionGraphRef,
    AssertionGraphRole, AssertionLedger, AssertionStatus, AssertionStatusEvent,
    AssertionStatusLedger, AssertionSupersession, AssertionSupersessionLedger, ConfidenceLedger,
    EPISTEMIC_CAPABILITY_VERSION, EvidenceLedger, EvidenceLink, EvidenceRole, EvidenceSourceKind,
    GraphObjectKind, ReasoningContentFormat, ReasoningKind, ReasoningLedger, ReasoningRecord,
    schema_registry,
};
use graphforge_provenance::{
    EventKind, LineageRecord, LineageRole, ProvenanceEvent, ProvenanceLedger, SubjectKind,
};
use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectStageOutcome, ResolvedProjectGeneration,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{CancellationToken, GraphForge, OperationId, PageRequest, PageToken, WriteContext};

/// One public graph UUID attached to an assertion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssertionGraphRefInput {
    /// Referenced node or edge UUID.
    pub graph_uuid: Uuid,
    /// Closed node/edge kind.
    pub graph_kind: GraphObjectKind,
    /// Closed subject/object/context role.
    pub role: AssertionGraphRole,
    /// Caller-significant contiguous position within the role.
    pub ordinal: u32,
}

/// Frozen request for one atomic assertion publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAssertionRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 assertion identity.
    pub assertion_uuid: Uuid,
    /// Exact claim text.
    pub claim: String,
    /// At least one graph UUID reference.
    pub graph_refs: Vec<AssertionGraphRefInput>,
}

/// Frozen filter and page request for assertions.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListAssertionsRequest {
    /// Optional graph UUID filter.
    pub graph_uuid: Option<Uuid>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Frozen confidence policy request.
#[derive(Clone, Debug, PartialEq)]
pub enum ConfidencePolicyRequest {
    /// Record the caller's explicit value.
    Explicit {
        /// Finite confidence in `[0, 1]`.
        value: f64,
    },
    /// Compute the minimum of requested immutable assessments.
    ConservativeMin {
        /// Requested input identities; normalized by UUID before evaluation.
        input_confidence_uuids: Vec<Uuid>,
    },
}

/// Frozen request for one atomic confidence publication.
#[derive(Clone, Debug, PartialEq)]
pub struct AssessConfidenceRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 confidence identity.
    pub confidence_uuid: Uuid,
    /// Existing immutable assertion being assessed.
    pub assertion_uuid: Uuid,
    /// Closed policy request.
    pub policy: ConfidencePolicyRequest,
}

/// Frozen filter and page request for confidence assessments.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListConfidenceAssessmentsRequest {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<Uuid>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Frozen request for one immutable evidence link.
#[derive(Clone, Debug, PartialEq)]
pub struct AttachEvidenceRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 evidence identity.
    pub evidence_uuid: Uuid,
    /// Existing immutable assertion.
    pub assertion_uuid: Uuid,
    /// Caller-managed source identity.
    pub source_uuid: Uuid,
    /// Closed source kind.
    pub source_kind: EvidenceSourceKind,
    /// Closed evidence role.
    pub role: EvidenceRole,
    /// Optional finite metadata weight in `[0, 1]`.
    pub weight: Option<f64>,
}

/// Frozen filter and page request for evidence links.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListEvidenceLinksRequest {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<Uuid>,
    /// Optional source UUID filter.
    pub source_uuid: Option<Uuid>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Frozen request for one immutable epistemic reasoning record or amendment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordReasoningRequest {
    /// Idempotency identity for the atomic generation publication.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 reasoning identity.
    pub reasoning_uuid: Uuid,
    /// Existing immutable knowledge assertion.
    pub assertion_uuid: Uuid,
    /// Closed reasoning purpose.
    pub kind: ReasoningKind,
    /// Closed exact-content encoding.
    pub content_format: ReasoningContentFormat,
    /// Exact UTF-8 content bytes.
    pub content: Vec<u8>,
    /// Optional prior reasoning record explicitly amended by this record.
    pub supersedes_reasoning_uuid: Option<Uuid>,
    /// Existing knowledge provenance event.
    pub provenance_uuid: Uuid,
}

/// Frozen filter and page request for reasoning history.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListReasoningRequest {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<Uuid>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Frozen request for one explicit append-only assertion-status event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordAssertionStatusRequest {
    /// Idempotency identity for atomic generation publication.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 event identity.
    pub status_event_uuid: Uuid,
    /// Existing immutable knowledge assertion.
    pub assertion_uuid: Uuid,
    /// Explicit non-supersession status.
    pub status: AssertionStatus,
    /// Optional existing immutable confidence assessment.
    pub confidence_uuid: Option<Uuid>,
    /// Optional existing immutable reasoning record.
    pub reasoning_uuid: Option<Uuid>,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
}

/// Frozen filter and page request for assertion-status history.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListAssertionStatusRequest {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<Uuid>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Atomic first-status input for a newly created assertion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FirstAssertionStatusInput {
    /// Caller-supplied UUIDv7 event identity.
    pub status_event_uuid: Uuid,
    /// Explicit non-supersession first status.
    pub status: AssertionStatus,
}

/// Frozen atomic assertion-plus-first-status request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAssertionWithStatusRequest {
    /// Complete assertion request; its operation UUID owns the publication.
    pub assertion: CreateAssertionRequest,
    /// Explicit first status stored in the separate epistemic participant.
    pub first_status: FirstAssertionStatusInput,
}

/// Frozen request for one atomic assertion supersession.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupersedeAssertionRequest {
    /// Idempotency identity for atomic generation publication.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 relation identity.
    pub supersession_uuid: Uuid,
    /// Existing assertion that becomes superseded.
    pub prior_assertion_uuid: Uuid,
    /// Existing replacement assertion.
    pub replacement_assertion_uuid: Uuid,
    /// Caller-supplied UUIDv7 paired status-event identity.
    pub status_event_uuid: Uuid,
    /// Existing reasoning record attached to the prior assertion.
    pub reasoning_uuid: Uuid,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
}

/// Frozen filter and page request for supersession history.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListAssertionSupersessionsRequest {
    /// Optional prior-assertion UUID filter.
    pub prior_assertion_uuid: Option<Uuid>,
    /// Optional replacement-assertion UUID filter.
    pub replacement_assertion_uuid: Option<Uuid>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// One evidence row in an atomic assertion bundle.
#[derive(Clone, Debug, PartialEq)]
pub struct EvidenceInput {
    /// Caller-supplied UUIDv7 evidence identity.
    pub evidence_uuid: Uuid,
    /// Caller-managed source identity.
    pub source_uuid: Uuid,
    /// Closed source kind.
    pub source_kind: EvidenceSourceKind,
    /// Closed evidence role.
    pub role: EvidenceRole,
    /// Optional finite metadata weight in `[0, 1]`.
    pub weight: Option<f64>,
}

/// Frozen atomic assertion-plus-evidence bundle.
#[derive(Clone, Debug, PartialEq)]
pub struct CreateAssertionWithEvidenceRequest {
    /// Complete assertion request; its operation UUID owns the bundle.
    pub assertion: CreateAssertionRequest,
    /// Non-empty immutable evidence set.
    pub evidence: Vec<EvidenceInput>,
}

impl CreateAssertionRequest {
    fn validate_context(&self) -> Result<(), GfError> {
        validate_write_context(&self.context)
    }
}

fn staged_assertion(
    request: &CreateAssertionRequest,
    recorded_at_micros: i64,
) -> Result<AssertionLedger, GfError> {
    let event = ProvenanceEvent::new(
        request.context.operation_uuid.0,
        EventKind::CreateAssertion,
        request.context.actor_uuid,
        recorded_at_micros,
    )
    .map_err(provenance_error)?;
    let assertion = Assertion::new(
        request.assertion_uuid,
        request.claim.clone(),
        event.provenance_uuid,
        recorded_at_micros,
    )
    .map_err(knowledge_error)?;
    let refs = request
        .graph_refs
        .iter()
        .map(|reference| {
            AssertionGraphRef::new(
                request.assertion_uuid,
                reference.graph_uuid,
                reference.graph_kind,
                reference.role,
                reference.ordinal,
            )
            .map_err(knowledge_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    AssertionLedger::new(vec![assertion], refs).map_err(knowledge_error)
}

fn validate_graph_refs(graph: &GraphForge, refs: &[AssertionGraphRefInput]) -> Result<(), GfError> {
    if refs.is_empty() {
        return Err(GfError::Validation(
            "assertion requires at least one graph reference".into(),
        ));
    }
    let mut node_ids = HashSet::new();
    let mut edge_ids = HashSet::new();
    for reference in refs {
        require_uuid(reference.graph_uuid, "graph_uuid")?;
        match reference.graph_kind {
            GraphObjectKind::Node => {
                node_ids.insert(reference.graph_uuid);
            }
            GraphObjectKind::Edge => {
                edge_ids.insert(reference.graph_uuid);
            }
        }
    }
    match_requested_node_uuids(graph, &mut node_ids)?;
    match_requested_edge_uuids(graph, &mut edge_ids)?;
    if node_ids.is_empty() && edge_ids.is_empty() {
        Ok(())
    } else {
        Err(GfError::Api {
            code: ApiErrorCode::NotFound,
            message: "assertion graph UUID was not found".into(),
        })
    }
}

pub(crate) fn lock_graph_visibility(
    graph: &GraphForge,
) -> Result<crate::write_modes::WritePermit<'_>, GfError> {
    graph.graph_visibility.lock()
}

fn match_requested_node_uuids(
    graph: &GraphForge,
    pending: &mut HashSet<Uuid>,
) -> Result<(), GfError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batches = graphforge_storage::read_nodes(&graph.dir())
        .map_err(|error| GfError::Storage(error.to_string()))?;
    match_requested_uuids(batches, "node_uuid", pending)
}

fn match_requested_edge_uuids(
    graph: &GraphForge,
    pending: &mut HashSet<Uuid>,
) -> Result<(), GfError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batches = graphforge_storage::read_edges_from_inventory(
        &graph.property_inventory_for_session(),
        "*",
        graph.ontology_mode,
    )
    .map_err(|error| GfError::Storage(error.to_string()))?;
    match_requested_uuids(batches, "edge_uuid", pending)
}

fn match_requested_uuids(
    batches: Vec<RecordBatch>,
    name: &'static str,
    pending: &mut HashSet<Uuid>,
) -> Result<(), GfError> {
    for batch in batches {
        match_requested_uuid_column(&batch, name, pending)?;
        if pending.is_empty() {
            break;
        }
    }
    Ok(())
}

fn match_requested_uuid_column(
    batch: &RecordBatch,
    name: &'static str,
    pending: &mut HashSet<Uuid>,
) -> Result<(), GfError> {
    let values = batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| GfError::Validation(format!("graph has malformed {name} data")))?;
    for row in 0..batch.num_rows() {
        if values.is_null(row) {
            return Err(GfError::Validation(format!("graph has null {name} data")));
        }
        let uuid = Uuid::from_slice(values.value(row))
            .map_err(|_| GfError::Validation(format!("graph has malformed {name} data")))?;
        pending.remove(&uuid);
        if pending.is_empty() {
            break;
        }
    }
    Ok(())
}

fn require_uuid(uuid: Uuid, name: &'static str) -> Result<(), GfError> {
    if uuid.is_nil() {
        Err(GfError::Validation(format!("{name} must not be nil")))
    } else {
        Ok(())
    }
}

fn validate_write_context(context: &WriteContext) -> Result<(), GfError> {
    require_uuid(context.operation_uuid.0, "operation_uuid")?;
    if let Some(actor_uuid) = context.actor_uuid {
        require_uuid(actor_uuid, "actor_uuid")?;
    }
    Ok(())
}

fn transaction_conflict(message: &'static str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::TransactionConflict,
        message: message.into(),
    }
}

fn not_found_kind(kind: &'static str) -> GfError {
    GfError::Api {
        code: ApiErrorCode::NotFound,
        message: format!("{kind} was not found"),
    }
}

fn not_found() -> GfError {
    GfError::Api {
        code: ApiErrorCode::NotFound,
        message: "assertion was not found".into(),
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "domain errors are consumed and converted once at the crate boundary"
)]
pub(crate) fn knowledge_error(error: graphforge_knowledge::KnowledgeError) -> GfError {
    let message = error.to_string();
    match error {
        graphforge_knowledge::KnowledgeError::Conflict(_)
        | graphforge_knowledge::KnowledgeError::TransactionConflict(_) => GfError::Project {
            code: ProjectErrorCode::TransactionConflict,
            message,
        },
        graphforge_knowledge::KnowledgeError::Limit { .. } => GfError::Api {
            code: ApiErrorCode::ResourceLimit,
            message,
        },
        graphforge_knowledge::KnowledgeError::Dangling(_) => GfError::Api {
            code: ApiErrorCode::NotFound,
            message,
        },
        graphforge_knowledge::KnowledgeError::Invalid { .. }
        | graphforge_knowledge::KnowledgeError::Duplicate(_)
        | graphforge_knowledge::KnowledgeError::Canonical(_) => GfError::Validation(message),
        graphforge_knowledge::KnowledgeError::Arrow(_) => GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message,
        },
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "domain errors are consumed and converted once at the crate boundary"
)]
pub(crate) fn provenance_error(error: graphforge_provenance::ProvenanceError) -> GfError {
    let message = error.to_string();
    match error {
        graphforge_provenance::ProvenanceError::Conflict(_) => GfError::Project {
            code: ProjectErrorCode::TransactionConflict,
            message,
        },
        graphforge_provenance::ProvenanceError::Limit { .. } => GfError::Api {
            code: ApiErrorCode::ResourceLimit,
            message,
        },
        graphforge_provenance::ProvenanceError::Invalid { .. }
        | graphforge_provenance::ProvenanceError::Duplicate(_)
        | graphforge_provenance::ProvenanceError::Dangling(_)
        | graphforge_provenance::ProvenanceError::Arrow(_) => GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message,
        },
        graphforge_provenance::ProvenanceError::Canonical(_) => GfError::Validation(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapabilityId, EnableCapabilityRequest};
    use std::collections::HashMap;

    pub(super) fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    #[test]
    fn domain_error_mapping_preserves_public_fault_domains() {
        use graphforge_knowledge::KnowledgeError;
        for (error, code) in [
            (
                KnowledgeError::Conflict("identity"),
                "GF_IDEMPOTENCY_CONFLICT",
            ),
            (
                KnowledgeError::TransactionConflict("transaction"),
                "GF_IDEMPOTENCY_CONFLICT",
            ),
            (
                KnowledgeError::Limit {
                    participant: "assertions",
                    observed: 2,
                    limit: 1,
                },
                "GF_RESOURCE_LIMIT",
            ),
            (KnowledgeError::Dangling("assertion"), "GF_NOT_FOUND"),
            (
                KnowledgeError::Invalid {
                    field: "claim",
                    message: "empty",
                },
                "GF_VALIDATION",
            ),
            (KnowledgeError::Duplicate("assertion_uuid"), "GF_VALIDATION"),
            (
                KnowledgeError::Canonical(graphforge_core::canonical::CanonicalError::Malformed(
                    "payload",
                )),
                "GF_VALIDATION",
            ),
            (
                KnowledgeError::Arrow(arrow::error::ArrowError::SchemaError("schema".into())),
                "GF_SCHEMA_MISMATCH",
            ),
        ] {
            assert_eq!(knowledge_error(error).code(), code);
        }

        use graphforge_provenance::ProvenanceError;
        for (error, code) in [
            (
                ProvenanceError::Conflict("identity"),
                "GF_IDEMPOTENCY_CONFLICT",
            ),
            (
                ProvenanceError::Limit {
                    participant: "events",
                    observed: 2,
                    limit: 1,
                },
                "GF_RESOURCE_LIMIT",
            ),
            (
                ProvenanceError::Invalid {
                    field: "event",
                    message: "invalid",
                },
                "GF_SCHEMA_MISMATCH",
            ),
            (
                ProvenanceError::Duplicate("event_uuid"),
                "GF_SCHEMA_MISMATCH",
            ),
            (
                ProvenanceError::Dangling("event_uuid"),
                "GF_SCHEMA_MISMATCH",
            ),
            (
                ProvenanceError::Arrow(arrow::error::ArrowError::SchemaError("schema".into())),
                "GF_SCHEMA_MISMATCH",
            ),
            (
                ProvenanceError::Canonical(graphforge_core::canonical::CanonicalError::Malformed(
                    "payload",
                )),
                "GF_VALIDATION",
            ),
        ] {
            assert_eq!(provenance_error(error).code(), code);
        }
        assert_eq!(
            transaction_conflict("changed").code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert_eq!(
            not_found_kind("evidence").to_string(),
            "GF_NOT_FOUND: evidence was not found"
        );
        assert_eq!(
            not_found().to_string(),
            "GF_NOT_FOUND: assertion was not found"
        );
    }

    #[test]
    fn knowledge_arrow_and_write_context_helpers_cover_empty_and_invalid_boundaries() {
        let schema = AssertionLedger::default()
            .assertion_batch()
            .unwrap()
            .schema();
        let empty = concat_or_empty(&[], &schema).unwrap();
        assert_eq!(empty.num_rows(), 0);
        let combined = concat_or_empty(&[empty.clone(), empty.clone()], &schema).unwrap();
        assert_eq!(combined.num_rows(), 0);
        let token = PageToken::new(uuid7(91), 4);
        let paged = with_next_token(&empty, Some(&token)).unwrap();
        assert_eq!(
            paged.schema().metadata()["graphforge.next_page_token"],
            token.as_str()
        );
        let unpaged = with_next_token(&empty, None).unwrap();
        assert!(
            !unpaged
                .schema()
                .metadata()
                .contains_key("graphforge.next_page_token")
        );
        let result = assertion_result(empty);
        assert_eq!(result.stats.rows_produced, 0);
        assert_eq!(result.batches.len(), 1);

        assert_eq!(
            require_uuid(Uuid::nil(), "record_uuid").unwrap_err().code(),
            "GF_VALIDATION"
        );
        let context = WriteContext {
            operation_uuid: OperationId(Uuid::nil()),
            actor_uuid: None,
        };
        assert_eq!(
            validate_write_context(&context).unwrap_err().code(),
            "GF_VALIDATION"
        );
        let context = WriteContext {
            operation_uuid: OperationId(uuid7(92)),
            actor_uuid: Some(Uuid::nil()),
        };
        assert_eq!(
            validate_write_context(&context).unwrap_err().code(),
            "GF_VALIDATION"
        );
        let context = WriteContext {
            operation_uuid: OperationId(uuid7(92)),
            actor_uuid: Some(uuid7(93)),
        };
        validate_write_context(&context).unwrap();
        assert_eq!(not_found().code(), "GF_NOT_FOUND");
    }

    pub(super) fn enable(graph: &GraphForge, capability_id: CapabilityId, seed: u8) {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(seed)),
                    actor_uuid: None,
                },
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }

    pub(super) fn assertion_fixture(
        graph: &GraphForge,
        assertion_uuid: Uuid,
        operation_seed: u8,
    ) -> Uuid {
        let node = graph.add_node("ReasoningSubject", &HashMap::new()).unwrap();
        let result = graph
            .create_assertion(CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(operation_seed)),
                    actor_uuid: None,
                },
                assertion_uuid,
                claim: "immutable claim".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap();
        let values = result.batches[0]
            .column_by_name("provenance_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        Uuid::from_slice(values.value(0)).unwrap()
    }

    pub(super) fn reasoning_fixture(
        graph: &GraphForge,
        assertion_uuid: Uuid,
        provenance_uuid: Uuid,
        reasoning_seed: u8,
        operation_seed: u8,
    ) -> Uuid {
        let reasoning_uuid = uuid7(reasoning_seed);
        graph
            .record_reasoning(RecordReasoningRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(operation_seed)),
                    actor_uuid: None,
                },
                reasoning_uuid,
                assertion_uuid,
                kind: ReasoningKind::DecisionRationale,
                content_format: ReasoningContentFormat::TextPlain,
                content: b"explicit replacement rationale".to_vec(),
                supersedes_reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap();
        reasoning_uuid
    }

    #[test]
    fn wave8_public_knowledge_bundle_and_reference_errors_are_exact() {
        let graph = GraphForge::new(None).unwrap();
        graph.set_clock_for_test(|| Ok(30));
        enable(&graph, CapabilityId::Provenance, 150);
        enable(&graph, CapabilityId::Knowledge, 151);
        enable(&graph, CapabilityId::Epistemic, 152);
        let node = graph.add_node("BoundarySubject", &HashMap::new()).unwrap();
        let assertion_uuid = uuid7(153);
        let request = CreateAssertionWithStatusRequest {
            assertion: CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(154)),
                    actor_uuid: None,
                },
                assertion_uuid,
                claim: "bundle boundary".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            },
            first_status: FirstAssertionStatusInput {
                status_event_uuid: uuid7(155),
                status: AssertionStatus::Hypothesis,
            },
        };
        graph.create_assertion_with_status(request.clone()).unwrap();
        let mut partial_replay = request.clone();
        partial_replay.first_status.status_event_uuid = uuid7(156);
        assert_eq!(
            graph
                .create_assertion_with_status(partial_replay)
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        let mut superseded = request;
        superseded.assertion.context.operation_uuid = OperationId(uuid7(157));
        superseded.assertion.assertion_uuid = uuid7(158);
        superseded.first_status.status_event_uuid = uuid7(159);
        superseded.first_status.status = AssertionStatus::Superseded;
        assert_eq!(
            graph
                .create_assertion_with_status(superseded)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );

        let provenance_uuid = graph.assertion(assertion_uuid, None).unwrap().batches[0]
            .column_by_name("provenance_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .and_then(|values| Uuid::from_slice(values.value(0)).ok())
            .unwrap();
        let reasoning = |reasoning_uuid, assertion_uuid, provenance_uuid, operation_uuid| {
            RecordReasoningRequest {
                context: WriteContext {
                    operation_uuid: OperationId(operation_uuid),
                    actor_uuid: None,
                },
                reasoning_uuid,
                assertion_uuid,
                kind: ReasoningKind::DecisionRationale,
                content_format: ReasoningContentFormat::TextPlain,
                content: b"boundary".to_vec(),
                supersedes_reasoning_uuid: None,
                provenance_uuid,
            }
        };
        assert_eq!(
            graph
                .record_reasoning(reasoning(
                    uuid7(160),
                    uuid7(161),
                    provenance_uuid,
                    uuid7(162)
                ))
                .unwrap_err()
                .code(),
            "GF_NOT_FOUND"
        );
        assert_eq!(
            graph
                .record_reasoning(reasoning(
                    uuid7(163),
                    assertion_uuid,
                    Uuid::from_u128(999),
                    uuid7(164)
                ))
                .unwrap_err()
                .code(),
            "GF_NOT_FOUND"
        );

        let replacement = uuid7(165);
        let replacement_provenance = assertion_fixture(&graph, replacement, 166);
        let rationale = reasoning_fixture(&graph, assertion_uuid, provenance_uuid, 167, 168);
        let base = SupersedeAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(169)),
                actor_uuid: None,
            },
            supersession_uuid: uuid7(170),
            prior_assertion_uuid: uuid7(171),
            replacement_assertion_uuid: replacement,
            status_event_uuid: uuid7(172),
            reasoning_uuid: rationale,
            provenance_uuid,
        };
        assert_eq!(
            graph.supersede_assertion(base).unwrap_err().code(),
            "GF_NOT_FOUND"
        );
        let missing_provenance = SupersedeAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(173)),
                actor_uuid: None,
            },
            supersession_uuid: uuid7(174),
            prior_assertion_uuid: assertion_uuid,
            replacement_assertion_uuid: replacement,
            status_event_uuid: uuid7(175),
            reasoning_uuid: rationale,
            provenance_uuid: Uuid::from_u128(999),
        };
        assert_eq!(
            graph
                .supersede_assertion(missing_provenance)
                .unwrap_err()
                .code(),
            "GF_NOT_FOUND"
        );
        assert_ne!(replacement_provenance, Uuid::nil());
    }

    #[test]
    fn stale_facade_rejects_each_knowledge_publication_without_partial_mutation() {
        let root = tempfile::tempdir().unwrap();
        let bootstrap = GraphForge::new(root.path().to_str()).unwrap();
        bootstrap.set_clock_for_test(|| Ok(100));
        enable(&bootstrap, CapabilityId::Provenance, 220);
        enable(&bootstrap, CapabilityId::Knowledge, 221);
        enable(&bootstrap, CapabilityId::Epistemic, 222);
        let assertion_uuid = uuid7(223);
        let provenance_uuid = assertion_fixture(&bootstrap, assertion_uuid, 224);
        drop(bootstrap);

        let stale = GraphForge::new(root.path().to_str()).unwrap();
        stale.set_clock_for_test(|| Ok(200));
        let concurrent = GraphForge::new(root.path().to_str()).unwrap();
        concurrent.add_node("Concurrent", &HashMap::new()).unwrap();
        let durable_generation = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();

        let confidence_uuid = uuid7(225);
        let confidence = stale
            .assess_confidence(AssessConfidenceRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(226)),
                    actor_uuid: None,
                },
                confidence_uuid,
                assertion_uuid,
                policy: ConfidencePolicyRequest::Explicit { value: 0.75 },
            })
            .unwrap_err();
        assert_eq!(confidence.code(), "GF_IDEMPOTENCY_CONFLICT");

        let reasoning_uuid = uuid7(227);
        let reasoning = stale
            .record_reasoning(RecordReasoningRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(228)),
                    actor_uuid: None,
                },
                reasoning_uuid,
                assertion_uuid,
                kind: ReasoningKind::DecisionRationale,
                content_format: ReasoningContentFormat::TextPlain,
                content: b"stale reasoning".to_vec(),
                supersedes_reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap_err();
        assert_eq!(reasoning.code(), "GF_IDEMPOTENCY_CONFLICT");

        let status = stale
            .record_assertion_status(RecordAssertionStatusRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(229)),
                    actor_uuid: None,
                },
                status_event_uuid: uuid7(230),
                assertion_uuid,
                status: AssertionStatus::Hypothesis,
                confidence_uuid: None,
                reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap_err();
        assert_eq!(status.code(), "GF_IDEMPOTENCY_CONFLICT");

        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            durable_generation
        );
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(
            reopened
                .confidence_assessment(confidence_uuid, None)
                .unwrap_err()
                .code(),
            "GF_NOT_FOUND"
        );
        assert_eq!(
            reopened.reasoning(reasoning_uuid, None).unwrap_err().code(),
            "GF_NOT_FOUND"
        );
        assert_eq!(
            reopened
                .assertion_status(assertion_uuid)
                .unwrap()
                .stats
                .rows_produced,
            0
        );
    }
}
