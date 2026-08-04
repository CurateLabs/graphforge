//! `graphforge-api` orchestration for immutable UUID-referenced assertions.

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

/// Frozen request for one immutable M21 reasoning record or amendment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordReasoningRequest {
    /// Idempotency identity for the atomic generation publication.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 reasoning identity.
    pub reasoning_uuid: Uuid,
    /// Existing immutable M20 assertion.
    pub assertion_uuid: Uuid,
    /// Closed reasoning purpose.
    pub kind: ReasoningKind,
    /// Closed exact-content encoding.
    pub content_format: ReasoningContentFormat,
    /// Exact UTF-8 content bytes.
    pub content: Vec<u8>,
    /// Optional prior reasoning record explicitly amended by this record.
    pub supersedes_reasoning_uuid: Option<Uuid>,
    /// Existing M20 provenance event.
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
    /// Existing immutable M20 assertion.
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
    /// Explicit first status stored in the separate M21 participant.
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

impl GraphForge {
    /// Atomically create one assertion, its graph references, and provenance.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn create_assertion(
        &self,
        request: CreateAssertionRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        request.validate_context()?;
        let _graph_visibility = lock_graph_visibility(self)?;
        validate_graph_refs(self, &request.graph_refs)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("knowledge", 1)?;
        parent.require_capability("provenance", 1)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "project generation changed before assertion publication".into(),
            });
        }

        let existing = read_ledger(&parent)?;
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged = staged_assertion(&request, recorded_at_micros)?;
        if let Some(index) = existing
            .assertions
            .iter()
            .position(|row| row.assertion_uuid == request.assertion_uuid)
        {
            if existing
                .assertion_fingerprint(request.assertion_uuid)
                .map_err(knowledge_error)?
                == staged
                    .assertion_fingerprint(request.assertion_uuid)
                    .map_err(knowledge_error)?
            {
                return Ok(assertion_result(
                    existing
                        .assertion_batch()
                        .map_err(knowledge_error)?
                        .slice(index, 1),
                ));
            }
            return Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "assertion UUID was reused for different canonical content".into(),
            });
        }

        let knowledge = existing.merge(&staged).map_err(knowledge_error)?;
        let provenance = merged_provenance(&parent, &request, &staged, recorded_at_micros)?;
        let participants = assertion_publication_participants(&parent, &knowledge, &provenance)?;
        let capabilities = parent
            .capabilities()
            .into_iter()
            .map(|entry| ProjectCapability {
                capability_id: entry.capability_id,
                capability_version: entry.capability_version,
            })
            .collect();
        let generation_uuid =
            assertion_generation_uuid(request.context.operation_uuid, &participants);
        let publication = ProjectGenerationRequest {
            transaction_uuid: request.context.operation_uuid.0,
            generation_uuid,
            capabilities,
            participants,
        };
        let receipt = match graphforge_storage::stage_project_generation(root, &publication)? {
            ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
            ProjectStageOutcome::Staged(staged_generation) => staged_generation
                .validate(
                    |_| Ok(()),
                    |actual_parent, _| {
                        if actual_parent.generation_uuid() != expected_parent {
                            return Err(GfError::Project {
                                code: ProjectErrorCode::TransactionConflict,
                                message: "project generation changed before assertion publication"
                                    .into(),
                            });
                        }
                        Ok(())
                    },
                )?
                .publish()?,
        };
        *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") = receipt.generation_uuid;
        let committed = graphforge_storage::resolve_project_generation(root)?;
        let ledger = read_ledger(&committed)?;
        let index = ledger
            .assertions
            .iter()
            .position(|row| row.assertion_uuid == request.assertion_uuid)
            .ok_or_else(|| GfError::Validation("committed assertion is absent".into()))?;
        Ok(assertion_result(
            ledger
                .assertion_batch()
                .map_err(knowledge_error)?
                .slice(index, 1),
        ))
    }

    /// Atomically create an M20 assertion and its first explicit M21 status.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m21-api/1 freezes owned request structs"
    )]
    pub fn create_assertion_with_status(
        &self,
        request: CreateAssertionWithStatusRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        request.assertion.validate_context()?;
        require_uuid(request.first_status.status_event_uuid, "status_event_uuid")?;
        if request.first_status.status == AssertionStatus::Superseded {
            return Err(GfError::Validation(
                "superseded status requires the atomic supersession API".into(),
            ));
        }
        let _graph_visibility = lock_graph_visibility(self)?;
        validate_graph_refs(self, &request.assertion.graph_refs)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("knowledge", 1)?;
        parent.require_capability("provenance", 1)?;
        parent.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(transaction_conflict(
                "project generation changed before assertion-status bundle publication",
            ));
        }
        let assertions = read_ledger(&parent)?;
        let statuses = read_status_ledger(&parent)?;
        let existing_assertion = assertions
            .assertions
            .iter()
            .find(|row| row.assertion_uuid == request.assertion.assertion_uuid);
        let existing_status = statuses
            .events
            .iter()
            .find(|row| row.status_event_uuid == request.first_status.status_event_uuid);
        match (existing_assertion, existing_status) {
            (Some(assertion), Some(status))
                if assertion.claim == request.assertion.claim
                    && assertion_refs_match(&assertions, &request.assertion)
                    && status.assertion_uuid == request.assertion.assertion_uuid
                    && status.status == request.first_status.status
                    && status.confidence_uuid.is_none()
                    && status.reasoning_uuid.is_none()
                    && status.provenance_uuid == assertion.provenance_uuid =>
            {
                let index = statuses
                    .events
                    .iter()
                    .position(|row| row.status_event_uuid == request.first_status.status_event_uuid)
                    .expect("matched status belongs to ledger");
                return Ok(assertion_result(
                    statuses.batch().map_err(knowledge_error)?.slice(index, 1),
                ));
            }
            (None, None) => {}
            _ => {
                return Err(transaction_conflict(
                    "assertion-status bundle identity was reused for different canonical content",
                ));
            }
        }
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged_assertions = staged_assertion(&request.assertion, recorded_at_micros)?;
        let provenance_uuid = staged_assertions.assertions[0].provenance_uuid;
        let staged_status = AssertionStatusLedger::new(vec![
            AssertionStatusEvent::new(
                request.first_status.status_event_uuid,
                request.assertion.assertion_uuid,
                request.first_status.status,
                None,
                None,
                provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        let merged_assertions = assertions
            .merge(&staged_assertions)
            .map_err(knowledge_error)?;
        let merged_status = statuses.merge(&staged_status).map_err(knowledge_error)?;
        let provenance = merged_provenance(
            &parent,
            &request.assertion,
            &staged_assertions,
            recorded_at_micros,
        )?;
        publish_assertion_status_bundle(
            self,
            &request,
            &parent,
            expected_parent,
            &merged_assertions,
            &merged_status,
            &provenance,
        )
    }

    /// Return one exact `assertion@1` row.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes an owned optional cancellation token"
    )]
    pub fn assertion(
        &self,
        assertion_uuid: Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(assertion_uuid, "assertion_uuid")?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        let index = ledger
            .assertions
            .iter()
            .position(|row| row.assertion_uuid == assertion_uuid)
            .ok_or_else(not_found)?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        Ok(assertion_result(
            ledger
                .assertion_batch()
                .map_err(knowledge_error)?
                .slice(index, 1),
        ))
    }

    /// Return one deterministic assertion page.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn list_assertions(
        &self,
        request: ListAssertionsRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(graph_uuid) = request.graph_uuid {
            require_uuid(graph_uuid, "graph_uuid")?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        let selected = ledger
            .assertions
            .iter()
            .enumerate()
            .filter(|(_, assertion)| {
                request.graph_uuid.is_none_or(|graph_uuid| {
                    ledger.graph_refs.iter().any(|reference| {
                        reference.assertion_uuid == assertion.assertion_uuid
                            && reference.graph_uuid == graph_uuid
                    })
                })
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.assertion_batch().map_err(knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &graphforge_knowledge::ASSERTION_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Return one assertion's graph references in canonical order.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned page requests"
    )]
    pub fn assertion_graph_refs(
        &self,
        assertion_uuid: Uuid,
        page: PageRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(assertion_uuid, "assertion_uuid")?;
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        if !ledger
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == assertion_uuid)
        {
            return Err(not_found());
        }
        let source = ledger.graph_ref_batch().map_err(knowledge_error)?;
        let selected = ledger
            .graph_refs
            .iter()
            .enumerate()
            .filter(|(_, row)| row.assertion_uuid == assertion_uuid)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) =
            crate::paging::validate_page(&page, generation.generation_uuid(), selected.len())?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &graphforge_knowledge::ASSERTION_GRAPH_REF_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Atomically record one confidence assessment, its input snapshot, and provenance.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn assess_confidence(
        &self,
        request: AssessConfidenceRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        require_uuid(request.confidence_uuid, "confidence_uuid")?;
        require_uuid(request.assertion_uuid, "assertion_uuid")?;
        let _graph_visibility = lock_graph_visibility(self)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("knowledge", 1)?;
        parent.require_capability("provenance", 1)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(transaction_conflict(
                "project generation changed before confidence publication",
            ));
        }
        let assertions = read_ledger(&parent)?;
        if !assertions
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == request.assertion_uuid)
        {
            return Err(not_found_kind("assertion"));
        }
        let existing = read_confidence_ledger(&parent)?;
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let event = ProvenanceEvent::new(
            request.context.operation_uuid.0,
            EventKind::AssessConfidence,
            request.context.actor_uuid,
            recorded_at_micros,
        )
        .map_err(provenance_error)?;
        let staged = match &request.policy {
            ConfidencePolicyRequest::Explicit { value } => ConfidenceLedger::explicit(
                request.confidence_uuid,
                request.assertion_uuid,
                *value,
                event.provenance_uuid,
                recorded_at_micros,
            ),
            ConfidencePolicyRequest::ConservativeMin {
                input_confidence_uuids,
            } => existing.conservative_min(
                request.confidence_uuid,
                request.assertion_uuid,
                input_confidence_uuids.clone(),
                event.provenance_uuid,
                recorded_at_micros,
            ),
        }
        .map_err(knowledge_error)?;
        if let Some(index) = existing
            .assessments
            .iter()
            .position(|row| row.confidence_uuid == request.confidence_uuid)
        {
            if existing
                .assessment_fingerprint(request.confidence_uuid)
                .map_err(knowledge_error)?
                == staged
                    .assessment_fingerprint(request.confidence_uuid)
                    .map_err(knowledge_error)?
            {
                return Ok(assertion_result(
                    existing
                        .assessment_batch()
                        .map_err(knowledge_error)?
                        .slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "confidence UUID was reused for different canonical content",
            ));
        }

        let knowledge = existing.merge(&staged).map_err(knowledge_error)?;
        let provenance = merged_confidence_provenance(&parent, &request, &staged, &event)?;
        publish_confidence(
            self,
            &request,
            &parent,
            expected_parent,
            &knowledge,
            &provenance,
        )
    }

    /// Return one exact `confidence_assessment@1` row.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes an owned optional cancellation token"
    )]
    pub fn confidence_assessment(
        &self,
        confidence_uuid: Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(confidence_uuid, "confidence_uuid")?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_confidence_ledger(&generation)?;
        let index = ledger
            .assessments
            .iter()
            .position(|row| row.confidence_uuid == confidence_uuid)
            .ok_or_else(|| not_found_kind("confidence assessment"))?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        Ok(assertion_result(
            ledger
                .assessment_batch()
                .map_err(knowledge_error)?
                .slice(index, 1),
        ))
    }

    /// Return a deterministic page of confidence assessments.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn list_confidence_assessments(
        &self,
        request: ListConfidenceAssessmentsRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(assertion_uuid) = request.assertion_uuid {
            require_uuid(assertion_uuid, "assertion_uuid")?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_confidence_ledger(&generation)?;
        let selected = ledger
            .assessments
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .assertion_uuid
                    .is_none_or(|assertion_uuid| row.assertion_uuid == assertion_uuid)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.assessment_batch().map_err(knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &graphforge_knowledge::CONFIDENCE_ASSESSMENT_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Return one assessment's immutable normalized input snapshot.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned page requests"
    )]
    pub fn confidence_inputs(
        &self,
        confidence_uuid: Uuid,
        page: PageRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(confidence_uuid, "confidence_uuid")?;
        let generation = self.generation_for_read()?;
        let ledger = read_confidence_ledger(&generation)?;
        if !ledger
            .assessments
            .iter()
            .any(|row| row.confidence_uuid == confidence_uuid)
        {
            return Err(not_found_kind("confidence assessment"));
        }
        let source = ledger.input_batch().map_err(knowledge_error)?;
        let selected = ledger
            .inputs
            .iter()
            .enumerate()
            .filter(|(_, row)| row.confidence_uuid == confidence_uuid)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) =
            crate::paging::validate_page(&page, generation.generation_uuid(), selected.len())?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &graphforge_knowledge::CONFIDENCE_INPUT_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Atomically create one assertion together with a non-empty evidence bundle.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn create_assertion_with_evidence(
        &self,
        request: CreateAssertionWithEvidenceRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        request.assertion.validate_context()?;
        if request.evidence.is_empty() {
            return Err(GfError::Validation(
                "assertion evidence bundle must not be empty".into(),
            ));
        }
        let _graph_visibility = lock_graph_visibility(self)?;
        validate_graph_refs(self, &request.assertion.graph_refs)?;
        for input in &request.evidence {
            require_uuid(input.evidence_uuid, "evidence_uuid")?;
            require_uuid(input.source_uuid, "source_uuid")?;
            validate_evidence_source(self, input.source_uuid, input.source_kind)?;
        }
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("knowledge", 1)?;
        parent.require_capability("provenance", 1)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(transaction_conflict(
                "project generation changed before assertion evidence publication",
            ));
        }
        let assertions = read_ledger(&parent)?;
        let evidence = read_evidence_ledger(&parent)?;
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged_assertions = staged_assertion(&request.assertion, recorded_at_micros)?;
        let event = ProvenanceEvent::new(
            request.assertion.context.operation_uuid.0,
            EventKind::CreateAssertion,
            request.assertion.context.actor_uuid,
            recorded_at_micros,
        )
        .map_err(provenance_error)?;
        let staged_evidence =
            staged_evidence_bundle(&request, event.provenance_uuid, recorded_at_micros)?;
        if let Some(index) = assertions
            .assertions
            .iter()
            .position(|row| row.assertion_uuid == request.assertion.assertion_uuid)
        {
            let assertion_same = assertions
                .assertion_fingerprint(request.assertion.assertion_uuid)
                .map_err(knowledge_error)?
                == staged_assertions
                    .assertion_fingerprint(request.assertion.assertion_uuid)
                    .map_err(knowledge_error)?;
            let evidence_same = staged_evidence.links.iter().all(|row| {
                evidence
                    .evidence_fingerprint(row.evidence_uuid)
                    .and_then(|existing| {
                        staged_evidence
                            .evidence_fingerprint(row.evidence_uuid)
                            .map(|staged| existing == staged)
                    })
                    .unwrap_or(false)
            });
            if assertion_same && evidence_same {
                return Ok(assertion_result(
                    assertions
                        .assertion_batch()
                        .map_err(knowledge_error)?
                        .slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "assertion evidence bundle identity was reused for different canonical content",
            ));
        }
        let merged_assertions = assertions
            .merge(&staged_assertions)
            .map_err(knowledge_error)?;
        let merged_evidence = evidence.merge(&staged_evidence).map_err(knowledge_error)?;
        let provenance =
            merged_assertion_evidence_provenance(&parent, &request, &staged_assertions, &event)?;
        publish_assertion_evidence(
            self,
            &request,
            &parent,
            expected_parent,
            &merged_assertions,
            &merged_evidence,
            &provenance,
        )
    }

    /// Atomically attach one immutable evidence link and its provenance.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn attach_evidence(
        &self,
        request: AttachEvidenceRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        require_uuid(request.evidence_uuid, "evidence_uuid")?;
        require_uuid(request.assertion_uuid, "assertion_uuid")?;
        require_uuid(request.source_uuid, "source_uuid")?;
        let _graph_visibility = lock_graph_visibility(self)?;
        validate_evidence_source(self, request.source_uuid, request.source_kind)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("knowledge", 1)?;
        parent.require_capability("provenance", 1)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(transaction_conflict(
                "project generation changed before evidence publication",
            ));
        }
        if !read_ledger(&parent)?
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == request.assertion_uuid)
        {
            return Err(not_found_kind("assertion"));
        }
        let existing = read_evidence_ledger(&parent)?;
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let event = ProvenanceEvent::new(
            request.context.operation_uuid.0,
            EventKind::RecordEvidence,
            request.context.actor_uuid,
            recorded_at_micros,
        )
        .map_err(provenance_error)?;
        let staged = EvidenceLedger::new(vec![
            EvidenceLink::new(
                request.evidence_uuid,
                request.assertion_uuid,
                request.source_uuid,
                request.source_kind,
                request.role,
                request.weight,
                event.provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        if let Some(index) = existing
            .links
            .iter()
            .position(|row| row.evidence_uuid == request.evidence_uuid)
        {
            if existing
                .evidence_fingerprint(request.evidence_uuid)
                .map_err(knowledge_error)?
                == staged
                    .evidence_fingerprint(request.evidence_uuid)
                    .map_err(knowledge_error)?
            {
                return Ok(assertion_result(
                    existing.batch().map_err(knowledge_error)?.slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "evidence UUID was reused for different canonical content",
            ));
        }
        let knowledge = existing.merge(&staged).map_err(knowledge_error)?;
        let provenance = merged_evidence_provenance(&parent, &request, &event)?;
        publish_evidence(
            self,
            &request,
            &parent,
            expected_parent,
            &knowledge,
            &provenance,
        )
    }

    /// Return one exact `evidence_link@1` row.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes an owned optional cancellation token"
    )]
    pub fn evidence_link(
        &self,
        evidence_uuid: Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(evidence_uuid, "evidence_uuid")?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_evidence_ledger(&generation)?;
        let index = ledger
            .links
            .iter()
            .position(|row| row.evidence_uuid == evidence_uuid)
            .ok_or_else(|| not_found_kind("evidence link"))?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        Ok(assertion_result(
            ledger.batch().map_err(knowledge_error)?.slice(index, 1),
        ))
    }

    /// Return a deterministic page of immutable evidence links.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn list_evidence_links(
        &self,
        request: ListEvidenceLinksRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(assertion_uuid) = request.assertion_uuid {
            require_uuid(assertion_uuid, "assertion_uuid")?;
        }
        if let Some(source_uuid) = request.source_uuid {
            require_uuid(source_uuid, "source_uuid")?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_evidence_ledger(&generation)?;
        let selected = ledger
            .links
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .assertion_uuid
                    .is_none_or(|id| row.assertion_uuid == id)
                    && request.source_uuid.is_none_or(|id| row.source_uuid == id)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.batch().map_err(knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &graphforge_knowledge::EVIDENCE_LINK_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Atomically append one immutable reasoning record.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m21-api/1 freezes owned request structs"
    )]
    pub fn record_reasoning(
        &self,
        request: RecordReasoningRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        require_uuid(request.reasoning_uuid, "reasoning_uuid")?;
        require_uuid(request.assertion_uuid, "assertion_uuid")?;
        require_uuid(request.provenance_uuid, "provenance_uuid")?;
        if let Some(previous) = request.supersedes_reasoning_uuid {
            require_uuid(previous, "supersedes_reasoning_uuid")?;
        }
        let _graph_visibility = lock_graph_visibility(self)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(transaction_conflict(
                "project generation changed before reasoning publication",
            ));
        }
        if !read_ledger(&parent)?
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == request.assertion_uuid)
        {
            return Err(not_found_kind("assertion"));
        }
        if !crate::provenance::read_ledger(&parent)?
            .events
            .iter()
            .any(|row| row.provenance_uuid == request.provenance_uuid)
        {
            return Err(not_found_kind("provenance event"));
        }
        let existing = read_reasoning_ledger(&parent)?;
        if let Some(index) = existing
            .records
            .iter()
            .position(|row| row.reasoning_uuid == request.reasoning_uuid)
        {
            let row = &existing.records[index];
            if row.assertion_uuid == request.assertion_uuid
                && row.kind == request.kind
                && row.content_format == request.content_format
                && row.content == request.content
                && row.supersedes_reasoning_uuid == request.supersedes_reasoning_uuid
                && row.provenance_uuid == request.provenance_uuid
            {
                return Ok(assertion_result(
                    existing.batch().map_err(knowledge_error)?.slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "reasoning UUID was reused for different canonical content",
            ));
        }
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let record = ReasoningRecord::new(
            request.reasoning_uuid,
            request.assertion_uuid,
            request.kind,
            request.content_format,
            request.content.clone(),
            request.supersedes_reasoning_uuid,
            request.provenance_uuid,
            recorded_at_micros,
        )
        .map_err(knowledge_error)?;
        let mut records = existing.records;
        records.push(record);
        let merged = ReasoningLedger::new(records).map_err(knowledge_error)?;
        publish_reasoning(self, &request, &parent, expected_parent, &merged)
    }

    /// Return one exact immutable reasoning record.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m21-api/1 freezes an owned optional cancellation token"
    )]
    pub fn reasoning(
        &self,
        reasoning_uuid: Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(reasoning_uuid, "reasoning_uuid")?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_reasoning_ledger(&generation)?;
        let index = ledger
            .records
            .iter()
            .position(|row| row.reasoning_uuid == reasoning_uuid)
            .ok_or_else(|| not_found_kind("reasoning record"))?;
        Ok(assertion_result(
            ledger.batch().map_err(knowledge_error)?.slice(index, 1),
        ))
    }

    /// Return deterministic immutable reasoning history.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m21-api/1 freezes owned request structs"
    )]
    pub fn list_reasoning(
        &self,
        request: ListReasoningRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(assertion_uuid) = request.assertion_uuid {
            require_uuid(assertion_uuid, "assertion_uuid")?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_reasoning_ledger(&generation)?;
        let selected = ledger
            .records
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .assertion_uuid
                    .is_none_or(|id| row.assertion_uuid == id)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.batch().map_err(knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &graphforge_knowledge::REASONING_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Atomically append one explicit assertion-status event.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m21-api/1 freezes owned request structs"
    )]
    pub fn record_assertion_status(
        &self,
        request: RecordAssertionStatusRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        validate_status_request(&request)?;
        if request.status == AssertionStatus::Superseded {
            return Err(GfError::Validation(
                "superseded status requires the atomic supersession API".into(),
            ));
        }
        let _graph_visibility = lock_graph_visibility(self)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(transaction_conflict(
                "project generation changed before assertion-status publication",
            ));
        }
        validate_status_references(
            &parent,
            request.assertion_uuid,
            request.confidence_uuid,
            request.reasoning_uuid,
            request.provenance_uuid,
        )?;
        let existing = read_status_ledger(&parent)?;
        if let Some(index) = existing
            .events
            .iter()
            .position(|row| row.status_event_uuid == request.status_event_uuid)
        {
            let row = &existing.events[index];
            if row.assertion_uuid == request.assertion_uuid
                && row.status == request.status
                && row.confidence_uuid == request.confidence_uuid
                && row.reasoning_uuid == request.reasoning_uuid
                && row.provenance_uuid == request.provenance_uuid
            {
                return Ok(assertion_result(
                    existing.batch().map_err(knowledge_error)?.slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "status event UUID was reused for different canonical content",
            ));
        }
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged = AssertionStatusLedger::new(vec![
            AssertionStatusEvent::new(
                request.status_event_uuid,
                request.assertion_uuid,
                request.status,
                request.confidence_uuid,
                request.reasoning_uuid,
                request.provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        let merged = existing.merge(&staged).map_err(knowledge_error)?;
        publish_status(self, &request, &parent, expected_parent, &merged)
    }

    /// Return the deterministic current status, or an empty Arrow table when statusless.
    pub fn assertion_status(
        &self,
        assertion_uuid: Uuid,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(assertion_uuid, "assertion_uuid")?;
        let generation = self.generation_for_read()?;
        if !read_ledger(&generation)?
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == assertion_uuid)
        {
            return Err(not_found_kind("assertion"));
        }
        let ledger = read_status_ledger(&generation)?;
        let batch = ledger.batch().map_err(knowledge_error)?;
        let current = ledger.current_for(assertion_uuid).map_or_else(
            || RecordBatch::new_empty(Arc::clone(&ASSERTION_STATUS_SCHEMA)),
            |event| {
                let index = ledger
                    .events
                    .iter()
                    .position(|row| row.status_event_uuid == event.status_event_uuid)
                    .expect("current status belongs to ledger");
                batch.slice(index, 1)
            },
        );
        Ok(assertion_result(current))
    }

    /// Return deterministic append-only assertion-status history.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m21-api/1 freezes owned request structs"
    )]
    pub fn list_assertion_status(
        &self,
        request: ListAssertionStatusRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(assertion_uuid) = request.assertion_uuid {
            require_uuid(assertion_uuid, "assertion_uuid")?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_status_ledger(&generation)?;
        let selected = ledger
            .events
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .assertion_uuid
                    .is_none_or(|id| row.assertion_uuid == id)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.batch().map_err(knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &ASSERTION_STATUS_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Atomically append a supersession relation and its exact terminal status event.
    #[allow(
        clippy::needless_pass_by_value,
        clippy::too_many_lines,
        reason = "graphforge-m21-api/1 freezes one explicit atomic validation transaction"
    )]
    pub fn supersede_assertion(
        &self,
        request: SupersedeAssertionRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        for (uuid, name) in [
            (request.supersession_uuid, "supersession_uuid"),
            (request.prior_assertion_uuid, "prior_assertion_uuid"),
            (
                request.replacement_assertion_uuid,
                "replacement_assertion_uuid",
            ),
            (request.status_event_uuid, "status_event_uuid"),
            (request.reasoning_uuid, "reasoning_uuid"),
            (request.provenance_uuid, "provenance_uuid"),
        ] {
            require_uuid(uuid, name)?;
        }
        let _graph_visibility = lock_graph_visibility(self)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("knowledge", 1)?;
        parent.require_capability("provenance", 1)?;
        parent.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(transaction_conflict(
                "project generation changed before assertion-supersession publication",
            ));
        }

        let assertions = read_ledger(&parent)?;
        for assertion_uuid in [
            request.prior_assertion_uuid,
            request.replacement_assertion_uuid,
        ] {
            if !assertions
                .assertions
                .iter()
                .any(|row| row.assertion_uuid == assertion_uuid)
            {
                return Err(not_found_kind("assertion"));
            }
        }
        let reasoning = read_reasoning_ledger(&parent)?;
        if !reasoning.records.iter().any(|row| {
            row.reasoning_uuid == request.reasoning_uuid
                && row.assertion_uuid == request.prior_assertion_uuid
        }) {
            return Err(not_found_kind("reasoning for prior assertion"));
        }
        if !crate::provenance::read_ledger(&parent)?
            .events
            .iter()
            .any(|row| row.provenance_uuid == request.provenance_uuid)
        {
            return Err(not_found_kind("provenance"));
        }

        let existing_relations = read_supersession_ledger(&parent)?;
        let existing_statuses = read_status_ledger(&parent)?;
        if let Some(index) = existing_relations
            .relations()
            .iter()
            .position(|row| row.supersession_uuid == request.supersession_uuid)
        {
            let row = &existing_relations.relations()[index];
            if row.prior_assertion_uuid == request.prior_assertion_uuid
                && row.replacement_assertion_uuid == request.replacement_assertion_uuid
                && row.status_event_uuid == request.status_event_uuid
                && row.reasoning_uuid == request.reasoning_uuid
                && row.provenance_uuid == request.provenance_uuid
                && existing_statuses.events.iter().any(|status| {
                    status.status_event_uuid == request.status_event_uuid
                        && status.assertion_uuid == request.prior_assertion_uuid
                        && status.status == AssertionStatus::Superseded
                        && status.reasoning_uuid == Some(request.reasoning_uuid)
                        && status.provenance_uuid == request.provenance_uuid
                })
            {
                return Ok(assertion_result(
                    existing_relations
                        .batch()
                        .map_err(knowledge_error)?
                        .slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "supersession identity was reused for different canonical content",
            ));
        }
        if existing_statuses
            .events
            .iter()
            .any(|row| row.status_event_uuid == request.status_event_uuid)
        {
            return Err(transaction_conflict(
                "status event UUID was reused outside the supersession relation",
            ));
        }

        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged_relations = AssertionSupersessionLedger::new(vec![
            AssertionSupersession::new(
                request.supersession_uuid,
                request.prior_assertion_uuid,
                request.replacement_assertion_uuid,
                request.status_event_uuid,
                request.reasoning_uuid,
                request.provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        let staged_statuses = AssertionStatusLedger::new(vec![
            AssertionStatusEvent::new(
                request.status_event_uuid,
                request.prior_assertion_uuid,
                AssertionStatus::Superseded,
                None,
                Some(request.reasoning_uuid),
                request.provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        let relations = existing_relations
            .merge(&staged_relations)
            .map_err(knowledge_error)?;
        let statuses = existing_statuses
            .merge(&staged_statuses)
            .map_err(knowledge_error)?;
        publish_supersession(
            self,
            &request,
            &parent,
            expected_parent,
            &relations,
            &statuses,
        )
    }

    /// Return deterministic branch-preserving supersession history.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m21-api/1 freezes owned request structs"
    )]
    pub fn list_assertion_supersessions(
        &self,
        request: ListAssertionSupersessionsRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        for (uuid, name) in [
            (request.prior_assertion_uuid, "prior_assertion_uuid"),
            (
                request.replacement_assertion_uuid,
                "replacement_assertion_uuid",
            ),
        ] {
            if let Some(uuid) = uuid {
                require_uuid(uuid, name)?;
            }
        }
        let generation = self.generation_for_read()?;
        let ledger = read_supersession_ledger(&generation)?;
        let selected = ledger
            .relations()
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .prior_assertion_uuid
                    .is_none_or(|id| row.prior_assertion_uuid == id)
                    && request
                        .replacement_assertion_uuid
                        .is_none_or(|id| row.replacement_assertion_uuid == id)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.batch().map_err(knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &ASSERTION_SUPERSESSION_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }
}

fn publish_status(
    graph: &GraphForge,
    request: &RecordAssertionStatusRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    status: &AssertionStatusLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants = status_publication_participants(parent, status)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.context.operation_uuid.0,
        generation_uuid: knowledge_generation_uuid(
            b"assertion-status",
            request.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graphforge_storage::stage_project_generation(root, &publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => staged
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before assertion-status publication",
                        ));
                    }
                    Ok(())
                },
            )?
            .publish()?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    committed_status_event(root, request.status_event_uuid)
}

fn publish_supersession(
    graph: &GraphForge,
    request: &SupersedeAssertionRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    relations: &AssertionSupersessionLedger,
    statuses: &AssertionStatusLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants = supersession_publication_participants(parent, relations, statuses)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.context.operation_uuid.0,
        generation_uuid: knowledge_generation_uuid(
            b"assertion-supersession",
            request.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graphforge_storage::stage_project_generation(root, &publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => staged
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before supersession publication",
                        ));
                    }
                    Ok(())
                },
            )?
            .publish()?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    let committed = graphforge_storage::resolve_project_generation(root)?;
    let ledger = read_supersession_ledger(&committed)?;
    let index = ledger
        .relations()
        .iter()
        .position(|row| row.supersession_uuid == request.supersession_uuid)
        .ok_or_else(|| GfError::Validation("committed supersession is absent".into()))?;
    Ok(assertion_result(
        ledger.batch().map_err(knowledge_error)?.slice(index, 1),
    ))
}

fn publish_assertion_status_bundle(
    graph: &GraphForge,
    request: &CreateAssertionWithStatusRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    assertions: &AssertionLedger,
    status: &AssertionStatusLedger,
    provenance: &ProvenanceLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants =
        assertion_status_bundle_participants(parent, assertions, status, provenance)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.assertion.context.operation_uuid.0,
        generation_uuid: knowledge_generation_uuid(
            b"assertion-status-bundle",
            request.assertion.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graphforge_storage::stage_project_generation(root, &publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => staged
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before assertion-status bundle publication",
                        ));
                    }
                    Ok(())
                },
            )?
            .publish()?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    committed_status_event(root, request.first_status.status_event_uuid)
}

fn committed_status_event(
    root: &Path,
    status_event_uuid: Uuid,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let committed = graphforge_storage::resolve_project_generation(root)?;
    let ledger = read_status_ledger(&committed)?;
    let index = ledger
        .events
        .iter()
        .position(|row| row.status_event_uuid == status_event_uuid)
        .ok_or_else(|| GfError::Validation("committed status event is absent".into()))?;
    Ok(assertion_result(
        ledger.batch().map_err(knowledge_error)?.slice(index, 1),
    ))
}

fn publish_reasoning(
    graph: &GraphForge,
    request: &RecordReasoningRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    reasoning: &ReasoningLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants = reasoning_publication_participants(parent, reasoning)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.context.operation_uuid.0,
        generation_uuid: knowledge_generation_uuid(
            b"reasoning",
            request.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graphforge_storage::stage_project_generation(root, &publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => staged
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before reasoning publication",
                        ));
                    }
                    Ok(())
                },
            )?
            .publish()?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    graph.reasoning(request.reasoning_uuid, None)
}

fn publish_confidence(
    graph: &GraphForge,
    request: &AssessConfidenceRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    knowledge: &ConfidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants = confidence_publication_participants(parent, knowledge, provenance)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let generation_uuid =
        knowledge_generation_uuid(b"confidence", request.context.operation_uuid, &participants);
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.context.operation_uuid.0,
        generation_uuid,
        capabilities,
        participants,
    };
    let receipt = match graphforge_storage::stage_project_generation(root, &publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged_generation) => staged_generation
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before confidence publication",
                        ));
                    }
                    Ok(())
                },
            )?
            .publish()?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    let ledger = read_confidence_ledger(&graphforge_storage::resolve_project_generation(root)?)?;
    let index = ledger
        .assessments
        .iter()
        .position(|row| row.confidence_uuid == request.confidence_uuid)
        .ok_or_else(|| GfError::Validation("committed confidence is absent".into()))?;
    Ok(assertion_result(
        ledger
            .assessment_batch()
            .map_err(knowledge_error)?
            .slice(index, 1),
    ))
}

fn publish_evidence(
    graph: &GraphForge,
    request: &AttachEvidenceRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    knowledge: &EvidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants = evidence_publication_participants(parent, knowledge, provenance)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.context.operation_uuid.0,
        generation_uuid: knowledge_generation_uuid(
            b"evidence",
            request.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graphforge_storage::stage_project_generation(root, &publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged_generation) => staged_generation
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before evidence publication",
                        ));
                    }
                    Ok(())
                },
            )?
            .publish()?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    let ledger = read_evidence_ledger(&graphforge_storage::resolve_project_generation(root)?)?;
    let index = ledger
        .links
        .iter()
        .position(|row| row.evidence_uuid == request.evidence_uuid)
        .ok_or_else(|| GfError::Validation("committed evidence is absent".into()))?;
    Ok(assertion_result(
        ledger.batch().map_err(knowledge_error)?.slice(index, 1),
    ))
}

fn publish_assertion_evidence(
    graph: &GraphForge,
    request: &CreateAssertionWithEvidenceRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    assertions: &AssertionLedger,
    evidence: &EvidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants =
        assertion_evidence_publication_participants(parent, assertions, evidence, provenance)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.assertion.context.operation_uuid.0,
        generation_uuid: knowledge_generation_uuid(
            b"assertion-evidence",
            request.assertion.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graphforge_storage::stage_project_generation(root, &publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged_generation) => staged_generation
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before assertion evidence publication",
                        ));
                    }
                    Ok(())
                },
            )?
            .publish()?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    let ledger = read_ledger(&graphforge_storage::resolve_project_generation(root)?)?;
    let index = ledger
        .assertions
        .iter()
        .position(|row| row.assertion_uuid == request.assertion.assertion_uuid)
        .ok_or_else(|| GfError::Validation("committed assertion is absent".into()))?;
    Ok(assertion_result(
        ledger
            .assertion_batch()
            .map_err(knowledge_error)?
            .slice(index, 1),
    ))
}

impl CreateAssertionRequest {
    fn validate_context(&self) -> Result<(), GfError> {
        validate_write_context(&self.context)
    }
}

pub(crate) fn empty_participants() -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = encode_ledger(&AssertionLedger::default())?;
    participants.extend(encode_confidence_ledger(&ConfidenceLedger::default())?);
    participants.extend(encode_evidence_ledger(&EvidenceLedger::default())?);
    participants.extend(crate::algorithm_runs::empty_participants()?);
    Ok(participants)
}

pub(crate) fn empty_epistemic_participants() -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = encode_reasoning_ledger(&ReasoningLedger::default())?;
    participants.extend(encode_status_ledger(&AssertionStatusLedger::default())?);
    participants.extend(encode_supersession_ledger(
        &AssertionSupersessionLedger::default(),
    )?);
    participants.extend(crate::hypotheses::empty_participants()?);
    participants.extend(crate::belief_projection::empty_participants()?);
    Ok(participants)
}

pub(crate) fn read_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<AssertionLedger, GfError> {
    generation.require_capability("knowledge", 1)?;
    let assertions = generation.participant_snapshot("knowledge", "assertions")?;
    let refs = generation.participant_snapshot("knowledge", "assertion_graph_refs")?;
    match (assertions, refs) {
        (None, None) => AssertionLedger::new(Vec::new(), Vec::new()).map_err(knowledge_error),
        (Some(assertions), Some(refs)) => {
            require_participant_contract(&assertions, "assertions")?;
            require_participant_contract(&refs, "assertion_graph_refs")?;
            let assertion_batches = read_or_empty(&assertions, true)?;
            let ref_batches = read_or_empty(&refs, false)?;
            AssertionLedger::from_batches(&assertion_batches, &ref_batches).map_err(knowledge_error)
        }
        _ => Err(GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: "knowledge assertion participant set is incomplete".into(),
        }),
    }
}

pub(crate) fn read_confidence_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<ConfidenceLedger, GfError> {
    generation.require_capability("knowledge", 1)?;
    let assessments = generation.participant_snapshot("knowledge", "confidence_assessments")?;
    let inputs = generation.participant_snapshot("knowledge", "confidence_inputs")?;
    match (assessments, inputs) {
        (None, None) => ConfidenceLedger::new(Vec::new(), Vec::new()).map_err(knowledge_error),
        (Some(assessments), Some(inputs)) => {
            require_participant_contract(&assessments, "confidence_assessments")?;
            require_participant_contract(&inputs, "confidence_inputs")?;
            ConfidenceLedger::from_batches(
                &read_confidence_or_empty(&assessments, true)?,
                &read_confidence_or_empty(&inputs, false)?,
            )
            .map_err(knowledge_error)
        }
        _ => Err(GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: "knowledge confidence participant set is incomplete".into(),
        }),
    }
}

pub(crate) fn read_evidence_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<EvidenceLedger, GfError> {
    generation.require_capability("knowledge", 1)?;
    match generation.participant_snapshot("knowledge", "evidence")? {
        None => EvidenceLedger::new(Vec::new()).map_err(knowledge_error),
        Some(snapshot) => {
            require_participant_contract(&snapshot, "evidence")?;
            EvidenceLedger::from_batches(&read_evidence_or_empty(&snapshot)?)
                .map_err(knowledge_error)
        }
    }
}

pub(crate) fn read_reasoning_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<ReasoningLedger, GfError> {
    generation.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
    match generation.participant_snapshot("epistemic", "reasoning")? {
        None => ReasoningLedger::new(Vec::new()).map_err(knowledge_error),
        Some(snapshot) => {
            require_participant_contract(&snapshot, "reasoning")?;
            let batches = if snapshot.row_count == 0 {
                vec![
                    ReasoningLedger::default()
                        .batch()
                        .map_err(knowledge_error)?,
                ]
            } else {
                read_parquet(&snapshot.bytes)?
            };
            ReasoningLedger::from_batches(&batches).map_err(knowledge_error)
        }
    }
}

pub(crate) fn read_status_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<AssertionStatusLedger, GfError> {
    generation.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
    match generation.participant_snapshot("epistemic", "assertion_status_events")? {
        None => AssertionStatusLedger::new(Vec::new()).map_err(knowledge_error),
        Some(snapshot) => {
            require_participant_contract(&snapshot, "assertion_status_events")?;
            let batches = if snapshot.row_count == 0 {
                vec![
                    AssertionStatusLedger::default()
                        .batch()
                        .map_err(knowledge_error)?,
                ]
            } else {
                read_parquet(&snapshot.bytes)?
            };
            AssertionStatusLedger::from_batches(&batches).map_err(knowledge_error)
        }
    }
}

pub(crate) fn read_supersession_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<AssertionSupersessionLedger, GfError> {
    generation.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
    match generation.participant_snapshot("epistemic", "assertion_supersessions")? {
        None => AssertionSupersessionLedger::new(Vec::new()).map_err(knowledge_error),
        Some(snapshot) => {
            require_participant_contract(&snapshot, "assertion_supersessions")?;
            let batches = if snapshot.row_count == 0 {
                vec![
                    AssertionSupersessionLedger::default()
                        .batch()
                        .map_err(knowledge_error)?,
                ]
            } else {
                read_parquet(&snapshot.bytes)?
            };
            AssertionSupersessionLedger::from_batches(&batches).map_err(knowledge_error)
        }
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

fn validate_status_request(request: &RecordAssertionStatusRequest) -> Result<(), GfError> {
    require_uuid(request.status_event_uuid, "status_event_uuid")?;
    require_uuid(request.assertion_uuid, "assertion_uuid")?;
    if let Some(value) = request.confidence_uuid {
        require_uuid(value, "confidence_uuid")?;
    }
    if let Some(value) = request.reasoning_uuid {
        require_uuid(value, "reasoning_uuid")?;
    }
    require_uuid(request.provenance_uuid, "provenance_uuid")
}

fn assertion_refs_match(ledger: &AssertionLedger, request: &CreateAssertionRequest) -> bool {
    let mut existing = ledger
        .graph_refs
        .iter()
        .filter(|row| row.assertion_uuid == request.assertion_uuid)
        .map(|row| (row.graph_uuid, row.graph_kind, row.role, row.ordinal))
        .collect::<Vec<_>>();
    let mut requested = request
        .graph_refs
        .iter()
        .map(|row| (row.graph_uuid, row.graph_kind, row.role, row.ordinal))
        .collect::<Vec<_>>();
    existing.sort_by_key(|row| (row.2.as_str(), row.3, row.0));
    requested.sort_by_key(|row| (row.2.as_str(), row.3, row.0));
    existing == requested
}

fn validate_status_references(
    generation: &ResolvedProjectGeneration,
    assertion_uuid: Uuid,
    confidence_uuid: Option<Uuid>,
    reasoning_uuid: Option<Uuid>,
    provenance_uuid: Uuid,
) -> Result<(), GfError> {
    if !read_ledger(generation)?
        .assertions
        .iter()
        .any(|row| row.assertion_uuid == assertion_uuid)
    {
        return Err(not_found_kind("assertion"));
    }
    if let Some(confidence_uuid) = confidence_uuid
        && !read_confidence_ledger(generation)?
            .assessments
            .iter()
            .any(|row| {
                row.confidence_uuid == confidence_uuid && row.assertion_uuid == assertion_uuid
            })
    {
        return Err(not_found_kind("confidence assessment for assertion"));
    }
    if let Some(reasoning_uuid) = reasoning_uuid
        && !read_reasoning_ledger(generation)?
            .records
            .iter()
            .any(|row| row.reasoning_uuid == reasoning_uuid && row.assertion_uuid == assertion_uuid)
    {
        return Err(not_found_kind("reasoning record for assertion"));
    }
    if !crate::provenance::read_ledger(generation)?
        .events
        .iter()
        .any(|row| row.provenance_uuid == provenance_uuid)
    {
        return Err(not_found_kind("provenance event"));
    }
    Ok(())
}

fn staged_evidence_bundle(
    request: &CreateAssertionWithEvidenceRequest,
    provenance_uuid: Uuid,
    recorded_at_micros: i64,
) -> Result<EvidenceLedger, GfError> {
    let links = request
        .evidence
        .iter()
        .map(|input| {
            EvidenceLink::new(
                input.evidence_uuid,
                request.assertion.assertion_uuid,
                input.source_uuid,
                input.source_kind,
                input.role,
                input.weight,
                provenance_uuid,
                recorded_at_micros,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(knowledge_error)?;
    EvidenceLedger::new(links).map_err(knowledge_error)
}

fn merged_provenance(
    parent: &ResolvedProjectGeneration,
    request: &CreateAssertionRequest,
    staged: &AssertionLedger,
    recorded_at_micros: i64,
) -> Result<ProvenanceLedger, GfError> {
    let existing = crate::provenance::read_ledger(parent)?;
    let event = ProvenanceEvent::new(
        request.context.operation_uuid.0,
        EventKind::CreateAssertion,
        request.context.actor_uuid,
        recorded_at_micros,
    )
    .map_err(provenance_error)?;
    let mut lineage = Vec::with_capacity(request.graph_refs.len() + 1);
    for (ordinal, reference) in staged.graph_refs.iter().enumerate() {
        lineage.push(
            LineageRecord::new(
                event.provenance_uuid,
                reference.graph_uuid,
                match reference.graph_kind {
                    GraphObjectKind::Node => SubjectKind::Node,
                    GraphObjectKind::Edge => SubjectKind::Edge,
                },
                LineageRole::Input,
                u32::try_from(ordinal)
                    .map_err(|_| GfError::Execution("lineage ordinal exceeds u32".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    lineage.push(
        LineageRecord::new(
            event.provenance_uuid,
            request.assertion_uuid,
            SubjectKind::Assertion,
            LineageRole::Output,
            0,
        )
        .map_err(provenance_error)?,
    );
    existing
        .merge(&ProvenanceLedger::new(vec![event], lineage).map_err(provenance_error)?)
        .map_err(provenance_error)
}

fn merged_confidence_provenance(
    parent: &ResolvedProjectGeneration,
    request: &AssessConfidenceRequest,
    staged: &ConfidenceLedger,
    event: &ProvenanceEvent,
) -> Result<ProvenanceLedger, GfError> {
    let existing = crate::provenance::read_ledger(parent)?;
    let mut lineage = Vec::with_capacity(staged.inputs.len() + 2);
    lineage.push(
        LineageRecord::new(
            event.provenance_uuid,
            request.assertion_uuid,
            SubjectKind::Assertion,
            LineageRole::Input,
            0,
        )
        .map_err(provenance_error)?,
    );
    for (ordinal, input) in staged.inputs.iter().enumerate() {
        lineage.push(
            LineageRecord::new(
                event.provenance_uuid,
                input.input_confidence_uuid,
                SubjectKind::ConfidenceAssessment,
                LineageRole::Input,
                u32::try_from(ordinal + 1)
                    .map_err(|_| GfError::Execution("lineage ordinal exceeds u32".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    lineage.push(
        LineageRecord::new(
            event.provenance_uuid,
            request.confidence_uuid,
            SubjectKind::ConfidenceAssessment,
            LineageRole::Output,
            0,
        )
        .map_err(provenance_error)?,
    );
    existing
        .merge(&ProvenanceLedger::new(vec![event.clone()], lineage).map_err(provenance_error)?)
        .map_err(provenance_error)
}

fn merged_evidence_provenance(
    parent: &ResolvedProjectGeneration,
    request: &AttachEvidenceRequest,
    event: &ProvenanceEvent,
) -> Result<ProvenanceLedger, GfError> {
    let existing = crate::provenance::read_ledger(parent)?;
    let source_kind = match request.source_kind {
        EvidenceSourceKind::GraphNode => SubjectKind::Node,
        EvidenceSourceKind::GraphEdge => SubjectKind::Edge,
        EvidenceSourceKind::Document | EvidenceSourceKind::Observation => SubjectKind::EvidenceLink,
    };
    let lineage = vec![
        LineageRecord::new(
            event.provenance_uuid,
            request.assertion_uuid,
            SubjectKind::Assertion,
            LineageRole::Input,
            0,
        )
        .map_err(provenance_error)?,
        LineageRecord::new(
            event.provenance_uuid,
            request.source_uuid,
            source_kind,
            LineageRole::Input,
            1,
        )
        .map_err(provenance_error)?,
        LineageRecord::new(
            event.provenance_uuid,
            request.evidence_uuid,
            SubjectKind::EvidenceLink,
            LineageRole::Output,
            0,
        )
        .map_err(provenance_error)?,
    ];
    existing
        .merge(&ProvenanceLedger::new(vec![event.clone()], lineage).map_err(provenance_error)?)
        .map_err(provenance_error)
}

fn merged_assertion_evidence_provenance(
    parent: &ResolvedProjectGeneration,
    request: &CreateAssertionWithEvidenceRequest,
    staged: &AssertionLedger,
    event: &ProvenanceEvent,
) -> Result<ProvenanceLedger, GfError> {
    let existing = crate::provenance::read_ledger(parent)?;
    let mut lineage = Vec::new();
    for (ordinal, reference) in staged.graph_refs.iter().enumerate() {
        lineage.push(
            LineageRecord::new(
                event.provenance_uuid,
                reference.graph_uuid,
                match reference.graph_kind {
                    GraphObjectKind::Node => SubjectKind::Node,
                    GraphObjectKind::Edge => SubjectKind::Edge,
                },
                LineageRole::Input,
                u32::try_from(ordinal)
                    .map_err(|_| GfError::Execution("lineage ordinal exceeds u32".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    let evidence_offset = staged.graph_refs.len();
    for (ordinal, input) in request.evidence.iter().enumerate() {
        lineage.push(
            LineageRecord::new(
                event.provenance_uuid,
                input.source_uuid,
                match input.source_kind {
                    EvidenceSourceKind::GraphNode => SubjectKind::Node,
                    EvidenceSourceKind::GraphEdge => SubjectKind::Edge,
                    EvidenceSourceKind::Document | EvidenceSourceKind::Observation => {
                        SubjectKind::EvidenceLink
                    }
                },
                LineageRole::Input,
                u32::try_from(evidence_offset + ordinal)
                    .map_err(|_| GfError::Execution("lineage ordinal exceeds u32".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    lineage.push(
        LineageRecord::new(
            event.provenance_uuid,
            request.assertion.assertion_uuid,
            SubjectKind::Assertion,
            LineageRole::Output,
            0,
        )
        .map_err(provenance_error)?,
    );
    for (ordinal, input) in request.evidence.iter().enumerate() {
        lineage.push(
            LineageRecord::new(
                event.provenance_uuid,
                input.evidence_uuid,
                SubjectKind::EvidenceLink,
                LineageRole::Output,
                u32::try_from(ordinal + 1)
                    .map_err(|_| GfError::Execution("lineage ordinal exceeds u32".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    existing
        .merge(&ProvenanceLedger::new(vec![event.clone()], lineage).map_err(provenance_error)?)
        .map_err(provenance_error)
}

fn assertion_publication_participants(
    parent: &ResolvedProjectGeneration,
    knowledge: &AssertionLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "assertions" | "assertion_graph_refs"
                )
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_ledger(knowledge)?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

fn confidence_publication_participants(
    parent: &ResolvedProjectGeneration,
    knowledge: &ConfidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "confidence_assessments" | "confidence_inputs"
                )
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_confidence_ledger(knowledge)?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

fn evidence_publication_participants(
    parent: &ResolvedProjectGeneration,
    knowledge: &EvidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge" && snapshot.record_family_id == "evidence"
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_evidence_ledger(knowledge)?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

fn reasoning_publication_participants(
    parent: &ResolvedProjectGeneration,
    reasoning: &ReasoningLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "epistemic" && snapshot.record_family_id == "reasoning")
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_reasoning_ledger(reasoning)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

fn status_publication_participants(
    parent: &ResolvedProjectGeneration,
    status: &AssertionStatusLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "epistemic"
                && snapshot.record_family_id == "assertion_status_events")
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_status_ledger(status)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

fn supersession_publication_participants(
    parent: &ResolvedProjectGeneration,
    relations: &AssertionSupersessionLedger,
    status: &AssertionStatusLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "epistemic"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "assertion_supersessions" | "assertion_status_events"
                ))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_supersession_ledger(relations)?);
    participants.extend(encode_status_ledger(status)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

fn assertion_status_bundle_participants(
    parent: &ResolvedProjectGeneration,
    assertions: &AssertionLedger,
    status: &AssertionStatusLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "assertions" | "assertion_graph_refs"
                )
                || snapshot.capability_id == "epistemic"
                    && snapshot.record_family_id == "assertion_status_events"
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_ledger(assertions)?);
    participants.extend(encode_status_ledger(status)?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

fn assertion_evidence_publication_participants(
    parent: &ResolvedProjectGeneration,
    assertions: &AssertionLedger,
    evidence: &EvidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "assertions" | "assertion_graph_refs" | "evidence"
                )
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_ledger(assertions)?);
    participants.extend(encode_evidence_ledger(evidence)?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(crate) fn encode_ledger(ledger: &AssertionLedger) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let assertions = registry
        .iter()
        .find(|entry| entry.record_family == "assertions")
        .expect("assertion registry");
    let refs = registry
        .iter()
        .find(|entry| entry.record_family == "assertion_graph_refs")
        .expect("assertion graph-ref registry");
    Ok(vec![
        participant(
            assertions,
            &ledger.assertion_batch().map_err(knowledge_error)?,
        )?,
        participant(refs, &ledger.graph_ref_batch().map_err(knowledge_error)?)?,
    ])
}

pub(crate) fn encode_confidence_ledger(
    ledger: &ConfidenceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let assessments = registry
        .iter()
        .find(|entry| entry.record_family == "confidence_assessments")
        .expect("confidence assessment registry");
    let inputs = registry
        .iter()
        .find(|entry| entry.record_family == "confidence_inputs")
        .expect("confidence input registry");
    Ok(vec![
        participant(
            assessments,
            &ledger.assessment_batch().map_err(knowledge_error)?,
        )?,
        participant(inputs, &ledger.input_batch().map_err(knowledge_error)?)?,
    ])
}

pub(crate) fn encode_evidence_ledger(
    ledger: &EvidenceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let evidence = registry
        .iter()
        .find(|entry| entry.record_family == "evidence")
        .expect("evidence registry");
    Ok(vec![participant(
        evidence,
        &ledger.batch().map_err(knowledge_error)?,
    )?])
}

pub(crate) fn encode_reasoning_ledger(
    ledger: &ReasoningLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let reasoning = registry
        .iter()
        .find(|entry| entry.record_family == "reasoning")
        .expect("reasoning registry");
    Ok(vec![participant(
        reasoning,
        &ledger.batch().map_err(knowledge_error)?,
    )?])
}

pub(crate) fn encode_status_ledger(
    ledger: &AssertionStatusLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let status = registry
        .iter()
        .find(|entry| entry.record_family == "assertion_status_events")
        .expect("assertion-status registry");
    Ok(vec![participant(
        status,
        &ledger.batch().map_err(knowledge_error)?,
    )?])
}

pub(crate) fn encode_supersession_ledger(
    ledger: &AssertionSupersessionLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let relation = registry
        .iter()
        .find(|entry| entry.record_family == "assertion_supersessions")
        .expect("assertion-supersession registry");
    Ok(vec![participant(
        relation,
        &ledger.batch().map_err(knowledge_error)?,
    )?])
}

pub(crate) fn participant(
    registry: &graphforge_knowledge::SchemaRegistryEntry,
    batch: &RecordBatch,
) -> Result<ProjectParticipant, GfError> {
    Ok(ProjectParticipant {
        capability_id: registry.capability_id.into(),
        capability_version: registry.capability_version,
        record_family_id: registry.record_family.into(),
        record_version: registry.record_version,
        encoding: ProjectParticipantEncoding::Parquet,
        schema_fingerprint: registry.schema_fingerprint,
        row_count: u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
        bytes: write_parquet(batch, &registry.schema)?,
    })
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

fn validate_evidence_source(
    graph: &GraphForge,
    source_uuid: Uuid,
    source_kind: EvidenceSourceKind,
) -> Result<(), GfError> {
    let mut pending = HashSet::from([source_uuid]);
    match source_kind {
        EvidenceSourceKind::Document | EvidenceSourceKind::Observation => return Ok(()),
        EvidenceSourceKind::GraphNode => match_requested_node_uuids(graph, &mut pending)?,
        EvidenceSourceKind::GraphEdge => match_requested_edge_uuids(graph, &mut pending)?,
    }
    if pending.is_empty() {
        Ok(())
    } else {
        Err(GfError::Api {
            code: ApiErrorCode::NotFound,
            message: "evidence graph source UUID was not found".into(),
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
    for batch in graphforge_storage::read_nodes(&graph.dir)
        .map_err(|error| GfError::Storage(error.to_string()))?
    {
        match_requested_uuid_column(&batch, "node_uuid", pending)?;
        if pending.is_empty() {
            break;
        }
    }
    Ok(())
}

fn match_requested_edge_uuids(
    graph: &GraphForge,
    pending: &mut HashSet<Uuid>,
) -> Result<(), GfError> {
    if pending.is_empty() {
        return Ok(());
    }
    for batch in graphforge_storage::read_edges(&graph.dir, "*", graph.ontology_mode)
        .map_err(|error| GfError::Storage(error.to_string()))?
    {
        match_requested_uuid_column(&batch, "edge_uuid", pending)?;
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

fn assertion_generation_uuid(
    operation_uuid: OperationId,
    participants: &[ProjectParticipant],
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-assertion-generation/1");
    hasher.update(operation_uuid.0.as_bytes());
    for participant in participants {
        hasher.update(participant.capability_id.as_bytes());
        hasher.update([0]);
        hasher.update(participant.record_family_id.as_bytes());
        hasher.update([0]);
        hasher.update(Sha256::digest(&participant.bytes));
    }
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

pub(crate) fn knowledge_generation_uuid(
    operation: &[u8],
    operation_uuid: OperationId,
    participants: &[ProjectParticipant],
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-knowledge-generation/1");
    hasher.update(operation);
    hasher.update([0]);
    hasher.update(operation_uuid.0.as_bytes());
    for participant in participants {
        hasher.update(participant.capability_id.as_bytes());
        hasher.update([0]);
        hasher.update(participant.record_family_id.as_bytes());
        hasher.update([0]);
        hasher.update(Sha256::digest(&participant.bytes));
    }
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

pub(crate) fn snapshot_to_participant(
    snapshot: graphforge_storage::ProjectParticipantSnapshot,
) -> Result<ProjectParticipant, GfError> {
    Ok(ProjectParticipant {
        capability_id: snapshot.capability_id,
        capability_version: snapshot.capability_version,
        record_family_id: snapshot.record_family_id,
        record_version: snapshot.record_version,
        encoding: match snapshot.encoding.as_str() {
            "parquet" => ProjectParticipantEncoding::Parquet,
            "arrow" => ProjectParticipantEncoding::Arrow,
            "json" => ProjectParticipantEncoding::Json,
            _ => {
                return Err(GfError::Validation(
                    "committed participant has unsupported encoding".into(),
                ));
            }
        },
        schema_fingerprint: snapshot.schema_fingerprint,
        row_count: snapshot.row_count,
        bytes: snapshot.bytes,
    })
}

fn write_parquet(batch: &RecordBatch, schema: &SchemaRef) -> Result<Vec<u8>, GfError> {
    let mut writer = ArrowWriter::try_new(Vec::new(), Arc::clone(schema), None)
        .map_err(|error| GfError::Storage(error.to_string()))?;
    writer
        .write(batch)
        .map_err(|error| GfError::Storage(error.to_string()))?;
    writer
        .into_inner()
        .map_err(|error| GfError::Storage(error.to_string()))
}

pub(crate) fn read_parquet(bytes: &[u8]) -> Result<Vec<RecordBatch>, GfError> {
    let file =
        tempfile::NamedTempFile::new().map_err(|error| GfError::Storage(error.to_string()))?;
    fs::write(file.path(), bytes).map_err(|error| GfError::Storage(error.to_string()))?;
    ParquetRecordBatchReaderBuilder::try_new(
        file.reopen()
            .map_err(|error| GfError::Storage(error.to_string()))?,
    )
    .map_err(|error| GfError::Validation(format!("invalid knowledge parquet: {error}")))?
    .build()
    .map_err(|error| GfError::Validation(format!("invalid knowledge parquet: {error}")))?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|error| GfError::Validation(format!("invalid knowledge parquet: {error}")))
}

fn read_or_empty(
    snapshot: &graphforge_storage::ProjectParticipantSnapshot,
    assertions: bool,
) -> Result<Vec<RecordBatch>, GfError> {
    if snapshot.row_count == 0 {
        let ledger = AssertionLedger::default();
        Ok(vec![if assertions {
            ledger.assertion_batch().map_err(knowledge_error)?
        } else {
            ledger.graph_ref_batch().map_err(knowledge_error)?
        }])
    } else {
        read_parquet(&snapshot.bytes)
    }
}

fn read_confidence_or_empty(
    snapshot: &graphforge_storage::ProjectParticipantSnapshot,
    assessments: bool,
) -> Result<Vec<RecordBatch>, GfError> {
    if snapshot.row_count == 0 {
        let ledger = ConfidenceLedger::default();
        Ok(vec![if assessments {
            ledger.assessment_batch().map_err(knowledge_error)?
        } else {
            ledger.input_batch().map_err(knowledge_error)?
        }])
    } else {
        read_parquet(&snapshot.bytes)
    }
}

fn read_evidence_or_empty(
    snapshot: &graphforge_storage::ProjectParticipantSnapshot,
) -> Result<Vec<RecordBatch>, GfError> {
    if snapshot.row_count == 0 {
        Ok(vec![
            EvidenceLedger::default().batch().map_err(knowledge_error)?,
        ])
    } else {
        read_parquet(&snapshot.bytes)
    }
}

pub(crate) fn require_participant_contract(
    snapshot: &graphforge_storage::ProjectParticipantSnapshot,
    family: &str,
) -> Result<(), GfError> {
    let registry = schema_registry();
    let expected = registry
        .iter()
        .find(|entry| entry.record_family == family)
        .expect("registered knowledge family");
    if snapshot.capability_version != expected.capability_version
        || snapshot.record_version != expected.record_version
        || snapshot.encoding != "parquet"
        || snapshot.schema_fingerprint != expected.schema_fingerprint
    {
        return Err(GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: "unsupported knowledge participant contract".into(),
        });
    }
    Ok(())
}

pub(crate) fn concat_or_empty(
    rows: &[RecordBatch],
    schema: &SchemaRef,
) -> Result<RecordBatch, GfError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    arrow::compute::concat_batches(schema, rows)
        .map_err(|error| GfError::Execution(error.to_string()))
}

pub(crate) fn with_next_token(
    batch: &RecordBatch,
    next: Option<&PageToken>,
) -> Result<RecordBatch, GfError> {
    let mut metadata = batch.schema().metadata().clone();
    if let Some(next) = next {
        metadata.insert(
            "graphforge.next_page_token".into(),
            next.as_str().to_owned(),
        );
    }
    let schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().to_vec(),
        metadata,
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec())
        .map_err(|error| GfError::Execution(error.to_string()))
}

pub(crate) fn assertion_result(batch: RecordBatch) -> graphforge_exec::ExecutionResult {
    let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
    graphforge_exec::ExecutionResult {
        schema: batch.schema(),
        batches: vec![batch],
        stats: graphforge_exec::ExecutionStats {
            rows_produced: rows,
            execution_time_ms: 0,
        },
        side_effects: None,
        mutation_receipt: None,
    }
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

    fn uuid7(seed: u8) -> Uuid {
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
    fn empty_ledger_codecs_and_participant_contracts_are_exact() {
        let assertion = AssertionLedger::default();
        let confidence = ConfidenceLedger::default();
        let evidence = EvidenceLedger::default();
        let reasoning = ReasoningLedger::default();
        let status = AssertionStatusLedger::default();
        let supersession = AssertionSupersessionLedger::default();
        for participants in [
            encode_ledger(&assertion).unwrap(),
            encode_confidence_ledger(&confidence).unwrap(),
            encode_evidence_ledger(&evidence).unwrap(),
            encode_reasoning_ledger(&reasoning).unwrap(),
            encode_status_ledger(&status).unwrap(),
            encode_supersession_ledger(&supersession).unwrap(),
        ] {
            assert!(!participants.is_empty());
            assert!(participants.iter().all(|participant| {
                participant.encoding == ProjectParticipantEncoding::Parquet
                    && participant.row_count == 0
                    && !participant.bytes.is_empty()
            }));
            for participant in participants {
                assert!(read_parquet(&participant.bytes).unwrap().is_empty());
            }
        }

        let registry = schema_registry();
        let entry = registry
            .iter()
            .find(|entry| entry.record_family == "assertions")
            .unwrap();
        let snapshot = graphforge_storage::ProjectParticipantSnapshot {
            capability_id: entry.capability_id.into(),
            capability_version: entry.capability_version,
            record_family_id: entry.record_family.into(),
            record_version: entry.record_version,
            encoding: "parquet".into(),
            schema_fingerprint: entry.schema_fingerprint,
            row_count: 0,
            bytes: Vec::new(),
        };
        require_participant_contract(&snapshot, "assertions").unwrap();
        assert_eq!(read_or_empty(&snapshot, true).unwrap()[0].num_rows(), 0);
        assert_eq!(read_or_empty(&snapshot, false).unwrap()[0].num_rows(), 0);
        let mut incompatible = snapshot.clone();
        incompatible.encoding = "json".into();
        assert_eq!(
            require_participant_contract(&incompatible, "assertions")
                .unwrap_err()
                .code(),
            "GF_SCHEMA_MISMATCH"
        );
        assert_eq!(
            snapshot_to_participant(incompatible).unwrap().encoding,
            ProjectParticipantEncoding::Json
        );
        let mut unsupported = snapshot;
        unsupported.encoding = "sqlite".into();
        assert_eq!(
            snapshot_to_participant(unsupported).unwrap_err().code(),
            "GF_VALIDATION"
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

    fn enable(graph: &GraphForge, capability_id: CapabilityId, seed: u8) {
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

    fn assertion_fixture(graph: &GraphForge, assertion_uuid: Uuid, operation_seed: u8) -> Uuid {
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

    fn reasoning_fixture(
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
    fn reasoning_is_append_only_exact_idempotent_and_reopenable() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(10));
        enable(&graph, CapabilityId::Provenance, 1);
        enable(&graph, CapabilityId::Knowledge, 2);
        enable(&graph, CapabilityId::Epistemic, 3);
        let assertion_uuid = uuid7(4);
        let provenance_uuid = assertion_fixture(&graph, assertion_uuid, 5);
        let first_uuid = uuid7(6);
        let first = RecordReasoningRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(7)),
                actor_uuid: None,
            },
            reasoning_uuid: first_uuid,
            assertion_uuid,
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextMarkdown,
            content: b"exact **reasoning**".to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid,
        };
        let created = graph.record_reasoning(first.clone()).unwrap();
        assert_eq!(
            created.batches[0].schema(),
            Arc::clone(&graphforge_knowledge::REASONING_SCHEMA)
        );
        assert_eq!(
            graph.record_reasoning(first).unwrap().batches[0],
            created.batches[0]
        );

        graph.set_clock_for_test(|| Ok(20));
        let amendment_uuid = uuid7(8);
        graph
            .record_reasoning(RecordReasoningRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(9)),
                    actor_uuid: None,
                },
                reasoning_uuid: amendment_uuid,
                assertion_uuid,
                kind: ReasoningKind::LogicalInference,
                content_format: ReasoningContentFormat::TextPlain,
                content: b"explicit amendment".to_vec(),
                supersedes_reasoning_uuid: Some(first_uuid),
                provenance_uuid,
            })
            .unwrap();
        let history = graph
            .list_reasoning(ListReasoningRequest {
                assertion_uuid: Some(assertion_uuid),
                page: PageRequest::default(),
            })
            .unwrap();
        assert_eq!(history.stats.rows_produced, 2);

        drop(graph);
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(
            reopened.reasoning(amendment_uuid, None).unwrap().batches[0].schema(),
            Arc::clone(&graphforge_knowledge::REASONING_SCHEMA)
        );
        assert_eq!(
            reopened
                .list_reasoning(ListReasoningRequest {
                    assertion_uuid: Some(assertion_uuid),
                    page: PageRequest::default(),
                })
                .unwrap()
                .stats
                .rows_produced,
            2
        );
    }

    #[test]
    fn reasoning_rejects_dangling_cross_assertion_and_conflicting_replay() {
        let graph = GraphForge::new(None).unwrap();
        graph.set_clock_for_test(|| Ok(10));
        enable(&graph, CapabilityId::Provenance, 20);
        enable(&graph, CapabilityId::Knowledge, 21);
        enable(&graph, CapabilityId::Epistemic, 22);
        let assertion_uuid = uuid7(23);
        let provenance_uuid = assertion_fixture(&graph, assertion_uuid, 24);
        let request = RecordReasoningRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(25)),
                actor_uuid: None,
            },
            reasoning_uuid: uuid7(26),
            assertion_uuid,
            kind: ReasoningKind::MethodologicalNote,
            content_format: ReasoningContentFormat::TextPlain,
            content: b"method".to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid,
        };
        graph.record_reasoning(request.clone()).unwrap();
        let mut conflict = request;
        conflict.content = b"changed".to_vec();
        assert_eq!(
            graph.record_reasoning(conflict).unwrap_err().code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        let dangling = graph
            .record_reasoning(RecordReasoningRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(27)),
                    actor_uuid: None,
                },
                reasoning_uuid: uuid7(28),
                assertion_uuid,
                kind: ReasoningKind::DecisionRationale,
                content_format: ReasoningContentFormat::ApplicationJson,
                content: br#"{"decision":"no"}"#.to_vec(),
                supersedes_reasoning_uuid: Some(uuid7(99)),
                provenance_uuid,
            })
            .unwrap_err();
        assert_eq!(dangling.code(), "GF_NOT_FOUND");
    }

    #[test]
    fn assertion_publication_is_atomic_idempotent_and_reopenable() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(1_234_567));
        enable(&graph, CapabilityId::Provenance, 1);
        let node = graph.add_node("Person", &HashMap::new()).unwrap();
        enable(&graph, CapabilityId::Knowledge, 2);
        let assertion_uuid = uuid7(3);
        let request = CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(4)),
                actor_uuid: Some(uuid7(5)),
            },
            assertion_uuid,
            claim: "e\u{301} is not é".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: node.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        };

        let created = graph.create_assertion(request.clone()).unwrap();
        assert_eq!(created.stats.rows_produced, 1);
        let generation = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        let replay = graph.create_assertion(request).unwrap();
        assert_eq!(replay.batches[0], created.batches[0]);
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            generation
        );
        let refs = graph
            .assertion_graph_refs(assertion_uuid, PageRequest::default())
            .unwrap();
        assert_eq!(refs.stats.rows_produced, 1);

        drop(graph);
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        let fetched = reopened.assertion(assertion_uuid, None).unwrap();
        let claim = fetched.batches[0]
            .column_by_name("claim")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(claim.value(0), "e\u{301} is not é");
        let history = reopened
            .list_provenance_history(crate::ProvenanceHistoryRequest {
                subject_uuid: Some(assertion_uuid),
                operation_uuid: None,
                page: PageRequest::default(),
            })
            .unwrap();
        assert_eq!(history.stats.rows_produced, 1);
    }

    #[test]
    fn invalid_graph_reference_and_conflicting_replay_publish_nothing() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(999));
        enable(&graph, CapabilityId::Provenance, 10);
        let node = graph.add_node("Person", &HashMap::new()).unwrap();
        enable(&graph, CapabilityId::Knowledge, 11);
        let before = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        let missing = graph
            .create_assertion(CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(12)),
                    actor_uuid: None,
                },
                assertion_uuid: uuid7(13),
                claim: "missing".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: uuid7(14),
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap_err();
        assert_eq!(missing.code(), "GF_NOT_FOUND");
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            before
        );

        let assertion_uuid = uuid7(15);
        let mut request = CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(16)),
                actor_uuid: None,
            },
            assertion_uuid,
            claim: "first".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: node.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        };
        graph.create_assertion(request.clone()).unwrap();
        let committed = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        request.claim = "different".into();
        let conflict = graph.create_assertion(request).unwrap_err();
        assert_eq!(conflict.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            committed
        );
    }

    #[test]
    fn assertion_lists_filter_and_page_with_generation_bound_tokens() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(2_000));
        enable(&graph, CapabilityId::Provenance, 20);
        let first_node = graph.add_node("Person", &HashMap::new()).unwrap();
        let second_node = graph.add_node("Person", &HashMap::new()).unwrap();
        enable(&graph, CapabilityId::Knowledge, 21);

        for (seed, claim, refs) in [
            (
                30,
                "first",
                vec![
                    AssertionGraphRefInput {
                        graph_uuid: first_node.uuid,
                        graph_kind: GraphObjectKind::Node,
                        role: AssertionGraphRole::Subject,
                        ordinal: 0,
                    },
                    AssertionGraphRefInput {
                        graph_uuid: second_node.uuid,
                        graph_kind: GraphObjectKind::Node,
                        role: AssertionGraphRole::Context,
                        ordinal: 0,
                    },
                ],
            ),
            (
                31,
                "second",
                vec![AssertionGraphRefInput {
                    graph_uuid: second_node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            ),
        ] {
            graph
                .create_assertion(CreateAssertionRequest {
                    context: WriteContext {
                        operation_uuid: OperationId(uuid7(seed + 10)),
                        actor_uuid: None,
                    },
                    assertion_uuid: uuid7(seed),
                    claim: claim.into(),
                    graph_refs: refs,
                })
                .unwrap();
        }

        let first_page = graph
            .list_assertions(ListAssertionsRequest {
                graph_uuid: None,
                page: PageRequest {
                    limit: 1,
                    after: None,
                    cancellation: None,
                },
            })
            .unwrap();
        let token = first_page.batches[0]
            .schema()
            .metadata()
            .get("graphforge.next_page_token")
            .cloned()
            .expect("first assertion page must continue");
        let second_page = graph
            .list_assertions(ListAssertionsRequest {
                graph_uuid: None,
                page: PageRequest {
                    limit: 1,
                    after: Some(PageToken::parse(&token).unwrap()),
                    cancellation: None,
                },
            })
            .unwrap();
        assert_eq!(first_page.stats.rows_produced, 1);
        assert_eq!(second_page.stats.rows_produced, 1);
        assert_ne!(first_page.batches[0], second_page.batches[0]);

        let excluded = graph
            .list_assertions(ListAssertionsRequest {
                graph_uuid: Some(uuid7(99)),
                page: PageRequest::default(),
            })
            .unwrap();
        assert_eq!(excluded.stats.rows_produced, 0);

        let first_refs = graph
            .assertion_graph_refs(
                uuid7(30),
                PageRequest {
                    limit: 1,
                    after: None,
                    cancellation: None,
                },
            )
            .unwrap();
        let ref_token = first_refs.batches[0]
            .schema()
            .metadata()
            .get("graphforge.next_page_token")
            .cloned()
            .expect("first graph-ref page must continue");
        let second_refs = graph
            .assertion_graph_refs(
                uuid7(30),
                PageRequest {
                    limit: 1,
                    after: Some(PageToken::parse(&ref_token).unwrap()),
                    cancellation: None,
                },
            )
            .unwrap();
        assert_eq!(first_refs.stats.rows_produced, 1);
        assert_eq!(second_refs.stats.rows_produced, 1);
        assert_ne!(first_refs.batches[0], second_refs.batches[0]);
    }

    #[test]
    fn confidence_publication_is_atomic_idempotent_deterministic_and_reopenable() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(3_000));
        enable(&graph, CapabilityId::Provenance, 50);
        let node = graph.add_node("Person", &HashMap::new()).unwrap();
        enable(&graph, CapabilityId::Knowledge, 51);
        let assertion_uuid = uuid7(52);
        graph
            .create_assertion(CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(53)),
                    actor_uuid: None,
                },
                assertion_uuid,
                claim: "confidence target".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap();
        let explicit_uuid = uuid7(54);
        let explicit = AssessConfidenceRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(55)),
                actor_uuid: None,
            },
            confidence_uuid: explicit_uuid,
            assertion_uuid,
            policy: ConfidencePolicyRequest::Explicit { value: 0.8 },
        };
        let created = graph.assess_confidence(explicit.clone()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        assert_eq!(
            graph.assess_confidence(explicit).unwrap().batches[0],
            created.batches[0]
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            generation
        );

        let derived_uuid = uuid7(56);
        graph
            .assess_confidence(AssessConfidenceRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(57)),
                    actor_uuid: None,
                },
                confidence_uuid: derived_uuid,
                assertion_uuid,
                policy: ConfidencePolicyRequest::ConservativeMin {
                    input_confidence_uuids: vec![uuid7(99), explicit_uuid],
                },
            })
            .unwrap();
        let inputs = graph
            .confidence_inputs(derived_uuid, PageRequest::default())
            .unwrap();
        assert_eq!(inputs.stats.rows_produced, 2);
        let values = inputs.batches[0]
            .column_by_name("input_value")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(values.is_valid(0));
        assert!(values.is_null(1));
        let list = graph
            .list_confidence_assessments(ListConfidenceAssessmentsRequest {
                assertion_uuid: Some(assertion_uuid),
                page: PageRequest::default(),
            })
            .unwrap();
        assert_eq!(list.stats.rows_produced, 2);

        drop(graph);
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(
            reopened
                .confidence_assessment(explicit_uuid, None)
                .unwrap()
                .batches[0],
            created.batches[0]
        );
        let history = reopened
            .list_provenance_history(crate::ProvenanceHistoryRequest {
                subject_uuid: Some(derived_uuid),
                operation_uuid: None,
                page: PageRequest::default(),
            })
            .unwrap();
        assert_eq!(history.stats.rows_produced, 1);
    }

    #[test]
    fn invalid_confidence_writes_publish_nothing() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(4_000));
        enable(&graph, CapabilityId::Provenance, 60);
        enable(&graph, CapabilityId::Knowledge, 61);
        let before = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        let missing = graph
            .assess_confidence(AssessConfidenceRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(62)),
                    actor_uuid: None,
                },
                confidence_uuid: uuid7(63),
                assertion_uuid: uuid7(64),
                policy: ConfidencePolicyRequest::Explicit { value: 0.5 },
            })
            .unwrap_err();
        assert_eq!(missing.code(), "GF_NOT_FOUND");
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            before
        );
    }

    #[test]
    fn evidence_publication_is_atomic_idempotent_filterable_and_reopenable() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(5_000));
        enable(&graph, CapabilityId::Provenance, 70);
        let node = graph.add_node("Person", &HashMap::new()).unwrap();
        enable(&graph, CapabilityId::Knowledge, 71);
        let assertion_uuid = uuid7(72);
        graph
            .create_assertion(CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(73)),
                    actor_uuid: None,
                },
                assertion_uuid,
                claim: "evidence target".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap();
        let evidence_uuid = uuid7(74);
        let request = AttachEvidenceRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(75)),
                actor_uuid: None,
            },
            evidence_uuid,
            assertion_uuid,
            source_uuid: node.uuid,
            source_kind: EvidenceSourceKind::GraphNode,
            role: EvidenceRole::Supports,
            weight: Some(0.9),
        };
        let created = graph.attach_evidence(request.clone()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        assert_eq!(
            graph.attach_evidence(request).unwrap().batches[0],
            created.batches[0]
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            generation
        );
        assert_eq!(
            graph
                .list_evidence_links(ListEvidenceLinksRequest {
                    assertion_uuid: Some(assertion_uuid),
                    source_uuid: Some(node.uuid),
                    page: PageRequest::default(),
                })
                .unwrap()
                .stats
                .rows_produced,
            1
        );
        drop(graph);
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(
            reopened.evidence_link(evidence_uuid, None).unwrap().batches[0],
            created.batches[0]
        );
        assert_eq!(
            reopened
                .list_provenance_history(crate::ProvenanceHistoryRequest {
                    subject_uuid: Some(evidence_uuid),
                    operation_uuid: None,
                    page: PageRequest::default(),
                })
                .unwrap()
                .stats
                .rows_produced,
            1
        );
    }

    #[test]
    fn invalid_evidence_companion_publishes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(6_000));
        enable(&graph, CapabilityId::Provenance, 80);
        enable(&graph, CapabilityId::Knowledge, 81);
        let before = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        let error = graph
            .attach_evidence(AttachEvidenceRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(82)),
                    actor_uuid: None,
                },
                evidence_uuid: uuid7(83),
                assertion_uuid: uuid7(84),
                source_uuid: uuid7(85),
                source_kind: EvidenceSourceKind::GraphNode,
                role: EvidenceRole::Supports,
                weight: None,
            })
            .unwrap_err();
        assert_eq!(error.code(), "GF_NOT_FOUND");
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            before
        );
    }

    #[test]
    fn assertion_with_evidence_is_one_atomic_idempotent_generation() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(7_000));
        enable(&graph, CapabilityId::Provenance, 90);
        let node = graph.add_node("Person", &HashMap::new()).unwrap();
        enable(&graph, CapabilityId::Knowledge, 91);
        let assertion_uuid = uuid7(92);
        let evidence_uuid = uuid7(93);
        let request = CreateAssertionWithEvidenceRequest {
            assertion: CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(94)),
                    actor_uuid: None,
                },
                assertion_uuid,
                claim: "atomic bundle".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            },
            evidence: vec![EvidenceInput {
                evidence_uuid,
                source_uuid: uuid7(95),
                source_kind: EvidenceSourceKind::Document,
                role: EvidenceRole::Context,
                weight: None,
            }],
        };
        let created = graph
            .create_assertion_with_evidence(request.clone())
            .unwrap();
        let generation = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        assert_eq!(
            graph
                .create_assertion_with_evidence(request)
                .unwrap()
                .batches[0],
            created.batches[0]
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            generation
        );
        assert_eq!(
            graph
                .evidence_link(evidence_uuid, None)
                .unwrap()
                .stats
                .rows_produced,
            1
        );
        assert_eq!(
            graph
                .list_provenance_history(crate::ProvenanceHistoryRequest {
                    subject_uuid: Some(assertion_uuid),
                    operation_uuid: None,
                    page: PageRequest::default(),
                })
                .unwrap()
                .stats
                .rows_produced,
            1
        );
    }

    #[test]
    fn assertion_status_is_explicit_append_only_idempotent_and_reopenable() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(10));
        enable(&graph, CapabilityId::Provenance, 110);
        enable(&graph, CapabilityId::Knowledge, 111);
        enable(&graph, CapabilityId::Epistemic, 112);
        let assertion_uuid = uuid7(113);
        let provenance_uuid = assertion_fixture(&graph, assertion_uuid, 114);
        assert_eq!(
            graph
                .assertion_status(assertion_uuid)
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        let first = RecordAssertionStatusRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(115)),
                actor_uuid: None,
            },
            status_event_uuid: uuid7(116),
            assertion_uuid,
            status: AssertionStatus::Hypothesis,
            confidence_uuid: None,
            reasoning_uuid: None,
            provenance_uuid,
        };
        let created = graph.record_assertion_status(first.clone()).unwrap();
        assert_eq!(
            graph.record_assertion_status(first).unwrap().batches[0],
            created.batches[0]
        );
        let confidence_uuid = uuid7(117);
        graph
            .assess_confidence(AssessConfidenceRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(118)),
                    actor_uuid: None,
                },
                confidence_uuid,
                assertion_uuid,
                policy: ConfidencePolicyRequest::Explicit { value: 0.75 },
            })
            .unwrap();
        let reasoning_uuid = uuid7(119);
        graph
            .record_reasoning(RecordReasoningRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(120)),
                    actor_uuid: None,
                },
                reasoning_uuid,
                assertion_uuid,
                kind: ReasoningKind::EvidenceInterpretation,
                content_format: ReasoningContentFormat::TextPlain,
                content: b"supports status".to_vec(),
                supersedes_reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap();
        graph
            .record_assertion_status(RecordAssertionStatusRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(121)),
                    actor_uuid: None,
                },
                status_event_uuid: uuid7(122),
                assertion_uuid,
                status: AssertionStatus::Supported,
                confidence_uuid: Some(confidence_uuid),
                reasoning_uuid: Some(reasoning_uuid),
                provenance_uuid,
            })
            .unwrap();
        assert_eq!(
            graph.assertion_status(assertion_uuid).unwrap().batches[0]
                .column_by_name("status")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0),
            "supported"
        );
        let tied_lower_uuid = RecordAssertionStatusRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(123)),
                actor_uuid: None,
            },
            status_event_uuid: uuid7(100),
            assertion_uuid,
            status: AssertionStatus::Disputed,
            confidence_uuid: None,
            reasoning_uuid: None,
            provenance_uuid,
        };
        let appended = graph
            .record_assertion_status(tied_lower_uuid.clone())
            .unwrap();
        assert_eq!(
            appended.batches[0]
                .column_by_name("status")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0),
            "disputed"
        );
        assert_eq!(
            graph
                .record_assertion_status(tied_lower_uuid)
                .unwrap()
                .batches[0],
            appended.batches[0]
        );
        assert_eq!(
            graph.assertion_status(assertion_uuid).unwrap().batches[0]
                .column_by_name("status")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0),
            "supported"
        );
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(
            reopened
                .list_assertion_status(ListAssertionStatusRequest {
                    assertion_uuid: Some(assertion_uuid),
                    page: PageRequest::default(),
                })
                .unwrap()
                .stats
                .rows_produced,
            3
        );
    }

    #[test]
    fn assertion_status_rejects_conflicts_missing_refs_and_direct_supersession() {
        let graph = GraphForge::new(None).unwrap();
        graph.set_clock_for_test(|| Ok(10));
        enable(&graph, CapabilityId::Provenance, 120);
        enable(&graph, CapabilityId::Knowledge, 121);
        enable(&graph, CapabilityId::Epistemic, 122);
        let assertion_uuid = uuid7(123);
        let provenance_uuid = assertion_fixture(&graph, assertion_uuid, 124);
        let request = RecordAssertionStatusRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(125)),
                actor_uuid: None,
            },
            status_event_uuid: uuid7(126),
            assertion_uuid,
            status: AssertionStatus::Disputed,
            confidence_uuid: None,
            reasoning_uuid: None,
            provenance_uuid,
        };
        graph.record_assertion_status(request.clone()).unwrap();
        let mut conflict = request.clone();
        conflict.status = AssertionStatus::Refuted;
        assert_eq!(
            graph.record_assertion_status(conflict).unwrap_err().code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        let mut missing = request.clone();
        missing.status_event_uuid = uuid7(127);
        missing.context.operation_uuid = OperationId(uuid7(128));
        missing.confidence_uuid = Some(uuid7(129));
        assert_eq!(
            graph.record_assertion_status(missing).unwrap_err().code(),
            "GF_NOT_FOUND"
        );
        let mut superseded = request;
        superseded.status_event_uuid = uuid7(130);
        superseded.context.operation_uuid = OperationId(uuid7(131));
        superseded.status = AssertionStatus::Superseded;
        assert_eq!(
            graph
                .record_assertion_status(superseded)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
    }

    #[test]
    fn assertion_and_first_status_publish_as_one_idempotent_generation() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(20));
        enable(&graph, CapabilityId::Provenance, 140);
        enable(&graph, CapabilityId::Knowledge, 141);
        enable(&graph, CapabilityId::Epistemic, 142);
        let node = graph.add_node("StatusSubject", &HashMap::new()).unwrap();
        let assertion_uuid = uuid7(143);
        let request = CreateAssertionWithStatusRequest {
            assertion: CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(144)),
                    actor_uuid: None,
                },
                assertion_uuid,
                claim: "explicit first status".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            },
            first_status: FirstAssertionStatusInput {
                status_event_uuid: uuid7(145),
                status: AssertionStatus::Hypothesis,
            },
        };
        let created = graph.create_assertion_with_status(request.clone()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        graph.set_clock_for_test(|| Ok(999));
        assert_eq!(
            graph.create_assertion_with_status(request).unwrap().batches[0],
            created.batches[0]
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            generation
        );
        assert_eq!(
            graph
                .assertion(assertion_uuid, None)
                .unwrap()
                .stats
                .rows_produced,
            1
        );
        assert_eq!(
            graph
                .assertion_status(assertion_uuid)
                .unwrap()
                .stats
                .rows_produced,
            1
        );
    }

    #[test]
    fn supersession_is_atomic_branch_preserving_idempotent_and_reopenable() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(50));
        enable(&graph, CapabilityId::Provenance, 200);
        enable(&graph, CapabilityId::Knowledge, 201);
        enable(&graph, CapabilityId::Epistemic, 202);
        let prior = uuid7(203);
        let first_replacement = uuid7(204);
        let second_replacement = uuid7(205);
        let provenance = assertion_fixture(&graph, prior, 206);
        assertion_fixture(&graph, first_replacement, 207);
        assertion_fixture(&graph, second_replacement, 208);
        let reasoning = reasoning_fixture(&graph, prior, provenance, 209, 210);
        let first = SupersedeAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(211)),
                actor_uuid: None,
            },
            supersession_uuid: uuid7(212),
            prior_assertion_uuid: prior,
            replacement_assertion_uuid: first_replacement,
            status_event_uuid: uuid7(213),
            reasoning_uuid: reasoning,
            provenance_uuid: provenance,
        };
        let created = graph.supersede_assertion(first.clone()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        assert_eq!(
            graph.supersede_assertion(first).unwrap().batches[0],
            created.batches[0]
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            generation
        );
        graph
            .supersede_assertion(SupersedeAssertionRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(214)),
                    actor_uuid: None,
                },
                supersession_uuid: uuid7(215),
                prior_assertion_uuid: prior,
                replacement_assertion_uuid: second_replacement,
                status_event_uuid: uuid7(216),
                reasoning_uuid: reasoning,
                provenance_uuid: provenance,
            })
            .unwrap();
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        let first_page = reopened
            .list_assertion_supersessions(ListAssertionSupersessionsRequest {
                prior_assertion_uuid: Some(prior),
                replacement_assertion_uuid: None,
                page: PageRequest {
                    limit: 1,
                    after: None,
                    cancellation: None,
                },
            })
            .unwrap();
        let token = first_page.batches[0]
            .schema()
            .metadata()
            .get("graphforge.next_page_token")
            .cloned()
            .expect("first supersession page must continue");
        let second_page = reopened
            .list_assertion_supersessions(ListAssertionSupersessionsRequest {
                prior_assertion_uuid: Some(prior),
                replacement_assertion_uuid: None,
                page: PageRequest {
                    limit: 1,
                    after: Some(PageToken::parse(&token).unwrap()),
                    cancellation: None,
                },
            })
            .unwrap();
        assert_eq!(first_page.stats.rows_produced, 1);
        assert_eq!(second_page.stats.rows_produced, 1);
        assert_ne!(first_page.batches[0], second_page.batches[0]);
        assert_eq!(
            reopened
                .list_assertion_supersessions(ListAssertionSupersessionsRequest {
                    prior_assertion_uuid: None,
                    replacement_assertion_uuid: Some(second_replacement),
                    page: PageRequest::default(),
                })
                .unwrap()
                .stats
                .rows_produced,
            1
        );
        assert_eq!(
            reopened
                .list_assertion_status(ListAssertionStatusRequest {
                    assertion_uuid: Some(prior),
                    page: PageRequest::default(),
                })
                .unwrap()
                .stats
                .rows_produced,
            2
        );
        for assertion_uuid in [prior, first_replacement, second_replacement] {
            assert_eq!(
                reopened
                    .assertion(assertion_uuid, None)
                    .unwrap()
                    .stats
                    .rows_produced,
                1
            );
        }
    }

    #[test]
    fn supersession_rejects_self_links_cycles_dangling_refs_and_conflicts() {
        let graph = GraphForge::new(None).unwrap();
        graph.set_clock_for_test(|| Ok(60));
        enable(&graph, CapabilityId::Provenance, 220);
        enable(&graph, CapabilityId::Knowledge, 221);
        enable(&graph, CapabilityId::Epistemic, 222);
        let first = uuid7(223);
        let second = uuid7(224);
        let first_provenance = assertion_fixture(&graph, first, 225);
        let second_provenance = assertion_fixture(&graph, second, 226);
        let first_reasoning = reasoning_fixture(&graph, first, first_provenance, 227, 228);
        let second_reasoning = reasoning_fixture(&graph, second, second_provenance, 229, 230);
        let request = SupersedeAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(231)),
                actor_uuid: None,
            },
            supersession_uuid: uuid7(232),
            prior_assertion_uuid: first,
            replacement_assertion_uuid: second,
            status_event_uuid: uuid7(233),
            reasoning_uuid: first_reasoning,
            provenance_uuid: first_provenance,
        };
        graph.supersede_assertion(request.clone()).unwrap();
        let mut conflict = request.clone();
        conflict.status_event_uuid = uuid7(234);
        assert_eq!(
            graph.supersede_assertion(conflict).unwrap_err().code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        let self_link = SupersedeAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(235)),
                actor_uuid: None,
            },
            supersession_uuid: uuid7(236),
            prior_assertion_uuid: second,
            replacement_assertion_uuid: second,
            status_event_uuid: uuid7(237),
            reasoning_uuid: second_reasoning,
            provenance_uuid: second_provenance,
        };
        assert_eq!(
            graph.supersede_assertion(self_link).unwrap_err().code(),
            "GF_VALIDATION"
        );
        let cycle = SupersedeAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(238)),
                actor_uuid: None,
            },
            supersession_uuid: uuid7(239),
            prior_assertion_uuid: second,
            replacement_assertion_uuid: first,
            status_event_uuid: uuid7(240),
            reasoning_uuid: second_reasoning,
            provenance_uuid: second_provenance,
        };
        assert_eq!(
            graph.supersede_assertion(cycle).unwrap_err().code(),
            "GF_VALIDATION"
        );
        let mut wrong_reasoning = request;
        wrong_reasoning.context.operation_uuid = OperationId(uuid7(241));
        wrong_reasoning.supersession_uuid = uuid7(242);
        wrong_reasoning.status_event_uuid = uuid7(243);
        wrong_reasoning.reasoning_uuid = second_reasoning;
        assert_eq!(
            graph
                .supersede_assertion(wrong_reasoning)
                .unwrap_err()
                .code(),
            "GF_NOT_FOUND"
        );
    }
}
