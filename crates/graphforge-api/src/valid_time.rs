//! Optional append-only valid-time interpretation over transaction snapshots.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray,
    TimestampMicrosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};
use graphforge_knowledge::{
    ASSERTION_VALIDITY_SCHEMA, AssertionValidityEvent, AssertionValidityLedger, schema_registry,
};
use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectStageOutcome,
    ResolvedProjectGeneration,
};
use uuid::Uuid;

use crate::{GraphForge, PageRequest, PageToken, WriteContext};

/// Versioned half-open interval interpretation.
pub const VALID_TIME_POLICY_VERSION: u32 = 1;

/// Append one immutable assertion-validity event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordAssertionValidityRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 event identity.
    pub validity_event_uuid: Uuid,
    /// Existing immutable assertion.
    pub assertion_uuid: Uuid,
    /// Inclusive lower valid-time bound.
    pub valid_from_micros: Option<i64>,
    /// Exclusive upper valid-time bound.
    pub valid_to_micros: Option<i64>,
    /// Optional existing M21 reasoning.
    pub reasoning_uuid: Option<Uuid>,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
}

/// Filter validity history.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListAssertionValidityRequest {
    /// Optional assertion UUID.
    pub assertion_uuid: Option<Uuid>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Apply valid time after resolving the transaction-time cutoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplyValidTimeRequest {
    /// Mandatory transaction-time cutoff.
    pub transaction_cutoff_micros: i64,
    /// Valid time to evaluate.
    pub valid_time_micros: i64,
}

impl GraphForge {
    /// Append one validity correction without rewriting prior transaction views.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-valid-time/1 freezes owned request structs"
    )]
    pub fn record_assertion_validity(
        &self,
        request: RecordAssertionValidityRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_context(&request.context)?;
        for (uuid, field) in [
            (request.validity_event_uuid, "validity_event_uuid"),
            (request.assertion_uuid, "assertion_uuid"),
            (request.provenance_uuid, "provenance_uuid"),
        ] {
            require_uuid(uuid, field)?;
        }
        if let Some(uuid) = request.reasoning_uuid {
            require_uuid(uuid, "reasoning_uuid")?;
        }
        let _visibility = crate::knowledge::lock_graph_visibility(self)?;
        let parent = resolve(self)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("valid_time", 1)?;
        validate_references(
            &parent,
            request.assertion_uuid,
            request.reasoning_uuid,
            request.provenance_uuid,
        )?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(conflict(
                "project generation changed before valid-time publication",
            ));
        }
        let existing = read_ledger(&parent)?;
        if let Some(index) = existing
            .events
            .iter()
            .position(|row| row.validity_event_uuid == request.validity_event_uuid)
        {
            let row = &existing.events[index];
            if row.assertion_uuid == request.assertion_uuid
                && row.valid_from_micros == request.valid_from_micros
                && row.valid_to_micros == request.valid_to_micros
                && row.reasoning_uuid == request.reasoning_uuid
                && row.provenance_uuid == request.provenance_uuid
            {
                return result_row(&existing, index);
            }
            return Err(conflict(
                "validity event UUID was reused for different content",
            ));
        }
        let recorded_at = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged = AssertionValidityLedger::new(vec![
            AssertionValidityEvent::new(
                request.validity_event_uuid,
                request.assertion_uuid,
                request.valid_from_micros,
                request.valid_to_micros,
                request.reasoning_uuid,
                request.provenance_uuid,
                recorded_at,
            )
            .map_err(crate::knowledge::knowledge_error)?,
        ])
        .map_err(crate::knowledge::knowledge_error)?;
        let merged = existing
            .merge(&staged)
            .map_err(crate::knowledge::knowledge_error)?;
        publish(self, &request.context, &parent, expected_parent, &merged)?;
        let committed = read_ledger(&resolve(self)?)?;
        let index = committed
            .events
            .iter()
            .position(|row| row.validity_event_uuid == request.validity_event_uuid)
            .ok_or_else(|| GfError::Validation("committed validity event is absent".into()))?;
        result_row(&committed, index)
    }

    /// Return deterministic append-only validity history.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-valid-time/1 freezes owned request structs"
    )]
    pub fn list_assertion_validity(
        &self,
        request: ListAssertionValidityRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(uuid) = request.assertion_uuid {
            require_uuid(uuid, "assertion_uuid")?;
        }
        let generation = resolve(self)?;
        let ledger = read_ledger(&generation)?;
        let selected = ledger
            .events
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .assertion_uuid
                    .is_none_or(|uuid| row.assertion_uuid == uuid)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.batch().map_err(crate::knowledge::knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = crate::knowledge::concat_or_empty(&rows, &ASSERTION_VALIDITY_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(crate::knowledge::assertion_result(
            crate::knowledge::with_next_token(&batch, next.as_ref())?,
        ))
    }

    /// Evaluate the selected validity event for every assertion visible at cutoff T.
    ///
    /// Assertions without a validity event are returned as `uninterpreted`.
    #[allow(
        clippy::too_many_lines,
        reason = "one explicit Arrow result contract is kept locally auditable"
    )]
    pub fn apply_valid_time(
        &self,
        request: ApplyValidTimeRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        let generation = resolve(self)?;
        generation.require_capability("valid_time", 1)?;
        let validity = read_ledger(&generation)?;
        let base = self.epistemic_snapshot(request.transaction_cutoff_micros)?;
        let base_batch = base
            .batches
            .first()
            .ok_or_else(|| GfError::Validation("epistemic snapshot returned no batch".into()))?;
        let entity_kinds = base_batch
            .column_by_name("entity_kind")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| GfError::Validation("epistemic entity kind is absent".into()))?;
        let assertion_values = base_batch
            .column_by_name("assertion_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| GfError::Validation("epistemic assertion UUID is absent".into()))?;
        let visible = (0..base_batch.num_rows())
            .filter(|row| entity_kinds.value(*row) == "assertion")
            .map(|row| {
                if assertion_values.is_null(row) {
                    return Err(GfError::Validation(
                        "epistemic assertion row has no assertion UUID".into(),
                    ));
                }
                Uuid::from_slice(assertion_values.value(row))
                    .map_err(|_| GfError::Validation("invalid epistemic assertion UUID".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut assertion_ids = FixedSizeBinaryBuilder::with_capacity(visible.len(), 16);
        let mut event_ids = FixedSizeBinaryBuilder::with_capacity(visible.len(), 16);
        let mut states = Vec::with_capacity(visible.len());
        let mut is_valid = Vec::with_capacity(visible.len());
        for assertion_uuid in visible {
            assertion_ids
                .append_value(assertion_uuid.as_bytes())
                .map_err(|_| GfError::Validation("invalid assertion UUID width".into()))?;
            let event = validity.current_for_at(assertion_uuid, request.transaction_cutoff_micros);
            if let Some(event) = event {
                event_ids
                    .append_value(event.validity_event_uuid.as_bytes())
                    .map_err(|_| GfError::Validation("invalid validity UUID width".into()))?;
                states.push("interpreted");
                is_valid.push(Some(event.contains(request.valid_time_micros)));
            } else {
                event_ids.append_null();
                states.push("uninterpreted");
                is_valid.push(None);
            }
        }
        let row_count = states.len();
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("assertion_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("validity_event_uuid", DataType::FixedSizeBinary(16), true),
                Field::new("interpretation", DataType::Utf8, false),
                Field::new("is_valid", DataType::Boolean, true),
                Field::new(
                    "transaction_cutoff",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "valid_time",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new("policy_version", DataType::UInt32, false),
            ],
            [
                ("graphforge.valid_time.policy".into(), "half-open/1".into()),
                ("graphforge.base_snapshot_unchanged".into(), "true".into()),
            ]
            .into(),
        ));
        let content = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(assertion_ids.finish()) as ArrayRef,
                Arc::new(event_ids.finish()),
                Arc::new(StringArray::from(states)),
                Arc::new(BooleanArray::from(is_valid)),
                Arc::new(
                    TimestampMicrosecondArray::from_value(
                        request.transaction_cutoff_micros,
                        row_count,
                    )
                    .with_timezone("UTC"),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from_value(request.valid_time_micros, row_count)
                        .with_timezone("UTC"),
                ),
                Arc::new(UInt32Array::from_value(
                    VALID_TIME_POLICY_VERSION,
                    row_count,
                )),
            ],
        )
        .map_err(|error| GfError::Execution(error.to_string()))?;
        let fingerprint =
            crate::canonical_arrow::result_fingerprint(std::slice::from_ref(&content))
                .map_err(|error| GfError::Execution(error.to_string()))?;
        let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            "result_fingerprint",
            DataType::FixedSizeBinary(32),
            false,
        )));
        let result_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let mut columns = content.columns().to_vec();
        let mut fingerprints = FixedSizeBinaryBuilder::with_capacity(row_count, 32);
        for _ in 0..row_count {
            fingerprints
                .append_value(fingerprint)
                .map_err(|_| GfError::Validation("invalid fingerprint width".into()))?;
        }
        columns.push(Arc::new(fingerprints.finish()));
        let batch = RecordBatch::try_new(result_schema, columns)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        Ok(crate::knowledge::assertion_result(batch))
    }
}

pub(crate) fn empty_participants() -> Result<Vec<ProjectParticipant>, GfError> {
    encode_ledger(&AssertionValidityLedger::default())
}

pub(crate) fn read_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<AssertionValidityLedger, GfError> {
    generation.require_capability("valid_time", 1)?;
    match generation.participant_snapshot("valid_time", "assertion_validity_events")? {
        None => AssertionValidityLedger::new(Vec::new()).map_err(crate::knowledge::knowledge_error),
        Some(snapshot) => {
            crate::knowledge::require_participant_contract(&snapshot, "assertion_validity_events")?;
            let batches = if snapshot.row_count == 0 {
                vec![RecordBatch::new_empty(Arc::clone(
                    &ASSERTION_VALIDITY_SCHEMA,
                ))]
            } else {
                crate::knowledge::read_parquet(&snapshot.bytes)?
            };
            AssertionValidityLedger::from_batches(&batches)
                .map_err(crate::knowledge::knowledge_error)
        }
    }
}

pub(crate) fn encode_ledger(
    ledger: &AssertionValidityLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry()
        .into_iter()
        .find(|entry| entry.record_family == "assertion_validity_events")
        .expect("registered validity family");
    Ok(vec![crate::knowledge::participant(
        &registry,
        &ledger.batch().map_err(crate::knowledge::knowledge_error)?,
    )?])
}

fn publish(
    graph: &GraphForge,
    context: &WriteContext,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    ledger: &AssertionValidityLedger,
) -> Result<(), GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "valid_time"
                && snapshot.record_family_id == "assertion_validity_events")
        })
        .map(crate::knowledge::snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_ledger(ledger)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let request = ProjectGenerationRequest {
        transaction_uuid: context.operation_uuid.0,
        generation_uuid: crate::knowledge::knowledge_generation_uuid(
            b"valid-time",
            context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graphforge_storage::stage_project_generation(
        graph.resolved_generation.container_root(),
        &request,
    )? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => staged
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(conflict(
                            "project generation changed before valid-time publication",
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
    Ok(())
}

fn validate_references(
    generation: &ResolvedProjectGeneration,
    assertion_uuid: Uuid,
    reasoning_uuid: Option<Uuid>,
    provenance_uuid: Uuid,
) -> Result<(), GfError> {
    if !crate::knowledge::read_ledger(generation)?
        .assertions
        .iter()
        .any(|row| row.assertion_uuid == assertion_uuid)
    {
        return Err(not_found("assertion"));
    }
    if let Some(reasoning_uuid) = reasoning_uuid
        && !crate::knowledge::read_reasoning_ledger(generation)?
            .records
            .iter()
            .any(|row| row.reasoning_uuid == reasoning_uuid && row.assertion_uuid == assertion_uuid)
    {
        return Err(not_found("reasoning"));
    }
    if !crate::provenance::read_ledger(generation)?
        .events
        .iter()
        .any(|row| row.provenance_uuid == provenance_uuid)
    {
        return Err(not_found("provenance"));
    }
    Ok(())
}

fn result_row(
    ledger: &AssertionValidityLedger,
    index: usize,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    Ok(crate::knowledge::assertion_result(
        ledger
            .batch()
            .map_err(crate::knowledge::knowledge_error)?
            .slice(index, 1),
    ))
}

fn resolve(graph: &GraphForge) -> Result<ResolvedProjectGeneration, GfError> {
    graph.generation_for_read()
}

fn validate_context(context: &WriteContext) -> Result<(), GfError> {
    require_uuid(context.operation_uuid.0, "operation_uuid")?;
    if let Some(uuid) = context.actor_uuid {
        require_uuid(uuid, "actor_uuid")?;
    }
    Ok(())
}

fn require_uuid(uuid: Uuid, field: &'static str) -> Result<(), GfError> {
    if uuid.is_nil() {
        Err(GfError::Validation(format!("{field} must not be nil")))
    } else {
        Ok(())
    }
}

fn conflict(message: &'static str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::TransactionConflict,
        message: message.into(),
    }
}

fn not_found(kind: &'static str) -> GfError {
    GfError::Api {
        code: ApiErrorCode::NotFound,
        message: format!("{kind} was not found"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::{Array, BooleanArray, FixedSizeBinaryArray, StringArray};

    use super::*;
    use crate::{
        AssertionGraphRefInput, CapabilityId, CreateAssertionRequest, EnableCapabilityRequest,
        OperationId,
    };
    use graphforge_knowledge::{AssertionGraphRole, GraphObjectKind};

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn context(seed: u8) -> WriteContext {
        WriteContext {
            operation_uuid: OperationId(uuid7(seed)),
            actor_uuid: None,
        }
    }

    fn enable(graph: &GraphForge, capability_id: CapabilityId, seed: u8) {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(seed),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }

    fn assertion(graph: &GraphForge, assertion_uuid: Uuid, seed: u8) -> Uuid {
        let node = graph.add_node("ValiditySubject", &HashMap::new()).unwrap();
        let result = graph
            .create_assertion(CreateAssertionRequest {
                context: context(seed),
                assertion_uuid,
                claim: "validity is interpreted separately".into(),
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

    #[test]
    fn correction_preserves_prior_cutoff_base_snapshot_and_reopen_fingerprint() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(10));
        enable(&graph, CapabilityId::Provenance, 1);
        enable(&graph, CapabilityId::Knowledge, 2);
        enable(&graph, CapabilityId::Epistemic, 3);
        enable(&graph, CapabilityId::ValidTime, 4);
        let assertion_uuid = uuid7(20);
        let provenance_uuid = assertion(&graph, assertion_uuid, 21);
        let base_before = graph.epistemic_snapshot(100).unwrap().batches;

        graph.set_clock_for_test(|| Ok(20));
        graph
            .record_assertion_validity(RecordAssertionValidityRequest {
                context: context(22),
                validity_event_uuid: uuid7(23),
                assertion_uuid,
                valid_from_micros: Some(100),
                valid_to_micros: Some(200),
                reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap();
        let before_correction = graph
            .apply_valid_time(ApplyValidTimeRequest {
                transaction_cutoff_micros: 25,
                valid_time_micros: 150,
            })
            .unwrap();
        graph.set_clock_for_test(|| Ok(30));
        graph
            .record_assertion_validity(RecordAssertionValidityRequest {
                context: context(24),
                validity_event_uuid: uuid7(25),
                assertion_uuid,
                valid_from_micros: Some(300),
                valid_to_micros: None,
                reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap();

        let prior_again = graph
            .apply_valid_time(ApplyValidTimeRequest {
                transaction_cutoff_micros: 25,
                valid_time_micros: 150,
            })
            .unwrap();
        assert_eq!(before_correction.batches, prior_again.batches);
        assert_eq!(base_before, graph.epistemic_snapshot(100).unwrap().batches);
        let current = graph
            .apply_valid_time(ApplyValidTimeRequest {
                transaction_cutoff_micros: 30,
                valid_time_micros: 150,
            })
            .unwrap();
        let batch = &current.batches[0];
        assert_eq!(
            batch
                .column_by_name("interpretation")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "interpreted"
        );
        assert!(
            !batch
                .column_by_name("is_valid")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
        let fingerprint = batch
            .column_by_name("result_fingerprint")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0)
            .to_vec();
        drop(graph);
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        let reopened_result = reopened
            .apply_valid_time(ApplyValidTimeRequest {
                transaction_cutoff_micros: 30,
                valid_time_micros: 150,
            })
            .unwrap();
        assert_eq!(current.batches, reopened_result.batches);
        assert_eq!(
            fingerprint,
            reopened_result.batches[0]
                .column_by_name("result_fingerprint")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0)
        );
    }

    #[test]
    fn absent_capability_is_a_structured_error() {
        let graph = GraphForge::new(None).unwrap();
        let error = graph
            .apply_valid_time(ApplyValidTimeRequest {
                transaction_cutoff_micros: 0,
                valid_time_micros: 0,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            GfError::Project {
                code: ProjectErrorCode::CapabilityDisabled,
                ..
            }
        ));
    }
}
