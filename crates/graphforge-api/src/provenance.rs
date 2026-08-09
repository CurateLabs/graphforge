//! `graphforge-api` orchestration between neutral graph receipts and `graphforge-provenance`.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};
use graphforge_exec::{MutationKind, MutationReceipt, MutationSubject, MutationSubjectKind};
use graphforge_provenance::{
    EventKind, LineageRecord, LineageRole, ProvenanceEvent, ProvenanceLedger, SubjectKind,
    schema_registry,
};
use graphforge_storage::{
    ProjectParticipant, ProjectParticipantEncoding, ResolvedProjectGeneration,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use uuid::Uuid;

use crate::{CancellationToken, GraphForge, OperationId, PageRequest};

/// Frozen provenance-history filter and pagination request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProvenanceHistoryRequest {
    /// Optional referenced graph/knowledge UUID.
    pub subject_uuid: Option<Uuid>,
    /// Optional operation/idempotency UUID.
    pub operation_uuid: Option<OperationId>,
    /// Bounded generation-pinned page request.
    pub page: PageRequest,
}

impl GraphForge {
    /// Return one exact `provenance_event@1` row.
    ///
    /// # Errors
    /// Returns structured capability, corruption, cancellation, or not-found
    /// errors without opening graph or knowledge participants.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes an owned optional cancellation token"
    )]
    pub fn provenance_event(
        &self,
        provenance_uuid: Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if provenance_uuid.is_nil() {
            return Err(GfError::Validation(
                "provenance_uuid must not be nil".into(),
            ));
        }
        if let Some(cancellation) = &cancellation {
            cancellation.checkpoint()?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        if let Some(cancellation) = &cancellation {
            cancellation.checkpoint()?;
        }
        let index = ledger
            .events
            .iter()
            .position(|event| event.provenance_uuid == provenance_uuid)
            .ok_or_else(|| GfError::Api {
                code: ApiErrorCode::NotFound,
                message: "provenance event was not found".into(),
            })?;
        let batch = ledger
            .event_batch()
            .map_err(provenance_error)?
            .slice(index, 1);
        Ok(execution_result(batch))
    }

    /// Return one deterministic page of `provenance_history@1`.
    ///
    /// # Errors
    /// Returns structured capability, corruption, cancellation, pagination, or
    /// validation failures.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn list_provenance_history(
        &self,
        request: ProvenanceHistoryRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if request.subject_uuid.is_some_and(|uuid| uuid.is_nil())
            || request
                .operation_uuid
                .is_some_and(|operation| operation.0.is_nil())
        {
            return Err(GfError::Validation(
                "provenance history UUID filters must not be nil".into(),
            ));
        }
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        let events = ledger
            .events
            .iter()
            .map(|event| (event.provenance_uuid, event))
            .collect::<HashMap<_, _>>();
        let filtered = ledger
            .lineage
            .iter()
            .filter(|row| {
                request
                    .subject_uuid
                    .is_none_or(|subject_uuid| row.subject_uuid == subject_uuid)
                    && request.operation_uuid.is_none_or(|operation_uuid| {
                        events[&row.provenance_uuid].operation_uuid == operation_uuid.0
                    })
            })
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            filtered.len(),
        )?;
        let next = (end < filtered.len())
            .then(|| crate::PageToken::new(generation.generation_uuid(), end));
        let batch = history_batch(&events, &filtered[start..end], next.as_ref())?;
        if let Some(cancellation) = &request.page.cancellation {
            cancellation.checkpoint()?;
        }
        Ok(execution_result(batch))
    }
}

/// Build complete replacement provenance participants for one graph receipt.
pub(crate) fn merged_participants(
    parent: &ResolvedProjectGeneration,
    receipt: &MutationReceipt,
    operation_uuid: Uuid,
    actor_uuid: Option<Uuid>,
    recorded_at_micros: i64,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let existing = read_ledger(parent)?;
    let staged = ledger_from_receipt(receipt, operation_uuid, actor_uuid, recorded_at_micros)?;
    let merged = existing.merge(&staged).map_err(provenance_error)?;
    encode_ledger(&merged)
}

pub(crate) fn read_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<ProvenanceLedger, GfError> {
    generation.require_capability("provenance", 1)?;
    let events = generation.participant_snapshot("provenance", "events")?;
    let lineage = generation.participant_snapshot("provenance", "lineage")?;
    match (events, lineage) {
        (None, None) => ProvenanceLedger::new(Vec::new(), Vec::new()).map_err(provenance_error),
        (Some(events), Some(lineage)) => {
            require_participant_contract(&events, "events")?;
            require_participant_contract(&lineage, "lineage")?;
            let event_batches = if events.row_count == 0 {
                vec![
                    ProvenanceLedger::default()
                        .event_batch()
                        .map_err(provenance_error)?,
                ]
            } else {
                read_parquet(&events.bytes)?
            };
            let lineage_batches = if lineage.row_count == 0 {
                vec![
                    ProvenanceLedger::default()
                        .lineage_batch()
                        .map_err(provenance_error)?,
                ]
            } else {
                read_parquet(&lineage.bytes)?
            };
            ProvenanceLedger::from_batches(&event_batches, &lineage_batches)
                .map_err(provenance_error)
        }
        _ => Err(GfError::Validation(
            "provenance participant set is incomplete".into(),
        )),
    }
}

fn ledger_from_receipt(
    receipt: &MutationReceipt,
    operation_uuid: Uuid,
    actor_uuid: Option<Uuid>,
    recorded_at_micros: i64,
) -> Result<ProvenanceLedger, GfError> {
    let mut events = Vec::with_capacity(receipt.effects.len());
    let mut lineage = Vec::new();
    for effect in &receipt.effects {
        let event = ProvenanceEvent::new(
            operation_uuid,
            event_kind(effect.kind),
            actor_uuid,
            recorded_at_micros,
        )
        .map_err(provenance_error)?;
        append_lineage(
            &mut lineage,
            event.provenance_uuid,
            &effect.inputs,
            LineageRole::Input,
        )?;
        append_lineage(
            &mut lineage,
            event.provenance_uuid,
            &effect.outputs,
            LineageRole::Output,
        )?;
        events.push(event);
    }
    ProvenanceLedger::new(events, lineage).map_err(provenance_error)
}

fn append_lineage(
    rows: &mut Vec<LineageRecord>,
    provenance_uuid: Uuid,
    subjects: &[MutationSubject],
    role: LineageRole,
) -> Result<(), GfError> {
    for (ordinal, subject) in subjects.iter().enumerate() {
        rows.push(
            LineageRecord::new(
                provenance_uuid,
                Uuid::from_bytes(subject.uuid),
                subject_kind(subject.kind),
                role,
                u32::try_from(ordinal)
                    .map_err(|_| GfError::Execution("GF_RESOURCE_LIMIT: lineage ordinal".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    Ok(())
}

pub(crate) fn empty_participants() -> Result<Vec<ProjectParticipant>, GfError> {
    encode_ledger(&ProvenanceLedger::default())
}

pub(crate) fn encode_ledger(ledger: &ProvenanceLedger) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let events = registry
        .iter()
        .find(|entry| entry.record_family == "events")
        .expect("events registry entry");
    let lineage = registry
        .iter()
        .find(|entry| entry.record_family == "lineage")
        .expect("lineage registry entry");
    let event_batch = ledger.event_batch().map_err(provenance_error)?;
    let lineage_batch = ledger.lineage_batch().map_err(provenance_error)?;
    Ok(vec![
        participant(events, &event_batch)?,
        participant(lineage, &lineage_batch)?,
    ])
}

fn participant(
    registry: &graphforge_provenance::SchemaRegistryEntry,
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

fn read_parquet(bytes: &[u8]) -> Result<Vec<RecordBatch>, GfError> {
    let file =
        tempfile::NamedTempFile::new().map_err(|error| GfError::Storage(error.to_string()))?;
    fs::write(file.path(), bytes).map_err(|error| GfError::Storage(error.to_string()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        file.reopen()
            .map_err(|error| GfError::Storage(error.to_string()))?,
    )
    .map_err(|error| GfError::Validation(format!("invalid provenance parquet: {error}")))?
    .build()
    .map_err(|error| GfError::Validation(format!("invalid provenance parquet: {error}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| GfError::Validation(format!("invalid provenance parquet: {error}")))
}

fn require_participant_contract(
    snapshot: &graphforge_storage::ProjectParticipantSnapshot,
    family: &str,
) -> Result<(), GfError> {
    let registry = schema_registry();
    let expected = registry
        .iter()
        .find(|entry| entry.record_family == family)
        .expect("registered provenance family");
    if snapshot.capability_version != expected.capability_version
        || snapshot.record_version != expected.record_version
        || snapshot.encoding != "parquet"
        || snapshot.schema_fingerprint != expected.schema_fingerprint
    {
        return Err(GfError::Validation(
            "unsupported provenance participant contract".into(),
        ));
    }
    Ok(())
}

const fn event_kind(kind: MutationKind) -> EventKind {
    match kind {
        MutationKind::CreateNode => EventKind::CreateNode,
        MutationKind::CreateEdge => EventKind::CreateEdge,
        MutationKind::MergeCreate => EventKind::MergeCreate,
        MutationKind::MergeMatchedNoop => EventKind::MergeMatchedNoop,
        MutationKind::SetProperty => EventKind::SetProperty,
        MutationKind::RemoveProperty => EventKind::RemoveProperty,
        MutationKind::AddLabel => EventKind::AddLabel,
        MutationKind::RemoveLabel => EventKind::RemoveLabel,
        MutationKind::Delete => EventKind::Delete,
        MutationKind::DetachDelete => EventKind::DetachDelete,
        MutationKind::OntologyInference => EventKind::OntologyInference,
    }
}

const fn subject_kind(kind: MutationSubjectKind) -> SubjectKind {
    match kind {
        MutationSubjectKind::Node => SubjectKind::Node,
        MutationSubjectKind::Edge => SubjectKind::Edge,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the domain error is consumed at the crate boundary and converted once"
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

fn execution_result(batch: RecordBatch) -> graphforge_exec::ExecutionResult {
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

fn history_batch(
    events: &HashMap<Uuid, &ProvenanceEvent>,
    rows: &[&LineageRecord],
    next: Option<&crate::PageToken>,
) -> Result<RecordBatch, GfError> {
    let mut provenance_ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut operation_ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut actor_ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut lineage_ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut subject_ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut event_kinds = Vec::with_capacity(rows.len());
    let mut recorded_at = Vec::with_capacity(rows.len());
    let mut event_versions = Vec::with_capacity(rows.len());
    let mut subject_kinds = Vec::with_capacity(rows.len());
    let mut roles = Vec::with_capacity(rows.len());
    let mut ordinals = Vec::with_capacity(rows.len());
    let mut lineage_versions = Vec::with_capacity(rows.len());
    for row in rows {
        let event = events
            .get(&row.provenance_uuid)
            .ok_or_else(|| GfError::Validation("dangling provenance history event".into()))?;
        provenance_ids
            .append_value(event.provenance_uuid.as_bytes())
            .map_err(|error| GfError::Execution(error.to_string()))?;
        operation_ids
            .append_value(event.operation_uuid.as_bytes())
            .map_err(|error| GfError::Execution(error.to_string()))?;
        match event.actor_uuid {
            Some(actor_uuid) => actor_ids
                .append_value(actor_uuid.as_bytes())
                .map_err(|error| GfError::Execution(error.to_string()))?,
            None => actor_ids.append_null(),
        }
        lineage_ids
            .append_value(row.lineage_uuid.as_bytes())
            .map_err(|error| GfError::Execution(error.to_string()))?;
        subject_ids
            .append_value(row.subject_uuid.as_bytes())
            .map_err(|error| GfError::Execution(error.to_string()))?;
        event_kinds.push(event.event_kind.as_str());
        recorded_at.push(event.recorded_at_micros);
        event_versions.push(event.contract_version);
        subject_kinds.push(row.subject_kind.as_str());
        roles.push(row.role.as_str());
        ordinals.push(row.ordinal);
        lineage_versions.push(row.contract_version);
    }
    let mut metadata = HashMap::from([
        (
            "graphforge.contract.id".to_owned(),
            "provenance_history".to_owned(),
        ),
        ("graphforge.contract.version".to_owned(), "1".to_owned()),
    ]);
    if let Some(next) = next {
        metadata.insert(
            "graphforge.next_page_token".to_owned(),
            next.as_str().to_owned(),
        );
    }
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("provenance_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("operation_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("event_kind", DataType::Utf8, false),
            Field::new("actor_uuid", DataType::FixedSizeBinary(16), true),
            Field::new(
                "recorded_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("contract_version", DataType::UInt32, false),
            Field::new("lineage_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("subject_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("subject_kind", DataType::Utf8, false),
            Field::new("role", DataType::Utf8, false),
            Field::new("ordinal", DataType::UInt32, false),
            Field::new("lineage_contract_version", DataType::UInt32, false),
        ],
        metadata,
    ));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(provenance_ids.finish()) as ArrayRef,
            Arc::new(operation_ids.finish()),
            Arc::new(StringArray::from(event_kinds)),
            Arc::new(actor_ids.finish()),
            Arc::new(TimestampMicrosecondArray::from(recorded_at).with_timezone("UTC")),
            Arc::new(UInt32Array::from(event_versions)),
            Arc::new(lineage_ids.finish()),
            Arc::new(subject_ids.finish()),
            Arc::new(StringArray::from(subject_kinds)),
            Arc::new(StringArray::from(roles)),
            Arc::new(UInt32Array::from(ordinals)),
            Arc::new(UInt32Array::from(lineage_versions)),
        ],
    )
    .map_err(|error| GfError::Execution(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapabilityId, EnableCapabilityRequest, GraphForge, OperationId, WriteContext};
    use graphforge_exec::{MutationEffect, MutationSubject};
    use std::collections::HashMap;

    #[test]
    fn receipt_translation_is_deterministic_and_role_ordered() {
        let receipt = MutationReceipt {
            effects: vec![MutationEffect {
                kind: MutationKind::CreateEdge,
                inputs: vec![
                    MutationSubject {
                        uuid: Uuid::from_u128(2).into_bytes(),
                        kind: MutationSubjectKind::Node,
                    },
                    MutationSubject {
                        uuid: Uuid::from_u128(3).into_bytes(),
                        kind: MutationSubjectKind::Node,
                    },
                ],
                outputs: vec![MutationSubject {
                    uuid: Uuid::from_u128(4).into_bytes(),
                    kind: MutationSubjectKind::Edge,
                }],
            }],
        };
        let operation = Uuid::from_u128(1);
        let first = ledger_from_receipt(&receipt, operation, None, 123).unwrap();
        let second = ledger_from_receipt(&receipt, operation, None, 123).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.lineage[0].role, LineageRole::Input);
        assert_eq!(first.lineage[2].role, LineageRole::Output);
    }

    #[test]
    fn enabled_project_commits_graph_and_provenance_in_one_generation() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(1_234_567));
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::from_u128(100)),
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Provenance,
                capability_version: 1,
            })
            .unwrap();

        let node = graph.add_node("Person", &HashMap::new()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(root.path()).unwrap();
        assert!(
            generation
                .participant_snapshot("graph", graphforge_storage::GRAPH_FILES_FAMILY)
                .unwrap()
                .is_some()
        );
        let ledger = read_ledger(&generation).unwrap();
        assert_eq!(ledger.events.len(), 1);
        assert_eq!(ledger.events[0].event_kind, EventKind::CreateNode);
        assert_eq!(ledger.events[0].recorded_at_micros, 1_234_567);
        assert_eq!(ledger.lineage.len(), 1);
        assert_eq!(ledger.lineage[0].subject_uuid, node.uuid);
        assert_eq!(ledger.lineage[0].role, LineageRole::Output);
        let event = graph
            .provenance_event(ledger.events[0].provenance_uuid, None)
            .unwrap();
        assert_eq!(event.stats.rows_produced, 1);
        let history = graph
            .list_provenance_history(ProvenanceHistoryRequest {
                subject_uuid: Some(node.uuid),
                operation_uuid: None,
                page: PageRequest::default(),
            })
            .unwrap();
        assert_eq!(history.stats.rows_produced, 1);
        assert_eq!(
            history
                .schema
                .metadata()
                .get("graphforge.contract.id")
                .map(String::as_str),
            Some("provenance_history")
        );

        drop(graph);
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(
            reopened
                .resolve_node_selector(&graphforge_core::NodeSelector::Uuid(node.uuid))
                .unwrap(),
            node.uuid
        );
    }

    #[test]
    fn supported_mutation_matrix_records_closed_kinds_and_explicit_noops() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(9_876_543));
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::from_u128(200)),
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Provenance,
                capability_version: 1,
            })
            .unwrap();

        let constructed_a = graph.add_node("Person", &HashMap::new()).unwrap();
        let constructed_b = graph.add_node("Person", &HashMap::new()).unwrap();
        graph
            .add_edge(&constructed_a, "KNOWS", &constructed_b, &HashMap::new())
            .unwrap();
        graph.execute("CREATE (:Person {name: 'cypher'})").unwrap();
        graph
            .execute("MERGE (:Person {name: 'created-by-merge'})")
            .unwrap();
        graph.execute("MERGE (:Person {name: 'cypher'})").unwrap();
        graph
            .execute("MATCH (n:Person {name: 'cypher'}) SET n.score = 1")
            .unwrap();

        let before_noop =
            read_ledger(&graphforge_storage::resolve_project_generation(root.path()).unwrap())
                .unwrap();
        graph
            .execute("MATCH (n:Person {name: 'cypher'}) REMOVE n.missing")
            .unwrap();
        let after_noop =
            read_ledger(&graphforge_storage::resolve_project_generation(root.path()).unwrap())
                .unwrap();
        assert_eq!(after_noop, before_noop);

        graph
            .execute("MATCH (n:Person {name: 'cypher'}) REMOVE n.score")
            .unwrap();
        graph
            .execute("MATCH (n:Person {name: 'cypher'}) SET n:Selected")
            .unwrap();
        graph
            .execute("MATCH (n:Person {name: 'cypher'}) REMOVE n:Selected")
            .unwrap();
        graph.execute("MATCH ()-[r:KNOWS]->() DELETE r").unwrap();
        graph
            .execute("MATCH (n:Person {name: 'cypher'}) DETACH DELETE n")
            .unwrap();

        let ledger =
            read_ledger(&graphforge_storage::resolve_project_generation(root.path()).unwrap())
                .unwrap();
        let kinds = ledger
            .events
            .iter()
            .map(|event| event.event_kind)
            .collect::<Vec<_>>();
        for expected in [
            EventKind::CreateNode,
            EventKind::CreateEdge,
            EventKind::MergeCreate,
            EventKind::MergeMatchedNoop,
            EventKind::SetProperty,
            EventKind::RemoveProperty,
            EventKind::AddLabel,
            EventKind::RemoveLabel,
            EventKind::Delete,
            EventKind::DetachDelete,
        ] {
            assert!(kinds.contains(&expected), "missing {expected:?}: {kinds:?}");
        }
    }

    #[test]
    fn operation_retry_is_idempotent_and_lineage_conflict_writes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::from_u128(300)),
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Provenance,
                capability_version: 1,
            })
            .unwrap();
        let first_node = graph.add_node("Person", &HashMap::new()).unwrap();
        let second_node = graph.add_node("Person", &HashMap::new()).unwrap();
        let operation_uuid = Uuid::from_u128(301);
        let receipt = |subject_uuid: Uuid| MutationReceipt {
            effects: vec![MutationEffect {
                kind: MutationKind::SetProperty,
                inputs: vec![],
                outputs: vec![MutationSubject {
                    uuid: subject_uuid.into_bytes(),
                    kind: MutationSubjectKind::Node,
                }],
            }],
        };

        graph
            .publish_graph_mutation_with_context(
                &receipt(first_node.uuid),
                operation_uuid,
                None,
                123,
            )
            .unwrap();
        let committed = graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        graph
            .publish_graph_mutation_with_context(
                &receipt(first_node.uuid),
                operation_uuid,
                None,
                123,
            )
            .unwrap();
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            committed
        );

        let error = graph
            .publish_graph_mutation_with_context(
                &receipt(second_node.uuid),
                operation_uuid,
                None,
                123,
            )
            .unwrap_err();
        assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert_eq!(
            graphforge_storage::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            committed
        );
    }

    #[test]
    fn corrupt_provenance_does_not_block_graph_only_reopen_or_reads() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::from_u128(400)),
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Provenance,
                capability_version: 1,
            })
            .unwrap();
        graph.add_node("Person", &HashMap::new()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(root.path()).unwrap();
        let events_path = generation.participant_path("provenance", "events").unwrap();
        drop(generation);
        drop(graph);
        std::fs::write(events_path, b"corrupt provenance only").unwrap();

        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(
            reopened
                .execute("MATCH (n:Person) RETURN count(n) AS total")
                .unwrap()
                .batches[0]
                .num_rows(),
            1
        );
        let error = reopened
            .list_provenance_history(ProvenanceHistoryRequest::default())
            .unwrap_err();
        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
    }
}
