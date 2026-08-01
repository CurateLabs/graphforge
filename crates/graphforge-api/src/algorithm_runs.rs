//! Durable, knowledge-neutral orchestration around M18 descriptor dispatch.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};
use graphforge_knowledge::{
    ALGORITHM_RUN_EVENT_SCHEMA, ALGORITHM_RUN_SCHEMA, AlgorithmRun, AlgorithmRunEvent,
    AlgorithmRunLedger, AlgorithmRunState, schema_registry,
};
use graphforge_provenance::{
    EventKind, LineageRecord, LineageRole, ProvenanceEvent, ProvenanceLedger, SubjectKind,
};
use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectStageOutcome,
    ResolvedProjectGeneration,
};
use sha2::{Digest, Sha256};
use uuid::{Uuid, Version};

use crate::knowledge::{
    assertion_result, concat_or_empty, knowledge_error, participant, provenance_error,
    read_parquet, require_participant_contract, snapshot_to_participant, with_next_token,
};
use crate::{
    Algorithm, CancellationToken, GraphForge, InvocationDescriptor, InvocationError, PageRequest,
    PageToken, WriteContext,
};

/// Frozen public algorithm identifier.
pub type AlgorithmId = Algorithm;

/// Frozen request for one recorded M18 invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordedAlgorithmRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 run identity.
    pub run_uuid: Uuid,
    /// Canonical neutral M18 descriptor.
    pub descriptor: InvocationDescriptor,
    /// Optional cooperative cancellation state.
    pub cancellation: Option<CancellationToken>,
}

/// Frozen filter and page request for algorithm runs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListAlgorithmRunsRequest {
    /// Optional exact public algorithm filter.
    pub algorithm: Option<AlgorithmId>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Result returned by the first successful recorded dispatch.
#[derive(Debug)]
pub struct RecordedAlgorithmResult {
    /// Durable run identity.
    pub run_uuid: Uuid,
    /// Canonical public Arrow result.
    pub result: graphforge_exec::ExecutionResult,
}

impl GraphForge {
    /// Publish a start, dispatch the unchanged M18 path, then publish one terminal event.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn invoke_recorded(
        &self,
        request: RecordedAlgorithmRequest,
    ) -> Result<RecordedAlgorithmResult, GfError> {
        self.invoke_recorded_on(self, &request)
    }

    /// Persist a run on this project while executing its neutral descriptor on
    /// a graph-only projection.
    pub(crate) fn invoke_recorded_on(
        &self,
        execution_graph: &GraphForge,
        request: &RecordedAlgorithmRequest,
    ) -> Result<RecordedAlgorithmResult, GfError> {
        begin_recorded_run(self, request)?;
        pause_after_start_for_subprocess_test();

        if request
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            publish_terminal(
                self,
                request.run_uuid,
                AlgorithmRunState::Cancelled,
                None,
                Some("GF_CANCELLED".into()),
                request.context.actor_uuid,
            )?;
            return Err(api_error(
                ApiErrorCode::Cancelled,
                "recorded algorithm invocation was cancelled",
            ));
        }

        match execution_graph.invoke_descriptor(&request.descriptor) {
            Ok(batch) => {
                let digest =
                    crate::canonical_arrow::result_fingerprint(std::slice::from_ref(&batch))
                        .map_err(|error| {
                            api_error(ApiErrorCode::SchemaMismatch, error.to_string())
                        })?;
                publish_terminal(
                    self,
                    request.run_uuid,
                    AlgorithmRunState::Completed,
                    Some(digest),
                    None,
                    request.context.actor_uuid,
                )?;
                Ok(RecordedAlgorithmResult {
                    run_uuid: request.run_uuid,
                    result: assertion_result(batch),
                })
            }
            Err(error) => {
                let code = error.code().to_owned();
                let state = if code == "GF_CANCELLED" {
                    AlgorithmRunState::Cancelled
                } else {
                    AlgorithmRunState::Failed
                };
                publish_terminal(
                    self,
                    request.run_uuid,
                    state,
                    None,
                    Some(code),
                    request.context.actor_uuid,
                )?;
                Err(invocation_error(error))
            }
        }
    }

    /// Return one immutable run identity row.
    pub fn algorithm_run(
        &self,
        run_uuid: Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(run_uuid, "run_uuid")?;
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        let index = ledger
            .runs
            .iter()
            .position(|row| row.run_uuid == run_uuid)
            .ok_or_else(|| api_error(ApiErrorCode::NotFound, "algorithm run was not found"))?;
        Ok(assertion_result(
            ledger.run_batch().map_err(knowledge_error)?.slice(index, 1),
        ))
    }

    /// List immutable run identities in `(started_at, run_uuid)` order.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn list_algorithm_runs(
        &self,
        request: ListAlgorithmRunsRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        let batch = ledger.run_batch().map_err(knowledge_error)?;
        let wanted = request.algorithm.map(algorithm_id);
        let rows = ledger
            .runs
            .iter()
            .enumerate()
            .filter(|(_, row)| wanted.as_ref().is_none_or(|name| &row.algorithm == name))
            .map(|(index, _)| batch.slice(index, 1))
            .collect::<Vec<_>>();
        page_rows(
            &rows,
            &ALGORITHM_RUN_SCHEMA,
            generation.generation_uuid(),
            &request.page,
        )
    }

    /// List one run's events in `(recorded_at, event_uuid)` order.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-m20-api/1 freezes owned request structs"
    )]
    pub fn algorithm_run_events(
        &self,
        run_uuid: Uuid,
        page: PageRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(run_uuid, "run_uuid")?;
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        if ledger.run(run_uuid).is_none() {
            return Err(api_error(
                ApiErrorCode::NotFound,
                "algorithm run was not found",
            ));
        }
        let batch = ledger.event_batch().map_err(knowledge_error)?;
        let rows = ledger
            .events
            .iter()
            .enumerate()
            .filter(|(_, row)| row.run_uuid == run_uuid)
            .map(|(index, _)| batch.slice(index, 1))
            .collect::<Vec<_>>();
        page_rows(
            &rows,
            &ALGORITHM_RUN_EVENT_SCHEMA,
            generation.generation_uuid(),
            &page,
        )
    }

    /// Reconcile every lone start to one deterministic interrupted event.
    pub(crate) fn reconcile_algorithm_runs(&self) -> Result<(), GfError> {
        for attempt in 0..2 {
            match self.reconcile_algorithm_runs_once() {
                Err(GfError::Project {
                    code: ProjectErrorCode::TransactionConflict,
                    ..
                }) if attempt == 0 => {}
                result => return result,
            }
        }
        unreachable!("bounded reconciliation retry returns from each branch")
    }

    fn reconcile_algorithm_runs_once(&self) -> Result<(), GfError> {
        let parent = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        if parent.require_capability("knowledge", 1).is_err()
            || parent.require_capability("provenance", 1).is_err()
        {
            return Ok(());
        }
        let ledger = read_ledger(&parent)?;
        let pending = ledger
            .runs
            .iter()
            .filter(|run| ledger.terminal_event(run.run_uuid).is_none())
            .cloned()
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }
        let mut staged_events = Vec::with_capacity(pending.len());
        let mut staged_provenance = ProvenanceLedger::default();
        for run in pending {
            let operation_uuid = terminal_uuid(
                run.run_uuid,
                AlgorithmRunState::Interrupted,
                None,
                Some("GF_INTERRUPTED"),
            );
            let provenance = ProvenanceEvent::new(
                operation_uuid,
                EventKind::RecordAlgorithmRun,
                None,
                run.started_at_micros.saturating_add(1),
            )
            .map_err(provenance_error)?;
            staged_events.push(
                AlgorithmRunEvent::new(
                    operation_uuid,
                    run.run_uuid,
                    AlgorithmRunState::Interrupted,
                    None,
                    Some("GF_INTERRUPTED".into()),
                    run.started_at_micros.saturating_add(1),
                    provenance.provenance_uuid,
                )
                .map_err(knowledge_error)?,
            );
            staged_provenance = staged_provenance
                .merge(&run_provenance_ledger(provenance, run.run_uuid)?)
                .map_err(provenance_error)?;
        }
        let mut events = ledger.events.clone();
        events.extend(staged_events);
        let updated =
            AlgorithmRunLedger::new(ledger.runs.clone(), events).map_err(knowledge_error)?;
        let provenance = crate::provenance::read_ledger(&parent)?
            .merge(&staged_provenance)
            .map_err(provenance_error)?;
        let operation_uuid = recovery_operation_uuid(&updated);
        publish(
            self,
            &parent,
            operation_uuid,
            &updated,
            &provenance,
            b"reconcile",
        )?;
        Ok(())
    }
}

#[cfg(test)]
fn pause_after_start_for_subprocess_test() {
    if std::env::var_os("GF_TEST_ALGORITHM_RUN_ROOT").is_none() {
        return;
    }
    let Ok(marker) = std::env::var("GF_TEST_ALGORITHM_RUN_STARTED_MARKER") else {
        return;
    };
    std::fs::write(marker, b"committed").expect("write algorithm-run start marker");
    loop {
        std::thread::park();
    }
}

#[cfg(not(test))]
const fn pause_after_start_for_subprocess_test() {}

fn begin_recorded_run(
    graph: &GraphForge,
    request: &RecordedAlgorithmRequest,
) -> Result<(), GfError> {
    validate_request(request)?;
    let parent =
        graphforge_storage::resolve_project_generation(graph.resolved_generation.container_root())?;
    let ledger = read_ledger(&parent)?;
    if let Some(existing) = ledger.run(request.run_uuid) {
        if existing.descriptor != request.descriptor.canonical_bytes() {
            return Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "run UUID was reused for a different descriptor".into(),
            });
        }
        return Err(existing_lifecycle_error(
            ledger.terminal_event(request.run_uuid),
        ));
    }

    let started_at = (graph.clock.lock().expect("clock lock poisoned"))()?;
    let provenance = ProvenanceEvent::new(
        request.context.operation_uuid.0,
        EventKind::RecordAlgorithmRun,
        request.context.actor_uuid,
        started_at,
    )
    .map_err(provenance_error)?;
    let run = AlgorithmRun::new(
        request.run_uuid,
        algorithm_id(request.descriptor.algorithm()),
        1,
        request.descriptor.descriptor_version(),
        request.descriptor.canonical_bytes().to_vec(),
        *request.descriptor.projection_fingerprint(),
        provenance.provenance_uuid,
        started_at,
    )
    .map_err(knowledge_error)?;
    let start = AlgorithmRunEvent::new(
        request.context.operation_uuid.0,
        request.run_uuid,
        AlgorithmRunState::Started,
        None,
        None,
        started_at,
        provenance.provenance_uuid,
    )
    .map_err(knowledge_error)?;
    let updated = ledger
        .merge(&AlgorithmRunLedger::new(vec![run], vec![start]).map_err(knowledge_error)?)
        .map_err(knowledge_error)?;
    let merged_provenance = merge_run_provenance(&parent, provenance, request.run_uuid)?;
    publish(
        graph,
        &parent,
        request.context.operation_uuid.0,
        &updated,
        &merged_provenance,
        b"start",
    )
}

fn validate_request(request: &RecordedAlgorithmRequest) -> Result<(), GfError> {
    require_uuid(request.context.operation_uuid.0, "operation_uuid")?;
    if let Some(actor) = request.context.actor_uuid {
        require_uuid(actor, "actor_uuid")?;
    }
    require_uuid(request.run_uuid, "run_uuid")?;
    if request.run_uuid.get_version() != Some(Version::SortRand) {
        return Err(GfError::Validation("run_uuid must be UUIDv7".into()));
    }
    Ok(())
}

fn publish_terminal(
    graph: &GraphForge,
    run_uuid: Uuid,
    state: AlgorithmRunState,
    result_fingerprint: Option<[u8; 32]>,
    error_code: Option<String>,
    actor_uuid: Option<Uuid>,
) -> Result<(), GfError> {
    let parent =
        graphforge_storage::resolve_project_generation(graph.resolved_generation.container_root())?;
    let ledger = read_ledger(&parent)?;
    if ledger.terminal_event(run_uuid).is_some() {
        return Ok(());
    }
    let started_at = ledger
        .run(run_uuid)
        .ok_or_else(|| api_error(ApiErrorCode::NotFound, "algorithm run was not found"))?
        .started_at_micros;
    let recorded_at =
        (graph.clock.lock().expect("clock lock poisoned"))()?.max(started_at.saturating_add(1));
    let operation_uuid = terminal_uuid(run_uuid, state, result_fingerprint, error_code.as_deref());
    let provenance = ProvenanceEvent::new(
        operation_uuid,
        EventKind::RecordAlgorithmRun,
        actor_uuid,
        recorded_at,
    )
    .map_err(provenance_error)?;
    let event = AlgorithmRunEvent::new(
        operation_uuid,
        run_uuid,
        state,
        result_fingerprint,
        error_code,
        recorded_at,
        provenance.provenance_uuid,
    )
    .map_err(knowledge_error)?;
    let mut events = ledger.events.clone();
    events.push(event);
    let updated = AlgorithmRunLedger::new(ledger.runs.clone(), events).map_err(knowledge_error)?;
    let merged_provenance = merge_run_provenance(&parent, provenance, run_uuid)?;
    publish(
        graph,
        &parent,
        operation_uuid,
        &updated,
        &merged_provenance,
        b"terminal",
    )?;
    Ok(())
}

fn merge_run_provenance(
    parent: &ResolvedProjectGeneration,
    event: ProvenanceEvent,
    run_uuid: Uuid,
) -> Result<ProvenanceLedger, GfError> {
    crate::provenance::read_ledger(parent)?
        .merge(&run_provenance_ledger(event, run_uuid)?)
        .map_err(provenance_error)
}

fn run_provenance_ledger(
    event: ProvenanceEvent,
    run_uuid: Uuid,
) -> Result<ProvenanceLedger, GfError> {
    let lineage = LineageRecord::new(
        event.provenance_uuid,
        run_uuid,
        SubjectKind::AlgorithmRun,
        LineageRole::Output,
        0,
    )
    .map_err(provenance_error)?;
    ProvenanceLedger::new(vec![event], vec![lineage]).map_err(provenance_error)
}

fn publish(
    graph: &GraphForge,
    parent: &ResolvedProjectGeneration,
    transaction_uuid: Uuid,
    ledger: &AlgorithmRunLedger,
    provenance: &ProvenanceLedger,
    phase: &[u8],
) -> Result<(), GfError> {
    parent.validate_complete_participant_inventory()?;
    parent.require_capability("knowledge", 1)?;
    parent.require_capability("provenance", 1)?;
    let expected_parent = parent.generation_uuid();
    let current = *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned");
    if current != expected_parent {
        return Err(GfError::Project {
            code: ProjectErrorCode::TransactionConflict,
            message: "project generation changed before algorithm-run publication".into(),
        });
    }
    let participants = publication_participants(parent, ledger, provenance)?;
    let request = ProjectGenerationRequest {
        transaction_uuid,
        generation_uuid: generation_uuid(transaction_uuid, phase, &participants),
        capabilities: parent
            .capabilities()
            .into_iter()
            .map(|value| ProjectCapability {
                capability_id: value.capability_id,
                capability_version: value.capability_version,
            })
            .collect(),
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
                        return Err(GfError::Project {
                            code: ProjectErrorCode::TransactionConflict,
                            message: "project generation changed before algorithm-run publication"
                                .into(),
                        });
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

fn publication_participants(
    parent: &ResolvedProjectGeneration,
    ledger: &AlgorithmRunLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "algorithm_runs" | "algorithm_run_events"
                )
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    let registry = schema_registry();
    let runs = registry
        .iter()
        .find(|entry| entry.record_family == "algorithm_runs")
        .expect("algorithm run registry");
    let events = registry
        .iter()
        .find(|entry| entry.record_family == "algorithm_run_events")
        .expect("algorithm run event registry");
    participants.push(participant(
        runs,
        &ledger.run_batch().map_err(knowledge_error)?,
    )?);
    participants.push(participant(
        events,
        &ledger.event_batch().map_err(knowledge_error)?,
    )?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(crate) fn empty_participants() -> Result<Vec<ProjectParticipant>, GfError> {
    let ledger = AlgorithmRunLedger::default();
    let registry = schema_registry();
    let runs = registry
        .iter()
        .find(|entry| entry.record_family == "algorithm_runs")
        .expect("algorithm run registry");
    let events = registry
        .iter()
        .find(|entry| entry.record_family == "algorithm_run_events")
        .expect("algorithm run event registry");
    Ok(vec![
        participant(runs, &ledger.run_batch().map_err(knowledge_error)?)?,
        participant(events, &ledger.event_batch().map_err(knowledge_error)?)?,
    ])
}

pub(crate) fn read_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<AlgorithmRunLedger, GfError> {
    generation.require_capability("knowledge", 1)?;
    let runs = generation.participant_snapshot("knowledge", "algorithm_runs")?;
    let events = generation.participant_snapshot("knowledge", "algorithm_run_events")?;
    match (runs, events) {
        (None, None) => AlgorithmRunLedger::new(Vec::new(), Vec::new()).map_err(knowledge_error),
        (Some(runs), Some(events)) => {
            require_participant_contract(&runs, "algorithm_runs")?;
            require_participant_contract(&events, "algorithm_run_events")?;
            let run_batches = if runs.row_count == 0 {
                vec![RecordBatch::new_empty(Arc::clone(&ALGORITHM_RUN_SCHEMA))]
            } else {
                read_parquet(&runs.bytes)?
            };
            let event_batches = if events.row_count == 0 {
                vec![RecordBatch::new_empty(Arc::clone(
                    &ALGORITHM_RUN_EVENT_SCHEMA,
                ))]
            } else {
                read_parquet(&events.bytes)?
            };
            AlgorithmRunLedger::from_batches(&run_batches, &event_batches).map_err(knowledge_error)
        }
        _ => Err(api_error(
            ApiErrorCode::SchemaMismatch,
            "algorithm-run participant set is incomplete",
        )),
    }
}

fn page_rows(
    rows: &[RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
    generation_uuid: Uuid,
    page: &PageRequest,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let (start, end) = crate::paging::validate_page(page, generation_uuid, rows.len())?;
    let batch = concat_or_empty(&rows[start..end], schema)?;
    let next = (end < rows.len()).then(|| PageToken::new(generation_uuid, end));
    Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
}

fn algorithm_id(algorithm: Algorithm) -> String {
    format!("{}.{}", algorithm.verb().as_str(), algorithm.as_str())
}

fn terminal_uuid(
    run_uuid: Uuid,
    state: AlgorithmRunState,
    result_fingerprint: Option<[u8; 32]>,
    error_code: Option<&str>,
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-algorithm-run-terminal/1");
    hasher.update(run_uuid.as_bytes());
    hasher.update(state.as_str().as_bytes());
    hasher.update(result_fingerprint.unwrap_or_default());
    hasher.update(error_code.unwrap_or_default().as_bytes());
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn recovery_operation_uuid(ledger: &AlgorithmRunLedger) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-algorithm-run-recovery/1");
    for event in &ledger.events {
        if event.state == AlgorithmRunState::Interrupted {
            hasher.update(event.event_uuid.as_bytes());
        }
    }
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn generation_uuid(
    transaction_uuid: Uuid,
    phase: &[u8],
    participants: &[ProjectParticipant],
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-algorithm-run-generation/1");
    hasher.update(transaction_uuid.as_bytes());
    hasher.update(phase);
    for participant in participants {
        hasher.update(participant.capability_id.as_bytes());
        hasher.update([0]);
        hasher.update(participant.record_family_id.as_bytes());
        hasher.update([0]);
        hasher.update(Sha256::digest(&participant.bytes));
    }
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn existing_lifecycle_error(terminal: Option<&AlgorithmRunEvent>) -> GfError {
    match terminal {
        Some(event) if event.state == AlgorithmRunState::Completed => api_error(
            ApiErrorCode::ResultNotRetained,
            "completed algorithm result rows were not retained",
        ),
        Some(event) => GfError::Execution(format!(
            "recorded algorithm run already terminated with {} ({})",
            event.state.as_str(),
            event.error_code.as_deref().unwrap_or("GF_EXECUTION")
        )),
        None => GfError::Execution(
            "recorded algorithm run is already started and will not be dispatched twice".into(),
        ),
    }
}

fn invocation_error(error: InvocationError) -> GfError {
    match error {
        InvocationError::Graph(error) => error,
        InvocationError::SchemaMismatch => api_error(
            ApiErrorCode::SchemaMismatch,
            "algorithm result schema mismatch",
        ),
        InvocationError::Descriptor(error) => GfError::Validation(error.to_string()),
        InvocationError::ProjectionChanged => {
            GfError::Execution("algorithm projection changed before dispatch".into())
        }
    }
}

fn require_uuid(value: Uuid, name: &'static str) -> Result<(), GfError> {
    if value.is_nil() {
        Err(GfError::Validation(format!("{name} must not be nil")))
    } else {
        Ok(())
    }
}

fn api_error(code: ApiErrorCode, message: impl Into<String>) -> GfError {
    GfError::Api {
        code,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use arrow::array::{Array, StringArray};

    use super::*;
    use crate::{CapabilityId, EnableCapabilityRequest, OperationId, RankAlgorithm, RankOptions};

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
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

    fn recorded_fixture(root: &tempfile::TempDir) -> (GraphForge, InvocationDescriptor) {
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        enable(&graph, CapabilityId::Provenance, 1);
        graph.add_node("Person", &HashMap::new()).unwrap();
        enable(&graph, CapabilityId::Knowledge, 2);
        let descriptor = graph
            .prepare_rank_invocation(
                "Person",
                &RankOptions {
                    by: RankAlgorithm::Degree,
                    ..RankOptions::default()
                },
            )
            .unwrap();
        (graph, descriptor)
    }

    #[test]
    fn recorded_dispatch_commits_start_and_completed_before_return() {
        let root = tempfile::tempdir().unwrap();
        let (graph, descriptor) = recorded_fixture(&root);
        graph.set_clock_for_test(|| Ok(10));
        let request = RecordedAlgorithmRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(3)),
                actor_uuid: Some(uuid7(4)),
            },
            run_uuid: uuid7(5),
            descriptor,
            cancellation: None,
        };
        let direct = graph.invoke_descriptor(&request.descriptor).unwrap();
        let result = graph.invoke_recorded(request.clone()).unwrap();
        assert_eq!(result.run_uuid, uuid7(5));
        assert_eq!(result.result.stats.rows_produced, 1);
        assert_eq!(
            crate::canonical_arrow::result_fingerprint(&[direct]).unwrap(),
            crate::canonical_arrow::result_fingerprint(&result.result.batches).unwrap()
        );

        let events = graph
            .algorithm_run_events(uuid7(5), PageRequest::default())
            .unwrap();
        let states = events.batches[0]
            .column_by_name("state")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (0..states.len())
                .map(|index| states.value(index))
                .collect::<Vec<_>>(),
            vec!["started", "completed"]
        );
        let replay = graph.invoke_recorded(request).unwrap_err();
        assert_eq!(replay.code(), "GF_RESULT_NOT_RETAINED");
        let conflicting = graph
            .prepare_rank_invocation(
                "Person",
                &RankOptions {
                    by: RankAlgorithm::PageRank,
                    ..RankOptions::default()
                },
            )
            .unwrap();
        let error = graph
            .invoke_recorded(RecordedAlgorithmRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(6)),
                    actor_uuid: None,
                },
                run_uuid: uuid7(5),
                descriptor: conflicting,
                cancellation: None,
            })
            .unwrap_err();
        assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert_eq!(
            graph
                .algorithm_run_events(uuid7(5), PageRequest::default())
                .unwrap()
                .stats
                .rows_produced,
            2
        );

        drop(graph);
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(
            reopened
                .algorithm_run(uuid7(5), None)
                .unwrap()
                .stats
                .rows_produced,
            1
        );
        assert_eq!(
            reopened
                .algorithm_run_events(uuid7(5), PageRequest::default())
                .unwrap()
                .stats
                .rows_produced,
            2
        );
    }

    #[test]
    fn pre_dispatch_cancellation_is_a_durable_terminal() {
        let root = tempfile::tempdir().unwrap();
        let (graph, descriptor) = recorded_fixture(&root);
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = graph
            .invoke_recorded(RecordedAlgorithmRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(3)),
                    actor_uuid: None,
                },
                run_uuid: uuid7(4),
                descriptor,
                cancellation: Some(cancellation),
            })
            .unwrap_err();
        assert_eq!(error.code(), "GF_CANCELLED");
        let events = graph
            .algorithm_run_events(uuid7(4), PageRequest::default())
            .unwrap();
        let states = events.batches[0]
            .column_by_name("state")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(states.value(1), "cancelled");
    }

    #[test]
    fn dispatch_failure_is_a_durable_terminal() {
        let root = tempfile::tempdir().unwrap();
        let (graph, descriptor) = recorded_fixture(&root);
        graph.add_node("Person", &HashMap::new()).unwrap();
        let error = graph
            .invoke_recorded(RecordedAlgorithmRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(3)),
                    actor_uuid: None,
                },
                run_uuid: uuid7(4),
                descriptor,
                cancellation: None,
            })
            .unwrap_err();
        assert_eq!(error.code(), "GF_EXECUTION");
        let events = graph
            .algorithm_run_events(uuid7(4), PageRequest::default())
            .unwrap();
        let states = events.batches[0]
            .column_by_name("state")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let errors = events.batches[0]
            .column_by_name("error_code")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(states.value(1), "failed");
        assert_eq!(errors.value(1), "GF_PROJECTION_CHANGED");
    }

    #[test]
    fn reopen_reconciles_one_lone_start_exactly_once() {
        let root = tempfile::tempdir().unwrap();
        let (graph, descriptor) = recorded_fixture(&root);
        let parent = graphforge_storage::resolve_project_generation(root.path()).unwrap();
        let operation_uuid = uuid7(3);
        let provenance =
            ProvenanceEvent::new(operation_uuid, EventKind::RecordAlgorithmRun, None, 10).unwrap();
        let run = AlgorithmRun::new(
            uuid7(4),
            algorithm_id(descriptor.algorithm()),
            1,
            descriptor.descriptor_version(),
            descriptor.canonical_bytes().to_vec(),
            *descriptor.projection_fingerprint(),
            provenance.provenance_uuid,
            10,
        )
        .unwrap();
        let start = AlgorithmRunEvent::new(
            operation_uuid,
            uuid7(4),
            AlgorithmRunState::Started,
            None,
            None,
            10,
            provenance.provenance_uuid,
        )
        .unwrap();
        publish(
            &graph,
            &parent,
            operation_uuid,
            &AlgorithmRunLedger::new(vec![run], vec![start]).unwrap(),
            &merge_run_provenance(&parent, provenance, uuid7(4)).unwrap(),
            b"start",
        )
        .unwrap();
        drop(graph);

        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(
            reopened
                .algorithm_run_events(uuid7(4), PageRequest::default())
                .unwrap()
                .stats
                .rows_produced,
            2
        );
        drop(reopened);
        let reopened_again = GraphForge::new(root.path().to_str()).unwrap();
        assert_eq!(
            reopened_again
                .algorithm_run_events(uuid7(4), PageRequest::default())
                .unwrap()
                .stats
                .rows_produced,
            2
        );
    }

    #[test]
    fn subprocess_recorded_run_harness() {
        let Ok(root) = std::env::var("GF_TEST_ALGORITHM_RUN_ROOT") else {
            return;
        };
        let graph = GraphForge::new(Some(root.as_str())).unwrap();
        let descriptor = graph
            .prepare_rank_invocation(
                "Person",
                &RankOptions {
                    by: RankAlgorithm::Degree,
                    ..RankOptions::default()
                },
            )
            .unwrap();
        let _ = graph.invoke_recorded(RecordedAlgorithmRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(8)),
                actor_uuid: None,
            },
            run_uuid: uuid7(9),
            descriptor,
            cancellation: None,
        });
    }

    #[test]
    fn subprocess_kill_reopens_as_exactly_one_interrupted_event() {
        let root = tempfile::tempdir().unwrap();
        let marker_dir = tempfile::tempdir().unwrap();
        let marker = marker_dir.path().join("started");
        let (graph, _) = recorded_fixture(&root);
        drop(graph);

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "algorithm_runs::tests::subprocess_recorded_run_harness",
                "--nocapture",
            ])
            .env("GF_TEST_ALGORITHM_RUN_ROOT", root.path())
            .env("GF_TEST_ALGORITHM_RUN_STARTED_MARKER", &marker)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !marker.exists() {
            assert!(
                Instant::now() < deadline,
                "child did not publish the start generation"
            );
            assert!(child.try_wait().unwrap().is_none(), "child exited early");
            std::thread::yield_now();
        }
        child.kill().unwrap();
        child.wait().unwrap();

        for _ in 0..2 {
            let reopened = GraphForge::new(root.path().to_str()).unwrap();
            let events = reopened
                .algorithm_run_events(uuid7(9), PageRequest::default())
                .unwrap();
            assert_eq!(events.stats.rows_produced, 2);
            let states = events.batches[0]
                .column_by_name("state")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(states.value(0), "started");
            assert_eq!(states.value(1), "interrupted");
        }
    }
}
