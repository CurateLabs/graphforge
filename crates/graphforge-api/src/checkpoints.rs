//! Public named-checkpoint lifecycle, read-only views, and logical comparisons.

mod diff;
use diff::logical_records;
mod view;

pub use view::CheckpointView;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{ExecutionResult, GraphForge, OperationId, PageRequest, PageToken};

/// Create-checkpoint request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRequest {
    /// Canonical checkpoint name.
    pub name: String,
    /// Optional bounded description.
    pub description: Option<String>,
    /// Idempotent operation identity.
    pub idempotency_key: OperationId,
    /// Optional actor identity.
    pub actor_uuid: Option<Uuid>,
}

/// Paginated checkpoint-list request.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ListCheckpointsRequest {
    /// Bounded page and cancellation controls.
    pub page: PageRequest,
}

/// Show-checkpoint request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowCheckpointRequest {
    /// Exact active checkpoint name.
    pub name: String,
}

/// Delete-checkpoint request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteCheckpointRequest {
    /// Exact active checkpoint name.
    pub name: String,
    /// Idempotent operation identity.
    pub idempotency_key: OperationId,
    /// Optional actor identity.
    pub actor_uuid: Option<Uuid>,
}

/// Complete-workspace checkpoint revert request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RevertCheckpointRequest {
    /// Exact active checkpoint name.
    pub name: String,
    /// Bounded human restoration reason.
    pub reason: String,
    /// Idempotent operation identity.
    pub idempotency_key: OperationId,
    /// Optional actor identity.
    pub actor_uuid: Option<Uuid>,
}

/// Non-mutating checkpoint-revert preview request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreviewRevertCheckpointRequest {
    /// Exact active checkpoint name.
    pub name: String,
}

/// Identities a caller must inspect before authorizing a checkpoint revert.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RevertCheckpointPreview {
    /// Stable checkpoint identity.
    pub checkpoint_uuid: Uuid,
    /// Complete generation pinned by the checkpoint.
    pub source_generation_uuid: Uuid,
    /// SHA-256 of the pinned generation's canonical manifest.
    pub source_manifest_sha256: String,
    /// Generation that is current at preview time.
    pub current_generation_uuid: Uuid,
}

/// Named checkpoint or the current committed generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointSelector {
    /// Active named checkpoint.
    Named(String),
    /// Current committed generation at call time.
    Current,
}

/// Participant domain selected for diffing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointDiffScope {
    /// All participant summary domains.
    Summary,
    /// Graph records.
    Graph,
    /// Ontology records.
    Ontology,
    /// Project configuration records.
    Configuration,
    /// Capability/workspace control records.
    Capabilities,
    /// Provenance and lineage records.
    Provenance,
    /// knowledge knowledge records.
    Knowledge,
    /// epistemic and valid-time records.
    Epistemic,
    /// Every registered domain.
    All,
}

/// Manifest-summary or logical-record detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointDiffDetail {
    /// Participant manifest summary.
    Summary,
    /// Owner-canonical logical records.
    Records,
}

/// Bounded checkpoint diff request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiffCheckpointsRequest {
    /// Earlier endpoint.
    pub from: CheckpointSelector,
    /// Later endpoint.
    pub to: CheckpointSelector,
    /// Selected participant domain.
    pub scope: CheckpointDiffScope,
    /// Summary or record detail.
    pub detail: CheckpointDiffDetail,
    /// Bounded page and cancellation controls.
    pub page: PageRequest,
}

impl GraphForge {
    /// Create a durable named checkpoint.
    pub fn checkpoint(&self, request: CheckpointRequest) -> Result<ExecutionResult, GfError> {
        let receipt = graphforge_storage::create_checkpoint_with_mode(
            self.resolved_generation.container_root(),
            &graphforge_storage::CheckpointCreateRequest {
                operation_uuid: request.idempotency_key.0,
                name: request.name,
                description: request.description,
                actor_uuid: request.actor_uuid,
            },
            self.lifecycle_mode,
        )?;
        Ok(receipt_result(&receipt))
    }

    /// List active checkpoints in canonical order.
    pub fn list_checkpoints(
        &self,
        request: ListCheckpointsRequest,
    ) -> Result<ExecutionResult, GfError> {
        let ListCheckpointsRequest { page } = request;
        cancellation(&page)?;
        let rows = graphforge_storage::list_checkpoints_with_mode(
            self.resolved_generation.container_root(),
            self.lifecycle_mode,
        )?;
        let snapshot = checkpoint_list_snapshot(&rows);
        let binding = request_binding("checkpoint-list", 0, 0);
        let cursors = rows
            .iter()
            .map(|row| page_cursor(&[row.name.as_bytes(), row.checkpoint_uuid.as_bytes()]))
            .collect::<Vec<_>>();
        let (start, end) = page_bounds("checkpoint-list", binding, &page, snapshot, &cursors)?;
        let next = (end < rows.len()).then(|| {
            PageToken::new_bound(
                "checkpoint-list",
                binding,
                snapshot,
                page.limit,
                end,
                cursors[end - 1],
            )
        });
        let result = checkpoint_rows(&rows[start..end], next.as_ref())?;
        // Re-check after the storage read so an AbortSignal that lands during a
        // short list still surfaces GF_CANCELLED instead of a late success.
        cancellation(&page)?;
        Ok(result)
    }

    /// Show the authoritative metadata for one active named checkpoint.
    pub fn show_checkpoint(
        &self,
        request: ShowCheckpointRequest,
    ) -> Result<ExecutionResult, GfError> {
        let ShowCheckpointRequest { name } = request;
        let (checkpoint, _) = graphforge_storage::open_checkpoint_generation_with_mode(
            self.resolved_generation.container_root(),
            &name,
            self.lifecycle_mode,
        )?;
        checkpoint_rows(std::slice::from_ref(&checkpoint), None)
    }

    /// Inspect checkpoint and current-generation identities without mutation.
    pub fn preview_revert_to_checkpoint(
        path: impl AsRef<std::path::Path>,
        request: PreviewRevertCheckpointRequest,
    ) -> Result<RevertCheckpointPreview, GfError> {
        let PreviewRevertCheckpointRequest { name } = request;
        let current = graphforge_storage::resolve_project_generation(path.as_ref())?;
        let (checkpoint, _) = graphforge_storage::open_checkpoint_generation(path.as_ref(), &name)?;
        Ok(RevertCheckpointPreview {
            checkpoint_uuid: checkpoint.checkpoint_uuid,
            source_generation_uuid: checkpoint.generation_uuid,
            source_manifest_sha256: checkpoint.generation_manifest_sha256,
            current_generation_uuid: current.generation_uuid(),
        })
    }

    /// Delete an active checkpoint reference.
    pub fn delete_checkpoint(
        &self,
        request: DeleteCheckpointRequest,
    ) -> Result<ExecutionResult, GfError> {
        let receipt = graphforge_storage::delete_checkpoint_with_mode(
            self.resolved_generation.container_root(),
            &graphforge_storage::CheckpointDeleteRequest {
                operation_uuid: request.idempotency_key.0,
                name: request.name,
                actor_uuid: request.actor_uuid,
            },
            self.lifecycle_mode,
        )?;
        Ok(receipt_result(&receipt))
    }

    /// Restore every authoritative participant from a checkpoint into a new generation.
    pub fn revert_to_checkpoint(
        &mut self,
        request: RevertCheckpointRequest,
    ) -> Result<ExecutionResult, GfError> {
        if self.read_only {
            return read_only();
        }
        let container_root = self.resolved_generation.container_root().to_path_buf();
        let clock = self.clock.lock().expect("clock lock poisoned").clone();
        let lifecycle_mode = self.lifecycle_mode;
        let write_options = self.write_options.clone();
        let resource_policy = self.resource_policy.clone();
        let select_clock = Arc::clone(&clock);
        let prepared = std::cell::RefCell::new(None);
        let (receipt, resolved) = graphforge_storage::revert_checkpoint_with_mode(
            &container_root,
            &graphforge_storage::CheckpointRevertRequest {
                operation_uuid: request.idempotency_key.0,
                name: request.name,
                reason: request.reason,
                actor_uuid: request.actor_uuid,
            },
            move || select_clock(),
            |generation| {
                validate_revert_source(generation, lifecycle_mode)?;
                prepared.replace(Some(GraphForge::open_resolved_with_options(
                    container_root.clone(),
                    generation.clone(),
                    true,
                    write_options.clone(),
                    resource_policy.clone(),
                    graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
                        generation.generation_uuid(),
                    ),
                )?));
                Ok(())
            },
            self.lifecycle_mode,
        )?;
        let result = receipt_result(&receipt);

        let mut reopened = prepared
            .into_inner()
            .expect("successful revert validation prepares the replacement facade");
        reopened.lifecycle_mode = lifecycle_mode;
        reopened.resolved_generation = resolved;
        *reopened
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") =
            reopened.resolved_generation.generation_uuid();
        reopened.read_only = false;
        reopened.project_open_recovery =
            graphforge_storage::ProjectOpenRecoveryEvidence::clean_open(
                reopened.resolved_generation.generation_uuid(),
            );
        let procedures = Arc::clone(&self.procedures);
        let provider_refresh_runtimes = Arc::clone(&self.provider_refresh_runtimes);
        let provider_find_runtimes = Arc::clone(&self.provider_find_runtimes);
        reopened.path.clone_from(&self.path);
        reopened.tempdir.clone_from(&self.tempdir);
        reopened.clock = std::sync::Mutex::new(clock);
        reopened.procedures = procedures;
        reopened.provider_refresh_runtimes = provider_refresh_runtimes;
        reopened.provider_find_runtimes = provider_find_runtimes;
        *self = reopened;
        Ok(result)
    }
}

fn validate_revert_source(
    generation: &graphforge_storage::ResolvedProjectGeneration,
    lifecycle_mode: graphforge_storage::filesystem_admission::ProjectLifecycleMode,
) -> Result<(), GfError> {
    generation.validate_complete_participant_inventory()?;
    let _workspace = crate::hydrate_graph_workspace(generation, true)?;
    let _ontology = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_ONTOLOGY_FAMILY,
        )?
        .map(|snapshot| graphforge_storage::WorkspaceOntology::from_canonical_json(&snapshot.bytes))
        .transpose()?;
    let _configuration = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_CONFIGURATION_FAMILY,
        )?
        .map(|snapshot| {
            graphforge_storage::WorkspaceConfiguration::from_canonical_json(&snapshot.bytes)
        })
        .transpose()?;
    let _records = logical_records(
        generation,
        CheckpointDiffScope::All,
        &PageRequest::default(),
        lifecycle_mode,
    )?;
    // Run each domain owner's decoder as well as the generic checkpoint adapters.
    // These readers enforce each ledger's schema and ledger-local invariants.
    let provenance = generation
        .capability("provenance")?
        .map(|_| crate::provenance::read_ledger(generation))
        .transpose()?;
    let mut knowledge = None;
    let mut confidence = None;
    let mut evidence = None;
    let mut algorithm_runs = None;
    if generation.capability("knowledge")?.is_some() {
        knowledge = Some(crate::knowledge::read_ledger(generation)?);
        confidence = Some(crate::knowledge::read_confidence_ledger(generation)?);
        evidence = Some(crate::knowledge::read_evidence_ledger(generation)?);
        algorithm_runs = Some(crate::algorithm_runs::read_ledger(generation)?);
    }
    let mut reasoning = None;
    let mut statuses = None;
    let mut supersessions = None;
    let mut hypotheses = None;
    if generation.capability("epistemic")?.is_some() {
        reasoning = Some(crate::knowledge::read_reasoning_ledger(generation)?);
        statuses = Some(crate::knowledge::read_status_ledger(generation)?);
        supersessions = Some(crate::knowledge::read_supersession_ledger(generation)?);
        hypotheses = Some(crate::hypotheses::read_ledger(generation)?);
    }
    let mut valid_time = None;
    if generation.capability("valid_time")?.is_some() {
        valid_time = Some(crate::valid_time::read_ledger(generation)?);
    }
    validate_composite_references(CompositeLedgers {
        provenance: provenance.as_ref(),
        knowledge: knowledge.as_ref(),
        confidence: confidence.as_ref(),
        evidence: evidence.as_ref(),
        reasoning: reasoning.as_ref(),
        statuses: statuses.as_ref(),
        supersessions: supersessions.as_ref(),
        hypotheses: hypotheses.as_ref(),
        valid_time: valid_time.as_ref(),
        algorithm_runs: algorithm_runs.as_ref(),
    })
}

#[derive(Clone, Copy)]
struct CompositeLedgers<'a> {
    provenance: Option<&'a graphforge_provenance::ProvenanceLedger>,
    knowledge: Option<&'a graphforge_knowledge::AssertionLedger>,
    confidence: Option<&'a graphforge_knowledge::ConfidenceLedger>,
    evidence: Option<&'a graphforge_knowledge::EvidenceLedger>,
    reasoning: Option<&'a graphforge_knowledge::ReasoningLedger>,
    statuses: Option<&'a graphforge_knowledge::AssertionStatusLedger>,
    supersessions: Option<&'a graphforge_knowledge::AssertionSupersessionLedger>,
    hypotheses: Option<&'a graphforge_knowledge::HypothesisLedger>,
    valid_time: Option<&'a graphforge_knowledge::AssertionValidityLedger>,
    algorithm_runs: Option<&'a graphforge_knowledge::AlgorithmRunLedger>,
}

#[expect(
    clippy::too_many_lines,
    reason = "one linear pass keeps the complete cross-ledger reference matrix auditable"
)]
fn validate_composite_references(ledgers: CompositeLedgers<'_>) -> Result<(), GfError> {
    let assertion_ids = ledgers
        .knowledge
        .into_iter()
        .flat_map(|ledger| ledger.assertions.iter().map(|row| row.assertion_uuid))
        .collect::<HashSet<_>>();
    let confidence_ids = ledgers
        .confidence
        .into_iter()
        .flat_map(|ledger| ledger.assessments.iter().map(|row| row.confidence_uuid))
        .collect::<HashSet<_>>();
    let reasoning_ids = ledgers
        .reasoning
        .into_iter()
        .flat_map(|ledger| ledger.records.iter().map(|row| row.reasoning_uuid))
        .collect::<HashSet<_>>();
    let provenance_ids = ledgers
        .provenance
        .into_iter()
        .flat_map(|ledger| ledger.events.iter().map(|row| row.provenance_uuid))
        .collect::<HashSet<_>>();
    let status_ids = ledgers
        .statuses
        .into_iter()
        .flat_map(|ledger| ledger.events.iter().map(|row| row.status_event_uuid))
        .collect::<HashSet<_>>();

    let require = |present: bool, kind: &'static str| {
        if present {
            Ok(())
        } else {
            Err(GfError::Validation(format!(
                "checkpoint source has a dangling {kind} reference"
            )))
        }
    };
    let provenance = |uuid| require(provenance_ids.contains(&uuid), "provenance");
    for row in ledgers
        .confidence
        .into_iter()
        .flat_map(|value| &value.assessments)
    {
        require(
            assertion_ids.contains(&row.assertion_uuid),
            "confidence assertion",
        )?;
        provenance(row.provenance_uuid)?;
    }
    for row in ledgers.evidence.into_iter().flat_map(|value| &value.links) {
        require(
            assertion_ids.contains(&row.assertion_uuid),
            "evidence assertion",
        )?;
        provenance(row.provenance_uuid)?;
    }
    for row in ledgers
        .reasoning
        .into_iter()
        .flat_map(|value| &value.records)
    {
        require(
            assertion_ids.contains(&row.assertion_uuid),
            "reasoning assertion",
        )?;
        provenance(row.provenance_uuid)?;
    }
    for row in ledgers.statuses.into_iter().flat_map(|value| &value.events) {
        require(
            assertion_ids.contains(&row.assertion_uuid),
            "status assertion",
        )?;
        if let Some(uuid) = row.confidence_uuid {
            require(confidence_ids.contains(&uuid), "status confidence")?;
        }
        if let Some(uuid) = row.reasoning_uuid {
            require(reasoning_ids.contains(&uuid), "status reasoning")?;
        }
        provenance(row.provenance_uuid)?;
    }
    for row in ledgers
        .supersessions
        .into_iter()
        .flat_map(graphforge_knowledge::AssertionSupersessionLedger::relations)
    {
        require(
            assertion_ids.contains(&row.prior_assertion_uuid),
            "supersession assertion",
        )?;
        require(
            assertion_ids.contains(&row.replacement_assertion_uuid),
            "supersession assertion",
        )?;
        require(
            status_ids.contains(&row.status_event_uuid),
            "supersession status",
        )?;
        require(
            reasoning_ids.contains(&row.reasoning_uuid),
            "supersession reasoning",
        )?;
        provenance(row.provenance_uuid)?;
    }
    if let Some(ledger) = ledgers.hypotheses {
        for row in ledger.groups() {
            provenance(row.provenance_uuid)?;
        }
        for row in ledger.membership_events() {
            require(
                assertion_ids.contains(&row.assertion_uuid),
                "hypothesis assertion",
            )?;
            require(
                reasoning_ids.contains(&row.reasoning_uuid),
                "hypothesis reasoning",
            )?;
            provenance(row.provenance_uuid)?;
        }
        for row in ledger.selection_events() {
            if let Some(uuid) = row.selected_assertion_uuid {
                require(
                    assertion_ids.contains(&uuid),
                    "hypothesis selection assertion",
                )?;
            }
            require(
                reasoning_ids.contains(&row.reasoning_uuid),
                "hypothesis reasoning",
            )?;
            provenance(row.provenance_uuid)?;
        }
    }
    for row in ledgers
        .valid_time
        .into_iter()
        .flat_map(|value| &value.events)
    {
        require(
            assertion_ids.contains(&row.assertion_uuid),
            "valid-time assertion",
        )?;
        if let Some(uuid) = row.reasoning_uuid {
            require(reasoning_ids.contains(&uuid), "valid-time reasoning")?;
        }
        provenance(row.provenance_uuid)?;
    }
    if let Some(ledger) = ledgers.algorithm_runs {
        for row in &ledger.runs {
            provenance(row.provenance_uuid)?;
        }
        for row in &ledger.events {
            provenance(row.provenance_uuid)?;
        }
    }
    Ok(())
}

fn checkpoint_api_error(code: ApiErrorCode, message: impl Into<String>) -> GfError {
    GfError::Api {
        code,
        message: message.into(),
    }
}

fn schema_mismatch(message: impl Into<String>) -> GfError {
    checkpoint_api_error(ApiErrorCode::SchemaMismatch, message)
}

fn page_invalid(message: impl Into<String>) -> GfError {
    checkpoint_api_error(ApiErrorCode::PageInvalid, message)
}

fn checkpoint_list_snapshot(rows: &[graphforge_storage::CheckpointRecord]) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"graphforge-checkpoint-list-page/1");
    for row in rows {
        h.update(row.checkpoint_uuid.as_bytes());
        h.update(row.created_revision.to_be_bytes());
    }
    graphforge_core::canonical::uuid_v8(h.finalize().into())
}
fn request_binding(method: &str, scope: u8, detail: u8) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"graphforge-page-request-binding/1");
    h.update(method.as_bytes());
    h.update([scope, detail]);
    graphforge_core::canonical::uuid_v8(h.finalize().into())
}
fn page_cursor(parts: &[&[u8]]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"graphforge-page-last-sort-tuple/1");
    for part in parts {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part);
    }
    h.finalize().into()
}
fn page_bounds(
    method: &str,
    binding: Uuid,
    page: &PageRequest,
    snapshot: Uuid,
    cursors: &[[u8; 32]],
) -> Result<(usize, usize), GfError> {
    if !(1..=crate::paging::MAX_PAGE_LIMIT).contains(&page.limit) {
        return Err(GfError::Validation(format!(
            "page limit must be in 1..={}",
            crate::paging::MAX_PAGE_LIMIT
        )));
    }
    cancellation(page)?;
    let start = match &page.after {
        Some(token) => {
            let (offset, cursor) = token.decode_bound(method, binding, snapshot, page.limit)?;
            if offset == 0 || cursors.get(offset - 1) != Some(&cursor) {
                return Err(page_invalid(
                    "page token cursor is not the last complete sort tuple",
                ));
            }
            offset
        }
        None => 0,
    };
    let count = cursors.len();
    if start > count {
        return Err(page_invalid("page token offset exceeds result rows"));
    }
    Ok((start, start.saturating_add(page.limit as usize).min(count)))
}
fn cancellation(page: &PageRequest) -> Result<(), GfError> {
    if let Some(c) = &page.cancellation {
        c.checkpoint()?;
    }
    Ok(())
}

fn execution(batch: RecordBatch) -> ExecutionResult {
    let rows = batch.num_rows() as u64;
    ExecutionResult {
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

fn checkpoint_rows(
    rows: &[graphforge_storage::CheckpointRecord],
    next: Option<&PageToken>,
) -> Result<ExecutionResult, GfError> {
    let mut id = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut name = StringBuilder::new();
    let mut desc = StringBuilder::new();
    let mut generation = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut digest = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
    let mut at = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut by = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        id.append_value(row.checkpoint_uuid.as_bytes())
            .map_err(arrow_error)?;
        name.append_value(&row.name);
        match &row.description {
            Some(v) => desc.append_value(v),
            None => desc.append_null(),
        }
        generation
            .append_value(row.generation_uuid.as_bytes())
            .map_err(arrow_error)?;
        digest
            .append_value(decode_hex(&row.generation_manifest_sha256)?)
            .map_err(arrow_error)?;
        at.append_value(row.created_at);
        match row.created_by {
            Some(v) => by.append_value(v.as_bytes()).map_err(arrow_error)?,
            None => by.append_null(),
        }
    }
    let fields = vec![
        Field::new("checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("generation_uuid", DataType::FixedSizeBinary(16), false),
        Field::new(
            "generation_manifest_sha256",
            DataType::FixedSizeBinary(32),
            false,
        ),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("created_by", DataType::FixedSizeBinary(16), true),
    ];
    let batch = make_batch(
        "checkpoint",
        fields,
        vec![
            Arc::new(id.finish()),
            Arc::new(name.finish()),
            Arc::new(desc.finish()),
            Arc::new(generation.finish()),
            Arc::new(digest.finish()),
            Arc::new(at.finish()),
            Arc::new(by.finish()),
        ],
        next,
    )?;
    Ok(execution(batch))
}

fn receipt_result(row: &graphforge_storage::CheckpointReceipt) -> ExecutionResult {
    let mut op = StringBuilder::new();
    op.append_value(row.operation);
    let mut operation = FixedSizeBinaryBuilder::with_capacity(1, 16);
    operation
        .append_value(row.operation_uuid.as_bytes())
        .expect("UUID width is fixed by the checkpoint receipt contract");
    let mut checkpoint = FixedSizeBinaryBuilder::with_capacity(1, 16);
    checkpoint
        .append_value(row.checkpoint_uuid.as_bytes())
        .expect("UUID width is fixed by the checkpoint receipt contract");
    let mut name = StringBuilder::new();
    name.append_value(&row.name);
    let mut source = FixedSizeBinaryBuilder::with_capacity(1, 16);
    source
        .append_value(row.source_generation_uuid.as_bytes())
        .expect("UUID width is fixed by the checkpoint receipt contract");
    let mut prior_current = FixedSizeBinaryBuilder::with_capacity(1, 16);
    match row.prior_current_generation_uuid {
        Some(value) => prior_current
            .append_value(value.as_bytes())
            .expect("UUID width is fixed by the checkpoint receipt contract"),
        None => prior_current.append_null(),
    }
    let mut result = FixedSizeBinaryBuilder::with_capacity(1, 16);
    match row.result_generation_uuid {
        Some(value) => result
            .append_value(value.as_bytes())
            .expect("UUID width is fixed by the checkpoint receipt contract"),
        None => result.append_null(),
    }
    let mut revision = UInt64Builder::new();
    revision.append_value(row.registry_revision);
    let mut at = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    at.append_value(row.committed_at);
    let fields = vec![
        Field::new("operation", DataType::Utf8, false),
        Field::new("operation_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "source_generation_uuid",
            DataType::FixedSizeBinary(16),
            false,
        ),
        Field::new(
            "prior_current_generation_uuid",
            DataType::FixedSizeBinary(16),
            true,
        ),
        Field::new(
            "result_generation_uuid",
            DataType::FixedSizeBinary(16),
            true,
        ),
        Field::new("registry_revision", DataType::UInt64, false),
        Field::new(
            "committed_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ];
    execution(
        make_batch(
            "checkpoint_receipt",
            fields,
            vec![
                Arc::new(op.finish()),
                Arc::new(operation.finish()),
                Arc::new(checkpoint.finish()),
                Arc::new(name.finish()),
                Arc::new(source.finish()),
                Arc::new(prior_current.finish()),
                Arc::new(result.finish()),
                Arc::new(revision.finish()),
                Arc::new(at.finish()),
            ],
            None,
        )
        .expect("checkpoint receipt columns are constructed from its fixed schema"),
    )
}

fn arrow_error(error: arrow::error::ArrowError) -> GfError {
    let message = error.to_string();
    drop(error);
    GfError::Execution(message)
}
fn make_batch(
    id: &str,
    fields: Vec<Field>,
    columns: Vec<ArrayRef>,
    next: Option<&PageToken>,
) -> Result<RecordBatch, GfError> {
    let mut metadata = HashMap::from([
        ("graphforge.contract.id".into(), id.into()),
        ("graphforge.contract.version".into(), "1".into()),
    ]);
    if let Some(next) = next {
        metadata.insert("graphforge.next_page_token".into(), next.as_str().into());
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, metadata)),
        columns,
    )
    .map_err(|e| GfError::Execution(e.to_string()))
}
fn decode_hex(value: &str) -> Result<[u8; 32], GfError> {
    if value.len() != 64 {
        return Err(GfError::Validation("invalid checkpoint digest".into()));
    }
    let mut out = [0; 32];
    for (i, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        out[i] = u8::from_str_radix(
            std::str::from_utf8(pair)
                .map_err(|_| GfError::Validation("invalid checkpoint digest".into()))?,
            16,
        )
        .map_err(|_| GfError::Validation("invalid checkpoint digest".into()))?;
    }
    Ok(out)
}

fn read_only<T>() -> Result<T, GfError> {
    Err(GfError::Project {
        code: ProjectErrorCode::ReadOnlyView,
        message: "checkpoint views are read-only".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::diff::{participant_scope, scope_matches};
    use super::*;
    use arrow::array::{Array, FixedSizeBinaryArray, StringArray};
    use graphforge_core::OntologyMode;
    use graphforge_knowledge::{AssertionGraphRole, AssertionStatus, GraphObjectKind};
    use tempfile::tempdir;

    pub(super) fn operation(value: u128) -> OperationId {
        OperationId(Uuid::from_u128(value))
    }

    #[test]
    fn checkpoint_digest_and_scope_helpers_are_closed_and_exact() {
        for (error, code, display) in [
            (
                schema_mismatch("schema detail"),
                "GF_SCHEMA_MISMATCH",
                "GF_SCHEMA_MISMATCH: schema detail",
            ),
            (
                page_invalid("page detail"),
                "GF_PAGE_INVALID",
                "GF_PAGE_INVALID: page detail",
            ),
            (
                checkpoint_api_error(ApiErrorCode::NotFound, "missing detail"),
                "GF_NOT_FOUND",
                "GF_NOT_FOUND: missing detail",
            ),
        ] {
            assert_eq!(error.code(), code);
            assert_eq!(error.to_string(), display);
        }
        assert_eq!(decode_hex(&"ab".repeat(32)).unwrap(), [0xab; 32]);
        for invalid in ["", "ab", &"gg".repeat(32)] {
            let error = decode_hex(invalid).unwrap_err();
            assert_eq!(error.code(), "GF_VALIDATION");
            assert_eq!(
                error.to_string(),
                "validation error: invalid checkpoint digest"
            );
        }
        for (capability, family, expected) in [
            ("graph", "snapshot", "graph"),
            ("ontology", "snapshot", "ontology"),
            ("provenance", "events", "provenance"),
            ("knowledge", "assertions", "knowledge"),
            ("epistemic", "status", "epistemic"),
            ("valid_time", "validity", "epistemic"),
            ("workspace", "ontology", "ontology"),
            ("workspace", "configuration", "configuration"),
            ("search", "index", "capabilities"),
        ] {
            assert_eq!(participant_scope(capability, family), expected);
        }
        for scope in [
            CheckpointDiffScope::Summary,
            CheckpointDiffScope::All,
            CheckpointDiffScope::Graph,
            CheckpointDiffScope::Ontology,
            CheckpointDiffScope::Configuration,
            CheckpointDiffScope::Capabilities,
            CheckpointDiffScope::Provenance,
            CheckpointDiffScope::Knowledge,
            CheckpointDiffScope::Epistemic,
        ] {
            let actual = match scope {
                CheckpointDiffScope::Summary | CheckpointDiffScope::All => "anything",
                CheckpointDiffScope::Graph => "graph",
                CheckpointDiffScope::Ontology => "ontology",
                CheckpointDiffScope::Configuration => "configuration",
                CheckpointDiffScope::Capabilities => "capabilities",
                CheckpointDiffScope::Provenance => "provenance",
                CheckpointDiffScope::Knowledge => "knowledge",
                CheckpointDiffScope::Epistemic => "epistemic",
            };
            assert!(scope_matches(scope, actual));
        }
        assert!(!scope_matches(CheckpointDiffScope::Graph, "knowledge"));
    }

    fn enable(graph: &GraphForge, capability_id: crate::CapabilityId, seed: u128) {
        graph
            .enable_capability(crate::EnableCapabilityRequest {
                context: crate::WriteContext {
                    operation_uuid: operation(seed),
                    actor_uuid: None,
                },
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }

    pub(super) fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn write_ontology(root: &std::path::Path) -> std::path::PathBuf {
        let path = root.join("shape.yaml");
        std::fs::write(
            &path,
            "ontology_id: checkpoint-shape\nversion: v1\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types: []\nproperties:\n  - owner: Person\n    name: name\n    type: utf8\n    nullable: false\nconstraints: []\nmigrations: []\n",
        )
        .unwrap();
        path
    }

    #[test]
    fn revert_source_validation_rejects_cross_domain_dangling_assertion() {
        let event = graphforge_provenance::ProvenanceEvent::new(
            Uuid::from_u128(1),
            graphforge_provenance::EventKind::AssessConfidence,
            None,
            1,
        )
        .unwrap();
        let provenance =
            graphforge_provenance::ProvenanceLedger::new(vec![event.clone()], vec![]).unwrap();
        let confidence = graphforge_knowledge::ConfidenceLedger::explicit(
            Uuid::now_v7(),
            Uuid::now_v7(),
            0.5,
            event.provenance_uuid,
            1,
        )
        .unwrap();

        let error = validate_composite_references(CompositeLedgers {
            provenance: Some(&provenance),
            knowledge: Some(&graphforge_knowledge::AssertionLedger::default()),
            confidence: Some(&confidence),
            evidence: None,
            reasoning: None,
            statuses: None,
            supersessions: None,
            hypotheses: None,
            valid_time: None,
            algorithm_runs: None,
        })
        .unwrap_err();

        assert_eq!(error.code(), "GF_VALIDATION");
        assert!(error.to_string().contains("dangling confidence assertion"));
    }

    #[test]
    fn wave8_composite_reference_validation_identifies_every_knowledge_link_kind() {
        use graphforge_knowledge::{
            Assertion, AssertionGraphRef, AssertionLedger, AssertionStatusEvent,
            AssertionStatusLedger, AssertionSupersession, AssertionSupersessionLedger,
            ConfidenceLedger, EvidenceLedger, EvidenceLink, EvidenceRole, EvidenceSourceKind,
            ReasoningContentFormat, ReasoningKind, ReasoningLedger, ReasoningRecord,
        };

        let event = graphforge_provenance::ProvenanceEvent::new(
            Uuid::from_u128(10),
            graphforge_provenance::EventKind::CreateAssertion,
            None,
            1,
        )
        .unwrap();
        let provenance =
            graphforge_provenance::ProvenanceLedger::new(vec![event.clone()], vec![]).unwrap();
        let a1 = uuid7(41);
        let a2 = uuid7(42);
        let missing = uuid7(99);
        let knowledge = AssertionLedger::new(
            vec![
                Assertion::new(a1, "first".into(), event.provenance_uuid, 1).unwrap(),
                Assertion::new(a2, "second".into(), event.provenance_uuid, 1).unwrap(),
            ],
            vec![
                AssertionGraphRef::new(
                    a1,
                    Uuid::from_u128(101),
                    GraphObjectKind::Node,
                    AssertionGraphRole::Subject,
                    0,
                )
                .unwrap(),
                AssertionGraphRef::new(
                    a2,
                    Uuid::from_u128(102),
                    GraphObjectKind::Node,
                    AssertionGraphRole::Subject,
                    0,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let reasoning_id = uuid7(43);
        let reasoning = ReasoningLedger::new(vec![
            ReasoningRecord::new(
                reasoning_id,
                a1,
                ReasoningKind::DecisionRationale,
                ReasoningContentFormat::TextPlain,
                b"because".to_vec(),
                None,
                event.provenance_uuid,
                1,
            )
            .unwrap(),
        ])
        .unwrap();
        let confidence_id = uuid7(44);
        let confidence =
            ConfidenceLedger::explicit(confidence_id, a1, 0.5, event.provenance_uuid, 1).unwrap();
        let status_id = uuid7(45);
        let statuses = AssertionStatusLedger::new(vec![
            AssertionStatusEvent::new(
                status_id,
                a1,
                AssertionStatus::Hypothesis,
                Some(confidence_id),
                Some(reasoning_id),
                event.provenance_uuid,
                1,
            )
            .unwrap(),
        ])
        .unwrap();

        for (assertion, provenance_uuid, expected) in [
            (missing, event.provenance_uuid, "evidence assertion"),
            (a1, Uuid::from_u128(999), "provenance"),
        ] {
            let evidence = EvidenceLedger::new(vec![
                EvidenceLink::new(
                    uuid7(46),
                    assertion,
                    Uuid::from_u128(7),
                    EvidenceSourceKind::Document,
                    EvidenceRole::Supports,
                    None,
                    provenance_uuid,
                    1,
                )
                .unwrap(),
            ])
            .unwrap();
            let error = validate_composite_references(CompositeLedgers {
                provenance: Some(&provenance),
                knowledge: Some(&knowledge),
                confidence: None,
                evidence: Some(&evidence),
                reasoning: None,
                statuses: None,
                supersessions: None,
                hypotheses: None,
                valid_time: None,
                algorithm_runs: None,
            })
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }

        for (assertion, provenance_uuid, expected) in [
            (missing, event.provenance_uuid, "reasoning assertion"),
            (a1, Uuid::from_u128(999), "provenance"),
        ] {
            let records = ReasoningLedger::new(vec![
                ReasoningRecord::new(
                    uuid7(47),
                    assertion,
                    ReasoningKind::DecisionRationale,
                    ReasoningContentFormat::TextPlain,
                    b"because".to_vec(),
                    None,
                    provenance_uuid,
                    1,
                )
                .unwrap(),
            ])
            .unwrap();
            let error = validate_composite_references(CompositeLedgers {
                provenance: Some(&provenance),
                knowledge: Some(&knowledge),
                confidence: None,
                evidence: None,
                reasoning: Some(&records),
                statuses: None,
                supersessions: None,
                hypotheses: None,
                valid_time: None,
                algorithm_runs: None,
            })
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }

        for (prior, replacement, status, rationale, provenance_uuid, expected) in [
            (
                missing,
                a2,
                status_id,
                reasoning_id,
                event.provenance_uuid,
                "supersession assertion",
            ),
            (
                a1,
                missing,
                status_id,
                reasoning_id,
                event.provenance_uuid,
                "supersession assertion",
            ),
            (
                a1,
                a2,
                missing,
                reasoning_id,
                event.provenance_uuid,
                "supersession status",
            ),
            (
                a1,
                a2,
                status_id,
                missing,
                event.provenance_uuid,
                "supersession reasoning",
            ),
            (
                a1,
                a2,
                status_id,
                reasoning_id,
                Uuid::from_u128(999),
                "provenance",
            ),
        ] {
            let relation = AssertionSupersessionLedger::new(vec![
                AssertionSupersession::new(
                    uuid7(48),
                    prior,
                    replacement,
                    status,
                    rationale,
                    provenance_uuid,
                    1,
                )
                .unwrap(),
            ])
            .unwrap();
            let error = validate_composite_references(CompositeLedgers {
                provenance: Some(&provenance),
                knowledge: Some(&knowledge),
                confidence: Some(&confidence),
                evidence: None,
                reasoning: Some(&reasoning),
                statuses: Some(&statuses),
                supersessions: Some(&relation),
                hypotheses: None,
                valid_time: None,
                algorithm_runs: None,
            })
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn revert_preview_is_non_mutating_and_receipt_identifies_prior_current() {
        let directory = tempdir().unwrap();
        let path = directory.path().to_str().unwrap();
        let mut graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute("CREATE (:State {value: 'checkpoint'})")
            .unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "Before".into(),
                description: Some("preview target".into()),
                idempotency_key: operation(140),
                actor_uuid: None,
            })
            .unwrap();
        let (checkpoint, _) =
            graphforge_storage::open_checkpoint_generation(directory.path(), "Before").unwrap();
        graph.execute("CREATE (:State {value: 'current'})").unwrap();
        let current_before = graph.generation_for_read().unwrap().generation_uuid();
        let generations_before = std::fs::read_dir(directory.path().join("generations"))
            .unwrap()
            .count();

        let preview = GraphForge::preview_revert_to_checkpoint(
            directory.path(),
            PreviewRevertCheckpointRequest {
                name: "Before".into(),
            },
        )
        .unwrap();
        assert_eq!(preview.checkpoint_uuid, checkpoint.checkpoint_uuid);
        assert_eq!(preview.source_generation_uuid, checkpoint.generation_uuid);
        assert_eq!(
            preview.source_manifest_sha256,
            checkpoint.generation_manifest_sha256
        );
        assert_eq!(preview.current_generation_uuid, current_before);
        assert_eq!(
            graph.generation_for_read().unwrap().generation_uuid(),
            current_before
        );
        assert_eq!(
            std::fs::read_dir(directory.path().join("generations"))
                .unwrap()
                .count(),
            generations_before
        );

        let missing = GraphForge::preview_revert_to_checkpoint(
            directory.path(),
            PreviewRevertCheckpointRequest {
                name: "Missing".into(),
            },
        )
        .unwrap_err();
        assert_eq!(missing.code(), "GF_CHECKPOINT_NOT_FOUND");

        let receipt = graph
            .revert_to_checkpoint(RevertCheckpointRequest {
                name: "Before".into(),
                reason: "previewed identities".into(),
                idempotency_key: operation(141),
                actor_uuid: None,
            })
            .unwrap();
        let prior_current = receipt.batches[0]
            .column_by_name("prior_current_generation_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(prior_current.value(0), current_before.as_bytes());
    }

    #[test]
    fn revert_is_visible_on_the_same_graphforge_instance() {
        let directory = tempdir().unwrap();
        let write_options = crate::GraphForgeOptions {
            write_mode: crate::ProjectWriteMode::QueuedWriter,
            write_queue_capacity: 7,
            max_rebase_attempts: 2,
            ..crate::GraphForgeOptions::default()
        };
        let mut graph = GraphForge::new_with_options(
            Some(directory.path().to_str().unwrap()),
            write_options.clone(),
        )
        .unwrap();
        graph.execute("CREATE (:Person {name: 'before'})").unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "Before".into(),
                description: None,
                idempotency_key: operation(100),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(graph.write_options, write_options);
        graph.execute("CREATE (:Person {name: 'after'})").unwrap();
        let post_checkpoint_handle = graph.add_node("Transient", &HashMap::new()).unwrap();

        let receipt = graph
            .revert_to_checkpoint(RevertCheckpointRequest {
                name: "Before".into(),
                reason: "return to known state".into(),
                idempotency_key: operation(101),
                actor_uuid: None,
            })
            .unwrap();
        assert!(
            !receipt.batches[0]
                .column_by_name("result_generation_uuid")
                .unwrap()
                .is_null(0)
        );
        let rows = graph
            .execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
            .unwrap();
        let names = rows.batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.len(), 1);
        assert_eq!(names.value(0), "before");
        assert!(
            graph
                .add_edge(
                    &post_checkpoint_handle,
                    "STALE",
                    &post_checkpoint_handle,
                    &HashMap::new(),
                )
                .is_err(),
            "revert must invalidate handles owned by the replaced facade"
        );
    }

    #[test]
    fn in_memory_revert_preserves_its_backing_project_owner() {
        let mut graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Memory {state: 'before'})").unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "Memory".into(),
                description: None,
                idempotency_key: operation(130),
                actor_uuid: None,
            })
            .unwrap();
        graph.execute("CREATE (:Memory {state: 'after'})").unwrap();
        graph
            .revert_to_checkpoint(RevertCheckpointRequest {
                name: "Memory".into(),
                reason: "in-memory ownership".into(),
                idempotency_key: operation(131),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Memory) RETURN count(n) AS total")
                .unwrap()
                .stats
                .rows_produced,
            1
        );
        assert!(
            graph
                .checkpoint(CheckpointRequest {
                    name: "AfterMemoryRevert".into(),
                    description: None,
                    idempotency_key: operation(132),
                    actor_uuid: None,
                })
                .is_ok()
        );
    }

    #[test]
    fn revert_accepts_graph_knowledge_and_full_epistemic_checkpoint_shapes_after_reopen() {
        for (index, capabilities) in [
            vec![],
            vec![
                crate::CapabilityId::Provenance,
                crate::CapabilityId::Knowledge,
            ],
            vec![
                crate::CapabilityId::Provenance,
                crate::CapabilityId::Knowledge,
                crate::CapabilityId::Epistemic,
                crate::CapabilityId::ValidTime,
            ],
        ]
        .into_iter()
        .enumerate()
        {
            let directory = tempdir().unwrap();
            let mut graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
            for (offset, capability) in capabilities.into_iter().enumerate() {
                enable(
                    &graph,
                    capability,
                    1_000 + index as u128 * 10 + offset as u128,
                );
            }
            graph
                .execute("CREATE (:Shape {state: 'checkpoint'})")
                .unwrap();
            graph
                .checkpoint(CheckpointRequest {
                    name: "Shape".into(),
                    description: None,
                    idempotency_key: operation(1_100 + index as u128),
                    actor_uuid: None,
                })
                .unwrap();
            graph.execute("CREATE (:Shape {state: 'later'})").unwrap();
            graph
                .revert_to_checkpoint(RevertCheckpointRequest {
                    name: "Shape".into(),
                    reason: "shape acceptance".into(),
                    idempotency_key: operation(1_200 + index as u128),
                    actor_uuid: None,
                })
                .unwrap();
            drop(graph);

            let reopened = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
            let count = reopened
                .execute("MATCH (n:Shape) RETURN count(n) AS total")
                .unwrap();
            let totals = count.batches[0]
                .column_by_name("total")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(totals.value(0), 1, "shape {index}");
            assert!(reopened.open_checkpoint("Shape").is_ok());
        }
    }

    #[test]
    fn populated_checkpoint_shapes_restore_real_domain_records() {
        for shape in [
            "ontology-free",
            "emergent",
            "advisory",
            "strict",
            "knowledge",
            "epistemic",
        ] {
            let directory = tempdir().unwrap();
            let mut graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
            graph.set_clock_for_test(|| Ok(1_000));

            if matches!(shape, "advisory" | "strict") {
                graph
                    .adopt_ontology(crate::AdoptOntologyRequest {
                        context: crate::WriteContext {
                            operation_uuid: operation(2_000),
                            actor_uuid: None,
                        },
                        path: write_ontology(directory.path()),
                        mode: if shape == "strict" {
                            OntologyMode::Strict
                        } else {
                            OntologyMode::Advisory
                        },
                    })
                    .unwrap();
            }

            let node_uuid = graph.add_node("Person", &HashMap::new()).unwrap().uuid;

            if matches!(shape, "knowledge" | "epistemic") {
                enable(&graph, crate::CapabilityId::Provenance, 2_100);
                enable(&graph, crate::CapabilityId::Knowledge, 2_101);
                if shape == "epistemic" {
                    enable(&graph, crate::CapabilityId::Epistemic, 2_102);
                }
                let assertion = crate::CreateAssertionRequest {
                    context: crate::WriteContext {
                        operation_uuid: operation(2_110),
                        actor_uuid: None,
                    },
                    assertion_uuid: uuid7(110),
                    claim: format!("{shape} checkpoint claim"),
                    graph_refs: vec![crate::AssertionGraphRefInput {
                        graph_uuid: node_uuid,
                        graph_kind: GraphObjectKind::Node,
                        role: AssertionGraphRole::Subject,
                        ordinal: 0,
                    }],
                };
                if shape == "epistemic" {
                    let assertion_result = graph
                        .create_assertion_with_status(crate::CreateAssertionWithStatusRequest {
                            assertion,
                            first_status: crate::FirstAssertionStatusInput {
                                status_event_uuid: uuid7(111),
                                status: AssertionStatus::Hypothesis,
                            },
                        })
                        .unwrap();
                    let provenance_uuid = Uuid::from_slice(
                        assertion_result.batches[0]
                            .column_by_name("provenance_uuid")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<FixedSizeBinaryArray>()
                            .unwrap()
                            .value(0),
                    )
                    .unwrap();
                    let confidence_uuid = uuid7(112);
                    graph
                        .assess_confidence(crate::AssessConfidenceRequest {
                            context: crate::WriteContext {
                                operation_uuid: operation(2_112),
                                actor_uuid: None,
                            },
                            confidence_uuid,
                            assertion_uuid: uuid7(110),
                            policy: crate::ConfidencePolicyRequest::Explicit { value: 0.8 },
                        })
                        .unwrap();
                    let reasoning_uuid = uuid7(113);
                    graph
                        .record_reasoning(crate::RecordReasoningRequest {
                            context: crate::WriteContext {
                                operation_uuid: operation(2_113),
                                actor_uuid: None,
                            },
                            reasoning_uuid,
                            assertion_uuid: uuid7(110),
                            kind: graphforge_knowledge::ReasoningKind::EvidenceInterpretation,
                            content_format: graphforge_knowledge::ReasoningContentFormat::TextPlain,
                            content: b"checkpoint rationale".to_vec(),
                            supersedes_reasoning_uuid: None,
                            provenance_uuid,
                        })
                        .unwrap();
                    graph
                        .record_assertion_status(crate::RecordAssertionStatusRequest {
                            context: crate::WriteContext {
                                operation_uuid: operation(2_114),
                                actor_uuid: None,
                            },
                            status_event_uuid: uuid7(114),
                            assertion_uuid: uuid7(110),
                            status: AssertionStatus::Supported,
                            confidence_uuid: Some(confidence_uuid),
                            reasoning_uuid: Some(reasoning_uuid),
                            provenance_uuid,
                        })
                        .unwrap();
                } else {
                    graph.create_assertion(assertion).unwrap();
                }
            }

            graph
                .checkpoint(CheckpointRequest {
                    name: "Populated".into(),
                    description: Some(shape.into()),
                    idempotency_key: operation(2_200),
                    actor_uuid: None,
                })
                .unwrap();
            graph.execute("MATCH (n) DETACH DELETE n").unwrap();
            graph
                .revert_to_checkpoint(RevertCheckpointRequest {
                    name: "Populated".into(),
                    reason: format!("restore {shape}"),
                    idempotency_key: operation(2_201),
                    actor_uuid: None,
                })
                .unwrap_or_else(|error| panic!("{shape}: {error:?}"));
            crate::permanent_parquet_test_support::assert_participants(
                directory.path(),
                graphforge_storage::WORKSPACE_CAPABILITY_ID,
            );
            if matches!(shape, "knowledge" | "epistemic") {
                crate::permanent_parquet_test_support::assert_participants(
                    directory.path(),
                    "knowledge",
                );
            }
            if shape == "epistemic" {
                crate::permanent_parquet_test_support::assert_participants(
                    directory.path(),
                    "epistemic",
                );
            }
            drop(graph);

            let reopened = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
            let restored = reopened
                .execute("MATCH (n:Person) RETURN count(n) AS total")
                .unwrap();
            assert_eq!(restored.stats.rows_produced, 1, "{shape}");
            if matches!(shape, "knowledge" | "epistemic") {
                assert_eq!(
                    reopened
                        .assertion(uuid7(110), None)
                        .unwrap()
                        .stats
                        .rows_produced,
                    1
                );
            }
            if shape == "epistemic" {
                assert_eq!(
                    reopened
                        .assertion_status(uuid7(110))
                        .unwrap()
                        .stats
                        .rows_produced,
                    1
                );
                assert_eq!(
                    reopened
                        .confidence_assessment(uuid7(112), None)
                        .unwrap()
                        .stats
                        .rows_produced,
                    1
                );
                assert_eq!(
                    reopened
                        .reasoning(uuid7(113), None)
                        .unwrap()
                        .stats
                        .rows_produced,
                    1
                );
            }
        }
    }

    #[test]
    fn in_memory_checkpoint_lifecycle_remains_ephemeral() {
        let mut graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Person {name: 'before'})").unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "Ephemeral".into(),
                description: None,
                idempotency_key: operation(230),
                actor_uuid: None,
            })
            .unwrap();
        graph.execute("CREATE (:Person {name: 'after'})").unwrap();
        graph
            .revert_to_checkpoint(RevertCheckpointRequest {
                name: "Ephemeral".into(),
                reason: "restore ephemeral checkpoint".into(),
                idempotency_key: operation(232),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(
            graph.lifecycle_mode,
            graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral
        );
        assert_eq!(
            graph
                .list_checkpoints(ListCheckpointsRequest::default())
                .unwrap()
                .stats
                .rows_produced,
            1
        );
        graph
            .delete_checkpoint(DeleteCheckpointRequest {
                name: "Ephemeral".into(),
                idempotency_key: operation(233),
                actor_uuid: None,
            })
            .unwrap();
    }

    #[test]
    fn list_and_summary_diff_are_arrow_ordered_and_page_bound() {
        let directory = tempdir().unwrap();
        let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "A".into(),
                description: None,
                idempotency_key: operation(10),
                actor_uuid: None,
            })
            .unwrap();
        graph.execute("CREATE (:Person {name: 'changed'})").unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "B".into(),
                description: None,
                idempotency_key: operation(11),
                actor_uuid: None,
            })
            .unwrap();

        let listed = graph
            .list_checkpoints(ListCheckpointsRequest {
                page: PageRequest {
                    limit: 1,
                    after: None,
                    cancellation: None,
                },
            })
            .unwrap();
        assert_eq!(listed.batches[0].num_rows(), 1);
        assert!(
            listed
                .schema
                .metadata()
                .contains_key("graphforge.next_page_token")
        );

        let diff = graph
            .diff_checkpoints(DiffCheckpointsRequest {
                from: CheckpointSelector::Named("A".into()),
                to: CheckpointSelector::Named("B".into()),
                scope: CheckpointDiffScope::All,
                detail: CheckpointDiffDetail::Summary,
                page: PageRequest::default(),
            })
            .unwrap();
        assert!(diff.batches[0].num_rows() >= 3);
        assert_eq!(diff.schema.field(0).name(), "from_checkpoint_uuid");

        let first_page = graph
            .diff_checkpoints(DiffCheckpointsRequest {
                from: CheckpointSelector::Named("A".into()),
                to: CheckpointSelector::Named("B".into()),
                scope: CheckpointDiffScope::All,
                detail: CheckpointDiffDetail::Summary,
                page: PageRequest {
                    limit: 1,
                    after: None,
                    cancellation: None,
                },
            })
            .unwrap();
        let token =
            PageToken::parse(first_page.schema.metadata()["graphforge.next_page_token"].as_str())
                .unwrap();
        graph
            .delete_checkpoint(DeleteCheckpointRequest {
                name: "B".into(),
                idempotency_key: operation(12),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(
            graph
                .diff_checkpoints(DiffCheckpointsRequest {
                    from: CheckpointSelector::Named("A".into()),
                    to: CheckpointSelector::Named("B".into()),
                    scope: CheckpointDiffScope::All,
                    detail: CheckpointDiffDetail::Summary,
                    page: PageRequest {
                        limit: 1,
                        after: Some(token),
                        cancellation: None,
                    },
                })
                .unwrap_err()
                .code(),
            "GF_PAGE_SNAPSHOT_GONE"
        );
    }

    #[test]
    fn show_checkpoint_returns_the_exact_list_metadata_row_and_rejects_unknown_names() {
        let directory = tempdir().unwrap();
        let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
        let actor_uuid = Uuid::from_u128(91);
        graph
            .checkpoint(CheckpointRequest {
                name: "Release".into(),
                description: Some("ready to publish".into()),
                idempotency_key: operation(90),
                actor_uuid: Some(actor_uuid),
            })
            .unwrap();

        let listed = graph
            .list_checkpoints(ListCheckpointsRequest::default())
            .unwrap();
        let shown = graph
            .show_checkpoint(ShowCheckpointRequest {
                name: "Release".into(),
            })
            .unwrap();

        assert_eq!(shown.schema, listed.schema);
        assert_eq!(shown.batches, listed.batches);
        assert_eq!(shown.batches[0].num_rows(), 1);

        let error = graph
            .show_checkpoint(ShowCheckpointRequest {
                name: "release".into(),
            })
            .unwrap_err();
        assert_eq!(error.code(), "GF_CHECKPOINT_NOT_FOUND");
    }

    #[test]
    fn checkpoint_revert_corruption_matrix_fails_closed() {
        for corrupt in ["registry", "checksum", "participant"] {
            let directory = tempdir().unwrap();
            let mut graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
            graph.execute("CREATE (:Stable {value: 1})").unwrap();
            graph
                .checkpoint(CheckpointRequest {
                    name: "Stable".into(),
                    description: None,
                    idempotency_key: operation(4_000),
                    actor_uuid: None,
                })
                .unwrap();

            match corrupt {
                "registry" => std::fs::write(
                    directory.path().join("checkpoints/registry.json"),
                    b"{invalid\n",
                )
                .unwrap(),
                "checksum" => std::fs::write(
                    directory.path().join("checkpoints/registry.json.sha256"),
                    b"00\n",
                )
                .unwrap(),
                "participant" => {
                    let (_, generation) =
                        graphforge_storage::open_checkpoint_generation(directory.path(), "Stable")
                            .unwrap();
                    let path = generation
                        .participant_path(
                            graphforge_storage::GRAPH_CAPABILITY_ID,
                            graphforge_storage::GRAPH_FILES_FAMILY,
                        )
                        .unwrap();
                    std::fs::write(path, b"corrupt checkpoint participant").unwrap();
                }
                _ => unreachable!(),
            }

            let before = graphforge_storage::resolve_project_generation(directory.path())
                .unwrap()
                .generation_uuid();
            let error = graph
                .revert_to_checkpoint(RevertCheckpointRequest {
                    name: "Stable".into(),
                    reason: format!("reject {corrupt}"),
                    idempotency_key: operation(4_001),
                    actor_uuid: None,
                })
                .unwrap_err();
            let expected = if corrupt == "participant" {
                "GF_PROJECT_CORRUPT"
            } else {
                "GF_CHECKPOINT_REGISTRY_CORRUPT"
            };
            assert_eq!(error.code(), expected, "{corrupt}: {error}");
            assert_eq!(
                graphforge_storage::resolve_project_generation(directory.path())
                    .unwrap()
                    .generation_uuid(),
                before,
                "failed revert must not advance CURRENT for {corrupt}"
            );
        }
    }
}
