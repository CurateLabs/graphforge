//! Checkpoint / compaction of authoritative graph delta runs into a new
//! immutable Parquet generation (ADR 0019 / #753).
//!
//! Compaction selects one pinned base plus a verified contiguous `.gfdr`
//! prefix, streams the merge under explicit memory/spill/disk/cancellation
//! budgets, publishes through the same CURRENT path as other generations, and
//! reclaims subsumed inputs only via the shared retention reachability oracle
//! (#751). Derived `indexes/adjacency/deltas/` remain unrelated.

use std::fs::{self, File};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use graphforge_core::{GfError, ProjectErrorCode};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::graph_delta_journal::{
    GraphDeltaJournalLimits, list_delta_runs, load_verified_delta_runs,
};
use crate::graph_files::{GraphFilesInventory, capture_graph_files, verify_graph_tree};
use crate::project_generation::resolve_project_generation;
use crate::project_publication::{
    ProjectCapability, ProjectGenerationRequest, ProjectPublicationReceipt, ProjectStageOutcome,
    published_project_transaction, stage_project_generation_from_admitted_parent,
};
use crate::project_retention::{
    ProjectCleanupReport, ProjectRetentionLimits, ProjectRetentionPolicy,
    execute_project_cleanup_with_mode,
};
use crate::{GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, empty_workspace_participants};

/// Default peak logical memory budget for one compaction invocation.
pub const DEFAULT_COMPACTION_MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;
/// Default spill-byte budget for one compaction invocation.
pub const DEFAULT_COMPACTION_MAX_SPILL_BYTES: u64 = 256 * 1024 * 1024;
/// Default staged output disk budget for one compaction invocation.
pub const DEFAULT_COMPACTION_MAX_DISK_BYTES: u64 = 512 * 1024 * 1024;
/// Maximum supported logical memory budget.
pub const MAX_COMPACTION_MEMORY_BYTES: usize = 1024 * 1024 * 1024;
/// Maximum supported spill-byte budget.
pub const MAX_COMPACTION_SPILL_BYTES: u64 = 4 * 1024 * 1024 * 1024;
/// Maximum supported staged output disk budget.
pub const MAX_COMPACTION_DISK_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Default and supported maximum aggregate input runs.
pub const DEFAULT_COMPACTION_MAX_INPUT_RUNS: u64 = 64;
/// Default aggregate encoded input-byte budget.
pub const DEFAULT_COMPACTION_MAX_INPUT_BYTES: u64 = 1024 * 1024 * 1024;
/// Maximum supported aggregate encoded input-byte budget.
pub const MAX_COMPACTION_INPUT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Default maximum canonical output rows.
pub const DEFAULT_COMPACTION_MAX_OUTPUT_ROWS: u64 = 100_000_000;
/// Maximum supported canonical output rows.
pub const MAX_COMPACTION_OUTPUT_ROWS: u64 = 1_000_000_000;
/// Default cancellation polling cadence in rows.
pub const DEFAULT_COMPACTION_CANCELLATION_CHECK_ROWS: u64 = 8_192;
/// Maximum supported cancellation polling cadence in rows.
pub const MAX_COMPACTION_CANCELLATION_CHECK_ROWS: u64 = 1_048_576;
/// Spill directory name under the project-local spill root.
pub const GRAPH_DELTA_COMPACTION_SPILL_DIR: &str = "graph-delta-compaction";

/// Resource budgets for preview/execute compaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GraphDeltaCompactionLimits {
    /// Journal validation / replay limits.
    pub journal: GraphDeltaJournalLimits,
    /// Peak estimated logical state memory.
    pub max_memory_bytes: usize,
    /// Maximum temporary spill bytes.
    pub max_spill_bytes: u64,
    /// Maximum staged generation bytes before CURRENT publish.
    pub max_disk_bytes: u64,
    /// Maximum aggregate verified input runs.
    pub max_input_runs: u64,
    /// Maximum aggregate encoded bytes across the compacted run prefix.
    pub max_input_bytes: u64,
    /// Maximum canonical topology rows emitted.
    pub max_output_rows: u64,
    /// Maximum rows processed between cancellation checks.
    pub cancellation_check_rows: u64,
}

impl Default for GraphDeltaCompactionLimits {
    fn default() -> Self {
        Self {
            journal: GraphDeltaJournalLimits::default(),
            max_memory_bytes: DEFAULT_COMPACTION_MAX_MEMORY_BYTES,
            max_spill_bytes: DEFAULT_COMPACTION_MAX_SPILL_BYTES,
            max_disk_bytes: DEFAULT_COMPACTION_MAX_DISK_BYTES,
            max_input_runs: DEFAULT_COMPACTION_MAX_INPUT_RUNS,
            max_input_bytes: DEFAULT_COMPACTION_MAX_INPUT_BYTES,
            max_output_rows: DEFAULT_COMPACTION_MAX_OUTPUT_ROWS,
            cancellation_check_rows: DEFAULT_COMPACTION_CANCELLATION_CHECK_ROWS,
        }
    }
}

impl GraphDeltaCompactionLimits {
    /// Validate non-zero budgets.
    ///
    /// # Errors
    /// Returns `GF_RESOURCE_LIMIT` when any hard budget is zero.
    pub fn validate(self) -> Result<Self, GfError> {
        if self.max_memory_bytes == 0 || self.max_memory_bytes > MAX_COMPACTION_MEMORY_BYTES {
            return Err(resource_limit("compaction max_memory_bytes"));
        }
        if self.max_spill_bytes == 0 || self.max_spill_bytes > MAX_COMPACTION_SPILL_BYTES {
            return Err(resource_limit("compaction max_spill_bytes"));
        }
        if self.max_disk_bytes == 0 || self.max_disk_bytes > MAX_COMPACTION_DISK_BYTES {
            return Err(resource_limit("compaction max_disk_bytes"));
        }
        if self.max_input_runs == 0 || self.max_input_runs > DEFAULT_COMPACTION_MAX_INPUT_RUNS {
            return Err(resource_limit("compaction max_input_runs"));
        }
        if self.max_input_bytes == 0 || self.max_input_bytes > MAX_COMPACTION_INPUT_BYTES {
            return Err(resource_limit("compaction max_input_bytes"));
        }
        if self.max_output_rows == 0 || self.max_output_rows > MAX_COMPACTION_OUTPUT_ROWS {
            return Err(resource_limit("compaction max_output_rows"));
        }
        if self.cancellation_check_rows == 0
            || self.cancellation_check_rows > MAX_COMPACTION_CANCELLATION_CHECK_ROWS
            || self.cancellation_check_rows < self.journal.max_batch_rows as u64
        {
            return Err(resource_limit("compaction cancellation_check_rows"));
        }
        Ok(self)
    }
}

/// Optional policy triggers for compaction (no background daemon).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GraphDeltaCompactionPolicy {
    /// Compact when verified run count reaches this threshold.
    pub compact_when_runs: Option<u64>,
    /// Compact when verified run bytes reach this threshold.
    pub compact_when_run_bytes: Option<u64>,
    /// Compact when estimated replay memory reaches this threshold.
    pub compact_when_replay_memory_bytes: Option<u64>,
}

/// Status of whether CURRENT should be compacted under a policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphDeltaCompactionStatus {
    /// CURRENT generation.
    pub generation_uuid: Uuid,
    /// Verified contiguous run count.
    pub run_count: u64,
    /// Verified run bytes.
    pub run_bytes: u64,
    /// Estimated replay memory for the full chain.
    pub estimated_replay_memory_bytes: u64,
    /// Canonical fingerprint for CURRENT base + all runs.
    pub state_fingerprint: [u8; 32],
    /// Whether any configured policy trigger fires.
    pub should_compact: bool,
    /// Human-readable trigger reasons (empty when not triggered).
    pub trigger_reasons: Vec<String>,
}

/// Request to preview or execute compaction.
#[derive(Clone, Debug)]
pub struct GraphDeltaCompactionRequest {
    /// Caller-stable transaction UUID (publication idempotency).
    pub transaction_uuid: Uuid,
    /// Generation UUID to publish for the compacted child.
    pub generation_uuid: Uuid,
    /// Compact runs `1..=through_run_sequence`. `None` compact all runs.
    pub through_run_sequence: Option<u64>,
    /// Resource budgets.
    pub limits: GraphDeltaCompactionLimits,
    /// When true, run shared GC after a successful commit.
    pub cleanup_after_commit: bool,
    /// Retention policy used when `cleanup_after_commit` is set.
    pub cleanup_policy: ProjectRetentionPolicy,
    /// Retention limits used when `cleanup_after_commit` is set.
    pub cleanup_limits: ProjectRetentionLimits,
}

/// Progress / evidence report for preview or execute.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphDeltaCompactionReport {
    /// True when no CURRENT publication occurred.
    pub dry_run: bool,
    /// Input (parent) generation UUID.
    pub input_generation_uuid: Uuid,
    /// Output generation UUID when published.
    pub output_generation_uuid: Option<Uuid>,
    /// Verified input runs examined.
    pub input_runs: u64,
    /// Runs folded into the new Parquet base.
    pub compacted_runs: u64,
    /// Later runs re-encoded onto the compacted generation.
    pub retained_suffix_runs: u64,
    /// Framed records in the compacted prefix.
    pub input_rows: u64,
    /// Logical rows written into the compacted base (nodes + edges).
    pub output_rows: u64,
    /// Input run bytes compacted.
    pub input_bytes: u64,
    /// Staged output graph-tree bytes.
    pub output_bytes: u64,
    /// Spill bytes written during the merge.
    pub spill_bytes: u64,
    /// Peak estimated logical memory.
    pub peak_memory_bytes: u64,
    /// Wall time in milliseconds.
    pub elapsed_ms: u64,
    /// Canonical fingerprint after compaction (must match pre-compaction).
    pub state_fingerprint: [u8; 32],
    /// Underlying publication receipt when executed.
    pub publication: Option<ProjectPublicationReceipt>,
    /// Optional cleanup report from the shared GC oracle.
    pub cleanup: Option<ProjectCleanupReport>,
}

/// Preview compaction without publishing CURRENT.
///
/// # Errors
/// Fail-closed on corrupt chains, missing prefixes, budget exhaustion, or
/// cancellation.
pub fn preview_graph_delta_compaction(
    container_root: impl AsRef<Path>,
    request: &GraphDeltaCompactionRequest,
    cancel: Option<&AtomicBool>,
) -> Result<GraphDeltaCompactionReport, GfError> {
    preview_graph_delta_compaction_with_mode(
        container_root,
        request,
        cancel,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Preview compaction using the lifecycle mode established by the owning facade.
///
/// # Errors
/// Returns the same errors as [`preview_graph_delta_compaction`].
pub fn preview_graph_delta_compaction_with_mode(
    container_root: impl AsRef<Path>,
    request: &GraphDeltaCompactionRequest,
    cancel: Option<&AtomicBool>,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<GraphDeltaCompactionReport, GfError> {
    let started = Instant::now();
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        mode,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    let parent = resolve_project_generation(admission.root())?;
    let prepared = prepare_compaction(admission.root(), &parent, request, cancel)?;
    Ok(report_from_prepared(
        &prepared,
        true,
        None,
        None,
        elapsed_ms(started),
    ))
}

/// Compact a contiguous delta prefix into a new immutable Parquet generation.
///
/// # Errors
/// Fail-closed on corrupt chains, budget exhaustion, cancellation, disk
/// exhaustion, and publication conflicts. Crash before CURRENT leaves the
/// prior generation authoritative; acknowledgement follows ADR 0013/0018.
pub fn compact_graph_delta(
    container_root: impl AsRef<Path>,
    request: &GraphDeltaCompactionRequest,
    cancel: Option<&AtomicBool>,
) -> Result<GraphDeltaCompactionReport, GfError> {
    compact_graph_delta_with_mode(
        container_root,
        request,
        cancel,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Compact using the lifecycle mode established by the owning facade.
///
/// # Errors
/// Returns the same errors as [`compact_graph_delta`].
pub fn compact_graph_delta_with_mode(
    container_root: impl AsRef<Path>,
    request: &GraphDeltaCompactionRequest,
    cancel: Option<&AtomicBool>,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<GraphDeltaCompactionReport, GfError> {
    compact_graph_delta_after_prepare(container_root, request, cancel, mode, |_| Ok(()))
}

fn compact_graph_delta_after_prepare(
    container_root: impl AsRef<Path>,
    request: &GraphDeltaCompactionRequest,
    cancel: Option<&AtomicBool>,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
    before_stage: impl FnOnce(&Path) -> Result<(), GfError>,
) -> Result<GraphDeltaCompactionReport, GfError> {
    let started = Instant::now();
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        mode,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    let admitted_root = admission.root().to_owned();
    let root = admitted_root.as_path();
    let limits = request.limits.validate()?;

    if let Some(publication) = published_project_transaction(root, request.transaction_uuid)? {
        if publication.generation_uuid != request.generation_uuid {
            return Err(idempotency_conflict(
                "graph delta compaction transaction_uuid reused with different generation_uuid",
            ));
        }
        return replay_compaction_receipt(root, request, publication, elapsed_ms(started));
    }

    let parent = resolve_project_generation(root)?;
    let prepared = prepare_compaction(root, &parent, request, cancel)?;
    check_cancel(cancel)?;

    let staging = tempfile::tempdir().map_err(|error| {
        GfError::Storage(format!(
            "create graph delta compaction staging directory: {error}"
        ))
    })?;
    materialize_compacted_workspace(staging.path(), &prepared, &limits, cancel)?;
    let output_bytes = directory_byte_size(staging.path())?;
    if output_bytes > limits.max_disk_bytes {
        return Err(resource_limit("compaction staged disk bytes"));
    }

    let (inventory, files_participant) = capture_graph_files(staging.path())?;
    let mut participants = empty_workspace_participants()?;
    participants.insert(0, files_participant);
    let generation_request = ProjectGenerationRequest {
        transaction_uuid: request.transaction_uuid,
        generation_uuid: request.generation_uuid,
        capabilities: vec![
            ProjectCapability {
                capability_id: GRAPH_CAPABILITY_ID.into(),
                capability_version: GRAPH_CAPABILITY_VERSION,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };

    check_cancel(cancel)?;
    before_stage(root)?;
    let publication = match stage_project_generation_from_admitted_parent(
        admission,
        parent,
        &generation_request,
        Some(staging.path()),
    )? {
        ProjectStageOutcome::Staged(staged) => {
            // Pre-publication verification authenticates the exact bounded
            // materialization planned above without rebuilding whole-graph maps.
            verify_graph_tree(staging.path(), &inventory)?;
            if inventory_state_fingerprint(&inventory) != prepared.expected_fingerprint {
                return Err(corrupt(
                    "compacted generation fingerprint mismatch before CURRENT",
                ));
            }
            staged.validate(|_| Ok(()), |_, _| Ok(()))?.publish()?
        }
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
    };

    let cleanup = if request.cleanup_after_commit {
        Some(execute_project_cleanup_with_mode(
            root,
            request.cleanup_policy,
            request.cleanup_limits,
            mode,
        )?)
    } else {
        None
    };

    let mut report = report_from_prepared(
        &prepared,
        false,
        Some(publication),
        cleanup,
        elapsed_ms(started),
    );
    report.output_bytes = output_bytes;
    Ok(report)
}

/// Inspect whether CURRENT triggers compaction under `policy`.
///
/// # Errors
/// Propagates open/reconstruction failures.
pub fn graph_delta_compaction_status(
    container_root: impl AsRef<Path>,
    policy: GraphDeltaCompactionPolicy,
    limits: GraphDeltaJournalLimits,
) -> Result<GraphDeltaCompactionStatus, GfError> {
    graph_delta_compaction_status_with_mode(
        container_root,
        policy,
        limits,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Inspect compaction status using the lifecycle mode established by the owner.
///
/// # Errors
/// Returns the same errors as [`graph_delta_compaction_status`].
pub fn graph_delta_compaction_status_with_mode(
    container_root: impl AsRef<Path>,
    policy: GraphDeltaCompactionPolicy,
    limits: GraphDeltaJournalLimits,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<GraphDeltaCompactionStatus, GfError> {
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        mode,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    let resolved = resolve_project_generation(admission.root())?;
    let inventory = resolved
        .graph_files_inventory()?
        .ok_or_else(|| validation("CURRENT generation lacks graph/files inventory"))?;
    let materialized = tempfile::tempdir().map_err(|error| {
        GfError::Storage(format!("create bounded compaction status view: {error}"))
    })?;
    let (_, evidence) = crate::graph_delta_journal::materialize_replayed_graph_tree(
        &resolved.graph_tree_root(),
        &inventory,
        materialized.path(),
        limits,
    )?;
    let (materialized_inventory, _) = capture_graph_files(materialized.path())?;
    let run_count = evidence.runs_replayed;
    let run_bytes = evidence.run_bytes_validated;
    let estimated = evidence.estimated_replay_memory_bytes;
    let mut trigger_reasons = Vec::new();
    if let Some(threshold) = policy.compact_when_runs
        && run_count >= threshold
    {
        trigger_reasons.push(format!("run_count>={threshold}"));
    }
    if let Some(threshold) = policy.compact_when_run_bytes
        && run_bytes >= threshold
    {
        trigger_reasons.push(format!("run_bytes>={threshold}"));
    }
    if let Some(threshold) = policy.compact_when_replay_memory_bytes
        && estimated >= threshold
    {
        trigger_reasons.push(format!("replay_memory>={threshold}"));
    }
    Ok(GraphDeltaCompactionStatus {
        generation_uuid: resolved.generation_uuid(),
        run_count,
        run_bytes,
        estimated_replay_memory_bytes: estimated,
        state_fingerprint: inventory_state_fingerprint(&materialized_inventory),
        should_compact: !trigger_reasons.is_empty(),
        trigger_reasons,
    })
}

struct PreparedCompaction {
    input_generation_uuid: Uuid,
    input_runs: u64,
    compacted_runs: u64,
    retained_suffix_runs: u64,
    input_rows: u64,
    output_rows: u64,
    input_bytes: u64,
    output_bytes: u64,
    spill_bytes: u64,
    peak_memory_bytes: u64,
    expected_fingerprint: [u8; 32],
    materialized: tempfile::TempDir,
    materialized_inventory: GraphFilesInventory,
}

fn prepare_compaction(
    _root: &Path,
    parent: &crate::ResolvedProjectGeneration,
    request: &GraphDeltaCompactionRequest,
    cancel: Option<&AtomicBool>,
) -> Result<PreparedCompaction, GfError> {
    let limits = request.limits.validate()?;
    check_cancel(cancel)?;

    let parent_inventory = parent
        .graph_files_inventory()?
        .ok_or_else(|| validation("parent generation lacks graph/files inventory"))?;
    let parent_tree = parent.graph_tree_root();
    verify_graph_tree(&parent_tree, &parent_inventory)?;

    let runs = load_verified_delta_runs(&parent_tree, &parent_inventory, limits.journal)?;
    let input_runs = runs.len() as u64;
    if input_runs == 0 {
        return Err(validation(
            "graph delta compaction requires at least one verified run",
        ));
    }
    if input_runs > limits.max_input_runs {
        return Err(resource_limit("compaction aggregate input runs"));
    }
    let input_bytes = runs.iter().try_fold(0_u64, |total, run| {
        total
            .checked_add(run.bytes.len() as u64)
            .ok_or_else(|| resource_limit("compaction aggregate input bytes"))
    })?;
    if input_bytes > limits.max_input_bytes {
        return Err(resource_limit("compaction aggregate input bytes"));
    }

    let through = request.through_run_sequence.unwrap_or(input_runs);
    if through == 0 || through > input_runs {
        return Err(validation(
            "graph delta compaction through_run_sequence out of bounds",
        ));
    }
    if through != input_runs {
        return Err(validation(
            "GF_UNSUPPORTED_PROJECT_FORMAT: bounded v1 compaction requires the full verified delta chain",
        ));
    }

    let materialized = tempfile::tempdir().map_err(|error| {
        GfError::Storage(format!(
            "create bounded compaction materialization: {error}"
        ))
    })?;
    let (_, replay) = crate::graph_delta_journal::materialize_replayed_graph_tree(
        &parent_tree,
        &parent_inventory,
        materialized.path(),
        limits.journal,
    )?;
    let (materialized_inventory, _) = capture_graph_files(materialized.path())?;
    let output_bytes = materialized_inventory.total_byte_length;
    if output_bytes > limits.max_disk_bytes {
        return Err(resource_limit("compaction staged disk bytes"));
    }
    let expected_fingerprint = inventory_state_fingerprint(&materialized_inventory);
    let input_rows = runs.iter().map(|run| run.records.len() as u64).sum();
    let output_rows = canonical_topology_rows(
        materialized.path(),
        limits.journal.max_batch_rows,
        limits.cancellation_check_rows,
        cancel,
    )?;
    if output_rows > limits.max_output_rows {
        return Err(resource_limit("compaction output rows"));
    }
    let peak_memory_bytes = replay.estimated_replay_memory_bytes;
    enforce_memory(peak_memory_bytes, limits.max_memory_bytes)?;
    Ok(PreparedCompaction {
        input_generation_uuid: parent.generation_uuid(),
        input_runs,
        compacted_runs: through,
        retained_suffix_runs: 0,
        input_rows,
        output_rows,
        input_bytes,
        output_bytes,
        spill_bytes: 0,
        peak_memory_bytes,
        expected_fingerprint,
        materialized,
        materialized_inventory,
    })
}

fn materialize_compacted_workspace(
    workspace: &Path,
    prepared: &PreparedCompaction,
    _limits: &GraphDeltaCompactionLimits,
    cancel: Option<&AtomicBool>,
) -> Result<(), GfError> {
    check_cancel(cancel)?;
    crate::graph_files::materialize_graph_tree(
        prepared.materialized.path(),
        &prepared.materialized_inventory,
        workspace,
    )?;
    Ok(())
}

fn inventory_state_fingerprint(inventory: &GraphFilesInventory) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-materialized-graph-tree/1\n");
    for entry in &inventory.files {
        if entry.relative_path.starts_with("deltas/") {
            continue;
        }
        hasher.update(entry.relative_path.as_bytes());
        hasher.update(b"|");
        hasher.update(entry.content_sha256.as_bytes());
        hasher.update(b"\n");
    }
    hasher.finalize().into()
}

fn canonical_topology_rows(
    root: &Path,
    batch_rows: usize,
    cancellation_check_rows: u64,
    cancel: Option<&AtomicBool>,
) -> Result<u64, GfError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut paths = crate::mutator::node_parquet_files(root)?;
    let edges = root.join("topology/edges");
    if edges.exists() {
        for entry in fs::read_dir(&edges)
            .map_err(|error| storage("list compacted edge files", &edges, error))?
        {
            let path = entry
                .map_err(|error| storage("read compacted edge entry", &edges, error))?
                .path();
            if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                paths.push(path);
            }
        }
    }
    let mut rows = 0_u64;
    let mut rows_since_cancel = 0_u64;
    for path in paths {
        let input =
            File::open(&path).map_err(|error| storage("open compacted topology", &path, error))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(input)
            .map_err(|error| corrupt(format!("decode compacted topology metadata: {error}")))?
            .with_batch_size(batch_rows)
            .build()
            .map_err(|error| corrupt(format!("open compacted topology reader: {error}")))?;
        for batch in reader {
            let batch = batch
                .map_err(|error| corrupt(format!("read compacted topology batch: {error}")))?;
            rows = rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| resource_limit("compaction topology rows"))?;
            rows_since_cancel = rows_since_cancel.saturating_add(batch.num_rows() as u64);
            if rows_since_cancel >= cancellation_check_rows {
                check_cancel(cancel)?;
                rows_since_cancel = 0;
            }
        }
    }
    Ok(rows)
}

fn report_from_prepared(
    prepared: &PreparedCompaction,
    dry_run: bool,
    publication: Option<ProjectPublicationReceipt>,
    cleanup: Option<ProjectCleanupReport>,
    elapsed_ms: u64,
) -> GraphDeltaCompactionReport {
    GraphDeltaCompactionReport {
        dry_run,
        input_generation_uuid: prepared.input_generation_uuid,
        output_generation_uuid: publication.as_ref().map(|receipt| receipt.generation_uuid),
        input_runs: prepared.input_runs,
        compacted_runs: prepared.compacted_runs,
        retained_suffix_runs: prepared.retained_suffix_runs,
        input_rows: prepared.input_rows,
        output_rows: prepared.output_rows,
        input_bytes: prepared.input_bytes,
        output_bytes: prepared.output_bytes,
        spill_bytes: prepared.spill_bytes,
        peak_memory_bytes: prepared.peak_memory_bytes,
        elapsed_ms,
        state_fingerprint: prepared.expected_fingerprint,
        publication,
        cleanup,
    }
}

fn replay_compaction_receipt(
    root: &Path,
    request: &GraphDeltaCompactionRequest,
    publication: ProjectPublicationReceipt,
    elapsed_ms: u64,
) -> Result<GraphDeltaCompactionReport, GfError> {
    let resolved = resolve_project_generation(root)?;
    let inventory = resolved
        .graph_files_inventory()?
        .ok_or_else(|| corrupt("published compaction generation missing graph inventory"))?;
    let materialized = tempfile::tempdir().map_err(|error| {
        GfError::Storage(format!("create bounded compaction receipt view: {error}"))
    })?;
    let (_, evidence) = crate::graph_delta_journal::materialize_replayed_graph_tree(
        &resolved.graph_tree_root(),
        &inventory,
        materialized.path(),
        request.limits.journal,
    )?;
    let (materialized_inventory, _) = capture_graph_files(materialized.path())?;
    let runs = list_delta_runs(&inventory, request.limits.journal)?;
    Ok(GraphDeltaCompactionReport {
        dry_run: false,
        input_generation_uuid: resolved
            .parent_generation_uuid()
            .unwrap_or(resolved.generation_uuid()),
        output_generation_uuid: Some(publication.generation_uuid),
        input_runs: evidence.runs_replayed,
        compacted_runs: 0,
        retained_suffix_runs: runs.len() as u64,
        input_rows: evidence.records_seen,
        output_rows: canonical_topology_rows(
            materialized.path(),
            request.limits.journal.max_batch_rows,
            request.limits.cancellation_check_rows,
            None,
        )?,
        input_bytes: evidence.run_bytes_validated,
        output_bytes: inventory.total_byte_length,
        spill_bytes: 0,
        peak_memory_bytes: evidence.estimated_replay_memory_bytes,
        elapsed_ms,
        state_fingerprint: inventory_state_fingerprint(&materialized_inventory),
        publication: Some(publication),
        cleanup: None,
    })
}

fn enforce_memory(peak: u64, max_memory_bytes: usize) -> Result<(), GfError> {
    if peak > max_memory_bytes as u64 {
        return Err(resource_limit("compaction memory bytes"));
    }
    Ok(())
}

fn directory_byte_size(root: &Path) -> Result<u64, GfError> {
    let mut total = 0_u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let meta = fs::symlink_metadata(&path).map_err(|error| storage("stat", &path, error))?;
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_file() {
            total = total.saturating_add(meta.len());
            continue;
        }
        if meta.is_dir() {
            for entry in fs::read_dir(&path).map_err(|error| storage("read_dir", &path, error))? {
                stack.push(
                    entry
                        .map_err(|error| storage("read_dir entry", &path, error))?
                        .path(),
                );
            }
        }
    }
    Ok(total)
}

fn check_cancel(cancel: Option<&AtomicBool>) -> Result<(), GfError> {
    if cancel.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
        return Err(GfError::Execution(
            "GF_CANCELLED: graph delta compaction cancelled".into(),
        ));
    }
    Ok(())
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn corrupt(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: message.into(),
    }
}

fn resource_limit(message: impl Into<String>) -> GfError {
    GfError::Execution(format!("GF_RESOURCE_LIMIT: {}", message.into()))
}

fn idempotency_conflict(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::TransactionConflict,
        message: message.into(),
    }
}

fn storage(action: &str, path: &Path, error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("{action} at {}: {error}", path.display()))
}

#[cfg(test)]
mod crash_oracle_tests {
    use super::*;
    use crate::GraphDeltaOp;
    use crate::project_fault_oracle::{
        AuthorityClass, PublicationIds, PublicationPhase, default_durable_ids, expected_authority,
        publication_ops, simulate_crash,
    };

    fn publish_graph_base(root: &Path) {
        publish_graph_base_with_mode(
            root,
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
        );
    }

    fn publish_graph_base_with_mode(
        root: &Path,
        mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) {
        match mode {
            crate::filesystem_admission::ProjectLifecycleMode::Durable => {
                crate::open_or_initialize_project(root).unwrap();
            }
            crate::filesystem_admission::ProjectLifecycleMode::Ephemeral => {
                crate::open_or_initialize_ephemeral_project(root).unwrap();
            }
        }
        let workspace = tempfile::tempdir().unwrap();
        let mut writer = crate::GraphWriter::open_at(
            workspace.path(),
            graphforge_core::OntologyMode::Strict,
            1_700_000_000_000_000,
        )
        .unwrap();
        writer.flush().unwrap();
        let (_, files) = capture_graph_files(workspace.path()).unwrap();
        let mut participants = empty_workspace_participants().unwrap();
        participants.insert(0, files);
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: vec![
                ProjectCapability {
                    capability_id: GRAPH_CAPABILITY_ID.into(),
                    capability_version: GRAPH_CAPABILITY_VERSION,
                },
                ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation_with_graph_tree_mode(
                root,
                &request,
                Some(workspace.path()),
                mode,
            )
            .unwrap()
        else {
            panic!("base publication unexpectedly replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
    }

    fn publish_one_node_delta(root: &Path) -> Uuid {
        publish_one_node_delta_with_mode(
            root,
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
        )
    }

    fn publish_one_node_delta_with_mode(
        root: &Path,
        mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) -> Uuid {
        let generation_uuid = Uuid::now_v7();
        crate::publish_graph_delta_with_mode(
            root,
            &crate::GraphDeltaPublishRequest {
                transaction_uuid: Uuid::now_v7(),
                generation_uuid,
                run_uuid: Uuid::now_v7(),
                operations: vec![GraphDeltaOp {
                    operation_uuid: Uuid::now_v7(),
                    kind: crate::GraphDeltaOpKind::UpsertNode,
                    payload: crate::GraphDeltaPayload::UpsertNodeV2 {
                        node_uuid: Uuid::now_v7().hyphenated().to_string(),
                        node_id: 1,
                        type_ids: vec![1],
                        created_at_micros: 1,
                        updated_at_micros: 1,
                    },
                }],
                limits: GraphDeltaJournalLimits::default(),
            },
            mode,
        )
        .unwrap();
        generation_uuid
    }

    fn stage_graph_clone(root: &Path) -> (Uuid, Box<crate::StagedProjectGeneration>) {
        let current = resolve_project_generation(root).unwrap();
        let graph_tree = current.graph_tree_root();
        let (_, files) = capture_graph_files(&graph_tree).unwrap();
        let mut participants = empty_workspace_participants().unwrap();
        participants.insert(0, files);
        let generation_uuid = Uuid::now_v7();
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid,
            capabilities: vec![
                ProjectCapability {
                    capability_id: GRAPH_CAPABILITY_ID.into(),
                    capability_version: GRAPH_CAPABILITY_VERSION,
                },
                ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation_with_graph_tree(root, &request, Some(&graph_tree))
                .unwrap()
        else {
            panic!("clone publication unexpectedly replayed");
        };
        (generation_uuid, staged)
    }

    #[test]
    fn crash_oracle_before_and_after_ack_matches_frozen_contract() {
        let seed = 753u64;
        let ids = PublicationIds::from_seed(seed);
        for phase in [
            PublicationPhase::BeforeCurrentReplace,
            PublicationPhase::AfterCurrentReplace,
            PublicationPhase::AfterRootFsync,
        ] {
            let ops = publication_ops(ids, phase);
            let durable = default_durable_ids(&ops, phase);
            let report = simulate_crash(seed, phase, &durable).unwrap();
            assert_eq!(report.expected, expected_authority(phase));
            assert_eq!(report.actual, report.expected);
            match phase {
                PublicationPhase::BeforeCurrentReplace => {
                    assert_eq!(report.expected, AuthorityClass::PriorGeneration);
                }
                PublicationPhase::AfterCurrentReplace | PublicationPhase::AfterRootFsync => {
                    assert_eq!(report.expected, AuthorityClass::NewGeneration);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn prepared_compaction_fails_busy_behind_a_live_current_writer() {
        let root = tempfile::tempdir().unwrap();
        publish_graph_base(root.path());
        publish_one_node_delta(root.path());
        let (concurrent_generation, concurrent) = stage_graph_clone(root.path());
        let request = GraphDeltaCompactionRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            through_run_sequence: None,
            limits: GraphDeltaCompactionLimits::default(),
            cleanup_after_commit: false,
            cleanup_policy: ProjectRetentionPolicy::default(),
            cleanup_limits: ProjectRetentionLimits::default(),
        };

        let error = compact_graph_delta_after_prepare(
            root.path(),
            &request,
            None,
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
            |_| Ok(()),
        )
        .unwrap_err();

        assert_eq!(error.code(), "GF_WRITER_BUSY");
        concurrent
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            concurrent_generation
        );
        let current = resolve_project_generation(root.path()).unwrap();
        assert_eq!(
            list_delta_runs(
                &current.graph_files_inventory().unwrap().unwrap(),
                GraphDeltaJournalLimits::default()
            )
            .unwrap()
            .len(),
            1
        );
    }

    #[test]
    fn ephemeral_compaction_cleanup_keeps_the_original_lifecycle_mode() {
        let root = tempfile::tempdir().unwrap();
        let mode = crate::filesystem_admission::ProjectLifecycleMode::Ephemeral;
        publish_graph_base_with_mode(root.path(), mode);
        publish_one_node_delta_with_mode(root.path(), mode);
        let request = GraphDeltaCompactionRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            through_run_sequence: None,
            limits: GraphDeltaCompactionLimits::default(),
            cleanup_after_commit: true,
            cleanup_policy: ProjectRetentionPolicy::default(),
            cleanup_limits: ProjectRetentionLimits::default(),
        };

        let report = compact_graph_delta_with_mode(root.path(), &request, None, mode).unwrap();

        assert!(report.cleanup.is_some());
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            request.generation_uuid
        );
    }
}
