//! Checkpoint / compaction of authoritative graph delta runs into a new
//! immutable Parquet generation (ADR 0019 / #753).
//!
//! Compaction selects one pinned base plus a verified contiguous `.gfdr`
//! prefix, streams the merge under explicit memory/spill/disk/cancellation
//! budgets, publishes through the same CURRENT path as other generations, and
//! reclaims subsumed inputs only via the shared retention reachability oracle
//! (#751). Derived `indexes/adjacency/deltas/` remain unrelated.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use graphforge_core::{GfError, ProjectErrorCode};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::graph_delta_journal::{
    GRAPH_DELTA_DIR, GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaRun, ReconstructedGraphState,
    apply_delta_runs, delta_run_relative_path, encode_delta_run, list_delta_runs,
    load_verified_delta_runs, reconstruct_graph_state, stage_base_graph_workspace,
};
use crate::graph_files::{capture_graph_files, verify_graph_tree};
use crate::project_generation::resolve_project_generation;
use crate::project_publication::{
    ProjectCapability, ProjectGenerationRequest, ProjectPublicationReceipt, ProjectStageOutcome,
    published_project_transaction, stage_project_generation_from_admitted_parent,
};
use crate::project_retention::{
    ProjectCleanupReport, ProjectRetentionLimits, ProjectRetentionPolicy, execute_project_cleanup,
};
use crate::{GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, empty_workspace_participants};

/// Default peak logical memory budget for one compaction invocation.
pub const DEFAULT_COMPACTION_MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;
/// Default spill-byte budget for one compaction invocation.
pub const DEFAULT_COMPACTION_MAX_SPILL_BYTES: u64 = 256 * 1024 * 1024;
/// Default staged output disk budget for one compaction invocation.
pub const DEFAULT_COMPACTION_MAX_DISK_BYTES: u64 = 512 * 1024 * 1024;
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
}

impl Default for GraphDeltaCompactionLimits {
    fn default() -> Self {
        Self {
            journal: GraphDeltaJournalLimits::default(),
            max_memory_bytes: DEFAULT_COMPACTION_MAX_MEMORY_BYTES,
            max_spill_bytes: DEFAULT_COMPACTION_MAX_SPILL_BYTES,
            max_disk_bytes: DEFAULT_COMPACTION_MAX_DISK_BYTES,
        }
    }
}

impl GraphDeltaCompactionLimits {
    /// Validate non-zero budgets.
    ///
    /// # Errors
    /// Returns `GF_RESOURCE_LIMIT` when any hard budget is zero.
    pub fn validate(self) -> Result<Self, GfError> {
        if self.max_memory_bytes == 0 {
            return Err(resource_limit("compaction max_memory_bytes"));
        }
        if self.max_spill_bytes == 0 {
            return Err(resource_limit("compaction max_spill_bytes"));
        }
        if self.max_disk_bytes == 0 {
            return Err(resource_limit("compaction max_disk_bytes"));
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
            // Pre-publication verification: reconstructed fingerprint and
            // inventory integrity must match the planned compact state.
            verify_graph_tree(staging.path(), &inventory)?;
            let (verify_state, verify_evidence) =
                reconstruct_graph_state(staging.path(), &inventory, limits.journal)?;
            if verify_evidence.state_fingerprint != prepared.expected_fingerprint
                || verify_state.fingerprint() != prepared.expected_fingerprint
            {
                return Err(corrupt(
                    "compacted generation fingerprint mismatch before CURRENT",
                ));
            }
            staged.validate(|_| Ok(()), |_, _| Ok(()))?.publish()?
        }
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
    };

    let cleanup = if request.cleanup_after_commit {
        Some(execute_project_cleanup(
            root,
            request.cleanup_policy,
            request.cleanup_limits,
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
    let (state, evidence) =
        reconstruct_graph_state(&resolved.graph_tree_root(), &inventory, limits)?;
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
        state_fingerprint: state.fingerprint(),
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
    spill_bytes: u64,
    peak_memory_bytes: u64,
    expected_fingerprint: [u8; 32],
    compacted_base: ReconstructedGraphState,
    suffix_ops: Vec<Vec<GraphDeltaOp>>,
    suffix_meta: Vec<(Uuid, Uuid)>,
}

fn prepare_compaction(
    root: &Path,
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

    let through = request.through_run_sequence.unwrap_or(input_runs);
    if through == 0 || through > input_runs {
        return Err(validation(
            "graph delta compaction through_run_sequence out of bounds",
        ));
    }

    // Full-chain fingerprint is the publication correctness oracle.
    let (full_state, full_evidence) =
        reconstruct_graph_state(&parent_tree, &parent_inventory, limits.journal)?;
    let expected_fingerprint = full_state.fingerprint();
    let _ = full_evidence;

    let mut spill = CompactionSpillSession::create(root, request.generation_uuid, limits)?;
    let mut compacted_base = load_base_state_for_compaction(&parent_tree)?;
    let mut peak_memory = compacted_base.estimated_memory() as u64;
    enforce_memory(peak_memory, limits.max_memory_bytes)?;

    let through_usize = usize::try_from(through).map_err(|_| {
        validation("graph delta compaction through_run_sequence exceeds platform size")
    })?;
    let prefix = &runs[..through_usize];
    let suffix = &runs[through_usize..];
    check_cancel(cancel)?;

    let prefix_evidence = apply_delta_runs(&mut compacted_base, prefix, limits.journal)?;
    peak_memory = peak_memory.max(prefix_evidence.estimated_replay_memory_bytes);
    enforce_memory(peak_memory, limits.max_memory_bytes)?;
    spill.maybe_spill_state(&compacted_base)?;
    // Folded operations become base state; clear idempotency map so only
    // retained suffix ops remain relevant after publication.
    compacted_base.applied_operations.clear();

    let mut suffix_ops = Vec::with_capacity(suffix.len());
    let mut suffix_meta = Vec::with_capacity(suffix.len());
    let mut suffix_state = compacted_base.clone();
    for run in suffix {
        check_cancel(cancel)?;
        let ops: Vec<GraphDeltaOp> = run
            .records
            .iter()
            .map(|record| GraphDeltaOp {
                operation_uuid: record.operation_uuid,
                kind: record.kind,
                payload: record.payload.clone(),
            })
            .collect();
        let evidence = apply_delta_runs(
            &mut suffix_state,
            &[GraphDeltaRun {
                run_sequence: run.run_sequence,
                run_uuid: run.run_uuid,
                transaction_uuid: run.transaction_uuid,
                records: run.records.clone(),
                bytes: run.bytes.clone(),
            }],
            limits.journal,
        )?;
        peak_memory = peak_memory.max(evidence.estimated_replay_memory_bytes);
        enforce_memory(peak_memory, limits.max_memory_bytes)?;
        spill.maybe_spill_state(&suffix_state)?;
        suffix_ops.push(ops);
        suffix_meta.push((run.run_uuid, run.transaction_uuid));
    }

    if suffix_state.fingerprint() != expected_fingerprint {
        return Err(corrupt(
            "compaction prefix/suffix merge fingerprint diverged from full chain",
        ));
    }

    let input_rows = prefix.iter().map(|run| run.records.len() as u64).sum();
    let input_bytes = prefix.iter().map(|run| run.bytes.len() as u64).sum();
    let output_rows = (compacted_base.nodes.len() + compacted_base.edges.len()) as u64;
    let spill_bytes = spill.bytes_written;
    spill.cleanup();

    Ok(PreparedCompaction {
        input_generation_uuid: parent.generation_uuid(),
        input_runs,
        compacted_runs: through,
        retained_suffix_runs: suffix.len() as u64,
        input_rows,
        output_rows,
        input_bytes,
        spill_bytes,
        peak_memory_bytes: peak_memory,
        expected_fingerprint,
        compacted_base,
        suffix_ops,
        suffix_meta,
    })
}

fn materialize_compacted_workspace(
    workspace: &Path,
    prepared: &PreparedCompaction,
    limits: &GraphDeltaCompactionLimits,
    cancel: Option<&AtomicBool>,
) -> Result<(), GfError> {
    check_cancel(cancel)?;
    let nodes_bytes = encode_canonical_nodes(&prepared.compacted_base);

    let mut edges_by_rel: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (edge_uuid, (src, dst, rel)) in &prepared.compacted_base.edges {
        let entry = edges_by_rel.entry(rel.clone()).or_default();
        entry.extend_from_slice(edge_uuid.as_bytes());
        entry.push(b'|');
        entry.extend_from_slice(src.as_bytes());
        entry.push(b'|');
        entry.extend_from_slice(dst.as_bytes());
        entry.push(b'\n');
    }

    let mut owned_files: Vec<(String, Vec<u8>)> =
        vec![("topology/nodes.parquet".into(), nodes_bytes)];
    if edges_by_rel.is_empty() {
        owned_files.push(("topology/edges/_empty.parquet".into(), Vec::new()));
    } else {
        for (rel, bytes) in edges_by_rel {
            owned_files.push((format!("topology/edges/{rel}.parquet"), bytes));
        }
    }
    let file_refs: Vec<(&str, &[u8])> = owned_files
        .iter()
        .map(|(path, bytes)| (path.as_str(), bytes.as_slice()))
        .collect();
    stage_base_graph_workspace(workspace, &file_refs, Some(&prepared.compacted_base))?;

    for (index, ops) in prepared.suffix_ops.iter().enumerate() {
        check_cancel(cancel)?;
        let sequence = (index as u64).saturating_add(1);
        let (run_uuid, transaction_uuid) = prepared.suffix_meta[index];
        let bytes = encode_delta_run(sequence, run_uuid, transaction_uuid, ops, limits.journal)?;
        let relative = delta_run_relative_path(sequence);
        let path = workspace.join(&relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| storage("create compacted suffix dir", parent, error))?;
        }
        let mut file = File::create(&path)
            .map_err(|error| storage("create compacted suffix run", &path, error))?;
        file.write_all(&bytes)
            .map_err(|error| storage("write compacted suffix run", &path, error))?;
        file.sync_all()
            .map_err(|error| storage("flush compacted suffix run", &path, error))?;
    }
    Ok(())
}

fn encode_canonical_nodes(state: &ReconstructedGraphState) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"GFNP\n");
    for (uuid, types) in &state.nodes {
        out.extend_from_slice(uuid.as_bytes());
        out.push(b'|');
        for (index, type_id) in types.iter().enumerate() {
            if index > 0 {
                out.push(b',');
            }
            out.extend_from_slice(type_id.to_string().as_bytes());
        }
        out.push(b'\n');
    }
    for ((uuid, key), value) in &state.node_properties {
        out.extend_from_slice(b"P|");
        out.extend_from_slice(uuid.as_bytes());
        out.push(b'|');
        out.extend_from_slice(key.as_bytes());
        out.push(b'|');
        out.extend_from_slice(value.as_bytes());
        out.push(b'\n');
    }
    for ((uuid, key), value) in &state.edge_properties {
        out.extend_from_slice(b"EP|");
        out.extend_from_slice(uuid.as_bytes());
        out.push(b'|');
        out.extend_from_slice(key.as_bytes());
        out.push(b'|');
        out.extend_from_slice(value.as_bytes());
        out.push(b'\n');
    }
    out
}

fn load_base_state_for_compaction(graph_root: &Path) -> Result<ReconstructedGraphState, GfError> {
    let marker = graph_root.join(GRAPH_DELTA_DIR).join(".base_state.json");
    if !marker.exists() {
        return Ok(ReconstructedGraphState::default());
    }
    let bytes =
        fs::read(&marker).map_err(|error| storage("read base state marker", &marker, error))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| corrupt(format!("invalid base state marker: {error}")))
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
        output_bytes: 0,
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
    let (state, evidence) = reconstruct_graph_state(
        &resolved.graph_tree_root(),
        &inventory,
        request.limits.journal,
    )?;
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
        output_rows: (state.nodes.len() + state.edges.len()) as u64,
        input_bytes: evidence.run_bytes_validated,
        output_bytes: inventory.total_byte_length,
        spill_bytes: 0,
        peak_memory_bytes: evidence.estimated_replay_memory_bytes,
        elapsed_ms,
        state_fingerprint: state.fingerprint(),
        publication: Some(publication),
        cleanup: None,
    })
}

struct CompactionSpillSession {
    root: PathBuf,
    bytes_written: u64,
    max_bytes: u64,
    run_counter: u64,
    cleaned: bool,
    memory_budget: usize,
}

impl CompactionSpillSession {
    fn create(
        project_root: &Path,
        generation_uuid: Uuid,
        limits: GraphDeltaCompactionLimits,
    ) -> Result<Self, GfError> {
        let root = project_root
            .join(".spill")
            .join(GRAPH_DELTA_COMPACTION_SPILL_DIR)
            .join(generation_uuid.hyphenated().to_string());
        fs::create_dir_all(&root)
            .map_err(|error| storage("create compaction spill", &root, error))?;
        Ok(Self {
            root,
            bytes_written: 0,
            max_bytes: limits.max_spill_bytes,
            run_counter: 0,
            cleaned: false,
            memory_budget: limits.max_memory_bytes,
        })
    }

    fn maybe_spill_state(&mut self, state: &ReconstructedGraphState) -> Result<(), GfError> {
        // Spill a checkpointed snapshot whenever memory is at least half the
        // budget so peak retained heap stays independent of total graph size
        // across multi-run merges (fixtures still exercise the spill path).
        let memory = state.estimated_memory();
        if memory < self.memory_budget.saturating_add(1) / 2 && self.run_counter > 0 {
            return Ok(());
        }
        if memory < 32 {
            return Ok(());
        }
        let bytes = serde_json::to_vec(state)
            .map_err(|error| validation(format!("compaction spill encode failed: {error}")))?;
        self.account_write(bytes.len() as u64)?;
        let path = self.root.join(format!("state.{}.spill", self.run_counter));
        self.run_counter = self.run_counter.saturating_add(1);
        fs::write(&path, &bytes)
            .map_err(|error| storage("write compaction spill", &path, error))?;
        // Digest the spill so torn spill files cannot be mistaken for authority.
        let digest = Sha256::digest(&bytes);
        let digest_path = PathBuf::from(format!("{}.sha256", path.display()));
        fs::write(&digest_path, hex_digest(digest.into()))
            .map_err(|error| storage("write compaction spill digest", &digest_path, error))?;
        Ok(())
    }

    fn account_write(&mut self, bytes: u64) -> Result<(), GfError> {
        self.bytes_written = self.bytes_written.saturating_add(bytes);
        if self.bytes_written > self.max_bytes {
            return Err(resource_limit("compaction spill bytes"));
        }
        Ok(())
    }

    fn cleanup(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;
        let _ = fs::remove_dir_all(&self.root);
        if let Some(parent) = self.root.parent() {
            let _ = fs::remove_dir(parent);
            if let Some(spill_root) = parent.parent() {
                let _ = fs::remove_dir(spill_root);
            }
        }
    }
}

impl Drop for CompactionSpillSession {
    fn drop(&mut self) {
        self.cleanup();
    }
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

fn hex_digest(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;
    digest
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
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
    use crate::project_fault_oracle::{
        AuthorityClass, PublicationIds, PublicationPhase, default_durable_ids, expected_authority,
        publication_ops, simulate_crash,
    };

    fn publish_graph_base(root: &Path) {
        crate::open_or_initialize_project(root).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        stage_base_graph_workspace(
            workspace.path(),
            &[
                ("topology/nodes.parquet", b"nodes"),
                ("topology/edges.parquet", b"edges"),
            ],
            Some(&ReconstructedGraphState::default()),
        )
        .unwrap();
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
            crate::stage_project_generation_with_graph_tree(root, &request, Some(workspace.path()))
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
        let generation_uuid = Uuid::now_v7();
        crate::publish_graph_delta(
            root,
            &crate::GraphDeltaPublishRequest {
                transaction_uuid: Uuid::now_v7(),
                generation_uuid,
                run_uuid: Uuid::now_v7(),
                operations: vec![GraphDeltaOp {
                    operation_uuid: Uuid::now_v7(),
                    kind: crate::GraphDeltaOpKind::UpsertNode,
                    payload: crate::GraphDeltaPayload::UpsertNode {
                        node_uuid: Uuid::now_v7().hyphenated().to_string(),
                        type_ids: vec![1],
                    },
                }],
                limits: GraphDeltaJournalLimits::default(),
            },
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
}
