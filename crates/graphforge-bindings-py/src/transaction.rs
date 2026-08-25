//! Thin Python wrappers for the Rust-owned [`GraphTransaction`] lifecycle (#755).

use std::collections::HashMap;
use std::sync::Mutex;

use graphforge_api::{GfError, GraphTransaction, IrLiteral, TransactionPhase, WriteContext};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use uuid::Uuid;

fn fingerprint_hex(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

use crate::{
    GraphForge, PyCancellationToken, canonical_operation_id, props_from_dict, py_to_ir_literal,
    to_pyerr,
};

fn phase_name(phase: TransactionPhase) -> &'static str {
    match phase {
        TransactionPhase::Open => "open",
        TransactionPhase::Validated => "validated",
        TransactionPhase::Committed => "committed",
        TransactionPhase::RolledBack => "rolled_back",
        _ => "unknown",
    }
}

fn write_mode_name(mode: graphforge_api::ProjectWriteMode) -> &'static str {
    match mode {
        graphforge_api::ProjectWriteMode::SingleWriter => "single_writer",
        graphforge_api::ProjectWriteMode::QueuedWriter => "queued_writer",
        graphforge_api::ProjectWriteMode::OptimisticMultiWriter => "optimistic_multi_writer",
        _ => "unknown",
    }
}

fn parse_uuid(py: Python<'_>, value: &str, field: &str) -> PyResult<Uuid> {
    Uuid::parse_str(value).map_err(|_| {
        to_pyerr(
            py,
            &GfError::Validation(format!("{field} must be a canonical UUID string")),
        )
    })
}

fn params_from_dict(
    py: Python<'_>,
    value: Option<&Bound<'_, PyDict>>,
) -> PyResult<HashMap<String, IrLiteral>> {
    let Some(dict) = value else {
        return Ok(HashMap::new());
    };
    let mut params = HashMap::with_capacity(dict.len());
    for (key, item) in dict {
        params.insert(key.extract::<String>()?, py_to_ir_literal(&item)?);
    }
    let _ = py;
    Ok(params)
}

/// Explicit multi-mutation transaction handle. Drop / context exit rolls back
/// without publishing when the handle was not committed.
#[pyclass(name = "GraphTransaction", module = "graphforge")]
pub struct PyGraphTransaction {
    parent: Py<GraphForge>,
    inner: Mutex<Option<GraphTransaction>>,
}

impl PyGraphTransaction {
    pub(crate) fn new(parent: Py<GraphForge>, inner: GraphTransaction) -> Self {
        Self {
            parent,
            inner: Mutex::new(Some(inner)),
        }
    }

    fn with_tx<R>(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&GraphTransaction) -> Result<R, GfError>,
    ) -> PyResult<R> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| to_pyerr(py, &GfError::Validation("transaction lock poisoned".into())))?;
        let tx = guard.as_ref().ok_or_else(|| {
            to_pyerr(
                py,
                &GfError::Validation("transaction handle already released".into()),
            )
        })?;
        f(tx).map_err(|error| to_pyerr(py, &error))
    }
}

#[pymethods]
impl PyGraphTransaction {
    /// Safe lifecycle status snapshot (no graph content).
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let status = self.with_tx(py, GraphTransaction::status)?;
        let dict = PyDict::new(py);
        dict.set_item("operation_uuid", status.operation_uuid.to_string())?;
        dict.set_item(
            "base_generation_uuid",
            status.base_generation_uuid.to_string(),
        )?;
        dict.set_item("phase", phase_name(status.phase))?;
        dict.set_item("write_mode", write_mode_name(status.write_mode))?;
        dict.set_item("staged_entry_count", status.staged_entry_count)?;
        dict.set_item("committed", status.committed)?;
        Ok(dict)
    }

    /// Stage one write Cypher statement for deferred execution at commit.
    #[pyo3(signature = (query, params=None))]
    fn stage_cypher(
        &self,
        py: Python<'_>,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let params = params_from_dict(py, params)?;
        self.with_tx(py, |tx| tx.stage_cypher(query, params))
    }

    /// Stage scalar node construction.
    #[pyo3(signature = (node_uuid, label, properties=None))]
    fn stage_add_node(
        &self,
        py: Python<'_>,
        node_uuid: &str,
        label: &str,
        properties: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let node_uuid = parse_uuid(py, node_uuid, "node_uuid")?;
        let properties = props_from_dict(properties)?;
        self.with_tx(py, |tx| tx.stage_add_node(node_uuid, label, properties))
    }

    /// Stage scalar edge construction.
    #[pyo3(signature = (edge_uuid, rel_type, source_uuid, target_uuid, properties=None))]
    fn stage_add_edge(
        &self,
        py: Python<'_>,
        edge_uuid: &str,
        rel_type: &str,
        source_uuid: &str,
        target_uuid: &str,
        properties: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let edge_uuid = parse_uuid(py, edge_uuid, "edge_uuid")?;
        let source_uuid = parse_uuid(py, source_uuid, "source_uuid")?;
        let target_uuid = parse_uuid(py, target_uuid, "target_uuid")?;
        let properties = props_from_dict(properties)?;
        self.with_tx(py, |tx| {
            tx.stage_add_edge(edge_uuid, rel_type, source_uuid, target_uuid, properties)
        })
    }

    /// Validate staged content without publishing.
    fn validate(&self, py: Python<'_>) -> PyResult<()> {
        let parent = self.parent.clone_ref(py);
        self.with_tx(py, |tx| {
            let graph = parent.bind(py).borrow();
            tx.validate(&graph.inner)
        })
    }

    /// Publish every staged participant as one generation.
    #[pyo3(signature = (*, cancellation=None))]
    fn commit(
        &self,
        py: Python<'_>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<String> {
        let parent = self.parent.clone_ref(py);
        let cancellation = cancellation.map(|token| token.inner.clone());
        let receipt = self.with_tx(py, |tx| {
            let graph = parent.bind(py).borrow();
            tx.commit_with_cancellation(&graph.inner, cancellation.clone())
        })?;
        Ok(receipt.generation_uuid.to_string())
    }

    /// Abandon staged work without publishing.
    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        self.with_tx(py, GraphTransaction::rollback)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, _exc=None, _tb=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        let _ = exc_type;
        let _ = self.with_tx(py, |tx| {
            // Already finished (commit/rollback) is fine — Drop is the safety net.
            let _ = tx.rollback();
            Ok(())
        });
        false
    }
}

/// Begin an explicit transaction on `forge`.
pub(crate) fn begin_transaction(
    forge: &Bound<'_, GraphForge>,
    py: Python<'_>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
) -> PyResult<Py<PyGraphTransaction>> {
    if forge.borrow().closed {
        return Err(to_pyerr(
            py,
            &GfError::Lifecycle("operation on a closed GraphForge instance".into()),
        ));
    }
    let operation_uuid =
        canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
    let actor_uuid = actor_uuid
        .map(canonical_operation_id)
        .transpose()
        .map_err(|error| to_pyerr(py, &error))?
        .map(|id| id.0);
    let context = WriteContext {
        operation_uuid,
        actor_uuid,
    };
    let inner = forge
        .borrow()
        .inner
        .begin_transaction(context)
        .map_err(|error| to_pyerr(py, &error))?;
    let parent = forge.clone().unbind();
    Bound::new(py, PyGraphTransaction::new(parent, inner)).map(Bound::unbind)
}

/// Convert recovery evidence into a safe Python dict.
pub(crate) fn recovery_evidence_dict<'py>(
    py: Python<'py>,
    evidence: &graphforge_api::ProjectOpenRecoveryEvidence,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item(
        "kind",
        match evidence.kind {
            graphforge_api::ProjectOpenRecoveryKind::ProjectOpen => "project_open",
            graphforge_api::ProjectOpenRecoveryKind::Initialization => "initialization",
            graphforge_api::ProjectOpenRecoveryKind::CheckpointView => "checkpoint_view",
        },
    )?;
    dict.set_item(
        "selected_generation_uuid",
        evidence.selected_generation_uuid.to_string(),
    )?;
    dict.set_item(
        "selected_generation_class",
        match evidence.selected_generation_class {
            graphforge_api::ProjectRecoveryGenerationClass::CommittedCurrent => "committed_current",
            graphforge_api::ProjectRecoveryGenerationClass::CheckpointPinned => "checkpoint_pinned",
        },
    )?;
    dict.set_item("work_detected", evidence.work_detected)?;
    dict.set_item("repaired_journals", evidence.repaired_journals)?;
    dict.set_item("aborted_journals", evidence.aborted_journals)?;
    dict.set_item("removed_generations", evidence.removed_generations)?;
    dict.set_item(
        "preserved_unknown_entries",
        evidence.preserved_unknown_entries,
    )?;
    dict.set_item(
        "deferred",
        evidence.deferred.map(|deferral| match deferral {
            graphforge_api::ProjectRecoveryDeferral::LiveWriterOwnsKernelLock => {
                "live_writer_owns_kernel_lock"
            }
        }),
    )?;
    dict.set_item("elapsed_ms", evidence.elapsed_ms)?;
    Ok(dict)
}

fn retention_policy(retained_ancestors: Option<usize>) -> graphforge_api::ProjectRetentionPolicy {
    graphforge_api::ProjectRetentionPolicy {
        retained_ancestors: retained_ancestors
            .unwrap_or(graphforge_api::ProjectRetentionPolicy::default().retained_ancestors),
    }
}

fn retention_limits(
    max_entries: Option<usize>,
    max_bytes_scanned: Option<u64>,
    max_work_units: Option<usize>,
    cleanup_batch: Option<usize>,
) -> graphforge_api::ProjectRetentionLimits {
    let defaults = graphforge_api::ProjectRetentionLimits::default();
    graphforge_api::ProjectRetentionLimits {
        max_entries: max_entries.unwrap_or(defaults.max_entries),
        max_bytes_scanned: max_bytes_scanned.unwrap_or(defaults.max_bytes_scanned),
        max_work_units: max_work_units.unwrap_or(defaults.max_work_units),
        cleanup_batch: cleanup_batch.unwrap_or(defaults.cleanup_batch),
    }
}

fn cleanup_report_dict<'py>(
    py: Python<'py>,
    report: &graphforge_api::ProjectCleanupReport,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("dry_run", report.dry_run)?;
    dict.set_item(
        "selected_generation_uuid",
        report.selected_generation_uuid.to_string(),
    )?;
    dict.set_item("retained_ancestors", report.policy.retained_ancestors)?;
    dict.set_item("reachable_count", report.reachable_count)?;
    dict.set_item("candidates", report.candidates)?;
    dict.set_item("removed", report.removed)?;
    dict.set_item("skipped_live", report.skipped_live)?;
    dict.set_item("quarantined", report.quarantined)?;
    dict.set_item("unknown", report.unknown)?;
    dict.set_item("remaining_bytes", report.remaining_bytes)?;
    dict.set_item("bytes_scanned", report.bytes_scanned)?;
    dict.set_item("entries_scanned", report.entries_scanned)?;
    dict.set_item("work_units", report.work_units)?;
    dict.set_item("bounded", report.bounded)?;
    let graph_object_sweep = PyDict::new(py);
    graph_object_sweep.set_item(
        "disposition",
        report.graph_object_sweep.disposition.as_str(),
    )?;
    graph_object_sweep.set_item("objects_marked", report.graph_object_sweep.objects_marked)?;
    graph_object_sweep.set_item("objects_removed", report.graph_object_sweep.objects_removed)?;
    graph_object_sweep.set_item("bytes_removed", report.graph_object_sweep.bytes_removed)?;
    dict.set_item("graph_object_sweep", graph_object_sweep)?;
    dict.set_item("elapsed_ms", report.elapsed_ms)?;
    let entries = PyList::empty(py);
    for entry in &report.entries {
        let item = PyDict::new(py);
        item.set_item(
            "generation_uuid",
            entry.generation_uuid.map(|uuid| uuid.to_string()),
        )?;
        item.set_item("location", entry.location.as_str())?;
        item.set_item("disposition", entry.disposition.as_str())?;
        item.set_item("bytes", entry.bytes)?;
        entries.append(item)?;
    }
    dict.set_item("entries", entries)?;
    Ok(dict)
}

fn reachability_report_dict<'py>(
    py: Python<'py>,
    report: &graphforge_api::ProjectReachabilityReport,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item(
        "selected_generation_uuid",
        report.selected_generation_uuid.to_string(),
    )?;
    dict.set_item(
        "retained_ancestors_policy",
        report.policy.retained_ancestors,
    )?;
    dict.set_item(
        "reachable",
        report
            .reachable
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )?;
    dict.set_item(
        "checkpoint_roots",
        report
            .checkpoint_roots
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )?;
    dict.set_item(
        "retained_ancestors",
        report
            .retained_ancestors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )?;
    dict.set_item("entries_scanned", report.entries_scanned)?;
    dict.set_item("bytes_scanned", report.bytes_scanned)?;
    dict.set_item("elapsed_ms", report.elapsed_ms)?;
    Ok(dict)
}

fn compaction_report_dict<'py>(
    py: Python<'py>,
    report: &graphforge_api::GraphDeltaCompactionReport,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("dry_run", report.dry_run)?;
    dict.set_item(
        "input_generation_uuid",
        report.input_generation_uuid.to_string(),
    )?;
    dict.set_item(
        "output_generation_uuid",
        report.output_generation_uuid.map(|uuid| uuid.to_string()),
    )?;
    dict.set_item("input_runs", report.input_runs)?;
    dict.set_item("compacted_runs", report.compacted_runs)?;
    dict.set_item("retained_suffix_runs", report.retained_suffix_runs)?;
    dict.set_item("input_rows", report.input_rows)?;
    dict.set_item("output_rows", report.output_rows)?;
    dict.set_item("input_bytes", report.input_bytes)?;
    dict.set_item("output_bytes", report.output_bytes)?;
    dict.set_item("spill_bytes", report.spill_bytes)?;
    dict.set_item("peak_memory_bytes", report.peak_memory_bytes)?;
    dict.set_item("elapsed_ms", report.elapsed_ms)?;
    dict.set_item(
        "state_fingerprint",
        fingerprint_hex(&report.state_fingerprint),
    )?;
    if let Some(cleanup) = &report.cleanup {
        dict.set_item("cleanup", cleanup_report_dict(py, cleanup)?)?;
    } else {
        dict.set_item("cleanup", py.None())?;
    }
    Ok(dict)
}

fn compaction_status_dict<'py>(
    py: Python<'py>,
    status: &graphforge_api::GraphDeltaCompactionStatus,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("generation_uuid", status.generation_uuid.to_string())?;
    dict.set_item("run_count", status.run_count)?;
    dict.set_item("run_bytes", status.run_bytes)?;
    dict.set_item(
        "estimated_replay_memory_bytes",
        status.estimated_replay_memory_bytes,
    )?;
    dict.set_item(
        "state_fingerprint",
        fingerprint_hex(&status.state_fingerprint),
    )?;
    dict.set_item("should_compact", status.should_compact)?;
    dict.set_item("trigger_reasons", status.trigger_reasons.clone())?;
    Ok(dict)
}

fn parse_compaction_request(
    py: Python<'_>,
    transaction_uuid: &str,
    generation_uuid: &str,
    through_run_sequence: Option<u64>,
    cleanup_after_commit: bool,
    retained_ancestors: Option<usize>,
) -> PyResult<graphforge_api::GraphDeltaCompactionRequest> {
    Ok(graphforge_api::GraphDeltaCompactionRequest {
        transaction_uuid: parse_uuid(py, transaction_uuid, "transaction_uuid")?,
        generation_uuid: parse_uuid(py, generation_uuid, "generation_uuid")?,
        through_run_sequence,
        limits: graphforge_api::GraphDeltaCompactionLimits::default(),
        cleanup_after_commit,
        cleanup_policy: retention_policy(retained_ancestors),
        cleanup_limits: graphforge_api::ProjectRetentionLimits::default(),
    })
}

/// Shared maintenance helpers invoked from [`GraphForge`] pymethods.
pub(crate) mod ops {
    #![allow(clippy::wildcard_imports)] // thin re-exports of parent helpers
    #![allow(clippy::too_many_arguments)] // keyword-shaped maintenance surface

    use super::*;

    pub(crate) fn project_open_recovery<'py>(
        forge: &GraphForge,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyDict>> {
        recovery_evidence_dict(py, forge.inner.project_open_recovery())
    }

    pub(crate) fn inspect_project_reachability<'py>(
        forge: &GraphForge,
        py: Python<'py>,
        retained_ancestors: Option<usize>,
        max_entries: Option<usize>,
        max_bytes_scanned: Option<u64>,
        max_work_units: Option<usize>,
        cleanup_batch: Option<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let report = forge
            .inner
            .inspect_project_reachability(
                retention_policy(retained_ancestors),
                retention_limits(
                    max_entries,
                    max_bytes_scanned,
                    max_work_units,
                    cleanup_batch,
                ),
            )
            .map_err(|error| to_pyerr(py, &error))?;
        reachability_report_dict(py, &report)
    }

    pub(crate) fn preview_project_cleanup<'py>(
        forge: &GraphForge,
        py: Python<'py>,
        retained_ancestors: Option<usize>,
        max_entries: Option<usize>,
        max_bytes_scanned: Option<u64>,
        max_work_units: Option<usize>,
        cleanup_batch: Option<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let report = forge
            .inner
            .preview_project_cleanup(
                retention_policy(retained_ancestors),
                retention_limits(
                    max_entries,
                    max_bytes_scanned,
                    max_work_units,
                    cleanup_batch,
                ),
            )
            .map_err(|error| to_pyerr(py, &error))?;
        cleanup_report_dict(py, &report)
    }

    pub(crate) fn execute_project_cleanup<'py>(
        forge: &GraphForge,
        py: Python<'py>,
        retained_ancestors: Option<usize>,
        max_entries: Option<usize>,
        max_bytes_scanned: Option<u64>,
        max_work_units: Option<usize>,
        cleanup_batch: Option<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let report = forge
            .inner
            .execute_project_cleanup(
                retention_policy(retained_ancestors),
                retention_limits(
                    max_entries,
                    max_bytes_scanned,
                    max_work_units,
                    cleanup_batch,
                ),
            )
            .map_err(|error| to_pyerr(py, &error))?;
        cleanup_report_dict(py, &report)
    }

    pub(crate) fn graph_delta_compaction_status<'py>(
        forge: &GraphForge,
        py: Python<'py>,
        compact_when_runs: Option<u64>,
        compact_when_run_bytes: Option<u64>,
        compact_when_replay_memory_bytes: Option<u64>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let status = forge
            .inner
            .graph_delta_compaction_status(
                graphforge_api::GraphDeltaCompactionPolicy {
                    compact_when_runs,
                    compact_when_run_bytes,
                    compact_when_replay_memory_bytes,
                },
                graphforge_api::GraphDeltaJournalLimits::default(),
            )
            .map_err(|error| to_pyerr(py, &error))?;
        compaction_status_dict(py, &status)
    }

    pub(crate) fn preview_graph_delta_compaction<'py>(
        forge: &GraphForge,
        py: Python<'py>,
        transaction_uuid: &str,
        generation_uuid: &str,
        through_run_sequence: Option<u64>,
        cleanup_after_commit: bool,
        retained_ancestors: Option<usize>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let request = parse_compaction_request(
            py,
            transaction_uuid,
            generation_uuid,
            through_run_sequence,
            cleanup_after_commit,
            retained_ancestors,
        )?;
        let report = forge
            .inner
            .preview_graph_delta_compaction(&request, cancellation.map(|token| &token.inner))
            .map_err(|error| to_pyerr(py, &error))?;
        compaction_report_dict(py, &report)
    }

    pub(crate) fn compact_graph_delta<'py>(
        forge: &GraphForge,
        py: Python<'py>,
        transaction_uuid: &str,
        generation_uuid: &str,
        through_run_sequence: Option<u64>,
        cleanup_after_commit: bool,
        retained_ancestors: Option<usize>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let request = parse_compaction_request(
            py,
            transaction_uuid,
            generation_uuid,
            through_run_sequence,
            cleanup_after_commit,
            retained_ancestors,
        )?;
        let report = forge
            .inner
            .compact_graph_delta(&request, cancellation.map(|token| &token.inner))
            .map_err(|error| to_pyerr(py, &error))?;
        compaction_report_dict(py, &report)
    }
}
