//! Lifecycle Python binding methods and conversions.

use super::{
    GraphForge, PyCancellationToken, adjacency_inspection_to_python, canonical_operation_id,
    py_operation_id, result_to_pyarrow, to_pyerr,
};
use crate::import_session;
use crate::portable;
use crate::transaction;
use graphforge_api::CommittedGenerationIdentity;
use graphforge_api::GenerationDiffDisposition;
use graphforge_api::GenerationDiffLimits;
use graphforge_api::GenerationDiffRequest;
use graphforge_api::GenerationGraphDiff;
use graphforge_api::GfError;
use graphforge_api::GraphChangeStream;
use graphforge_api::ReloadRequiredReason;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use uuid::Uuid;

/// Read-only native facade pinned to one checkpoint generation.
#[pyclass(name = "CheckpointView", module = "graphforge")]
pub struct PyCheckpointView {
    inner: graphforge_api::CheckpointView,
}

#[pymethods]
impl PyCheckpointView {
    /// Stable UUID of the named checkpoint.
    #[getter]
    fn checkpoint_uuid(&self) -> String {
        self.inner.checkpoint_uuid().to_string()
    }

    /// UUID of the immutable generation pinned by this view.
    #[getter]
    fn generation_uuid(&self) -> String {
        self.inner.generation_uuid().to_string()
    }

    /// Execute one read-only Cypher query and return a `pyarrow.Table`.
    fn execute(&self, py: Python<'_>, query: &str) -> PyResult<Py<PyAny>> {
        let query = query.to_owned();
        let result = py
            .detach(|| self.inner.execute(&query))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return pinned project capabilities as a `pyarrow.Table`.
    fn project_capabilities(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let result = py
            .detach(|| self.inner.project_capabilities())
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Inspect adjacency freshness from the pinned generation.
    fn inspect_adjacency(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inspection = py
            .detach(|| self.inner.inspect_adjacency())
            .map_err(|error| to_pyerr(py, &error))?;
        adjacency_inspection_to_python(py, inspection)
    }
}

fn py_generation_identity(
    py: Python<'_>,
    generation_uuid: &Bound<'_, PyAny>,
    manifest_sha256: &Bound<'_, PyAny>,
) -> PyResult<CommittedGenerationIdentity> {
    let uuid_bytes: Vec<u8> = generation_uuid.extract()?;
    let manifest: Vec<u8> = manifest_sha256.extract()?;
    let generation_uuid = Uuid::from_slice(&uuid_bytes).map_err(|_| {
        to_pyerr(
            py,
            &GfError::Validation("generation_uuid must contain exactly 16 bytes".into()),
        )
    })?;
    let manifest_sha256: [u8; 32] = manifest.try_into().map_err(|_| {
        to_pyerr(
            py,
            &GfError::Validation("manifest_sha256 must contain exactly 32 bytes".into()),
        )
    })?;
    Ok(CommittedGenerationIdentity {
        generation_uuid,
        manifest_sha256,
    })
}

fn py_identity_dict(
    py: Python<'_>,
    identity: CommittedGenerationIdentity,
) -> PyResult<Bound<'_, PyDict>> {
    let out = PyDict::new(py);
    out.set_item(
        "generation_uuid",
        PyBytes::new(py, identity.generation_uuid.as_bytes()),
    )?;
    out.set_item(
        "manifest_sha256",
        PyBytes::new(py, &identity.manifest_sha256),
    )?;
    Ok(out)
}

fn py_change_stream<'py>(
    py: Python<'py>,
    stream: &GraphChangeStream,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("row_count", stream.row_count)?;
    out.set_item("ipc", PyBytes::new(py, &stream.ipc))?;
    Ok(out)
}

fn py_generation_diff<'py>(
    py: Python<'py>,
    diff: &GenerationGraphDiff,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("kind", "ready")?;
    out.set_item("source", py_identity_dict(py, diff.source)?)?;
    out.set_item("target", py_identity_dict(py, diff.target)?)?;
    for (name, stream) in [
        ("added_nodes", &diff.added_nodes),
        ("removed_nodes", &diff.removed_nodes),
        ("modified_nodes", &diff.modified_nodes),
        ("added_edges", &diff.added_edges),
        ("removed_edges", &diff.removed_edges),
        ("modified_edges", &diff.modified_edges),
    ] {
        out.set_item(name, py_change_stream(py, stream)?)?;
    }
    let node_properties = PyDict::new(py);
    for (uuid, names) in &diff.modified_node_properties {
        node_properties.set_item(PyBytes::new(py, uuid.as_bytes()), names)?;
    }
    let edge_properties = PyDict::new(py);
    for (uuid, names) in &diff.modified_edge_properties {
        edge_properties.set_item(PyBytes::new(py, uuid.as_bytes()), names)?;
    }
    out.set_item("modified_node_properties", node_properties)?;
    out.set_item("modified_edge_properties", edge_properties)?;
    out.set_item(
        "checkpoint_binding",
        PyBytes::new(py, &diff.checkpoint_binding),
    )?;
    Ok(out)
}

fn reload_reason(reason: ReloadRequiredReason) -> &'static str {
    match reason {
        ReloadRequiredReason::GenerationUnavailable => "generation_unavailable",
        ReloadRequiredReason::IdentityMismatch => "identity_mismatch",
        ReloadRequiredReason::CorruptGeneration => "corrupt_generation",
        ReloadRequiredReason::IncompatibleGraph => "incompatible_graph",
        ReloadRequiredReason::ResourceLimit => "resource_limit",
    }
}

#[pymethods]
impl GraphForge {
    /// Inspect the committed project capability manifest as a `pyarrow.Table`.
    fn project_capabilities(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let result = py
            .detach(|| self.inner.project_capabilities())
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Create a durable named checkpoint and return its native Arrow receipt.
    #[pyo3(signature = (*, name, idempotency_key, description=None, actor_uuid=None))]
    fn checkpoint(
        &self,
        py: Python<'_>,
        name: String,
        idempotency_key: &Bound<'_, PyAny>,
        description: Option<String>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request = graphforge_api::CheckpointRequest {
            name,
            description,
            idempotency_key: py_operation_id(idempotency_key)
                .map_err(|error| to_pyerr(py, &error))?,
            actor_uuid: actor_uuid
                .map(canonical_operation_id)
                .transpose()
                .map_err(|error| to_pyerr(py, &error))?
                .map(|id| id.0),
        };
        let result = py
            .detach(|| self.inner.checkpoint(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// List active checkpoints with native pagination and cancellation.
    #[pyo3(signature = (*, limit=100, after=None, cancellation=None))]
    fn list_checkpoints(
        &self,
        py: Python<'_>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_checkpoints(graphforge_api::ListCheckpointsRequest {
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Open an immutable view pinned to one named checkpoint.
    fn open_checkpoint(&self, py: Python<'_>, name: &str) -> PyResult<PyCheckpointView> {
        self.ensure_open()?;
        let name = name.to_owned();
        py.detach(|| self.inner.open_checkpoint(&name))
            .map(|inner| PyCheckpointView { inner })
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Delete an active checkpoint reference and return its Arrow receipt.
    #[pyo3(signature = (*, name, idempotency_key, actor_uuid=None))]
    fn delete_checkpoint(
        &self,
        py: Python<'_>,
        name: String,
        idempotency_key: &Bound<'_, PyAny>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request = graphforge_api::DeleteCheckpointRequest {
            name,
            idempotency_key: py_operation_id(idempotency_key)
                .map_err(|error| to_pyerr(py, &error))?,
            actor_uuid: actor_uuid
                .map(canonical_operation_id)
                .transpose()
                .map_err(|error| to_pyerr(py, &error))?
                .map(|id| id.0),
        };
        let result = py
            .detach(|| self.inner.delete_checkpoint(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Restore a complete workspace from a checkpoint using an audited reason.
    #[pyo3(signature = (*, name, reason, idempotency_key, actor_uuid=None))]
    fn revert_to_checkpoint(
        &mut self,
        py: Python<'_>,
        name: String,
        reason: String,
        idempotency_key: &Bound<'_, PyAny>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request = graphforge_api::RevertCheckpointRequest {
            name,
            reason,
            idempotency_key: py_operation_id(idempotency_key)
                .map_err(|error| to_pyerr(py, &error))?,
            actor_uuid: actor_uuid
                .map(canonical_operation_id)
                .transpose()
                .map_err(|error| to_pyerr(py, &error))?
                .map(|id| id.0),
        };
        let result = py
            .detach(|| self.inner.revert_to_checkpoint(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return the exact binary identity of the selected committed generation.
    fn committed_generation_identity(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let identity = self
            .inner
            .committed_generation_identity()
            .map_err(|error| to_pyerr(py, &error))?;
        Ok(py_identity_dict(py, identity)?.into_any().unbind())
    }

    /// Return Rust-owned semantic Arrow IPC changes between two generations.
    #[pyo3(signature = (*, source_generation_uuid, source_manifest_sha256, target_generation_uuid, target_manifest_sha256, max_records_per_generation=1_000_000, max_output_bytes=268_435_456, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn diff_committed_generations(
        &self,
        py: Python<'_>,
        source_generation_uuid: &Bound<'_, PyAny>,
        source_manifest_sha256: &Bound<'_, PyAny>,
        target_generation_uuid: &Bound<'_, PyAny>,
        target_manifest_sha256: &Bound<'_, PyAny>,
        max_records_per_generation: usize,
        max_output_bytes: usize,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request = GenerationDiffRequest {
            source: py_generation_identity(py, source_generation_uuid, source_manifest_sha256)?,
            target: py_generation_identity(py, target_generation_uuid, target_manifest_sha256)?,
            limits: GenerationDiffLimits {
                max_records_per_generation,
                max_output_bytes,
            },
            cancellation: cancellation.map(|token| token.inner.clone()),
        };
        let disposition = py
            .detach(|| self.inner.diff_committed_generations(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        let out = match disposition {
            GenerationDiffDisposition::Ready(diff) => py_generation_diff(py, &diff)?,
            GenerationDiffDisposition::ReloadRequired(reason) => {
                let out = PyDict::new(py);
                out.set_item("kind", "reload_required")?;
                out.set_item("reason", reload_reason(reason))?;
                out
            }
        };
        Ok(out.into_any().unbind())
    }

    /// Preview one content-free portable-v2 component selection.
    #[pyo3(signature = (*, checkpoint=None, profile="complete", identities=None, strict=false, limits=None))]
    fn preview_portable_v2_selection(
        &self,
        py: Python<'_>,
        checkpoint: Option<String>,
        profile: &str,
        identities: Option<&Bound<'_, PyList>>,
        strict: bool,
        limits: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        portable::preview_portable_v2_selection(
            self, py, checkpoint, profile, identities, strict, limits,
        )
    }

    /// Preview one content-free portable-v2 graph-data subset.
    #[pyo3(signature = (*, subset, checkpoint=None, limits=None))]
    fn preview_portable_v2_graph_subset(
        &self,
        py: Python<'_>,
        subset: &Bound<'_, PyDict>,
        checkpoint: Option<String>,
        limits: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        portable::preview_portable_v2_graph_subset(self, py, checkpoint, subset, limits)
    }

    /// Export one pinned generation as an expanded or bundled portable-v2 package.
    #[pyo3(signature = (*, output_path, representation="bundle", profile="complete", identities=None, checkpoint=None, subset=None, limits=None, cancellation=None, progress=None))]
    #[allow(clippy::too_many_arguments)]
    fn export_portable_v2(
        &self,
        py: Python<'_>,
        output_path: &str,
        representation: &str,
        profile: &str,
        identities: Option<&Bound<'_, PyList>>,
        checkpoint: Option<String>,
        subset: Option<&Bound<'_, PyDict>>,
        limits: Option<&Bound<'_, PyDict>>,
        cancellation: Option<&PyCancellationToken>,
        progress: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        portable::export_portable_v2(
            self,
            py,
            output_path,
            representation,
            profile,
            identities,
            checkpoint,
            subset,
            limits,
            cancellation,
            progress,
        )
    }

    /// Verify portable-v2 content without opening or mutating a project.
    #[staticmethod]
    #[pyo3(signature = (input, *, mode="full", limits=None, cancellation=None))]
    fn verify_portable_v2(
        py: Python<'_>,
        input: &str,
        mode: &str,
        limits: Option<&Bound<'_, PyDict>>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        portable::verify_portable_v2(py, input, mode, limits, cancellation)
    }

    /// Verify and atomically import a complete portable-v2 package.
    #[staticmethod]
    #[pyo3(signature = (project_root, *, input, operation_id, limits=None, cancellation=None))]
    fn import_portable_v2(
        py: Python<'_>,
        project_root: &str,
        input: &str,
        operation_id: &str,
        limits: Option<&Bound<'_, PyDict>>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        portable::import_portable_v2(py, project_root, input, operation_id, limits, cancellation)
    }

    /// Publish a verified portable-v2 package to an OCI Distribution registry.
    #[staticmethod]
    #[pyo3(signature = (*, package_path, registry, repository, tag=None, limits=None, authenticity=None, signature=None, insecure_http=false, credential=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn publish_portable_v2_oci(
        py: Python<'_>,
        package_path: &str,
        registry: &str,
        repository: &str,
        tag: Option<String>,
        limits: Option<&Bound<'_, PyDict>>,
        authenticity: Option<&Bound<'_, PyDict>>,
        signature: Option<&Bound<'_, PyDict>>,
        insecure_http: bool,
        credential: Option<String>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        portable::publish_portable_v2_oci(
            py,
            package_path,
            registry,
            repository,
            tag,
            limits,
            authenticity,
            signature,
            insecure_http,
            credential,
            cancellation,
        )
    }

    /// Pull and verify a portable-v2 package from an OCI Distribution registry.
    #[staticmethod]
    #[pyo3(signature = (*, registry, repository, reference, destination, expected_oci_digest=None, limits=None, authenticity=None, insecure_http=false, credential=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn pull_portable_v2_oci(
        py: Python<'_>,
        registry: &str,
        repository: &str,
        reference: &str,
        destination: &str,
        expected_oci_digest: Option<String>,
        limits: Option<&Bound<'_, PyDict>>,
        authenticity: Option<&Bound<'_, PyDict>>,
        insecure_http: bool,
        credential: Option<String>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        portable::pull_portable_v2_oci(
            py,
            registry,
            repository,
            reference,
            destination,
            expected_oci_digest,
            limits,
            authenticity,
            insecure_http,
            credential,
            cancellation,
        )
    }

    /// Begin a durable staged import session.
    #[pyo3(signature = (*, operation_uuid, batch_rows=None, max_source_bytes=None, max_files=None, max_rejected_rows=None, io_concurrency=None))]
    #[allow(clippy::too_many_arguments)]
    fn begin_import_session(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        operation_uuid: &str,
        batch_rows: Option<usize>,
        max_source_bytes: Option<u64>,
        max_files: Option<u64>,
        max_rejected_rows: Option<u64>,
        io_concurrency: Option<usize>,
    ) -> PyResult<Py<import_session::PyGraphImportSession>> {
        import_session::begin_import_session(
            slf,
            py,
            operation_uuid,
            batch_rows,
            max_source_bytes,
            max_files,
            max_rejected_rows,
            io_concurrency,
        )
    }

    /// Resume one durable, non-terminal import session.
    #[pyo3(signature = (session_uuid,))]
    fn resume_import_session(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        session_uuid: &str,
    ) -> PyResult<Py<import_session::PyGraphImportSession>> {
        import_session::resume_import_session(slf, py, session_uuid)
    }

    /// Abort and remove non-terminal sessions older than `max_age_secs`.
    #[pyo3(signature = (*, max_age_secs))]
    fn cleanup_stale_import_sessions(&self, py: Python<'_>, max_age_secs: u64) -> PyResult<u64> {
        import_session::cleanup_stale_import_sessions(self, py, max_age_secs)
    }

    /// Diff two checkpoint/current endpoints through the Rust-owned engine.
    #[pyo3(signature = (*, from_checkpoint=None, to_checkpoint=None, scope="summary", detail="summary", limit=100, after=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn diff_checkpoints(
        &self,
        py: Python<'_>,
        from_checkpoint: Option<String>,
        to_checkpoint: Option<String>,
        scope: &str,
        detail: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let selector = |name: Option<String>| {
            name.map_or(
                graphforge_api::CheckpointSelector::Current,
                graphforge_api::CheckpointSelector::Named,
            )
        };
        let scope = match scope {
            "summary" => graphforge_api::CheckpointDiffScope::Summary,
            "graph" => graphforge_api::CheckpointDiffScope::Graph,
            "ontology" => graphforge_api::CheckpointDiffScope::Ontology,
            "configuration" => graphforge_api::CheckpointDiffScope::Configuration,
            "capabilities" => graphforge_api::CheckpointDiffScope::Capabilities,
            "provenance" => graphforge_api::CheckpointDiffScope::Provenance,
            "knowledge" => graphforge_api::CheckpointDiffScope::Knowledge,
            "epistemic" => graphforge_api::CheckpointDiffScope::Epistemic,
            "all" => graphforge_api::CheckpointDiffScope::All,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("invalid checkpoint diff scope".into()),
                ));
            }
        };
        let detail = match detail {
            "summary" => graphforge_api::CheckpointDiffDetail::Summary,
            "records" => graphforge_api::CheckpointDiffDetail::Records,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("invalid checkpoint diff detail".into()),
                ));
            }
        };
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .diff_checkpoints(graphforge_api::DiffCheckpointsRequest {
                        from: selector(from_checkpoint),
                        to: selector(to_checkpoint),
                        scope,
                        detail,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Begin an explicit multi-mutation transaction (Rust-owned lifecycle).
    #[pyo3(signature = (*, operation_uuid, actor_uuid=None))]
    fn begin_transaction(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<transaction::PyGraphTransaction>> {
        transaction::begin_transaction(slf, py, operation_uuid, actor_uuid)
    }

    /// Safe recovery-on-open evidence for this instance.
    fn project_open_recovery<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.ensure_open()?;
        transaction::ops::project_open_recovery(self, py)
    }

    /// Inspect verified generation reachability for retention/GC planning.
    #[pyo3(signature = (*, retained_ancestors=None, max_entries=None, max_bytes_scanned=None, max_work_units=None, cleanup_batch=None))]
    fn inspect_project_reachability<'py>(
        &self,
        py: Python<'py>,
        retained_ancestors: Option<usize>,
        max_entries: Option<usize>,
        max_bytes_scanned: Option<u64>,
        max_work_units: Option<usize>,
        cleanup_batch: Option<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.ensure_open()?;
        transaction::ops::inspect_project_reachability(
            self,
            py,
            retained_ancestors,
            max_entries,
            max_bytes_scanned,
            max_work_units,
            cleanup_batch,
        )
    }

    /// Preview retention/GC candidates without removing anything.
    #[pyo3(signature = (*, retained_ancestors=None, max_entries=None, max_bytes_scanned=None, max_work_units=None, cleanup_batch=None))]
    fn preview_project_cleanup<'py>(
        &self,
        py: Python<'py>,
        retained_ancestors: Option<usize>,
        max_entries: Option<usize>,
        max_bytes_scanned: Option<u64>,
        max_work_units: Option<usize>,
        cleanup_batch: Option<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.ensure_open()?;
        transaction::ops::preview_project_cleanup(
            self,
            py,
            retained_ancestors,
            max_entries,
            max_bytes_scanned,
            max_work_units,
            cleanup_batch,
        )
    }

    /// Execute retention/GC for unreachable generations.
    #[pyo3(signature = (*, retained_ancestors=None, max_entries=None, max_bytes_scanned=None, max_work_units=None, cleanup_batch=None))]
    fn execute_project_cleanup<'py>(
        &self,
        py: Python<'py>,
        retained_ancestors: Option<usize>,
        max_entries: Option<usize>,
        max_bytes_scanned: Option<u64>,
        max_work_units: Option<usize>,
        cleanup_batch: Option<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.ensure_open()?;
        transaction::ops::execute_project_cleanup(
            self,
            py,
            retained_ancestors,
            max_entries,
            max_bytes_scanned,
            max_work_units,
            cleanup_batch,
        )
    }

    /// Report whether CURRENT's verified delta chain should compact.
    #[pyo3(signature = (*, compact_when_runs=None, compact_when_run_bytes=None, compact_when_replay_memory_bytes=None))]
    fn graph_delta_compaction_status<'py>(
        &self,
        py: Python<'py>,
        compact_when_runs: Option<u64>,
        compact_when_run_bytes: Option<u64>,
        compact_when_replay_memory_bytes: Option<u64>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.ensure_open()?;
        transaction::ops::graph_delta_compaction_status(
            self,
            py,
            compact_when_runs,
            compact_when_run_bytes,
            compact_when_replay_memory_bytes,
        )
    }

    /// Preview delta compaction without publishing CURRENT.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, transaction_uuid, generation_uuid, through_run_sequence=None, cleanup_after_commit=false, retained_ancestors=None, cancellation=None))]
    fn preview_graph_delta_compaction<'py>(
        &self,
        py: Python<'py>,
        transaction_uuid: &str,
        generation_uuid: &str,
        through_run_sequence: Option<u64>,
        cleanup_after_commit: bool,
        retained_ancestors: Option<usize>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.ensure_open()?;
        transaction::ops::preview_graph_delta_compaction(
            self,
            py,
            transaction_uuid,
            generation_uuid,
            through_run_sequence,
            cleanup_after_commit,
            retained_ancestors,
            cancellation,
        )
    }

    /// Compact a contiguous verified delta prefix into a new Parquet generation.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, transaction_uuid, generation_uuid, through_run_sequence=None, cleanup_after_commit=false, retained_ancestors=None, cancellation=None))]
    fn compact_graph_delta<'py>(
        &mut self,
        py: Python<'py>,
        transaction_uuid: &str,
        generation_uuid: &str,
        through_run_sequence: Option<u64>,
        cleanup_after_commit: bool,
        retained_ancestors: Option<usize>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.ensure_open()?;
        transaction::ops::compact_graph_delta(
            self,
            py,
            transaction_uuid,
            generation_uuid,
            through_run_sequence,
            cleanup_after_commit,
            retained_ancestors,
            cancellation,
        )
    }
}
