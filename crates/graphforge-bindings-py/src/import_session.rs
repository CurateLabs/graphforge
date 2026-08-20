//! Thin Python bindings for durable staged graph-import sessions (#744 / #738).

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use graphforge_api::{
    BulkInputKind, GfError, GraphImportSession, ImportPhase, ImportProgress, ImportSessionLimits,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use uuid::Uuid;

use crate::{
    GraphForge, PyCancellationToken, canonical_operation_id, py_bulk_input_to_batch, to_pyerr,
};

impl GraphForge {
    /// Run durable import validation while releasing the GIL on `&self`.
    pub(crate) fn run_import_validate(
        &self,
        py: Python<'_>,
        session: &Mutex<Option<GraphImportSession>>,
        cancellation: Option<&graphforge_api::CancellationToken>,
    ) -> PyResult<ImportProgress> {
        self.ensure_open()?;
        // Lock only inside `detach` so a concurrent GIL-holding caller cannot
        // wait on this mutex while the detached worker needs the GIL to return.
        py.detach(|| {
            let mut guard = session
                .lock()
                .map_err(|_| GfError::Execution("import session lock poisoned".into()))?;
            let session = guard
                .as_mut()
                .ok_or_else(|| GfError::Lifecycle("import session handle is closed".into()))?;
            session.validate_with_cancellation(&self.inner, cancellation)
        })
        .map_err(|error| to_pyerr(py, &error))
    }

    /// Publish a fully staged import while releasing the GIL on `&self`.
    pub(crate) fn run_import_commit(
        &self,
        py: Python<'_>,
        session: &Mutex<Option<GraphImportSession>>,
        cancellation: Option<&graphforge_api::CancellationToken>,
    ) -> PyResult<String> {
        self.ensure_open()?;
        py.detach(|| {
            let mut guard = session
                .lock()
                .map_err(|_| GfError::Execution("import session lock poisoned".into()))?;
            let session = guard
                .as_mut()
                .ok_or_else(|| GfError::Lifecycle("import session handle is closed".into()))?;
            session
                .commit(&self.inner, cancellation)
                .map(|uuid| uuid.to_string())
        })
        .map_err(|error| to_pyerr(py, &error))
    }
}

fn phase_name(phase: ImportPhase) -> &'static str {
    match phase {
        ImportPhase::Open => "open",
        ImportPhase::Validated => "validated",
        ImportPhase::Committed => "committed",
        ImportPhase::Aborted => "aborted",
        ImportPhase::Quarantined => "quarantined",
    }
}

fn progress_dict(py: Python<'_>, progress: &ImportProgress) -> PyResult<Py<PyAny>> {
    let out = PyDict::new(py);
    out.set_item("rows_accepted", progress.rows_accepted)?;
    out.set_item("rows_rejected", progress.rows_rejected)?;
    out.set_item("bytes_accepted", progress.bytes_accepted)?;
    out.set_item("files_accepted", progress.files_accepted)?;
    out.set_item("files_pending", progress.files_pending)?;
    out.set_item("elapsed_millis", progress.elapsed_millis)?;
    out.set_item("peak_batch_rows", progress.peak_batch_rows)?;
    out.set_item("io_concurrency_limit", progress.io_concurrency_limit)?;
    Ok(out.into_any().unbind())
}

fn status_dict(
    py: Python<'_>,
    phase: ImportPhase,
    progress: &ImportProgress,
) -> PyResult<Py<PyAny>> {
    let out = PyDict::new(py);
    out.set_item("phase", phase_name(phase))?;
    out.set_item("progress", progress_dict(py, progress)?)?;
    Ok(out.into_any().unbind())
}

fn parse_kind(py: Python<'_>, kind: &str) -> PyResult<BulkInputKind> {
    match kind {
        "node" | "nodes" => Ok(BulkInputKind::Node),
        "edge" | "edges" => Ok(BulkInputKind::Edge),
        _ => Err(to_pyerr(
            py,
            &GfError::Validation("kind must be node or edge".into()),
        )),
    }
}

fn parse_limits(
    batch_rows: Option<usize>,
    max_source_bytes: Option<u64>,
    max_files: Option<u64>,
    max_rejected_rows: Option<u64>,
    io_concurrency: Option<usize>,
) -> ImportSessionLimits {
    let defaults = ImportSessionLimits::default();
    ImportSessionLimits {
        batch_rows: batch_rows.unwrap_or(defaults.batch_rows),
        max_source_bytes: max_source_bytes.unwrap_or(defaults.max_source_bytes),
        max_files: max_files.unwrap_or(defaults.max_files),
        max_rejected_rows: max_rejected_rows.unwrap_or(defaults.max_rejected_rows),
        io_concurrency: io_concurrency.unwrap_or(defaults.io_concurrency),
    }
}

/// Owned durable import-session handle. Contains no live rows.
#[pyclass(name = "GraphImportSession", module = "graphforge")]
pub struct PyGraphImportSession {
    parent: Py<GraphForge>,
    inner: Mutex<Option<GraphImportSession>>,
}

impl PyGraphImportSession {
    fn take_inner(&self, py: Python<'_>) -> PyResult<GraphImportSession> {
        self.inner
            .lock()
            .map_err(|_| {
                to_pyerr(
                    py,
                    &GfError::Execution("import session lock poisoned".into()),
                )
            })?
            .take()
            .ok_or_else(|| {
                to_pyerr(
                    py,
                    &GfError::Lifecycle("import session handle is closed".into()),
                )
            })
    }

    fn with_mut<R>(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&mut GraphImportSession) -> Result<R, GfError>,
    ) -> PyResult<R> {
        let mut guard = self.inner.lock().map_err(|_| {
            to_pyerr(
                py,
                &GfError::Execution("import session lock poisoned".into()),
            )
        })?;
        let session = guard.as_mut().ok_or_else(|| {
            to_pyerr(
                py,
                &GfError::Lifecycle("import session handle is closed".into()),
            )
        })?;
        f(session).map_err(|error| to_pyerr(py, &error))
    }
}

#[pymethods]
impl PyGraphImportSession {
    /// Durable identifier used for resume.
    #[getter]
    fn session_uuid(&self, py: Python<'_>) -> PyResult<String> {
        self.with_mut(py, |session| Ok(session.session_uuid().to_string()))
    }

    /// Current durable phase and counters.
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (phase, progress) = self.with_mut(py, |session| Ok(session.status()))?;
        status_dict(py, phase, &progress)
    }

    /// Append one Arrow partition without retaining live rows.
    fn append_arrow(&self, py: Python<'_>, kind: &str, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let kind = parse_kind(py, kind)?;
        let batch = py_bulk_input_to_batch(py, data)?;
        self.with_mut(py, |session| session.append_arrow(kind, &[batch]))
    }

    /// Register a local Parquet source by copying it into durable ownership.
    fn register_parquet(&self, py: Python<'_>, kind: &str, path: &str) -> PyResult<()> {
        let kind = parse_kind(py, kind)?;
        let path = PathBuf::from(path);
        self.with_mut(py, |session| session.register_parquet(kind, &path))
    }

    /// Persist counters and source ordering without publishing graph state.
    fn checkpoint(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let progress = self.with_mut(py, GraphImportSession::checkpoint)?;
        progress_dict(py, &progress)
    }

    /// Validate and durably stage every source with optional cancellation.
    #[pyo3(signature = (*, cancellation=None))]
    fn validate(
        &self,
        py: Python<'_>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let cancellation = cancellation.map(|token| token.inner.clone());
        let progress = self.parent.bind(py).borrow().run_import_validate(
            py,
            &self.inner,
            cancellation.as_ref(),
        )?;
        progress_dict(py, &progress)
    }

    /// Publish the fully staged graph as one generation.
    #[pyo3(signature = (*, cancellation=None))]
    fn commit(
        &self,
        py: Python<'_>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<String> {
        let cancellation = cancellation.map(|token| token.inner.clone());
        self.parent
            .bind(py)
            .borrow()
            .run_import_commit(py, &self.inner, cancellation.as_ref())
    }

    /// Abort without changing CURRENT.
    fn abort(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let session = self.take_inner(py)?;
        let progress = py
            .detach(|| session.abort())
            .map_err(|error| to_pyerr(py, &error))?;
        progress_dict(py, &progress)
    }
}

/// Begin a durable import pinned to the facade's current project generation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn begin_import_session(
    slf: &Bound<'_, GraphForge>,
    py: Python<'_>,
    operation_uuid: &str,
    batch_rows: Option<usize>,
    max_source_bytes: Option<u64>,
    max_files: Option<u64>,
    max_rejected_rows: Option<u64>,
    io_concurrency: Option<usize>,
) -> PyResult<Py<PyGraphImportSession>> {
    if slf.borrow().closed {
        return Err(to_pyerr(
            py,
            &GfError::Lifecycle("operation on a closed GraphForge instance".into()),
        ));
    }
    let operation = canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
    let limits = parse_limits(
        batch_rows,
        max_source_bytes,
        max_files,
        max_rejected_rows,
        io_concurrency,
    );
    let session = slf
        .borrow()
        .inner
        .begin_import_session(operation, limits)
        .map_err(|error| to_pyerr(py, &error))?;
    Bound::new(
        py,
        PyGraphImportSession {
            parent: slf.clone().unbind(),
            inner: Mutex::new(Some(session)),
        },
    )
    .map(Bound::unbind)
}

/// Resume one durable, non-terminal session after process interruption.
pub(crate) fn resume_import_session(
    forge: &Bound<'_, GraphForge>,
    py: Python<'_>,
    session_uuid: &str,
) -> PyResult<Py<PyGraphImportSession>> {
    if forge.borrow().closed {
        return Err(to_pyerr(
            py,
            &GfError::Lifecycle("operation on a closed GraphForge instance".into()),
        ));
    }
    let uuid = Uuid::parse_str(session_uuid).map_err(|_| {
        to_pyerr(
            py,
            &GfError::Validation("session_uuid must be a canonical UUID string".into()),
        )
    })?;
    let session = forge
        .borrow()
        .inner
        .resume_import_session(uuid)
        .map_err(|error| to_pyerr(py, &error))?;
    Bound::new(
        py,
        PyGraphImportSession {
            parent: forge.clone().unbind(),
            inner: Mutex::new(Some(session)),
        },
    )
    .map(Bound::unbind)
}

/// Abort and remove durable staging for non-terminal sessions older than `max_age_secs`.
pub(crate) fn cleanup_stale_import_sessions(
    forge: &GraphForge,
    py: Python<'_>,
    max_age_secs: u64,
) -> PyResult<u64> {
    forge.ensure_open()?;
    let max_age = Duration::from_secs(max_age_secs);
    py.detach(|| forge.inner.cleanup_stale_import_sessions(max_age))
        .map_err(|error| to_pyerr(py, &error))
}
