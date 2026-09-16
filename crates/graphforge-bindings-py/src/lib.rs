//! GraphForge Python bindings via PyO3.
//!
//! Domain modules own conversions, annotated methods, and native handles/tasks.
//! The root retains facade identity, runtime ownership, and registration.
// PyO3 macros expand to unsafe FFI code; unsafe is permitted here but audited.
#![warn(unsafe_code)]

mod analyst;
mod assertions;
mod construction;
mod conversions;
mod epistemic;
mod lifecycle;
mod ontology;
mod providers;
mod query;
mod recorded;

pub use analyst::PyGraphScaleIndexProfile;
pub use analyst::PyInvocationDescriptor;
use analyst::parse_algorithm_id;
use analyst::parse_terminal_uuids;
use assertions::assertion_status;
use assertions::py_operation_id;
pub use construction::PyEdgeHandle;
pub use construction::PyNodeHandle;
use conversions::algorithm_result;
use conversions::bulk_edge_publication_error;
use conversions::bulk_node_publication_error;
use conversions::ensure_bulk_edge_batch;
use conversions::ensure_bulk_node_batch;
use conversions::json_map;
use conversions::json_value_to_python;
use conversions::params_from_dict;
use conversions::props_from_dict;
use conversions::py_bulk_input_to_batch;
use conversions::py_to_ir_literal;
use conversions::py_to_json_value;
use conversions::py_to_prop_value;
use conversions::pyarrow_table_to_batch;
use conversions::record_batch_to_pyarrow_table;
use conversions::result_to_pyarrow;
use conversions::string_map;
use graphforge_api::GfError;
use graphforge_api::GraphDirectedness;
use graphforge_api::GraphForgeOptions;
use graphforge_api::InvocationError;
use graphforge_api::NodeSelector;
use graphforge_api::OperationId;
use graphforge_api::ProjectWriteMode;
use graphforge_api::WriteContext;
pub use lifecycle::PyCheckpointView;
use ontology::rename_map;
use providers::ConfiguredProviderBinding;
use providers::adjacency_inspection_to_python;
use providers::embedding_options_from_kwargs;
use providers::embedding_validation;
use providers::py_to_node_selector;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
pub use recorded::PyRecordedAlgorithmResult;
pub use recorded::PyResolvedBeliefProjection;
pub use recorded::PyResolvedRecordedAlgorithmResult;
use std::sync::Condvar;
use std::sync::LazyLock;
use std::sync::Mutex;

mod composite;
mod import_session;
mod multi_ontology;
mod portable;
mod telemetry;
mod transaction;

// Python exception hierarchy (re-exported as `graphforge.exceptions`): a
// `GraphForgeError` base so callers can catch broadly, plus one subclass per
// `GfError` fault domain. `GfError::NotImplemented` maps to the builtin
// `NotImplementedError` (the idiomatic "pending" signal).
create_exception!(
    graphforge,
    GraphForgeError,
    PyException,
    "Base class for all GraphForge exceptions."
);
create_exception!(
    graphforge,
    ParseError,
    GraphForgeError,
    "The Cypher parser rejected the input; carries a `span` (offset, length)."
);
create_exception!(
    graphforge,
    PlanError,
    GraphForgeError,
    "The binder or query planner could not produce a valid plan."
);
create_exception!(
    graphforge,
    ExecutionError,
    GraphForgeError,
    "A runtime fault occurred during query execution."
);
create_exception!(
    graphforge,
    StorageError,
    GraphForgeError,
    "A storage I/O operation failed."
);
create_exception!(
    graphforge,
    LifecycleError,
    GraphForgeError,
    "An operation was invalid for the current instance lifecycle state."
);
create_exception!(
    graphforge,
    ValidationError,
    GraphForgeError,
    "Input failed validation at the API boundary."
);
create_exception!(
    graphforge,
    OntologyError,
    GraphForgeError,
    "An ontology file could not be loaded or applied."
);

/// Convert a [`GfError`] into the matching Python exception. `ParseError` and
/// binder failures ([`GfError::Bind`]) carry a `span` attribute — the
/// `(offset, length)` of the offending token, per the shim's `ParseError`
/// contract. Binder failures share the public `ParseError` / `GF_PARSE` domain.
pub(crate) fn to_pyerr(py: Python<'_>, err: &GfError) -> PyErr {
    let error = match err {
        GfError::Parse { msg, span, .. } | GfError::Bind { msg, span, .. } => {
            let e = PyErr::new::<ParseError, _>(msg.clone());
            let _ = e
                .value(py)
                .setattr("span", (span.start, span.end.saturating_sub(span.start)));
            e
        }
        GfError::LoweringExecution(error) => PyErr::new::<ExecutionError, _>(error.to_string()),
        GfError::Lowering(error) => match error {
            graphforge_api::LoweringError::InvalidType(_) => {
                PyErr::new::<ValidationError, _>(error.to_string())
            }
            _ => PyErr::new::<PlanError, _>(error.to_string()),
        },
        GfError::BindValidation { msg, .. } | GfError::Validation(msg) => {
            PyErr::new::<ValidationError, _>(msg.clone())
        }
        GfError::Algorithm(error) => match error {
            graphforge_api::AlgorithmError::Unavailable { .. }
            | graphforge_api::AlgorithmError::DuplicateCapability { .. } => {
                PyErr::new::<ValidationError, _>(error.to_string())
            }
            _ => PyErr::new::<ExecutionError, _>(error.to_string()),
        },
        GfError::BindPlan { msg, .. } | GfError::Plan(msg) => {
            PyErr::new::<PlanError, _>(msg.clone())
        }
        GfError::Execution(m) => PyErr::new::<ExecutionError, _>(m.clone()),
        GfError::Provider {
            class,
            provider,
            model,
        } => {
            let error = PyErr::new::<ExecutionError, _>(format!(
                "provider invocation failed: class={class} provider={provider} model={model}"
            ));
            let value = error.value(py);
            let _ = value.setattr("provider_class", class);
            let _ = value.setattr("provider", provider);
            let _ = value.setattr("model", model);
            error
        }
        GfError::Storage(m) => PyErr::new::<StorageError, _>(m.clone()),
        GfError::Project { message, .. } => PyErr::new::<StorageError, _>(message.clone()),
        GfError::Api { message, .. } => PyErr::new::<ValidationError, _>(message.clone()),
        GfError::Lifecycle(m) => PyErr::new::<LifecycleError, _>(m.clone()),
        GfError::Ontology(m) => PyErr::new::<OntologyError, _>(m.clone()),
        GfError::NotImplemented(name) => PyErr::new::<PyNotImplementedError, _>((*name).to_owned()),
    };
    let _ = error.value(py).setattr("code", err.code());
    error
}

fn to_py_invocation_error(py: Python<'_>, err: &InvocationError) -> PyErr {
    if let InvocationError::Graph(error) = err {
        return to_pyerr(py, error);
    }
    let error = PyErr::new::<ValidationError, _>(err.to_string());
    let _ = error.value(py).setattr("code", err.code());
    error
}

pub(crate) fn canonical_operation_id(value: &str) -> Result<OperationId, GfError> {
    if value.len() != 36 {
        return Err(GfError::Validation(format!("invalid UUID {value:?}")));
    }
    let NodeSelector::Uuid(uuid) = NodeSelector::uuid(value)? else {
        unreachable!("UUID parser always constructs a UUID selector")
    };
    if uuid.hyphenated().to_string() != value {
        return Err(GfError::Validation(format!("invalid UUID {value:?}")));
    }
    Ok(OperationId(uuid))
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}

/// The GraphForge engine — a Python handle over the native Rust core
/// ([`graphforge_api::GraphForge`]).
///
/// Construct in-memory (`GraphForge()`) or over a Parquet project directory
/// (`GraphForge(path)`), then query with [`execute`](Self::execute). The
/// analyst verbs (`rank`/`cluster`/…) are exposed in a follow-up binding-surface PR.
#[pyclass(module = "graphforge")]
pub struct GraphForge {
    pub(crate) inner: graphforge_api::GraphForge,
    provider: Option<ConfiguredProviderBinding>,
    pub(crate) closed: bool,
}

/// Native cloneable cooperative cancellation token.
#[pyclass(name = "CancellationToken", module = "graphforge")]
pub struct PyCancellationToken {
    pub(crate) inner: graphforge_api::CancellationToken,
}

#[pymethods]
impl PyCancellationToken {
    #[new]
    fn new() -> Self {
        Self {
            inner: graphforge_api::CancellationToken::new(),
        }
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    #[getter]
    fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

impl GraphForge {
    /// Guard mirroring the v0.5 lifecycle contract: operations after `close()`
    /// raise `LifecycleError`.
    pub(crate) fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(Python::attach(|py| {
                to_pyerr(
                    py,
                    &GfError::Lifecycle("operation on a closed GraphForge instance".into()),
                )
            }));
        }
        Ok(())
    }
}

fn project_write_mode(value: &str) -> Result<ProjectWriteMode, GfError> {
    match value {
        "single_writer" => Ok(ProjectWriteMode::SingleWriter),
        "queued_writer" => Ok(ProjectWriteMode::QueuedWriter),
        "optimistic_multi_writer" => Ok(ProjectWriteMode::OptimisticMultiWriter),
        _ => Err(GfError::Validation(
            "write_mode must be single_writer, queued_writer, or optimistic_multi_writer".into(),
        )),
    }
}

#[pymethods]
impl GraphForge {
    /// Open an in-memory (`path=None`) or Parquet-backed (`path=<dir>`) instance.
    #[new]
    #[pyo3(signature = (path=None, *, write_mode="single_writer", write_queue_capacity=64, max_rebase_attempts=3))]
    fn new(
        py: Python<'_>,
        path: Option<&str>,
        write_mode: &str,
        write_queue_capacity: i64,
        max_rebase_attempts: i64,
    ) -> PyResult<Self> {
        let path = path.map(str::to_owned);
        let options = GraphForgeOptions {
            write_mode: project_write_mode(write_mode).map_err(|error| to_pyerr(py, &error))?,
            write_queue_capacity: usize::try_from(write_queue_capacity).map_err(|_| {
                to_pyerr(
                    py,
                    &GfError::Validation("write_queue_capacity must be between 1 and 65536".into()),
                )
            })?,
            max_rebase_attempts: u32::try_from(max_rebase_attempts).map_err(|_| {
                to_pyerr(
                    py,
                    &GfError::Validation("max_rebase_attempts must not exceed 32".into()),
                )
            })?,
            ..GraphForgeOptions::default()
        };
        let inner = py
            .detach(|| graphforge_api::GraphForge::new_with_options(path.as_deref(), options))
            .map_err(|e| to_pyerr(py, &e))?;
        Ok(Self {
            inner,
            provider: None,
            closed: false,
        })
    }

    // ----- Construction (write API).

    // ----- Introspection.

    /// Sorted label and relationship counts as a `pyarrow.Table`.
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        algorithm_result(py, py.detach(|| self.inner.schema()))
    }

    /// The node labels present in the graph.
    fn labels(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.ensure_open()?;
        py.detach(|| self.inner.labels())
            .map_err(|e| to_pyerr(py, &e))
    }

    /// The relationship types present in the graph.
    fn relationship_types(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.ensure_open()?;
        py.detach(|| self.inner.relationship_types())
            .map_err(|e| to_pyerr(py, &e))
    }

    /// Count nodes (optionally for one `label`).
    #[pyo3(signature = (label=None))]
    fn node_count(&self, py: Python<'_>, label: Option<&str>) -> PyResult<u64> {
        self.ensure_open()?;
        let label = label.map(str::to_owned);
        py.detach(|| self.inner.node_count(label.as_deref().unwrap_or("")))
            .map_err(|e| to_pyerr(py, &e))
    }

    /// Read optional project-level graph directedness (`directed` / `undirected`).
    fn graph_directedness(&self, py: Python<'_>) -> PyResult<Option<&'static str>> {
        self.ensure_open()?;
        py.detach(|| self.inner.graph_directedness())
            .map(|value| value.map(GraphDirectedness::as_str))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Set or clear project-level graph directedness for GSI grading.
    #[pyo3(signature = (directedness=None, *, operation_uuid, actor_uuid=None))]
    fn set_graph_directedness(
        &mut self,
        py: Python<'_>,
        directedness: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let directedness = match directedness {
            None => None,
            Some(value) => {
                Some(GraphDirectedness::parse(value).map_err(|error| to_pyerr(py, &error))?)
            }
        };
        let context = WriteContext {
            operation_uuid: canonical_operation_id(operation_uuid)
                .map_err(|error| to_pyerr(py, &error))?,
            actor_uuid: actor_uuid
                .map(canonical_operation_id)
                .transpose()
                .map_err(|error| to_pyerr(py, &error))?
                .map(|operation| operation.0),
        };
        py.detach(|| self.inner.set_graph_directedness(&context, directedness))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Grade the live graph to a Graph Scale Index profile.
    fn profile_gsi(&self, py: Python<'_>) -> PyResult<PyGraphScaleIndexProfile> {
        self.ensure_open()?;
        py.detach(|| self.inner.profile_gsi())
            .map(|inner| PyGraphScaleIndexProfile { inner })
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Close the instance; subsequent operations raise `LifecycleError`.
    /// Idempotent. Storage is flushed/released when the handle is dropped.
    fn close(&mut self) {
        self.closed = true;
    }

    /// The storage path, or `None` for an in-memory instance.
    #[getter]
    fn path(&self) -> Option<String> {
        self.inner.path().map(|p| p.display().to_string())
    }

    /// The effective ontology mode: `"exploratory"` | `"advisory"` | `"strict"`.
    #[getter]
    fn ontology_mode(&self) -> String {
        format!("{:?}", self.inner.ontology_mode()).to_lowercase()
    }

    fn __repr__(&self) -> String {
        self.inner.path().map_or_else(
            || "GraphForge(in-memory)".to_owned(),
            |p| format!("GraphForge(path={})", p.display()),
        )
    }
}

#[derive(Default)]
struct GilReleaseProbeState {
    entered: bool,
    released: bool,
}

static GIL_RELEASE_PROBE: LazyLock<(Mutex<GilReleaseProbeState>, Condvar)> =
    LazyLock::new(|| (Mutex::new(GilReleaseProbeState::default()), Condvar::new()));

/// Deterministic native blocking probe used by wheel acceptance tests.
#[pyfunction]
fn _test_gil_release_probe(py: Python<'_>) {
    py.detach(|| {
        let (lock, condition) = &*GIL_RELEASE_PROBE;
        let mut state = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.released = false;
        state.entered = true;
        condition.notify_all();
        while !state.released {
            state = condition
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        state.entered = false;
    });
}

/// Wait until the blocking probe has entered native code without holding the GIL.
#[pyfunction]
fn _test_gil_release_probe_wait(py: Python<'_>) {
    py.detach(|| {
        let (lock, condition) = &*GIL_RELEASE_PROBE;
        let mut state = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.entered {
            state = condition
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    });
}

/// Release the deterministic native blocking probe.
#[pyfunction]
fn _test_gil_release_probe_signal() {
    let (lock, condition) = &*GIL_RELEASE_PROBE;
    let mut state = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.released = true;
    condition.notify_all();
}

#[derive(Default)]
struct WriterHoldProbeState {
    held: Option<graphforge_api::concurrency_test_support::HeldWriter>,
}

static WRITER_HOLD_PROBE: LazyLock<Mutex<WriterHoldProbeState>> =
    LazyLock::new(|| Mutex::new(WriterHoldProbeState::default()));

/// Stage and retain the project writer lock for concurrency acceptance tests.
#[pyfunction]
fn _test_acquire_writer_hold(py: Python<'_>, path: &str) -> PyResult<()> {
    {
        let state = WRITER_HOLD_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.held.is_some() {
            return Err(to_pyerr(
                py,
                &GfError::Validation("writer-hold probe already active".into()),
            ));
        }
    }
    let owned = path.to_owned();
    let held = py
        .detach(|| graphforge_api::concurrency_test_support::hold_writer(&owned))
        .map_err(|error| to_pyerr(py, &error))?;
    let mut state = WRITER_HOLD_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.held.is_some() {
        return Err(to_pyerr(
            py,
            &GfError::Validation("writer-hold probe already active".into()),
        ));
    }
    state.held = Some(held);
    Ok(())
}

/// Drop the staged writer hold created by [`_test_acquire_writer_hold`].
#[pyfunction]
fn _test_release_writer_hold(py: Python<'_>) -> PyResult<()> {
    let mut state = WRITER_HOLD_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.held.take().is_none() {
        return Err(to_pyerr(
            py,
            &GfError::Validation("writer-hold probe is not active".into()),
        ));
    }
    Ok(())
}

/// Execute the Rust-owned CLI without reimplementing parsing or behavior in Python.
#[pyfunction]
fn _cli_execute(
    py: Python<'_>,
    args: Vec<String>,
) -> (i32, Bound<'_, PyBytes>, Bound<'_, PyBytes>) {
    let execution = graphforge_cli::execute(args);
    (
        execution.exit_code,
        PyBytes::new(py, &execution.stdout),
        PyBytes::new(py, &execution.stderr),
    )
}

/// GraphForge native extension module (`graphforge._graphforge_rs`).
#[pymodule]
fn _graphforge_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(composite::composite_provenance_uuid, m)?)?;
    m.add_function(wrap_pyfunction!(_test_gil_release_probe, m)?)?;
    m.add_function(wrap_pyfunction!(_test_gil_release_probe_wait, m)?)?;
    m.add_function(wrap_pyfunction!(_test_gil_release_probe_signal, m)?)?;
    m.add_function(wrap_pyfunction!(_test_acquire_writer_hold, m)?)?;
    m.add_function(wrap_pyfunction!(_test_release_writer_hold, m)?)?;
    m.add_function(wrap_pyfunction!(_cli_execute, m)?)?;
    m.add_class::<GraphForge>()?;
    m.add_class::<transaction::PyGraphTransaction>()?;
    m.add_class::<import_session::PyGraphImportSession>()?;
    m.add_class::<PyCheckpointView>()?;
    m.add_class::<PyCancellationToken>()?;
    m.add_class::<telemetry::PyTelemetryRuntime>()?;
    m.add_class::<PyNodeHandle>()?;
    m.add_class::<PyEdgeHandle>()?;
    m.add_class::<PyInvocationDescriptor>()?;
    m.add_class::<PyGraphScaleIndexProfile>()?;
    m.add_class::<PyRecordedAlgorithmResult>()?;
    m.add_class::<PyResolvedBeliefProjection>()?;
    m.add_class::<PyResolvedRecordedAlgorithmResult>()?;
    m.add("GraphForgeError", py.get_type::<GraphForgeError>())?;
    m.add("ParseError", py.get_type::<ParseError>())?;
    m.add("PlanError", py.get_type::<PlanError>())?;
    m.add("ExecutionError", py.get_type::<ExecutionError>())?;
    m.add("StorageError", py.get_type::<StorageError>())?;
    m.add("LifecycleError", py.get_type::<LifecycleError>())?;
    m.add("ValidationError", py.get_type::<ValidationError>())?;
    m.add("OntologyError", py.get_type::<OntologyError>())?;
    Ok(())
}

/// Returns the crate version.
#[pyfunction]
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
