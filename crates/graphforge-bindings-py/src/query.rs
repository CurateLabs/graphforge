//! Query Python binding methods and conversions.

use super::{GraphForge, PyCancellationToken, params_from_dict, result_to_pyarrow, to_pyerr};
use crate::portable;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::pyarrow::IntoPyArrow;
use arrow::record_batch::RecordBatchReader;
use futures::StreamExt;
use graphforge_api::RuntimeGuard;
use graphforge_api::SendableRecordBatchStream;
use pyo3::exceptions::PyImportError;
use pyo3::exceptions::PyModuleNotFoundError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// A synchronous [`RecordBatchReader`] that drives the facade's async streaming
/// query one batch per pull. Handed to PyArrow (via the Arrow C Stream
/// Interface) so `execute_stream` returns a genuine `pyarrow.RecordBatchReader`.
///
/// Owns a [`RuntimeGuard`] so the Tokio runtime and on-disk graph workspace
/// outlive the parent `GraphForge`: the reader is lazy and `'static`, and a bare
/// runtime handle would not keep the runtime (and the stream's worker threads /
/// Parquet fragment paths) alive.
struct StreamReader {
    schema: SchemaRef,
    stream: SendableRecordBatchStream,
    guard: RuntimeGuard,
}

impl Iterator for StreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Pulled from Python through the Arrow C stream with the GIL held;
        // release it while blocking on the next batch so other Python threads
        // run. A panic here would unwind into the C `get_next` callback (UB), so
        // catch it and convert to an Arrow error. DataFusion stream errors map
        // into the Arrow error domain (the typed GfError cannot survive the C
        // boundary — only build-time errors keep their Python exception class).
        let pulled = Python::attach(|py| {
            py.detach(|| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.guard.block_on(self.stream.next())
                }))
            })
        });
        match pulled {
            Ok(item) => item.map(|r| r.map_err(|e| ArrowError::ExternalError(Box::new(e)))),
            Err(_) => Some(Err(ArrowError::ExternalError(
                "panic while polling the GraphForge stream".into(),
            ))),
        }
    }
}

impl RecordBatchReader for StreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[pymethods]
impl GraphForge {
    /// Stream a query into an atomic Parquet file with explicit limits.
    #[pyo3(signature = (query, path, *, params=None, max_row_group_rows=65536, max_batch_rows=65536, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn execute_to_parquet_stream(
        &self,
        py: Python<'_>,
        query: &str,
        path: &str,
        params: Option<&Bound<'_, PyDict>>,
        max_row_group_rows: usize,
        max_batch_rows: usize,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        portable::execute_to_parquet_stream(
            self,
            py,
            query,
            path,
            params,
            max_row_group_rows,
            max_batch_rows,
            cancellation,
        )
    }

    /// Stream a query into an atomic Arrow IPC stream file with explicit limits.
    #[pyo3(signature = (query, path, *, params=None, max_row_group_rows=65536, max_batch_rows=65536, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn execute_to_arrow_ipc_stream(
        &self,
        py: Python<'_>,
        query: &str,
        path: &str,
        params: Option<&Bound<'_, PyDict>>,
        max_row_group_rows: usize,
        max_batch_rows: usize,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        portable::execute_to_arrow_ipc_stream(
            self,
            py,
            query,
            path,
            params,
            max_row_group_rows,
            max_batch_rows,
            cancellation,
        )
    }

    /// Run a Cypher query and return the result as a `pyarrow.Table`.
    ///
    /// `params` binds `$name` placeholders (values: `None`/`bool`/`int`/`float`/
    /// `str`/`uuid.UUID`/`list`/`dict`). Writes (`CREATE`/`SET`/`DELETE`/…) execute
    /// and return a summary.
    #[pyo3(signature = (query, params=None))]
    fn execute(
        &self,
        py: Python<'_>,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let query = query.to_owned();
        let result = match params {
            Some(_) => {
                let p = params_from_dict(params)?;
                py.detach(|| native.execute_with_params(&query, &p))
            }
            None => py.detach(|| native.execute(&query)),
        }
        .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }

    /// Run a Cypher query and return the result as a `polars.DataFrame`.
    ///
    /// Thin convenience wrapper over [`execute`](Self::execute) (Polars consumes
    /// Arrow zero-copy). Requires the optional `polars` dependency; if it is not
    /// installed this raises `ImportError` with install guidance. `params` binds
    /// `$name` placeholders exactly as [`execute`](Self::execute).
    #[pyo3(signature = (query, params=None))]
    fn execute_polars(
        &self,
        py: Python<'_>,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let table = self.execute(py, query, params)?;
        let polars = py.import("polars").map_err(|err| {
            // Only rewrite the message when polars is genuinely absent; surface
            // any other import failure (e.g. a broken polars install) unchanged.
            if err.is_instance_of::<PyModuleNotFoundError>(py) {
                PyImportError::new_err(
                    "execute_polars() requires the optional 'polars' dependency; \
                     install it with `pip install \"graphforge[polars]\"`",
                )
            } else {
                err
            }
        })?;
        let df = polars.call_method1("from_arrow", (table,))?;
        Ok(df.unbind())
    }

    /// Run a read-only Cypher query and return a lazy `pyarrow.RecordBatchReader`.
    ///
    /// The reader's `schema` is available immediately (before iterating); batches
    /// are produced on demand, so a large result is never fully materialised.
    /// Writes (`CREATE`/`MERGE`/`DELETE`/`SET`/`REMOVE`) raise `ValidationError` —
    /// use [`execute`](Self::execute) for those. `params` binds `$name`
    /// placeholders exactly as [`execute`](Self::execute). Errors raised before
    /// the reader is returned (parse/bind/validation) keep their typed exception
    /// class; an error encountered *mid-stream* surfaces as a `pyarrow` Arrow
    /// error (the typed `GfError` cannot cross the Arrow C stream boundary).
    #[pyo3(signature = (query, params=None))]
    fn execute_stream(
        &self,
        py: Python<'_>,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let query = query.to_owned();
        let p = params_from_dict(params)?;
        let (stream, schema, guard) = py
            .detach(|| native.execute_stream_owned(&query, &p))
            .map_err(|e| to_pyerr(py, &e))?;
        let reader = StreamReader {
            schema,
            stream,
            guard,
        };
        let boxed: Box<dyn RecordBatchReader + Send> = Box::new(reader);
        Ok(boxed.into_pyarrow(py)?.unbind())
    }

    /// Return a human-readable explanation of the compiler pipeline for `query`
    /// (`AST` → `GraphIR` → `LogicalPlan` → `PhysicalPlan`).
    fn explain(&self, py: Python<'_>, query: &str) -> PyResult<String> {
        let native = self.ensure_open()?;
        let query = query.to_owned();
        py.detach(|| native.explain(&query))
            .map_err(|e| to_pyerr(py, &e))
    }
}
