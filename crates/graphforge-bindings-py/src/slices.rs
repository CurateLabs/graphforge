//! Thin Rust-owned Slice selection, frozen membership and continuation bindings.
use super::{GraphForge, PyCancellationToken, py_to_json_value, result_to_pyarrow, to_pyerr};
use graphforge_api::{CancellationToken, PageRequest, PageToken, SlicePageKind};
use pyo3::prelude::*;
fn contract<T: serde::de::DeserializeOwned>(
    py: Python<'_>,
    value: serde_json::Value,
) -> PyResult<T> {
    serde_json::from_value(value).map_err(|_| {
        to_pyerr(
            py,
            &graphforge_api::GfError::Validation("invalid Slice JSON contract".into()),
        )
    })
}
fn page(
    py: Python<'_>,
    limit: u32,
    after: Option<&str>,
    cancellation: Option<&PyCancellationToken>,
) -> PyResult<PageRequest> {
    Ok(PageRequest {
        limit,
        after: after
            .map(PageToken::parse)
            .transpose()
            .map_err(|e| to_pyerr(py, &e))?,
        cancellation: cancellation.map(|t| t.inner.clone()),
    })
}
#[pymethods]
impl GraphForge {
    /// Evaluate a bounded native selection against one explicit source.
    #[pyo3(signature = (request, kind="included", *, limit=100, after=None, cancellation=None))]
    fn preview_slice(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        kind: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request = contract(py, py_to_json_value(request)?)?;
        let kind: SlicePageKind = contract(py, serde_json::Value::String(kind.into()))?;
        let page = page(py, limit, after, cancellation)?;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.preview_slice(&request, kind, page))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Freeze Arrow membership/context against an explicitly retained Version.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn freeze_slice(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request = contract(py, py_to_json_value(request)?)?;
        let cancellation: CancellationToken =
            cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.freeze_slice(&request, &cancellation))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Inspect canonical frozen Arrow IPC without reevaluating its selector.
    #[pyo3(signature = (capsule, kind="included", *, limit=100, after=None, cancellation=None))]
    fn inspect_frozen_slice(
        &self,
        py: Python<'_>,
        capsule: &[u8],
        kind: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let kind: SlicePageKind = contract(py, serde_json::Value::String(kind.into()))?;
        let page = page(py, limit, after, cancellation)?;
        let capsule = bounded_capsule(py, capsule)?;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.inspect_frozen_slice(&capsule, kind, page))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Revise exact membership with explicit historical-source selection.
    #[pyo3(signature = (capsule, revision, *, cancellation=None))]
    fn revise_frozen_slice(
        &self,
        py: Python<'_>,
        capsule: &[u8],
        revision: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let revision = contract(py, py_to_json_value(revision)?)?;
        let cancellation: CancellationToken =
            cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let capsule = bounded_capsule(py, capsule)?;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.revise_frozen_slice(&capsule, &revision, &cancellation))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
}

fn bounded_capsule(py: Python<'_>, bytes: &[u8]) -> PyResult<Vec<u8>> {
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(to_pyerr(
            py,
            &graphforge_api::GfError::Api {
                code: graphforge_api::ApiErrorCode::ResourceLimit,
                message: "Slice capsule exceeds byte bound".into(),
            },
        ));
    }
    Ok(bytes.to_vec())
}
