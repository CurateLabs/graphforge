//! Construction Python binding methods and conversions.

use super::{
    GraphForge, bulk_edge_publication_error, bulk_node_publication_error, ensure_bulk_edge_batch,
    ensure_bulk_node_batch, props_from_dict, py_bulk_input_to_batch, py_operation_id,
    record_batch_to_pyarrow_table, to_pyerr,
};
use crate::composite;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyList;

/// UUID-backed node handle returned by [`GraphForge::add_node`].
#[pyclass(name = "NodeHandle", module = "graphforge")]
pub struct PyNodeHandle {
    pub(super) inner: graphforge_api::NodeHandle,
}

#[pymethods]
impl PyNodeHandle {
    /// Stable public UUID identity.
    #[getter]
    fn uuid(&self) -> String {
        self.inner.uuid.to_string()
    }

    /// Primary label metadata (not an identity surrogate).
    #[getter]
    fn label(&self) -> &str {
        &self.inner.label
    }

    fn __repr__(&self) -> String {
        self.inner.to_string()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }
}

/// UUID-backed edge handle returned by [`GraphForge::add_edge`].
#[pyclass(name = "EdgeHandle", module = "graphforge")]
pub struct PyEdgeHandle {
    pub(super) inner: graphforge_api::EdgeHandle,
}

#[pymethods]
impl PyEdgeHandle {
    /// Stable public UUID identity.
    #[getter]
    fn uuid(&self) -> String {
        self.inner.uuid.to_string()
    }

    /// Relationship-type metadata (not an identity surrogate).
    #[getter]
    fn rel_type(&self) -> &str {
        &self.inner.rel_type
    }

    fn __repr__(&self) -> String {
        self.inner.to_string()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }
}

#[pymethods]
impl GraphForge {
    /// Add one node through the Rust facade and return its UUID handle.
    #[pyo3(signature = (label, **props))]
    fn add_node(
        &self,
        py: Python<'_>,
        label: &str,
        props: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyNodeHandle> {
        let native = self.ensure_open()?;
        let props = props_from_dict(props)?;
        let label = label.to_owned();
        py.detach(|| native.add_node(&label, &props))
            .map(|inner| PyNodeHandle { inner })
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Add a directed edge and return its graph UUID handle.
    #[pyo3(signature = (src, rel_type, dst, **props))]
    fn add_edge(
        &self,
        py: Python<'_>,
        src: &PyNodeHandle,
        rel_type: &str,
        dst: &PyNodeHandle,
        props: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyEdgeHandle> {
        let native = self.ensure_open()?;
        let props = props_from_dict(props)?;
        let src = src.inner.clone();
        let dst = dst.inner.clone();
        let rel_type = rel_type.to_owned();
        py.detach(|| native.add_edge(&src, &rel_type, &dst, &props))
            .map(|inner| PyEdgeHandle { inner })
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Publish one composite graph + knowledge/epistemic transaction through Rust.
    ///
    /// Python only converts the request; validation, staging, publication,
    /// recovery, and idempotency stay in Rust. Returns the canonical Arrow
    /// receipt without a bespoke wrapper.
    #[pyo3(signature = (*, operation_uuid, graph_mutations, knowledge=None, actor_uuid=None, contract_version=1))]
    fn publish_composite_transaction(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        graph_mutations: &Bound<'_, PyList>,
        knowledge: Option<&Bound<'_, PyDict>>,
        actor_uuid: Option<&str>,
        contract_version: u32,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let request = composite::py_composite_request(
            py,
            operation_uuid,
            graph_mutations,
            knowledge,
            actor_uuid,
            contract_version,
        )?;
        let receipt = py
            .detach(|| native.publish_composite_transaction(request))
            .map_err(|error| to_pyerr(py, &error))?;
        record_batch_to_pyarrow_table(py, &receipt)
    }

    /// Publish one atomic bulk node batch through the Rust-owned Arrow contract.
    ///
    /// `data` may be a `pyarrow.Table`, Arrow-compatible DataFrame, or
    /// `list[dict]` of canonical node rows.
    #[pyo3(signature = (operation_uuid, data))]
    fn publish_bulk_nodes(
        &self,
        py: Python<'_>,
        operation_uuid: &Bound<'_, PyAny>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let operation_uuid =
            py_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let batch = py_bulk_input_to_batch(py, data)?;
        let receipt = py
            .detach(|| native.publish_bulk_nodes(operation_uuid, &[batch]))
            .map_err(|error| bulk_node_publication_error(py, error))?;
        record_batch_to_pyarrow_table(py, &receipt)
    }

    /// Publish one atomic bulk edge batch through the Rust-owned Arrow contract.
    ///
    /// `data` may be a `pyarrow.Table`, Arrow-compatible DataFrame, or
    /// `list[dict]` of canonical edge rows.
    #[pyo3(signature = (operation_uuid, data))]
    fn publish_bulk_edges(
        &self,
        py: Python<'_>,
        operation_uuid: &Bound<'_, PyAny>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let operation_uuid =
            py_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let batch = py_bulk_input_to_batch(py, data)?;
        let receipt = py
            .detach(|| native.publish_bulk_edges(operation_uuid, &[batch]))
            .map_err(|error| bulk_edge_publication_error(py, error))?;
        record_batch_to_pyarrow_table(py, &receipt)
    }

    /// Bulk-add nodes by normalizing convenience containers onto
    /// [`Self::publish_bulk_nodes`].
    ///
    /// `data` may omit `label` / `node_uuid`; the binding injects the provided
    /// label and nullable UUID column, then forwards the canonical Arrow batch.
    #[pyo3(signature = (label, data, *, operation_uuid))]
    fn add_nodes(
        &self,
        py: Python<'_>,
        label: &str,
        data: &Bound<'_, PyAny>,
        operation_uuid: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let operation_uuid =
            py_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let batch = ensure_bulk_node_batch(py, label, data)?;
        let receipt = py
            .detach(|| native.publish_bulk_nodes(operation_uuid, &[batch]))
            .map_err(|error| bulk_node_publication_error(py, error))?;
        record_batch_to_pyarrow_table(py, &receipt)
    }

    /// Bulk-add edges by normalizing convenience containers onto
    /// [`Self::publish_bulk_edges`].
    ///
    /// Endpoint columns default to `src_id` / `dst_id` and are renamed to the
    /// canonical `source_uuid` / `target_uuid` fields before Rust publication.
    #[pyo3(signature = (rel_type, data, *, operation_uuid, src="src_id", dst="dst_id"))]
    fn add_edges(
        &self,
        py: Python<'_>,
        rel_type: &str,
        data: &Bound<'_, PyAny>,
        operation_uuid: &Bound<'_, PyAny>,
        src: &str,
        dst: &str,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let operation_uuid =
            py_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let batch = ensure_bulk_edge_batch(py, rel_type, data, src, dst)?;
        let receipt = py
            .detach(|| native.publish_bulk_edges(operation_uuid, &[batch]))
            .map_err(|error| bulk_edge_publication_error(py, error))?;
        record_batch_to_pyarrow_table(py, &receipt)
    }

    /// Remove all nodes and edges (in-memory instances only).
    fn clear(&self, py: Python<'_>) -> PyResult<()> {
        let native = self.ensure_open()?;
        py.detach(|| native.clear()).map_err(|e| to_pyerr(py, &e))
    }
}
