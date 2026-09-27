//! Thin binding for Rust-owned provider-neutral decision validation.
use super::{GraphForge, py_to_json_value, record_batch_to_pyarrow_table, to_pyerr};
use pyo3::prelude::*;

#[pymethods]
impl GraphForge {
    /// Validate a caller-supplied decision batch with the Rust API.
    fn validate_decision_batch(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request: graphforge_api::DecisionBatchV1 =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid decision batch JSON contract".into(),
                    ),
                )
            })?;
        let result = py
            .detach(|| request.validate())
            .map_err(|error| to_pyerr(py, &error))?;
        record_batch_to_pyarrow_table(py, &result)
    }
}
