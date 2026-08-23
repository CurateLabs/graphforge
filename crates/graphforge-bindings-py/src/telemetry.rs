use graphforge_api::telemetry::{
    OtlpConfig, TelemetryConfig, TelemetryMode, TelemetryRuntime as RustTelemetryRuntime,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::BTreeMap;
use std::time::Duration;

#[pyclass(name = "TelemetryRuntime", module = "graphforge")]
pub(crate) struct PyTelemetryRuntime {
    runtime: RustTelemetryRuntime,
}

#[pymethods]
impl PyTelemetryRuntime {
    #[new]
    #[pyo3(signature = (mode="disabled", endpoint=None, headers=None, queue_capacity=256, batch_size=64, export_timeout_ms=3000, lifecycle_timeout_ms=5000, max_retries=2))]
    fn new(
        mode: &str,
        endpoint: Option<String>,
        headers: Option<BTreeMap<String, String>>,
        queue_capacity: usize,
        batch_size: usize,
        export_timeout_ms: u64,
        lifecycle_timeout_ms: u64,
        max_retries: u8,
    ) -> PyResult<Self> {
        let mode = match mode {
            "disabled" => TelemetryMode::Disabled,
            "in_memory" => TelemetryMode::InMemory,
            "otlp_http_json" => TelemetryMode::OtlpHttpJson,
            _ => return Err(PyValueError::new_err("GF_TELEMETRY_INVALID_MODE")),
        };
        let otlp = endpoint.map(|endpoint| OtlpConfig {
            endpoint,
            headers: headers.unwrap_or_default(),
        });
        let config = TelemetryConfig {
            mode,
            queue_capacity,
            batch_size,
            export_timeout: Duration::from_millis(export_timeout_ms),
            lifecycle_timeout: Duration::from_millis(lifecycle_timeout_ms),
            max_retries,
            otlp,
            ..TelemetryConfig::default()
        };
        RustTelemetryRuntime::new(config)
            .map(|runtime| Self { runtime })
            .map_err(|error| PyValueError::new_err(error.code.as_str()))
    }

    #[getter]
    fn enabled(&self) -> bool {
        self.runtime.is_enabled()
    }

    fn force_flush(&self) -> String {
        self.runtime.force_flush().as_str().to_owned()
    }
    fn shutdown(&self) -> String {
        self.runtime.shutdown().as_str().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_projection_uses_rust_lifecycle() {
        let runtime =
            PyTelemetryRuntime::new("disabled", None, None, 256, 64, 3_000, 5_000, 2).unwrap();
        assert!(!runtime.enabled());
        assert_eq!(runtime.force_flush(), "disabled");
        assert_eq!(runtime.shutdown(), "disabled");
    }
}
