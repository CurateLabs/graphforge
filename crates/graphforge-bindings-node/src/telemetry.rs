use graphforge_api::telemetry::{
    OtlpConfig, TelemetryConfig, TelemetryMode, TelemetryRuntime as RustTelemetryRuntime,
};
use napi_derive::napi;
use std::collections::BTreeMap;
use std::time::Duration;

#[napi(object)]
pub struct TelemetryConfigInput {
    pub mode: Option<String>,
    pub endpoint: Option<String>,
    pub headers: Option<BTreeMap<String, String>>,
    pub queue_capacity: Option<u32>,
    pub batch_size: Option<u32>,
    pub export_timeout_ms: Option<u32>,
    pub lifecycle_timeout_ms: Option<u32>,
    pub max_retries: Option<u8>,
}

#[napi(js_name = "TelemetryRuntime")]
pub struct NodeTelemetryRuntime {
    runtime: RustTelemetryRuntime,
}

#[napi]
impl NodeTelemetryRuntime {
    #[napi(constructor)]
    pub fn new(config: Option<TelemetryConfigInput>) -> napi::Result<Self> {
        let config = config.unwrap_or(TelemetryConfigInput {
            mode: None,
            endpoint: None,
            headers: None,
            queue_capacity: None,
            batch_size: None,
            export_timeout_ms: None,
            lifecycle_timeout_ms: None,
            max_retries: None,
        });
        let mode = match config.mode.as_deref().unwrap_or("disabled") {
            "disabled" => TelemetryMode::Disabled,
            "in_memory" => TelemetryMode::InMemory,
            "otlp_http_json" => TelemetryMode::OtlpHttpJson,
            _ => return Err(napi::Error::from_reason("GF_TELEMETRY_INVALID_MODE")),
        };
        let otlp = config.endpoint.map(|endpoint| OtlpConfig {
            endpoint,
            headers: config.headers.unwrap_or_default(),
        });
        let defaults = TelemetryConfig::default();
        let native = TelemetryConfig {
            mode,
            queue_capacity: config
                .queue_capacity
                .map_or(defaults.queue_capacity, |value| value as usize),
            batch_size: config
                .batch_size
                .map_or(defaults.batch_size, |value| value as usize),
            export_timeout: Duration::from_millis(u64::from(
                config.export_timeout_ms.unwrap_or(3_000),
            )),
            lifecycle_timeout: Duration::from_millis(u64::from(
                config.lifecycle_timeout_ms.unwrap_or(5_000),
            )),
            max_retries: config.max_retries.unwrap_or(2),
            otlp,
            ..defaults
        };
        RustTelemetryRuntime::new(native)
            .map(|runtime| Self { runtime })
            .map_err(|error| napi::Error::from_reason(error.code.as_str()))
    }

    #[napi(getter)]
    pub fn enabled(&self) -> bool {
        self.runtime.is_enabled()
    }

    #[napi]
    pub fn force_flush(&self) -> String {
        self.runtime.force_flush().as_str().to_owned()
    }
    #[napi]
    pub fn shutdown(&self) -> String {
        self.runtime.shutdown().as_str().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_projection_uses_rust_lifecycle() {
        let runtime = NodeTelemetryRuntime::new(None).unwrap();
        assert!(!runtime.enabled());
        assert_eq!(runtime.force_flush(), "disabled");
        assert_eq!(runtime.shutdown(), "disabled");
    }
}
