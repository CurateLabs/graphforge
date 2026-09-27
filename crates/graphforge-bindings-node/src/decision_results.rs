//! Thin binding for Rust-owned provider-neutral decision validation.
use crate::{GraphForge, Result, napi};
use napi::bindgen_prelude::Buffer;

#[napi]
impl GraphForge {
    /// Validate a caller-supplied decision batch with Rust and return Arrow IPC.
    #[napi]
    pub fn validate_decision_batch(&self, request: serde_json::Value) -> Result<Buffer> {
        let _guard = self.open_guard()?;
        let request =
            serde_json::from_value::<graphforge_api::DecisionBatchV1>(request).map_err(|_| {
                crate::to_napi_err(&graphforge_api::GfError::Validation(
                    "invalid decision batch JSON contract".into(),
                ))
            })?;
        let result = request
            .validate()
            .map_err(|error| crate::to_napi_err(&error))?;
        crate::record_batch_to_ipc(&result)
            .map(Buffer::from)
            .map_err(|error| crate::to_napi_err(&error))
    }
}
