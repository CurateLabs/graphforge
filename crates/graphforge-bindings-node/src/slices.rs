//! Thin async projections; all selection and frozen semantics remain in Rust.
use crate::{Buffer, Env, GraphForge, Result, Task, napi};
use graphforge_api::{
    CancellationToken, PageRequest, PageToken, SlicePageKind, SliceRequest, SliceRevisionRequest,
};
fn contract<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T> {
    serde_json::from_value(value).map_err(|_| {
        crate::to_napi_err(&graphforge_api::GfError::Validation(
            "invalid Slice JSON contract".into(),
        ))
    })
}
pub(crate) fn cancellation(
    env: Env,
    signal: Option<crate::Object<'_>>,
) -> Result<CancellationToken> {
    use napi::bindgen_prelude::JsObjectValue;
    let token = CancellationToken::new();
    if let Some(signal) = signal {
        if signal
            .get_named_property::<bool>("aborted")
            .map_err(|_| crate::type_error(env, "signal must expose a boolean aborted property"))?
        {
            token.cancel();
        } else {
            let signal = crate::research_versions::native_abort_signal(env, &signal)
                .map_err(|_| crate::type_error(env, "signal must be an AbortSignal"))?;
            let callback = token.clone();
            signal.on_abort(move || callback.cancel());
        }
    }
    Ok(token)
}
fn page(
    limit: Option<u32>,
    after: Option<String>,
    cancellation: CancellationToken,
) -> Result<PageRequest> {
    Ok(PageRequest {
        limit: limit.unwrap_or(100),
        after: after
            .as_deref()
            .map(PageToken::parse)
            .transpose()
            .map_err(|e| crate::to_napi_err(&e))?,
        cancellation: Some(cancellation),
    })
}
enum Operation {
    Preview(SliceRequest, SlicePageKind, PageRequest),
    Freeze(SliceRequest, CancellationToken),
    Inspect(Vec<u8>, SlicePageKind, PageRequest),
    Revise(Vec<u8>, SliceRevisionRequest, CancellationToken),
}
#[napi]
impl GraphForge {
    /// Evaluate a bounded source-pinned Slice and return Arrow IPC.
    #[napi]
    pub fn preview_slice(
        &self,
        request: serde_json::Value,
        kind: String,
        limit: Option<u32>,
        after: Option<String>,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<SliceTask>> {
        self.ensure_open()?;
        let operation = Operation::Preview(
            contract(request)?,
            contract(serde_json::Value::String(kind))?,
            page(limit, after, cancellation(env, signal)?)?,
        );
        Ok(crate::AsyncTask::new(SliceTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation,
        }))
    }
    /// Freeze exact Arrow membership/context without creating research ownership.
    #[napi]
    pub fn freeze_slice(
        &self,
        request: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<SliceTask>> {
        self.ensure_open()?;
        Ok(crate::AsyncTask::new(SliceTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Operation::Freeze(contract(request)?, cancellation(env, signal)?),
        }))
    }
    /// Inspect a frozen capsule without reading live parent content.
    #[napi]
    pub fn inspect_frozen_slice(
        &self,
        capsule: Buffer,
        kind: String,
        limit: Option<u32>,
        after: Option<String>,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<SliceTask>> {
        self.ensure_open()?;
        Ok(crate::AsyncTask::new(SliceTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Operation::Inspect(
                bounded_capsule(&capsule)?,
                contract(serde_json::Value::String(kind))?,
                page(limit, after, cancellation(env, signal)?)?,
            ),
        }))
    }
    /// Revise exact membership against the original or explicitly chosen Version.
    #[napi]
    pub fn revise_frozen_slice(
        &self,
        capsule: Buffer,
        revision: serde_json::Value,
        env: Env,
        #[napi(ts_arg_type = "AbortSignal | null | undefined")] signal: Option<crate::Object>,
    ) -> Result<crate::AsyncTask<SliceTask>> {
        self.ensure_open()?;
        Ok(crate::AsyncTask::new(SliceTask {
            engine: std::sync::Arc::clone(&self.inner),
            operation: Operation::Revise(
                bounded_capsule(&capsule)?,
                contract(revision)?,
                cancellation(env, signal)?,
            ),
        }))
    }
}
pub struct SliceTask {
    engine: std::sync::Arc<std::sync::RwLock<graphforge_api::GraphForge>>,
    operation: Operation,
}
impl Task for SliceTask {
    type Output = std::result::Result<Vec<u8>, graphforge_api::GfError>;
    type JsValue = Buffer;
    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self.engine.read().map_err(|_| {
                graphforge_api::GfError::Execution("GraphForge lock poisoned".into())
            })?;
            let result = match &self.operation {
                Operation::Preview(request, kind, page) => {
                    graph.preview_slice(request, *kind, page.clone())
                }
                Operation::Freeze(request, token) => graph.freeze_slice(request, token),
                Operation::Inspect(bytes, kind, page) => {
                    graph.inspect_frozen_slice(bytes, *kind, page.clone())
                }
                Operation::Revise(bytes, revision, token) => {
                    graph.revise_frozen_slice(bytes, revision, token)
                }
            }?;
            crate::result_to_ipc(&result)
        })())
    }
    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(Buffer::from)
            .map_err(|e| crate::to_napi_deferred_err(env, &e))
    }
}

fn bounded_capsule(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(crate::to_napi_err(&graphforge_api::GfError::Api {
            code: graphforge_api::ApiErrorCode::ResourceLimit,
            message: "Slice capsule exceeds byte bound".into(),
        }));
    }
    Ok(bytes.to_vec())
}
