//! Query bindings and native task ownership.

use crate::Arc;
use crate::AsyncTask;
use crate::AtomicBool;
use crate::Buffer;
use crate::Env;
use crate::GfError;
use crate::GraphForge;
use crate::HashMap;
use crate::IrLiteral;
use crate::Ordering;
use crate::Result;
use crate::RwLock;
use crate::Task;
use crate::napi;
use crate::params_from_map;
use crate::portable;
use crate::result_to_ipc;
use crate::to_napi_deferred_err;
use crate::to_napi_err;

/// A deferred query over a [`GraphForge`], produced by `GraphForge.plan(...)`.
///
/// Shares the parent engine (so it stays usable after the parent is dropped).
/// `explain()` is synchronous; `collectIpc()`/`sinkParquet()` run on a libuv
/// worker thread (napi `AsyncTask`) and return Promises — avoiding a `block_on`
/// inside napi's own runtime. Async rejections preserve GraphForge's structured
/// fault-domain code through the shared deferred-error bridge.
#[napi]
pub struct PlanHandle {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    closed: Arc<AtomicBool>,
    cypher: String,
    params: HashMap<String, IrLiteral>,
}

#[napi]
impl PlanHandle {
    /// Run the query and resolve to an Arrow IPC stream `Buffer`.
    #[napi]
    #[must_use]
    pub fn collect_ipc(&self) -> AsyncTask<CollectIpcTask> {
        AsyncTask::new(CollectIpcTask {
            engine: Arc::clone(&self.engine),
            closed: Arc::clone(&self.closed),
            cypher: self.cypher.clone(),
            params: self.params.clone(),
        })
    }

    /// Explain the compiler pipeline for the deferred query (synchronous).
    #[napi]
    pub fn explain(&self) -> Result<String> {
        if self.closed.load(Ordering::Acquire) {
            return Err(to_napi_err(&GfError::Lifecycle(
                "operation on a closed GraphForge instance".into(),
            )));
        }
        let g = self
            .engine
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))?;
        g.explain(&self.cypher).map_err(|e| to_napi_err(&e))
    }

    /// Run the query and write a streamed Parquet result with optional limits.
    #[napi]
    pub fn sink_parquet(
        &self,
        path: String,
        options: Option<portable::ResultSinkOptionsInput>,
    ) -> Result<AsyncTask<portable::SinkStreamTask>> {
        let (options, cancellation) = portable::parse_sink_options(options)?;
        Ok(AsyncTask::new(portable::SinkStreamTask {
            engine: Arc::clone(&self.engine),
            closed: Arc::clone(&self.closed),
            cypher: self.cypher.clone(),
            params: self.params.clone(),
            path,
            format: graphforge_api::ResultSinkFormat::Parquet,
            options,
            cancellation,
        }))
    }

    /// Run the query and write a streamed Arrow IPC result with optional limits.
    #[napi]
    pub fn sink_arrow_ipc(
        &self,
        path: String,
        options: Option<portable::ResultSinkOptionsInput>,
    ) -> Result<AsyncTask<portable::SinkStreamTask>> {
        let (options, cancellation) = portable::parse_sink_options(options)?;
        Ok(AsyncTask::new(portable::SinkStreamTask {
            engine: Arc::clone(&self.engine),
            closed: Arc::clone(&self.closed),
            cypher: self.cypher.clone(),
            params: self.params.clone(),
            path,
            format: graphforge_api::ResultSinkFormat::ArrowIpc,
            options,
            cancellation,
        }))
    }
}

/// `AsyncTask` backing [`PlanHandle::collect_ipc`].
pub struct CollectIpcTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    closed: Arc<AtomicBool>,
    cypher: String,
    params: HashMap<String, IrLiteral>,
}

impl Task for CollectIpcTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            if self.closed.load(Ordering::Acquire) {
                return Err(GfError::Lifecycle(
                    "operation on a closed GraphForge instance".into(),
                ));
            }
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result = graph.execute_with_params(&self.cypher, &self.params)?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

#[napi]
impl GraphForge {
    /// Run a Cypher query and return the result as an Arrow IPC stream `Buffer`
    /// (decode with apache-arrow `tableFromIPC`).
    ///
    /// `params` binds `$name` placeholders (values: JSON null/boolean/number/
    /// string/array/object, plus exact `{ "$uuid": "..." }` identity tags).
    /// Writes (`CREATE`/`SET`/`DELETE`/…) execute and return a summary.
    #[napi]
    pub fn execute(
        &self,
        cypher: String,
        params: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<Buffer> {
        let has_params = params.is_some();
        let p = params_from_map(params)?;
        let g = self.open_guard()?;
        let result = if has_params {
            g.execute_with_params(&cypher, &p)
        } else {
            g.execute(&cypher)
        }
        .map_err(|e| to_napi_err(&e))?;
        let bytes = result_to_ipc(&result).map_err(|e| to_napi_err(&e))?;
        Ok(Buffer::from(bytes))
    }

    /// Human-readable explanation of the compiler pipeline for `cypher`
    /// (`AST` → `GraphIR` → `LogicalPlan` → `PhysicalPlan`).
    #[napi]
    pub fn explain(&self, cypher: String) -> Result<String> {
        let g = self.open_guard()?;
        g.explain(&cypher).map_err(|e| to_napi_err(&e))
    }

    /// Prepare a deferred query, returning a [`PlanHandle`] for `explain()`
    /// (sync) and the async `collectIpc()` / `sinkParquet()` sinks.
    #[napi]
    pub fn plan(
        &self,
        cypher: String,
        params: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<PlanHandle> {
        self.ensure_open()?;
        let p = params_from_map(params)?;
        Ok(PlanHandle {
            engine: Arc::clone(&self.inner),
            closed: Arc::clone(&self.closed),
            cypher,
            params: p,
        })
    }
}
