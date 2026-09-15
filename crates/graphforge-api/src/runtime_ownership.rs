//! Runtime lifetime, synchronous execution, and query admission.

use super::{GfError, GraphForge, GraphWorkspace, NormalizedResourcePolicy, resource_policy};
use std::sync::Arc;

impl GraphForge {
    /// Normalized execution resource policy for this instance (#337).
    #[must_use]
    pub fn resource_policy(&self) -> &NormalizedResourcePolicy {
        &self.resource_policy
    }

    /// Safe aggregate diagnostics for the instance resource policy (#337).
    #[must_use]
    pub fn resource_diagnostics(&self) -> resource_policy::ResourcePolicyDiagnostics {
        resource_policy::ResourcePolicyDiagnostics {
            mode: self.resource_policy.mode,
            tokio_worker_threads: self.resource_policy.tokio_worker_threads,
            target_partitions: self.resource_policy.target_partitions,
            batch_size: self.resource_policy.batch_size,
            memory_budget_bytes: self.resource_policy.memory_budget_bytes,
            spill_enabled: self.resource_policy.spill_enabled,
            io_concurrency: self.resource_policy.io_concurrency,
            compute_threads: self.resource_policy.compute_threads,
            max_concurrent_heavy_queries: self.resource_policy.max_concurrent_heavy_queries,
            heavy_query_available: self.heavy_query_admission.available_permits(),
            observed_logical_cpus: self.resource_policy.observed_logical_cpus,
        }
    }

    pub(super) fn session_resource_config(&self) -> graphforge_exec::SessionResourceConfig {
        graphforge_exec::SessionResourceConfig {
            target_partitions: self.resource_policy.target_partitions,
            batch_size: self.resource_policy.batch_size,
            memory_budget_bytes: self.resource_policy.memory_budget_bytes,
            spill_enabled: self.resource_policy.spill_enabled,
            spill_directory: self.resource_policy.spill_directory.clone(),
            spill_max_bytes: self.resource_policy.spill_max_bytes,
            io_concurrency: self.resource_policy.io_concurrency,
        }
    }

    pub(super) fn admit_heavy_query(&self) -> Result<tokio::sync::SemaphorePermit<'_>, GfError> {
        self.graph_visibility.health.check()?;
        self.heavy_query_admission.try_acquire()
    }

    pub(super) fn admit_heavy_query_owned(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, GfError> {
        self.graph_visibility.health.check()?;
        self.heavy_query_admission.try_acquire_owned()
    }

    /// Drive a future on the instance's runtime from a synchronous caller.
    ///
    /// `Handle::block_on` panics if the calling thread is already inside a Tokio
    /// runtime (e.g. an async test/harness like the cucumber BDD runner), so in
    /// that case run on a scoped thread — outside any ambient runtime — that
    /// blocks on our (multi-thread) runtime's handle instead.
    pub(super) fn block_on<T, F>(&self, fut: F) -> Result<T, GfError>
    where
        T: Send,
        F: std::future::Future<Output = Result<T, GfError>> + Send,
    {
        let handle = self.runtime.handle().clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            let capture_session = graphforge_exec::demand::bound_capture_session();
            std::thread::scope(|s| {
                s.spawn(|| {
                    graphforge_exec::demand::set_bound_capture_session(capture_session);
                    handle.block_on(fut)
                })
                .join()
                .map_err(|_| GfError::Execution("execution thread panicked".into()))?
            })
        } else {
            handle.block_on(fut)
        }
    }
}

/// A long-lived multi-thread Tokio runtime that shuts down **without blocking**
/// on drop.
///
/// `GraphForge` owns this for its lifetime so a streaming query's background
/// tasks (repartition/coalesce) run on worker threads that outlive the call
/// that created the stream — a per-call runtime would cancel them mid-stream.
///
/// Dropping a bare `tokio::runtime::Runtime` from inside an async context panics
/// ("Cannot drop a runtime in a context where blocking is not allowed"), and a
/// `GraphForge` may well be dropped inside someone else's async task (e.g. the
/// cucumber harness). The `Drop` here calls `shutdown_background`, which returns
/// immediately and never blocks, so dropping is safe from any context.
#[derive(Debug)]
pub(super) struct OwnedRuntime(Option<tokio::runtime::Runtime>);

impl OwnedRuntime {
    fn handle(&self) -> &tokio::runtime::Handle {
        self.0
            .as_ref()
            .expect("runtime present until drop")
            .handle()
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

/// An opaque guard that keeps a [`GraphForge`]'s Tokio runtime and on-disk graph
/// workspace alive after the instance is dropped, so a detached
/// [`execute_stream_owned`](GraphForge::execute_stream_owned) stream can still
/// be driven to completion (e.g. a lazy `pyarrow.RecordBatchReader`, #587).
///
/// Cheap to clone (`Arc` bumps). The runtime shuts down and temp workspaces are
/// removed only once the `GraphForge` and all guards have dropped. Streaming
/// Parquet scans (#339) open fragment paths at pull time, so pinning the
/// workspace is required for the same lifetime contract MemTable planning had.
#[derive(Clone, Debug)]
pub struct RuntimeGuard {
    pub(super) runtime: Arc<OwnedRuntime>,
    /// Private mutable graph workspace hydrated for this facade (`dir`).
    /// Held solely so `TempDir` cleanup waits until stream consumers finish.
    #[allow(dead_code)]
    pub(super) workspace: GraphWorkspace,
    /// In-memory project root, when the facade is not path-backed.
    #[allow(dead_code)]
    pub(super) tempdir: Option<Arc<tempfile::TempDir>>,
}

impl RuntimeGuard {
    /// Drive a future to completion on the guarded runtime from a synchronous
    /// caller.
    ///
    /// `Handle::block_on` panics if the calling thread is already inside a Tokio
    /// runtime, so — mirroring [`GraphForge::block_on`] — detect that and run on
    /// a scoped thread outside any ambient runtime. A panic inside `fut` resumes
    /// on the caller (callers across an FFI boundary must guard with
    /// `catch_unwind`).
    pub fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        let handle = self.runtime.handle().clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            let capture_session = graphforge_exec::demand::bound_capture_session();
            std::thread::scope(|s| {
                s.spawn(|| {
                    graphforge_exec::demand::set_bound_capture_session(capture_session);
                    handle.block_on(fut)
                })
                .join()
                .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
            })
        } else {
            handle.block_on(fut)
        }
    }
}

/// Build the instance's long-lived multi-thread runtime.
pub(super) fn build_runtime(
    policy: &resource_policy::NormalizedResourcePolicy,
) -> Result<Arc<OwnedRuntime>, GfError> {
    policy
        .build_tokio_runtime()
        .map(|rt| Arc::new(OwnedRuntime(Some(rt))))
}

#[cfg(test)]
mod tests;
