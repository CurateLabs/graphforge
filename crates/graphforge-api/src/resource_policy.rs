//! Bounded per-instance embedded execution resource policy (#337).
//!
//! One normalized policy configures Tokio workers, DataFusion partitions /
//! batch size, memory, spill, I/O concurrency, and heavy-query admission
//! before a [`crate::GraphForge`] instance begins work. Algorithm kernels are
//! not parallelized here; `compute_threads` reserves a future private pool.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::thread;

use graphforge_core::{ApiErrorCode, GfError};

/// How requested knobs are interpreted at construction time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResourcePolicyMode {
    /// Caller-supplied knobs (with documented defaults for omitted fields).
    #[default]
    Explicit,
    /// Derive a bounded configuration from machine parallelism and memory.
    Automatic,
}

/// Fail-closed spill configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SpillPolicy {
    /// When false, DataFusion must not spill to disk.
    pub enabled: bool,
    /// Optional spill directory. Relative paths are rejected; must be absolute
    /// when enabled. Symlinks and non-directories fail closed at normalize time.
    pub directory: Option<PathBuf>,
    /// Optional upper bound on temporary spill bytes.
    pub max_bytes: Option<u64>,
}

/// Requested (pre-normalization) execution resource policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionResourcePolicy {
    /// Explicit vs automatic selection.
    pub mode: ResourcePolicyMode,
    /// Tokio multi-thread worker count. `None` → mode default.
    pub tokio_worker_threads: Option<usize>,
    /// DataFusion `target_partitions`. `None` → mode default.
    pub target_partitions: Option<usize>,
    /// DataFusion batch size. `None` → DataFusion/GraphForge default (8192).
    pub batch_size: Option<usize>,
    /// Soft memory budget for the DataFusion memory pool. `None` → 512 MiB.
    pub memory_budget_bytes: Option<u64>,
    /// Spill configuration.
    pub spill: SpillPolicy,
    /// Bound on concurrent filtered/storage I/O helpers. `None` → mode default.
    pub io_concurrency: Option<usize>,
    /// Maximum concurrent heavy Cypher / analyst invocations. `None` → 1.
    pub max_concurrent_heavy_queries: Option<usize>,
    /// Reserved compute-thread budget for future private CPU pools. Not used to
    /// parallelize algorithms in #337.
    pub compute_threads: Option<usize>,
}

impl Default for ExecutionResourcePolicy {
    fn default() -> Self {
        Self {
            // Preserve pre-#337 behavior: fixed two-worker facade.
            mode: ResourcePolicyMode::Explicit,
            tokio_worker_threads: Some(2),
            target_partitions: Some(2),
            batch_size: Some(DEFAULT_BATCH_SIZE),
            memory_budget_bytes: Some(DEFAULT_MEMORY_BUDGET_BYTES),
            spill: SpillPolicy::default(),
            io_concurrency: Some(2),
            max_concurrent_heavy_queries: Some(1),
            compute_threads: Some(2),
        }
    }
}

/// Immutable normalized policy applied to runtime + DataFusion adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedResourcePolicy {
    /// Selection mode that produced this policy.
    pub mode: ResourcePolicyMode,
    /// Tokio worker threads.
    pub tokio_worker_threads: usize,
    /// DataFusion target partitions.
    pub target_partitions: usize,
    /// DataFusion batch size.
    pub batch_size: usize,
    /// Memory budget bytes for the session memory pool.
    pub memory_budget_bytes: u64,
    /// Whether spill is enabled.
    pub spill_enabled: bool,
    /// Absolute spill directory when spill is enabled.
    pub spill_directory: Option<PathBuf>,
    /// Optional spill byte cap.
    pub spill_max_bytes: Option<u64>,
    /// I/O concurrency budget.
    pub io_concurrency: usize,
    /// Heavy-query admission slots.
    pub max_concurrent_heavy_queries: usize,
    /// Reserved compute-thread budget (future private pool).
    pub compute_threads: usize,
    /// Machine logical parallelism observed at normalize time.
    pub observed_logical_cpus: usize,
}

/// Safe aggregate diagnostics for an instance resource policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourcePolicyDiagnostics {
    /// Selection mode.
    pub mode: ResourcePolicyMode,
    /// Tokio workers.
    pub tokio_worker_threads: usize,
    /// DataFusion target partitions.
    pub target_partitions: usize,
    /// DataFusion batch size.
    pub batch_size: usize,
    /// Memory budget bytes.
    pub memory_budget_bytes: u64,
    /// Whether spill is enabled.
    pub spill_enabled: bool,
    /// I/O concurrency budget.
    pub io_concurrency: usize,
    /// Reserved compute threads.
    pub compute_threads: usize,
    /// Heavy-query admission limit.
    pub max_concurrent_heavy_queries: usize,
    /// Currently available heavy-query slots.
    pub heavy_query_available: usize,
    /// Logical CPUs observed at normalize time.
    pub observed_logical_cpus: usize,
}

pub(crate) const DEFAULT_BATCH_SIZE: usize = 8_192;
pub(crate) const DEFAULT_MEMORY_BUDGET_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const MIN_THREADS: usize = 1;
pub(crate) const MAX_THREADS: usize = 256;
pub(crate) const MIN_BATCH_SIZE: usize = 1;
pub(crate) const MAX_BATCH_SIZE: usize = 1_048_576;
pub(crate) const MIN_MEMORY_BUDGET_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const MAX_MEMORY_BUDGET_BYTES: u64 = 1024 * 1024 * 1024 * 1024; // 1 TiB

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn resource_limit(message: impl Into<String>) -> GfError {
    GfError::Api {
        code: ApiErrorCode::ResourceLimit,
        message: message.into(),
    }
}

fn logical_cpus() -> usize {
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .clamp(MIN_THREADS, MAX_THREADS)
}

fn validate_thread_count(label: &str, value: usize) -> Result<usize, GfError> {
    if !(MIN_THREADS..=MAX_THREADS).contains(&value) {
        return Err(validation(format!(
            "{label} must be between {MIN_THREADS} and {MAX_THREADS}"
        )));
    }
    Ok(value)
}

fn validate_spill_directory(path: &Path) -> Result<PathBuf, GfError> {
    if !path.is_absolute() {
        return Err(validation(
            "spill directory must be an absolute path when spill is enabled",
        ));
    }
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(validation(
                "spill directory must not contain parent-directory components",
            ));
        }
    }
    if path.exists() {
        let meta = std::fs::symlink_metadata(path).map_err(|e| {
            validation(format!("spill directory metadata unavailable: {e}"))
        })?;
        if meta.file_type().is_symlink() {
            return Err(validation("spill directory must not be a symlink"));
        }
        if !meta.is_dir() {
            return Err(validation("spill directory must be a directory"));
        }
    }
    Ok(path.to_path_buf())
}

impl ExecutionResourcePolicy {
    /// Normalize and validate this policy for a new GraphForge instance.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for unsafe/unsupported settings.
    pub fn normalize(self) -> Result<NormalizedResourcePolicy, GfError> {
        let observed = logical_cpus();
        let (tokio_workers, partitions, io_conc, compute) = match self.mode {
            ResourcePolicyMode::Explicit => {
                let workers = validate_thread_count(
                    "tokio_worker_threads",
                    self.tokio_worker_threads.unwrap_or(2),
                )?;
                let partitions = validate_thread_count(
                    "target_partitions",
                    self.target_partitions.unwrap_or(workers),
                )?;
                let io = validate_thread_count(
                    "io_concurrency",
                    self.io_concurrency.unwrap_or(workers),
                )?;
                let compute = validate_thread_count(
                    "compute_threads",
                    self.compute_threads.unwrap_or(workers),
                )?;
                (workers, partitions, io, compute)
            }
            ResourcePolicyMode::Automatic => {
                // Prefer a bounded fraction of logical CPUs; small machines stay
                // serial/minimally partitioned.
                let auto = if observed <= 2 {
                    1
                } else {
                    observed.div_ceil(2).clamp(MIN_THREADS, 8)
                };
                let workers = validate_thread_count(
                    "tokio_worker_threads",
                    self.tokio_worker_threads.unwrap_or(auto),
                )?;
                let partitions = validate_thread_count(
                    "target_partitions",
                    self.target_partitions.unwrap_or(auto.min(workers)),
                )?;
                let io = validate_thread_count(
                    "io_concurrency",
                    self.io_concurrency.unwrap_or(auto.min(workers)),
                )?;
                let compute = validate_thread_count(
                    "compute_threads",
                    self.compute_threads.unwrap_or(auto.min(workers)),
                )?;
                (workers, partitions, io, compute)
            }
        };

        // Primary schedulers (Tokio + DataFusion partitions) must stay within a
        // machine-relative budget. Reserved I/O and future compute pools must
        // not individually exceed that same cap — they are not free extras.
        let primary = tokio_workers.saturating_add(partitions);
        let max_primary = observed
            .saturating_mul(2)
            .clamp(4, MAX_THREADS.saturating_mul(2));
        if primary > max_primary {
            return Err(validation(format!(
                "combined tokio/partition concurrency {primary} exceeds instance budget {max_primary}"
            )));
        }
        let reserve_cap = tokio_workers.max(observed).clamp(MIN_THREADS, MAX_THREADS);
        if io_conc > reserve_cap {
            return Err(validation(format!(
                "io_concurrency {io_conc} exceeds reserve cap {reserve_cap}"
            )));
        }
        if compute > reserve_cap {
            return Err(validation(format!(
                "compute_threads {compute} exceeds reserve cap {reserve_cap}"
            )));
        }

        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        if !(MIN_BATCH_SIZE..=MAX_BATCH_SIZE).contains(&batch_size) {
            return Err(validation(format!(
                "batch_size must be between {MIN_BATCH_SIZE} and {MAX_BATCH_SIZE}"
            )));
        }

        let memory_budget_bytes = self
            .memory_budget_bytes
            .unwrap_or(DEFAULT_MEMORY_BUDGET_BYTES);
        if !(MIN_MEMORY_BUDGET_BYTES..=MAX_MEMORY_BUDGET_BYTES).contains(&memory_budget_bytes) {
            return Err(validation(format!(
                "memory_budget_bytes must be between {MIN_MEMORY_BUDGET_BYTES} and {MAX_MEMORY_BUDGET_BYTES}"
            )));
        }

        let heavy = self.max_concurrent_heavy_queries.unwrap_or(1);
        if !(1..=64).contains(&heavy) {
            return Err(validation(
                "max_concurrent_heavy_queries must be between 1 and 64",
            ));
        }

        let (spill_enabled, spill_directory, spill_max_bytes) = if self.spill.enabled {
            let Some(dir) = self.spill.directory.as_ref() else {
                return Err(validation(
                    "spill.directory is required when spill is enabled",
                ));
            };
            let dir = validate_spill_directory(dir)?;
            if let Some(max) = self.spill.max_bytes {
                if max == 0 {
                    return Err(validation("spill.max_bytes must be greater than zero"));
                }
            }
            (true, Some(dir), self.spill.max_bytes)
        } else {
            if self.spill.directory.is_some() || self.spill.max_bytes.is_some() {
                return Err(validation(
                    "spill directory/max_bytes require spill.enabled=true",
                ));
            }
            (false, None, None)
        };

        Ok(NormalizedResourcePolicy {
            mode: self.mode,
            tokio_worker_threads: tokio_workers,
            target_partitions: partitions,
            batch_size,
            memory_budget_bytes,
            spill_enabled,
            spill_directory,
            spill_max_bytes,
            io_concurrency: io_conc,
            max_concurrent_heavy_queries: heavy,
            compute_threads: compute,
            observed_logical_cpus: observed,
        })
    }
}

impl NormalizedResourcePolicy {
    /// Build a Tokio multi-thread runtime honoring this policy.
    pub(crate) fn build_tokio_runtime(&self) -> Result<tokio::runtime::Runtime, GfError> {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.tokio_worker_threads)
            .enable_all()
            .build()
            .map_err(|e| GfError::Execution(format!("failed to build runtime: {e}")))
    }
}

/// Instance-owned heavy-query admission gate.
pub(crate) struct HeavyQueryAdmission {
    slots: Arc<tokio::sync::Semaphore>,
    limit: usize,
}

impl HeavyQueryAdmission {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            slots: Arc::new(tokio::sync::Semaphore::new(limit)),
            limit,
        }
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit
    }

    pub(crate) fn available_permits(&self) -> usize {
        self.slots.available_permits()
    }

    pub(crate) fn try_acquire(&self) -> Result<tokio::sync::SemaphorePermit<'_>, GfError> {
        self.slots
            .try_acquire()
            .map_err(|_| resource_limit("heavy query admission limit exceeded"))
    }

    pub(crate) fn try_acquire_owned(&self) -> Result<tokio::sync::OwnedSemaphorePermit, GfError> {
        Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| resource_limit("heavy query admission limit exceeded"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_fixed_two_worker_baseline() {
        let normalized = ExecutionResourcePolicy::default()
            .normalize()
            .expect("default policy");
        assert_eq!(normalized.tokio_worker_threads, 2);
        assert_eq!(normalized.target_partitions, 2);
        assert_eq!(normalized.mode, ResourcePolicyMode::Explicit);
        assert!(!normalized.spill_enabled);
        assert_eq!(normalized.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(normalized.memory_budget_bytes, DEFAULT_MEMORY_BUDGET_BYTES);
        assert_eq!(normalized.max_concurrent_heavy_queries, 1);
    }

    #[test]
    fn unsupported_thread_counts_fail_closed() {
        let err = ExecutionResourcePolicy {
            tokio_worker_threads: Some(0),
            ..ExecutionResourcePolicy::default()
        }
        .normalize()
        .expect_err("zero workers");
        assert!(matches!(err, GfError::Validation(_)));

        let err = ExecutionResourcePolicy {
            tokio_worker_threads: Some(512),
            ..ExecutionResourcePolicy::default()
        }
        .normalize()
        .expect_err("too many workers");
        assert!(matches!(err, GfError::Validation(_)));
    }

    #[test]
    fn spill_without_directory_fails() {
        let err = ExecutionResourcePolicy {
            spill: SpillPolicy {
                enabled: true,
                directory: None,
                max_bytes: Some(1024),
            },
            ..ExecutionResourcePolicy::default()
        }
        .normalize()
        .expect_err("spill needs directory");
        assert!(matches!(err, GfError::Validation(_)));
    }

    #[test]
    fn relative_spill_path_fails() {
        let err = ExecutionResourcePolicy {
            spill: SpillPolicy {
                enabled: true,
                directory: Some(PathBuf::from("relative/spill")),
                max_bytes: None,
            },
            ..ExecutionResourcePolicy::default()
        }
        .normalize()
        .expect_err("relative spill");
        assert!(matches!(err, GfError::Validation(_)));
    }

    #[test]
    fn automatic_mode_records_selection() {
        let normalized = ExecutionResourcePolicy {
            mode: ResourcePolicyMode::Automatic,
            tokio_worker_threads: None,
            target_partitions: None,
            batch_size: None,
            memory_budget_bytes: None,
            spill: SpillPolicy::default(),
            io_concurrency: None,
            max_concurrent_heavy_queries: None,
            compute_threads: None,
        }
        .normalize()
        .expect("automatic");
        assert_eq!(normalized.mode, ResourcePolicyMode::Automatic);
        assert!(normalized.tokio_worker_threads >= 1);
        assert!(normalized.target_partitions >= 1);
        assert!(normalized.observed_logical_cpus >= 1);
        // Small-workload low-overhead path: <=2 CPUs stay serial/minimal.
        if normalized.observed_logical_cpus <= 2 {
            assert_eq!(normalized.tokio_worker_threads, 1);
            assert_eq!(normalized.target_partitions, 1);
        }
    }

    #[test]
    fn explicit_one_through_eight_honor_machine_budget() {
        let observed = logical_cpus();
        for n in [1_usize, 2, 4, 8] {
            let result = ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                tokio_worker_threads: Some(n),
                target_partitions: Some(n),
                io_concurrency: Some(n.min(observed.max(n))),
                compute_threads: Some(n.min(observed.max(n))),
                ..ExecutionResourcePolicy::default()
            }
            .normalize();
            let max_primary = observed.saturating_mul(2).clamp(4, MAX_THREADS.saturating_mul(2));
            if n.saturating_add(n) > max_primary {
                assert!(
                    result.is_err(),
                    "{n} should fail closed when over budget on {observed} CPUs"
                );
            } else {
                let normalized = result.unwrap_or_else(|e| panic!("{n}: {e}"));
                assert_eq!(normalized.tokio_worker_threads, n);
                assert_eq!(normalized.target_partitions, n);
            }
        }
    }

    #[test]
    fn heavy_admission_rejects_overflow() {
        let gate = HeavyQueryAdmission::new(1);
        let _permit = gate.try_acquire().expect("first slot");
        let err = gate.try_acquire().expect_err("second slot");
        assert_eq!(err.code(), "GF_RESOURCE_LIMIT");
        assert_eq!(gate.available_permits(), 0);
    }

    #[test]
    fn combined_budget_rejects_oversubscription() {
        let observed = logical_cpus();
        let n = observed.saturating_mul(2).saturating_add(1).min(MAX_THREADS);
        let err = ExecutionResourcePolicy {
            mode: ResourcePolicyMode::Explicit,
            tokio_worker_threads: Some(n),
            target_partitions: Some(n),
            io_concurrency: Some(1),
            compute_threads: Some(1),
            ..ExecutionResourcePolicy::default()
        }
        .normalize()
        .expect_err("oversubscribed primary concurrency");
        assert!(matches!(err, GfError::Validation(_)));
    }
}
