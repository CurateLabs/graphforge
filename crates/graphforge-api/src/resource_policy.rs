//! Bounded per-instance embedded execution resource policy (#337).
//!
//! One normalized policy configures Tokio workers, DataFusion partitions /
//! batch size, memory, spill, I/O concurrency, and heavy-query admission
//! before a [`crate::GraphForge`] instance begins work. `compute_threads`
//! sizes the instance-owned private CPU pool consumed by parallel cosine KNN
//! (#342), parallel PageRank (#343), Node2Vec walk generation (#344),
//! triangle count (#588), and sibling deterministic CPU kernels.

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
///
/// The default (#1595) is `enabled` with no `directory`: a durable project's
/// queries spill into its own scratch directory
/// (`graphforge_storage::query_spill`), capped at `max_bytes` or
/// `DEFAULT_QUERY_SPILL_MAX_BYTES` per query, and an in-memory instance does
/// not spill at all. `enabled: false` never spills: a query over its memory
/// budget fails with a resource error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpillPolicy {
    /// When false, queries never spill to disk.
    pub enabled: bool,
    /// Optional spill directory. Relative paths are rejected. Symlinks and
    /// non-directories fail closed at normalize time. `None` with `enabled`
    /// selects the project scratch directory.
    pub directory: Option<PathBuf>,
    /// Optional upper bound on one query's temporary spill bytes.
    pub max_bytes: Option<u64>,
}

impl Default for SpillPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            directory: None,
            max_bytes: None,
        }
    }
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
    /// Maximum concurrent heavy Cypher / analyst invocations. `None` → 64.
    pub max_concurrent_heavy_queries: Option<usize>,
    /// Compute-thread budget for the instance-owned private CPU pool (#337 / #342 / #343 / #344 / #588).
    ///
    /// Parallel cosine KNN, PageRank destination updates, and Node2Vec walk
    /// generation, and global triangle counting partition work through that
    /// pool above documented crossovers; `1` keeps the serial path.
    pub compute_threads: Option<usize>,
    /// Compute threads construction may never use (#1586, ADR 0047).
    ///
    /// Every import on the instance shares one limit of `compute_threads -
    /// construction_cpu_reserve` parallel construction lanes, so queries keep
    /// at least this share of the compute budget while imports run. At least
    /// one, and below `compute_threads` unless that is one (a one-thread
    /// instance runs construction on one lane). `None` → the default, 1
    /// (see <https://github.com/CurateLabs/graphforge/blob/29a7b34ebe441a85ffb9274164d58aaeeb68dc8a/docs/development/evidence/construction-cpu-budget-1586.md>).
    pub construction_cpu_reserve: Option<usize>,
}

impl Default for ExecutionResourcePolicy {
    fn default() -> Self {
        Self {
            // Derive concurrency from machine parallelism (#1387). The former
            // fixed two-worker facade predates #337 and left the G500 ladder at
            // 0.89 effective cores across a 64x edge range on a 16-thread host,
            // because every default-constructed instance took two workers
            // whatever the machine had. `None` defers each knob to the mode.
            mode: ResourcePolicyMode::Automatic,
            tokio_worker_threads: None,
            target_partitions: None,
            batch_size: Some(DEFAULT_BATCH_SIZE),
            memory_budget_bytes: Some(DEFAULT_MEMORY_BUDGET_BYTES),
            spill: SpillPolicy::default(),
            io_concurrency: None,
            max_concurrent_heavy_queries: Some(DEFAULT_MAX_CONCURRENT_HEAVY_QUERIES),
            compute_threads: None,
            construction_cpu_reserve: None,
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
    /// Caller-configured absolute spill directory. `None` with spill enabled
    /// selects the project scratch directory (#1595).
    pub spill_directory: Option<PathBuf>,
    /// Optional spill byte cap.
    pub spill_max_bytes: Option<u64>,
    /// I/O concurrency budget.
    pub io_concurrency: usize,
    /// Heavy-query admission slots.
    pub max_concurrent_heavy_queries: usize,
    /// Compute-thread budget for the instance-owned private CPU pool (#342 / #343 / #344 / #588).
    pub compute_threads: usize,
    /// Compute threads construction may never use (#1586).
    pub construction_cpu_reserve: usize,
    /// Parallel construction lanes shared by every import (#1586).
    pub construction_cpu_limit: usize,
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
    /// Compute-thread budget for the private CPU pool.
    pub compute_threads: usize,
    /// Heavy-query admission limit.
    pub max_concurrent_heavy_queries: usize,
    /// Currently available heavy-query slots.
    pub heavy_query_available: usize,
    /// Compute threads construction may never use (#1586).
    pub construction_cpu_reserve: usize,
    /// Parallel construction lanes shared by every import (#1586).
    pub construction_cpu_limit: usize,
    /// Construction lanes currently leased.
    pub construction_cpu_in_use: usize,
    /// Most construction lanes leased at once since the instance opened.
    pub construction_cpu_peak: usize,
    /// Logical CPUs observed at normalize time.
    pub observed_logical_cpus: usize,
}

/// Fail-closed ceiling that still preserves concurrent same-instance reads
/// (pre-#337 had no admission semaphore).
pub(crate) const DEFAULT_MAX_CONCURRENT_HEAVY_QUERIES: usize = 64;
const DEFAULT_BATCH_SIZE: usize = 8_192;
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
        .map_or(1, usize::from)
        .clamp(MIN_THREADS, MAX_THREADS)
}

/// Query memory each default DataFusion partition is sized for. A sort
/// partition reserves `sort_spill_reservation_bytes` (10 MiB in DataFusion
/// 54) plus GraphForge's 1 MiB minimum run before it holds any data
/// (`graphforge-exec` `sort_runs`), all from the one `memory_budget` pool.
/// 32 MiB per partition keeps one sort's up-front reservations near a third of
/// the pool, so default queries spill rather than fail as partitions grow
/// with the machine.
const QUERY_PARTITION_MEMORY_BYTES: u64 = 32 * 1024 * 1024;

/// Automatic-mode defaults, derived from the machine and the memory budget,
/// never from a host-tuned ceiling (#1863).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AutomaticDefaults {
    /// Tokio workers and I/O concurrency: half the logical CPUs.
    half: usize,
    /// Compute threads, so the construction admission: every logical CPU.
    all: usize,
    /// DataFusion partitions: half the CPUs, as many as the query memory
    /// budget can host at [`QUERY_PARTITION_MEMORY_BYTES`] each.
    partitions: usize,
}

fn automatic_defaults(observed: usize, memory_budget_bytes: u64) -> AutomaticDefaults {
    if observed <= 2 {
        // Small machines stay serial.
        return AutomaticDefaults {
            half: 1,
            all: 1,
            partitions: 1,
        };
    }
    let half = observed.div_ceil(2);
    let affordable = usize::try_from(memory_budget_bytes / QUERY_PARTITION_MEMORY_BYTES)
        .unwrap_or(usize::MAX)
        .max(1);
    AutomaticDefaults {
        half,
        all: observed,
        partitions: half.min(affordable),
    }
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
        let meta = std::fs::symlink_metadata(path)
            .map_err(|e| validation(format!("spill directory metadata unavailable: {e}")))?;
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
    #[allow(clippy::too_many_lines)]
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
                let defaults = automatic_defaults(
                    observed,
                    self.memory_budget_bytes
                        .unwrap_or(DEFAULT_MEMORY_BUDGET_BYTES),
                );
                let (half, all) = (defaults.half, defaults.all);
                let workers = validate_thread_count(
                    "tokio_worker_threads",
                    self.tokio_worker_threads.unwrap_or(half),
                )?;
                let partitions = validate_thread_count(
                    "target_partitions",
                    self.target_partitions
                        .unwrap_or(defaults.partitions.min(workers)),
                )?;
                let io = validate_thread_count(
                    "io_concurrency",
                    self.io_concurrency.unwrap_or(half.min(workers)),
                )?;
                let compute =
                    validate_thread_count("compute_threads", self.compute_threads.unwrap_or(all))?;
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

        let (construction_reserve, construction_limit) =
            construction_cpu_split(compute, self.construction_cpu_reserve)?;

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

        let heavy = self
            .max_concurrent_heavy_queries
            .unwrap_or(DEFAULT_MAX_CONCURRENT_HEAVY_QUERIES);
        if !(1..=64).contains(&heavy) {
            return Err(validation(
                "max_concurrent_heavy_queries must be between 1 and 64",
            ));
        }

        let (spill_enabled, spill_directory, spill_max_bytes) = if self.spill.enabled {
            // No directory selects the project scratch directory (#1595),
            // resolved when an instance opens.
            let dir = self
                .spill
                .directory
                .as_ref()
                .map(|dir| validate_spill_directory(dir))
                .transpose()?;
            if let Some(max) = self.spill.max_bytes
                && max == 0
            {
                return Err(validation("spill.max_bytes must be greater than zero"));
            }
            (true, dir, self.spill.max_bytes)
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
            construction_cpu_reserve: construction_reserve,
            construction_cpu_limit: construction_limit,
            observed_logical_cpus: observed,
        })
    }
}

/// Default compute threads kept from construction: one (#1586 evidence).
///
/// Measured at 4 and 8 compute threads, a larger reserve did not lower query
/// latency during concurrent imports beyond run-to-run variation, so the
/// smallest reserve ADR 0047 allows is the default; it costs imports least.
pub(crate) fn default_construction_cpu_reserve(_compute_threads: usize) -> usize {
    1
}

/// Validate the construction reserve and derive the shared lane limit.
fn construction_cpu_split(
    compute_threads: usize,
    requested: Option<usize>,
) -> Result<(usize, usize), GfError> {
    let reserve = requested.unwrap_or_else(|| default_construction_cpu_reserve(compute_threads));
    if reserve == 0 {
        return Err(validation("construction_cpu_reserve must be at least one"));
    }
    if compute_threads == 1 {
        // One compute thread cannot be split: construction runs one lane and
        // queries run inline on their callers.
        return Ok((reserve, 1));
    }
    if reserve >= compute_threads {
        return Err(validation(format!(
            "construction_cpu_reserve {reserve} must be below compute_threads {compute_threads}"
        )));
    }
    Ok((reserve, compute_threads - reserve))
}

impl NormalizedResourcePolicy {
    /// A fresh instance construction admission sized by this policy (#1586).
    pub(crate) fn construction_cpu_admission(
        &self,
    ) -> std::sync::Arc<graphforge_storage::ConstructionCpuAdmission> {
        let limit = std::num::NonZeroUsize::new(self.construction_cpu_limit)
            .unwrap_or(std::num::NonZeroUsize::MIN);
        std::sync::Arc::new(graphforge_storage::ConstructionCpuAdmission::new(limit))
    }

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
}

impl HeavyQueryAdmission {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            slots: Arc::new(tokio::sync::Semaphore::new(limit)),
        }
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
    #[test]
    fn construction_reserve_defaults_to_one_and_leaves_queries_a_share() {
        for (compute, reserve, limit) in [(1, 1, 1), (2, 1, 1), (4, 1, 3), (8, 1, 7), (16, 1, 15)] {
            let normalized = ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                tokio_worker_threads: Some(compute.min(4)),
                compute_threads: Some(compute),
                ..Default::default()
            }
            .normalize();
            // Some hosts cannot admit 16 compute threads; skip those rows.
            let Ok(normalized) = normalized else {
                continue;
            };
            assert_eq!(
                normalized.construction_cpu_reserve, reserve,
                "compute={compute}"
            );
            assert_eq!(
                normalized.construction_cpu_limit, limit,
                "compute={compute}"
            );
            assert_eq!(
                normalized.construction_cpu_admission().limit(),
                limit,
                "compute={compute}"
            );
        }
    }

    #[test]
    fn construction_reserve_is_validated() {
        let policy = |compute, reserve| ExecutionResourcePolicy {
            mode: ResourcePolicyMode::Explicit,
            tokio_worker_threads: Some(2),
            compute_threads: Some(compute),
            construction_cpu_reserve: Some(reserve),
            ..Default::default()
        };
        assert!(
            policy(2, 0)
                .normalize()
                .unwrap_err()
                .to_string()
                .contains("at least one")
        );
        assert!(
            policy(2, 2)
                .normalize()
                .unwrap_err()
                .to_string()
                .contains("must be below compute_threads")
        );
        let split = policy(2, 1).normalize().unwrap();
        assert_eq!(
            (split.construction_cpu_reserve, split.construction_cpu_limit),
            (1, 1)
        );
        // One compute thread cannot be split: construction keeps one lane.
        let single = policy(1, 3).normalize().unwrap();
        assert_eq!(single.construction_cpu_limit, 1);
    }

    use super::*;

    #[test]
    fn defaults_derive_concurrency_from_machine_parallelism() {
        let normalized = ExecutionResourcePolicy::default()
            .normalize()
            .expect("default policy");
        let observed = logical_cpus();
        let (half, all) = if observed <= 2 {
            (1, 1)
        } else {
            (observed.div_ceil(2), observed)
        };
        assert_eq!(normalized.tokio_worker_threads, half);
        assert_eq!(
            normalized.target_partitions,
            automatic_defaults(observed, DEFAULT_MEMORY_BUDGET_BYTES).partitions
        );
        assert_eq!(normalized.io_concurrency, half);
        // #1863: no fixed ceiling; compute and the construction admission
        // scale with every logical CPU the machine has.
        assert_eq!(normalized.compute_threads, all);
        assert_eq!(normalized.mode, ResourcePolicyMode::Automatic);
        // #1595: spill into the project scratch directory by default.
        assert!(normalized.spill_enabled);
        assert_eq!(normalized.spill_directory, None);
        assert_eq!(normalized.spill_max_bytes, None);
        assert_eq!(normalized.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(normalized.memory_budget_bytes, DEFAULT_MEMORY_BUDGET_BYTES);
        assert_eq!(
            normalized.max_concurrent_heavy_queries,
            DEFAULT_MAX_CONCURRENT_HEAVY_QUERIES
        );
    }

    /// #1863: defaults scale with the machine, and DataFusion partitions also
    /// stay within what the query memory budget can host, so default sorts
    /// on a many-core machine spill instead of failing their reservations.
    #[test]
    fn automatic_defaults_scale_with_cpus_within_the_query_memory_budget() {
        let mib = 1024 * 1024;
        for (cpus, budget, half, all, partitions) in [
            (1, DEFAULT_MEMORY_BUDGET_BYTES, 1, 1, 1),
            (2, DEFAULT_MEMORY_BUDGET_BYTES, 1, 1, 1),
            (16, DEFAULT_MEMORY_BUDGET_BYTES, 8, 16, 8),
            (96, DEFAULT_MEMORY_BUDGET_BYTES, 48, 96, 16),
            (256, DEFAULT_MEMORY_BUDGET_BYTES, 128, 256, 16),
            (96, 4096 * mib, 48, 96, 48),
            (16, 16 * mib, 8, 16, 1),
        ] {
            let defaults = automatic_defaults(cpus, budget);
            assert_eq!(
                (defaults.half, defaults.all, defaults.partitions),
                (half, all, partitions),
                "{cpus} CPUs, {budget} bytes"
            );
            assert!(
                defaults.partitions == 1
                    || defaults.partitions as u64 * QUERY_PARTITION_MEMORY_BYTES <= budget,
                "{cpus} CPUs, {budget} bytes"
            );
        }
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

    /// #1595: spill enabled with no directory selects the project scratch
    /// directory, with the caller's cap; a zero cap and a cap or directory
    /// without spill are still refused.
    #[test]
    fn spill_without_directory_selects_project_scratch() {
        let normalized = ExecutionResourcePolicy {
            spill: SpillPolicy {
                enabled: true,
                directory: None,
                max_bytes: Some(1024),
            },
            ..ExecutionResourcePolicy::default()
        }
        .normalize()
        .unwrap();
        assert!(normalized.spill_enabled);
        assert_eq!(normalized.spill_directory, None);
        assert_eq!(normalized.spill_max_bytes, Some(1024));
        for spill in [
            SpillPolicy {
                enabled: true,
                directory: None,
                max_bytes: Some(0),
            },
            SpillPolicy {
                enabled: false,
                directory: None,
                max_bytes: Some(1024),
            },
        ] {
            let err = ExecutionResourcePolicy {
                spill,
                ..ExecutionResourcePolicy::default()
            }
            .normalize()
            .expect_err("invalid spill");
            assert!(matches!(err, GfError::Validation(_)));
        }
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
            construction_cpu_reserve: None,
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
            let max_primary = observed
                .saturating_mul(2)
                .clamp(4, MAX_THREADS.saturating_mul(2));
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
        let n = observed
            .saturating_mul(2)
            .saturating_add(1)
            .min(MAX_THREADS);
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
