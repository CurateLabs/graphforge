//! Durable, content-free embedding refresh policy and terminal outcomes.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    EmbeddingCompatibilityId, EmbeddingSourceFingerprint, SearchArtifactError,
    SearchCoordinationLimits,
};

/// Refresh-control document schema implemented by this release.
pub const EMBEDDING_REFRESH_CONFIG_VERSION: u32 = 1;
/// Default process-local proactive refresh debounce.
pub const DEFAULT_EMBEDDING_REFRESH_DEBOUNCE: Duration = Duration::from_millis(500);
/// Default and maximum producer concurrency for one embedded project.
pub const MAX_EMBEDDING_REFRESH_JOBS: usize = 2;
/// Maximum accepted refresh-control bytes by default.
pub const MAX_EMBEDDING_REFRESH_CONFIG_BYTES: usize = 256 * 1024;
/// Maximum per-lineage policy/outcome records by default.
pub const MAX_EMBEDDING_REFRESH_CONFIG_ENTRIES: usize = 1_024;

const EMBEDDINGS_DIR: &str = "embeddings";
const CONFIG_FILE: &str = "refresh.json";
const CONFIG_LOCK: &str = ".refresh.lock";
const CHECKSUM_DOMAIN: &[u8] = b"graphforge.embedding.refresh-config.v1\0";
const MAX_DEBOUNCE: Duration = Duration::from_hours(1);

/// Resource and coordination bounds for refresh-control access.
#[derive(Clone, Copy, Debug)]
pub struct EmbeddingRefreshConfigLimits {
    /// Maximum canonical JSON bytes.
    pub metadata_bytes: usize,
    /// Maximum distinct compatibility-lineage records.
    pub entries: usize,
    /// Refresh-control writer lock and cleanup bounds.
    pub coordination: SearchCoordinationLimits,
}

impl Default for EmbeddingRefreshConfigLimits {
    fn default() -> Self {
        Self {
            metadata_bytes: MAX_EMBEDDING_REFRESH_CONFIG_BYTES,
            entries: MAX_EMBEDDING_REFRESH_CONFIG_ENTRIES,
            coordination: SearchCoordinationLimits::default(),
        }
    }
}

/// Project-wide refresh defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingRefreshProjectPolicy {
    /// Queue relevant mutations while this process is open.
    pub proactive: bool,
    /// Delay after the newest relevant mutation before work is ready.
    pub debounce: Duration,
    /// Maximum producer jobs active for this project.
    pub max_concurrent_jobs: usize,
}

impl Default for EmbeddingRefreshProjectPolicy {
    fn default() -> Self {
        Self {
            proactive: true,
            debounce: DEFAULT_EMBEDDING_REFRESH_DEBOUNCE,
            max_concurrent_jobs: MAX_EMBEDDING_REFRESH_JOBS,
        }
    }
}

/// Optional per-lineage overrides of project refresh defaults.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EmbeddingRefreshSpacePolicy {
    /// Override proactive enablement for this lineage.
    pub proactive: Option<bool>,
    /// Override mutation debounce for this lineage.
    pub debounce: Option<Duration>,
}

/// Fully resolved policy for one compatibility lineage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedEmbeddingRefreshPolicy {
    /// Effective proactive enablement.
    pub proactive: bool,
    /// Effective mutation debounce.
    pub debounce: Duration,
    /// Project-wide producer concurrency bound.
    pub max_concurrent_jobs: usize,
}

/// Stable, content-free terminal refresh failure classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingRefreshFailureClass {
    /// Provider or callback work failed.
    Provider,
    /// Caller input or producer output was invalid.
    Validation,
    /// A named time, memory, disk, queue, token, or cost bound was exhausted.
    ResourceExhausted,
    /// Durable storage or publication failed.
    Storage,
    /// The graph changed across both bounded attempts.
    ConcurrentMutation,
    /// Space identity or producer contract was incompatible.
    Incompatible,
    /// Primary or derived durable bytes were corrupt.
    Corrupt,
    /// The producer is unavailable in this process.
    Unavailable,
}

/// Stable terminal interpretation of one refresh attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingRefreshOutcomeStatus {
    /// One complete generation published or was content-idempotently reused.
    Succeeded,
    /// Cooperative cancellation stopped private work.
    Cancelled,
    /// Refresh failed without publishing partial state.
    Failed(EmbeddingRefreshFailureClass),
}

/// Content-free durable outcome for one compatibility lineage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingRefreshOutcomeRecord {
    /// Terminal completion classification.
    pub status: EmbeddingRefreshOutcomeStatus,
    /// Committed graph generation targeted by the attempt.
    pub graph_generation: u64,
    /// Exact source fingerprint targeted by the attempt.
    pub source_fingerprint: EmbeddingSourceFingerprint,
    /// Terminal UTC timestamp in microseconds since Unix epoch.
    pub completed_at_micros: i64,
}

/// Content-free terminal outcome fields captured before durable lock acquisition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingRefreshOutcomeAttempt {
    /// Terminal completion classification.
    pub status: EmbeddingRefreshOutcomeStatus,
    /// Committed graph generation targeted by the attempt.
    pub graph_generation: u64,
    /// Exact source fingerprint targeted by the attempt.
    pub source_fingerprint: EmbeddingSourceFingerprint,
}

/// One deterministic compatibility-lineage refresh record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingRefreshSpaceState {
    /// Exact embedding compatibility lineage.
    pub compatibility_id: EmbeddingCompatibilityId,
    /// Optional per-lineage override.
    pub policy: Option<EmbeddingRefreshSpacePolicy>,
    /// Optional latest content-free terminal outcome.
    pub last_outcome: Option<EmbeddingRefreshOutcomeRecord>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct StoredSpaceState {
    policy: Option<EmbeddingRefreshSpacePolicy>,
    last_outcome: Option<EmbeddingRefreshOutcomeRecord>,
}

/// Fully validated durable refresh control state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EmbeddingRefreshConfig {
    project: EmbeddingRefreshProjectPolicy,
    spaces: BTreeMap<EmbeddingCompatibilityId, StoredSpaceState>,
}

impl EmbeddingRefreshConfig {
    /// Project-wide defaults.
    #[must_use]
    pub const fn project_policy(&self) -> EmbeddingRefreshProjectPolicy {
        self.project
    }

    /// Number of lineages with an override or terminal outcome.
    #[must_use]
    pub fn len(&self) -> usize {
        self.spaces.len()
    }

    /// Whether no per-lineage state is retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.spaces.is_empty()
    }

    /// Deterministic compatibility-identity-ordered lineage state.
    #[must_use]
    pub fn spaces(&self) -> Vec<EmbeddingRefreshSpaceState> {
        self.spaces
            .iter()
            .map(|(compatibility_id, state)| EmbeddingRefreshSpaceState {
                compatibility_id: *compatibility_id,
                policy: state.policy,
                last_outcome: state.last_outcome,
            })
            .collect()
    }

    /// Resolve one lineage's project defaults and optional overrides.
    #[must_use]
    pub fn resolved_policy(
        &self,
        compatibility_id: EmbeddingCompatibilityId,
    ) -> ResolvedEmbeddingRefreshPolicy {
        let override_policy = self
            .spaces
            .get(&compatibility_id)
            .and_then(|state| state.policy);
        ResolvedEmbeddingRefreshPolicy {
            proactive: override_policy
                .and_then(|policy| policy.proactive)
                .unwrap_or(self.project.proactive),
            debounce: override_policy
                .and_then(|policy| policy.debounce)
                .unwrap_or(self.project.debounce),
            max_concurrent_jobs: self.project.max_concurrent_jobs,
        }
    }

    fn to_canonical_json(
        &self,
        limits: EmbeddingRefreshConfigLimits,
    ) -> Result<Vec<u8>, SearchArtifactError> {
        validate_limits(limits)?;
        if self.spaces.len() > limits.entries {
            return Err(exhausted(
                "embedding_refresh_config_entries",
                limits.entries,
            ));
        }
        validate_project_policy(self.project)?;
        let spaces = self
            .spaces
            .iter()
            .map(|(compatibility_id, state)| wire_space(*compatibility_id, *state))
            .collect::<Result<Vec<_>, _>>()?;
        let material = WireMaterial {
            config_version: EMBEDDING_REFRESH_CONFIG_VERSION,
            project: wire_project(self.project)?,
            spaces,
        };
        let material_bytes = serde_json::to_vec(&material)
            .map_err(|error| invalid("embedding refresh config", error.to_string()))?;
        let bytes = serde_json::to_vec(&WireDocument {
            config_version: material.config_version,
            project: material.project,
            spaces: material.spaces,
            checksum: checksum(&material_bytes),
        })
        .map_err(|error| invalid("embedding refresh config", error.to_string()))?;
        if bytes.len() > limits.metadata_bytes {
            return Err(exhausted(
                "embedding_refresh_config_bytes",
                limits.metadata_bytes,
            ));
        }
        Ok(bytes)
    }
}

/// One explicit mutation of durable refresh control state.
#[derive(Clone, Copy, Debug)]
pub enum EmbeddingRefreshConfigUpdate {
    /// Replace project-wide defaults.
    SetProjectPolicy(EmbeddingRefreshProjectPolicy),
    /// Set or clear one lineage override. Clearing retains its last outcome.
    SetSpacePolicy {
        /// Exact compatibility lineage.
        compatibility_id: EmbeddingCompatibilityId,
        /// Override to persist, or `None` to inherit project defaults.
        policy: Option<EmbeddingRefreshSpacePolicy>,
    },
    /// Record one terminal content-free outcome.
    RecordOutcome {
        /// Exact compatibility lineage.
        compatibility_id: EmbeddingCompatibilityId,
        /// Newest terminal outcome.
        outcome: EmbeddingRefreshOutcomeRecord,
    },
    /// Remove all policy and outcome state for one lineage.
    RemoveSpace {
        /// Exact compatibility lineage.
        compatibility_id: EmbeddingCompatibilityId,
    },
}

/// Read and validate durable embedding refresh controls.
///
/// A missing file returns pinned defaults without creating directories.
///
/// # Errors
/// Returns structured cancellation, limit, corruption, incompatibility, or I/O errors.
pub fn read_embedding_refresh_config<C>(
    project_dir: &Path,
    limits: EmbeddingRefreshConfigLimits,
    mut checkpoint: C,
) -> Result<EmbeddingRefreshConfig, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    validate_limits(limits)?;
    checkpoint()?;
    read_config_file(&config_path(project_dir), limits)
}

/// Apply one refresh-control mutation under a bounded cross-process writer lock.
///
/// Exact-idempotent updates do not rewrite durable bytes. Publication uses a
/// synchronized sibling temporary file and atomic replacement.
///
/// # Errors
/// Returns structured validation, cancellation, lock, limit, corruption, or I/O errors.
pub fn update_embedding_refresh_config<C>(
    project_dir: &Path,
    update: EmbeddingRefreshConfigUpdate,
    limits: EmbeddingRefreshConfigLimits,
    checkpoint: C,
) -> Result<EmbeddingRefreshConfig, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    mutate_embedding_refresh_config(project_dir, limits, checkpoint, |config, limits| {
        apply_update(config, update, limits)
    })
}

/// Record one terminal outcome with monotonic time assigned under the writer lock.
///
/// The latest durable same-lineage outcome is read only after cross-process lock
/// acquisition. The new completion time is the current UTC microsecond or one
/// microsecond after the prior outcome, whichever is greater.
///
/// # Errors
/// Returns structured validation, cancellation, lock, clock-overflow, limit,
/// corruption, or I/O errors. Failure leaves durable bytes unchanged.
pub fn record_embedding_refresh_outcome<C>(
    project_dir: &Path,
    compatibility_id: EmbeddingCompatibilityId,
    attempt: EmbeddingRefreshOutcomeAttempt,
    limits: EmbeddingRefreshConfigLimits,
    checkpoint: C,
) -> Result<EmbeddingRefreshConfig, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    record_embedding_refresh_outcome_with_clock(
        project_dir,
        compatibility_id,
        attempt,
        limits,
        transaction_time_micros,
        checkpoint,
    )
}

fn record_embedding_refresh_outcome_with_clock<C, N>(
    project_dir: &Path,
    compatibility_id: EmbeddingCompatibilityId,
    attempt: EmbeddingRefreshOutcomeAttempt,
    limits: EmbeddingRefreshConfigLimits,
    now: N,
    checkpoint: C,
) -> Result<EmbeddingRefreshConfig, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
    N: FnOnce() -> i64,
{
    mutate_embedding_refresh_config(project_dir, limits, checkpoint, move |config, limits| {
        let prior_time = config
            .spaces
            .get(&compatibility_id)
            .and_then(|state| state.last_outcome)
            .map_or(Ok(0), |outcome| {
                outcome.completed_at_micros.checked_add(1).ok_or(
                    SearchArtifactError::ResourceExhausted {
                        resource: "embedding_refresh_outcome_timestamp",
                        limit: u64::MAX,
                    },
                )
            })?;
        apply_update(
            config,
            EmbeddingRefreshConfigUpdate::RecordOutcome {
                compatibility_id,
                outcome: EmbeddingRefreshOutcomeRecord {
                    status: attempt.status,
                    graph_generation: attempt.graph_generation,
                    source_fingerprint: attempt.source_fingerprint,
                    completed_at_micros: now().max(prior_time),
                },
            },
            limits,
        )
    })
}

fn mutate_embedding_refresh_config<C, M>(
    project_dir: &Path,
    limits: EmbeddingRefreshConfigLimits,
    mut checkpoint: C,
    mutate: M,
) -> Result<EmbeddingRefreshConfig, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
    M: FnOnce(
        &mut EmbeddingRefreshConfig,
        EmbeddingRefreshConfigLimits,
    ) -> Result<bool, SearchArtifactError>,
{
    validate_limits(limits)?;
    checkpoint()?;
    let embeddings = project_dir.join(EMBEDDINGS_DIR);
    ensure_owned_directory(&embeddings)?;
    let _lock = ConfigWriterLock::acquire(&embeddings, limits.coordination, &mut checkpoint)?;
    checkpoint()?;
    cleanup_abandoned_temps(&embeddings, limits.coordination.cleanup_entries)?;
    let path = embeddings.join(CONFIG_FILE);
    let mut config = read_config_file(&path, limits)?;
    if mutate(&mut config, limits)? {
        checkpoint()?;
        let bytes = config.to_canonical_json(limits)?;
        checkpoint()?;
        persist_synced_file(&path, &bytes)?;
    }
    Ok(config)
}

fn transaction_time_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
        })
}

fn apply_update(
    config: &mut EmbeddingRefreshConfig,
    update: EmbeddingRefreshConfigUpdate,
    limits: EmbeddingRefreshConfigLimits,
) -> Result<bool, SearchArtifactError> {
    match update {
        EmbeddingRefreshConfigUpdate::SetProjectPolicy(policy) => {
            validate_project_policy(policy)?;
            if config.project == policy {
                Ok(false)
            } else {
                config.project = policy;
                Ok(true)
            }
        }
        EmbeddingRefreshConfigUpdate::SetSpacePolicy {
            compatibility_id,
            policy,
        } => {
            if let Some(policy) = policy {
                validate_space_policy(policy)?;
            }
            if policy.is_none() && !config.spaces.contains_key(&compatibility_id) {
                return Ok(false);
            }
            ensure_entry_capacity(config, compatibility_id, limits)?;
            let current = config.spaces.entry(compatibility_id).or_default();
            if current.policy == policy {
                return Ok(false);
            }
            current.policy = policy;
            if current.policy.is_none() && current.last_outcome.is_none() {
                config.spaces.remove(&compatibility_id);
            }
            Ok(true)
        }
        EmbeddingRefreshConfigUpdate::RecordOutcome {
            compatibility_id,
            outcome,
        } => {
            validate_outcome(outcome)?;
            ensure_entry_capacity(config, compatibility_id, limits)?;
            let current = config.spaces.entry(compatibility_id).or_default();
            if current.last_outcome == Some(outcome) {
                return Ok(false);
            }
            if let Some(prior) = current.last_outcome {
                validate_outcome_progress(prior, outcome)?;
            }
            current.last_outcome = Some(outcome);
            Ok(true)
        }
        EmbeddingRefreshConfigUpdate::RemoveSpace { compatibility_id } => {
            Ok(config.spaces.remove(&compatibility_id).is_some())
        }
    }
}

fn ensure_entry_capacity(
    config: &EmbeddingRefreshConfig,
    compatibility_id: EmbeddingCompatibilityId,
    limits: EmbeddingRefreshConfigLimits,
) -> Result<(), SearchArtifactError> {
    if !config.spaces.contains_key(&compatibility_id) && config.spaces.len() >= limits.entries {
        Err(exhausted(
            "embedding_refresh_config_entries",
            limits.entries,
        ))
    } else {
        Ok(())
    }
}

fn validate_project_policy(
    policy: EmbeddingRefreshProjectPolicy,
) -> Result<(), SearchArtifactError> {
    validate_debounce(policy.debounce)?;
    if !(1..=MAX_EMBEDDING_REFRESH_JOBS).contains(&policy.max_concurrent_jobs) {
        return Err(invalid(
            "embedding refresh max_concurrent_jobs",
            "must be in 1..=2",
        ));
    }
    Ok(())
}

fn validate_space_policy(policy: EmbeddingRefreshSpacePolicy) -> Result<(), SearchArtifactError> {
    if policy.proactive.is_none() && policy.debounce.is_none() {
        return Err(invalid(
            "embedding refresh space policy",
            "must override proactive or debounce",
        ));
    }
    if let Some(debounce) = policy.debounce {
        validate_debounce(debounce)?;
    }
    Ok(())
}

fn validate_debounce(debounce: Duration) -> Result<(), SearchArtifactError> {
    if debounce.is_zero() || debounce > MAX_DEBOUNCE {
        Err(invalid(
            "embedding refresh debounce",
            "must be in 1 millisecond..=1 hour",
        ))
    } else if !debounce.subsec_nanos().is_multiple_of(1_000_000) {
        Err(invalid(
            "embedding refresh debounce",
            "must use whole milliseconds",
        ))
    } else {
        Ok(())
    }
}

fn validate_outcome(outcome: EmbeddingRefreshOutcomeRecord) -> Result<(), SearchArtifactError> {
    if outcome.completed_at_micros < 0 {
        Err(invalid(
            "embedding refresh completed_at_micros",
            "must be non-negative",
        ))
    } else {
        Ok(())
    }
}

fn validate_outcome_progress(
    prior: EmbeddingRefreshOutcomeRecord,
    next: EmbeddingRefreshOutcomeRecord,
) -> Result<(), SearchArtifactError> {
    if next.completed_at_micros < prior.completed_at_micros {
        return Err(invalid(
            "embedding refresh outcome",
            "completion timestamp regressed",
        ));
    }
    match next.graph_generation.cmp(&prior.graph_generation) {
        std::cmp::Ordering::Less => Err(invalid(
            "embedding refresh outcome",
            "graph generation regressed",
        )),
        std::cmp::Ordering::Equal if next.source_fingerprint != prior.source_fingerprint => {
            Err(invalid(
                "embedding refresh outcome",
                "source fingerprint conflicts at the same graph generation",
            ))
        }
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => Ok(()),
    }
}

fn read_config_file(
    path: &Path,
    limits: EmbeddingRefreshConfigLimits,
) -> Result<EmbeddingRefreshConfig, SearchArtifactError> {
    if !path_exists(path)? {
        return Ok(EmbeddingRefreshConfig::default());
    }
    ensure_regular_file(path)?;
    let metadata = std::fs::metadata(path)
        .map_err(|source| io("inspect embedding refresh config", path, source))?;
    if metadata.len() > limits.metadata_bytes as u64 {
        return Err(exhausted(
            "embedding_refresh_config_bytes",
            limits.metadata_bytes,
        ));
    }
    let bytes =
        std::fs::read(path).map_err(|source| io("read embedding refresh config", path, source))?;
    let raw: RawDocument =
        serde_json::from_slice(&bytes).map_err(|error| corrupt(path, error.to_string()))?;
    if raw.config_version != u64::from(EMBEDDING_REFRESH_CONFIG_VERSION) {
        return Err(SearchArtifactError::IncompatibleManifest {
            path: path.to_path_buf(),
            found: raw.config_version,
            supported: EMBEDDING_REFRESH_CONFIG_VERSION,
        });
    }
    if raw.spaces.len() > limits.entries {
        return Err(exhausted(
            "embedding_refresh_config_entries",
            limits.entries,
        ));
    }
    let project = parse_project(&raw.project).map_err(|error| corrupt(path, error.to_string()))?;
    let mut spaces = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for raw_space in raw.spaces {
        let (compatibility_id, state) =
            parse_space(raw_space).map_err(|error| corrupt(path, error.to_string()))?;
        if !seen.insert(compatibility_id) {
            return Err(corrupt(path, "duplicate refresh compatibility identity"));
        }
        spaces.insert(compatibility_id, state);
    }
    let config = EmbeddingRefreshConfig { project, spaces };
    let material_bytes = material_bytes(&config)?;
    if raw.checksum != checksum(&material_bytes) {
        return Err(corrupt(path, "embedding refresh config checksum mismatch"));
    }
    let canonical = config
        .to_canonical_json(limits)
        .map_err(|error| corrupt(path, error.to_string()))?;
    if canonical != bytes {
        return Err(corrupt(
            path,
            "embedding refresh config bytes are not exact canonical JSON",
        ));
    }
    Ok(config)
}

fn material_bytes(config: &EmbeddingRefreshConfig) -> Result<Vec<u8>, SearchArtifactError> {
    let spaces = config
        .spaces
        .iter()
        .map(|(compatibility_id, state)| wire_space(*compatibility_id, *state))
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_vec(&WireMaterial {
        config_version: EMBEDDING_REFRESH_CONFIG_VERSION,
        project: wire_project(config.project)?,
        spaces,
    })
    .map_err(|error| invalid("embedding refresh config", error.to_string()))
}

#[derive(Clone, Serialize)]
struct WireProject {
    proactive: bool,
    debounce_millis: u64,
    max_concurrent_jobs: usize,
}

#[derive(Clone, Serialize)]
struct WireSpace {
    compatibility_id: String,
    policy: Option<WireSpacePolicy>,
    last_outcome: Option<WireOutcome>,
}

#[derive(Clone, Serialize)]
struct WireSpacePolicy {
    proactive: Option<bool>,
    debounce_millis: Option<u64>,
}

#[derive(Clone, Serialize)]
struct WireOutcome {
    status: &'static str,
    failure_class: Option<&'static str>,
    graph_generation: u64,
    source_fingerprint: String,
    completed_at_micros: i64,
}

#[derive(Serialize)]
struct WireMaterial {
    config_version: u32,
    project: WireProject,
    spaces: Vec<WireSpace>,
}

#[derive(Serialize)]
struct WireDocument {
    config_version: u32,
    project: WireProject,
    spaces: Vec<WireSpace>,
    checksum: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDocument {
    config_version: u64,
    project: RawProject,
    spaces: Vec<RawSpace>,
    checksum: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProject {
    proactive: bool,
    debounce_millis: u64,
    max_concurrent_jobs: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSpace {
    compatibility_id: String,
    policy: Option<RawSpacePolicy>,
    last_outcome: Option<RawOutcome>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSpacePolicy {
    proactive: Option<bool>,
    debounce_millis: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOutcome {
    status: String,
    failure_class: Option<String>,
    graph_generation: u64,
    source_fingerprint: String,
    completed_at_micros: i64,
}

fn wire_project(policy: EmbeddingRefreshProjectPolicy) -> Result<WireProject, SearchArtifactError> {
    validate_project_policy(policy)?;
    Ok(WireProject {
        proactive: policy.proactive,
        debounce_millis: duration_millis(policy.debounce)?,
        max_concurrent_jobs: policy.max_concurrent_jobs,
    })
}

fn wire_space(
    compatibility_id: EmbeddingCompatibilityId,
    state: StoredSpaceState,
) -> Result<WireSpace, SearchArtifactError> {
    if state.policy.is_none() && state.last_outcome.is_none() {
        return Err(invalid(
            "embedding refresh space state",
            "must retain a policy or outcome",
        ));
    }
    let policy = state
        .policy
        .map(|policy| {
            validate_space_policy(policy)?;
            Ok(WireSpacePolicy {
                proactive: policy.proactive,
                debounce_millis: policy.debounce.map(duration_millis).transpose()?,
            })
        })
        .transpose()?;
    let last_outcome = state
        .last_outcome
        .map(|outcome| {
            validate_outcome(outcome)?;
            let (status, failure_class) = outcome_tokens(outcome.status);
            Ok(WireOutcome {
                status,
                failure_class,
                graph_generation: outcome.graph_generation,
                source_fingerprint: outcome.source_fingerprint.to_hex(),
                completed_at_micros: outcome.completed_at_micros,
            })
        })
        .transpose()?;
    Ok(WireSpace {
        compatibility_id: compatibility_id.to_hex(),
        policy,
        last_outcome,
    })
}

fn parse_project(raw: &RawProject) -> Result<EmbeddingRefreshProjectPolicy, SearchArtifactError> {
    let policy = EmbeddingRefreshProjectPolicy {
        proactive: raw.proactive,
        debounce: Duration::from_millis(raw.debounce_millis),
        max_concurrent_jobs: raw.max_concurrent_jobs,
    };
    validate_project_policy(policy)?;
    Ok(policy)
}

fn parse_space(
    raw: RawSpace,
) -> Result<(EmbeddingCompatibilityId, StoredSpaceState), SearchArtifactError> {
    let compatibility_id = EmbeddingCompatibilityId::from_hex(&raw.compatibility_id)?;
    let policy = raw
        .policy
        .map(|raw| {
            let policy = EmbeddingRefreshSpacePolicy {
                proactive: raw.proactive,
                debounce: raw.debounce_millis.map(Duration::from_millis),
            };
            validate_space_policy(policy)?;
            Ok(policy)
        })
        .transpose()?;
    let last_outcome = raw.last_outcome.as_ref().map(parse_outcome).transpose()?;
    if policy.is_none() && last_outcome.is_none() {
        return Err(invalid(
            "embedding refresh space state",
            "must retain a policy or outcome",
        ));
    }
    Ok((
        compatibility_id,
        StoredSpaceState {
            policy,
            last_outcome,
        },
    ))
}

fn parse_outcome(raw: &RawOutcome) -> Result<EmbeddingRefreshOutcomeRecord, SearchArtifactError> {
    let status = match (raw.status.as_str(), raw.failure_class.as_deref()) {
        ("succeeded", None) => EmbeddingRefreshOutcomeStatus::Succeeded,
        ("cancelled", None) => EmbeddingRefreshOutcomeStatus::Cancelled,
        ("failed", Some(class)) => {
            EmbeddingRefreshOutcomeStatus::Failed(parse_failure_class(class)?)
        }
        _ => {
            return Err(invalid(
                "embedding refresh outcome status",
                "status and failure_class are inconsistent",
            ));
        }
    };
    let outcome = EmbeddingRefreshOutcomeRecord {
        status,
        graph_generation: raw.graph_generation,
        source_fingerprint: EmbeddingSourceFingerprint::from_hex(&raw.source_fingerprint)?,
        completed_at_micros: raw.completed_at_micros,
    };
    validate_outcome(outcome)?;
    Ok(outcome)
}

fn outcome_tokens(status: EmbeddingRefreshOutcomeStatus) -> (&'static str, Option<&'static str>) {
    match status {
        EmbeddingRefreshOutcomeStatus::Succeeded => ("succeeded", None),
        EmbeddingRefreshOutcomeStatus::Cancelled => ("cancelled", None),
        EmbeddingRefreshOutcomeStatus::Failed(class) => ("failed", Some(failure_token(class))),
    }
}

fn failure_token(class: EmbeddingRefreshFailureClass) -> &'static str {
    match class {
        EmbeddingRefreshFailureClass::Provider => "provider",
        EmbeddingRefreshFailureClass::Validation => "validation",
        EmbeddingRefreshFailureClass::ResourceExhausted => "resource_exhausted",
        EmbeddingRefreshFailureClass::Storage => "storage",
        EmbeddingRefreshFailureClass::ConcurrentMutation => "concurrent_mutation",
        EmbeddingRefreshFailureClass::Incompatible => "incompatible",
        EmbeddingRefreshFailureClass::Corrupt => "corrupt",
        EmbeddingRefreshFailureClass::Unavailable => "unavailable",
    }
}

fn parse_failure_class(value: &str) -> Result<EmbeddingRefreshFailureClass, SearchArtifactError> {
    match value {
        "provider" => Ok(EmbeddingRefreshFailureClass::Provider),
        "validation" => Ok(EmbeddingRefreshFailureClass::Validation),
        "resource_exhausted" => Ok(EmbeddingRefreshFailureClass::ResourceExhausted),
        "storage" => Ok(EmbeddingRefreshFailureClass::Storage),
        "concurrent_mutation" => Ok(EmbeddingRefreshFailureClass::ConcurrentMutation),
        "incompatible" => Ok(EmbeddingRefreshFailureClass::Incompatible),
        "corrupt" => Ok(EmbeddingRefreshFailureClass::Corrupt),
        "unavailable" => Ok(EmbeddingRefreshFailureClass::Unavailable),
        _ => Err(invalid(
            "embedding refresh failure_class",
            "is not a supported token",
        )),
    }
}

fn duration_millis(duration: Duration) -> Result<u64, SearchArtifactError> {
    validate_debounce(duration)?;
    u64::try_from(duration.as_millis())
        .map_err(|_| exhausted("embedding_refresh_debounce_millis", usize::MAX))
}

fn checksum(material: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(CHECKSUM_DOMAIN);
    hasher.update(material);
    format!("{:x}", hasher.finalize())
}

struct ConfigWriterLock {
    file: File,
}

impl ConfigWriterLock {
    fn acquire<C>(
        embeddings: &Path,
        limits: SearchCoordinationLimits,
        checkpoint: &mut C,
    ) -> Result<Self, SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        let path = embeddings.join(CONFIG_LOCK);
        if path_exists(&path)? {
            ensure_regular_file(&path)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| SearchArtifactError::Lock {
                path: path.clone(),
                reason: source.to_string(),
            })?;
        let started = Instant::now();
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(Self { file }),
                Err(std::fs::TryLockError::WouldBlock) => {
                    checkpoint()?;
                    if started.elapsed() >= limits.lock_timeout {
                        return Err(SearchArtifactError::Lock {
                            path,
                            reason: format!(
                                "timed out after {} ms",
                                limits.lock_timeout.as_millis()
                            ),
                        });
                    }
                    std::thread::sleep(limits.lock_poll_interval);
                }
                Err(std::fs::TryLockError::Error(source)) => {
                    return Err(SearchArtifactError::Lock {
                        path,
                        reason: source.to_string(),
                    });
                }
            }
        }
    }
}

impl Drop for ConfigWriterLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn cleanup_abandoned_temps(
    embeddings: &Path,
    max_entries: usize,
) -> Result<usize, SearchArtifactError> {
    if max_entries == 0 {
        return Err(invalid(
            "embedding refresh cleanup_entries",
            "must be non-zero",
        ));
    }
    let mut inspected = 0_usize;
    let mut removed = 0_usize;
    for entry in std::fs::read_dir(embeddings)
        .map_err(|source| io("read embedding refresh directory", embeddings, source))?
    {
        inspected = inspected
            .checked_add(1)
            .ok_or_else(|| exhausted("embedding_refresh_cleanup_entries", max_entries))?;
        if inspected > max_entries {
            return Err(exhausted("embedding_refresh_cleanup_entries", max_entries));
        }
        let entry =
            entry.map_err(|source| io("read embedding refresh entry", embeddings, source))?;
        let file_type = entry
            .file_type()
            .map_err(|source| io("inspect embedding refresh entry", &entry.path(), source))?;
        if file_type.is_file() && is_refresh_temp_name(&entry.file_name()) {
            std::fs::remove_file(entry.path())
                .map_err(|source| io("remove embedding refresh temp", &entry.path(), source))?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn is_refresh_temp_name(name: &std::ffi::OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    let Some(random) = name
        .strip_prefix(".refresh.json.")
        .and_then(|name| name.strip_suffix(".tmp"))
    else {
        return false;
    };
    !random.is_empty() && random.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn validate_limits(limits: EmbeddingRefreshConfigLimits) -> Result<(), SearchArtifactError> {
    if limits.metadata_bytes == 0 || limits.entries == 0 || limits.coordination.cleanup_entries == 0
    {
        Err(invalid(
            "embedding refresh config limits",
            "must be non-zero",
        ))
    } else {
        Ok(())
    }
}

fn config_path(project_dir: &Path) -> PathBuf {
    project_dir.join(EMBEDDINGS_DIR).join(CONFIG_FILE)
}

fn ensure_owned_directory(path: &Path) -> Result<(), SearchArtifactError> {
    if path_exists(path)? {
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|source| io("inspect embedding refresh directory", path, source))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(corrupt(path, "expected an owned directory"));
        }
        return Ok(());
    }
    std::fs::create_dir_all(path)
        .map_err(|source| io("create embedding refresh directory", path, source))?;
    sync_directory(path.parent().unwrap_or(path))
}

fn ensure_regular_file(path: &Path) -> Result<(), SearchArtifactError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|source| io("inspect embedding refresh config", path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(corrupt(path, "expected a regular file"));
    }
    Ok(())
}

fn persist_synced_file(path: &Path, bytes: &[u8]) -> Result<(), SearchArtifactError> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("embedding refresh config", "path has no parent"))?;
    let mut temp = tempfile::Builder::new()
        .prefix(".refresh.json.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|source| io("create embedding refresh temp", path, source))?;
    temp.write_all(bytes)
        .map_err(|source| io("write embedding refresh temp", path, source))?;
    temp.as_file()
        .sync_all()
        .map_err(|source| io("sync embedding refresh temp", path, source))?;
    temp.persist(path)
        .map_err(|error| io("publish embedding refresh config", path, error.error))?;
    sync_directory(parent)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), SearchArtifactError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io("sync embedding refresh directory", path, source))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), SearchArtifactError> {
    Ok(())
}

fn path_exists(path: &Path) -> Result<bool, SearchArtifactError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io("inspect embedding refresh path", path, source)),
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn corrupt(path: &Path, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::CorruptManifest {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

fn exhausted(resource: &'static str, limit: usize) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted {
        resource,
        limit: limit as u64,
    }
}

fn io(operation: &'static str, path: &Path, source: std::io::Error) -> SearchArtifactError {
    SearchArtifactError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::{Arc, Barrier};

    use super::*;

    fn id(value: u8) -> EmbeddingCompatibilityId {
        EmbeddingCompatibilityId::from_hex(&format!("{value:02x}").repeat(32)).unwrap()
    }

    fn fingerprint(value: u8) -> EmbeddingSourceFingerprint {
        EmbeddingSourceFingerprint::from_hex(&format!("{value:02x}").repeat(32)).unwrap()
    }

    fn read(project: &Path) -> EmbeddingRefreshConfig {
        read_embedding_refresh_config(project, EmbeddingRefreshConfigLimits::default(), || Ok(()))
            .unwrap()
    }

    fn update(project: &Path, mutation: EmbeddingRefreshConfigUpdate) -> EmbeddingRefreshConfig {
        update_embedding_refresh_config(
            project,
            mutation,
            EmbeddingRefreshConfigLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn attempt(status: EmbeddingRefreshOutcomeStatus) -> EmbeddingRefreshOutcomeAttempt {
        EmbeddingRefreshOutcomeAttempt {
            status,
            graph_generation: 1,
            source_fingerprint: fingerprint(1),
        }
    }

    #[test]
    fn locked_outcome_stamping_is_monotonic_under_same_lineage_contention() {
        let project = tempfile::tempdir().unwrap();
        let project = Arc::new(project.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(2));
        let mut threads = Vec::new();
        for status in [
            EmbeddingRefreshOutcomeStatus::Succeeded,
            EmbeddingRefreshOutcomeStatus::Cancelled,
        ] {
            let project = Arc::clone(&project);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                record_embedding_refresh_outcome_with_clock(
                    &project,
                    id(1),
                    attempt(status),
                    EmbeddingRefreshConfigLimits::default(),
                    || 5,
                    || Ok(()),
                )
                .unwrap()
                .spaces()
                .into_iter()
                .find(|state| state.compatibility_id == id(1))
                .unwrap()
                .last_outcome
                .unwrap()
                .completed_at_micros
            }));
        }
        let mut completed = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        completed.sort_unstable();
        assert_eq!(completed, [5, 6]);
        assert_eq!(
            read(&project)
                .spaces()
                .into_iter()
                .find(|state| state.compatibility_id == id(1))
                .unwrap()
                .last_outcome
                .unwrap()
                .completed_at_micros,
            6
        );
    }

    #[test]
    fn locked_outcome_timestamp_overflow_is_structured_and_atomic() {
        let project = tempfile::tempdir().unwrap();
        update(
            project.path(),
            EmbeddingRefreshConfigUpdate::RecordOutcome {
                compatibility_id: id(1),
                outcome: EmbeddingRefreshOutcomeRecord {
                    completed_at_micros: i64::MAX,
                    ..outcome(1, 1, 1)
                },
            },
        );
        let stable = std::fs::read(config_path(project.path())).unwrap();
        assert!(matches!(
            record_embedding_refresh_outcome_with_clock(
                project.path(),
                id(1),
                attempt(EmbeddingRefreshOutcomeStatus::Succeeded),
                EmbeddingRefreshConfigLimits::default(),
                || 1,
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_refresh_outcome_timestamp",
                ..
            })
        ));
        assert_eq!(std::fs::read(config_path(project.path())).unwrap(), stable);
    }

    fn outcome(
        generation: u64,
        fingerprint_value: u8,
        completed_at_micros: i64,
    ) -> EmbeddingRefreshOutcomeRecord {
        EmbeddingRefreshOutcomeRecord {
            status: EmbeddingRefreshOutcomeStatus::Succeeded,
            graph_generation: generation,
            source_fingerprint: fingerprint(fingerprint_value),
            completed_at_micros,
        }
    }

    #[test]
    fn missing_config_returns_defaults_without_creating_files() {
        let project = tempfile::tempdir().unwrap();
        let config = read(project.path());
        assert_eq!(
            config.project_policy(),
            EmbeddingRefreshProjectPolicy::default()
        );
        assert!(config.is_empty());
        assert!(!project.path().join(EMBEDDINGS_DIR).exists());
    }

    #[test]
    fn project_space_and_outcome_state_round_trip_canonically() {
        let project = tempfile::tempdir().unwrap();
        let compatibility_id = id(2);
        update(
            project.path(),
            EmbeddingRefreshConfigUpdate::SetProjectPolicy(EmbeddingRefreshProjectPolicy {
                proactive: false,
                debounce: Duration::from_millis(750),
                max_concurrent_jobs: 1,
            }),
        );
        update(
            project.path(),
            EmbeddingRefreshConfigUpdate::SetSpacePolicy {
                compatibility_id,
                policy: Some(EmbeddingRefreshSpacePolicy {
                    proactive: Some(true),
                    debounce: Some(Duration::from_secs(2)),
                }),
            },
        );
        let config = update(
            project.path(),
            EmbeddingRefreshConfigUpdate::RecordOutcome {
                compatibility_id,
                outcome: EmbeddingRefreshOutcomeRecord {
                    status: EmbeddingRefreshOutcomeStatus::Failed(
                        EmbeddingRefreshFailureClass::Provider,
                    ),
                    graph_generation: 9,
                    source_fingerprint: fingerprint(3),
                    completed_at_micros: 10,
                },
            },
        );
        assert_eq!(config, read(project.path()));
        assert_eq!(
            config.resolved_policy(compatibility_id),
            ResolvedEmbeddingRefreshPolicy {
                proactive: true,
                debounce: Duration::from_secs(2),
                max_concurrent_jobs: 1,
            }
        );
        let state = config.spaces()[0];
        assert_eq!(state.compatibility_id, compatibility_id);
        assert!(matches!(
            state.last_outcome.unwrap().status,
            EmbeddingRefreshOutcomeStatus::Failed(EmbeddingRefreshFailureClass::Provider)
        ));

        let path = config_path(project.path());
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(
            bytes,
            config
                .to_canonical_json(EmbeddingRefreshConfigLimits::default())
                .unwrap()
        );
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("\"checksum\""));
        for forbidden in [
            "alias",
            "credential",
            "payload",
            "vector",
            "source_text",
            "confidence",
        ] {
            assert!(!text.contains(forbidden));
        }
    }

    #[test]
    fn idempotence_removal_limits_and_non_monotonic_outcomes_are_structured() {
        let project = tempfile::tempdir().unwrap();
        let compatibility_id = id(1);
        update(
            project.path(),
            EmbeddingRefreshConfigUpdate::RecordOutcome {
                compatibility_id,
                outcome: outcome(2, 2, 20),
            },
        );
        let path = config_path(project.path());
        let stable = std::fs::read(&path).unwrap();
        update(
            project.path(),
            EmbeddingRefreshConfigUpdate::RecordOutcome {
                compatibility_id,
                outcome: outcome(2, 2, 20),
            },
        );
        assert_eq!(std::fs::read(&path).unwrap(), stable);

        for invalid_outcome in [outcome(1, 1, 21), outcome(2, 3, 21), outcome(3, 3, 19)] {
            assert!(matches!(
                update_embedding_refresh_config(
                    project.path(),
                    EmbeddingRefreshConfigUpdate::RecordOutcome {
                        compatibility_id,
                        outcome: invalid_outcome,
                    },
                    EmbeddingRefreshConfigLimits::default(),
                    || Ok(())
                ),
                Err(SearchArtifactError::InvalidSelector { .. })
            ));
            assert_eq!(std::fs::read(&path).unwrap(), stable);
        }

        assert!(matches!(
            update_embedding_refresh_config(
                project.path(),
                EmbeddingRefreshConfigUpdate::SetProjectPolicy(EmbeddingRefreshProjectPolicy {
                    max_concurrent_jobs: 3,
                    ..EmbeddingRefreshProjectPolicy::default()
                }),
                EmbeddingRefreshConfigLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::InvalidSelector { .. })
        ));
        assert!(matches!(
            update_embedding_refresh_config(
                project.path(),
                EmbeddingRefreshConfigUpdate::SetSpacePolicy {
                    compatibility_id: id(9),
                    policy: Some(EmbeddingRefreshSpacePolicy::default()),
                },
                EmbeddingRefreshConfigLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::InvalidSelector { .. })
        ));

        let config = update(
            project.path(),
            EmbeddingRefreshConfigUpdate::RemoveSpace { compatibility_id },
        );
        assert!(config.is_empty());
    }

    #[test]
    fn corruption_version_cancellation_and_interrupted_temp_fail_closed() {
        let project = tempfile::tempdir().unwrap();
        let compatibility_id = id(1);
        update(
            project.path(),
            EmbeddingRefreshConfigUpdate::RecordOutcome {
                compatibility_id,
                outcome: outcome(1, 1, 1),
            },
        );
        let path = config_path(project.path());
        let stable = std::fs::read(&path).unwrap();
        let calls = Cell::new(0_u8);
        assert!(matches!(
            update_embedding_refresh_config(
                project.path(),
                EmbeddingRefreshConfigUpdate::SetProjectPolicy(EmbeddingRefreshProjectPolicy {
                    proactive: false,
                    ..EmbeddingRefreshProjectPolicy::default()
                }),
                EmbeddingRefreshConfigLimits::default(),
                || {
                    let next = calls.get() + 1;
                    calls.set(next);
                    if next >= 4 {
                        Err(SearchArtifactError::Cancelled)
                    } else {
                        Ok(())
                    }
                }
            ),
            Err(SearchArtifactError::Cancelled)
        ));
        assert_eq!(std::fs::read(&path).unwrap(), stable);

        let interrupted = project
            .path()
            .join(EMBEDDINGS_DIR)
            .join(".refresh.json.ABC123.tmp");
        std::fs::write(&interrupted, b"partial").unwrap();
        assert_eq!(read(project.path()).spaces().len(), 1);
        update(
            project.path(),
            EmbeddingRefreshConfigUpdate::SetProjectPolicy(EmbeddingRefreshProjectPolicy {
                proactive: false,
                ..EmbeddingRefreshProjectPolicy::default()
            }),
        );
        assert!(!interrupted.exists());

        let valid = std::fs::read_to_string(&path).unwrap();
        let damaged = valid.replacen("\"checksum\":\"", "\"checksum\":\"0", 1);
        std::fs::write(&path, damaged).unwrap();
        assert!(matches!(
            read_embedding_refresh_config(
                project.path(),
                EmbeddingRefreshConfigLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));

        std::fs::write(
            &path,
            b"{\"config_version\":1,\"project\":{\"proactive\":true,\"debounce_millis\":500,\"max_concurrent_jobs\":2},\"spaces\":[{\"compatibility_id\":\"invalid\",\"policy\":{\"proactive\":false,\"debounce_millis\":null},\"last_outcome\":null}],\"checksum\":\"bad\"}",
        )
        .unwrap();
        assert!(matches!(
            read_embedding_refresh_config(
                project.path(),
                EmbeddingRefreshConfigLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));

        std::fs::write(
            &path,
            b"{\"config_version\":2,\"project\":{\"proactive\":true,\"debounce_millis\":500,\"max_concurrent_jobs\":2},\"spaces\":[],\"checksum\":\"bad\"}",
        )
        .unwrap();
        assert!(matches!(
            read_embedding_refresh_config(
                project.path(),
                EmbeddingRefreshConfigLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::IncompatibleManifest { .. })
        ));
    }

    #[test]
    fn concurrent_writers_serialize_without_lost_lineages() {
        use std::sync::{Arc, Barrier};

        let project = tempfile::tempdir().unwrap();
        let project_path = Arc::new(project.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(3));
        let workers = [id(1), id(2)].map(|compatibility_id| {
            let project_path = Arc::clone(&project_path);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                update_embedding_refresh_config(
                    &project_path,
                    EmbeddingRefreshConfigUpdate::SetSpacePolicy {
                        compatibility_id,
                        policy: Some(EmbeddingRefreshSpacePolicy {
                            proactive: Some(false),
                            debounce: None,
                        }),
                    },
                    EmbeddingRefreshConfigLimits::default(),
                    || Ok(()),
                )
                .unwrap();
            })
        });
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        let config = read(project.path());
        assert_eq!(
            config
                .spaces()
                .iter()
                .map(|state| state.compatibility_id)
                .collect::<Vec<_>>(),
            [id(1), id(2)]
        );
    }

    #[test]
    fn entry_and_metadata_bounds_do_not_publish_partial_state() {
        let project = tempfile::tempdir().unwrap();
        let limits = EmbeddingRefreshConfigLimits {
            entries: 1,
            ..EmbeddingRefreshConfigLimits::default()
        };
        update_embedding_refresh_config(
            project.path(),
            EmbeddingRefreshConfigUpdate::RecordOutcome {
                compatibility_id: id(1),
                outcome: outcome(1, 1, 1),
            },
            limits,
            || Ok(()),
        )
        .unwrap();
        let path = config_path(project.path());
        let stable = std::fs::read(&path).unwrap();
        assert!(matches!(
            update_embedding_refresh_config(
                project.path(),
                EmbeddingRefreshConfigUpdate::RecordOutcome {
                    compatibility_id: id(2),
                    outcome: outcome(2, 2, 2),
                },
                limits,
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted { .. })
        ));
        assert_eq!(std::fs::read(&path).unwrap(), stable);

        let tiny = EmbeddingRefreshConfigLimits {
            metadata_bytes: 1,
            ..EmbeddingRefreshConfigLimits::default()
        };
        assert!(matches!(
            read_embedding_refresh_config(project.path(), tiny, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted { .. })
        ));
    }

    #[cfg(not(unix))]
    #[test]
    fn directory_sync_is_a_supported_noop() {
        let directory = tempfile::tempdir().unwrap();
        sync_directory(directory.path()).unwrap();
    }

    #[test]
    fn policy_and_wire_validation_matrix_is_exact_and_side_effect_free() {
        for debounce in [
            Duration::ZERO,
            MAX_DEBOUNCE + Duration::from_millis(1),
            Duration::from_nanos(1_500_000),
        ] {
            assert!(matches!(
                validate_debounce(debounce),
                Err(SearchArtifactError::InvalidSelector {
                    field: "embedding refresh debounce",
                    ..
                })
            ));
        }
        assert!(validate_debounce(Duration::from_millis(1)).is_ok());
        for max_concurrent_jobs in [0, MAX_EMBEDDING_REFRESH_JOBS + 1] {
            assert!(
                validate_project_policy(EmbeddingRefreshProjectPolicy {
                    max_concurrent_jobs,
                    ..EmbeddingRefreshProjectPolicy::default()
                })
                .is_err()
            );
        }
        assert!(
            validate_space_policy(EmbeddingRefreshSpacePolicy {
                proactive: None,
                debounce: None,
            })
            .is_err()
        );
        assert!(
            validate_outcome(EmbeddingRefreshOutcomeRecord {
                status: EmbeddingRefreshOutcomeStatus::Cancelled,
                graph_generation: 0,
                source_fingerprint: fingerprint(1),
                completed_at_micros: -1,
            })
            .is_err()
        );

        let prior = outcome(3, 3, 30);
        for next in [outcome(3, 3, 29), outcome(2, 2, 30), outcome(3, 4, 30)] {
            assert!(validate_outcome_progress(prior, next).is_err());
        }
        assert!(validate_outcome_progress(prior, prior).is_ok());

        let classes = [
            EmbeddingRefreshFailureClass::Provider,
            EmbeddingRefreshFailureClass::Validation,
            EmbeddingRefreshFailureClass::ResourceExhausted,
            EmbeddingRefreshFailureClass::Storage,
            EmbeddingRefreshFailureClass::ConcurrentMutation,
            EmbeddingRefreshFailureClass::Incompatible,
            EmbeddingRefreshFailureClass::Corrupt,
            EmbeddingRefreshFailureClass::Unavailable,
        ];
        for class in classes {
            let token = failure_token(class);
            assert_eq!(parse_failure_class(token).unwrap(), class);
            assert_eq!(
                outcome_tokens(EmbeddingRefreshOutcomeStatus::Failed(class)),
                ("failed", Some(token))
            );
        }
        assert!(parse_failure_class("unknown").is_err());
        assert_eq!(
            outcome_tokens(EmbeddingRefreshOutcomeStatus::Succeeded),
            ("succeeded", None)
        );
        assert_eq!(
            outcome_tokens(EmbeddingRefreshOutcomeStatus::Cancelled),
            ("cancelled", None)
        );
    }
}
