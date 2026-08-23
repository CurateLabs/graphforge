//! Rust-owned, disabled-by-default telemetry runtime.
//!
//! The runtime never installs a process-global provider or subscriber. Enabled
//! runtimes own one bounded worker and export only the finite semantic contract
//! declared in this module.
#![forbid(unsafe_code)]

use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use url::Url;

/// Version of the GraphForge telemetry semantic contract.
pub const SEMANTIC_CONTRACT_VERSION: u16 = 1;
/// Instrumentation scope used by every GraphForge signal.
pub const INSTRUMENTATION_SCOPE: &str = "io.graphforge.engine";
/// Instrumentation scope version.
pub const INSTRUMENTATION_SCOPE_VERSION: &str = "1";

/// Stable configuration error codes projected by every binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryConfigErrorCode {
    /// A numeric bound is outside the supported range.
    InvalidBound,
    /// The batch size is larger than its queue.
    BatchExceedsQueue,
    /// An OTLP endpoint is absent or unsafe to retain.
    InvalidEndpoint,
    /// An OTLP header name or value is invalid.
    InvalidHeader,
    /// OTLP settings were supplied for a different mode.
    InvalidMode,
    /// The bounded exporter worker could not be created.
    ExporterUnavailable,
}

impl TelemetryConfigErrorCode {
    /// Stable cross-language code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidBound => "GF_TELEMETRY_INVALID_BOUND",
            Self::BatchExceedsQueue => "GF_TELEMETRY_BATCH_EXCEEDS_QUEUE",
            Self::InvalidEndpoint => "GF_TELEMETRY_INVALID_ENDPOINT",
            Self::InvalidHeader => "GF_TELEMETRY_INVALID_HEADER",
            Self::InvalidMode => "GF_TELEMETRY_INVALID_MODE",
            Self::ExporterUnavailable => "GF_TELEMETRY_EXPORTER_UNAVAILABLE",
        }
    }
}

/// Sanitized configuration failure.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("GraphForge telemetry configuration rejected ({code:?})")]
pub struct TelemetryConfigError {
    /// Stable, credential-free classification.
    pub code: TelemetryConfigErrorCode,
}

/// Runtime mode. Disabled is the default and owns no heap state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryMode {
    /// No worker, timer, DNS, socket, or record allocation.
    #[default]
    Disabled,
    /// Deterministic snapshots for tests and embedding validation.
    InMemory,
    /// OTLP/HTTP JSON export to an explicitly supplied endpoint.
    OtlpHttpJson,
}

/// OTLP transport configuration. Debug output deliberately redacts headers.
#[derive(Clone, Eq, PartialEq)]
pub struct OtlpConfig {
    /// Collector base URL; `/v1/{signal}` is appended.
    pub endpoint: String,
    /// Explicit request headers, commonly authorization material.
    pub headers: BTreeMap<String, String>,
}

impl fmt::Debug for OtlpConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtlpConfig")
            .field("endpoint", &redacted_endpoint(&self.endpoint))
            .field(
                "headers",
                &format_args!("<redacted:{}>", self.headers.len()),
            )
            .finish()
    }
}

/// Fully bounded runtime policy. No environment variables are read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelemetryConfig {
    /// Explicit runtime mode.
    pub mode: TelemetryMode,
    /// Maximum queued records.
    pub queue_capacity: usize,
    /// Maximum records exported together.
    pub batch_size: usize,
    /// Maximum delay before a partial batch is exported.
    pub scheduled_delay: Duration,
    /// Per-attempt network bound.
    pub export_timeout: Duration,
    /// Caller-visible flush/shutdown bound.
    pub lifecycle_timeout: Duration,
    /// Maximum additional export attempts.
    pub max_retries: u8,
    /// Initial retry delay; doubles up to the lifecycle bound.
    pub retry_backoff: Duration,
    /// OTLP settings, required only for OTLP mode.
    pub otlp: Option<OtlpConfig>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            mode: TelemetryMode::Disabled,
            queue_capacity: 256,
            batch_size: 64,
            scheduled_delay: Duration::from_secs(5),
            export_timeout: Duration::from_secs(3),
            lifecycle_timeout: Duration::from_secs(5),
            max_retries: 2,
            retry_backoff: Duration::from_millis(100),
            otlp: None,
        }
    }
}

impl TelemetryConfig {
    fn validate(&self) -> Result<Option<ValidatedOtlp>, TelemetryConfigError> {
        if self.queue_capacity == 0
            || self.queue_capacity > 65_536
            || self.batch_size == 0
            || self.scheduled_delay.is_zero()
            || self.scheduled_delay > Duration::from_mins(1)
            || self.export_timeout.is_zero()
            || self.export_timeout > Duration::from_secs(30)
            || self.lifecycle_timeout.is_zero()
            || self.lifecycle_timeout > Duration::from_mins(1)
            || self.max_retries > 5
            || self.retry_backoff > Duration::from_secs(5)
        {
            return Err(config_error(TelemetryConfigErrorCode::InvalidBound));
        }
        if self.batch_size > self.queue_capacity {
            return Err(config_error(TelemetryConfigErrorCode::BatchExceedsQueue));
        }
        match (self.mode, &self.otlp) {
            (TelemetryMode::OtlpHttpJson, Some(config)) => validate_otlp(config).map(Some),
            (TelemetryMode::OtlpHttpJson, None) => {
                Err(config_error(TelemetryConfigErrorCode::InvalidEndpoint))
            }
            (_, Some(_)) => Err(config_error(TelemetryConfigErrorCode::InvalidMode)),
            (_, None) => Ok(None),
        }
    }
}

fn config_error(code: TelemetryConfigErrorCode) -> TelemetryConfigError {
    TelemetryConfigError { code }
}

#[derive(Clone)]
struct ValidatedOtlp {
    endpoint: Url,
    headers: BTreeMap<String, String>,
}

fn validate_otlp(config: &OtlpConfig) -> Result<ValidatedOtlp, TelemetryConfigError> {
    let endpoint = Url::parse(&config.endpoint)
        .map_err(|_| config_error(TelemetryConfigErrorCode::InvalidEndpoint))?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(config_error(TelemetryConfigErrorCode::InvalidEndpoint));
    }
    for (name, value) in &config.headers {
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || value.is_empty()
            || value.len() > 4096
            || value.bytes().any(|byte| matches!(byte, b'\r' | b'\n'))
        {
            return Err(config_error(TelemetryConfigErrorCode::InvalidHeader));
        }
    }
    Ok(ValidatedOtlp {
        endpoint,
        headers: config.headers.clone(),
    })
}

fn redacted_endpoint(raw: &str) -> String {
    Url::parse(raw).map_or_else(
        |_| "<invalid-endpoint>".to_owned(),
        |mut endpoint| {
            let _ = endpoint.set_username("");
            let _ = endpoint.set_password(None);
            endpoint.set_query(None);
            endpoint.set_fragment(None);
            endpoint.to_string()
        },
    )
}

macro_rules! semantic_enum {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
        #[serde(rename_all = "snake_case")]
        #[allow(missing_docs)]
        pub enum $name { $($variant),+ }
        impl $name { #[must_use] #[allow(missing_docs)] pub const fn as_str(self) -> &'static str { match self { $(Self::$variant => $value),+ } } }
    };
}

semantic_enum!(Operation {
    Query => "query", Mutation => "mutation", Persist => "persist", Import => "import",
    Export => "export", Clone => "clone", Inspect => "inspect", Maintenance => "maintenance"
});
semantic_enum!(Stage {
    Parse => "parse", Bind => "bind", Plan => "plan", Execute => "execute",
    Commit => "commit", Transfer => "transfer", Recover => "recover"
});
semantic_enum!(Outcome { Ok => "ok", Denied => "denied", Cancelled => "cancelled", Failed => "failed" });
semantic_enum!(Failure {
    InvalidInput => "invalid_input", ResourceLimit => "resource_limit", Storage => "storage",
    Network => "network", Provider => "provider", Internal => "internal"
});
semantic_enum!(Limit { Time => "time", Memory => "memory", Rows => "rows", Bytes => "bytes", Concurrency => "concurrency" });
semantic_enum!(JobFamily {
    Query => "query", Import => "import", Export => "export", Clone => "clone",
    Checkpoint => "checkpoint", Recovery => "recovery", Publication => "publication", Maintenance => "maintenance"
});
semantic_enum!(WaitReason {
    Queue => "queue", WorkspaceLock => "workspace_lock", Storage => "storage",
    Network => "network", Provider => "provider", Backoff => "backoff"
});
semantic_enum!(ComponentKind {
    Cli => "cli", Api => "api", Discovery => "discovery", CypherParser => "cypher_parser",
    Ir => "ir", RelationalPlanner => "relational_planner", Executor => "executor",
    Adjacency => "adjacency", Storage => "storage", TextSearch => "text_search",
    VectorSearch => "vector_search", Provider => "provider", PortableVerify => "portable_verify",
    PortableImport => "portable_import", PortableExport => "portable_export", Checkpoint => "checkpoint",
    Recovery => "recovery", Publication => "publication", NetworkTransport => "network_transport"
});
semantic_enum!(ComponentRole {
    Facade => "facade", Parser => "parser", Planner => "planner", Compute => "compute",
    Index => "index", Persistence => "persistence", Verification => "verification",
    Transfer => "transfer", Coordination => "coordination"
});
semantic_enum!(HandoffKind { Call => "call", Read => "read", Write => "write", Transfer => "transfer", Return => "return" });

/// One non-overlapping active interval in a local workspace job.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct JobStage {
    /// Finite stage class.
    pub stage: Stage,
    /// Component that actually ran.
    pub component: ComponentKind,
    /// Finite role played by that component.
    pub component_role: ComponentRole,
    /// Offset from job start, in nanoseconds.
    pub start_offset_ns: u64,
    /// Wall duration, in nanoseconds.
    pub duration_ns: u64,
    /// Time within the interval spent waiting.
    pub wait_duration_ns: u64,
    /// Finite reason when wait duration is nonzero.
    pub wait_reason: Option<WaitReason>,
    /// One-based attempt number.
    pub attempt: u32,
    /// Exact byte count, when the operation already knows it.
    pub bytes: Option<u64>,
    /// Exact record count, when the operation already knows it.
    pub records: Option<u64>,
    /// Normalized stage outcome.
    pub outcome: Outcome,
}

/// One ordered handoff between components actually used by a job.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ComponentHandoff {
    /// Calling component.
    pub from: ComponentKind,
    /// Receiving component.
    pub to: ComponentKind,
    /// Finite handoff class.
    pub kind: HandoffKind,
    /// Handoff wall duration, in nanoseconds.
    pub duration_ns: u64,
    /// Explicit wait within the handoff.
    pub wait_duration_ns: u64,
    /// Exact bytes, when known.
    pub bytes: Option<u64>,
    /// Exact records, when known.
    pub records: Option<u64>,
}

/// Complete, deterministic timing record for one local workspace job.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct JobSnapshot {
    /// Finite job family.
    pub family: JobFamily,
    /// Enqueue boundary on the job's monotonic timeline.
    pub enqueued_ns: u64,
    /// Start boundary on the same timeline.
    pub started_ns: u64,
    /// Finish boundary on the same timeline.
    pub finished_ns: u64,
    /// Normalized job outcome.
    pub outcome: Outcome,
    /// Non-overlapping active intervals in chronological order.
    pub stages: Vec<JobStage>,
    /// Ordered component transfers actually observed.
    pub handoffs: Vec<ComponentHandoff>,
}

/// Invalid local-job timing or component accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid GraphForge local-job telemetry")]
pub struct InvalidJobSnapshot;

impl JobSnapshot {
    /// Validate timing math and deny ambiguous or overlapping attribution.
    pub fn validate(&self) -> Result<(), InvalidJobSnapshot> {
        if self.enqueued_ns > self.started_ns || self.started_ns > self.finished_ns {
            return Err(InvalidJobSnapshot);
        }
        let active = self.finished_ns - self.started_ns;
        let mut cursor = 0_u64;
        for stage in &self.stages {
            if stage.attempt == 0
                || stage.duration_ns == 0
                || stage.wait_duration_ns > stage.duration_ns
                || (stage.wait_duration_ns == 0) != stage.wait_reason.is_none()
                || stage.start_offset_ns != cursor
            {
                return Err(InvalidJobSnapshot);
            }
            cursor = cursor
                .checked_add(stage.duration_ns)
                .ok_or(InvalidJobSnapshot)?;
        }
        if cursor != active {
            return Err(InvalidJobSnapshot);
        }
        if self.handoffs.iter().any(|handoff| {
            handoff.from == handoff.to || handoff.wait_duration_ns > handoff.duration_ns
        }) {
            return Err(InvalidJobSnapshot);
        }
        Ok(())
    }

    /// Queue delay in nanoseconds.
    #[must_use]
    pub const fn queue_delay_ns(&self) -> u64 {
        self.started_ns - self.enqueued_ns
    }

    /// Active wall time in nanoseconds.
    #[must_use]
    pub const fn active_duration_ns(&self) -> u64 {
        self.finished_ns - self.started_ns
    }

    /// Total elapsed wall time in nanoseconds.
    #[must_use]
    pub const fn total_duration_ns(&self) -> u64 {
        self.finished_ns - self.enqueued_ns
    }
}

/// Closed attribute set. Operation code cannot add arbitrary keys or values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Attributes {
    /// Finite operation class.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<Operation>,
    /// Finite lifecycle stage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<Stage>,
    /// Finite outcome class.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,
    /// Finite failure class.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<Failure>,
    /// Finite resource-limit class.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<Limit>,
}

impl Attributes {
    /// Start an allowlisted attribute set for one operation.
    #[must_use]
    pub const fn for_operation(operation: Operation) -> Self {
        Self {
            operation: Some(operation),
            stage: None,
            outcome: None,
            failure: None,
            limit: None,
        }
    }
    /// Add a registered stage.
    #[must_use]
    pub const fn stage(mut self, stage: Stage) -> Self {
        self.stage = Some(stage);
        self
    }
    /// Add a registered outcome.
    #[must_use]
    pub const fn outcome(mut self, outcome: Outcome) -> Self {
        self.outcome = Some(outcome);
        self
    }
    /// Add a registered failure class.
    #[must_use]
    pub const fn failure(mut self, failure: Failure) -> Self {
        self.failure = Some(failure);
        self
    }
    /// Add a registered limit class.
    #[must_use]
    pub const fn limit(mut self, limit: Limit) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// OpenTelemetry signal family.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    /// Span/trace signal.
    Trace,
    /// Numeric metric signal.
    Metric,
    /// Structured log event.
    Event,
}

/// Versioned signal registry used by later instrumentation stories.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum Signal {
    /// One complete local workspace-job timing trace.
    LocalJobTrace,
    /// One bounded operation trace.
    OperationTrace,
    /// Monotonic operation counter.
    OperationCount,
    /// Structured lifecycle diagnostic.
    LifecycleEvent,
}

/// Static metadata for one registered signal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct SignalDescriptor {
    /// Stable signal name.
    pub name: &'static str,
    /// OpenTelemetry signal family.
    pub kind: SignalKind,
    /// UCUM unit.
    pub unit: &'static str,
}

impl Signal {
    /// Registered name, signal kind, and UCUM unit.
    #[must_use]
    pub const fn descriptor(self) -> SignalDescriptor {
        match self {
            Self::LocalJobTrace => SignalDescriptor {
                name: "graphforge.workspace.job",
                kind: SignalKind::Trace,
                unit: "ns",
            },
            Self::OperationTrace => SignalDescriptor {
                name: "graphforge.operation",
                kind: SignalKind::Trace,
                unit: "ns",
            },
            Self::OperationCount => SignalDescriptor {
                name: "graphforge.operation.count",
                kind: SignalKind::Metric,
                unit: "{operation}",
            },
            Self::LifecycleEvent => SignalDescriptor {
                name: "graphforge.lifecycle",
                kind: SignalKind::Event,
                unit: "1",
            },
        }
    }
}

/// Immutable deterministic in-memory record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Snapshot {
    /// Semantic contract version.
    pub contract_version: u16,
    /// Instrumentation scope.
    pub scope: &'static str,
    /// Instrumentation scope version.
    pub scope_version: &'static str,
    /// Registered descriptor.
    pub signal: SignalDescriptor,
    /// Allowlisted attributes.
    pub attributes: Attributes,
    /// Typed local-job detail, present only for the local-job trace signal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job: Option<JobSnapshot>,
}

/// Non-failing recording result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordStatus {
    /// Runtime is disabled.
    Disabled,
    /// Record entered the bounded queue.
    Accepted,
    /// Queue was full; the graph operation remains unaffected.
    DroppedQueueFull,
    /// Runtime was already stopped.
    Shutdown,
}

/// Stable lifecycle result. Failures are diagnostics, never graph errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleStatus {
    /// Runtime is disabled.
    Disabled,
    /// Lifecycle request completed.
    Complete,
    /// Configured caller bound elapsed.
    TimedOut,
    /// One or more exports failed.
    ExportFailed,
    /// Runtime was already stopped.
    AlreadyShutdown,
}

impl LifecycleStatus {
    /// Stable cross-language status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Complete => "complete",
            Self::TimedOut => "timed_out",
            Self::ExportFailed => "export_failed",
            Self::AlreadyShutdown => "already_shutdown",
        }
    }
}

/// Sanitized bounded runtime counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct TelemetryStats {
    /// Records accepted into the queue.
    pub accepted: u64,
    /// Records dropped on saturation.
    pub dropped: u64,
    /// Failed batches.
    pub export_failures: u64,
}

/// Cloneable explicit GraphForge telemetry provider handle.
#[derive(Clone, Default)]
pub struct TelemetryRuntime {
    active: Option<Arc<ActiveRuntime>>,
}

impl fmt::Debug for TelemetryRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(if self.active.is_some() {
            "TelemetryRuntime::Active"
        } else {
            "TelemetryRuntime::Disabled"
        })
    }
}

/// Alias emphasizing that clones share one explicit provider lifetime.
pub type TelemetryGuard = TelemetryRuntime;

struct ActiveRuntime {
    sender: SyncSender<Command>,
    lifecycle_timeout: Duration,
    state: AtomicU8,
    accepted: AtomicU64,
    dropped: AtomicU64,
    export_failures: Arc<AtomicU64>,
    snapshots: Option<Arc<Mutex<Vec<Snapshot>>>>,
}

enum Command {
    Record(Snapshot),
    Flush(SyncSender<LifecycleStatus>),
    Shutdown(SyncSender<LifecycleStatus>),
}

impl TelemetryRuntime {
    /// Construct one explicit provider. No global state is read or changed.
    pub fn new(config: TelemetryConfig) -> Result<Self, TelemetryConfigError> {
        let validated = config.validate()?;
        if config.mode == TelemetryMode::Disabled {
            return Ok(Self::default());
        }
        let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);
        let snapshots =
            (config.mode == TelemetryMode::InMemory).then(|| Arc::new(Mutex::new(Vec::new())));
        let failures = Arc::new(AtomicU64::new(0));
        let backend = match (config.mode, validated) {
            (TelemetryMode::InMemory, None) => {
                Backend::Memory(Arc::clone(snapshots.as_ref().expect("memory sink")))
            }
            (TelemetryMode::OtlpHttpJson, Some(otlp)) => {
                Backend::Otlp(OtlpExporter::new(otlp, &config))
            }
            _ => return Err(config_error(TelemetryConfigErrorCode::InvalidMode)),
        };
        let worker_failures = Arc::clone(&failures);
        let lifecycle_timeout = config.lifecycle_timeout;
        thread::Builder::new()
            .name("graphforge-otel".to_owned())
            .spawn(move || worker(&receiver, &backend, &config, worker_failures.as_ref()))
            .map_err(|_| config_error(TelemetryConfigErrorCode::ExporterUnavailable))?;
        Ok(Self {
            active: Some(Arc::new(ActiveRuntime {
                sender,
                lifecycle_timeout,
                state: AtomicU8::new(0),
                accepted: AtomicU64::new(0),
                dropped: AtomicU64::new(0),
                export_failures: failures,
                snapshots,
            })),
        })
    }

    /// Whether this handle owns an enabled provider.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.active.is_some()
    }

    /// Record one registered signal without blocking or surfacing exporter failure.
    #[must_use]
    pub fn record(&self, signal: Signal, attributes: Attributes) -> RecordStatus {
        let Some(active) = &self.active else {
            return RecordStatus::Disabled;
        };
        if active.state.load(Ordering::Acquire) != 0 {
            return RecordStatus::Shutdown;
        }
        let snapshot = Snapshot {
            contract_version: SEMANTIC_CONTRACT_VERSION,
            scope: INSTRUMENTATION_SCOPE,
            scope_version: INSTRUMENTATION_SCOPE_VERSION,
            signal: signal.descriptor(),
            attributes,
            job: None,
        };
        Self::enqueue(active, snapshot)
    }

    /// Record one validated local workspace-job trace without blocking the job.
    pub fn record_job(&self, job: JobSnapshot) -> Result<RecordStatus, InvalidJobSnapshot> {
        job.validate()?;
        let Some(active) = &self.active else {
            return Ok(RecordStatus::Disabled);
        };
        if active.state.load(Ordering::Acquire) != 0 {
            return Ok(RecordStatus::Shutdown);
        }
        let snapshot = Snapshot {
            contract_version: SEMANTIC_CONTRACT_VERSION,
            scope: INSTRUMENTATION_SCOPE,
            scope_version: INSTRUMENTATION_SCOPE_VERSION,
            signal: Signal::LocalJobTrace.descriptor(),
            attributes: Attributes::default(),
            job: Some(job),
        };
        Ok(Self::enqueue(active, snapshot))
    }

    fn enqueue(active: &ActiveRuntime, snapshot: Snapshot) -> RecordStatus {
        match active.sender.try_send(Command::Record(snapshot)) {
            Ok(()) => {
                active.accepted.fetch_add(1, Ordering::Relaxed);
                RecordStatus::Accepted
            }
            Err(TrySendError::Full(_)) => {
                active.dropped.fetch_add(1, Ordering::Relaxed);
                RecordStatus::DroppedQueueFull
            }
            Err(TrySendError::Disconnected(_)) => RecordStatus::Shutdown,
        }
    }

    /// Flush records queued before this call within the configured bound.
    #[must_use]
    pub fn force_flush(&self) -> LifecycleStatus {
        self.lifecycle(false)
    }

    /// Idempotently stop this explicit provider within the configured bound.
    #[must_use]
    pub fn shutdown(&self) -> LifecycleStatus {
        self.lifecycle(true)
    }

    fn lifecycle(&self, shutdown: bool) -> LifecycleStatus {
        let Some(active) = &self.active else {
            return LifecycleStatus::Disabled;
        };
        if shutdown && active.state.swap(1, Ordering::AcqRel) != 0 {
            return LifecycleStatus::AlreadyShutdown;
        }
        if !shutdown && active.state.load(Ordering::Acquire) != 0 {
            return LifecycleStatus::AlreadyShutdown;
        }
        let (reply, response) = mpsc::sync_channel(1);
        let command = if shutdown {
            Command::Shutdown(reply)
        } else {
            Command::Flush(reply)
        };
        let deadline = Instant::now() + active.lifecycle_timeout;
        let mut pending = command;
        loop {
            match active.sender.try_send(pending) {
                Ok(()) => break,
                Err(TrySendError::Disconnected(_)) => return LifecycleStatus::AlreadyShutdown,
                Err(TrySendError::Full(command)) => {
                    if Instant::now() >= deadline {
                        return LifecycleStatus::TimedOut;
                    }
                    pending = command;
                    thread::yield_now();
                }
            }
        }
        response
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(LifecycleStatus::TimedOut)
    }

    /// Deterministic memory snapshot; empty for disabled/OTLP modes.
    #[must_use]
    pub fn snapshots(&self) -> Vec<Snapshot> {
        let Some(active) = &self.active else {
            return Vec::new();
        };
        active.snapshots.as_ref().map_or_else(Vec::new, |records| {
            records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        })
    }

    /// Sanitized counters suitable for diagnostics.
    #[must_use]
    pub fn stats(&self) -> TelemetryStats {
        let Some(active) = &self.active else {
            return TelemetryStats::default();
        };
        TelemetryStats {
            accepted: active.accepted.load(Ordering::Relaxed),
            dropped: active.dropped.load(Ordering::Relaxed),
            export_failures: active.export_failures.load(Ordering::Relaxed),
        }
    }
}

impl Drop for ActiveRuntime {
    fn drop(&mut self) {
        if self.state.swap(1, Ordering::AcqRel) == 0 {
            let (reply, _) = mpsc::sync_channel(1);
            let _ = self.sender.try_send(Command::Shutdown(reply));
        }
    }
}

enum Backend {
    Memory(Arc<Mutex<Vec<Snapshot>>>),
    Otlp(OtlpExporter),
}

impl Backend {
    fn export(&self, batch: &[Snapshot]) -> bool {
        match self {
            Self::Memory(records) => {
                records
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .extend_from_slice(batch);
                true
            }
            Self::Otlp(exporter) => exporter.export(batch),
        }
    }
}

fn worker(
    receiver: &Receiver<Command>,
    backend: &Backend,
    config: &TelemetryConfig,
    failures: &AtomicU64,
) {
    let mut batch = Vec::with_capacity(config.batch_size);
    loop {
        match receiver.recv_timeout(config.scheduled_delay) {
            Ok(Command::Record(record)) => {
                batch.push(record);
                if batch.len() >= config.batch_size {
                    export_batch(backend, &mut batch, failures);
                }
            }
            Ok(Command::Flush(reply)) => {
                export_batch(backend, &mut batch, failures);
                let status = if failures.load(Ordering::Relaxed) == 0 {
                    LifecycleStatus::Complete
                } else {
                    LifecycleStatus::ExportFailed
                };
                let _ = reply.try_send(status);
            }
            Ok(Command::Shutdown(reply)) => {
                export_batch(backend, &mut batch, failures);
                let status = if failures.load(Ordering::Relaxed) == 0 {
                    LifecycleStatus::Complete
                } else {
                    LifecycleStatus::ExportFailed
                };
                let _ = reply.try_send(status);
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => export_batch(backend, &mut batch, failures),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                export_batch(backend, &mut batch, failures);
                break;
            }
        }
    }
}

fn export_batch(backend: &Backend, batch: &mut Vec<Snapshot>, failures: &AtomicU64) {
    if batch.is_empty() {
        return;
    }
    if !backend.export(batch) {
        failures.fetch_add(1, Ordering::Relaxed);
    }
    batch.clear();
}

struct OtlpExporter {
    endpoint: Url,
    headers: BTreeMap<String, String>,
    agent: ureq::Agent,
    max_retries: u8,
    retry_backoff: Duration,
    lifecycle_timeout: Duration,
    export_sequence: AtomicU64,
}

impl OtlpExporter {
    fn new(config: ValidatedOtlp, policy: &TelemetryConfig) -> Self {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(policy.export_timeout))
            .timeout_connect(Some(policy.export_timeout))
            .max_redirects(0)
            .build()
            .into();
        Self {
            endpoint: config.endpoint,
            headers: config.headers,
            agent,
            max_retries: policy.max_retries,
            retry_backoff: policy.retry_backoff,
            lifecycle_timeout: policy.lifecycle_timeout,
            export_sequence: AtomicU64::new(export_sequence_seed()),
        }
    }

    fn export(&self, batch: &[Snapshot]) -> bool {
        let started = Instant::now();
        for kind in [SignalKind::Trace, SignalKind::Metric, SignalKind::Event] {
            let records: Vec<_> = batch
                .iter()
                .filter(|record| record.signal.kind == kind)
                .collect();
            if records.is_empty() {
                continue;
            }
            let sequence = self.export_sequence.fetch_add(1, Ordering::Relaxed);
            let body = otlp_json(kind, &records, sequence);
            let body = serde_json::to_string(&body).expect("OTLP JSON serialization");
            let mut attempt = 0_u8;
            loop {
                let mut request = self
                    .agent
                    .post(self.signal_url(kind).as_str())
                    .header("content-type", "application/json");
                for (name, value) in &self.headers {
                    request = request.header(name, value);
                }
                if request.send(&body).is_ok() {
                    break;
                }
                if attempt >= self.max_retries || started.elapsed() >= self.lifecycle_timeout {
                    return false;
                }
                let delay = self.retry_backoff.saturating_mul(1_u32 << attempt.min(5));
                thread::sleep(delay.min(self.lifecycle_timeout.saturating_sub(started.elapsed())));
                attempt += 1;
            }
        }
        true
    }

    fn signal_url(&self, kind: SignalKind) -> Url {
        let suffix = match kind {
            SignalKind::Trace => "v1/traces",
            SignalKind::Metric => "v1/metrics",
            SignalKind::Event => "v1/logs",
        };
        self.endpoint.join(suffix).expect("validated base URL")
    }
}

fn otlp_json(kind: SignalKind, records: &[&Snapshot], sequence: u64) -> Value {
    let scope = json!({"name": INSTRUMENTATION_SCOPE, "version": INSTRUMENTATION_SCOPE_VERSION});
    match kind {
        SignalKind::Trace => {
            let exported_at = unix_nanos_value();
            let spans = records
                .iter()
                .enumerate()
                .flat_map(|(index, record)| trace_spans(record, sequence, index, exported_at))
                .collect::<Vec<_>>();
            json!({"resourceSpans":[{"scopeSpans":[{"scope":scope,"spans":spans}]}]})
        }
        SignalKind::Metric => {
            json!({"resourceMetrics":[{"scopeMetrics":[{"scope":scope,"metrics":records.iter().map(|record| json!({"name":record.signal.name,"unit":record.signal.unit,"sum":{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{"asInt":"1","timeUnixNano":unix_nanos(),"attributes":otel_attributes(record.attributes)}]}})).collect::<Vec<_>>() }]}]})
        }
        SignalKind::Event => {
            json!({"resourceLogs":[{"scopeLogs":[{"scope":scope,"logRecords":records.iter().map(|record| json!({"timeUnixNano":unix_nanos(),"severityNumber":9,"body":{"stringValue":record.signal.name},"attributes":otel_attributes(record.attributes)})).collect::<Vec<_>>() }]}]})
        }
    }
}

fn trace_spans(record: &Snapshot, sequence: u64, index: usize, exported_at: u128) -> Vec<Value> {
    let trace_id = format!("{sequence:016x}{:016x}", index as u64 + 1);
    let root_span_id = span_id(sequence, index as u64 + 1);
    let Some(job) = &record.job else {
        return vec![json!({
            "traceId": trace_id,
            "spanId": root_span_id,
            "name": record.signal.name,
            "kind": 1,
            "startTimeUnixNano": exported_at.saturating_sub(1).to_string(),
            "endTimeUnixNano": exported_at.to_string(),
            "attributes": otel_attributes(record.attributes)
        })];
    };

    let total = u128::from(job.total_duration_ns());
    let root_start = exported_at.saturating_sub(total);
    let active_start = root_start + u128::from(job.queue_delay_ns());
    let mut root_attributes = vec![
        otel_string("graphforge.job.family", job.family.as_str()),
        otel_string("graphforge.outcome", job.outcome.as_str()),
        otel_int("graphforge.job.queue_delay_ns", job.queue_delay_ns()),
        otel_int(
            "graphforge.job.active_duration_ns",
            job.active_duration_ns(),
        ),
        otel_int("graphforge.job.total_duration_ns", job.total_duration_ns()),
        otel_int("graphforge.job.stage_count", job.stages.len() as u64),
        otel_int("graphforge.job.handoff_count", job.handoffs.len() as u64),
    ];
    root_attributes.extend(otel_attributes(record.attributes));
    let events = job
        .handoffs
        .iter()
        .enumerate()
        .map(|(handoff_index, handoff)| {
            let mut attributes = vec![
                otel_int("graphforge.handoff.index", handoff_index as u64),
                otel_string("graphforge.handoff.from", handoff.from.as_str()),
                otel_string("graphforge.handoff.to", handoff.to.as_str()),
                otel_string("graphforge.handoff.kind", handoff.kind.as_str()),
                otel_int("graphforge.handoff.duration_ns", handoff.duration_ns),
                otel_int("graphforge.handoff.wait_duration_ns", handoff.wait_duration_ns),
            ];
            push_optional_int(&mut attributes, "graphforge.handoff.bytes", handoff.bytes);
            push_optional_int(&mut attributes, "graphforge.handoff.records", handoff.records);
            json!({"timeUnixNano":active_start.to_string(),"name":"graphforge.component.handoff","attributes":attributes})
        })
        .collect::<Vec<_>>();
    let mut spans = vec![json!({
        "traceId": trace_id,
        "spanId": root_span_id,
        "name": record.signal.name,
        "kind": 1,
        "startTimeUnixNano": root_start.to_string(),
        "endTimeUnixNano": exported_at.to_string(),
        "attributes": root_attributes,
        "events": events
    })];
    spans.extend(job.stages.iter().enumerate().map(|(stage_index, stage)| {
        let start = active_start + u128::from(stage.start_offset_ns);
        let end = start + u128::from(stage.duration_ns);
        let mut attributes = vec![
            otel_string("graphforge.stage", stage.stage.as_str()),
            otel_string(
                "graphforge.workspace.component.kind",
                stage.component.as_str(),
            ),
            otel_string(
                "graphforge.workspace.component.role",
                stage.component_role.as_str(),
            ),
            otel_int("graphforge.stage.duration_ns", stage.duration_ns),
            otel_int("graphforge.stage.wait_duration_ns", stage.wait_duration_ns),
            otel_int("graphforge.stage.attempt", u64::from(stage.attempt)),
            otel_string("graphforge.outcome", stage.outcome.as_str()),
        ];
        if let Some(reason) = stage.wait_reason {
            attributes.push(otel_string("graphforge.stage.wait_reason", reason.as_str()));
        }
        push_optional_int(&mut attributes, "graphforge.stage.bytes", stage.bytes);
        push_optional_int(&mut attributes, "graphforge.stage.records", stage.records);
        json!({
            "traceId": trace_id,
            "spanId": span_id(sequence, stage_index as u64 + 2),
            "parentSpanId": root_span_id,
            "name": "graphforge.workspace.stage",
            "kind": 1,
            "startTimeUnixNano": start.to_string(),
            "endTimeUnixNano": end.to_string(),
            "attributes": attributes
        })
    }));
    spans
}

fn span_id(sequence: u64, ordinal: u64) -> String {
    format!(
        "{:016x}",
        sequence
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .wrapping_add(ordinal)
    )
}

fn otel_string(key: &'static str, value: &'static str) -> Value {
    json!({"key":key,"value":{"stringValue":value}})
}

fn otel_int(key: &'static str, value: u64) -> Value {
    json!({"key":key,"value":{"intValue":value.to_string()}})
}

fn push_optional_int(attributes: &mut Vec<Value>, key: &'static str, value: Option<u64>) {
    if let Some(value) = value {
        attributes.push(otel_int(key, value));
    }
}

fn unix_nanos_value() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn export_sequence_seed() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_secs().rotate_left(32) ^ u64::from(now.subsec_nanos())).max(1)
}

fn unix_nanos() -> String {
    unix_nanos_value().to_string()
}

fn otel_attributes(attributes: Attributes) -> Vec<Value> {
    let mut values = Vec::with_capacity(5);
    if let Some(value) = attributes.operation {
        values.push(json!({"key":"graphforge.operation","value":{"stringValue":value.as_str()}}));
    }
    if let Some(value) = attributes.stage {
        values.push(json!({"key":"graphforge.stage","value":{"stringValue":value.as_str()}}));
    }
    if let Some(value) = attributes.outcome {
        values.push(json!({"key":"graphforge.outcome","value":{"stringValue":value.as_str()}}));
    }
    if let Some(value) = attributes.failure {
        values.push(json!({"key":"graphforge.failure","value":{"stringValue":value.as_str()}}));
    }
    if let Some(value) = attributes.limit {
        values.push(json!({"key":"graphforge.limit","value":{"stringValue":value.as_str()}}));
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn default_is_zero_state_disabled_and_idempotent() {
        let runtime = TelemetryRuntime::default();
        assert_eq!(format!("{runtime:?}"), "TelemetryRuntime::Disabled");
        assert!(!runtime.is_enabled());
        assert_eq!(
            runtime.record(Signal::OperationCount, Attributes::default()),
            RecordStatus::Disabled
        );
        assert_eq!(runtime.force_flush(), LifecycleStatus::Disabled);
        assert_eq!(runtime.shutdown(), LifecycleStatus::Disabled);
        assert_eq!(runtime.stats(), TelemetryStats::default());
    }

    #[test]
    fn memory_snapshots_use_only_the_registry() {
        let runtime = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::InMemory,
            ..TelemetryConfig::default()
        })
        .unwrap();
        let attributes = Attributes::for_operation(Operation::Query)
            .stage(Stage::Execute)
            .outcome(Outcome::Ok);
        for signal in [
            Signal::OperationTrace,
            Signal::OperationCount,
            Signal::LifecycleEvent,
        ] {
            assert_eq!(runtime.record(signal, attributes), RecordStatus::Accepted);
        }
        assert_eq!(runtime.force_flush(), LifecycleStatus::Complete);
        let snapshots = runtime.snapshots();
        assert_eq!(snapshots.len(), 3);
        assert!(
            snapshots
                .iter()
                .all(|snapshot| snapshot.scope == INSTRUMENTATION_SCOPE
                    && snapshot.contract_version == SEMANTIC_CONTRACT_VERSION
                    && snapshot.attributes == attributes)
        );
        assert_eq!(runtime.shutdown(), LifecycleStatus::Complete);
        assert_eq!(runtime.shutdown(), LifecycleStatus::AlreadyShutdown);
    }

    #[test]
    fn configuration_errors_and_debug_output_never_expose_secrets() {
        let secret = "gf-secret-canary";
        let config = TelemetryConfig {
            mode: TelemetryMode::OtlpHttpJson,
            otlp: Some(OtlpConfig {
                endpoint: format!("https://user:{secret}@collector.example/v1?token={secret}"),
                headers: BTreeMap::from([("authorization".into(), format!("Bearer {secret}"))]),
            }),
            ..TelemetryConfig::default()
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains(secret));
        let error = TelemetryRuntime::new(config).unwrap_err();
        assert_eq!(error.code, TelemetryConfigErrorCode::InvalidEndpoint);
        assert!(!error.to_string().contains(secret));
    }

    #[test]
    fn bounded_queue_saturation_is_fail_open() {
        let runtime = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::InMemory,
            queue_capacity: 1,
            batch_size: 1,
            ..TelemetryConfig::default()
        })
        .unwrap();
        for _ in 0..10_000 {
            let _ = runtime.record(Signal::OperationCount, Attributes::default());
        }
        assert_eq!(runtime.force_flush(), LifecycleStatus::Complete);
        let stats = runtime.stats();
        assert_eq!(stats.accepted + stats.dropped, 10_000);
    }

    #[test]
    fn clones_share_one_runtime_and_drop_order_does_not_stop_it() {
        let runtime = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::InMemory,
            ..TelemetryConfig::default()
        })
        .unwrap();
        let clone = runtime.clone();
        drop(runtime);
        assert_eq!(
            clone.record(Signal::OperationCount, Attributes::default()),
            RecordStatus::Accepted
        );
        assert_eq!(clone.shutdown(), LifecycleStatus::Complete);
        assert_eq!(clone.snapshots().len(), 1);
    }

    #[test]
    fn otlp_http_json_uses_signal_routes_and_allowlisted_payloads() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut paths = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut bytes = vec![0_u8; 16_384];
                let read = stream.read(&mut bytes).unwrap();
                let request = String::from_utf8_lossy(&bytes[..read]).into_owned();
                let first = request
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_owned();
                assert!(request.contains("application/json"));
                assert!(
                    !request.contains("repository-canary")
                        && !request.contains("token-canary")
                        && !request.contains("018f0f4e")
                );
                paths.push(first);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}")
                    .unwrap();
            }
            paths
        });
        let runtime = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::OtlpHttpJson,
            lifecycle_timeout: Duration::from_secs(2),
            export_timeout: Duration::from_secs(1),
            max_retries: 0,
            otlp: Some(OtlpConfig {
                endpoint: format!("http://{address}/"),
                headers: BTreeMap::new(),
            }),
            ..TelemetryConfig::default()
        })
        .unwrap();
        let attributes = Attributes::for_operation(Operation::Clone).outcome(Outcome::Ok);
        for signal in [
            Signal::OperationTrace,
            Signal::OperationCount,
            Signal::LifecycleEvent,
        ] {
            assert_eq!(runtime.record(signal, attributes), RecordStatus::Accepted);
        }
        assert_eq!(runtime.force_flush(), LifecycleStatus::Complete);
        assert_eq!(
            server.join().unwrap(),
            vec!["/v1/traces", "/v1/metrics", "/v1/logs"]
        );
        assert_eq!(runtime.shutdown(), LifecycleStatus::Complete);
    }

    #[test]
    fn unavailable_exporter_is_bounded_and_sanitized() {
        let runtime = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::OtlpHttpJson,
            lifecycle_timeout: Duration::from_millis(100),
            export_timeout: Duration::from_millis(20),
            max_retries: 1,
            retry_backoff: Duration::from_millis(1),
            otlp: Some(OtlpConfig {
                endpoint: "http://127.0.0.1:1/".into(),
                headers: BTreeMap::new(),
            }),
            ..TelemetryConfig::default()
        })
        .unwrap();
        assert_eq!(
            runtime.record(Signal::LifecycleEvent, Attributes::default()),
            RecordStatus::Accepted
        );
        let started = Instant::now();
        assert!(matches!(
            runtime.force_flush(),
            LifecycleStatus::ExportFailed | LifecycleStatus::TimedOut
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(runtime.stats().export_failures <= 1);
    }

    #[test]
    fn stalled_exporter_cannot_outlive_the_lifecycle_bound() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(250));
        });
        let runtime = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::OtlpHttpJson,
            lifecycle_timeout: Duration::from_millis(50),
            export_timeout: Duration::from_millis(25),
            max_retries: 0,
            otlp: Some(OtlpConfig {
                endpoint: format!("http://{address}/"),
                headers: BTreeMap::new(),
            }),
            ..TelemetryConfig::default()
        })
        .unwrap();
        let _ = runtime.record(Signal::LifecycleEvent, Attributes::default());
        let started = Instant::now();
        assert!(matches!(
            runtime.force_flush(),
            LifecycleStatus::ExportFailed | LifecycleStatus::TimedOut
        ));
        assert!(started.elapsed() < Duration::from_millis(100));
        server.join().unwrap();
    }

    #[test]
    fn golden_privacy_canaries_are_not_representable() {
        for forbidden_key in [
            "repository_id",
            "project_id",
            "graph_id",
            "uuid",
            "path",
            "query_text",
            "query_parameters",
            "property_data",
            "credential",
            "manifest",
            "result_content",
            "token",
        ] {
            assert!(
                !otel_attributes(Attributes::default())
                    .iter()
                    .any(|value| value.to_string().contains(forbidden_key))
            );
        }
    }

    #[test]
    fn deterministic_job_fixture_accounts_for_queue_and_active_time() {
        let job = JobSnapshot {
            family: JobFamily::Clone,
            enqueued_ns: 100,
            started_ns: 125,
            finished_ns: 225,
            outcome: Outcome::Ok,
            stages: vec![
                JobStage {
                    stage: Stage::Transfer,
                    component: ComponentKind::NetworkTransport,
                    component_role: ComponentRole::Transfer,
                    start_offset_ns: 0,
                    duration_ns: 70,
                    wait_duration_ns: 20,
                    wait_reason: Some(WaitReason::Network),
                    attempt: 2,
                    bytes: Some(4096),
                    records: None,
                    outcome: Outcome::Ok,
                },
                JobStage {
                    stage: Stage::Commit,
                    component: ComponentKind::Storage,
                    component_role: ComponentRole::Persistence,
                    start_offset_ns: 70,
                    duration_ns: 30,
                    wait_duration_ns: 0,
                    wait_reason: None,
                    attempt: 1,
                    bytes: Some(4096),
                    records: Some(8),
                    outcome: Outcome::Ok,
                },
            ],
            handoffs: vec![ComponentHandoff {
                from: ComponentKind::NetworkTransport,
                to: ComponentKind::Storage,
                kind: HandoffKind::Write,
                duration_ns: 5,
                wait_duration_ns: 0,
                bytes: Some(4096),
                records: Some(8),
            }],
        };
        job.validate().unwrap();
        assert_eq!(
            job.queue_delay_ns() + job.active_duration_ns(),
            job.total_duration_ns()
        );

        let runtime = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::InMemory,
            ..TelemetryConfig::default()
        })
        .unwrap();
        assert_eq!(
            runtime.record_job(job.clone()).unwrap(),
            RecordStatus::Accepted
        );
        assert_eq!(runtime.force_flush(), LifecycleStatus::Complete);
        assert_eq!(runtime.snapshots()[0].job.as_ref(), Some(&job));
    }

    #[test]
    fn job_paths_report_only_used_components_and_attribute_injected_slowdown() {
        let stage = |component, role, duration_ns| JobStage {
            stage: Stage::Execute,
            component,
            component_role: role,
            start_offset_ns: 0,
            duration_ns,
            wait_duration_ns: 0,
            wait_reason: None,
            attempt: 1,
            bytes: None,
            records: None,
            outcome: Outcome::Ok,
        };
        let query = JobSnapshot {
            family: JobFamily::Query,
            enqueued_ns: 0,
            started_ns: 10,
            finished_ns: 50,
            outcome: Outcome::Ok,
            stages: vec![stage(ComponentKind::Executor, ComponentRole::Compute, 40)],
            handoffs: vec![],
        };
        let publication = JobSnapshot {
            family: JobFamily::Publication,
            enqueued_ns: 0,
            started_ns: 10,
            finished_ns: 100,
            outcome: Outcome::Ok,
            stages: vec![stage(
                ComponentKind::Publication,
                ComponentRole::Persistence,
                90,
            )],
            handoffs: vec![],
        };
        query.validate().unwrap();
        publication.validate().unwrap();
        assert_eq!(query.stages[0].component, ComponentKind::Executor);
        assert_eq!(publication.stages[0].component, ComponentKind::Publication);
        assert_eq!(
            publication.active_duration_ns() - query.active_duration_ns(),
            50
        );
    }

    #[test]
    fn overlapping_or_incomplete_job_attribution_is_rejected() {
        let invalid = JobSnapshot {
            family: JobFamily::Query,
            enqueued_ns: 0,
            started_ns: 0,
            finished_ns: 10,
            outcome: Outcome::Ok,
            stages: vec![JobStage {
                stage: Stage::Execute,
                component: ComponentKind::Executor,
                component_role: ComponentRole::Compute,
                start_offset_ns: 1,
                duration_ns: 10,
                wait_duration_ns: 0,
                wait_reason: None,
                attempt: 1,
                bytes: None,
                records: None,
                outcome: Outcome::Ok,
            }],
            handoffs: vec![],
        };
        assert_eq!(invalid.validate(), Err(InvalidJobSnapshot));
    }

    #[test]
    fn otlp_job_trace_exports_coherent_timing_and_unique_batch_ids() {
        let snapshot = Snapshot {
            contract_version: SEMANTIC_CONTRACT_VERSION,
            scope: INSTRUMENTATION_SCOPE,
            scope_version: INSTRUMENTATION_SCOPE_VERSION,
            signal: Signal::LocalJobTrace.descriptor(),
            attributes: Attributes::default(),
            job: Some(JobSnapshot {
                family: JobFamily::Clone,
                enqueued_ns: 10,
                started_ns: 20,
                finished_ns: 70,
                outcome: Outcome::Ok,
                stages: vec![JobStage {
                    stage: Stage::Transfer,
                    component: ComponentKind::NetworkTransport,
                    component_role: ComponentRole::Transfer,
                    start_offset_ns: 0,
                    duration_ns: 50,
                    wait_duration_ns: 5,
                    wait_reason: Some(WaitReason::Network),
                    attempt: 1,
                    bytes: Some(128),
                    records: Some(2),
                    outcome: Outcome::Ok,
                }],
                handoffs: vec![ComponentHandoff {
                    from: ComponentKind::NetworkTransport,
                    to: ComponentKind::Storage,
                    kind: HandoffKind::Write,
                    duration_ns: 3,
                    wait_duration_ns: 1,
                    bytes: Some(128),
                    records: Some(2),
                }],
            }),
        };
        let first = otlp_json(SignalKind::Trace, &[&snapshot], 41);
        let second = otlp_json(SignalKind::Trace, &[&snapshot], 42);
        let spans = first["resourceSpans"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0]["traceId"].as_str().unwrap().len(), 32);
        assert_eq!(spans[0]["spanId"].as_str().unwrap().len(), 16);
        assert_eq!(spans[1]["spanId"].as_str().unwrap().len(), 16);
        assert_ne!(
            spans[0]["traceId"],
            second["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"]
        );
        let root_start = spans[0]["startTimeUnixNano"]
            .as_str()
            .unwrap()
            .parse::<u128>()
            .unwrap();
        let root_end = spans[0]["endTimeUnixNano"]
            .as_str()
            .unwrap()
            .parse::<u128>()
            .unwrap();
        let stage_start = spans[1]["startTimeUnixNano"]
            .as_str()
            .unwrap()
            .parse::<u128>()
            .unwrap();
        let stage_end = spans[1]["endTimeUnixNano"]
            .as_str()
            .unwrap()
            .parse::<u128>()
            .unwrap();
        assert_eq!(root_end - root_start, 60);
        assert_eq!(stage_start - root_start, 10);
        assert_eq!(stage_end - stage_start, 50);
        let rendered = first.to_string();
        for expected in [
            "graphforge.job.queue_delay_ns",
            "graphforge.workspace.component.kind",
            "graphforge.component.handoff",
            "network_transport",
        ] {
            assert!(rendered.contains(expected));
        }
        for forbidden in ["repository", "project", "path", "query", "token", "digest"] {
            assert!(!rendered.contains(forbidden));
        }
    }
}
