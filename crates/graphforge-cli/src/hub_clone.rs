//! Secure Hub discovery, download, and atomic portable-v2 import.

use clap::Args;
use fs4::{FileExt, TryLockError};
use graphforge_api::telemetry::{
    ComponentHandoff, ComponentKind, ComponentRole, Failure, HandoffKind, JobFamily, JobSnapshot,
    JobStage, OtlpConfig, Outcome, Stage, TelemetryConfig, TelemetryMode, TelemetryRuntime,
    WaitReason,
};
use graphforge_api::{
    DiscoveryPortableV2Error, DiscoveryPortableV2Mismatch, DiscoveryPortableV2Request,
    DiscoveryResearchVersionError, DiscoveryResearchVersionRequest, PortableV2ErrorCode,
    verify_discovered_portable_v2, verify_discovered_research_version,
};
use graphforge_api::{
    GraphForge, OperationId, PortableV2ImportRequest, PortableV2Limits, PortableV2Mode,
};
use graphforge_discovery::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ObjectDescriptor,
    RefSet, RepositoryIdentity, ResearchLineage,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use url::Url;

#[cfg(test)]
use crate::hub_http::fetch;
use crate::hub_http::{
    HttpResponse, HttpTransport, MAX_METADATA_BYTES, RETRY_POLICY, RetryPolicy, Transport,
    endpoint, fetch_once, fetch_with_attempts, hash_reader, limit_error, network, parse_input,
    parse_input_at, read_bounded, storage, validate_url, validation,
};

mod module_fetch;
pub(crate) use module_fetch::{ModuleFetchArgs, run_module_fetch};

/// Largest single object a clone downloads: the cumulative object bound
/// `gf publish` admits ([`DiscoveryLimits::default`]), so any object a
/// publisher could produce fits. The discovery manifest keeps that cumulative
/// bound across all objects; clone downloads only one of them.
pub(crate) const MAX_OBJECT_BYTES: u64 = 1024 * 1024_u64.pow(4);

const CLONE_CONTRACT: &str = "graphforge-hub-clone/1";

/// Clone a verified portable project from GraphForge Hub.
#[derive(Args)]
pub(crate) struct CloneArgs {
    /// Canonical owner/repository name or an HTTPS Hub repository URL.
    pub repository: String,
    /// New project directory; defaults to the repository name.
    pub destination: Option<PathBuf>,
    /// Explicit local OTLP collector base URL. Clone telemetry is otherwise disabled.
    #[arg(long)]
    pub telemetry_endpoint: Option<String>,
    /// Branch ref naming a research head when the Hub advertises `lineage`.
    #[arg(long = "ref")]
    pub git_ref: Option<String>,
    /// Immutable research Version UUID to clone instead of the Project package.
    #[arg(long)]
    pub version_uuid: Option<String>,
}

#[derive(Clone, Default)]
enum CloneClock {
    #[default]
    Monotonic,
    #[cfg(test)]
    Manual(std::rc::Rc<std::cell::Cell<Instant>>),
}

impl CloneClock {
    fn now(&self) -> Instant {
        match self {
            Self::Monotonic => Instant::now(),
            #[cfg(test)]
            Self::Manual(clock) => clock.get(),
        }
    }

    fn elapsed_since(&self, started: Instant) -> Duration {
        match self {
            Self::Monotonic => started.elapsed(),
            #[cfg(test)]
            Self::Manual(clock) => clock.get().duration_since(started),
        }
    }

    fn wait(&self, duration: Duration) {
        match self {
            Self::Monotonic => std::thread::sleep(duration),
            #[cfg(test)]
            Self::Manual(clock) => clock.set(clock.get() + duration),
        }
    }

    #[cfg(test)]
    fn manual() -> Self {
        Self::Manual(std::rc::Rc::new(std::cell::Cell::new(Instant::now())))
    }
}

struct CloneProfile<'a> {
    runtime: &'a TelemetryRuntime,
    clock: CloneClock,
    started: Option<Instant>,
    cursor_ns: u64,
    stages: Vec<JobStage>,
    handoffs: Vec<ComponentHandoff>,
}

impl<'a> CloneProfile<'a> {
    fn new(runtime: &'a TelemetryRuntime) -> Self {
        Self {
            runtime,
            clock: CloneClock::default(),
            started: None,
            cursor_ns: 0,
            stages: Vec::new(),
            handoffs: Vec::new(),
        }
    }

    fn handoff(
        &mut self,
        from: ComponentKind,
        to: ComponentKind,
        kind: HandoffKind,
        bytes: Option<u64>,
    ) {
        self.handoffs.push(ComponentHandoff {
            start_offset_ns: self.cursor_ns,
            from,
            to,
            kind,
            duration_ns: 0,
            wait_duration_ns: 0,
            bytes,
            records: None,
        });
    }

    fn stage<T>(
        &mut self,
        stage: Stage,
        component: ComponentKind,
        role: ComponentRole,
        wait_reason: Option<WaitReason>,
        attempt: u32,
        operation: impl FnOnce() -> Result<(T, Option<u64>, Option<u64>), graphforge_api::GfError>,
    ) -> Result<T, graphforge_api::GfError> {
        let origin = *self.started.get_or_insert_with(|| self.clock.now());
        let operation_start_ns =
            u64::try_from(self.clock.elapsed_since(origin).as_nanos()).unwrap_or(u64::MAX);
        if !self.stages.is_empty() && operation_start_ns > self.cursor_ns {
            let duration_ns = operation_start_ns - self.cursor_ns;
            self.stages.push(JobStage {
                stage: Stage::Orchestration,
                component: ComponentKind::Cli,
                component_role: ComponentRole::Coordination,
                start_offset_ns: self.cursor_ns,
                duration_ns,
                wait_duration_ns: 0,
                wait_reason: None,
                attempt: 1,
                bytes: None,
                resumed_bytes: None,
                records: None,
                outcome: Outcome::Ok,
            });
            self.cursor_ns = operation_start_ns;
        }
        let started = self.clock.now();
        let result = operation();
        let duration_ns = u64::try_from(self.clock.elapsed_since(started).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        let (bytes, records) = result
            .as_ref()
            .map_or((None, None), |(_, bytes, records)| (*bytes, *records));
        self.stages.push(JobStage {
            stage,
            component,
            component_role: role,
            start_offset_ns: self.cursor_ns,
            duration_ns,
            wait_duration_ns: wait_reason.map_or(0, |_| duration_ns),
            wait_reason,
            attempt,
            bytes,
            resumed_bytes: None,
            records,
            outcome: if result.is_ok() {
                Outcome::Ok
            } else {
                Outcome::Failed
            },
        });
        self.cursor_ns = self.cursor_ns.saturating_add(duration_ns);
        result.map(|(value, _, _)| value)
    }

    fn finish(mut self, result: &Result<(), graphforge_api::GfError>) {
        let elapsed_ns = self.started.map_or(0, |started| {
            u64::try_from(self.clock.elapsed_since(started).as_nanos()).unwrap_or(u64::MAX)
        });
        if elapsed_ns > self.cursor_ns {
            let duration_ns = elapsed_ns - self.cursor_ns;
            self.stages.push(JobStage {
                stage: Stage::Orchestration,
                component: ComponentKind::Cli,
                component_role: ComponentRole::Coordination,
                start_offset_ns: self.cursor_ns,
                duration_ns,
                wait_duration_ns: 0,
                wait_reason: None,
                attempt: 1,
                bytes: None,
                resumed_bytes: None,
                records: None,
                outcome: if result.is_ok() {
                    Outcome::Ok
                } else {
                    Outcome::Failed
                },
            });
            self.cursor_ns = elapsed_ns;
        }
        let failure = result.as_ref().err().map(classify_failure);
        let _ = self.runtime.record_job(JobSnapshot {
            family: JobFamily::Clone,
            enqueued_ns: 0,
            started_ns: 0,
            finished_ns: self.cursor_ns,
            outcome: if result.is_ok() {
                Outcome::Ok
            } else {
                Outcome::Failed
            },
            failure,
            stages: self.stages,
            handoffs: self.handoffs,
        });
    }
}

fn classify_failure(error: &graphforge_api::GfError) -> Failure {
    let detail = match error {
        graphforge_api::GfError::Validation(detail) | graphforge_api::GfError::Storage(detail) => {
            detail.as_str()
        }
        _ => return Failure::Internal,
    };
    let code = detail.split_once(':').map_or(detail, |(code, _)| code);
    match code {
        "hub.network" | "hub.unsafe_location" => Failure::Network,
        "hub.limit_exceeded" | "hub.package.limit_exceeded" => Failure::ResourceLimit,
        "hub.invalid_identity"
        | "hub.malformed_response"
        | "hub.unsupported_future"
        | "hub.missing_ref"
        | "hub.missing_object"
        | "hub.duplicate"
        | "hub.destination_conflict"
        | "hub.concurrent_clone"
        | "hub.interrupted"
        | "hub.package.cancelled"
        | "hub.package.unsupported_future"
        | "hub.package.incompatible"
        | "hub.integrity"
        | "hub.integrity_failure"
        | "hub.package.repository_mismatch"
        | "hub.package.immutable_version_mismatch"
        | "hub.package.package_digest_mismatch"
        | "hub.package.research_version_mismatch"
        | "hub.module.identity_mismatch"
        | "hub.module.content_digest_mismatch"
        | "hub.package.invalid_participant"
        | "hub.package.invalid_structure"
        | "hub.package.invalid_path"
        | "hub.package.duplicate_entry"
        | "hub.package.digest_mismatch"
        | "hub.package.concurrent_mutation" => Failure::InvalidInput,
        "hub.package.io" => Failure::Storage,
        _ if matches!(error, graphforge_api::GfError::Storage(_)) => Failure::Storage,
        _ => Failure::Internal,
    }
}

#[derive(Serialize, Deserialize)]
struct CloneResult {
    contract: String,
    repository: String,
    destination: String,
    immutable_version: String,
    package_digest: String,
    generation_uuid: String,
    resumed_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    research_version_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    research_version_kind: Option<String>,
}

struct ResearchCloneContext {
    lineage_bytes: Vec<u8>,
    version_uuid: String,
    version_kind: String,
    identity_digest: String,
    package_digest: String,
}

fn select_bundle(
    manifest: &DiscoveryManifest,
) -> Result<&ObjectDescriptor, graphforge_api::GfError> {
    manifest
        .package_object()
        .map_err(|error| protocol_error(&error))
}

fn fetch_inventory_object(
    transport: &dyn Transport,
    object: &ObjectDescriptor,
    max_bytes: usize,
    retry: &RetryPolicy,
) -> Result<Vec<u8>, graphforge_api::GfError> {
    let location = object
        .locations
        .first()
        .ok_or_else(|| validation("hub.missing_object", "inventory object has no location"))?;
    let url = Url::parse(location)
        .map_err(|_| validation("hub.unsafe_location", "inventory location is invalid"))?;
    validate_url(&url)?;
    let mut attempts = 0;
    let response = fetch_with_attempts(
        transport,
        &url,
        None,
        None,
        max_bytes as u64,
        &mut attempts,
        retry,
    )?;
    let bytes = read_bounded(response, max_bytes)?;
    let actual = hash_reader(&mut std::io::Cursor::new(&bytes))?;
    if actual != object.digest.0 {
        return Err(validation(
            "hub.integrity_failure",
            "inventory object digest mismatch",
        ));
    }
    Ok(bytes)
}

fn resolve_research_version_uuid(
    lineage: &ResearchLineage,
    git_ref: Option<&str>,
    version_uuid: Option<&str>,
) -> Result<String, graphforge_api::GfError> {
    if let Some(uuid) = version_uuid {
        if lineage.version(uuid).is_none() {
            return Err(validation(
                "hub.missing_object",
                "research Version is absent from lineage",
            ));
        }
        return Ok(uuid.to_owned());
    }
    let git_ref = git_ref.ok_or_else(|| {
        validation(
            "hub.missing_ref",
            "branch ref or version UUID is required for research clone",
        )
    })?;
    let branch = lineage
        .branches
        .iter()
        .find(|branch| branch.ref_name == git_ref)
        .ok_or_else(|| {
            validation(
                "hub.missing_ref",
                "branch ref is absent from research lineage",
            )
        })?;
    Ok(branch.head_version_uuid.clone())
}

fn prepare_research_clone(
    transport: &dyn Transport,
    manifest: &DiscoveryManifest,
    refs: &RefSet,
    limits: DiscoveryLimits,
    git_ref: Option<&str>,
    version_uuid: Option<&str>,
    retry: &RetryPolicy,
) -> Result<(ObjectDescriptor, ResearchCloneContext), graphforge_api::GfError> {
    let lineage_object = manifest
        .lineage_object()
        .map_err(|error| protocol_error(&error))?;
    let lineage_bytes =
        fetch_inventory_object(transport, lineage_object, limits.max_lineage_bytes, retry)?;
    let lineage = ResearchLineage::from_json(&lineage_bytes, limits)
        .map_err(|error| protocol_error(&error))?;
    manifest
        .bind_lineage(refs, &lineage)
        .map_err(|error| protocol_error(&error))?;
    let selected_uuid = resolve_research_version_uuid(&lineage, git_ref, version_uuid)?;
    let (version, object) = manifest
        .research_version_object(&lineage, &selected_uuid)
        .map_err(|error| protocol_error(&error))?;
    Ok((
        object.clone(),
        ResearchCloneContext {
            lineage_bytes,
            version_uuid: selected_uuid,
            version_kind: version.kind.clone(),
            identity_digest: version.identity_digest.0.clone(),
            package_digest: version
                .package
                .as_ref()
                .map(|package| package.package_digest.0.clone())
                .ok_or_else(|| {
                    validation(
                        "hub.missing_object",
                        "research Version package is not advertised",
                    )
                })?,
        },
    ))
}

fn staging_path(destination: &Path) -> Result<PathBuf, graphforge_api::GfError> {
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    let name = destination
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or_else(|| {
            validation(
                "hub.destination_conflict",
                "destination must have a UTF-8 name",
            )
        })?;
    Ok(parent.join(format!(".{name}.graphforge-clone")))
}

/// Owner-private staging beside the destination, held by an exclusive lock:
/// the download, its resume checkpoint, the import target `project/` with
/// the import's own residue, and the install record.
#[derive(Debug)]
pub(crate) struct CloneStaging {
    pub(crate) root: PathBuf,
    pub(crate) partial: PathBuf,
    project: PathBuf,
    operation: PathBuf,
    installed: PathBuf,
    lock: PathBuf,
    _lock: File,
}

impl CloneStaging {
    fn new(root: PathBuf, lock: File) -> Self {
        Self {
            partial: root.join("package.part"),
            project: root.join("project"),
            operation: root.join("operation"),
            installed: root.join("installed.json"),
            lock: root.join("clone.lock"),
            root,
            _lock: lock,
        }
    }

    fn lock_name(&self) -> std::ffi::OsString {
        file_name(&self.lock)
    }
}

#[cfg(unix)]
pub(crate) fn acquire_staging(destination: &Path) -> Result<CloneStaging, graphforge_api::GfError> {
    use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
    let root = staging_path(destination)?;
    match std::fs::symlink_metadata(&root) {
        Ok(m) if m.file_type().is_symlink() || !m.is_dir() => {
            return Err(validation(
                "hub.destination_conflict",
                "staging path is unsafe",
            ));
        }
        Ok(_) => std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .map_err(storage)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut b = std::fs::DirBuilder::new();
            b.mode(0o700);
            b.create(&root).map_err(storage)?;
        }
        Err(e) => return Err(storage(e)),
    }
    let lock_path = root.join("clone.lock");
    if std::fs::symlink_metadata(&lock_path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(validation(
            "hub.destination_conflict",
            "staging lock is unsafe",
        ));
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(lock_path)
        .map_err(storage)?;
    match FileExt::try_lock(&lock) {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            return Err(validation(
                "hub.concurrent_clone",
                "clone already in progress",
            ));
        }
        Err(TryLockError::Error(e)) => return Err(storage(e)),
    }
    Ok(CloneStaging::new(root, lock))
}

#[cfg(not(unix))]
pub(crate) fn acquire_staging(destination: &Path) -> Result<CloneStaging, graphforge_api::GfError> {
    let root = staging_path(destination)?;
    match std::fs::symlink_metadata(&root) {
        Ok(m) if !m.is_dir() => {
            return Err(validation(
                "hub.destination_conflict",
                "staging path is unsafe",
            ));
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(&root).map_err(storage)?
        }
        Err(e) => return Err(storage(e)),
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join("clone.lock"))
        .map_err(storage)?;
    match FileExt::try_lock(&lock) {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            return Err(validation(
                "hub.concurrent_clone",
                "clone already in progress",
            ));
        }
        Err(TryLockError::Error(e)) => return Err(storage(e)),
    }
    Ok(CloneStaging::new(root, lock))
}

#[derive(Serialize, Deserialize)]
struct ResumeState {
    digest: String,
    length: u64,
    location: String,
    etag: String,
}

fn save_resume(
    checkpoint: &Path,
    object: &ObjectDescriptor,
    location: &str,
    response: &HttpResponse,
) -> Result<(), graphforge_api::GfError> {
    let etag = response
        .etag
        .as_deref()
        .filter(|v| v.starts_with('"') && !v.starts_with("W/"))
        .ok_or_else(|| validation("hub.integrity", "object response requires a strong ETag"))?;
    let state = ResumeState {
        digest: object.digest.0.clone(),
        length: object.length,
        location: location.to_owned(),
        etag: etag.to_owned(),
    };
    reject_unsafe_state_path(checkpoint)?;
    let temporary = checkpoint.with_extension("resume.json.tmp");
    reject_unsafe_state_path(&temporary)?;
    let bytes = serde_json::to_vec(&state).map_err(storage)?;
    let mut file = open_private_checkpoint(&temporary)?;
    file.write_all(&bytes).map_err(storage)?;
    file.sync_all().map_err(storage)?;
    drop(file);
    std::fs::rename(&temporary, checkpoint).map_err(storage)?;
    if let Some(parent) = checkpoint.parent() {
        let _ = File::open(parent).and_then(|directory| directory.sync_all());
    }
    Ok(())
}

fn reject_unsafe_state_path(path: &Path) -> Result<(), graphforge_api::GfError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(validation(
                "hub.destination_conflict",
                "resume checkpoint path is unsafe",
            ))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage(error)),
    }
}

#[cfg(unix)]
fn open_private_checkpoint(path: &Path) -> Result<File, graphforge_api::GfError> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(storage)
}

#[cfg(not(unix))]
fn open_private_checkpoint(path: &Path) -> Result<File, graphforge_api::GfError> {
    reject_unsafe_state_path(path)?;
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(storage)
}

fn read_resume(checkpoint: &Path) -> Result<Option<ResumeState>, graphforge_api::GfError> {
    match std::fs::symlink_metadata(checkpoint) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(validation(
                "hub.destination_conflict",
                "resume checkpoint path is unsafe",
            ))
        }
        Ok(_) => {
            let file = open_read_nofollow(checkpoint).map_err(storage)?;
            let mut bytes = Vec::new();
            file.take(64 * 1024)
                .read_to_end(&mut bytes)
                .map_err(storage)?;
            Ok(serde_json::from_slice(&bytes).ok())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage(error)),
    }
}

#[cfg(test)]
fn download(
    transport: &dyn Transport,
    object: &ObjectDescriptor,
    partial: &Path,
) -> Result<DownloadReport, graphforge_api::GfError> {
    download_with_progress(
        transport,
        object,
        partial,
        &mut DownloadReport::default(),
        &mut DownloadControl::quiet(&TEST_RETRY_POLICY),
    )
}

/// Retry policy for in-process tests: production attempt count, no waiting.
#[cfg(test)]
const TEST_RETRY_POLICY: RetryPolicy = RetryPolicy {
    attempts: RETRY_POLICY.attempts,
    initial_backoff: Duration::ZERO,
    max_backoff: Duration::ZERO,
};

/// Caller-owned retry bound, cancellation, and byte progress for a download.
pub(crate) struct DownloadControl<'a> {
    pub(crate) retry: &'a RetryPolicy,
    pub(crate) cancelled: &'a AtomicBool,
    /// Called with the durable byte count and the object length.
    pub(crate) progress: Option<&'a mut dyn FnMut(u64, u64)>,
}

static NEVER_CANCELLED: AtomicBool = AtomicBool::new(false);

impl<'a> DownloadControl<'a> {
    /// No cancellation and no progress reporting.
    pub(crate) fn quiet(retry: &'a RetryPolicy) -> Self {
        Self {
            retry,
            cancelled: &NEVER_CANCELLED,
            progress: None,
        }
    }

    fn report(&mut self, bytes: u64, length: u64) {
        if let Some(progress) = self.progress.as_mut() {
            progress(bytes, length);
        }
    }
}

fn cancelled_error() -> graphforge_api::GfError {
    validation("hub.interrupted", "clone was cancelled; rerun to resume")
}

/// Download `object` into `partial`, resuming a previous invocation's bytes
/// and retrying transient failures in-process.
///
/// Each request after the first asks for the remaining bytes with `Range` and
/// the strong ETag the bytes on disk came from as `If-Range`. Only an exact
/// matching `206` appends; a `200` is the whole object again and replaces the
/// partial file, read to the object length. The complete file must match the
/// declared length and digest.
#[allow(clippy::too_many_lines)]
fn download_with_progress(
    transport: &dyn Transport,
    object: &ObjectDescriptor,
    partial: &Path,
    report: &mut DownloadReport,
    control: &mut DownloadControl<'_>,
) -> Result<DownloadReport, graphforge_api::GfError> {
    let checkpoint = partial.with_extension("resume.json");
    if object.length > MAX_OBJECT_BYTES {
        return Err(limit_error("object exceeds the clone byte bound"));
    }
    let mut offset = match std::fs::symlink_metadata(partial) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => metadata.len(),
        Ok(_) => {
            return Err(validation(
                "hub.destination_conflict",
                "resume path is not a regular file",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(storage(error)),
    };
    if offset > object.length {
        open_partial_nofollow(partial, false).map_err(storage)?;
        offset = 0;
    }
    let location = object
        .locations
        .first()
        .ok_or_else(|| validation("hub.missing_object", "portable bundle has no location"))?;
    let url = Url::parse(location)
        .map_err(|_| validation("hub.unsafe_location", "invalid object URL"))?;
    let saved = read_resume(&checkpoint)?;
    let mut validator = saved
        .filter(|s| {
            s.digest == object.digest.0 && s.length == object.length && s.location == *location
        })
        .map(|s| s.etag);
    if offset > 0 && validator.is_none() {
        offset = 0;
    }
    report.resumed_bytes = offset;
    let mut first_request = true;
    let mut failures = 0_u32;
    let mut high_water = offset;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    control.report(offset, object.length);
    while offset < object.length {
        if control.cancelled.load(Ordering::Relaxed) {
            return Err(cancelled_error());
        }
        let range = (offset > 0).then_some(offset);
        // The server may ignore `Range` and send the whole object, so the
        // transport bound is always the object length; the expected size is
        // applied once the status is known.
        let failure = match fetch_once(
            transport,
            &url,
            range,
            range.and(validator.as_deref()),
            object.length,
            &mut report.attempts,
        ) {
            Err(failure) if failure.transient => failure.error,
            Err(failure) => return Err(failure.error),
            Ok(mut response) => {
                let append = if range.is_some() && response.status == 206 {
                    let expected_range =
                        format!("bytes {offset}-{}/{}", object.length - 1, object.length);
                    if response.content_range.as_deref() != Some(expected_range.as_str())
                        || response
                            .etag
                            .as_deref()
                            .is_some_and(|etag| Some(etag) != validator.as_deref())
                    {
                        let _ = std::fs::remove_file(partial);
                        let _ = std::fs::remove_file(&checkpoint);
                        return Err(validation(
                            "hub.integrity",
                            "range response does not match the requested object",
                        ));
                    }
                    true
                } else if response.status == 206 {
                    return Err(validation(
                        "hub.integrity",
                        "unrequested range response for the object",
                    ));
                } else {
                    false
                };
                if !append {
                    save_resume(&checkpoint, object, location, &response)?;
                    validator.clone_from(&response.etag);
                    offset = 0;
                    if first_request {
                        report.resumed_bytes = 0;
                    }
                }
                first_request = false;
                let mut file = open_partial_nofollow(partial, append).map_err(storage)?;
                let mut interrupted = None;
                loop {
                    if control.cancelled.load(Ordering::Relaxed) {
                        file.sync_all().map_err(storage)?;
                        return Err(cancelled_error());
                    }
                    let Ok(read) = response.body.read(&mut buffer) else {
                        interrupted = Some(network("object read failed"));
                        break;
                    };
                    if read == 0 {
                        break;
                    }
                    let next = offset
                        .checked_add(read as u64)
                        .filter(|next| *next <= object.length)
                        .ok_or_else(|| limit_error("object exceeds declared size"))?;
                    file.write_all(&buffer[..read]).map_err(storage)?;
                    offset = next;
                    report.transferred_bytes = report.transferred_bytes.saturating_add(read as u64);
                    control.report(offset, object.length);
                }
                file.sync_all().map_err(storage)?;
                match interrupted {
                    Some(error) => error,
                    None if offset < object.length => {
                        validation("hub.interrupted", "download is incomplete; rerun to resume")
                    }
                    None => break,
                }
            }
        };
        if offset > high_water {
            high_water = offset;
            failures = 0;
        }
        failures += 1;
        if failures >= control.retry.attempts {
            return Err(if offset > 0 {
                validation("hub.interrupted", "download is incomplete; rerun to resume")
            } else {
                failure
            });
        }
        std::thread::sleep(control.retry.backoff(failures));
    }
    let mut file = open_read_nofollow(partial).map_err(storage)?;
    let length = file.metadata().map_err(storage)?.len();
    if length != object.length {
        return Err(validation(
            "hub.interrupted",
            "download is incomplete; rerun to resume",
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(storage)?;
    let actual = hash_reader(&mut file)?;
    if actual != object.digest.0 {
        let _ = std::fs::remove_file(partial);
        let _ = std::fs::remove_file(&checkpoint);
        return Err(validation("hub.integrity", "download digest mismatch"));
    }
    Ok(*report)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DownloadReport {
    resumed_bytes: u64,
    transferred_bytes: u64,
    attempts: u32,
}

#[cfg(unix)]
fn open_partial_nofollow(path: &Path, append: bool) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(unix)]
fn open_read_nofollow(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_read_nofollow(path: &Path) -> std::io::Result<File> {
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other("resume path is not a regular file"));
    }
    Ok(file)
}

fn ensure_destination_absent(destination: &Path) -> Result<(), graphforge_api::GfError> {
    match std::fs::symlink_metadata(destination) {
        Ok(_) => Err(validation(
            "hub.destination_conflict",
            "destination already exists",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage(error)),
    }
}

#[cfg(not(unix))]
fn open_partial_nofollow(path: &Path, append: bool) -> std::io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other("resume path is not a regular file"));
    }
    Ok(file)
}

pub(crate) fn run_clone(
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let runtime = clone_telemetry_runtime(args.telemetry_endpoint.as_deref());
    let attached = PROCESS_ATTACHED.load(Ordering::Relaxed);
    if attached {
        install_interrupt_handler();
    }
    let mut stderr = std::io::stderr();
    let mut env = CloneEnv {
        retry: RETRY_POLICY,
        cancelled: if attached {
            &INTERRUPTED
        } else {
            &NEVER_CANCELLED
        },
        available_space,
        progress: CloneProgress::new(attached.then_some(&mut stderr as &mut dyn Write)),
    };
    let result = run_clone_profiled_with_delays(
        &HttpTransport::new(),
        args,
        json,
        output,
        &runtime,
        &CloneDelays::default(),
        &mut env,
    );
    let _ = runtime.shutdown();
    result
}

/// Set by the native `gf` process only: an embedding host keeps its own
/// signal handling and standard error.
static PROCESS_ATTACHED: AtomicBool = AtomicBool::new(false);
/// Set by the first Ctrl-C during a clone.
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Let `gf clone` handle Ctrl-C and print byte progress on standard error.
/// Only the native `gf` process calls this.
pub(crate) fn attach_to_process() {
    PROCESS_ATTACHED.store(true, Ordering::Relaxed);
}

/// The first Ctrl-C asks the running download, verification, or import to
/// stop at its next cancellation check, which leaves a state a rerun resumes.
/// A second Ctrl-C exits at once; that is also resumable.
fn install_interrupt_handler() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        // A host that already owns SIGINT keeps it; the clone then simply
        // cannot be cancelled cooperatively.
        let _ = ctrlc::set_handler(|| {
            if INTERRUPTED.swap(true, Ordering::SeqCst) {
                std::process::exit(130);
            }
            let _ = writeln!(
                std::io::stderr(),
                "gf clone: cancelling; a rerun resumes. Press Ctrl-C again to stop now."
            );
        });
    });
}

fn available_space(path: &Path) -> std::io::Result<u64> {
    fs4::available_space(path)
}

/// Process-level effects of one clone: retry bound, cancellation, the free
/// space probe, and progress output. Tests inject each of them.
struct CloneEnv<'a> {
    retry: RetryPolicy,
    cancelled: &'a AtomicBool,
    available_space: fn(&Path) -> std::io::Result<u64>,
    progress: CloneProgress<'a>,
}

#[cfg(test)]
impl CloneEnv<'_> {
    fn quiet<'a>() -> CloneEnv<'a> {
        CloneEnv {
            retry: TEST_RETRY_POLICY,
            cancelled: &NEVER_CANCELLED,
            available_space,
            progress: CloneProgress::new(None),
        }
    }
}

/// At most one byte-progress line per interval, plus the final one.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

/// Human progress lines; writing them is best effort and never fails a clone.
struct CloneProgress<'a> {
    sink: Option<&'a mut dyn Write>,
    last: Option<Instant>,
}

impl<'a> CloneProgress<'a> {
    fn new(sink: Option<&'a mut dyn Write>) -> Self {
        Self { sink, last: None }
    }

    fn phase(&mut self, message: &str) {
        if let Some(sink) = self.sink.as_mut() {
            let _ = writeln!(sink, "gf clone: {message}");
        }
    }

    fn bytes(&mut self, done: u64, total: u64) {
        let Some(sink) = self.sink.as_mut() else {
            return;
        };
        let now = Instant::now();
        if done < total
            && self
                .last
                .is_some_and(|last| now.duration_since(last) < PROGRESS_INTERVAL)
        {
            return;
        }
        self.last = Some(now);
        let percent = if total == 0 {
            100
        } else {
            u128::from(done) * 100 / u128::from(total)
        };
        let _ = writeln!(
            sink,
            "gf clone: downloaded {} of {} ({percent}%)",
            human_bytes(done),
            human_bytes(total)
        );
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut unit = 0;
    let mut scale = 1_u64;
    while unit + 1 < UNITS.len() && bytes / scale >= 1024 {
        scale *= 1024;
        unit += 1;
    }
    if unit == 0 {
        return format!("{bytes} B");
    }
    // One decimal place, truncated, in exact integer arithmetic.
    let tenths = u128::from(bytes) * 10 / u128::from(scale);
    format!("{}.{} {}", tenths / 10, tenths % 10, UNITS[unit])
}

/// Free space a clone needs beside the destination: the rest of the download
/// plus the import's transient peak. The import was measured at 2.20-2.22x the
/// package (`observed_transient_peak_allocated_bytes`); 2.25x keeps a margin.
fn required_space(length: u64, downloaded: u64) -> u64 {
    let import_peak = (u128::from(length) * 9).div_ceil(4);
    u64::try_from(u128::from(length.saturating_sub(downloaded)) + import_peak).unwrap_or(u64::MAX)
}

fn check_free_space(
    staging: &CloneStaging,
    length: u64,
    available: fn(&Path) -> std::io::Result<u64>,
) -> Result<(), graphforge_api::GfError> {
    let downloaded = std::fs::symlink_metadata(&staging.partial)
        .ok()
        .filter(std::fs::Metadata::is_file)
        .map_or(0, |metadata| metadata.len().min(length));
    let required = required_space(length, downloaded);
    let available = available(&staging.root).map_err(storage)?;
    if available < required {
        return Err(graphforge_api::GfError::Storage(format!(
            "hub.insufficient_space: the destination filesystem has {} available; \
             this clone needs about {}",
            human_bytes(available),
            human_bytes(required)
        )));
    }
    Ok(())
}

fn clone_telemetry_runtime(endpoint: Option<&str>) -> TelemetryRuntime {
    let Some(endpoint) = endpoint else {
        return TelemetryRuntime::default();
    };
    TelemetryRuntime::new(TelemetryConfig {
        mode: TelemetryMode::OtlpHttpJson,
        otlp: Some(OtlpConfig {
            endpoint: endpoint.to_owned(),
            headers: BTreeMap::default(),
        }),
        ..TelemetryConfig::default()
    })
    .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn run_clone_with(
    transport: &dyn Transport,
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    run_clone_profiled(transport, args, json, output, &TelemetryRuntime::default())
}

#[cfg(test)]
fn run_clone_profiled(
    transport: &dyn Transport,
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
    runtime: &TelemetryRuntime,
) -> Result<(), graphforge_api::GfError> {
    run_clone_profiled_with_delays(
        transport,
        args,
        json,
        output,
        runtime,
        &CloneDelays::default(),
        &mut CloneEnv::quiet(),
    )
}

#[derive(Default)]
struct CloneDelays {
    clock: CloneClock,
    verification: Duration,
    import: Duration,
    reopen: Duration,
    /// Runs immediately before the import, after verification.
    #[cfg(test)]
    before_import: Option<Box<dyn Fn()>>,
}

fn run_clone_profiled_with_delays(
    transport: &dyn Transport,
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
    runtime: &TelemetryRuntime,
    delays: &CloneDelays,
    env: &mut CloneEnv<'_>,
) -> Result<(), graphforge_api::GfError> {
    let mut profile = CloneProfile::new(runtime);
    #[cfg(test)]
    {
        profile.clock = delays.clock.clone();
    }
    let result = run_clone_job(transport, args, &mut profile, delays, env)
        .and_then(|result| write_clone_result(&result, json, output));
    profile.finish(&result);
    result
}

/// Test-only process exit at a named clone phase, for kill-and-rerun tests.
#[cfg(test)]
fn clone_failpoint(name: &str) {
    if std::env::var("GRAPHFORGE_CLONE_FAILPOINT").as_deref() == Ok(name) {
        std::process::exit(CLONE_FAILPOINT_EXIT);
    }
}

#[cfg(test)]
const CLONE_FAILPOINT_EXIT: i32 = 86;

#[cfg(not(test))]
const fn clone_failpoint(_name: &str) {}

/// What one completed clone left in its staging directory before the
/// destination was installed, so a rerun after a crash can finish it.
#[derive(Serialize, Deserialize)]
struct InstallRecord {
    generation_uuid: String,
    result: CloneResult,
}

/// Finish a clone whose destination was installed but whose staging was not
/// yet removed: the rerun reports the recorded result and removes the staging.
fn finish_installed_clone(
    destination: &Path,
) -> Result<Option<CloneResult>, graphforge_api::GfError> {
    let root = staging_path(destination)?;
    match std::fs::symlink_metadata(&root) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        _ => return Ok(None),
    }
    let staging = acquire_staging(destination)?;
    if std::fs::symlink_metadata(&staging.project).is_ok() {
        return Ok(None);
    }
    let Some(record) = read_install_record(&staging.installed)? else {
        return Ok(None);
    };
    let installed = destination
        .to_str()
        .and_then(|path| GraphForge::new(Some(path)).ok())
        .and_then(|graph| graph.committed_generation_identity().ok())
        .map(|identity| identity.generation_uuid.to_string());
    if installed.as_deref() != Some(record.generation_uuid.as_str()) {
        return Ok(None);
    }
    release_staging(staging);
    Ok(Some(record.result))
}

fn read_install_record(path: &Path) -> Result<Option<InstallRecord>, graphforge_api::GfError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            let mut bytes = Vec::new();
            open_read_nofollow(path)
                .map_err(storage)?
                .take(64 * 1024)
                .read_to_end(&mut bytes)
                .map_err(storage)?;
            Ok(serde_json::from_slice(&bytes).ok())
        }
        Ok(_) => Err(validation(
            "hub.destination_conflict",
            "install record path is unsafe",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage(error)),
    }
}

/// Write `bytes` to `path` in the staging directory and make it durable.
fn write_staging_file(path: &Path, bytes: &[u8]) -> Result<(), graphforge_api::GfError> {
    reject_unsafe_state_path(path)?;
    let temporary = path.with_extension("tmp");
    reject_unsafe_state_path(&temporary)?;
    let mut file = open_private_checkpoint(&temporary)?;
    file.write_all(bytes).map_err(storage)?;
    file.sync_all().map_err(storage)?;
    drop(file);
    std::fs::rename(&temporary, path).map_err(storage)?;
    if let Some(parent) = path.parent() {
        let _ = File::open(parent).and_then(|directory| directory.sync_all());
    }
    Ok(())
}

/// Bind the staged import target to `operation`. A target staged for another
/// operation (the Hub moved to a newer version since the previous run) is
/// removed with its import residue, so the import starts from an empty
/// target; the same operation resumes or replays in place.
fn bind_import_target(
    staging: &CloneStaging,
    operation: &OperationId,
) -> Result<(), graphforge_api::GfError> {
    let expected = operation.0.hyphenated().to_string();
    let recorded = match std::fs::symlink_metadata(&staging.operation) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            let mut bytes = Vec::new();
            open_read_nofollow(&staging.operation)
                .map_err(storage)?
                .take(128)
                .read_to_end(&mut bytes)
                .map_err(storage)?;
            Some(bytes)
        }
        Ok(_) => {
            return Err(validation(
                "hub.destination_conflict",
                "staged operation path is unsafe",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(storage(error)),
    };
    if recorded.as_deref() == Some(expected.as_bytes()) {
        return Ok(());
    }
    clear_import_target(staging)?;
    write_staging_file(&staging.operation, expected.as_bytes())
}

/// Remove the staged import target with the import's residue and the
/// operation binding; the download and its checkpoint stay.
fn clear_import_target(staging: &CloneStaging) -> Result<(), graphforge_api::GfError> {
    let keep = [
        staging.lock_name(),
        file_name(&staging.partial),
        file_name(&staging.partial.with_extension("resume.json")),
    ];
    for entry in std::fs::read_dir(&staging.root).map_err(storage)? {
        let entry = entry.map_err(storage)?;
        if keep.iter().any(|name| *name == entry.file_name()) {
            continue;
        }
        let path = entry.path();
        if entry.file_type().map_err(storage)?.is_dir() {
            std::fs::remove_dir_all(&path).map_err(storage)?;
        } else {
            std::fs::remove_file(&path).map_err(storage)?;
        }
    }
    Ok(())
}

fn file_name(path: &Path) -> std::ffi::OsString {
    path.file_name().map(ToOwned::to_owned).unwrap_or_default()
}

/// Atomically install the imported project at `destination`; an existing
/// destination is never replaced.
fn install_destination(
    staging: &CloneStaging,
    destination: &Path,
) -> Result<(), graphforge_api::GfError> {
    match graphforge_filesystem::rename_no_replace(&staging.project, destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(validation(
                "hub.destination_conflict",
                "destination already exists",
            ));
        }
        Err(error) => return Err(storage(error)),
    }
    if let Some(parent) = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let _ = File::open(parent).and_then(|directory| directory.sync_all());
    }
    Ok(())
}

fn import_error(error: &graphforge_api::PortableV2Error) -> graphforge_api::GfError {
    let code = portable_code(error.code);
    let mut message = format!("{code}: portable project import failed: {error}");
    if let Some(cause) = &error.cause {
        // Sanitized: host paths are reduced to their last two components.
        let _ = write!(message, " (cause: {cause})");
    }
    match error.code {
        PortableV2ErrorCode::Io | PortableV2ErrorCode::Cancelled => {
            graphforge_api::GfError::Storage(message)
        }
        _ => graphforge_api::GfError::Validation(message),
    }
}

fn portable_code(code: PortableV2ErrorCode) -> &'static str {
    match code {
        PortableV2ErrorCode::Cancelled => "hub.package.cancelled",
        PortableV2ErrorCode::LimitExceeded => "hub.package.limit_exceeded",
        PortableV2ErrorCode::Io => "hub.package.io",
        PortableV2ErrorCode::InvalidStructure => "hub.package.invalid_structure",
        PortableV2ErrorCode::InvalidPath => "hub.package.invalid_path",
        PortableV2ErrorCode::DuplicateEntry => "hub.package.duplicate_entry",
        PortableV2ErrorCode::UnsupportedFuture => "hub.package.unsupported_future",
        PortableV2ErrorCode::Incompatible => "hub.package.incompatible",
        PortableV2ErrorCode::DigestMismatch => "hub.package.digest_mismatch",
        PortableV2ErrorCode::ConcurrentMutation => "hub.package.concurrent_mutation",
    }
}

/// Remove a staging directory that holds nothing a rerun can reuse.
fn release_staging_if_unused(staging: CloneStaging) {
    let reusable = std::fs::read_dir(&staging.root).is_ok_and(|entries| {
        entries
            .flatten()
            .any(|entry| entry.file_name() != staging.lock_name())
    });
    if reusable {
        drop(staging);
    } else {
        release_staging(staging);
    }
}

/// Remove everything staged, on success and on failure alike.
///
/// The contents (partial file, checkpoint, staged output, and the lock file
/// itself) are removed while the lock is still held, so a concurrent run can
/// never acquire the directory and then have its files deleted. Only then is
/// the lock released and the now-empty directory removed with `remove_dir`,
/// which fails harmlessly if a concurrent run has meanwhile re-created its
/// lock; that run's staging is never touched.
pub(crate) fn release_staging(staging: CloneStaging) {
    clear_staging(&staging);
    let root = staging.root.clone();
    drop(staging);
    remove_staging_dir(&root);
}

/// `remove_dir`, not `remove_dir_all`: it refuses a directory that a concurrent
/// run has re-populated, and that refusal is expected and benign.
fn remove_staging_dir(root: &Path) {
    let _ = std::fs::remove_dir(root);
}

/// Best-effort removal of every entry in the staging directory. Cleanup can
/// neither turn a published success into a failure nor mask the real error.
fn clear_staging(staging: &CloneStaging) {
    let Ok(entries) = std::fs::read_dir(&staging.root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let _ = match entry.file_type() {
            Ok(kind) if kind.is_dir() => std::fs::remove_dir_all(&path),
            _ => std::fs::remove_file(&path),
        };
    }
}

#[allow(clippy::too_many_lines)]
fn run_clone_job(
    transport: &dyn Transport,
    mut args: CloneArgs,
    profile: &mut CloneProfile<'_>,
    delays: &CloneDelays,
    env: &mut CloneEnv<'_>,
) -> Result<CloneResult, graphforge_api::GfError> {
    if args.git_ref.is_some() && args.version_uuid.is_some() {
        return Err(validation(
            "hub.invalid_identity",
            "specify only one of --ref and --version-uuid",
        ));
    }
    let requested_destination = args.destination.take();
    let (identity, base, destination, finished) = profile.stage(
        Stage::IdentityValidation,
        ComponentKind::Cli,
        ComponentRole::Facade,
        None,
        1,
        || {
            let (identity, base) = parse_input(&args.repository)?;
            let destination =
                requested_destination.unwrap_or_else(|| PathBuf::from(&identity.repository));
            if std::fs::symlink_metadata(&destination).is_ok() {
                let finished = finish_installed_clone(&destination)?.ok_or_else(|| {
                    validation("hub.destination_conflict", "destination already exists")
                })?;
                return Ok(((identity, base, destination, Some(finished)), None, None));
            }
            ensure_destination_absent(&destination)?;
            // Refuse an inadmissible destination filesystem with its real
            // code before any network request or download.
            graphforge_api::filesystem_durability_preflight(&destination)?;
            Ok(((identity, base, destination, None), None, None))
        },
    )?;
    if let Some(finished) = finished {
        return Ok(finished);
    }
    profile.handoff(
        ComponentKind::Cli,
        ComponentKind::NetworkTransport,
        HandoffKind::Call,
        None,
    );
    let mut refs_attempts = 0;
    let refs_result = profile.stage(
        Stage::RefsDiscovery,
        ComponentKind::NetworkTransport,
        ComponentRole::Transfer,
        Some(WaitReason::Network),
        1,
        || {
            let bytes = read_bounded(
                fetch_with_attempts(
                    transport,
                    &endpoint(&base, "refs"),
                    None,
                    None,
                    MAX_METADATA_BYTES as u64,
                    &mut refs_attempts,
                    &env.retry,
                )?,
                MAX_METADATA_BYTES,
            )?;
            let length = bytes.len() as u64;
            Ok((bytes, Some(length), None))
        },
    );
    if let Some(stage) = profile.stages.last_mut() {
        stage.attempt = refs_attempts.max(1);
    }
    let refs_bytes = refs_result?;
    let mut manifest_attempts = 0;
    let manifest_result = profile.stage(
        Stage::ManifestDiscovery,
        ComponentKind::NetworkTransport,
        ComponentRole::Transfer,
        Some(WaitReason::Network),
        1,
        || {
            let bytes = read_bounded(
                fetch_with_attempts(
                    transport,
                    &endpoint(&base, "manifest"),
                    None,
                    None,
                    MAX_METADATA_BYTES as u64,
                    &mut manifest_attempts,
                    &env.retry,
                )?,
                MAX_METADATA_BYTES,
            )?;
            let length = bytes.len() as u64;
            Ok((bytes, Some(length), None))
        },
    );
    if let Some(stage) = profile.stages.last_mut() {
        stage.attempt = manifest_attempts.max(1);
    }
    let manifest_bytes = manifest_result?;
    let limits = DiscoveryLimits {
        max_response_bytes: MAX_METADATA_BYTES,
        ..DiscoveryLimits::default()
    };
    profile.handoff(
        ComponentKind::NetworkTransport,
        ComponentKind::Discovery,
        HandoffKind::Return,
        Some((refs_bytes.len() + manifest_bytes.len()) as u64),
    );
    let (manifest, staging) = profile.stage(
        Stage::ManifestDiscovery,
        ComponentKind::Discovery,
        ComponentRole::Verification,
        None,
        1,
        || {
            let refs =
                RefSet::from_json(&refs_bytes, limits).map_err(|error| protocol_error(&error))?;
            let manifest = DiscoveryManifest::from_json(&manifest_bytes, limits)
                .map_err(|error| protocol_error(&error))?;
            if refs.repository != identity || manifest.repository != identity {
                return Err(validation("hub.integrity", "discovery repository mismatch"));
            }
            refs.validate_manifest(&manifest)
                .map_err(|error| protocol_error(&error))?;
            let staging = acquire_staging(&destination)?;
            Ok(((manifest, staging), None, None))
        },
    )?;
    let discovered = Discovered {
        identity: &identity,
        destination: &destination,
        refs_bytes: &refs_bytes,
        manifest_bytes: &manifest_bytes,
        manifest: &manifest,
        limits,
    };
    let result = clone_into_staging(
        transport,
        &args,
        &discovered,
        &staging,
        profile,
        delays,
        env,
    );
    match result {
        Ok(result) => {
            // The destination is already atomically installed; cleanup cannot
            // turn success into a reported failure.
            profile.stage(
                Stage::Cleanup,
                ComponentKind::Storage,
                ComponentRole::Persistence,
                None,
                1,
                || {
                    release_staging(staging);
                    Ok(((), None, None))
                },
            )?;
            Ok(result)
        }
        Err(error) => {
            release_staging_if_unused(staging);
            Err(error)
        }
    }
}

/// Validated discovery state shared by the staged clone phases.
struct Discovered<'a> {
    identity: &'a RepositoryIdentity,
    destination: &'a Path,
    refs_bytes: &'a [u8],
    manifest_bytes: &'a [u8],
    manifest: &'a DiscoveryManifest,
    limits: DiscoveryLimits,
}

#[allow(clippy::too_many_lines)]
fn clone_into_staging(
    transport: &dyn Transport,
    args: &CloneArgs,
    discovered: &Discovered<'_>,
    staging: &CloneStaging,
    profile: &mut CloneProfile<'_>,
    delays: &CloneDelays,
    env: &mut CloneEnv<'_>,
) -> Result<CloneResult, graphforge_api::GfError> {
    let Discovered {
        identity,
        destination,
        refs_bytes,
        manifest_bytes,
        manifest,
        limits,
    } = *discovered;
    let refs = RefSet::from_json(refs_bytes, limits).map_err(|error| protocol_error(&error))?;
    let research_requested = args.git_ref.is_some() || args.version_uuid.is_some();
    let (object, research_context) = if research_requested {
        if manifest.lineage.is_none() {
            return Err(validation(
                "hub.missing_object",
                "research clone requires an advertised lineage document",
            ));
        }
        profile.handoff(
            ComponentKind::Discovery,
            ComponentKind::NetworkTransport,
            HandoffKind::Call,
            None,
        );
        let (object, context) = prepare_research_clone(
            transport,
            manifest,
            &refs,
            limits,
            args.git_ref.as_deref(),
            args.version_uuid.as_deref(),
            &env.retry,
        )?;
        profile.handoff(
            ComponentKind::NetworkTransport,
            ComponentKind::Discovery,
            HandoffKind::Return,
            Some(context.lineage_bytes.len() as u64),
        );
        (object, Some(context))
    } else {
        (select_bundle(manifest)?.clone(), None)
    };
    if object.length > MAX_OBJECT_BYTES {
        return Err(limit_error("object exceeds the clone byte bound"));
    }
    check_free_space(staging, object.length, env.available_space)?;
    let partial = staging.partial.clone();
    profile.handoff(
        ComponentKind::Discovery,
        ComponentKind::NetworkTransport,
        HandoffKind::Call,
        None,
    );
    let mut download_progress = DownloadReport::default();
    let download_result = profile.stage(
        Stage::Download,
        ComponentKind::NetworkTransport,
        ComponentRole::Transfer,
        Some(WaitReason::Network),
        1,
        || {
            let CloneEnv {
                retry,
                cancelled,
                progress,
                ..
            } = env;
            let mut report_bytes = |done, total| progress.bytes(done, total);
            let report = download_with_progress(
                transport,
                &object,
                &partial,
                &mut download_progress,
                &mut DownloadControl {
                    retry,
                    cancelled,
                    progress: Some(&mut report_bytes),
                },
            )?;
            Ok((report, Some(report.transferred_bytes), None))
        },
    );
    if let Some(stage) = profile.stages.last_mut() {
        stage.attempt = download_progress.attempts.max(1);
        stage.bytes = Some(download_progress.transferred_bytes);
        stage.resumed_bytes = Some(download_progress.resumed_bytes);
    }
    let download = download_result?;
    clone_failpoint("clone.after_download");
    let portable_limits = PortableV2Limits::default();
    profile.handoff(
        ComponentKind::NetworkTransport,
        ComponentKind::PortableVerify,
        HandoffKind::Transfer,
        Some(download.resumed_bytes + download.transferred_bytes),
    );
    env.progress.phase("verifying the package");
    let cancelled = env.cancelled;
    let verified = profile.stage(
        Stage::PortableVerification,
        ComponentKind::PortableVerify,
        ComponentRole::Verification,
        None,
        1,
        || {
            delays.clock.wait(delays.verification);
            let outcome = if let Some(context) = &research_context {
                verify_discovered_research_version(&DiscoveryResearchVersionRequest {
                    manifest_json: manifest_bytes,
                    refs_json: refs_bytes,
                    lineage_json: &context.lineage_bytes,
                    expected_repository: identity,
                    version_uuid: &context.version_uuid,
                    package: &partial,
                    discovery_limits: limits,
                    portable_limits,
                    mode: PortableV2Mode::Full,
                    cancelled: Some(cancelled),
                    scratch: Some(&staging.root),
                })
                .map_err(research_version_error)?;
                (
                    manifest.immutable_version.0.clone(),
                    Some(context.version_uuid.clone()),
                    Some(context.version_kind.clone()),
                )
            } else {
                let accepted = verify_discovered_portable_v2(&DiscoveryPortableV2Request {
                    manifest_json: manifest_bytes,
                    refs_json: refs_bytes,
                    expected_repository: identity,
                    package: &partial,
                    discovery_limits: limits,
                    portable_limits,
                    mode: PortableV2Mode::Full,
                    cancelled: Some(cancelled),
                })
                .map_err(portable_error)?;
                (accepted.immutable_version, None, None)
            };
            Ok((outcome, Some(object.length), None))
        },
    )?;
    let (immutable_version, research_version_uuid, research_version_kind) = verified;
    // A Project-package clone keeps its historical derivation. A research clone
    // binds the selected Version too, so different Versions of one snapshot get
    // distinct import operations and therefore distinct generation identities.
    let operation_id = OperationId(match &research_context {
        None => graphforge_api::hub_clone_operation(&canonical_name(identity), &immutable_version),
        Some(context) => graphforge_api::hub_research_clone_operation(
            &canonical_name(identity),
            &immutable_version,
            &context.version_uuid,
            &context.identity_digest,
        ),
    });
    profile.handoff(
        ComponentKind::PortableVerify,
        ComponentKind::Api,
        HandoffKind::Call,
        Some(object.length),
    );
    profile.handoff(
        ComponentKind::Api,
        ComponentKind::PortableImport,
        HandoffKind::Call,
        Some(object.length),
    );
    env.progress.phase("importing the project");
    let imported = profile.stage(
        Stage::AtomicImport,
        ComponentKind::PortableImport,
        ComponentRole::Persistence,
        None,
        1,
        || {
            delays.clock.wait(delays.import);
            #[cfg(test)]
            if let Some(hook) = &delays.before_import {
                hook();
            }
            // The import targets a private directory inside the staging
            // directory; only a complete, reopened project is installed at
            // the destination, by one atomic rename.
            bind_import_target(staging, &operation_id)?;
            GraphForge::import_portable_v2(
                &staging.project,
                &PortableV2ImportRequest {
                    input: partial.clone(),
                    operation_id,
                    limits: portable_limits,
                },
                Some(cancelled),
            )
            .map_err(|error| {
                // An import that did not commit leaves nothing worth
                // keeping; the rerun starts from an empty target. A committed
                // one stays, and the rerun replays it.
                if error.committed_import.is_none()
                    && let Err(cleanup) = clear_import_target(staging)
                {
                    return graphforge_api::GfError::Storage(format!(
                        "{}: portable project import failed ({error}), and removing \
                         its staged target failed: {cleanup}",
                        portable_code(error.code)
                    ));
                }
                import_error(&error)
            })
            .map(|imported| (imported, Some(object.length), None))
        },
    )?;
    if let Some(context) = &research_context
        && imported.package_digest != context.package_digest
    {
        return Err(validation(
            "hub.package.package_digest_mismatch",
            "imported research package differs from the verified Version package",
        ));
    }
    profile.handoff(
        ComponentKind::PortableImport,
        ComponentKind::Storage,
        HandoffKind::Write,
        Some(object.length),
    );
    profile.handoff(
        ComponentKind::Storage,
        ComponentKind::Publication,
        HandoffKind::Write,
        Some(object.length),
    );
    profile.handoff(
        ComponentKind::Publication,
        ComponentKind::Api,
        HandoffKind::Return,
        None,
    );
    profile.handoff(
        ComponentKind::Api,
        ComponentKind::Recovery,
        HandoffKind::Return,
        None,
    );
    // `import_portable_v2` already reopened the imported project through the
    // public facade and verified the reopened generation UUID matches the
    // published receipt (see `import_portable_v2_with_allocation`), including
    // its own UTF-8 path check — success there already proves normal runtime
    // readability. The stage itself is kept (with its test-only injected
    // delay) so clone job telemetry keeps reporting a distinct
    // recovery/verification phase.
    profile.stage(
        Stage::Reopen,
        ComponentKind::Recovery,
        ComponentRole::Verification,
        None,
        1,
        || {
            delays.clock.wait(delays.reopen);
            Ok(((), None, None))
        },
    )?;
    profile.handoff(
        ComponentKind::Recovery,
        ComponentKind::Storage,
        HandoffKind::Call,
        None,
    );
    let result = CloneResult {
        contract: CLONE_CONTRACT.to_owned(),
        repository: canonical_name(identity),
        destination: destination.display().to_string(),
        immutable_version,
        package_digest: imported.package_digest,
        generation_uuid: imported.generation_uuid.to_string(),
        resumed_bytes: download.resumed_bytes,
        research_version_uuid,
        research_version_kind,
    };
    let record = InstallRecord {
        generation_uuid: result.generation_uuid.clone(),
        result,
    };
    write_staging_file(
        &staging.installed,
        &serde_json::to_vec(&record).map_err(storage)?,
    )?;
    clone_failpoint("clone.before_install");
    install_destination(staging, destination)?;
    clone_failpoint("clone.after_install");
    Ok(record.result)
}

fn write_clone_result(
    result: &CloneResult,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if json {
        serde_json::to_writer(&mut *output, &result)
            .map_err(|e| graphforge_api::GfError::Execution(e.to_string()))?;
        writeln!(output).map_err(storage)?;
    } else {
        writeln!(
            output,
            "Cloned {} to {}",
            result.repository, result.destination
        )
        .map_err(storage)?;
    }
    Ok(())
}

fn canonical_name(identity: &RepositoryIdentity) -> String {
    identity.canonical_name()
}
fn protocol_error(error: &DiscoveryError) -> graphforge_api::GfError {
    let code = match error.code {
        DiscoveryErrorCode::InvalidIdentity => "hub.invalid_identity",
        DiscoveryErrorCode::MalformedResponse => "hub.malformed_response",
        DiscoveryErrorCode::UnsupportedFuture => "hub.unsupported_future",
        DiscoveryErrorCode::MissingRef => "hub.missing_ref",
        DiscoveryErrorCode::MissingObject => "hub.missing_object",
        DiscoveryErrorCode::IntegrityFailure => "hub.integrity_failure",
        DiscoveryErrorCode::UnsafeLocation => "hub.unsafe_location",
        DiscoveryErrorCode::LimitExceeded => "hub.limit_exceeded",
        DiscoveryErrorCode::Duplicate => "hub.duplicate",
    };
    validation(code, error.detail())
}
fn research_version_error(error: DiscoveryResearchVersionError) -> graphforge_api::GfError {
    match error {
        DiscoveryResearchVersionError::Discovery(error) => protocol_error(&error),
        DiscoveryResearchVersionError::ReferenceMismatch(mismatch) => validation(
            match mismatch {
                DiscoveryPortableV2Mismatch::Repository => "hub.package.repository_mismatch",
                DiscoveryPortableV2Mismatch::ImmutableVersion => {
                    "hub.package.immutable_version_mismatch"
                }
                DiscoveryPortableV2Mismatch::PackageDigest => "hub.package.package_digest_mismatch",
                DiscoveryPortableV2Mismatch::ModuleIdentity => "hub.module.identity_mismatch",
                DiscoveryPortableV2Mismatch::ModuleContentDigest => {
                    "hub.module.content_digest_mismatch"
                }
                DiscoveryPortableV2Mismatch::ResearchVersionIdentity => {
                    "hub.package.research_version_mismatch"
                }
            },
            "research discovery reference mismatch",
        ),
        DiscoveryResearchVersionError::Portable(error) => validation(
            portable_code(error.code),
            "research portable verification failed",
        ),
    }
}

fn portable_error(error: DiscoveryPortableV2Error) -> graphforge_api::GfError {
    match error {
        DiscoveryPortableV2Error::Discovery(error) => protocol_error(&error),
        DiscoveryPortableV2Error::ReferenceMismatch(mismatch) => validation(
            match mismatch {
                DiscoveryPortableV2Mismatch::Repository => "hub.package.repository_mismatch",
                DiscoveryPortableV2Mismatch::ImmutableVersion => {
                    "hub.package.immutable_version_mismatch"
                }
                DiscoveryPortableV2Mismatch::PackageDigest => "hub.package.package_digest_mismatch",
                DiscoveryPortableV2Mismatch::ModuleIdentity => "hub.module.identity_mismatch",
                DiscoveryPortableV2Mismatch::ModuleContentDigest => {
                    "hub.module.content_digest_mismatch"
                }
                DiscoveryPortableV2Mismatch::ResearchVersionIdentity => {
                    "hub.package.research_version_mismatch"
                }
            },
            "portable discovery reference mismatch",
        ),
        DiscoveryPortableV2Error::Participant { .. } => validation(
            "hub.package.invalid_participant",
            "portable project participant is invalid",
        ),
        DiscoveryPortableV2Error::Portable(error) => validation(
            portable_code(error.code),
            "portable project verification failed",
        ),
    }
}
#[cfg(test)]
mod research_clone_tests;
#[cfg(test)]
mod transfer_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub_http::public_ip;
    use crate::hub_http::{ReadTimeouts, read_agent};
    use std::collections::VecDeque;
    use std::io::{BufRead as _, BufReader};
    use std::net::IpAddr;
    use std::net::TcpListener;
    use std::sync::Mutex;
    use ureq::unversioned::resolver::DefaultResolver;

    pub(super) struct Scripted(Mutex<VecDeque<HttpResponse>>);

    impl Scripted {
        pub(super) fn new(responses: Vec<HttpResponse>) -> Self {
            Self(Mutex::new(responses.into()))
        }

        pub(super) fn remaining(&self) -> usize {
            self.0.lock().unwrap().len()
        }

        fn replace_last(&self, response: HttpResponse) {
            let mut responses = self.0.lock().unwrap();
            responses.pop_back().unwrap();
            responses.push_back(response);
        }
    }

    impl Transport for Scripted {
        /// Like [`HttpTransport`], read at most one byte past `limit`; an
        /// exhausted script is a server that refuses connections.
        fn get(
            &self,
            _url: &Url,
            _range: Option<u64>,
            _if_range: Option<&str>,
            limit: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            let mut response = self
                .0
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| network("request failed"))?;
            response.body = Box::new(response.body.take(limit.saturating_add(1)));
            Ok(response)
        }
    }

    struct DelayedTransport {
        inner: Scripted,
        clock: CloneClock,
        discovery: Duration,
        download: Duration,
    }

    impl Transport for DelayedTransport {
        fn get(
            &self,
            url: &Url,
            range: Option<u64>,
            if_range: Option<&str>,
            limit: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            let delay = if url.path().contains("/.gf/objects/")
                || std::path::Path::new(url.path())
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("gfpb"))
            {
                self.download
            } else {
                self.discovery
            };
            self.clock.wait(delay);
            self.inner.get(url, range, if_range, limit)
        }
    }

    pub(super) struct LoopbackTransport(HttpTransport);

    impl LoopbackTransport {
        pub(super) fn new() -> Self {
            Self::with_idle(Duration::from_secs(2))
        }

        /// The production read agent over plain loopback HTTP, with `idle`
        /// as every phase bound.
        pub(super) fn with_idle(idle: Duration) -> Self {
            Self(HttpTransport {
                agent: read_agent(
                    false,
                    &ReadTimeouts {
                        connect: idle,
                        response: idle,
                        idle,
                    },
                    DefaultResolver::default(),
                ),
            })
        }
    }

    impl Transport for LoopbackTransport {
        fn validate(&self, url: &Url) -> Result<(), graphforge_api::GfError> {
            if url.scheme() == "http"
                && url
                    .host_str()
                    .and_then(|host| host.parse::<IpAddr>().ok())
                    .is_some_and(|address| address.is_loopback())
            {
                Ok(())
            } else {
                Err(validation(
                    "hub.unsafe_location",
                    "test URL is not loopback HTTP",
                ))
            }
        }

        fn get(
            &self,
            url: &Url,
            range: Option<u64>,
            if_range: Option<&str>,
            limit: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            self.0.get(url, range, if_range, limit)
        }
    }

    pub(super) fn response(status: u16, content_range: Option<&str>, body: &[u8]) -> HttpResponse {
        HttpResponse {
            status,
            location: None,
            content_range: content_range.map(str::to_owned),
            etag: Some("\"fixture-1\"".to_owned()),
            body: Box::new(std::io::Cursor::new(body.to_vec())),
        }
    }

    pub(super) fn object(bytes: &[u8]) -> ObjectDescriptor {
        let mut cursor = std::io::Cursor::new(bytes);
        ObjectDescriptor {
            digest: graphforge_discovery::Sha256Digest(hash_reader(&mut cursor).unwrap()),
            length: bytes.len() as u64,
            media_type: graphforge_discovery::PORTABLE_V2_MEDIA_TYPE.into(),
            locations: vec!["https://objects.example/project.gfpb".into()],
        }
    }
    #[test]
    fn identity_forms_are_equivalent() {
        let (short, _) = parse_input("openalex/openalex").unwrap();
        let (url, _) = parse_input("https://graphforge.sh/openalex/openalex").unwrap();
        assert_eq!(short, url);
    }
    #[test]
    fn rejects_non_public_address_classes() {
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "192.0.2.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
        ] {
            assert!(!public_ip(ip.parse().unwrap()), "{ip}");
        }
        assert!(public_ip("1.1.1.1".parse().unwrap()));
        assert!(public_ip("2606:4700:4700::1111".parse().unwrap()));
    }
    #[test]
    fn rejects_credentials_and_non_https() {
        for url in [
            "http://example.com/a/b",
            "https://user@example.com/a/b",
            "https://127.0.0.1/a/b",
        ] {
            assert!(parse_input(url).is_err());
        }
    }

    #[test]
    fn rejects_redirect_to_private_network_before_following_it() {
        let mut redirect = response(302, None, b"");
        redirect.location = Some("https://127.0.0.1/private".into());
        let transport = Scripted::new(vec![redirect, response(200, None, b"secret")]);
        let start = Url::parse("https://hub.example/repository/.gf/manifest").unwrap();
        let Err(error) = fetch(&transport, &start, None, None, 1024) else {
            panic!("private redirect unexpectedly succeeded");
        };
        assert!(error.to_string().contains("hub.unsafe_location"));
        assert_eq!(
            transport.remaining(),
            1,
            "private target was never requested"
        );
    }

    #[test]
    fn redirect_attempts_count_each_transport_request() {
        let mut redirect = response(302, None, b"");
        redirect.location = Some("https://objects.example/final".into());
        let transport = Scripted::new(vec![redirect, response(200, None, b"ok")]);
        let start = Url::parse("https://hub.example/start").unwrap();
        let mut attempts = 0;
        let response = fetch_with_attempts(
            &transport,
            &start,
            None,
            None,
            1024,
            &mut attempts,
            &TEST_RETRY_POLICY,
        )
        .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(attempts, 2);
    }

    #[test]
    fn interrupted_download_resumes_with_exact_range() {
        let bytes = b"verified portable bytes";
        let descriptor = object(bytes);
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("project");
        let first = Scripted::new(vec![response(200, None, &bytes[..8])]);
        let error = download(&first, &descriptor, &destination).unwrap_err();
        assert!(error.to_string().contains("hub.interrupted"));
        let range = format!("bytes 8-{}/{}", bytes.len() - 1, bytes.len());
        let second = Scripted::new(vec![response(206, Some(&range), &bytes[8..])]);
        assert_eq!(
            download(&second, &descriptor, &destination).unwrap(),
            DownloadReport {
                resumed_bytes: 8,
                transferred_bytes: (bytes.len() - 8) as u64,
                attempts: 1,
            }
        );
    }

    #[test]
    fn torn_checkpoint_restarts_without_range() {
        let bytes = b"verified portable bytes";
        let descriptor = object(bytes);
        let root = tempfile::tempdir().unwrap();
        let partial = root.path().join("package.part");
        std::fs::write(&partial, &bytes[..8]).unwrap();
        std::fs::write(partial.with_extension("resume.json"), b"{torn").unwrap();
        let transport = Scripted::new(vec![response(200, None, bytes)]);
        assert_eq!(
            download(&transport, &descriptor, &partial).unwrap(),
            DownloadReport {
                resumed_bytes: 0,
                transferred_bytes: bytes.len() as u64,
                attempts: 1,
            }
        );
        assert_eq!(std::fs::read(partial).unwrap(), bytes);
    }

    #[cfg(unix)]
    #[test]
    fn checkpoint_symlink_is_rejected_without_touching_target() {
        use std::os::unix::fs::symlink;
        let bytes = b"verified portable bytes";
        let descriptor = object(bytes);
        let root = tempfile::tempdir().unwrap();
        let partial = root.path().join("package.part");
        std::fs::write(&partial, &bytes[..8]).unwrap();
        let victim = root.path().join("victim");
        std::fs::write(&victim, b"untouched").unwrap();
        symlink(&victim, partial.with_extension("resume.json")).unwrap();
        let transport = Scripted::new(vec![response(200, None, bytes)]);
        let error = download(&transport, &descriptor, &partial).unwrap_err();
        assert!(error.to_string().contains("hub.destination_conflict"));
        assert_eq!(std::fs::read(victim).unwrap(), b"untouched");
    }

    #[test]
    fn real_http_interruption_resumes_in_process_with_range() {
        let bytes = b"verified portable bytes".to_vec();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_bytes = bytes.clone();
        let server = std::thread::spawn(move || {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    request.push_str(&line);
                }
                if attempt == 0 {
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"fixture-1\"\r\nConnection: close\r\n\r\n",
                        server_bytes.len()
                    )
                    .unwrap();
                    stream.write_all(&server_bytes[..8]).unwrap();
                } else {
                    assert!(
                        request.to_ascii_lowercase().contains("range: bytes=8-"),
                        "{request}"
                    );
                    assert!(
                        request.contains("if-range: \"fixture-1\"")
                            || request.contains("If-Range: \"fixture-1\""),
                        "{request}"
                    );
                    write!(
                        stream,
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes 8-{}/{}\r\nETag: \"fixture-1\"\r\nConnection: close\r\n\r\n",
                        server_bytes.len() - 8,
                        server_bytes.len() - 1,
                        server_bytes.len()
                    )
                    .unwrap();
                    stream.write_all(&server_bytes[8..]).unwrap();
                }
            }
        });
        let mut descriptor = object(&bytes);
        descriptor.locations = vec![format!("http://{address}/project.gfpb")];
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("project");
        let transport = LoopbackTransport::new();
        // The cut body is retried in-process: one invocation completes.
        assert_eq!(
            download(&transport, &descriptor, &destination).unwrap(),
            DownloadReport {
                resumed_bytes: 0,
                transferred_bytes: bytes.len() as u64,
                attempts: 2,
            }
        );
        assert_eq!(std::fs::read(&destination).unwrap(), bytes);
        server.join().unwrap();
    }

    #[test]
    fn corrupt_download_is_removed_and_never_published() {
        let descriptor = object(b"expected");
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("project");
        let transport = Scripted::new(vec![response(200, None, b"corrupt!")]);
        let error = download(&transport, &descriptor, &destination).unwrap_err();
        assert!(error.to_string().contains("hub.integrity"));
        assert!(!destination.exists());
        assert!(!destination.exists());
    }

    #[cfg(unix)]
    #[test]
    fn resume_path_symlink_is_never_followed() {
        use std::os::unix::fs::symlink;

        let bytes = b"verified portable bytes";
        let descriptor = object(bytes);
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("project");
        let victim = root.path().join("victim");
        std::fs::write(&victim, b"keep me").unwrap();
        symlink(&victim, &destination).unwrap();
        let transport = Scripted::new(vec![response(200, None, bytes)]);
        let error = download(&transport, &descriptor, &destination).unwrap_err();
        assert!(error.to_string().contains("hub.destination_conflict"));
        assert_eq!(std::fs::read(&victim).unwrap(), b"keep me");
        assert_eq!(transport.remaining(), 1, "object was never requested");
    }

    #[cfg(unix)]
    #[test]
    fn dangling_destination_symlink_is_a_conflict() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("project");
        symlink(root.path().join("missing"), &destination).unwrap();
        let transport = Scripted::new(vec![]);
        let error = run_clone_with(
            &transport,
            CloneArgs {
                repository: "openalex/openalex".into(),
                destination: Some(destination),
                telemetry_endpoint: None,
                git_ref: None,
                version_uuid: None,
            },
            false,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("hub.destination_conflict"));
        assert_eq!(transport.remaining(), 0, "discovery was never requested");
    }

    #[test]
    fn endpoint_does_not_duplicate_repository_path() {
        let base = Url::parse("https://graphforge.sh/openalex/openalex").unwrap();
        assert_eq!(
            endpoint(&base, "refs").as_str(),
            "https://graphforge.sh/openalex/openalex/.gf/refs"
        );
    }

    #[test]
    fn staging_lock_is_exclusive_and_crash_releasing() {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("project");
        let first = acquire_staging(&destination).unwrap();
        assert!(
            acquire_staging(&destination)
                .unwrap_err()
                .to_string()
                .contains("hub.concurrent_clone")
        );
        drop(first);
        assert!(acquire_staging(&destination).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn staging_directory_symlink_is_rejected() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("project");
        let victim = root.path().join("victim");
        std::fs::create_dir(&victim).unwrap();
        symlink(&victim, staging_path(&destination).unwrap()).unwrap();
        assert!(
            acquire_staging(&destination)
                .unwrap_err()
                .to_string()
                .contains("hub.destination_conflict")
        );
        assert!(std::fs::read_dir(victim).unwrap().next().is_none());
    }

    #[test]
    fn research_ref_without_lineage_stops_before_project_download() {
        let repository = serde_json::json!({"owner":"openalex","repository":"openalex"});
        let immutable = format!("sha256:{}", "a".repeat(64));
        let refs = serde_json::to_vec(&serde_json::json!({
            "format":"graphforge-discovery/1","version":{"major":1,"minor":0},
            "repository":repository.clone(),"default_ref":"main",
            "refs":[{"name":"main","target":immutable,"validator":format!("sha256:{}", "d".repeat(64))}]
        }))
        .unwrap();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "format":"graphforge-discovery/1","version":{"major":1,"minor":0},
            "repository":repository,"default_ref":"main","resolved_ref":"main",
            "immutable_version":format!("sha256:{}", "a".repeat(64)),
            "package":{
                "format":"graphforge-project/2",
                "package_digest":format!("sha256:{}", "b".repeat(64)),
                "object_digest":format!("sha256:{}", "c".repeat(64))
            },
            "requirements":[{"capability":"portable-v2","major":1}],"capabilities":[],
            "objects":[{
                "digest":format!("sha256:{}", "c".repeat(64)),"length":1,
                "media_type":graphforge_discovery::PORTABLE_V2_MEDIA_TYPE,
                "locations":["https://objects.example/project.gfpb"]
            }]
        }))
        .unwrap();
        let transport = Scripted::new(vec![
            response(200, None, &refs),
            response(200, None, &manifest),
            response(200, None, b"must not be read"),
        ]);
        let root = tempfile::tempdir().unwrap();
        let error = run_clone_with(
            &transport,
            CloneArgs {
                repository: "openalex/openalex".into(),
                destination: Some(root.path().join("project")),
                telemetry_endpoint: None,
                git_ref: Some("main".into()),
                version_uuid: None,
            },
            true,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("hub.missing_object"), "{error}");
        assert_eq!(
            transport.remaining(),
            1,
            "project object was never requested"
        );
    }

    #[test]
    fn unsupported_future_manifest_stops_before_object_access() {
        let repository = serde_json::json!({"owner":"openalex","repository":"openalex"});
        let immutable = format!("sha256:{}", "a".repeat(64));
        let refs = serde_json::to_vec(&serde_json::json!({
            "format":"graphforge-discovery/1","version":{"major":1,"minor":0},
            "repository":repository.clone(),"default_ref":"main",
            "refs":[{"name":"main","target":immutable,"validator":format!("sha256:{}", "d".repeat(64))}]
        }))
        .unwrap();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "format":"graphforge-discovery/1","version":{"major":2,"minor":0},
            "repository":repository,"default_ref":"main","resolved_ref":"main",
            "immutable_version":format!("sha256:{}", "a".repeat(64)),
            "package":{
                "format":"graphforge-project/2",
                "package_digest":format!("sha256:{}", "b".repeat(64)),
                "object_digest":format!("sha256:{}", "c".repeat(64))
            },
            "requirements":[],"capabilities":[],
            "objects":[{
                "digest":format!("sha256:{}", "c".repeat(64)),"length":1,
                "media_type":graphforge_discovery::PORTABLE_V2_MEDIA_TYPE,
                "locations":["https://objects.example/project.gfpb"]
            }]
        }))
        .unwrap();
        let transport = Scripted::new(vec![
            response(200, None, &refs),
            response(200, None, &manifest),
            response(200, None, b"must not be read"),
        ]);
        let root = tempfile::tempdir().unwrap();
        let error = run_clone_with(
            &transport,
            CloneArgs {
                repository: "openalex/openalex".into(),
                destination: Some(root.path().join("project")),
                telemetry_endpoint: None,
                git_ref: None,
                version_uuid: None,
            },
            true,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("hub.unsupported_future"),
            "{error}"
        );
        assert_eq!(
            transport.remaining(),
            1,
            "object endpoint was never requested"
        );
    }

    pub(super) fn clone_script(bundle: &[u8], package_digest: &str) -> Scripted {
        let (refs, manifest) = clone_documents(bundle, package_digest, &[]);
        Scripted::new(vec![
            response(200, None, &refs),
            response(200, None, &manifest),
            response(200, None, bundle),
        ])
    }

    /// Refs and manifest advertising `bundle` as the Project package, plus
    /// `extra` objects that clone must never fetch.
    pub(super) fn clone_documents(
        bundle: &[u8],
        package_digest: &str,
        extra: &[serde_json::Value],
    ) -> (Vec<u8>, Vec<u8>) {
        let object_digest = hash_reader(&mut std::io::Cursor::new(bundle)).unwrap();
        let repository = serde_json::json!({"owner":"openalex","repository":"openalex"});
        let immutable = format!("sha256:{}", "a".repeat(64));
        let validator = format!("sha256:{}", "d".repeat(64));
        let refs = serde_json::to_vec(&serde_json::json!({
            "format":"graphforge-discovery/1","version":{"major":1,"minor":0},
            "repository":repository.clone(),"default_ref":"main",
            "refs":[{"name":"main","target":immutable.clone(),"validator":validator}]
        }))
        .unwrap();
        let mut objects = vec![
            serde_json::json!({"digest":object_digest,"length":bundle.len(),"media_type":graphforge_discovery::PORTABLE_V2_MEDIA_TYPE,"locations":["https://objects.example/project.gfpb"]}),
        ];
        objects.extend(extra.iter().cloned());
        objects.sort_by(|left, right| left["digest"].as_str().cmp(&right["digest"].as_str()));
        let manifest = serde_json::to_vec(&serde_json::json!({
            "format":"graphforge-discovery/1","version":{"major":1,"minor":0},
            "repository":repository,"default_ref":"main","resolved_ref":"main",
            "immutable_version":immutable,
            "package":{"format":"graphforge-project/2","package_digest":package_digest,"object_digest":object_digest},
            "requirements":[{"capability":"portable-v2","major":1}],"capabilities":[{"capability":"range-requests","major":1}],
            "objects":objects
        })).unwrap();
        (refs, manifest)
    }

    /// A real complete portable-v2 bundle of an empty project and its
    /// semantic package digest.
    pub(super) fn real_bundle() -> (Vec<u8>, String) {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        GraphForge::new(source.to_str()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(&source).unwrap();
        let limits = PortableV2Limits::default();
        let plan = graphforge_storage::plan_complete_portable_v2(&generation, limits).unwrap();
        let bundle_path = root.path().join("complete.gfpb");
        graphforge_storage::export_complete_portable_v2(
            &plan,
            &bundle_path,
            graphforge_storage::PortableV2Output::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let report = graphforge_storage::verify_portable_v2(
            &bundle_path,
            PortableV2Mode::Full,
            limits,
            None,
        )
        .unwrap();
        (std::fs::read(&bundle_path).unwrap(), report.package_digest)
    }

    /// Scripted responses that also record every requested URL.
    pub(super) struct RecordingTransport {
        pub(super) inner: Scripted,
        pub(super) requested: Mutex<Vec<String>>,
    }

    impl Transport for RecordingTransport {
        fn get(
            &self,
            url: &Url,
            range: Option<u64>,
            if_range: Option<&str>,
            limit: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            self.requested.lock().unwrap().push(url.as_str().to_owned());
            self.inner.get(url, range, if_range, limit)
        }
    }

    #[test]
    fn clone_succeeds_against_the_checked_in_hub_fixture() {
        macro_rules! fixture {
            ($name:literal) => {
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../tests/fixtures/hub/generated/v1/",
                    $name
                ))
            };
        }
        let refs = fixture!("refs.json");
        let manifest_bytes = fixture!("manifest.json");
        let project = fixture!("objects/openalex-openalex.gfpb");
        let manifest = graphforge_discovery::DiscoveryManifest::from_json(
            manifest_bytes,
            graphforge_discovery::DiscoveryLimits::default(),
        )
        .unwrap();
        // The fixture advertises a summary and a module package besides the
        // Project package; clone needs neither and must not fetch them.
        assert!(manifest.summary.is_some());
        assert!(manifest.ontology.is_some());
        assert!(manifest.objects.len() > 1);
        let transport = RecordingTransport {
            inner: Scripted::new(vec![
                response(200, None, refs),
                response(200, None, manifest_bytes),
                response(200, None, project),
            ]),
            requested: Mutex::new(Vec::new()),
        };
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("openalex");
        let mut output = Vec::new();
        run_clone_with(
            &transport,
            CloneArgs {
                repository: "openalex/openalex".into(),
                destination: Some(destination.clone()),
                telemetry_endpoint: None,
                git_ref: None,
                version_uuid: None,
            },
            true,
            &mut output,
        )
        .unwrap();
        assert_eq!(transport.inner.remaining(), 0);
        let requested = transport.requested.lock().unwrap().clone();
        assert_eq!(requested.len(), 3, "{requested:?}");
        assert_eq!(
            requested[2],
            manifest.package_object().unwrap().locations[0],
            "only the Project package object is downloaded"
        );
        let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(result["contract"], "graphforge-hub-clone/1");
        assert_eq!(result["package_digest"], manifest.package.package_digest.0);
        let cloned = GraphForge::new(destination.to_str()).expect("clone reopens");
        let metadata = cloned.research_project_metadata().unwrap();
        assert_eq!(metadata.title.as_deref(), Some("OpenAlex"));
        assert_eq!(metadata.license.as_deref(), Some("CC0-1.0"));
    }

    #[test]
    fn interrupted_clone_retains_resumed_and_transferred_bytes_once() {
        let bundle = b"0123456789abcdef";
        let package_digest = format!("sha256:{}", "b".repeat(64));
        let transport = clone_script(bundle, &package_digest);
        let range = format!("bytes 8-{}/{}", bundle.len() - 1, bundle.len());
        transport.replace_last(response(206, Some(&range), &bundle[8..12]));
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("clone");
        let staging = staging_path(&destination).unwrap();
        std::fs::create_dir(&staging).unwrap();
        let partial = staging.join("package.part");
        std::fs::write(&partial, &bundle[..8]).unwrap();
        let digest = hash_reader(&mut std::io::Cursor::new(bundle)).unwrap();
        std::fs::write(
            partial.with_extension("resume.json"),
            serde_json::to_vec(&ResumeState {
                digest,
                length: bundle.len() as u64,
                location: "https://objects.example/project.gfpb".into(),
                etag: "\"fixture-1\"".into(),
            })
            .unwrap(),
        )
        .unwrap();
        let runtime = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::InMemory,
            ..TelemetryConfig::default()
        })
        .unwrap();
        let error = run_clone_profiled(
            &transport,
            CloneArgs {
                repository: "openalex/openalex".into(),
                destination: Some(destination),
                telemetry_endpoint: None,
                git_ref: None,
                version_uuid: None,
            },
            true,
            &mut Vec::new(),
            &runtime,
        )
        .unwrap_err();
        assert!(error.to_string().contains("hub.interrupted"));
        assert_eq!(
            runtime.force_flush(),
            graphforge_api::telemetry::LifecycleStatus::Complete
        );
        let snapshots = runtime.snapshots();
        assert_eq!(snapshots.len(), 1);
        let job = snapshots[0].job.as_ref().unwrap();
        assert_eq!(job.outcome, Outcome::Failed);
        let stage = job
            .stages
            .iter()
            .find(|stage| stage.stage == Stage::Download)
            .unwrap();
        assert_eq!(stage.resumed_bytes, Some(8));
        assert_eq!(stage.bytes, Some(4));
        // One short `206`, then refused connections until the retry bound.
        assert_eq!(stage.attempt, RETRY_POLICY.attempts);
        assert!(!job.handoffs.iter().any(|handoff| {
            matches!(
                handoff.to,
                ComponentKind::PortableVerify | ComponentKind::PortableImport
            )
        }));
    }

    #[test]
    fn disabled_and_failed_exporters_do_not_change_clone_stage_results() {
        let execute = |runtime: &TelemetryRuntime| {
            let mut profile = CloneProfile::new(runtime);
            let value = profile
                .stage(
                    Stage::IdentityValidation,
                    ComponentKind::Cli,
                    ComponentRole::Facade,
                    None,
                    1,
                    || Ok((42_u8, None, None)),
                )
                .unwrap();
            profile.finish(&Ok(()));
            value
        };
        let disabled = TelemetryRuntime::default();
        let failed = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::OtlpHttpJson,
            export_timeout: Duration::from_millis(5),
            lifecycle_timeout: Duration::from_millis(20),
            max_retries: 0,
            otlp: Some(OtlpConfig {
                endpoint: "http://127.0.0.1:1/".into(),
                headers: BTreeMap::default(),
            }),
            ..TelemetryConfig::default()
        })
        .unwrap();
        assert_eq!(execute(&disabled), execute(&failed));
        assert!(matches!(
            failed.force_flush(),
            graphforge_api::telemetry::LifecycleStatus::ExportFailed
                | graphforge_api::telemetry::LifecycleStatus::TimedOut
        ));
    }

    #[test]
    fn invalid_clone_emits_one_normalized_terminal_without_identity() {
        let runtime = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::InMemory,
            ..TelemetryConfig::default()
        })
        .unwrap();
        let canary = "not/a/valid/repository-secret-canary";
        let error = run_clone_profiled(
            &Scripted::new(vec![]),
            CloneArgs {
                repository: canary.into(),
                destination: None,
                telemetry_endpoint: None,
                git_ref: None,
                version_uuid: None,
            },
            true,
            &mut Vec::new(),
            &runtime,
        )
        .unwrap_err();
        assert!(error.to_string().contains("hub.invalid_identity"));
        assert_eq!(
            runtime.force_flush(),
            graphforge_api::telemetry::LifecycleStatus::Complete
        );
        let snapshots = runtime.snapshots();
        assert_eq!(snapshots.len(), 1);
        let job = snapshots[0].job.as_ref().unwrap();
        assert_eq!(job.outcome, Outcome::Failed);
        assert_eq!(job.failure, Some(Failure::InvalidInput));
        assert_eq!(job.stages[0].stage, Stage::IdentityValidation);
        assert!(!serde_json::to_string(&snapshots).unwrap().contains(canary));
    }

    #[test]
    fn hub_failure_codes_map_through_a_finite_matrix() {
        for (code, expected) in [
            ("hub.invalid_identity", Failure::InvalidInput),
            ("hub.destination_conflict", Failure::InvalidInput),
            ("hub.unsupported_future", Failure::InvalidInput),
            ("hub.integrity", Failure::InvalidInput),
            ("hub.module.identity_mismatch", Failure::InvalidInput),
            ("hub.module.content_digest_mismatch", Failure::InvalidInput),
            (
                "hub.package.research_version_mismatch",
                Failure::InvalidInput,
            ),
            ("hub.package.invalid_participant", Failure::InvalidInput),
            ("hub.limit_exceeded", Failure::ResourceLimit),
            ("hub.unsafe_location", Failure::Network),
            ("hub.network", Failure::Network),
            ("hub.package.io", Failure::Storage),
        ] {
            assert_eq!(classify_failure(&validation(code, "redacted")), expected);
        }
        assert_eq!(
            classify_failure(&validation("hub.unknown", "redacted")),
            Failure::Internal
        );
        assert_eq!(classify_failure(&storage("redacted")), Failure::Storage);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn both_identity_forms_import_and_reopen_the_same_real_project() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        GraphForge::new(source.to_str()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(&source).unwrap();
        let limits = PortableV2Limits::default();
        let plan = graphforge_storage::plan_complete_portable_v2(&generation, limits).unwrap();
        let bundle_path = root.path().join("complete.gfpb");
        graphforge_storage::export_complete_portable_v2(
            &plan,
            &bundle_path,
            graphforge_storage::PortableV2Output::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let bundle = std::fs::read(&bundle_path).unwrap();
        let report = graphforge_storage::verify_portable_v2(
            &bundle_path,
            PortableV2Mode::Full,
            limits,
            None,
        )
        .unwrap();

        let mut fail_open_results = Vec::new();
        let mut fail_open_elapsed = Vec::new();
        let mut delayed_jobs = Vec::new();
        for (index, input) in [
            "openalex/openalex",
            "https://graphforge.sh/openalex/openalex",
            "openalex/openalex",
            "openalex/openalex",
            "openalex/openalex",
            "openalex/openalex",
            "openalex/openalex",
            "openalex/openalex",
            "openalex/openalex",
        ]
        .into_iter()
        .enumerate()
        {
            let destination = root.path().join(format!("clone-{index}"));
            let mut output = Vec::new();
            let runtime = match index {
                2 => TelemetryRuntime::default(),
                3 => TelemetryRuntime::new(TelemetryConfig {
                    mode: TelemetryMode::OtlpHttpJson,
                    export_timeout: Duration::from_millis(5),
                    lifecycle_timeout: Duration::from_millis(20),
                    max_retries: 0,
                    otlp: Some(OtlpConfig {
                        endpoint: "http://127.0.0.1:1/".into(),
                        headers: BTreeMap::default(),
                    }),
                    ..TelemetryConfig::default()
                })
                .unwrap(),
                _ => TelemetryRuntime::new(TelemetryConfig {
                    mode: TelemetryMode::InMemory,
                    ..TelemetryConfig::default()
                })
                .unwrap(),
            };
            // The five attribution cases execute real imports, but only their
            // injected work advances this isolated clock. Filesystem latency
            // and unrelated tests cannot change which stage dominates.
            let clock = if index >= 4 {
                CloneClock::manual()
            } else {
                CloneClock::default()
            };
            let transport = DelayedTransport {
                inner: clone_script(&bundle, &report.package_digest),
                clock: clock.clone(),
                discovery: if index == 4 {
                    Duration::from_secs(2)
                } else {
                    Duration::ZERO
                },
                download: if index == 5 {
                    Duration::from_secs(2)
                } else {
                    Duration::ZERO
                },
            };
            let delays = CloneDelays {
                clock,
                verification: if index == 6 {
                    Duration::from_secs(2)
                } else {
                    Duration::ZERO
                },
                import: if index == 7 {
                    Duration::from_secs(2)
                } else {
                    Duration::ZERO
                },
                reopen: if index == 8 {
                    Duration::from_secs(2)
                } else {
                    Duration::ZERO
                },
                before_import: None,
            };
            let clone_started = Instant::now();
            run_clone_profiled_with_delays(
                &transport,
                CloneArgs {
                    repository: input.into(),
                    destination: Some(destination.clone()),
                    telemetry_endpoint: None,
                    git_ref: None,
                    version_uuid: None,
                },
                true,
                &mut output,
                &runtime,
                &delays,
                &mut CloneEnv::quiet(),
            )
            .unwrap();
            let clone_elapsed = clone_started.elapsed();
            let lifecycle = runtime.force_flush();
            if !(2..4).contains(&index) {
                assert_eq!(
                    lifecycle,
                    graphforge_api::telemetry::LifecycleStatus::Complete
                );
            }
            let snapshots = runtime.snapshots();
            if !(2..4).contains(&index) {
                assert_eq!(snapshots.len(), 1);
                let job = snapshots[0].job.as_ref().unwrap();
                assert_eq!(job.family, JobFamily::Clone);
                assert_eq!(job.outcome, Outcome::Ok);
                assert_eq!(job.stages.first().unwrap().stage, Stage::IdentityValidation);
                assert!(job.stages.iter().any(|stage| stage.stage == Stage::Cleanup));
                assert!(job.stages.iter().any(|stage| stage.stage == Stage::Reopen));
                assert!(
                    job.handoffs
                        .windows(2)
                        .all(|pair| { pair[0].start_offset_ns <= pair[1].start_offset_ns })
                );
                assert!(job.handoffs.iter().any(|handoff| {
                    handoff.from == ComponentKind::NetworkTransport
                        && handoff.to == ComponentKind::PortableVerify
                        && handoff.kind == HandoffKind::Transfer
                        && handoff.bytes == Some(bundle.len() as u64)
                }));
                let path: Vec<_> = job
                    .handoffs
                    .iter()
                    .map(|handoff| (handoff.from, handoff.to))
                    .collect();
                assert_eq!(
                    path,
                    vec![
                        (ComponentKind::Cli, ComponentKind::NetworkTransport),
                        (ComponentKind::NetworkTransport, ComponentKind::Discovery),
                        (ComponentKind::Discovery, ComponentKind::NetworkTransport),
                        (
                            ComponentKind::NetworkTransport,
                            ComponentKind::PortableVerify
                        ),
                        (ComponentKind::PortableVerify, ComponentKind::Api),
                        (ComponentKind::Api, ComponentKind::PortableImport),
                        (ComponentKind::PortableImport, ComponentKind::Storage),
                        (ComponentKind::Storage, ComponentKind::Publication),
                        (ComponentKind::Publication, ComponentKind::Api),
                        (ComponentKind::Api, ComponentKind::Recovery),
                        (ComponentKind::Recovery, ComponentKind::Storage),
                    ]
                );
                if index >= 4 {
                    delayed_jobs.push(job.clone());
                }
            }
            let serialized = serde_json::to_string(&snapshots).unwrap();
            for canary in [input, destination.to_str().unwrap(), &report.package_digest] {
                assert!(!serialized.contains(canary));
            }
            let mut result: serde_json::Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(result["contract"], "graphforge-hub-clone/1");
            assert_eq!(result["package_digest"], report.package_digest);
            GraphForge::new(destination.to_str()).expect("cloned project reopens through facade");
            if (2..=3).contains(&index) {
                result.as_object_mut().unwrap().remove("destination");
                fail_open_results.push(result);
                fail_open_elapsed.push(clone_elapsed);
            }
        }
        assert_eq!(fail_open_results[0], fail_open_results[1]);
        assert!(fail_open_elapsed[1] <= fail_open_elapsed[0] + Duration::from_secs(1));
        assert_eq!(delayed_jobs.len(), 5);
        for (job, expected) in delayed_jobs.iter().zip([
            ComponentKind::NetworkTransport,
            ComponentKind::NetworkTransport,
            ComponentKind::PortableVerify,
            ComponentKind::PortableImport,
            ComponentKind::Recovery,
        ]) {
            // Real work takes zero manual time, retaining the one-nanosecond
            // stage minimum without reversing offsets or overlapping stages.
            assert!(job.stages.windows(2).all(|pair| {
                pair[0].start_offset_ns + pair[0].duration_ns <= pair[1].start_offset_ns
            }));
            let last = job.stages.last().unwrap();
            assert_eq!(job.finished_ns, last.start_offset_ns + last.duration_ns);
            let dominant = job
                .stages
                .iter()
                .filter(|stage| stage.stage != Stage::Orchestration)
                .max_by_key(|stage| stage.duration_ns)
                .unwrap();
            assert_eq!(dominant.component, expected);
            assert_eq!(dominant.duration_ns, 2_000_000_000);
        }
    }

    #[test]
    #[ignore = "manual perf measurement for #1404, not part of the regular suite"]
    fn measure_clone_wall_time() {
        const ITERATIONS: usize = 20;
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        GraphForge::new(source.to_str()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(&source).unwrap();
        let limits = PortableV2Limits::default();
        let plan = graphforge_storage::plan_complete_portable_v2(&generation, limits).unwrap();
        let bundle_path = root.path().join("complete.gfpb");
        graphforge_storage::export_complete_portable_v2(
            &plan,
            &bundle_path,
            graphforge_storage::PortableV2Output::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let bundle = std::fs::read(&bundle_path).unwrap();
        let report = graphforge_storage::verify_portable_v2(
            &bundle_path,
            PortableV2Mode::Full,
            limits,
            None,
        )
        .unwrap();

        let runtime = TelemetryRuntime::default();
        let mut total = Duration::ZERO;
        for i in 0..ITERATIONS {
            let transport = clone_script(&bundle, &report.package_digest);
            let destination = root.path().join(format!("clone-{i}"));
            let started = Instant::now();
            run_clone_profiled(
                &transport,
                CloneArgs {
                    repository: "openalex/openalex".into(),
                    destination: Some(destination),
                    telemetry_endpoint: None,
                    git_ref: None,
                    version_uuid: None,
                },
                true,
                &mut Vec::new(),
                &runtime,
            )
            .unwrap();
            total += started.elapsed();
        }
        println!(
            "hub clone: iterations={ITERATIONS} total={total:?} ({:?}/call)",
            total / u32::try_from(ITERATIONS).unwrap(),
        );
    }
}
