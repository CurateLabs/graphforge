//! Secure Hub discovery, download, and atomic portable-v2 import.

use clap::Args;
use fs4::{FileExt, TryLockError};
use graphforge_api::telemetry::{
    ComponentHandoff, ComponentKind, ComponentRole, Failure, HandoffKind, JobFamily, JobSnapshot,
    JobStage, OtlpConfig, Outcome, Stage, TelemetryConfig, TelemetryMode, TelemetryRuntime,
    WaitReason,
};
use graphforge_api::{
    GraphForge, OperationId, PortableV2ImportRequest, PortableV2Limits, PortableV2Mode,
};
use graphforge_discovery::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ObjectDescriptor,
    RefSet, RepositoryIdentity,
};
use graphforge_storage::{
    DiscoveryPortableV2Error, DiscoveryPortableV2Mismatch, DiscoveryPortableV2Request,
    PortableV2ErrorCode, verify_discovered_portable_v2,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use ureq::unversioned::resolver::{DefaultResolver, ResolvedSocketAddrs, Resolver};
use ureq::unversioned::transport::DefaultConnector;
use url::Url;

const DEFAULT_HUB: &str = "https://graphforge.sh";
const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_BUNDLE_BYTES: u64 = 100 * 1024 * 1024 * 1024;
const MAX_REDIRECTS: usize = 4;

#[derive(Args)]
pub(crate) struct CloneArgs {
    /// Canonical owner/repository name or an HTTPS Hub repository URL.
    pub repository: String,
    /// New project directory; defaults to the repository name.
    pub destination: Option<PathBuf>,
    /// Explicit local OTLP collector base URL. Clone telemetry is otherwise disabled.
    #[arg(long)]
    pub telemetry_endpoint: Option<String>,
}

struct CloneProfile<'a> {
    runtime: &'a TelemetryRuntime,
    started: Option<Instant>,
    cursor_ns: u64,
    stages: Vec<JobStage>,
    handoffs: Vec<ComponentHandoff>,
}

impl<'a> CloneProfile<'a> {
    fn new(runtime: &'a TelemetryRuntime) -> Self {
        Self {
            runtime,
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
        let origin = *self.started.get_or_insert_with(Instant::now);
        let operation_start_ns = u64::try_from(origin.elapsed().as_nanos()).unwrap_or(u64::MAX);
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
        let started = Instant::now();
        let result = operation();
        let duration_ns = u64::try_from(started.elapsed().as_nanos())
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
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
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

#[derive(Serialize)]
struct CloneResult {
    contract: &'static str,
    repository: String,
    destination: String,
    immutable_version: String,
    package_digest: String,
    generation_uuid: String,
    resumed_bytes: u64,
}

#[derive(Debug)]
struct PublicResolver(DefaultResolver);

impl Resolver for PublicResolver {
    fn resolve(
        &self,
        uri: &ureq::http::Uri,
        config: &ureq::config::Config,
        timeout: ureq::unversioned::transport::NextTimeout,
    ) -> Result<ResolvedSocketAddrs, ureq::Error> {
        let addresses = self.0.resolve(uri, config, timeout)?;
        let mut approved = self.empty();
        for address in addresses
            .iter()
            .copied()
            .filter(|address| public_ip(address.ip()))
        {
            approved.push(address);
        }
        if approved.is_empty() {
            Err(ureq::Error::HostNotFound)
        } else {
            Ok(approved)
        }
    }
}

fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => public_v4(ip),
        IpAddr::V6(ip) => {
            if let Some(v4) = ip.to_ipv4_mapped() {
                return public_v4(v4);
            }
            !(ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_multicast()
                || in_v6(ip, "fc00::".parse().unwrap(), 7)
                || in_v6(ip, "fe80::".parse().unwrap(), 10)
                || in_v6(ip, "2001:db8::".parse().unwrap(), 32))
        }
    }
}

fn public_v4(ip: Ipv4Addr) -> bool {
    let value = u32::from(ip);
    let blocked = [
        ("0.0.0.0", 8),
        ("10.0.0.0", 8),
        ("100.64.0.0", 10),
        ("127.0.0.0", 8),
        ("169.254.0.0", 16),
        ("172.16.0.0", 12),
        ("192.0.0.0", 24),
        ("192.0.2.0", 24),
        ("192.168.0.0", 16),
        ("198.18.0.0", 15),
        ("198.51.100.0", 24),
        ("203.0.113.0", 24),
        ("224.0.0.0", 4),
        ("240.0.0.0", 4),
    ];
    !blocked.iter().any(|(base, bits)| {
        let base = u32::from(base.parse::<Ipv4Addr>().unwrap());
        let mask = u32::MAX.checked_shl(32 - bits).unwrap_or(0);
        value & mask == base & mask
    })
}

fn in_v6(ip: Ipv6Addr, base: Ipv6Addr, bits: u32) -> bool {
    let mask = u128::MAX.checked_shl(128 - bits).unwrap_or(0);
    u128::from(ip) & mask == u128::from(base) & mask
}

struct HttpResponse {
    status: u16,
    location: Option<String>,
    content_range: Option<String>,
    etag: Option<String>,
    body: Box<dyn Read + Send>,
}

trait Transport {
    fn validate(&self, url: &Url) -> Result<(), graphforge_api::GfError> {
        validate_url(url)
    }

    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError>;
}

struct HttpTransport {
    agent: ureq::Agent,
}

impl HttpTransport {
    fn new() -> Self {
        let config = ureq::Agent::config_builder()
            .https_only(true)
            .http_status_as_error(false)
            .max_redirects(0)
            .proxy(None)
            .timeout_global(Some(Duration::from_mins(1)))
            .timeout_connect(Some(Duration::from_secs(10)))
            .build();
        Self {
            agent: ureq::Agent::with_parts(
                config,
                DefaultConnector::new(),
                PublicResolver(DefaultResolver::default()),
            ),
        }
    }
}

impl Transport for HttpTransport {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        let mut request = self.agent.get(url.as_str());
        if let Some(offset) = range {
            request = request.header("Range", format!("bytes={offset}-"));
        }
        if let Some(validator) = if_range {
            request = request.header("If-Range", validator);
        }
        let response = request.call().map_err(|_| network("request failed"))?;
        let status = response.status().as_u16();
        let location = response
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let content_range = response
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let (_, body) = response.into_parts();
        Ok(HttpResponse {
            status,
            location,
            content_range,
            etag,
            body: Box::new(body.into_reader().take(limit.saturating_add(1))),
        })
    }
}

fn validate_url(url: &Url) -> Result<(), graphforge_api::GfError> {
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(validation(
            "hub.unsafe_location",
            "URL must be credential-free HTTPS",
        ));
    }
    if let Some(host) = url.host_str()
        && host.parse::<IpAddr>().is_ok_and(|ip| !public_ip(ip))
    {
        return Err(validation("hub.unsafe_location", "URL host is not public"));
    }
    Ok(())
}

#[cfg(test)]
fn fetch(
    transport: &dyn Transport,
    start: &Url,
    range: Option<u64>,
    if_range: Option<&str>,
    limit: u64,
) -> Result<HttpResponse, graphforge_api::GfError> {
    let mut attempts = 0;
    fetch_with_attempts(transport, start, range, if_range, limit, &mut attempts)
}

fn fetch_with_attempts(
    transport: &dyn Transport,
    start: &Url,
    range: Option<u64>,
    if_range: Option<&str>,
    limit: u64,
    attempts: &mut u32,
) -> Result<HttpResponse, graphforge_api::GfError> {
    let mut url = start.clone();
    for hop in 0..=MAX_REDIRECTS {
        transport.validate(&url)?;
        *attempts = attempts.saturating_add(1);
        let response = transport.get(&url, range, if_range, limit)?;
        if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            if hop == MAX_REDIRECTS {
                return Err(network("redirect limit exceeded"));
            }
            let location = response
                .location
                .as_deref()
                .ok_or_else(|| network("redirect is missing Location"))?;
            url = url
                .join(location)
                .map_err(|_| validation("hub.unsafe_location", "invalid redirect URL"))?;
            continue;
        }
        if !(200..300).contains(&response.status) {
            return Err(network("Hub returned an unsuccessful status"));
        }
        return Ok(response);
    }
    unreachable!()
}

fn read_bounded(
    mut response: HttpResponse,
    limit: usize,
) -> Result<Vec<u8>, graphforge_api::GfError> {
    let mut bytes = Vec::new();
    response
        .body
        .read_to_end(&mut bytes)
        .map_err(|_| network("response read failed"))?;
    if bytes.len() > limit {
        return Err(limit_error("response exceeds byte bound"));
    }
    Ok(bytes)
}

fn parse_input(value: &str) -> Result<(RepositoryIdentity, Url), graphforge_api::GfError> {
    if !value.contains("://") {
        let identity = RepositoryIdentity::parse(value)
            .map_err(|_| validation("hub.invalid_identity", "invalid repository identity"))?;
        let base = Url::parse(&format!(
            "{DEFAULT_HUB}/{}/{}",
            identity.owner, identity.repository
        ))
        .unwrap();
        return Ok((identity, base));
    }
    let base = Url::parse(value)
        .map_err(|_| validation("hub.invalid_identity", "invalid repository URL"))?;
    validate_url(&base)?;
    if base.query().is_some() || base.path().ends_with('/') {
        return Err(validation(
            "hub.invalid_identity",
            "repository URL must end in owner/repository",
        ));
    }
    let segments: Vec<_> = base.path_segments().into_iter().flatten().collect();
    if segments.len() != 2 {
        return Err(validation(
            "hub.invalid_identity",
            "repository URL must end in owner/repository",
        ));
    }
    let identity = RepositoryIdentity::parse(&format!("{}/{}", segments[0], segments[1]))
        .map_err(|_| validation("hub.invalid_identity", "invalid repository identity"))?;
    Ok((identity, base))
}

fn endpoint(base: &Url, name: &str) -> Url {
    let mut endpoint = base.clone();
    endpoint.set_path(&format!("{}/.gf/{name}", base.path().trim_end_matches('/')));
    endpoint
}

fn select_bundle(
    manifest: &DiscoveryManifest,
) -> Result<&ObjectDescriptor, graphforge_api::GfError> {
    manifest
        .package_object()
        .map_err(|error| protocol_error(&error))
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

#[derive(Debug)]
struct CloneStaging {
    root: PathBuf,
    partial: PathBuf,
    _lock: File,
}

#[cfg(unix)]
fn acquire_staging(destination: &Path) -> Result<CloneStaging, graphforge_api::GfError> {
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
    Ok(CloneStaging {
        partial: root.join("package.part"),
        root,
        _lock: lock,
    })
}

#[cfg(not(unix))]
fn acquire_staging(destination: &Path) -> Result<CloneStaging, graphforge_api::GfError> {
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
    Ok(CloneStaging {
        partial: root.join("package.part"),
        root,
        _lock: lock,
    })
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

#[allow(clippy::too_many_lines)]
#[cfg(test)]
fn download(
    transport: &dyn Transport,
    object: &ObjectDescriptor,
    partial: &Path,
) -> Result<DownloadReport, graphforge_api::GfError> {
    download_with_progress(transport, object, partial, &mut DownloadReport::default())
}

#[allow(clippy::too_many_lines)]
fn download_with_progress(
    transport: &dyn Transport,
    object: &ObjectDescriptor,
    partial: &Path,
    progress: &mut DownloadReport,
) -> Result<DownloadReport, graphforge_api::GfError> {
    let checkpoint = partial.with_extension("resume.json");
    if object.length > MAX_BUNDLE_BYTES {
        return Err(limit_error("portable bundle exceeds clone byte bound"));
    }
    let mut resumed = match std::fs::symlink_metadata(partial) {
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
    if resumed > object.length {
        open_partial_nofollow(partial, false).map_err(storage)?;
        resumed = 0;
    }
    let location = object
        .locations
        .first()
        .ok_or_else(|| validation("hub.missing_object", "portable bundle has no location"))?;
    let url = Url::parse(location)
        .map_err(|_| validation("hub.unsafe_location", "invalid object URL"))?;
    let saved = read_resume(&checkpoint)?;
    let validator = saved
        .as_ref()
        .filter(|s| {
            s.digest == object.digest.0 && s.length == object.length && s.location == *location
        })
        .map(|s| s.etag.as_str());
    if resumed > 0 && validator.is_none() {
        resumed = 0;
    }
    progress.resumed_bytes = resumed;
    let mut transferred = 0_u64;
    if resumed < object.length {
        let mut response = fetch_with_attempts(
            transport,
            &url,
            (resumed > 0).then_some(resumed),
            validator,
            object.length.saturating_sub(resumed),
            &mut progress.attempts,
        )?;
        let expected_range = format!("bytes {resumed}-{}/{}", object.length - 1, object.length);
        let append = resumed > 0
            && response.status == 206
            && response.content_range.as_deref() == Some(expected_range.as_str());
        if resumed > 0 && response.status == 206 && !append {
            let _ = std::fs::remove_file(partial);
            return Err(validation(
                "hub.integrity",
                "range response does not match the requested object",
            ));
        }
        if resumed > 0 && !append {
            resumed = 0;
        }
        if resumed == 0 {
            save_resume(&checkpoint, object, location, &response)?;
        }
        let mut file = open_partial_nofollow(partial, append).map_err(storage)?;
        let mut copied = 0_u64;
        let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
        loop {
            let read = response
                .body
                .read(&mut buffer)
                .map_err(|_| network("object read failed"))?;
            if read == 0 {
                break;
            }
            copied = copied
                .checked_add(read as u64)
                .ok_or_else(|| limit_error("object exceeds byte bound"))?;
            transferred = copied;
            progress.transferred_bytes = transferred;
            if copied > object.length.saturating_sub(resumed) {
                return Err(limit_error("object exceeds declared size"));
            }
            file.write_all(&buffer[..read]).map_err(storage)?;
        }
        file.sync_all().map_err(storage)?;
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
    Ok(DownloadReport {
        resumed_bytes: resumed,
        transferred_bytes: transferred,
        attempts: progress.attempts,
    })
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
    let result = run_clone_profiled(&HttpTransport::new(), args, json, output, &runtime);
    let _ = runtime.shutdown();
    result
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
fn run_clone_with(
    transport: &dyn Transport,
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    run_clone_profiled(transport, args, json, output, &TelemetryRuntime::default())
}

fn run_clone_profiled(
    transport: &dyn Transport,
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
    runtime: &TelemetryRuntime,
) -> Result<(), graphforge_api::GfError> {
    let mut profile = CloneProfile::new(runtime);
    let result = run_clone_job(transport, args, &mut profile)
        .and_then(|result| write_clone_result(&result, json, output));
    profile.finish(&result);
    result
}

#[allow(clippy::too_many_lines)]
fn run_clone_job(
    transport: &dyn Transport,
    args: CloneArgs,
    profile: &mut CloneProfile<'_>,
) -> Result<CloneResult, graphforge_api::GfError> {
    let (identity, base, destination) = profile.stage(
        Stage::IdentityValidation,
        ComponentKind::Cli,
        ComponentRole::Facade,
        None,
        1,
        || {
            let (identity, base) = parse_input(&args.repository)?;
            let destination = args
                .destination
                .unwrap_or_else(|| PathBuf::from(&identity.repository));
            ensure_destination_absent(&destination)?;
            Ok(((identity, base, destination), None, None))
        },
    )?;
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
        max_cumulative_object_bytes: MAX_BUNDLE_BYTES,
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
    let object = select_bundle(&manifest)?;
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
            let report =
                download_with_progress(transport, object, &partial, &mut download_progress)?;
            Ok((report, Some(report.transferred_bytes), None))
        },
    );
    if let Some(stage) = profile.stages.last_mut() {
        stage.attempt = download_progress.attempts.max(1);
        stage.bytes = Some(download_progress.transferred_bytes);
        stage.resumed_bytes = Some(download_progress.resumed_bytes);
    }
    let download = download_result?;
    let portable_limits = PortableV2Limits::default();
    profile.handoff(
        ComponentKind::NetworkTransport,
        ComponentKind::PortableVerify,
        HandoffKind::Transfer,
        Some(download.resumed_bytes + download.transferred_bytes),
    );
    let verified = profile.stage(
        Stage::PortableVerification,
        ComponentKind::PortableVerify,
        ComponentRole::Verification,
        None,
        1,
        || {
            verify_discovered_portable_v2(&DiscoveryPortableV2Request {
                manifest_json: &manifest_bytes,
                refs_json: &refs_bytes,
                expected_repository: &identity,
                package: &partial,
                discovery_limits: limits,
                portable_limits,
                mode: PortableV2Mode::Full,
                cancelled: None,
            })
            .map_err(portable_error)
            .map(|verified| (verified, Some(object.length), None))
        },
    )?;
    let operation_id = OperationId(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!(
            "{}:{}",
            canonical_name(&identity),
            verified.immutable_version
        )
        .as_bytes(),
    ));
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
    let imported = profile.stage(
        Stage::AtomicImport,
        ComponentKind::PortableImport,
        ComponentRole::Persistence,
        None,
        1,
        || {
            GraphForge::import_portable_v2(
                &destination,
                &PortableV2ImportRequest {
                    input: partial.clone(),
                    operation_id,
                    limits: portable_limits,
                },
                None,
            )
            .map_err(|_| validation("hub.integrity", "portable project import failed"))
            .map(|imported| (imported, Some(object.length), None))
        },
    )?;
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
    profile.stage(
        Stage::Reopen,
        ComponentKind::Recovery,
        ComponentRole::Verification,
        None,
        1,
        || {
            let destination = destination.to_str().ok_or_else(|| {
                validation("hub.destination_conflict", "destination must be UTF-8")
            })?;
            GraphForge::new(Some(destination)).map(|graph| (graph, None, None))
        },
    )?;
    profile.handoff(
        ComponentKind::Recovery,
        ComponentKind::Storage,
        HandoffKind::Call,
        None,
    );
    let staging_root = staging.root.clone();
    drop(staging);
    // The destination is already atomically published; cleanup cannot turn
    // success into a reported failure.
    profile.stage(
        Stage::Cleanup,
        ComponentKind::Storage,
        ComponentRole::Persistence,
        None,
        1,
        || {
            let _ = std::fs::remove_dir_all(staging_root);
            Ok(((), None, None))
        },
    )?;
    Ok(CloneResult {
        contract: "graphforge-hub-clone/1",
        repository: canonical_name(&identity),
        destination: destination.display().to_string(),
        immutable_version: verified.immutable_version,
        package_digest: imported.package_digest,
        generation_uuid: imported.generation_uuid.to_string(),
        resumed_bytes: download.resumed_bytes,
    })
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

fn validation(code: &str, message: &str) -> graphforge_api::GfError {
    graphforge_api::GfError::Validation(format!("{code}: {message}"))
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
            },
            "portable discovery reference mismatch",
        ),
        DiscoveryPortableV2Error::Portable(error) => validation(
            match error.code {
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
            },
            "portable project verification failed",
        ),
    }
}
fn network(message: &str) -> graphforge_api::GfError {
    graphforge_api::GfError::Storage(format!("hub.network: {message}"))
}
fn limit_error(message: &str) -> graphforge_api::GfError {
    validation("hub.limit_exceeded", message)
}
fn storage(error: impl std::fmt::Display) -> graphforge_api::GfError {
    graphforge_api::GfError::Storage(error.to_string())
}
fn hash_reader(reader: &mut impl Read) -> Result<String, graphforge_api::GfError> {
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = reader.read(&mut buffer).map_err(storage)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    let digest = hash.finalize();
    let hex = digest
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            write!(hex, "{byte:02x}").expect("writing to a string cannot fail");
            hex
        });
    Ok(format!("sha256:{hex}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io::{BufRead as _, BufReader};
    use std::net::TcpListener;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    struct Scripted(Mutex<VecDeque<HttpResponse>>);

    impl Scripted {
        fn new(responses: Vec<HttpResponse>) -> Self {
            Self(Mutex::new(responses.into()))
        }

        fn remaining(&self) -> usize {
            self.0.lock().unwrap().len()
        }

        fn replace_last(&self, response: HttpResponse) {
            let mut responses = self.0.lock().unwrap();
            responses.pop_back().unwrap();
            responses.push_back(response);
        }
    }

    impl Transport for Scripted {
        fn get(
            &self,
            _url: &Url,
            _range: Option<u64>,
            _if_range: Option<&str>,
            _limit: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            Ok(self.0.lock().unwrap().pop_front().unwrap())
        }
    }

    struct LoopbackTransport(HttpTransport);

    impl LoopbackTransport {
        fn new() -> Self {
            let config = ureq::Agent::config_builder()
                .https_only(false)
                .http_status_as_error(false)
                .max_redirects(0)
                .proxy(None)
                .timeout_global(Some(Duration::from_secs(2)))
                .build();
            Self(HttpTransport {
                agent: ureq::Agent::with_parts(
                    config,
                    DefaultConnector::new(),
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

    fn response(status: u16, content_range: Option<&str>, body: &[u8]) -> HttpResponse {
        HttpResponse {
            status,
            location: None,
            content_range: content_range.map(str::to_owned),
            etag: Some("\"fixture-1\"".to_owned()),
            body: Box::new(std::io::Cursor::new(body.to_vec())),
        }
    }

    fn object(bytes: &[u8]) -> ObjectDescriptor {
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
        let response =
            fetch_with_attempts(&transport, &start, None, None, 1024, &mut attempts).unwrap();
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
    fn real_http_interruption_resumes_with_range() {
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
        assert!(download(&transport, &descriptor, &destination).is_err());
        assert_eq!(
            download(&transport, &descriptor, &destination).unwrap(),
            DownloadReport {
                resumed_bytes: 8,
                transferred_bytes: (bytes.len() - 8) as u64,
                attempts: 1,
            }
        );
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

    fn clone_script(bundle: &[u8], package_digest: &str) -> Scripted {
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
        let manifest = serde_json::to_vec(&serde_json::json!({
            "format":"graphforge-discovery/1","version":{"major":1,"minor":0},
            "repository":repository,"default_ref":"main","resolved_ref":"main",
            "immutable_version":immutable,
            "package":{"format":"graphforge-project/2","package_digest":package_digest,"object_digest":object_digest},
            "requirements":[{"capability":"portable-v2","major":1}],"capabilities":[{"capability":"range-requests","major":1}],
            "objects":[{"digest":object_digest,"length":bundle.len(),"media_type":graphforge_discovery::PORTABLE_V2_MEDIA_TYPE,"locations":["https://objects.example/project.gfpb"]}]
        })).unwrap();
        Scripted::new(vec![
            response(200, None, &refs),
            response(200, None, &manifest),
            response(200, None, bundle),
        ])
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
        assert_eq!(stage.attempt, 1);
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
        let mut attribution_jobs = Vec::new();
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
            let transport = clone_script(&bundle, &report.package_digest);
            let clone_started = Instant::now();
            run_clone_profiled(
                &transport,
                CloneArgs {
                    repository: input.into(),
                    destination: Some(destination.clone()),
                    telemetry_endpoint: None,
                },
                true,
                &mut output,
                &runtime,
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
                    attribution_jobs.push(job.clone());
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
        assert_eq!(attribution_jobs.len(), 5);
        for (job, (expected_stage, expected_component)) in attribution_jobs.iter().zip([
            (Stage::RefsDiscovery, ComponentKind::NetworkTransport),
            (Stage::Download, ComponentKind::NetworkTransport),
            (Stage::PortableVerification, ComponentKind::PortableVerify),
            (Stage::AtomicImport, ComponentKind::PortableImport),
            (Stage::Reopen, ComponentKind::Recovery),
        ]) {
            let attributed = job
                .stages
                .iter()
                .find(|stage| {
                    stage.stage == expected_stage && stage.component == expected_component
                })
                .unwrap_or_else(|| {
                    panic!(
                        "missing deterministic stage attribution {expected_stage:?}/{expected_component:?}: {:#?}",
                        job.stages
                    )
                });
            assert!(attributed.duration_ns > 0);
        }
    }
}
