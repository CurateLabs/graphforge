//! Verification-first Hub clone orchestration.

use clap::Args;
use fs4::{FileExt, TryLockError};
use graphforge_api::{GraphForge, OperationId, PortableV2ImportRequest};
use graphforge_discovery::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, RefSet,
    RepositoryIdentity,
};
use graphforge_storage::{
    DiscoveryPortableV2Error, DiscoveryPortableV2Mismatch, DiscoveryPortableV2Request,
    PortableV2ErrorCode, PortableV2Limits, PortableV2Mode, verify_discovered_portable_v2,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use url::Url;
use uuid::Uuid;

const HUB_ORIGIN: &str = "https://graphforge.sh";
const MAX_OBJECT_BYTES: u64 = 64 * 1024_u64.pow(3);

#[derive(Args)]
pub(crate) struct CloneArgs {
    /// `owner/repository` or its canonical HTTPS Hub URL.
    source: String,
    /// New destination directory; defaults to the repository name.
    destination: Option<PathBuf>,
}

#[derive(Serialize)]
struct CloneResult<'a> {
    contract: &'static str,
    repository: &'a str,
    resolved_ref: &'a str,
    immutable_version: &'a str,
    package_digest: &'a str,
    generation_uuid: Uuid,
}

trait CloneTransport {
    fn get_bounded(&self, url: &Url, max: usize) -> Result<Vec<u8>, &'static str>;
    fn download_object(
        &self,
        locations: &[String],
        output: &Path,
        length: u64,
        digest: &str,
    ) -> Result<(), &'static str>;
}

struct UreqTransport {
    agent: ureq::Agent,
}

impl UreqTransport {
    fn new() -> Self {
        let config = ureq::Agent::config_builder()
            .https_only(true)
            .max_redirects(0)
            .timeout_global(Some(Duration::from_mins(1)))
            .build();
        Self {
            agent: ureq::Agent::with_parts(
                config,
                ureq::unversioned::transport::DefaultConnector::new(),
                PublicResolver,
            ),
        }
    }
}

impl CloneTransport for UreqTransport {
    fn get_bounded(&self, url: &Url, max: usize) -> Result<Vec<u8>, &'static str> {
        get_bounded(&self.agent, url.clone(), max)
    }

    fn download_object(
        &self,
        locations: &[String],
        output: &Path,
        length: u64,
        digest: &str,
    ) -> Result<(), &'static str> {
        download_object(&self.agent, locations, output, length, digest)
    }
}

pub(crate) fn run_clone(
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), crate::CliRuntimeError> {
    let started = Instant::now();
    let transport = UreqTransport::new();
    let result = run_clone_inner(args, json, output, started, &transport);
    if let Err(crate::CliRuntimeError::Core(error)) = &result {
        emit_clone_metric(
            0,
            started.elapsed(),
            crate::stable_semantic_code(error).unwrap_or("error"),
        );
    } else if result.is_err() {
        emit_clone_metric(0, started.elapsed(), "error");
    }
    result
}

fn run_clone_inner(
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
    started: Instant,
    transport: &dyn CloneTransport,
) -> Result<(), crate::CliRuntimeError> {
    let (repository, base) = resolve_source(&args.source).map_err(failure)?;
    let destination = args
        .destination
        .unwrap_or_else(|| PathBuf::from(&repository.repository));
    ensure_new_destination(&destination).map_err(failure)?;

    let limits = DiscoveryLimits::default();
    let manifest_url = protocol_url(&base, "manifest");
    let refs_url = protocol_url(&base, "refs");
    let manifest_json = transport
        .get_bounded(&manifest_url, limits.max_response_bytes)
        .map_err(failure)?;
    let refs_json = transport
        .get_bounded(&refs_url, limits.max_response_bytes)
        .map_err(failure)?;

    // All discovery semantics and the requested identity are admitted before object I/O.
    let manifest = DiscoveryManifest::from_json(&manifest_json, limits)
        .map_err(|error| discovery_failure(&error))?;
    let refs = RefSet::from_json(&refs_json, limits).map_err(|error| discovery_failure(&error))?;
    if manifest.repository != repository || refs.repository != repository {
        return Err(failure("clone.repository_mismatch").into());
    }
    refs.validate_manifest(&manifest)
        .map_err(|error| discovery_failure(&error))?;
    let object = manifest
        .package_object()
        .map_err(|error| discovery_failure(&error))?;
    if object.length > MAX_OBJECT_BYTES {
        return Err(failure("clone.object_too_large").into());
    }

    let (staging, staging_lock) = acquire_staging(&destination).map_err(failure)?;
    let package = staging.join("package.part");
    transport
        .download_object(&object.locations, &package, object.length, &object.digest.0)
        .map_err(failure)?;

    let verified = verify_discovered_portable_v2(&DiscoveryPortableV2Request {
        manifest_json: &manifest_json,
        refs_json: &refs_json,
        expected_repository: &repository,
        package: &package,
        discovery_limits: limits,
        portable_limits: PortableV2Limits::default(),
        mode: PortableV2Mode::Full,
        cancelled: None,
    })
    .map_err(portable_failure)?;
    let imported = GraphForge::import_portable_v2(
        &destination,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .map_err(|_| failure("clone.import_failed"))?;
    drop(staging_lock);
    // Publication is already complete. A stale private staging directory is
    // resumable/auditable cleanup state, never a reason to report clone failure.
    let _ = fs::remove_dir_all(&staging);

    let name = repository.canonical_name();
    let result = CloneResult {
        contract: "graphforge-clone/1",
        repository: &name,
        resolved_ref: &verified.resolved_ref,
        immutable_version: &verified.immutable_version,
        package_digest: &imported.package_digest,
        generation_uuid: imported.generation_uuid,
    };
    emit_clone_metric(object.length, started.elapsed(), "ok");
    if json {
        serde_json::to_writer(&mut *output, &result).map_err(|_| failure("clone.output_failed"))?;
        writeln!(output).map_err(|_| failure("clone.output_failed"))?;
    } else {
        writeln!(output, "cloned {} into {}", name, destination.display())
            .map_err(|_| failure("clone.output_failed"))?;
    }
    Ok(())
}

fn failure(code: &'static str) -> graphforge_api::GfError {
    graphforge_api::GfError::Storage(format!("{code}: clone failed"))
}

fn discovery_failure(error: &DiscoveryError) -> graphforge_api::GfError {
    let suffix = match error.code {
        DiscoveryErrorCode::InvalidIdentity => "invalid_identity",
        DiscoveryErrorCode::MalformedResponse => "malformed_response",
        DiscoveryErrorCode::UnsupportedFuture => "unsupported_future",
        DiscoveryErrorCode::MissingRef => "missing_ref",
        DiscoveryErrorCode::MissingObject => "missing_object",
        DiscoveryErrorCode::IntegrityFailure => "integrity_failure",
        DiscoveryErrorCode::UnsafeLocation => "unsafe_location",
        DiscoveryErrorCode::LimitExceeded => "limit_exceeded",
        DiscoveryErrorCode::Duplicate => "duplicate",
    };
    let version = error.version.map_or_else(String::new, |version| {
        format!(
            " supported_major={} requested_major={}",
            version
                .supported_major
                .map_or_else(|| "none".to_owned(), |value| value.to_string()),
            version.requested_major
        )
    });
    graphforge_api::GfError::Storage(format!(
        "clone.discovery.{suffix}: {}{version}",
        error.detail()
    ))
}

fn portable_failure(error: DiscoveryPortableV2Error) -> graphforge_api::GfError {
    match error {
        DiscoveryPortableV2Error::Discovery(error) => discovery_failure(&error),
        DiscoveryPortableV2Error::ReferenceMismatch(mismatch) => failure(match mismatch {
            DiscoveryPortableV2Mismatch::Repository => "clone.package.repository_mismatch",
            DiscoveryPortableV2Mismatch::ImmutableVersion => {
                "clone.package.immutable_version_mismatch"
            }
            DiscoveryPortableV2Mismatch::PackageDigest => "clone.package.package_digest_mismatch",
        }),
        DiscoveryPortableV2Error::Portable(error) => {
            let code = match error.code {
                PortableV2ErrorCode::Cancelled => "clone.package.cancelled",
                PortableV2ErrorCode::LimitExceeded => "clone.package.limit_exceeded",
                PortableV2ErrorCode::Io => "clone.package.io",
                PortableV2ErrorCode::InvalidStructure => "clone.package.invalid_structure",
                PortableV2ErrorCode::InvalidPath => "clone.package.invalid_path",
                PortableV2ErrorCode::DuplicateEntry => "clone.package.duplicate_entry",
                PortableV2ErrorCode::UnsupportedFuture => "clone.package.unsupported_future",
                PortableV2ErrorCode::Incompatible => "clone.package.incompatible",
                PortableV2ErrorCode::DigestMismatch => "clone.package.digest_mismatch",
                PortableV2ErrorCode::ConcurrentMutation => "clone.package.concurrent_mutation",
            };
            failure(code)
        }
    }
}

fn ensure_new_destination(path: &Path) -> Result<(), &'static str> {
    if path.as_os_str().is_empty() || path.exists() {
        return Err("clone.destination_exists");
    }
    let parent = destination_parent(path);
    if !parent.is_dir() {
        return Err("clone.destination_parent_missing");
    }
    Ok(())
}

fn destination_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn staging_path(destination: &Path) -> Result<PathBuf, &'static str> {
    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or("clone.destination_invalid")?;
    Ok(destination_parent(destination).join(format!(".{name}.graphforge-clone")))
}

fn acquire_staging(destination: &Path) -> Result<(PathBuf, File), &'static str> {
    let staging = staging_path(destination)?;
    match fs::symlink_metadata(&staging) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err("clone.staging_unsafe");
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_private_directory(&staging)?;
        }
        Err(_) => return Err("clone.staging_failed"),
    }
    let metadata = fs::symlink_metadata(&staging).map_err(|_| "clone.staging_failed")?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("clone.staging_unsafe");
    }
    make_staging_private(&staging)?;

    let lock_path = staging.join("clone.lock");
    if fs::symlink_metadata(&lock_path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err("clone.staging_unsafe");
    }
    let lock = open_private_lock(&lock_path)?;
    match FileExt::try_lock(&lock) {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Err("clone.concurrent_clone"),
        Err(TryLockError::Error(_)) => return Err("clone.staging_failed"),
    }
    Ok((staging, lock))
}

#[cfg(unix)]
fn create_private_directory(path: &Path) -> Result<(), &'static str> {
    use std::os::unix::fs::DirBuilderExt;
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            "clone.staging_unsafe"
        } else {
            "clone.staging_failed"
        }
    })
}

#[cfg(unix)]
fn make_staging_private(path: &Path) -> Result<(), &'static str> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|_| "clone.staging_failed")
}

#[cfg(not(unix))]
fn make_staging_private(_path: &Path) -> Result<(), &'static str> {
    Ok(())
}

#[cfg(not(unix))]
fn create_private_directory(path: &Path) -> Result<(), &'static str> {
    fs::create_dir(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            "clone.staging_unsafe"
        } else {
            "clone.staging_failed"
        }
    })
}

#[cfg(unix)]
fn open_private_lock(path: &Path) -> Result<File, &'static str> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)
        .map_err(|_| "clone.staging_failed")
}

#[cfg(not(unix))]
fn open_private_lock(path: &Path) -> Result<File, &'static str> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|_| "clone.staging_failed")
}

fn resolve_source(source: &str) -> Result<(RepositoryIdentity, Url), &'static str> {
    if !source.contains("://") {
        let repository = RepositoryIdentity::parse(source).map_err(|_| "clone.source_invalid")?;
        let base =
            Url::parse(&format!("{HUB_ORIGIN}/{source}")).map_err(|_| "clone.source_invalid")?;
        return Ok((repository, base));
    }
    let base = Url::parse(source).map_err(|_| "clone.source_invalid")?;
    if base.scheme() != "https"
        || !base.username().is_empty()
        || base.password().is_some()
        || base.query().is_some()
        || base.fragment().is_some()
    {
        return Err("clone.source_unsafe");
    }
    validate_public_host(&base)?;
    let segments: Vec<_> = base
        .path_segments()
        .ok_or("clone.source_invalid")?
        .filter(|segment| !segment.is_empty())
        .collect();
    if segments.len() != 2 {
        return Err("clone.source_invalid");
    }
    let repository = RepositoryIdentity::parse(&format!("{}/{}", segments[0], segments[1]))
        .map_err(|_| "clone.source_invalid")?;
    Ok((repository, base))
}

fn validate_public_host(url: &Url) -> Result<(), &'static str> {
    let host = url.host_str().ok_or("clone.source_unsafe")?;
    if host.eq_ignore_ascii_case("localhost")
        || host.to_ascii_lowercase().ends_with(".localhost")
        || host.to_ascii_lowercase().ends_with(".local")
    {
        return Err("clone.source_unsafe");
    }
    if let Ok(ip) = host.parse::<IpAddr>()
        && !is_public_ip(ip)
    {
        return Err("clone.source_unsafe");
    }
    Ok(())
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_broadcast()
                || a == 0
                || (a == 100 && (64..=127).contains(&b))
                || (a == 192 && b == 0 && c == 2)
                || (a == 198 && (b == 18 || b == 19 || b == 51))
                || (a == 203 && b == 0 && c == 113)
                || a >= 240)
        }
        IpAddr::V6(ip) => {
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8)
        }
    }
}

#[derive(Debug)]
struct PublicResolver;

impl ureq::unversioned::resolver::Resolver for PublicResolver {
    fn resolve(
        &self,
        uri: &ureq::http::Uri,
        config: &ureq::config::Config,
        timeout: ureq::unversioned::transport::NextTimeout,
    ) -> Result<ureq::unversioned::resolver::ResolvedSocketAddrs, ureq::Error> {
        use ureq::unversioned::resolver::{DefaultResolver, Resolver};
        let resolved = Resolver::resolve(&DefaultResolver::default(), uri, config, timeout)?;
        let mut admitted = self.empty();
        for &address in &resolved {
            if is_public_ip(address.ip()) {
                admitted.push(address);
            }
        }
        if admitted.is_empty() {
            Err(ureq::Error::HostNotFound)
        } else {
            Ok(admitted)
        }
    }
}

fn protocol_url(base: &Url, leaf: &str) -> Url {
    let mut result = base.clone();
    result.set_path(&format!("{}/.gf/{leaf}", base.path().trim_end_matches('/')));
    result
}

fn get_bounded(agent: &ureq::Agent, url: Url, max: usize) -> Result<Vec<u8>, &'static str> {
    let mut response = get_with_safe_redirects(agent, url, None, false)?;
    let mut bytes = Vec::new();
    response
        .body_mut()
        .as_reader()
        .take((max + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "clone.transport_failed")?;
    if bytes.len() > max {
        return Err("clone.response_too_large");
    }
    Ok(bytes)
}

fn get_with_safe_redirects(
    agent: &ureq::Agent,
    mut url: Url,
    range: Option<(u64, &str)>,
    allow_http_test_transport: bool,
) -> Result<ureq::http::Response<ureq::Body>, &'static str> {
    for redirects in 0..=3 {
        if !allow_http_test_transport {
            validate_public_host(&url)?;
        }
        let request = agent.get(url.as_str());
        let response = if let Some((offset, validator)) = range {
            request
                .header("Range", format!("bytes={offset}-"))
                .header("If-Range", validator)
                .call()
        } else {
            request.call()
        }
        .map_err(|_| "clone.transport_failed")?;
        if !response.status().is_redirection() {
            return Ok(response);
        }
        if redirects == 3 {
            return Err("clone.redirect_limit");
        }
        let location = response
            .headers()
            .get(ureq::http::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .ok_or("clone.redirect_invalid")?;
        let next = url.join(location).map_err(|_| "clone.redirect_invalid")?;
        if (!allow_http_test_transport && next.scheme() != "https")
            || !next.username().is_empty()
            || next.password().is_some()
        {
            return Err("clone.redirect_unsafe");
        }
        if next.query().is_some() || next.fragment().is_some() {
            return Err("clone.redirect_unsafe");
        }
        if !allow_http_test_transport {
            validate_public_host(&next)?;
        }
        url = next;
    }
    Err("clone.redirect_limit")
}

fn download_object(
    agent: &ureq::Agent,
    locations: &[String],
    output: &Path,
    length: u64,
    digest: &str,
) -> Result<(), &'static str> {
    let mut last = "clone.transport_failed";
    for location in locations {
        match download_one(agent, location, output, length, digest, false) {
            Ok(()) => return Ok(()),
            Err(error) => {
                last = error;
            }
        }
    }
    Err(last)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ResumeState {
    version: u8,
    object_digest: String,
    object_length: u64,
    location: String,
    etag: Option<String>,
}

fn download_one(
    agent: &ureq::Agent,
    location: &str,
    output: &Path,
    expected_length: u64,
    expected_digest: &str,
    allow_http_test_transport: bool,
) -> Result<(), &'static str> {
    let url = Url::parse(location).map_err(|_| "clone.location_unsafe")?;
    if (!allow_http_test_transport && url.scheme() != "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("clone.location_unsafe");
    }
    if !allow_http_test_transport {
        validate_public_host(&url)?;
    }
    let checkpoint = output.with_extension("resume.json");
    let saved = read_resume_state(&checkpoint);
    let mut offset = fs::metadata(output).map_or(0, |metadata| metadata.len());
    let validator = saved.as_ref().and_then(|state| {
        (state.version == 1
            && state.object_digest == expected_digest
            && state.object_length == expected_length
            && state.location == location
            && offset < expected_length)
            .then_some(state.etag.as_deref())
            .flatten()
    });
    if offset > 0 && validator.is_none() {
        offset = 0;
    }
    let mut response = get_with_safe_redirects(
        agent,
        url,
        validator.map(|value| (offset, value)),
        allow_http_test_transport,
    )?;
    let resumed = offset > 0 && response.status() == ureq::http::StatusCode::PARTIAL_CONTENT;
    if resumed {
        let expected_range = format!("bytes {offset}-{}/{expected_length}", expected_length - 1);
        let actual_range = response
            .headers()
            .get(ureq::http::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .ok_or("clone.range_invalid")?;
        if actual_range != expected_range {
            return Err("clone.range_invalid");
        }
    } else {
        offset = 0;
    }
    let remaining = expected_length - offset;
    if response
        .body()
        .content_length()
        .is_some_and(|value| value != remaining)
    {
        return Err("clone.length_mismatch");
    }
    let etag = response
        .headers()
        .get(ureq::http::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.starts_with("W/") && value.starts_with('"') && value.ends_with('"'))
        .map(str::to_owned);
    write_resume_state(
        &checkpoint,
        &ResumeState {
            version: 1,
            object_digest: expected_digest.to_owned(),
            object_length: expected_length,
            location: location.to_owned(),
            etag,
        },
    )?;
    let mut hasher = Sha256::new();
    if offset > 0 {
        let mut existing = File::open(output).map_err(|_| "clone.staging_failed")?;
        std::io::copy(&mut existing, &mut DigestWriter(&mut hasher))
            .map_err(|_| "clone.staging_failed")?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(resumed)
        .truncate(!resumed)
        .open(output)
        .map_err(|_| "clone.staging_failed")?;
    let mut reader = response.body_mut().as_reader();
    copy_resumed_verified(
        &mut reader,
        &mut file,
        &mut hasher,
        offset,
        expected_length,
        expected_digest,
    )?;
    file.sync_all().map_err(|_| "clone.staging_failed")?;
    let _ = fs::remove_file(checkpoint);
    Ok(())
}

struct DigestWriter<'a>(&'a mut Sha256);

impl Write for DigestWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn read_resume_state(path: &Path) -> Option<ResumeState> {
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_resume_state(path: &Path, state: &ResumeState) -> Result<(), &'static str> {
    let temporary = path.with_extension("resume.json.tmp");
    let bytes = serde_json::to_vec(state).map_err(|_| "clone.staging_failed")?;
    fs::write(&temporary, bytes).map_err(|_| "clone.staging_failed")?;
    fs::rename(temporary, path).map_err(|_| "clone.staging_failed")
}

fn copy_resumed_verified(
    reader: &mut dyn Read,
    output: &mut dyn Write,
    hasher: &mut Sha256,
    initial_length: u64,
    expected_length: u64,
    expected_digest: &str,
) -> Result<(), &'static str> {
    let mut total = initial_length;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = reader
            .read(&mut buffer)
            .map_err(|_| "clone.transport_failed")?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .ok_or("clone.object_too_large")?;
        if total > expected_length {
            return Err("clone.length_mismatch");
        }
        output
            .write_all(&buffer[..count])
            .map_err(|_| "clone.staging_failed")?;
        hasher.update(&buffer[..count]);
    }
    if total != expected_length {
        return Err("clone.length_mismatch");
    }
    validate_digest(hasher, expected_digest)
}

fn validate_digest(hasher: &mut Sha256, expected_digest: &str) -> Result<(), &'static str> {
    let actual =
        hasher
            .clone()
            .finalize()
            .iter()
            .fold(String::from("sha256:"), |mut output, byte| {
                use std::fmt::Write as _;
                write!(output, "{byte:02x}").expect("writing to String cannot fail");
                output
            });
    if actual != expected_digest {
        return Err("clone.digest_mismatch");
    }
    Ok(())
}

#[cfg(test)]
fn copy_verified(
    reader: &mut dyn Read,
    output: &mut dyn Write,
    expected_length: u64,
    expected_digest: &str,
) -> Result<(), &'static str> {
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = reader
            .read(&mut buffer)
            .map_err(|_| "clone.transport_failed")?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .ok_or("clone.object_too_large")?;
        if total > expected_length {
            return Err("clone.length_mismatch");
        }
        output
            .write_all(&buffer[..count])
            .map_err(|_| "clone.staging_failed")?;
        hasher.update(&buffer[..count]);
    }
    if total != expected_length {
        return Err("clone.length_mismatch");
    }
    let actual = hasher
        .finalize()
        .iter()
        .fold(String::from("sha256:"), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        });
    if actual != expected_digest {
        return Err("clone.digest_mismatch");
    }
    Ok(())
}

/// Best-effort OTel-exporter handoff. Values deliberately exclude source,
/// repository, host, URL, ref, digest, destination, user, and credentials.
fn emit_clone_metric(bytes: u64, duration: Duration, result: &str) {
    let Ok(path) = std::env::var("GRAPHFORGE_OTEL_JSONL") else {
        return;
    };
    let event = serde_json::json!({
        "name": "graphforge.clone",
        "operation_class": "repository_clone",
        "result": result,
        "bytes": bytes,
        "duration_ms": u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
    });
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = serde_json::to_writer(&mut file, &event);
        let _ = writeln!(file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphforge_api::{PortableSelection, PortableV2ExportRequest};
    use graphforge_discovery::{
        DISCOVERY_FORMAT, ObjectDescriptor, PORTABLE_V2_FORMAT, PORTABLE_V2_MEDIA_TYPE,
        PortablePackageReference, ProtocolVersion, RepositoryRef, Sha256Digest,
    };
    use graphforge_storage::{PortableV2Output, PortableV2SelectionProfile};
    use std::collections::{BTreeMap, HashMap};
    use std::net::TcpListener;
    use std::sync::Mutex;
    use std::thread;

    struct LocalHttpTransport {
        agent: ureq::Agent,
        origin: String,
        requested: Mutex<Vec<String>>,
    }

    impl LocalHttpTransport {
        fn new(address: std::net::SocketAddr) -> Self {
            Self {
                agent: ureq::Agent::config_builder()
                    .https_only(false)
                    .max_redirects(0)
                    .timeout_global(Some(Duration::from_secs(5)))
                    .build()
                    .into(),
                origin: format!("http://{address}"),
                requested: Mutex::new(Vec::new()),
            }
        }

        fn mapped(&self, url: &Url) -> String {
            self.requested.lock().unwrap().push(url.to_string());
            format!("{}{}", self.origin, url.path())
        }
    }

    impl CloneTransport for LocalHttpTransport {
        fn get_bounded(&self, url: &Url, max: usize) -> Result<Vec<u8>, &'static str> {
            let mapped = self.mapped(url);
            let mut response = self
                .agent
                .get(&mapped)
                .call()
                .map_err(|_| "clone.transport_failed")?;
            let mut bytes = Vec::new();
            response
                .body_mut()
                .as_reader()
                .take((max + 1) as u64)
                .read_to_end(&mut bytes)
                .map_err(|_| "clone.transport_failed")?;
            if bytes.len() > max {
                return Err("clone.response_too_large");
            }
            Ok(bytes)
        }

        fn download_object(
            &self,
            locations: &[String],
            output: &Path,
            length: u64,
            digest: &str,
        ) -> Result<(), &'static str> {
            let logical = Url::parse(locations.first().ok_or("clone.object_missing")?)
                .map_err(|_| "clone.location_unsafe")?;
            let mapped = self.mapped(&logical);
            download_one(&self.agent, &mapped, output, length, digest, true)
        }
    }

    fn serve_routes(
        routes: HashMap<String, Vec<u8>>,
        request_count: usize,
    ) -> (std::net::SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            for _ in 0..request_count {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0_u8; 4096];
                let read = stream.read(&mut request).unwrap();
                let first = String::from_utf8_lossy(&request[..read]);
                let path = first
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap();
                let body = routes.get(path).unwrap();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"fixture-1\"\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                stream.write_all(body).unwrap();
            }
        });
        (address, handle)
    }

    fn digest(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .fold(String::from("sha256:"), |mut output, byte| {
                use std::fmt::Write as _;
                write!(output, "{byte:02x}").unwrap();
                output
            })
    }

    fn discovery_documents(
        package: &[u8],
        package_digest: &str,
        version: ProtocolVersion,
    ) -> (Vec<u8>, Vec<u8>) {
        let repository = RepositoryIdentity::parse("acme/demo").unwrap();
        let immutable = Sha256Digest(format!("sha256:{}", "a".repeat(64)));
        let object_digest = Sha256Digest(digest(package));
        let manifest = DiscoveryManifest {
            format: DISCOVERY_FORMAT.to_owned(),
            version,
            repository: repository.clone(),
            default_ref: "main".to_owned(),
            resolved_ref: "main".to_owned(),
            immutable_version: immutable.clone(),
            package: PortablePackageReference {
                format: PORTABLE_V2_FORMAT.to_owned(),
                package_digest: Sha256Digest(package_digest.to_owned()),
                object_digest: object_digest.clone(),
            },
            requirements: Vec::new(),
            capabilities: Vec::new(),
            objects: vec![ObjectDescriptor {
                digest: object_digest,
                length: package.len() as u64,
                media_type: PORTABLE_V2_MEDIA_TYPE.to_owned(),
                locations: vec!["https://objects.test/object".to_owned()],
            }],
            extensions: BTreeMap::new(),
        };
        let refs = RefSet {
            format: DISCOVERY_FORMAT.to_owned(),
            version,
            repository,
            default_ref: "main".to_owned(),
            refs: vec![RepositoryRef {
                name: "main".to_owned(),
                target: immutable,
                validator: Sha256Digest(format!("sha256:{}", "b".repeat(64))),
            }],
            extensions: BTreeMap::new(),
        };
        (
            serde_json::to_vec(&manifest).unwrap(),
            serde_json::to_vec(&refs).unwrap(),
        )
    }

    #[test]
    fn resolves_short_and_canonical_sources() {
        let (short, short_url) = resolve_source("openalex/openalex").unwrap();
        let (full, full_url) = resolve_source("https://graphforge.sh/openalex/openalex").unwrap();
        assert_eq!(short, full);
        assert_eq!(short_url, full_url);
        assert_eq!(
            protocol_url(&full_url, "manifest").as_str(),
            "https://graphforge.sh/openalex/openalex/.gf/manifest"
        );
    }

    #[test]
    fn staging_lock_rejects_a_concurrent_clone_and_releases_on_drop() {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("clone");
        let (_, first) = acquire_staging(&destination).unwrap();
        assert_eq!(
            acquire_staging(&destination).unwrap_err(),
            "clone.concurrent_clone"
        );
        drop(first);
        assert!(acquire_staging(&destination).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn staging_rejects_precreated_symlink() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("clone");
        let staging = staging_path(&destination).unwrap();
        let redirect = root.path().join("redirect");
        fs::create_dir(&redirect).unwrap();
        symlink(&redirect, &staging).unwrap();
        assert_eq!(
            acquire_staging(&destination).unwrap_err(),
            "clone.staging_unsafe"
        );
        assert!(fs::read_dir(redirect).unwrap().next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn staging_rejects_symlinked_lock_file() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("clone");
        let staging = staging_path(&destination).unwrap();
        fs::create_dir(&staging).unwrap();
        let redirect = root.path().join("redirect");
        fs::write(&redirect, b"untouched").unwrap();
        symlink(&redirect, staging.join("clone.lock")).unwrap();
        assert_eq!(
            acquire_staging(&destination).unwrap_err(),
            "clone.staging_unsafe"
        );
        assert_eq!(fs::read(redirect).unwrap(), b"untouched");
    }

    #[test]
    fn rejects_unsafe_sources_and_existing_destinations() {
        assert_eq!(
            resolve_source("http://graphforge.sh/a/b").unwrap_err(),
            "clone.source_unsafe"
        );
        assert_eq!(
            resolve_source("https://127.0.0.1/a/b").unwrap_err(),
            "clone.source_unsafe"
        );
        assert!(!is_public_ip("169.254.169.254".parse().unwrap()));
        assert!(!is_public_ip("fd00::1".parse().unwrap()));
        assert!(is_public_ip("1.1.1.1".parse().unwrap()));
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(
            ensure_new_destination(temp.path()).unwrap_err(),
            "clone.destination_exists"
        );
        assert!(ensure_new_destination(Path::new("new-repository")).is_ok());
    }

    #[test]
    fn transport_length_and_digest_are_both_required() {
        let bytes = b"portable bytes";
        let digest =
            Sha256::digest(bytes)
                .iter()
                .fold(String::from("sha256:"), |mut output, byte| {
                    use std::fmt::Write as _;
                    write!(output, "{byte:02x}").unwrap();
                    output
                });
        let mut accepted = Vec::new();
        copy_verified(
            &mut bytes.as_slice(),
            &mut accepted,
            bytes.len() as u64,
            &digest,
        )
        .unwrap();
        assert_eq!(accepted, bytes);

        assert_eq!(
            copy_verified(&mut bytes.as_slice(), &mut Vec::new(), 1, &digest).unwrap_err(),
            "clone.length_mismatch"
        );
        assert_eq!(
            copy_verified(
                &mut bytes.as_slice(),
                &mut Vec::new(),
                bytes.len() as u64,
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap_err(),
            "clone.digest_mismatch"
        );
    }

    #[test]
    fn resumed_stream_hashes_the_existing_prefix() {
        let prefix = b"portable ";
        let suffix = b"bytes";
        let complete = [prefix.as_slice(), suffix.as_slice()].concat();
        let digest =
            Sha256::digest(&complete)
                .iter()
                .fold(String::from("sha256:"), |mut output, byte| {
                    use std::fmt::Write as _;
                    write!(output, "{byte:02x}").unwrap();
                    output
                });
        let mut hasher = Sha256::new();
        hasher.update(prefix);
        let mut output = prefix.to_vec();
        copy_resumed_verified(
            &mut suffix.as_slice(),
            &mut output,
            &mut hasher,
            prefix.len() as u64,
            complete.len() as u64,
            &digest,
        )
        .unwrap();
        assert_eq!(output, complete);
    }

    #[test]
    fn json_and_text_failures_keep_stable_clone_codes() {
        let json = crate::execute(["gf", "--json", "clone", "http://graphforge.sh/a/b"]);
        assert_ne!(json.exit_code, 0);
        let envelope: serde_json::Value = serde_json::from_slice(&json.stderr).unwrap();
        assert_eq!(
            envelope["error"]["details"]["semantic_code"],
            "clone.source_unsafe"
        );
        let text = crate::execute(["gf", "clone", "http://graphforge.sh/a/b"]);
        assert!(
            String::from_utf8(text.stderr)
                .unwrap()
                .contains("clone.source_unsafe")
        );
    }

    #[test]
    fn local_http_interruption_resumes_with_validated_range() {
        let bytes = b"portable package bytes".to_vec();
        let digest =
            Sha256::digest(&bytes)
                .iter()
                .fold(String::from("sha256:"), |mut output, byte| {
                    use std::fmt::Write as _;
                    write!(output, "{byte:02x}").unwrap();
                    output
                });
        let prefix_len = 9_usize;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let served = bytes.clone();
        let server = thread::spawn(move || {
            for request_number in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0_u8; 4096];
                let read = stream.read(&mut request).unwrap();
                let request = String::from_utf8_lossy(&request[..read]);
                if request_number == 0 {
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"immutable-1\"\r\nConnection: close\r\n\r\n",
                        served.len()
                    )
                    .unwrap();
                    stream.write_all(&served[..prefix_len]).unwrap();
                } else {
                    let request = request.to_ascii_lowercase();
                    assert!(request.contains(&format!("range: bytes={prefix_len}-")));
                    assert!(request.contains("if-range: \"immutable-1\""));
                    write!(
                        stream,
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {prefix_len}-{}/{}\r\nETag: \"immutable-1\"\r\nConnection: close\r\n\r\n",
                        served.len() - prefix_len,
                        served.len() - 1,
                        served.len()
                    )
                    .unwrap();
                    stream.write_all(&served[prefix_len..]).unwrap();
                }
            }
        });
        let config = ureq::Agent::config_builder()
            .https_only(false)
            .max_redirects(0)
            .timeout_global(Some(Duration::from_secs(5)))
            .build();
        let agent: ureq::Agent = config.into();
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("package.part");
        let location = format!("http://{address}/object");
        assert_eq!(
            download_one(
                &agent,
                &location,
                &output,
                bytes.len() as u64,
                &digest,
                true,
            )
            .unwrap_err(),
            "clone.transport_failed"
        );
        assert_eq!(fs::metadata(&output).unwrap().len(), prefix_len as u64);
        download_one(
            &agent,
            &location,
            &output,
            bytes.len() as u64,
            &digest,
            true,
        )
        .unwrap();
        assert_eq!(fs::read(output).unwrap(), bytes);
        server.join().unwrap();
    }

    #[test]
    fn integrated_clone_imports_and_reopens_while_failures_publish_no_destination() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        fs::create_dir(&source).unwrap();
        let graph = GraphForge::new(source.to_str()).unwrap();
        let bundle = root.path().join("source.gfpb");
        let exported = graph
            .export_portable_v2(
                &PortableV2ExportRequest {
                    selection: PortableSelection::Current,
                    output_path: bundle.clone(),
                    representation: PortableV2Output::Bundle,
                    profile: PortableV2SelectionProfile::Complete,
                    subset: None,
                    limits: PortableV2Limits::default(),
                },
                None,
                |_| {},
            )
            .unwrap();
        let package = fs::read(bundle).unwrap();
        let (manifest, refs) =
            discovery_documents(&package, &exported.package_digest, ProtocolVersion::CURRENT);

        let mut routes = HashMap::new();
        routes.insert("/acme/demo/.gf/manifest".to_owned(), manifest.clone());
        routes.insert("/acme/demo/.gf/refs".to_owned(), refs.clone());
        routes.insert("/object".to_owned(), package.clone());
        let (address, server) = serve_routes(routes, 3);
        let transport = LocalHttpTransport::new(address);
        let destination = root.path().join("clone");
        let mut output = Vec::new();
        let result = run_clone_inner(
            CloneArgs {
                source: "https://hub.test/acme/demo".to_owned(),
                destination: Some(destination.clone()),
            },
            true,
            &mut output,
            Instant::now(),
            &transport,
        );
        assert!(result.is_ok());
        server.join().unwrap();
        let receipt: serde_json::Value = serde_json::from_slice(&output).unwrap();
        let _reopened = GraphForge::new(destination.to_str()).unwrap();
        assert!(receipt["generation_uuid"].as_str().is_some());
        let requested = transport.requested.lock().unwrap();
        assert_eq!(requested[0], "https://hub.test/acme/demo/.gf/manifest");
        assert_eq!(requested[1], "https://hub.test/acme/demo/.gf/refs");
        assert_eq!(requested[2], "https://objects.test/object");

        let mut corrupt_routes = HashMap::new();
        corrupt_routes.insert("/acme/demo/.gf/manifest".to_owned(), manifest);
        corrupt_routes.insert("/acme/demo/.gf/refs".to_owned(), refs);
        corrupt_routes.insert("/object".to_owned(), vec![0_u8; package.len()]);
        let (address, server) = serve_routes(corrupt_routes, 3);
        let corrupt_destination = root.path().join("corrupt-clone");
        let result = run_clone_inner(
            CloneArgs {
                source: "https://hub.test/acme/demo".to_owned(),
                destination: Some(corrupt_destination.clone()),
            },
            true,
            &mut Vec::new(),
            Instant::now(),
            &LocalHttpTransport::new(address),
        );
        assert!(result.is_err());
        server.join().unwrap();
        assert!(!corrupt_destination.exists());

        let (future_manifest, future_refs) = discovery_documents(
            &package,
            &exported.package_digest,
            ProtocolVersion {
                major: 99,
                minor: 0,
            },
        );
        let mut future_routes = HashMap::new();
        future_routes.insert("/acme/demo/.gf/manifest".to_owned(), future_manifest);
        future_routes.insert("/acme/demo/.gf/refs".to_owned(), future_refs);
        let (address, server) = serve_routes(future_routes, 2);
        let future_transport = LocalHttpTransport::new(address);
        let future_destination = root.path().join("future-clone");
        let result = run_clone_inner(
            CloneArgs {
                source: "https://hub.test/acme/demo".to_owned(),
                destination: Some(future_destination.clone()),
            },
            true,
            &mut Vec::new(),
            Instant::now(),
            &future_transport,
        );
        assert!(result.is_err());
        server.join().unwrap();
        assert_eq!(future_transport.requested.lock().unwrap().len(), 2);
        assert!(!future_destination.exists());
    }
}
