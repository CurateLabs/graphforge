//! Secure Hub discovery, download, and atomic portable-v2 import.

use clap::Args;
use fs4::{FileExt, TryLockError};
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
use std::fmt::Debug;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;
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

fn fetch(
    transport: &dyn Transport,
    start: &Url,
    range: Option<u64>,
    if_range: Option<&str>,
    limit: u64,
) -> Result<HttpResponse, graphforge_api::GfError> {
    let mut url = start.clone();
    for hop in 0..=MAX_REDIRECTS {
        transport.validate(&url)?;
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

fn download(
    transport: &dyn Transport,
    object: &ObjectDescriptor,
    partial: &Path,
) -> Result<u64, graphforge_api::GfError> {
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
    if resumed < object.length {
        let mut response = fetch(
            transport,
            &url,
            (resumed > 0).then_some(resumed),
            validator,
            object.length.saturating_sub(resumed),
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
    Ok(resumed)
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
    run_clone_with(&HttpTransport::new(), args, json, output)
}

fn run_clone_with(
    transport: &dyn Transport,
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let (identity, base) = parse_input(&args.repository)?;
    let destination = args
        .destination
        .unwrap_or_else(|| PathBuf::from(&identity.repository));
    ensure_destination_absent(&destination)?;
    let refs_bytes = read_bounded(
        fetch(
            transport,
            &endpoint(&base, "refs"),
            None,
            None,
            MAX_METADATA_BYTES as u64,
        )?,
        MAX_METADATA_BYTES,
    )?;
    let manifest_bytes = read_bounded(
        fetch(
            transport,
            &endpoint(&base, "manifest"),
            None,
            None,
            MAX_METADATA_BYTES as u64,
        )?,
        MAX_METADATA_BYTES,
    )?;
    let limits = DiscoveryLimits {
        max_response_bytes: MAX_METADATA_BYTES,
        max_cumulative_object_bytes: MAX_BUNDLE_BYTES,
        ..DiscoveryLimits::default()
    };
    let refs = RefSet::from_json(&refs_bytes, limits).map_err(|error| protocol_error(&error))?;
    let manifest = DiscoveryManifest::from_json(&manifest_bytes, limits)
        .map_err(|error| protocol_error(&error))?;
    if refs.repository != identity || manifest.repository != identity {
        return Err(validation("hub.integrity", "discovery repository mismatch"));
    }
    refs.validate_manifest(&manifest)
        .map_err(|error| protocol_error(&error))?;
    let object = select_bundle(&manifest)?;
    let staging = acquire_staging(&destination)?;
    let partial = staging.partial.clone();
    let resumed_bytes = download(transport, object, &partial)?;
    let portable_limits = PortableV2Limits::default();
    let verified = verify_discovered_portable_v2(&DiscoveryPortableV2Request {
        manifest_json: &manifest_bytes,
        refs_json: &refs_bytes,
        expected_repository: &identity,
        package: &partial,
        discovery_limits: limits,
        portable_limits,
        mode: PortableV2Mode::Full,
        cancelled: None,
    })
    .map_err(portable_error)?;
    let operation_id = OperationId(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!(
            "{}:{}",
            canonical_name(&identity),
            verified.immutable_version
        )
        .as_bytes(),
    ));
    let imported = GraphForge::import_portable_v2(
        &destination,
        &PortableV2ImportRequest {
            input: partial.clone(),
            operation_id,
            limits: portable_limits,
        },
        None,
    )
    .map_err(|_| validation("hub.integrity", "portable project import failed"))?;
    let staging_root = staging.root.clone();
    drop(staging);
    // The destination is already atomically published; cleanup cannot turn
    // success into a reported failure.
    let _ = std::fs::remove_dir_all(staging_root);
    let result = CloneResult {
        contract: "graphforge-hub-clone/1",
        repository: canonical_name(&identity),
        destination: destination.display().to_string(),
        immutable_version: verified.immutable_version,
        package_digest: imported.package_digest,
        generation_uuid: imported.generation_uuid.to_string(),
        resumed_bytes,
    };
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
        let error = match fetch(&transport, &start, None, None, 1024) {
            Ok(_) => panic!("private redirect unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("hub.unsafe_location"));
        assert_eq!(
            transport.remaining(),
            1,
            "private target was never requested"
        );
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
        assert_eq!(download(&second, &descriptor, &destination).unwrap(), 8);
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
        assert_eq!(download(&transport, &descriptor, &partial).unwrap(), 0);
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
        assert_eq!(download(&transport, &descriptor, &destination).unwrap(), 8);
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

        for (index, input) in [
            "openalex/openalex",
            "https://graphforge.sh/openalex/openalex",
        ]
        .into_iter()
        .enumerate()
        {
            let destination = root.path().join(format!("clone-{index}"));
            let mut output = Vec::new();
            run_clone_with(
                &clone_script(&bundle, &report.package_digest),
                CloneArgs {
                    repository: input.into(),
                    destination: Some(destination.clone()),
                },
                true,
                &mut output,
            )
            .unwrap();
            let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(result["contract"], "graphforge-hub-clone/1");
            assert_eq!(result["package_digest"], report.package_digest);
            GraphForge::new(destination.to_str()).expect("cloned project reopens through facade");
        }
    }
}
