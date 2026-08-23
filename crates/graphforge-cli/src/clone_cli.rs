//! Verification-first Hub clone orchestration.

use clap::Args;
use graphforge_api::{GraphForge, OperationId, PortableV2ImportRequest};
use graphforge_discovery::{DiscoveryLimits, DiscoveryManifest, RefSet, RepositoryIdentity};
use graphforge_storage::{
    DiscoveryPortableV2Request, PortableV2Limits, PortableV2Mode, verify_discovered_portable_v2,
};
use serde::Serialize;
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

pub(crate) fn run_clone(
    args: CloneArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), crate::CliRuntimeError> {
    let started = Instant::now();
    let (repository, base) = resolve_source(&args.source).map_err(failure)?;
    let destination = args
        .destination
        .unwrap_or_else(|| PathBuf::from(&repository.repository));
    ensure_new_destination(&destination).map_err(failure)?;

    let agent: ureq::Agent = ureq::Agent::config_builder()
        .https_only(true)
        // Redirects are followed below so every hop is re-admitted by host policy.
        .max_redirects(0)
        .timeout_global(Some(Duration::from_mins(1)))
        .build()
        .into();
    let limits = DiscoveryLimits::default();
    let manifest_url = protocol_url(&base, "manifest");
    let refs_url = protocol_url(&base, "refs");
    let manifest_json =
        get_bounded(&agent, manifest_url, limits.max_response_bytes).map_err(failure)?;
    let refs_json = get_bounded(&agent, refs_url, limits.max_response_bytes).map_err(failure)?;

    // All discovery semantics and the requested identity are admitted before object I/O.
    let manifest = DiscoveryManifest::from_json(&manifest_json, limits)
        .map_err(|_| failure("clone.discovery_invalid"))?;
    let refs =
        RefSet::from_json(&refs_json, limits).map_err(|_| failure("clone.discovery_invalid"))?;
    if manifest.repository != repository || refs.repository != repository {
        return Err(failure("clone.repository_mismatch").into());
    }
    refs.validate_manifest(&manifest)
        .map_err(|_| failure("clone.discovery_mismatch"))?;
    let object = manifest
        .package_object()
        .map_err(|_| failure("clone.object_missing"))?;
    if object.length > MAX_OBJECT_BYTES {
        return Err(failure("clone.object_too_large").into());
    }

    let parent = destination_parent(&destination);
    let staging = tempfile::Builder::new()
        .prefix(".graphforge-clone-")
        .tempdir_in(parent)
        .map_err(|_| failure("clone.staging_failed"))?;
    let package = staging.path().join("package.gfpb");
    download_object(
        &agent,
        &object.locations,
        &package,
        object.length,
        &object.digest.0,
    )
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
    .map_err(|_| failure("clone.package_invalid"))?;
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
    if let Ok(ip) = host.parse::<IpAddr>() {
        let unsafe_ip = match ip {
            IpAddr::V4(ip) => {
                ip.is_private()
                    || ip.is_loopback()
                    || ip.is_link_local()
                    || ip.is_unspecified()
                    || ip.is_multicast()
            }
            IpAddr::V6(ip) => {
                ip.is_loopback()
                    || ip.is_unspecified()
                    || ip.is_multicast()
                    || ip.is_unique_local()
                    || ip.is_unicast_link_local()
            }
        };
        if unsafe_ip {
            return Err("clone.source_unsafe");
        }
    }
    Ok(())
}

fn protocol_url(base: &Url, leaf: &str) -> Url {
    let mut result = base.clone();
    result.set_path(&format!("{}/.gf/{leaf}", base.path().trim_end_matches('/')));
    result
}

fn get_bounded(agent: &ureq::Agent, url: Url, max: usize) -> Result<Vec<u8>, &'static str> {
    let mut response = get_with_safe_redirects(agent, url)?;
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
) -> Result<ureq::http::Response<ureq::Body>, &'static str> {
    for redirects in 0..=3 {
        validate_public_host(&url)?;
        let response = agent
            .get(url.as_str())
            .call()
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
        if next.scheme() != "https" || !next.username().is_empty() || next.password().is_some() {
            return Err("clone.redirect_unsafe");
        }
        validate_public_host(&next)?;
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
        match download_one(agent, location, output, length, digest) {
            Ok(()) => return Ok(()),
            Err(error) => {
                last = error;
                let _ = fs::remove_file(output);
            }
        }
    }
    Err(last)
}

fn download_one(
    agent: &ureq::Agent,
    location: &str,
    output: &Path,
    expected_length: u64,
    expected_digest: &str,
) -> Result<(), &'static str> {
    let url = Url::parse(location).map_err(|_| "clone.location_unsafe")?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return Err("clone.location_unsafe");
    }
    validate_public_host(&url)?;
    let mut response = get_with_safe_redirects(agent, url)?;
    if response
        .body()
        .content_length()
        .is_some_and(|value| value != expected_length)
    {
        return Err("clone.length_mismatch");
    }
    let mut reader = response.body_mut().as_reader();
    let mut file = File::create(output).map_err(|_| "clone.staging_failed")?;
    copy_verified(&mut reader, &mut file, expected_length, expected_digest)?;
    file.sync_all().map_err(|_| "clone.staging_failed")?;
    Ok(())
}

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
fn emit_clone_metric(bytes: u64, duration: Duration, result: &'static str) {
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
    fn rejects_unsafe_sources_and_existing_destinations() {
        assert_eq!(
            resolve_source("http://graphforge.sh/a/b").unwrap_err(),
            "clone.source_unsafe"
        );
        assert_eq!(
            resolve_source("https://127.0.0.1/a/b").unwrap_err(),
            "clone.source_unsafe"
        );
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
}
