//! Fetch and verify one exact ontology module from a Hub repository.
//!
//! This applies the clone pipeline's discovery, transport, staging, and download
//! safety to one per-module package object instead of the Project package. It
//! reuses the parent module's private pieces, so the two commands cannot drift
//! apart. Sub-issue of #1732.

use super::{
    CloneStaging, DownloadControl, DownloadReport, HttpTransport, MAX_METADATA_BYTES, RETRY_POLICY,
    RetryPolicy, Transport, acquire_staging, canonical_name, download_with_progress, endpoint,
    ensure_destination_absent, fetch_with_attempts, open_partial_nofollow, parse_input_at,
    portable_error, protocol_error, read_bounded, release_staging, storage, validation,
};
use clap::Args;
use graphforge_api::{
    DiscoveryOntologyModuleRequest, OntologyModuleId, PortableV2Limits,
    resolve_discovered_ontology_module,
};
use graphforge_discovery::{
    DiscoveryLimits, DiscoveryManifest, ExactIdentity, ObjectDescriptor, RefSet,
    RepositoryIdentity, Sha256Digest,
};
use serde::Serialize;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use url::Url;

/// Bound on one per-module package object; a module never needs the bundle bound.
const MAX_MODULE_PACKAGE_BYTES: u64 = 64 * 1024 * 1024;

/// Fetch and verify one exact ontology module from a Hub repository without
/// downloading its Project package.
#[derive(Args)]
pub(crate) struct ModuleFetchArgs {
    /// Canonical owner/repository name.
    pub repository: String,
    /// Exact module ontology identifier, as advertised by the repository.
    #[arg(long)]
    pub ontology_id: String,
    /// Exact module authored version, as advertised by the repository.
    #[arg(long)]
    pub version: String,
    /// Exact canonical content digest: 64 lowercase hexadecimal digits,
    /// optionally prefixed with `sha256:`.
    #[arg(long)]
    pub digest: String,
    /// New file that receives the verified module document; never overwritten.
    #[arg(long)]
    pub output: PathBuf,
    /// HTTPS Hub base URL; defaults to https://graphforge.sh.
    #[arg(long)]
    pub hub: Option<String>,
}

#[derive(Serialize)]
struct ModuleFetchResult {
    contract: &'static str,
    repository: String,
    module: OntologyModuleId,
    package_digest: String,
    module_sha256: String,
    output: String,
}

pub(crate) fn run_module_fetch(
    args: &ModuleFetchArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    run_module_fetch_with(&HttpTransport::new(), args, json, output, &RETRY_POLICY)
}

fn run_module_fetch_with(
    transport: &dyn Transport,
    args: &ModuleFetchArgs,
    json: bool,
    output: &mut dyn Write,
    retry: &RetryPolicy,
) -> Result<(), graphforge_api::GfError> {
    let result = run_module_fetch_job(transport, args, retry)?;
    write_module_fetch_result(&result, json, output)
}

fn write_module_fetch_result(
    result: &ModuleFetchResult,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if json {
        serde_json::to_writer(&mut *output, result)
            .map_err(|e| graphforge_api::GfError::Execution(e.to_string()))?;
        writeln!(output).map_err(storage)?;
    } else {
        writeln!(
            output,
            "Fetched ontology module {} {} from {} to {}",
            result.module.ontology_id,
            result.module.authored_version,
            result.repository,
            result.output
        )
        .map_err(storage)?;
    }
    Ok(())
}

/// Build the exact module identity the caller asks for. The digest is the
/// canonical content digest, which is not the package digest nor the document's
/// file SHA-256.
fn module_identity(args: &ModuleFetchArgs) -> Result<ExactIdentity, graphforge_api::GfError> {
    let hex = args.digest.strip_prefix("sha256:").unwrap_or(&args.digest);
    let content_digest = Sha256Digest(format!("sha256:{hex}"));
    content_digest.validate().map_err(|_| {
        validation(
            "hub.invalid_identity",
            "--digest must be 64 lowercase hexadecimal digits, optionally prefixed with sha256:",
        )
    })?;
    Ok(ExactIdentity {
        id: args.ontology_id.clone(),
        version: args.version.clone(),
        content_digest,
    })
}

/// Refuse an existing output, or one whose directory is absent, before any I/O.
fn ensure_output_available(output: &Path) -> Result<(), graphforge_api::GfError> {
    let directory = match output.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    if !std::fs::metadata(directory).is_ok_and(|metadata| metadata.is_dir()) {
        return Err(validation(
            "hub.destination_conflict",
            "output directory does not exist",
        ));
    }
    ensure_destination_absent(output)
}

fn fetch_document(
    transport: &dyn Transport,
    url: &Url,
    retry: &RetryPolicy,
) -> Result<Vec<u8>, graphforge_api::GfError> {
    let mut attempts = 0;
    read_bounded(
        fetch_with_attempts(
            transport,
            url,
            None,
            None,
            MAX_METADATA_BYTES as u64,
            &mut attempts,
            retry,
        )?,
        MAX_METADATA_BYTES,
    )
}

fn run_module_fetch_job(
    transport: &dyn Transport,
    args: &ModuleFetchArgs,
    retry: &RetryPolicy,
) -> Result<ModuleFetchResult, graphforge_api::GfError> {
    let (identity, base) = parse_input_at(&args.repository, args.hub.as_deref())?;
    let module = module_identity(args)?;
    ensure_output_available(&args.output)?;
    let refs_bytes = fetch_document(transport, &endpoint(&base, "refs"), retry)?;
    let manifest_bytes = fetch_document(transport, &endpoint(&base, "manifest"), retry)?;
    let limits = DiscoveryLimits {
        max_response_bytes: MAX_METADATA_BYTES,
        max_module_package_bytes: MAX_MODULE_PACKAGE_BYTES,
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
    let (_, _, object) = manifest
        .ontology_module_selection(&module)
        .map_err(|error| protocol_error(&error))?;
    let staging = acquire_staging(&args.output)?;
    let result = fetch_and_publish_module(
        transport,
        args,
        &identity,
        &module,
        &manifest_bytes,
        &refs_bytes,
        object,
        limits,
        &staging,
        retry,
    );
    release_staging(staging);
    result
}

// `release_staging` removes everything this fetch staged, on success and on
// failure alike: the document is already published, and the package is cheap
// to download again.

#[allow(clippy::too_many_arguments)]
fn fetch_and_publish_module(
    transport: &dyn Transport,
    args: &ModuleFetchArgs,
    identity: &RepositoryIdentity,
    module: &ExactIdentity,
    manifest_bytes: &[u8],
    refs_bytes: &[u8],
    object: &ObjectDescriptor,
    limits: DiscoveryLimits,
    staging: &CloneStaging,
    retry: &RetryPolicy,
) -> Result<ModuleFetchResult, graphforge_api::GfError> {
    download_with_progress(
        transport,
        object,
        &staging.partial,
        &mut DownloadReport::default(),
        &mut DownloadControl::quiet(retry),
    )?;
    let resolved = resolve_discovered_ontology_module(&DiscoveryOntologyModuleRequest {
        manifest_json: manifest_bytes,
        refs_json: refs_bytes,
        expected_repository: identity,
        module,
        package: &staging.partial,
        discovery_limits: limits,
        portable_limits: PortableV2Limits::default(),
        cancelled: None,
    })
    .map_err(portable_error)?;
    let staged = staging.root.join("module.out");
    let mut file = open_partial_nofollow(&staged, false).map_err(storage)?;
    file.write_all(&resolved.document).map_err(storage)?;
    file.sync_all().map_err(storage)?;
    drop(file);
    publish_without_replacing(&staged, &args.output)?;
    Ok(ModuleFetchResult {
        contract: "graphforge-hub-module-fetch/1",
        repository: canonical_name(identity),
        module: resolved.module,
        package_digest: resolved.package_digest,
        module_sha256: resolved.module_sha256,
        output: args.output.display().to_string(),
    })
}

/// Publish `staged` at `output` atomically. A hard link fails if `output`
/// exists, even as a dangling symlink, so a file created after the up-front
/// check is never overwritten (a rename would replace it silently).
fn publish_without_replacing(staged: &Path, output: &Path) -> Result<(), graphforge_api::GfError> {
    match std::fs::hard_link(staged, output) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(validation(
                "hub.destination_conflict",
                "destination already exists",
            ));
        }
        Err(error) => return Err(hard_link_error(error)),
    }
    if let Some(parent) = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let _ = File::open(parent).and_then(|directory| directory.sync_all());
    }
    Ok(())
}

/// Classify a failed hard link. A filesystem that cannot hard-link (for example
/// FAT or some network and FUSE mounts) cannot publish the output without
/// risking replacement of a concurrently created file, so it is refused with a
/// stable code. Any other failure (permissions, space, I/O) stays a storage error.
fn hard_link_error(error: std::io::Error) -> graphforge_api::GfError {
    #[cfg(unix)]
    let unsupported = error.raw_os_error().is_some_and(|code| {
        code == libc::EPERM || code == libc::ENOTSUP || code == libc::EOPNOTSUPP
    });
    #[cfg(not(unix))]
    let unsupported = false;
    if unsupported || error.kind() == std::io::ErrorKind::Unsupported {
        validation(
            "hub.destination_conflict",
            "output filesystem cannot publish atomically",
        )
    } else {
        storage(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub_clone::tests::{RecordingTransport, Scripted, response};
    use crate::hub_clone::{HttpResponse, hash_reader};
    use crate::hub_clone::{TEST_RETRY_POLICY, clear_staging, remove_staging_dir};
    use std::sync::Mutex;

    const FIXTURE_REFS: &[u8] =
        include_bytes!("../../../../tests/fixtures/hub/generated/v1/refs.json");
    const FIXTURE_MANIFEST: &[u8] =
        include_bytes!("../../../../tests/fixtures/hub/generated/v1/manifest.json");

    fn fixture_module_package() -> Vec<u8> {
        let manifest =
            DiscoveryManifest::from_json(FIXTURE_MANIFEST, DiscoveryLimits::default()).unwrap();
        let module = &manifest.ontology.as_ref().unwrap().modules[0];
        let hex = module.content_digest.0.strip_prefix("sha256:").unwrap();
        std::fs::read(format!(
            "{}/../../tests/fixtures/hub/generated/v1/objects/ontology-module-{hex}.gfpb",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
    }

    /// Fetch arguments for the fixture's only advertised module.
    fn fixture_module_args(output: &Path) -> ModuleFetchArgs {
        let manifest =
            DiscoveryManifest::from_json(FIXTURE_MANIFEST, DiscoveryLimits::default()).unwrap();
        let module = &manifest.ontology.as_ref().unwrap().modules[0];
        ModuleFetchArgs {
            repository: "openalex/openalex".into(),
            ontology_id: module.id.clone(),
            version: module.version.clone(),
            digest: module.content_digest.0.clone(),
            output: output.to_path_buf(),
            hub: None,
        }
    }

    /// The fixture manifest after `change`, as bytes.
    fn changed_fixture_manifest(change: impl FnOnce(&mut serde_json::Value)) -> Vec<u8> {
        let mut manifest: serde_json::Value = serde_json::from_slice(FIXTURE_MANIFEST).unwrap();
        change(&mut manifest);
        serde_json::to_vec(&manifest).unwrap()
    }

    /// The transport object entry of the fixture's only module package.
    fn module_object_entry(manifest: &mut serde_json::Value) -> &mut serde_json::Value {
        let digest = manifest["ontology"]["modules"][0]["package"]["object_digest"].clone();
        manifest["objects"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|object| object["digest"] == digest)
            .unwrap()
    }

    fn module_transport(manifest: &[u8], object: &[u8]) -> RecordingTransport {
        RecordingTransport {
            inner: Scripted::new(vec![
                response(200, None, FIXTURE_REFS),
                response(200, None, manifest),
                response(200, None, object),
            ]),
            requested: Mutex::new(Vec::new()),
        }
    }

    fn fetch_module(
        transport: &dyn Transport,
        args: &ModuleFetchArgs,
    ) -> Result<serde_json::Value, graphforge_api::GfError> {
        let mut output = Vec::new();
        run_module_fetch_with(transport, args, true, &mut output, &TEST_RETRY_POLICY)?;
        Ok(serde_json::from_slice(&output).unwrap())
    }

    fn directory_entries(directory: &Path) -> Vec<String> {
        let mut names: Vec<_> = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// Run a fetch that must fail with `code`; return the requested URLs.
    fn assert_module_fetch_fails(
        code: &str,
        transport: &RecordingTransport,
        args: &ModuleFetchArgs,
        directory: &Path,
        expected_entries: &[&str],
    ) -> Vec<String> {
        let error = fetch_module(transport, args).unwrap_err().to_string();
        assert!(error.contains(code), "expected {code}, got {error}");
        assert_eq!(
            directory_entries(directory),
            expected_entries,
            "a failed fetch leaves no output or staging file"
        );
        transport.requested.lock().unwrap().clone()
    }

    #[test]
    fn module_fetch_requests_only_the_module_package_and_writes_the_verified_document() {
        let manifest =
            DiscoveryManifest::from_json(FIXTURE_MANIFEST, DiscoveryLimits::default()).unwrap();
        let package = fixture_module_package();
        let transport = module_transport(FIXTURE_MANIFEST, &package);
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("works.json");
        let args = fixture_module_args(&output);
        let receipt = fetch_module(&transport, &args).unwrap();

        assert_eq!(transport.inner.remaining(), 0);
        let identity = ExactIdentity {
            id: args.ontology_id.clone(),
            version: args.version.clone(),
            content_digest: Sha256Digest(args.digest.clone()),
        };
        let (descriptor, advertised, module_object) =
            manifest.ontology_module_selection(&identity).unwrap();
        assert_eq!(
            transport.requested.lock().unwrap().clone(),
            [
                "https://graphforge.sh/openalex/openalex/.gf/refs".to_owned(),
                "https://graphforge.sh/openalex/openalex/.gf/manifest".to_owned(),
                module_object.locations[0].clone(),
            ],
            "refs, manifest, and the module object; never the Project package or summary"
        );
        assert_ne!(module_object.digest, manifest.package.object_digest);

        // The output is exactly the document the verified package carries.
        let package_path = root.path().join("expected.gfpb");
        std::fs::write(&package_path, &package).unwrap();
        let expected = resolve_discovered_ontology_module(&DiscoveryOntologyModuleRequest {
            manifest_json: FIXTURE_MANIFEST,
            refs_json: FIXTURE_REFS,
            expected_repository: &RepositoryIdentity::parse("openalex/openalex").unwrap(),
            module: &identity,
            package: &package_path,
            discovery_limits: DiscoveryLimits::default(),
            portable_limits: PortableV2Limits::default(),
            cancelled: None,
        })
        .unwrap();
        let written = std::fs::read(&output).unwrap();
        assert_eq!(written, expected.document);
        assert!(!written.is_empty());

        let document_sha256 = hash_reader(&mut std::io::Cursor::new(&written)).unwrap();
        assert_eq!(
            receipt,
            serde_json::json!({
                "contract": "graphforge-hub-module-fetch/1",
                "repository": "openalex/openalex",
                "module": {
                    "ontology_id": descriptor.id,
                    "authored_version": descriptor.version,
                    "canonical_digest": descriptor.content_digest.0.strip_prefix("sha256:").unwrap(),
                },
                "package_digest": advertised.package_digest.0,
                "module_sha256": document_sha256.strip_prefix("sha256:").unwrap(),
                "output": output.display().to_string(),
            })
        );
        assert_eq!(
            directory_entries(root.path()),
            ["expected.gfpb", "works.json"],
            "staging is removed after a successful fetch"
        );
    }

    #[test]
    fn module_fetch_accepts_a_bare_hex_digest_and_a_custom_hub() {
        let package = fixture_module_package();
        let transport = module_transport(FIXTURE_MANIFEST, &package);
        let root = tempfile::tempdir().unwrap();
        let mut args = fixture_module_args(&root.path().join("works.json"));
        // The main fetch test passes the `sha256:`-prefixed form.
        args.digest = args.digest.strip_prefix("sha256:").unwrap().to_owned();
        args.hub = Some("https://hub.example/prefix/".into());
        fetch_module(&transport, &args).unwrap();
        let requested = transport.requested.lock().unwrap().clone();
        assert_eq!(
            requested[..2],
            [
                "https://hub.example/prefix/openalex/openalex/.gf/refs",
                "https://hub.example/prefix/openalex/openalex/.gf/manifest",
            ]
        );
    }

    #[test]
    fn module_fetch_rejects_malformed_inputs_before_any_request() {
        let root = tempfile::tempdir().unwrap();
        let good = fixture_module_args(&root.path().join("works.json"));
        let digest = good.digest.clone();
        let hex = digest.strip_prefix("sha256:").unwrap().to_owned();
        for (change, code) in [
            (
                Box::new(|args: &mut ModuleFetchArgs| args.digest = hex.to_uppercase())
                    as Box<dyn Fn(&mut ModuleFetchArgs)>,
                "hub.invalid_identity",
            ),
            (
                Box::new(|args| args.digest = hex[..63].to_owned()),
                "hub.invalid_identity",
            ),
            (
                Box::new(|args| args.repository = "not a repository".into()),
                "hub.invalid_identity",
            ),
            (
                Box::new(|args| args.hub = Some("http://hub.example".into())),
                "hub.unsafe_location",
            ),
            (
                Box::new(|args| args.hub = Some("https://127.0.0.1".into())),
                "hub.unsafe_location",
            ),
            (
                Box::new(|args| args.hub = Some("https://user@hub.example".into())),
                "hub.unsafe_location",
            ),
            (
                Box::new(|args| {
                    args.repository = "https://hub.example/openalex/openalex".into();
                    args.hub = Some("https://other.example".into());
                }),
                "hub.invalid_identity",
            ),
            (
                Box::new(|args| args.output = root.path().join("absent-directory/works.json")),
                "hub.destination_conflict",
            ),
        ] {
            let mut args = fixture_module_args(&good.output);
            change(&mut args);
            let transport = module_transport(FIXTURE_MANIFEST, b"unused");
            let requested = assert_module_fetch_fails(code, &transport, &args, root.path(), &[]);
            assert!(requested.is_empty(), "{code}: {requested:?}");
        }
    }

    #[test]
    fn module_fetch_rejects_unsafe_object_locations_before_requesting_the_object() {
        for location in [
            "http://data.example/objects/module",
            "https://user:secret@data.example/objects/module",
            "https://10.0.0.1/objects/module",
            "https://127.0.0.1/objects/module",
        ] {
            let manifest = changed_fixture_manifest(|manifest| {
                module_object_entry(manifest)["locations"] = serde_json::json!([location]);
            });
            let root = tempfile::tempdir().unwrap();
            let args = fixture_module_args(&root.path().join("works.json"));
            let transport = module_transport(&manifest, &fixture_module_package());
            let requested = assert_module_fetch_fails(
                "hub.unsafe_location",
                &transport,
                &args,
                root.path(),
                &[],
            );
            assert_eq!(requested.len(), 2, "{location}: only refs and manifest");
            assert_eq!(transport.inner.remaining(), 1, "{location}");
        }
    }

    #[test]
    fn module_fetch_of_an_unadvertised_identity_is_a_missing_object() {
        let package = fixture_module_package();
        let root = tempfile::tempdir().unwrap();
        for change in [
            Box::new(|args: &mut ModuleFetchArgs| args.version = "9999.01".into())
                as Box<dyn Fn(&mut ModuleFetchArgs)>,
            Box::new(|args| args.ontology_id = "https://openalex.org/ontology/other".into()),
            Box::new(|args| args.digest = "0".repeat(64)),
        ] {
            let mut args = fixture_module_args(&root.path().join("works.json"));
            change(&mut args);
            let transport = module_transport(FIXTURE_MANIFEST, &package);
            let requested = assert_module_fetch_fails(
                "hub.missing_object",
                &transport,
                &args,
                root.path(),
                &[],
            );
            assert_eq!(requested.len(), 2, "no object is requested: {requested:?}");
        }
        // An advertised module without a per-module package has no object to fetch.
        let manifest = changed_fixture_manifest(|manifest| {
            manifest["ontology"]["modules"][0]
                .as_object_mut()
                .unwrap()
                .remove("package");
        });
        let args = fixture_module_args(&root.path().join("works.json"));
        let transport = module_transport(&manifest, &package);
        let requested =
            assert_module_fetch_fails("hub.missing_object", &transport, &args, root.path(), &[]);
        assert_eq!(requested.len(), 2);
    }

    #[test]
    fn module_fetch_enforces_the_module_byte_bound() {
        let package = fixture_module_package();
        // A descriptor larger than the module bound fails before the object is requested.
        let manifest = changed_fixture_manifest(|manifest| {
            module_object_entry(manifest)["length"] =
                serde_json::json!(MAX_MODULE_PACKAGE_BYTES + 1);
        });
        let root = tempfile::tempdir().unwrap();
        let args = fixture_module_args(&root.path().join("works.json"));
        let transport = module_transport(&manifest, &package);
        let requested =
            assert_module_fetch_fails("hub.limit_exceeded", &transport, &args, root.path(), &[]);
        assert_eq!(requested.len(), 2);

        // A body longer than the declared length is cut off and rejected.
        let mut longer = package.clone();
        longer.push(0);
        let transport = module_transport(FIXTURE_MANIFEST, &longer);
        let requested =
            assert_module_fetch_fails("hub.limit_exceeded", &transport, &args, root.path(), &[]);
        assert_eq!(requested.len(), 3);

        // A body shorter than the declared length is incomplete, not accepted.
        let transport = module_transport(FIXTURE_MANIFEST, &package[..package.len() - 1]);
        assert_module_fetch_fails("hub.interrupted", &transport, &args, root.path(), &[]);
    }

    #[test]
    fn module_fetch_rejects_corrupt_bytes_and_publishes_nothing() {
        let package = fixture_module_package();
        let root = tempfile::tempdir().unwrap();
        let args = fixture_module_args(&root.path().join("works.json"));

        // Same length, different bytes: the transport digest disagrees.
        let mut corrupt = package.clone();
        let middle = corrupt.len() / 2;
        corrupt[middle] ^= 0xff;
        let transport = module_transport(FIXTURE_MANIFEST, &corrupt);
        let requested =
            assert_module_fetch_fails("hub.integrity", &transport, &args, root.path(), &[]);
        assert_eq!(requested.len(), 3);

        // Bytes that match their transport digest but are not a valid package
        // are rejected by the portable verifier.
        let garbage = vec![7_u8; package.len()];
        let garbage_digest = hash_reader(&mut std::io::Cursor::new(&garbage)).unwrap();
        let manifest = changed_fixture_manifest(|manifest| {
            let advertised = manifest["ontology"]["modules"][0]["package"]["object_digest"].clone();
            module_object_entry(manifest)["digest"] = serde_json::json!(garbage_digest);
            manifest["ontology"]["modules"][0]["package"]["object_digest"] =
                serde_json::json!(garbage_digest);
            assert_ne!(advertised, serde_json::json!(garbage_digest));
            manifest["objects"]
                .as_array_mut()
                .unwrap()
                .sort_by(|a, b| a["digest"].as_str().cmp(&b["digest"].as_str()));
        });
        let transport = module_transport(&manifest, &garbage);
        assert_module_fetch_fails("hub.package.", &transport, &args, root.path(), &[]);

        // A genuine package that is not the advertised one fails the identity binding.
        let manifest = changed_fixture_manifest(|manifest| {
            manifest["ontology"]["modules"][0]["package"]["package_digest"] =
                serde_json::json!(format!("sha256:{}", "e".repeat(64)));
        });
        let transport = module_transport(&manifest, &package);
        assert_module_fetch_fails(
            "hub.package.package_digest_mismatch",
            &transport,
            &args,
            root.path(),
            &[],
        );
    }

    #[test]
    fn module_fetch_refuses_an_existing_output_without_touching_it() {
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("works.json");
        std::fs::write(&output, b"precious").unwrap();
        let args = fixture_module_args(&output);
        let transport = module_transport(FIXTURE_MANIFEST, &fixture_module_package());
        let requested = assert_module_fetch_fails(
            "hub.destination_conflict",
            &transport,
            &args,
            root.path(),
            &["works.json"],
        );
        assert!(requested.is_empty(), "refused before any request");
        assert_eq!(std::fs::read(&output).unwrap(), b"precious");
    }

    #[cfg(unix)]
    #[test]
    fn module_fetch_refuses_a_dangling_symlink_output() {
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("works.json");
        std::os::unix::fs::symlink(root.path().join("elsewhere"), &output).unwrap();
        let args = fixture_module_args(&output);
        let transport = module_transport(FIXTURE_MANIFEST, &fixture_module_package());
        assert_module_fetch_fails(
            "hub.destination_conflict",
            &transport,
            &args,
            root.path(),
            &["works.json"],
        );
        assert!(!root.path().join("elsewhere").exists());
    }

    /// Creates `output` the moment the module object is requested.
    struct CreatesOutputDuringFetch {
        inner: RecordingTransport,
        output: PathBuf,
    }

    impl Transport for CreatesOutputDuringFetch {
        fn get(
            &self,
            url: &Url,
            range: Option<u64>,
            if_range: Option<&str>,
            limit: u64,
        ) -> Result<HttpResponse, graphforge_api::GfError> {
            if self.inner.requested.lock().unwrap().len() == 2 {
                std::fs::write(&self.output, b"created concurrently").unwrap();
            }
            self.inner.get(url, range, if_range, limit)
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_filesystem_without_hard_links_is_a_stable_conflict_not_a_storage_error() {
        for code in [libc::EPERM, libc::ENOTSUP, libc::EOPNOTSUPP] {
            let error = hard_link_error(std::io::Error::from_raw_os_error(code)).to_string();
            assert!(
                error.contains("hub.destination_conflict")
                    && error.contains("output filesystem cannot publish atomically"),
                "{code}: {error}"
            );
        }
        // Permission to write the directory is a different failure.
        let error = hard_link_error(std::io::Error::from_raw_os_error(libc::EACCES)).to_string();
        assert!(!error.contains("hub.destination_conflict"), "{error}");
    }

    #[test]
    fn staging_release_empties_it_under_the_lock_and_never_clobbers_a_new_lock() {
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("works.json");
        let staging = acquire_staging(&output).unwrap();
        let staging_root = staging.root.clone();
        for name in ["package.part", "package.resume.json", "module.out"] {
            std::fs::write(staging_root.join(name), b"staged").unwrap();
        }
        clear_staging(&staging);
        assert_eq!(
            directory_entries(&staging_root),
            Vec::<String>::new(),
            "contents and lock file go while the lock is held"
        );
        // A concurrent fetch re-creates its lock while ours is being released;
        // our final removal fails harmlessly and leaves its staging alone.
        let concurrent = acquire_staging(&output).unwrap();
        std::fs::write(&concurrent.partial, b"theirs").unwrap();
        drop(staging);
        remove_staging_dir(&staging_root);
        assert_eq!(
            std::fs::read(&concurrent.partial).unwrap(),
            b"theirs",
            "the concurrent fetch's files survive"
        );
        assert!(
            acquire_staging(&output)
                .is_err_and(|error| error.to_string().contains("hub.concurrent_clone")),
            "its lock still excludes a third fetch"
        );
        drop(concurrent);

        // The real release leaves nothing behind.
        let staging = acquire_staging(&output).unwrap();
        std::fs::write(&staging.partial, b"staged").unwrap();
        release_staging(staging);
        assert!(!staging_root.exists());
        assert_eq!(directory_entries(root.path()), Vec::<String>::new());
    }

    #[test]
    fn module_fetch_never_replaces_an_output_created_during_the_fetch() {
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("works.json");
        let transport = CreatesOutputDuringFetch {
            inner: module_transport(FIXTURE_MANIFEST, &fixture_module_package()),
            output: output.clone(),
        };
        let error = fetch_module(&transport, &fixture_module_args(&output))
            .unwrap_err()
            .to_string();
        assert!(error.contains("hub.destination_conflict"), "{error}");
        assert_eq!(std::fs::read(&output).unwrap(), b"created concurrently");
        assert_eq!(directory_entries(root.path()), ["works.json"]);
    }
}
