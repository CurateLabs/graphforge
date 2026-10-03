//! Clone discovery, download, telemetry, and import tests.

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
    let report =
        graphforge_storage::verify_portable_v2(&bundle_path, PortableV2Mode::Full, limits, None)
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
    let report =
        graphforge_storage::verify_portable_v2(&bundle_path, PortableV2Mode::Full, limits, None)
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
    let report =
        graphforge_storage::verify_portable_v2(&bundle_path, PortableV2Mode::Full, limits, None)
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
