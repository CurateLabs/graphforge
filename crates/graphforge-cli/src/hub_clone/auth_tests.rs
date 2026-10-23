//! Native request-header evidence for authenticated discovery and clone.

use super::*;
use crate::hub_http::{ReadTimeouts, read_agent};
use std::io::{BufRead as _, BufReader};
use std::net::TcpListener;
use ureq::http::HeaderValue;
use ureq::unversioned::resolver::DefaultResolver;

const TOKEN: &str = "test-private-hub-token";
const BYPASS: &str = "test-preview-bypass";

fn credential_lookup(name: &str) -> Result<String, std::env::VarError> {
    match name {
        "HUB_TOKEN" => Ok(TOKEN.into()),
        "PREVIEW_SECRET" => Ok(BYPASS.into()),
        _ => Err(std::env::VarError::NotPresent),
    }
}

fn credentials() -> DiscoveryCredentials {
    DiscoveryCredentials::load(
        &Url::parse("https://hub.example").unwrap(),
        Some("HUB_TOKEN"),
        Some("PREVIEW_SECRET"),
        credential_lookup,
    )
    .unwrap()
    .unwrap()
}

/// Route public virtual origins to a local HTTP server only after the fetcher
/// performs the real URL validation. DNS/socket routing is the only test seam.
pub(super) struct NativeTransport {
    http: HttpTransport,
    port: u16,
}

impl NativeTransport {
    fn route(&self, url: &Url) -> Url {
        let mut routed = Url::parse(&format!("http://127.0.0.1:{}", self.port)).unwrap();
        routed.set_path(url.path());
        routed.set_query(url.query());
        routed
    }
}

impl Transport for NativeTransport {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        self.http.get(&self.route(url), range, if_range, limit)
    }

    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, HeaderValue)],
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        self.http.get_discovery(&self.route(url), headers, limit)
    }
}

pub(super) struct Reply {
    status: u16,
    location: Option<String>,
    body: Vec<u8>,
}

impl Reply {
    pub(super) fn ok(body: &[u8]) -> Self {
        Self {
            status: 200,
            location: None,
            body: body.into(),
        }
    }
}

pub(super) fn server(
    replies: Vec<Reply>,
) -> (NativeTransport, std::thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        let mut requests = Vec::new();
        let expected = replies.len();
        let deadline = Instant::now() + Duration::from_secs(15);
        for reply in replies {
            let (mut socket, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "fixture deadline: {} of {expected} expected requests received",
                            requests.len()
                        );
                        std::thread::park_timeout(Duration::from_millis(5));
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut request = String::new();
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                request.push_str(&line);
            }
            requests.push(request);
            write!(
                socket,
                "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nETag: \"auth-fixture\"\r\nConnection: close\r\n",
                reply.status,
                reply.body.len()
            )
            .unwrap();
            if let Some(location) = reply.location {
                write!(socket, "Location: {location}\r\n").unwrap();
            }
            socket.write_all(b"\r\n").unwrap();
            socket.write_all(&reply.body).unwrap();
        }
        requests
    });
    let timeout = Duration::from_secs(5);
    (
        NativeTransport {
            http: HttpTransport {
                agent: read_agent(
                    false,
                    &ReadTimeouts {
                        connect: timeout,
                        response: timeout,
                        idle: timeout,
                    },
                    DefaultResolver::default(),
                ),
            },
            port,
        },
        server,
    )
}

fn assert_headers(request: &str, token: bool, bypass: bool) {
    let headers = request.to_ascii_lowercase();
    assert_eq!(headers.contains("\r\nauthorization:"), token, "{request}");
    assert_eq!(
        headers.contains("\r\nx-vercel-protection-bypass:"),
        bypass,
        "{request}"
    );
    if token {
        assert!(headers.contains(&format!("authorization: bearer {TOKEN}\r\n")));
    }
    if bypass {
        assert!(headers.contains(&format!("x-vercel-protection-bypass: {BYPASS}\r\n")));
    }
}

#[test]
fn hub_and_preview_credentials_are_independent_native_headers() {
    for (token, bypass) in [(true, false), (false, true), (true, true), (false, false)] {
        let credentials = DiscoveryCredentials::load(
            &Url::parse("https://hub.example").unwrap(),
            token.then_some("HUB_TOKEN"),
            bypass.then_some("PREVIEW_SECRET"),
            credential_lookup,
        )
        .unwrap();
        let (transport, server) = server(vec![Reply::ok(b"refs"), Reply::ok(b"manifest")]);
        for path in ["refs", "manifest"] {
            let url = Url::parse(&format!("https://hub.example/a/b/.gf/{path}")).unwrap();
            let response = fetch_discovery_with_attempts(
                &transport,
                &url,
                credentials.as_ref(),
                1024,
                &mut 0,
                &TEST_RETRY_POLICY,
            )
            .unwrap();
            assert_eq!(read_bounded(response, 1024).unwrap(), path.as_bytes());
        }
        for request in server.join().unwrap() {
            assert_headers(&request, token, bypass);
        }
    }
}

#[test]
fn redirects_never_receive_credentials_even_on_the_same_origin() {
    for target in [
        "/redirected",
        "https://hub.example/redirected",
        "https://objects.example/redirected",
    ] {
        let (transport, server) = server(vec![
            Reply {
                status: 302,
                location: Some(target.into()),
                body: vec![],
            },
            Reply::ok(b"public"),
        ]);
        let response = fetch_discovery_with_attempts(
            &transport,
            &Url::parse("https://hub.example/a/b/.gf/refs").unwrap(),
            Some(&credentials()),
            1024,
            &mut 0,
            &TEST_RETRY_POLICY,
        )
        .unwrap();
        assert_eq!(read_bounded(response, 1024).unwrap(), b"public");
        let requests = server.join().unwrap();
        assert_headers(&requests[0], true, true);
        assert_headers(&requests[1], false, false);
    }
}

#[test]
fn authenticated_discovery_preserves_unsafe_redirect_rejection() {
    for target in [
        "https://127.0.0.1/private",
        "http://hub.example/plain",
        "https://user@hub.example/private",
        "https://hub.example/sso?token=secret",
        "https://hub.example/path#secret",
    ] {
        let (transport, server) = server(vec![Reply {
            status: 307,
            location: Some(target.into()),
            body: vec![],
        }]);
        let result = fetch_discovery_with_attempts(
            &transport,
            &Url::parse("https://hub.example/a/b/.gf/refs").unwrap(),
            Some(&credentials()),
            1024,
            &mut 0,
            &TEST_RETRY_POLICY,
        );
        let error = result.err().expect("unsafe redirect rejected");
        assert!(error.to_string().contains("hub.unsafe_location"), "{error}");
        assert_eq!(server.join().unwrap().len(), 1);
    }
}

#[test]
fn discovery_credentials_require_exact_scheme_host_and_effective_port() {
    let credentials = credentials();
    for start in [
        "https://other.example/a/b/.gf/refs",
        "https://hub.example:444/a/b/.gf/refs",
        "http://hub.example/a/b/.gf/refs",
    ] {
        let transport = tests::Scripted::new(vec![]);
        let mut attempts = 0;
        let result = fetch_discovery_with_attempts(
            &transport,
            &Url::parse(start).unwrap(),
            Some(&credentials),
            1024,
            &mut attempts,
            &TEST_RETRY_POLICY,
        );
        let error = result
            .err()
            .expect("foreign origin rejected before transport");
        let expected = if start.starts_with("http:") {
            "hub.unsafe_location"
        } else {
            "clone credentials require the configured Hub origin"
        };
        assert!(error.to_string().contains(expected), "{error}");
        assert!(attempts <= 1);
    }
    let (transport, server) = server(vec![Reply::ok(b"ok")]);
    fetch_discovery_with_attempts(
        &transport,
        &Url::parse("https://hub.example:443/a/b/.gf/refs").unwrap(),
        Some(&credentials),
        1024,
        &mut 0,
        &TEST_RETRY_POLICY,
    )
    .unwrap();
    assert_headers(&server.join().unwrap()[0], true, true);
}

#[test]
fn transient_discovery_retry_reauthenticates_only_the_initial_request() {
    let (transport, server) = server(vec![
        Reply {
            status: 503,
            location: None,
            body: vec![],
        },
        Reply::ok(b"ok"),
    ]);
    let mut attempts = 0;
    fetch_discovery_with_attempts(
        &transport,
        &Url::parse("https://hub.example/a/b/.gf/refs").unwrap(),
        Some(&credentials()),
        1024,
        &mut attempts,
        &TEST_RETRY_POLICY,
    )
    .unwrap();
    assert_eq!(attempts, 2);
    for request in server.join().unwrap() {
        assert_headers(&request, true, true);
    }
}

fn clone_args(destination: &Path) -> CloneArgs {
    CloneArgs {
        repository: "opendata/openalex".into(),
        hub: Some("https://hub.example".into()),
        token_env: Some("HUB_TOKEN".into()),
        protection_bypass_env: Some("PREVIEW_SECRET".into()),
        destination: Some(destination.into()),
        telemetry_endpoint: None,
        git_ref: None,
        version_uuid: None,
    }
}

fn clone(
    transport: &dyn Transport,
    args: CloneArgs,
    output: &mut Vec<u8>,
) -> Result<(), graphforge_api::GfError> {
    let mut env = CloneEnv {
        credential_lookup,
        ..CloneEnv::quiet()
    };
    run_clone_profiled_with_delays(
        transport,
        args,
        true,
        output,
        &TelemetryRuntime::default(),
        &CloneDelays::default(),
        &mut env,
    )
}

#[test]
fn invalid_or_expired_credentials_stop_clone_without_retry_or_object_download() {
    for status in [401, 403] {
        let (transport, server) = server(vec![Reply {
            status,
            location: None,
            body: format!("{TOKEN} {BYPASS}").into_bytes(),
        }]);
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("clone");
        let mut output = Vec::new();
        let error = clone(&transport, clone_args(&destination), &mut output).unwrap_err();
        assert!(
            error.to_string().contains(&format!("HTTP status {status}")),
            "{error}"
        );
        assert!(!error.to_string().contains(TOKEN));
        assert!(!error.to_string().contains(BYPASS));
        assert!(output.is_empty());
        assert!(!destination.exists());
        assert!(!staging_path(&destination).unwrap().exists());
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert_headers(&requests[0], true, true);
    }
}

#[test]
fn clone_authenticates_discovery_but_never_same_or_cross_origin_objects() {
    let refs = include_bytes!("../../../../tests/fixtures/hub/generated/v1/refs.json");
    let manifest = include_bytes!("../../../../tests/fixtures/hub/generated/v1/manifest.json");
    let project = include_bytes!(
        "../../../../tests/fixtures/hub/generated/v1/objects/opendata-openalex.gfpb"
    );
    for origin in ["hub.example", "objects.example"] {
        let mut manifest =
            DiscoveryManifest::from_json(manifest, DiscoveryLimits::default()).unwrap();
        let package_digest = manifest.package.object_digest.clone();
        manifest
            .objects
            .iter_mut()
            .find(|object| object.digest == package_digest)
            .unwrap()
            .locations = vec![format!("https://{origin}/project.gfpb")];
        let (transport, server) = server(vec![
            Reply::ok(refs),
            Reply::ok(&serde_json::to_vec(&manifest).unwrap()),
            Reply::ok(project),
        ]);
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("clone");
        let mut output = Vec::new();
        clone(&transport, clone_args(&destination), &mut output).unwrap();
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("GET /opendata/openalex/.gf/refs "));
        assert!(requests[1].starts_with("GET /opendata/openalex/.gf/manifest "));
        assert!(requests[2].starts_with("GET /project.gfpb "));
        assert_headers(&requests[0], true, true);
        assert_headers(&requests[1], true, true);
        assert_headers(&requests[2], false, false);
        let output = String::from_utf8(output).unwrap();
        assert!(!output.contains(TOKEN));
        assert!(!output.contains(BYPASS));
        let graph = GraphForge::new(destination.to_str()).expect("authenticated clone reopens");
        assert_eq!(
            graph.research_project_metadata().unwrap().title.as_deref(),
            Some("OpenAlex")
        );
    }
}

#[test]
fn missing_and_malformed_credentials_fail_before_network_access() {
    for secret in [
        "",
        " ",
        "bad token",
        "secret\r\nx-injected: yes",
        "\tsecret",
        "non-ascii-\u{e9}",
    ] {
        for (token, bypass) in [(Some("TOKEN"), None), (None, Some("BYPASS"))] {
            let result = DiscoveryCredentials::load(
                &Url::parse("https://hub.example").unwrap(),
                token,
                bypass,
                |_| Ok(secret.into()),
            );
            let error = result.err().expect("invalid header rejected");
            assert!(error.to_string().contains("hub.auth_denied"));
            if !secret.trim().is_empty() {
                assert!(!error.to_string().contains(secret));
            }
        }
    }
    let root = tempfile::tempdir().unwrap();
    let mut args = clone_args(&root.path().join("clone"));
    args.token_env = Some("MISSING".into());
    let transport = tests::Scripted::new(vec![tests::response(200, None, b"never")]);
    assert!(
        clone(&transport, args, &mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("hub.auth_denied")
    );
    assert_eq!(transport.remaining(), 1);
}

#[test]
fn unauthenticated_clone_does_not_consult_any_credential_source() {
    let result = DiscoveryCredentials::load(
        &Url::parse("https://hub.example").unwrap(),
        None,
        None,
        |_| panic!("no environment variables should be read"),
    )
    .unwrap();
    assert!(result.is_none());
}

#[test]
fn clone_cli_parses_explicit_hub_and_credential_source_names() {
    use clap::Parser as _;
    let cli = crate::Cli::try_parse_from([
        "gf",
        "clone",
        "opendata/openalex",
        "--hub",
        "https://hub.example",
        "--token-env",
        "HUB_TOKEN",
        "--protection-bypass-env",
        "PREVIEW_SECRET",
    ])
    .unwrap();
    let Some(crate::Command::Clone(args)) = cli.command else {
        panic!("expected clone command")
    };
    assert_eq!(args.hub.as_deref(), Some("https://hub.example"));
    assert_eq!(args.token_env.as_deref(), Some("HUB_TOKEN"));
    assert_eq!(
        args.protection_bypass_env.as_deref(),
        Some("PREVIEW_SECRET")
    );
}

#[test]
fn credential_setup_errors_emit_redacted_invalid_input_telemetry() {
    for source in ["MISSING", "MALFORMED"] {
        let runtime = TelemetryRuntime::new(TelemetryConfig {
            mode: TelemetryMode::InMemory,
            ..TelemetryConfig::default()
        })
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let mut args = clone_args(&root.path().join("clone"));
        args.token_env = Some(source.into());
        let mut env = CloneEnv {
            credential_lookup: |name| {
                if name == "MALFORMED" {
                    Ok(format!("{TOKEN} invalid"))
                } else {
                    credential_lookup(name)
                }
            },
            ..CloneEnv::quiet()
        };
        let transport = tests::Scripted::new(vec![tests::response(200, None, b"never")]);
        let mut output = Vec::new();
        let error = run_clone_profiled_with_delays(
            &transport,
            args,
            true,
            &mut output,
            &runtime,
            &CloneDelays::default(),
            &mut env,
        )
        .unwrap_err();
        assert!(error.to_string().contains("hub.auth_denied"));
        assert_eq!(transport.remaining(), 1);
        assert!(output.is_empty());
        assert_eq!(
            runtime.force_flush(),
            graphforge_api::telemetry::LifecycleStatus::Complete
        );
        let snapshots = runtime.snapshots();
        assert_eq!(snapshots.len(), 1);
        let job = snapshots[0].job.as_ref().unwrap();
        assert_eq!(job.outcome, Outcome::Failed);
        assert_eq!(job.failure, Some(Failure::InvalidInput));
        let serialized = serde_json::to_string(&snapshots).unwrap();
        assert!(!serialized.contains(TOKEN));
        assert!(!serialized.contains(BYPASS));
        assert!(!serialized.contains(source));
    }
}
