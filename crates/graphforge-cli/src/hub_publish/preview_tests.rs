//! Purpose and origin isolation through the real HTTP request builder and agent.
use super::*;
use crate::hub_http::{DiscoveryCredentials, HttpResponse, send_with_agent};
use std::io::{BufRead as _, BufReader};
use std::net::TcpListener;

const HUB: &str = "https://hub.test";
const SECRET: &str = "preview-secret";
struct Wire {
    agent: ureq::Agent,
    port: u16,
}
impl Transport for Wire {
    fn get(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<HttpResponse, GfError> {
        panic!("publish exchanges use the bounded request path")
    }
    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        let original = Url::parse(&request.url).unwrap();
        assert_eq!(original.scheme(), "https");
        assert!(matches!(
            original.host_str(),
            Some("hub.test" | "external.test")
        ));
        let mut routed = Url::parse(&format!("http://127.0.0.1:{}", self.port)).unwrap();
        routed.set_path(original.path());
        routed.set_query(original.query());
        let mut request = request.clone();
        request.url = routed.to_string();
        send_with_agent(&self.agent, &request, limit)
    }
}
fn response(status: u16, location: Option<&str>) -> HubResponse {
    HubResponse {
        status,
        headers: location
            .map(|value| vec![("location".into(), value.into())])
            .unwrap_or_default(),
        body: b"{}".to_vec(),
    }
}
fn wire(replies: Vec<HubResponse>) -> (Wire, std::thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let mut requests = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        for reply in replies {
            let (mut socket, _) = loop {
                match listener.accept() {
                    Ok(socket) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "missing expected wire request"
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
            let length = request
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            reader.read_exact(&mut vec![0; length]).unwrap();
            let body = if request.starts_with("HEAD ") {
                &[][..]
            } else {
                &reply.body
            };
            write!(
                socket,
                "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n",
                reply.status,
                body.len()
            )
            .unwrap();
            for (name, value) in reply.headers {
                write!(socket, "{name}: {value}\r\n").unwrap();
            }
            socket.write_all(b"\r\n").unwrap();
            socket.write_all(body).unwrap();
            requests.push(request);
        }
        requests
    });
    let config = ureq::Agent::config_builder()
        .https_only(false)
        .http_status_as_error(false)
        .max_redirects(0)
        .proxy(None)
        .timeout_global(Some(Duration::from_secs(5)))
        .middleware(
            |request: ureq::http::Request<ureq::SendBody>,
             next: ureq::middleware::MiddlewareNext<'_>| {
                for name in ["authorization", "x-vercel-protection-bypass"] {
                    if let Some(value) = request.headers().get(name) {
                        assert!(
                            value.is_sensitive(),
                            "the final HTTP header must be sensitive"
                        );
                    }
                }
                next.handle(request)
            },
        )
        .build();
    (
        Wire {
            agent: config.into(),
            port,
        },
        server,
    )
}
fn bypass() -> DiscoveryCredentials {
    DiscoveryCredentials::load(
        &Url::parse(HUB).unwrap(),
        Some("IDENTITY_TOKEN"),
        Some("BYPASS"),
        |name| {
            Ok(if name == "IDENTITY_TOKEN" {
                "must-not-reach-control-reads"
            } else {
                SECRET
            }
            .into())
        },
    )
    .unwrap()
    .unwrap()
}
fn headers(request: &str, bypass: bool, bearer: bool) {
    let request = request.to_ascii_lowercase();
    assert_eq!(
        request.contains("\r\nx-vercel-protection-bypass:"),
        bypass,
        "unexpected preview credential presence"
    );
    assert_eq!(
        request.contains("\r\nauthorization:"),
        bearer,
        "unexpected bearer presence"
    );
}
#[test]
fn only_selected_control_purposes_receive_preview_credentials_on_the_wire() {
    let (wire, server) = wire((0..16).map(|_| response(200, None)).collect());
    let bypass = bypass();
    let hub = Hub {
        transport: &wire,
        base: Url::parse("https://hub.test/owner/repo").unwrap(),
        bypass: Some(&bypass),
    };
    let token = PublishToken::new("publish-token".into());
    for suffix in ["publish", "refs", "manifest"] {
        hub.read_control(&endpoint(&hub.base, suffix), 1024)
            .unwrap();
    }
    for (method, suffix) in [
        (HubMethod::Get, "publish/operations/id"),
        (HubMethod::Post, "publish/sessions"),
        (HubMethod::Post, "publish/sessions/id/commit"),
    ] {
        hub.control(
            HubRequest::new(method, endpoint(&hub.base, suffix).as_str()),
            &token,
        )
        .unwrap();
    }
    for suffix in ["device", "token"] {
        hub.oauth(
            &format!("{HUB}/{suffix}"),
            &[("client_id", "graphforge-cli")],
        )
        .unwrap();
    }
    for origin in [HUB, "https://external.test"] {
        for method in [HubMethod::Head, HubMethod::Put] {
            hub.data(&HubRequest::new(
                method,
                format!("{origin}/upload?capability=opaque"),
            ))
            .unwrap();
        }
    }
    hub.read(&Url::parse("https://hub.test/object").unwrap(), 1024)
        .unwrap();
    hub.read_control(&Url::parse("https://external.test/metadata").unwrap(), 1024)
        .unwrap();
    for suffix in ["device", "token"] {
        hub.oauth(
            &format!("https://external.test/{suffix}"),
            &[("client_id", "graphforge-cli")],
        )
        .unwrap();
    }
    assert!(
        hub.control(
            HubRequest::new(HubMethod::Post, "https://external.test/control"),
            &token
        )
        .is_err()
    );
    assert!(hub.oauth("http://127.0.0.1/token", &[]).is_err());
    for name in ["authorization", "x-vercel-protection-bypass"] {
        assert!(
            hub.data(
                &HubRequest::new(HubMethod::Put, format!("{HUB}/upload"))
                    .with_header(name, "secret")
            )
            .is_err()
        );
    }
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 16);
    for (index, request) in requests.iter().enumerate() {
        headers(request, index < 8, (3..6).contains(&index));
    }
}
#[test]
fn redirects_strip_preview_credentials_and_writes_never_follow_them() {
    let (wire, server) = wire(vec![
        response(302, Some("/same-origin-target")),
        response(200, None),
        response(302, Some("https://external.test/target")),
        response(200, None),
        response(302, Some("/must-not-follow-control")),
        response(302, Some("/must-not-follow-oauth")),
        response(200, None),
    ]);
    let bypass = bypass();
    let hub = Hub {
        transport: &wire,
        base: Url::parse("https://hub.test/owner/repo").unwrap(),
        bypass: Some(&bypass),
    };
    hub.read_control(
        &Url::parse("https://hub.test/same-origin-redirect").unwrap(),
        1024,
    )
    .unwrap();
    hub.read_control(
        &Url::parse("https://hub.test/off-origin-redirect").unwrap(),
        1024,
    )
    .unwrap();
    assert_eq!(
        hub.control(
            HubRequest::new(HubMethod::Post, "https://hub.test/control"),
            &PublishToken::new("token".into())
        )
        .unwrap()
        .status,
        302
    );
    assert_eq!(
        hub.oauth(
            "https://hub.test/device",
            &[("client_id", "graphforge-cli")]
        )
        .unwrap()
        .status,
        302
    );
    let default = Hub {
        transport: &wire,
        base: hub.base.clone(),
        bypass: None,
    };
    default
        .read_control(&Url::parse("https://hub.test/default").unwrap(), 1024)
        .unwrap();
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 7);
    for (index, request) in requests.iter().enumerate() {
        headers(request, matches!(index, 0 | 2 | 4 | 5), index == 4);
    }
}
#[test]
fn publish_preview_argument_and_validation_preserve_default_captured_behavior() {
    use clap::Parser as _;
    let cli = crate::Cli::try_parse_from([
        "gf",
        "publish",
        "owner/repo",
        "--ref",
        "main",
        "--protection-bypass-env",
        "PREVIEW_SECRET",
    ])
    .unwrap();
    let Some(crate::Command::Publish(args)) = cli.command else {
        panic!("expected publish")
    };
    assert_eq!(
        args.protection_bypass_env.as_deref(),
        Some("PREVIEW_SECRET")
    );
    for secret in [
        "",
        "space secret",
        "line\nsecret",
        "tab\tsecret",
        "é",
        "nul\0secret",
    ] {
        let error =
            DiscoveryCredentials::load(&Url::parse(HUB).unwrap(), None, Some("BYPASS"), |_| {
                Ok(secret.into())
            })
            .err()
            .unwrap();
        if !secret.is_empty() {
            assert!(!error.to_string().contains(secret));
        }
    }
    assert!(
        DiscoveryCredentials::load(&Url::parse(HUB).unwrap(), None, Some("BYPASS"), |_| Err(
            std::env::VarError::NotPresent
        ))
        .is_err()
    );
    assert!(
        DiscoveryCredentials::load(&Url::parse(HUB).unwrap(), None, None, |_| panic!(
            "default reads no environment credentials"
        ))
        .unwrap()
        .is_none()
    );
    let missing = format!("GF_ABSENT_PREVIEW_{}", Uuid::now_v7().simple());
    let execution = crate::execute([
        "gf",
        "--json",
        "publish",
        "owner/repo",
        "--ref",
        "main",
        "--protection-bypass-env",
        &missing,
    ]);
    assert_ne!(execution.exit_code, 0);
    assert!(execution.stdout.is_empty());
    assert!(
        String::from_utf8(execution.stderr)
            .unwrap()
            .contains("hub.auth_denied")
    );
}

#[test]
fn private_context_shape_status_bounds_and_redirects_fail_closed_on_the_wire() {
    let invalid = [
        "{}",
        "null",
        "[]",
        r#"{"refs_json":null,"manifest_json":null}"#,
        r#"{"refs_json":null,"manifest_json":null,"lineage_json":null,"extra":null}"#,
        r#"{"refs_json":null,"refs_json":null,"manifest_json":null,"lineage_json":null}"#,
        r#"{"refs_json":"{}","manifest_json":null,"lineage_json":null}"#,
        r#"{"refs_json":null,"manifest_json":"{}","lineage_json":null}"#,
        r#"{"refs_json":false,"manifest_json":false,"lineage_json":false}"#,
        r#"{"refs_json":"{}","manifest_json":"{}","lineage_json":"{}"}"#,
    ];
    let mut replies: Vec<_> = invalid
        .iter()
        .map(|body| HubResponse {
            status: 200,
            headers: vec![],
            body: body.as_bytes().to_vec(),
        })
        .collect();
    for status in [401, 403, 404, 422, 500, 302, 307] {
        replies.push(response(status, Some("https://external.test/steal")));
    }
    replies.push(HubResponse {
        status: 200,
        headers: vec![],
        body: vec![b' '; MAX_CONTROL_RESPONSE_BYTES + 1],
    });
    let count = replies.len();
    replies.push(HubResponse {
        status: 200,
        headers: vec![],
        body: br#"{"refs_json":null,"manifest_json":null,"lineage_json":null}"#.to_vec(),
    });
    let (transport, server) = wire(replies);
    let bypass = bypass();
    let hub = Hub {
        transport: &transport,
        bypass: Some(&bypass),
        base: Url::parse("https://hub.test/alice/private").unwrap(),
    };
    let token = PublishToken::new("ci-private-token".into());
    let identity = RepositoryIdentity::parse("alice/private").unwrap();
    for _ in 0..count {
        assert!(read_private_context(&hub, &identity, &token).is_err());
    }
    assert!(
        read_private_context(&hub, &identity, &token)
            .unwrap()
            .is_none()
    );
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), count + 1);
    for request in requests {
        assert!(request.starts_with("GET /alice/private/.gf/publish/context "));
        headers(&request, true, true);
    }
}
