//! Real native POST framing, finite response reads, and credential isolation.
use super::*;
use crate::hub_http::{self, HttpTransport, ReadTimeouts, WriteTimeouts};
use std::io::{BufRead as _, BufReader};
use std::net::TcpListener;
use std::time::{Duration, Instant};
use ureq::unversioned::resolver::DefaultResolver;

struct NativeControl {
    inner: HubTransport,
    read: HttpTransport,
    write: ureq::Agent,
    port: u16,
}
impl Clone for NativeControl {
    fn clone(&self) -> Self {
        Self {
            inner: HubTransport::with_agents(
                HttpTransport {
                    agent: self.read.agent.clone(),
                },
                self.write.clone(),
            ),
            read: HttpTransport {
                agent: self.read.agent.clone(),
            },
            write: self.write.clone(),
            port: self.port,
        }
    }
}
impl Transport for NativeControl {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<hub_http::HttpResponse, GfError> {
        self.read.get(&self.route(url), range, if_range, limit)
    }
    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
        limit: u64,
    ) -> Result<hub_http::HttpResponse, GfError> {
        self.read.get_discovery(&self.route(url), headers, limit)
    }
    fn get_authorized(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
        headers: &[(&str, ureq::http::HeaderValue)],
    ) -> Result<hub_http::HttpResponse, GfError> {
        self.read
            .get_authorized(&self.route(url), range, if_range, limit, headers)
    }
    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        // The caller has validated the real virtual HTTPS URL. Only this
        // injected test transport substitutes DNS/socket routing.
        let source = Url::parse(&request.url).unwrap();
        assert_eq!(source.origin(), origin(DEFAULT_HUB).unwrap().origin());
        let mut local = Url::parse(&format!("http://127.0.0.1:{}", self.port)).unwrap();
        local.set_path(source.path());
        let mut routed = request.clone();
        routed.url = local.to_string();
        self.inner.exchange(&routed, limit)
    }
}
impl NativeControl {
    fn route(&self, url: &Url) -> Url {
        assert!(["graphforge.sh", "data.graphforge.sh"].contains(&url.host_str().unwrap()));
        let mut routed = Url::parse(&format!("http://127.0.0.1:{}", self.port)).unwrap();
        routed.set_path(url.path());
        routed.set_query(url.query());
        routed
    }
}
struct Reply {
    status: u16,
    body: Vec<u8>,
    location: Option<&'static str>,
}
fn reply(status: u16, result: serde_json::Value) -> Reply {
    Reply {
        status,
        body: serde_json::to_vec(&envelope(result)).unwrap(),
        location: None,
    }
}
type WireRequests = Vec<(String, Vec<u8>)>;
fn server(replies: Vec<Reply>) -> (NativeControl, std::thread::JoinHandle<WireRequests>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let task = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut requests = Vec::new();
        for reply in replies {
            let (mut socket, _) = loop {
                match listener.accept() {
                    Ok(socket) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "fixture deadline");
                        std::thread::park_timeout(Duration::from_millis(5));
                    }
                    Err(error) => panic!("fixture accept: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut head = String::new();
            let mut length = 0;
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
                head.push_str(&line);
                assert!(head.len() < 8192);
            }
            assert!(length <= MAX_HUB_REQUEST_BYTES);
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            requests.push((head, body));
            write!(
                socket,
                "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nETag: \"native-fixture\"\r\nConnection: close\r\n",
                reply.status,
                reply.body.len()
            )
            .unwrap();
            if let Some(location) = reply.location {
                write!(socket, "Location: {location}\r\n").unwrap();
            }
            socket.write_all(b"\r\n").unwrap();
            // An over-limit client may close before the fixture finishes sending.
            let _ = socket.write_all(&reply.body);
        }
        requests
    });
    let timeout = Duration::from_secs(3);
    let read = HttpTransport {
        agent: hub_http::read_agent(
            false,
            &ReadTimeouts {
                connect: timeout,
                response: timeout,
                idle: timeout,
            },
            DefaultResolver::default(),
        ),
    };
    let write = ureq::Agent::new_with_config(hub_http::write_agent_config(
        false,
        &WriteTimeouts {
            send_body: timeout,
            response: timeout,
        },
    ));
    (
        NativeControl {
            read: HttpTransport {
                agent: read.agent.clone(),
            },
            write: write.clone(),
            inner: HubTransport::with_agents(read, write),
            port,
        },
        task,
    )
}
#[test]
fn native_control_public_saved_session_and_revoked_denial_use_real_http() {
    let public = control(json!({"kind":"capabilities"}));
    let (wire, task) = server(vec![
        reply(
            200,
            json!({"kind":"capabilities","capabilities":["public_search"]}),
        ),
        reply(
            200,
            json!({"kind":"accounts","accounts":[],"page":{"next_cursor":null,"coverage":"complete","total":{"kind":"exact","value":0}}}),
        ),
        reply(
            403,
            json!({"kind":"failure","code":"access_denied","retry_after_ms":null}),
        ),
    ]);
    let hub = origin(DEFAULT_HUB).unwrap();
    exchange_control_from(&FailedStore, &wire, &hub, &public, false, true, 100).unwrap();
    exchange_control_from(&saved(), &wire, &hub, &accounts(), false, true, 100).unwrap();
    assert!(matches!(
        exchange_control_from(&saved(), &wire, &hub, &accounts(), false, true, 100)
            .unwrap()
            .result,
        HubControlResult::Failure {
            code: HubControlErrorCode::AccessDenied,
            ..
        }
    ));
    let requests = task.join().unwrap();
    assert_eq!(requests.len(), 3);
    for (index, (head, body)) in requests.iter().enumerate() {
        assert!(head.starts_with("POST /api/hub/v1/control HTTP/1.1\r\n"));
        assert_eq!(
            head.to_ascii_lowercase()
                .contains("authorization: bearer human-session-secret\r\n"),
            index != 0
        );
        assert!(!head.contains("refresh-secret"));
        assert_eq!(
            HubControlRequest::from_json(body).unwrap(),
            if index == 0 {
                public.clone()
            } else {
                accounts()
            }
        );
    }
}
#[test]
fn native_control_bounds_redirects_and_malformed_apply_require_same_intent_status() {
    let apply = control(json!({"kind":"apply","plan":prepared()}));
    let status = control(
        json!({"kind":"status","operation_uuid":OPERATION,"intent_digest":prepared().intent_digest}),
    );
    for bad in [
        Reply {
            status: 307,
            body: vec![],
            location: Some("https://other.example/private-secret"),
        },
        Reply {
            status: 200,
            body: b"{\"private-secret\":true}".to_vec(),
            location: None,
        },
        Reply {
            status: 200,
            body: vec![b' '; MAX_HUB_RESPONSE_BYTES + 1],
            location: None,
        },
    ] {
        let (wire, task) = server(vec![
            bad,
            reply(
                200,
                json!({"kind":"receipt","receipt":{"operation_uuid":OPERATION,"intent_digest":prepared().intent_digest,"state":"pending","refusal":null,"record":null}}),
            ),
        ]);
        let hub = origin(DEFAULT_HUB).unwrap();
        let error = exchange_control_from(&saved(), &wire, &hub, &apply, false, true, 100)
            .unwrap_err()
            .to_string();
        assert!(error.contains("hub.control_outcome_uncertain"));
        assert!(!error.contains("private-secret"));
        assert!(matches!(
            exchange_control_from(&saved(), &wire, &hub, &status, false, true, 100)
                .unwrap()
                .result,
            HubControlResult::Receipt {
                receipt: HubManagementReceipt {
                    state: HubManagementState::Pending,
                    ..
                }
            }
        ));
        let requests = task.join().unwrap();
        assert_eq!(requests.len(), 2, "no automatic apply retry or redirect");
        assert_eq!(
            HubControlRequest::from_json(&requests[1].1).unwrap(),
            status
        );
    }
}
#[test]
fn native_control_expired_session_is_revoked_before_private_request() {
    let (wire, task) = server(vec![Reply {
        status: 400,
        body: br#"{"error":"invalid_grant"}"#.to_vec(),
        location: None,
    }]);
    let error = exchange_control_from(
        &saved(),
        &wire,
        &origin(DEFAULT_HUB).unwrap(),
        &accounts(),
        false,
        true,
        500,
    )
    .unwrap_err();
    assert!(error.to_string().contains("hub.auth_denied"));
    let requests = task.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].0.starts_with("POST /api/cli/token "));
}

fn module_fixture() -> (
    graphforge_discovery::ItemDescriptor,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
) {
    use graphforge_discovery::{
        DiscoveryLimits, DiscoveryManifest, ItemDescriptor, ItemDiscoverySnapshot, ItemId, RefSet,
    };
    let manifest =
        include_bytes!("../../../../../tests/fixtures/hub/generated/v1/manifest.json").to_vec();
    let refs = include_bytes!("../../../../../tests/fixtures/hub/generated/v1/refs.json").to_vec();
    let parsed = DiscoveryManifest::from_json(&manifest, DiscoveryLimits::default()).unwrap();
    let module = parsed.ontology.as_ref().unwrap().modules[0].identity();
    let id = ItemId::parse_reference(&module.id).unwrap();
    let descriptor = ItemDescriptor {
        format: "graphforge-item/1".into(),
        id: id.as_str().into(),
        kind: id.kind(),
        repository: parsed.repository.clone(),
        module,
        version_order: None,
        discovery: ItemDiscoverySnapshot {
            refs_revision: Sha256Digest(
                hub_http::hash_reader(&mut std::io::Cursor::new(
                    RefSet::from_json(&refs, DiscoveryLimits::default())
                        .unwrap()
                        .to_canonical_json()
                        .unwrap(),
                ))
                .unwrap(),
            ),
            manifest_validator: parsed.canonical_digest().unwrap(),
        },
    };
    let fixture: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../../../tests/fixtures/hub/generated/v1/fixture.json"
    ))
    .unwrap();
    let package = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/hub/generated/v1")
            .join(
                fixture["module_objects"][0]["object_path"]
                    .as_str()
                    .unwrap(),
            ),
    )
    .unwrap();
    (descriptor, refs, manifest, package)
}
fn module_transport(
    wire: NativeControl,
    descriptor: &graphforge_discovery::ItemDescriptor,
) -> CloneTransport<NativeControl> {
    let url = Url::parse(&format!(
        "{DEFAULT_HUB}/{}/{}",
        descriptor.id, descriptor.module.version
    ))
    .unwrap();
    let template = CloneTransport::for_item(url.clone(), None).unwrap();
    let grant_selection = template.identity.clone();
    let grant_wire = wire.clone();
    CloneTransport {
        inner: wire,
        hub: template.hub,
        metadata: template.metadata,
        descriptor: template.descriptor,
        identity: template.identity,
        discovery_acquire: Box::new(|| {
            session_lookup(
                &saved(),
                &Mock::new(vec![]),
                &origin(DEFAULT_HUB).unwrap(),
                100,
            )
            .map(|s| s.map(|s| s.access_token))
        }),
        discovery_token: RefCell::new(None),
        acquire: Box::new(move || {
            assert_eq!(
                grant_selection.borrow().as_ref(),
                Some(
                    &graphforge_discovery::RepositoryIdentity::parse("opendata/openalex").unwrap()
                )
            );
            let selected = grant_selection.borrow();
            let selected = selected.as_ref().unwrap();
            let hub = origin(DEFAULT_HUB).unwrap();
            let session = session_lookup(&saved(), &grant_wire, &hub, 100)?.unwrap();
            exchange_repository_token(
                &grant_wire,
                &hub,
                &session.access_token,
                &selected.owner,
                &selected.repository,
                "read",
            )
            .map(Some)
        }),
        token: RefCell::new(None),
    }
}
#[test]
fn native_private_item_descriptor_snapshots_and_verified_package_use_distinct_credentials() {
    let (descriptor, refs, manifest, package) = module_fixture();
    let plain = |status, body| Reply {
        status,
        body,
        location: None,
    };
    let (wire, task) = server(vec![
        plain(404, vec![]),
        plain(200, serde_json::to_vec(&descriptor).unwrap()),
        plain(200, refs),
        plain(200, manifest),
        plain(403, vec![]),
        plain(200, serde_json::to_vec(&json!({"access_token":"scoped-read-secret","origin":"https://data.graphforge.sh","token_type":"Bearer","expires_in":300,"repository_id":"repository-key"})).unwrap()),
        plain(200, package),
    ]);
    let transport = module_transport(wire, &descriptor);
    let directory = tempfile::tempdir().unwrap();
    let args = crate::hub_clone::ModuleFetchArgs {
        repository: format!("{}/{}", descriptor.id, descriptor.module.version),
        ontology_id: None,
        version: None,
        digest: Some(descriptor.module.content_digest.0.clone()),
        output: directory.path().join("module.json"),
        hub: Some(DEFAULT_HUB.into()),
        protection_bypass_env: None,
    };
    let mut output = Vec::new();
    crate::hub_clone::module_fetch::run_module_fetch_with(
        &transport,
        &args,
        true,
        &mut output,
        &hub_http::RetryPolicy {
            attempts: 1,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        },
    )
    .unwrap();
    assert!(
        args.output.is_file(),
        "native verifier published the exact module"
    );
    let requests = task.join().unwrap();
    assert_eq!(requests.len(), 7);
    for (index, (head, _)) in requests.iter().enumerate() {
        let head = head.to_ascii_lowercase();
        assert_eq!(
            head.contains("authorization: bearer human-session-secret"),
            (1..=3).contains(&index) || index == 5
        );
        assert_eq!(
            head.contains("authorization: bearer scoped-read-secret"),
            index == 6
        );
        assert_eq!(
            head.contains("accept: application/vnd.graphforge.item+json"),
            index < 2
        );
    }
    assert!(requests[2].0.contains(&format!(
        "/.gf/refs?revision={}",
        descriptor.discovery.refs_revision.0.replace(':', "%3A")
    )));
    assert!(requests[3].0.contains("/.gf/manifest?validator="));
    assert!(requests[5].0.starts_with("POST /api/cli/repository-token "));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&requests[5].1).unwrap(),
        json!({"owner":"opendata","name":"openalex","capability":"read"})
    );
    // A different snapshot is not a discovery endpoint for the saved bearer.
    let mut other = Url::parse(&format!("{DEFAULT_HUB}/opendata/openalex/.gf/refs")).unwrap();
    other
        .query_pairs_mut()
        .append_pair("revision", &format!("sha256:{}", "f".repeat(64)));
    assert!(transport.get_discovery(&other, &[], 100).is_err());
}
#[test]
fn native_private_descriptor_never_follows_authenticated_redirect_or_uses_it_as_repository_move() {
    let (descriptor, _, _, _) = module_fixture();
    for status in [302, 308] {
        let (wire, task) = server(vec![
            Reply {
                status: 404,
                body: vec![],
                location: None,
            },
            Reply {
                status,
                body: vec![],
                location: Some("https://other.example/secret"),
            },
        ]);
        let transport = module_transport(wire, &descriptor);
        let selected = transport.descriptor.as_ref().unwrap();
        assert!(
            transport
                .get_discovery(
                    selected,
                    &[(
                        "accept",
                        ureq::http::HeaderValue::from_static(
                            graphforge_discovery::ITEM_DESCRIPTOR_MEDIA_TYPE
                        )
                    )],
                    1024
                )
                .is_err()
        );
        assert_eq!(task.join().unwrap().len(), 2);
        assert!(transport.identity.borrow().is_none());
        assert!(transport.token.borrow().is_none());
    }
}

#[test]
fn native_control_preserves_pagination_and_refuses_unknown_or_private_public_results() {
    let public = control(
        json!({"kind":"search","query":{"scope":{"kind":"public"},"text":"literal","object_kind":null,"tags":[],"visibility":null},"page":{"limit":2,"cursor":null}}),
    );
    let (wire, task) = server(vec![reply(
        200,
        json!({"kind":"search","records":[],"page":{"next_cursor":"opaque-next","coverage":"partial","total":{"kind":"unknown"}}}),
    )]);
    let response = exchange_control_from(
        &FailedStore,
        &wire,
        &origin(DEFAULT_HUB).unwrap(),
        &public,
        true,
        true,
        100,
    )
    .unwrap();
    assert!(matches!(
        response.result,
        HubControlResult::Search {
            page: HubPageInfo {
                coverage: HubCoverage::Partial,
                total: HubCount::Unknown {},
                next_cursor: Some(_),
            },
            ..
        }
    ));
    task.join().unwrap();
    let corpus: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../../../docs/contracts/hub-control/v1/conformance.json"
    ))
    .unwrap();
    for name in [
        "public-rejects-private",
        "duplicate-record",
        "unsupported-capability",
    ] {
        let case = corpus["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == name)
            .unwrap();
        let request =
            HubControlRequest::from_json(case["request"].as_str().unwrap().as_bytes()).unwrap();
        let (wire, task) = server(vec![Reply {
            status: 200,
            body: case["response"].as_str().unwrap().as_bytes().to_vec(),
            location: None,
        }]);
        let error = exchange_control_from(
            &FailedStore,
            &wire,
            &origin(DEFAULT_HUB).unwrap(),
            &request,
            true,
            true,
            100,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("hub.control_invalid"));
        assert!(!error.contains("publisher"));
        assert_eq!(task.join().unwrap().len(), 1);
    }
}
