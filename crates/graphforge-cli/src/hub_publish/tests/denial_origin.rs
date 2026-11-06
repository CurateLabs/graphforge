//! The denied request supplies the origin; response prose and selected Hub hints do not.
use super::*;
use std::collections::VecDeque;

fn refusal() -> HubResponse {
    HubResponse {
        status: 403,
        headers: Vec::new(),
        body: serde_json::to_vec(&json!({
            "code":"auth_denied",
            "message":"Sign in again. https://wrong.example/capability-secret?token=query-secret Bearer response-secret"
        })).unwrap(),
    }
}

fn rendered(error: &CliRuntimeError, json: bool, expected: &str) {
    assert_eq!(crate::error_exit_code(error), 2);
    let mut output = Vec::new();
    crate::write_runtime_error(error, json, &mut output, &[]).unwrap();
    let text = String::from_utf8(output).unwrap();
    assert!(
        text.contains(&format!("Request origin: {expected}")),
        "{text}"
    );
    for forbidden in [
        "secret",
        "wrong.example",
        "user:",
        "password",
        "?token=",
        "/capability",
    ] {
        assert!(!text.contains(forbidden), "{text}");
    }
    if json {
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["error"]["code"], "GF_VALIDATION");
        assert_eq!(
            value["error"]["details"]["semantic_code"],
            "hub.publish.auth_denied"
        );
        assert!(
            value["error"]["message"]
                .as_str()
                .unwrap()
                .contains(expected)
        );
    }
}

struct RefuseStage<'a> {
    inner: &'a dyn Transport,
    stage: &'a str,
    denied: std::cell::Cell<bool>,
}
impl Transport for RefuseStage<'_> {
    fn get(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<HttpResponse, GfError> {
        panic!("publication uses exchange")
    }
    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        let path = Url::parse(&request.url).unwrap().path().to_owned();
        if (self.stage == "capabilities" && path.ends_with("/publish")) || path.contains(self.stage)
        {
            self.denied.set(true);
            return Ok(refusal());
        }
        self.inner.exchange(request, limit)
    }
}

#[test]
fn parsed_publish_denials_name_the_hub_at_capabilities_status_open_and_commit() {
    let History { root, graph, .. } = history();
    drop(graph);
    let hub = LoopbackHub::start();
    let transport = hub.transport();
    for json in [false, true] {
        for stage in [
            "capabilities",
            "publish/operations/",
            "publish/sessions",
            "/commit",
        ] {
            let transport = RefuseStage {
                inner: &transport,
                stage,
                denied: std::cell::Cell::new(false),
            };
            let mut argv = vec![
                "gf",
                "publish",
                "curate/claims",
                "--hub",
                HUB,
                "--ref",
                "main",
            ];
            if stage == "capabilities" {
                argv.push("--public");
            }
            if json {
                argv.push("--json");
            }
            let cli = crate::parse_cli(argv).unwrap();
            let Some(crate::Command::Publish(args)) = cli.command else {
                panic!("publish")
            };
            let mut notices = Vec::new();
            let mut stdout = Vec::new();
            let loads = std::cell::Cell::new(0);
            let error = run_command_with_transport(
                &transport,
                || {
                    loads.set(loads.get() + 1);
                    Ok(Some(token(&hub, "curate/claims")))
                },
                &args,
                Some(root.path().join("project")),
                None,
                json,
                &mut stdout,
                &mut crate::Diagnostics::new(json, &mut notices),
            )
            .unwrap_err();
            assert!(transport.denied.get(), "{stage}: {error}");
            assert_eq!(loads.get(), usize::from(stage != "capabilities"));
            if json {
                assert!(stdout.is_empty());
            } else {
                // Plain mode may print the operation UUID before a refusal.
                assert!(!String::from_utf8(stdout).unwrap().contains("Published "));
            }
            rendered(&error, json, HUB);
        }
    }
    assert!(
        hub.log()
            .iter()
            .all(|entry| !entry.path.ends_with("/commit"))
    );
    assert_eq!(refs_document(&hub, "curate/claims").status, 404);
}

struct Replies {
    replies: RefCell<VecDeque<HubResponse>>,
    requests: RefCell<Vec<HubRequest>>,
}
impl Replies {
    fn new(replies: Vec<HubResponse>) -> Self {
        Self {
            replies: RefCell::new(replies.into()),
            requests: RefCell::new(Vec::new()),
        }
    }
}
impl Transport for Replies {
    fn get(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<HttpResponse, GfError> {
        panic!("expected exchange")
    }
    fn exchange(&self, request: &HubRequest, _: usize) -> Result<HubResponse, GfError> {
        self.requests.borrow_mut().push(request.clone());
        Ok(self
            .replies
            .borrow_mut()
            .pop_front()
            .expect("no extra token or formatting requests"))
    }
}

#[test]
fn redirected_read_names_the_final_origin_and_never_forwards_credentials() {
    let transport = Replies::new(vec![
        HubResponse {
            status: 302,
            headers: vec![(
                "location".into(),
                "https://metadata.example:8443/capability-secret".into(),
            )],
            body: Vec::new(),
        },
        refusal(),
    ]);
    let hub = Hub {
        refusal: None,
        bypass: None,
        transport: &transport,
        base: Url::parse(HUB).unwrap(),
    };
    let error = hub
        .read_control(
            &Url::parse("https://hub.test/owner/repo/.gf/refs").unwrap(),
            4096,
        )
        .unwrap_err();
    let error = error.into();
    for json in [false, true] {
        rendered(&error, json, "https://metadata.example:8443");
    }
    let requests = transport.requests.borrow();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1]
            .url
            .starts_with("https://metadata.example:8443/")
    );
    assert!(requests.iter().all(|request| request.headers.is_empty()));
}

#[test]
fn upload_denial_names_the_capability_origin_without_its_path_or_query() {
    for put in [false, true] {
        let mut replies = Vec::new();
        if put {
            replies.push(HubResponse {
                status: 200,
                headers: vec![("upload-offset".into(), "0".into())],
                body: Vec::new(),
            });
        }
        replies.push(refusal());
        let transport = Replies::new(replies);
        let hub = Hub {
            refusal: None,
            bypass: None,
            transport: &transport,
            base: Url::parse(HUB).unwrap(),
        };
        let target = UploadTarget {
            digest: sha256_digest(b"a"),
            length: 1,
            received: 0,
            chunk_bytes: None,
            upload_url:
                "https://objects.example:443/uploads/a/../capability-secret?token=query-secret"
                    .into(),
        };
        let error = upload(&hub, &target, &Source::Bytes(b"a".to_vec()), 1, false).unwrap_err();
        let error = error.into();
        for json in [false, true] {
            rendered(&error, json, "https://objects.example");
        }
        let requests = transport.requests.borrow();
        assert_eq!(requests.len(), if put { 2 } else { 1 });
        assert!(
            requests
                .iter()
                .all(|request| request.url == target.upload_url),
            "HEAD and PUT must preserve the issued capability URL: {requests:?}"
        );
        assert!(
            requests
                .iter()
                .all(|request| request.header("authorization").is_none()
                    && request.header("x-vercel-protection-bypass").is_none())
        );
    }
}

#[test]
fn publish_device_refusal_names_the_actual_authorization_endpoint_origin() {
    let reference = ReferenceHub::with_config(ReferenceHubConfig {
        base_url: HUB.into(),
        ..ReferenceHubConfig::default()
    })
    .unwrap();
    for poll in [false, true] {
        let mut capabilities =
            reference.capabilities(&RepositoryIdentity::parse("curate/claims").unwrap());
        capabilities.authorization.device_authorization_endpoint =
            "https://device.example:8443/device".into();
        capabilities.authorization.token_endpoint = "https://tokens.example/token".into();
        capabilities.validate().unwrap();
        let mut replies = Vec::new();
        if poll {
            replies.push(HubResponse { status: 200, headers: Vec::new(), body: serde_json::to_vec(&json!({
                "device_code":"device-secret", "user_code":"ABCD", "verification_uri":"https://verify.example/device", "expires_in":60, "interval":1
            })).unwrap() });
        }
        replies.push(HubResponse {
            status: 403,
            headers: Vec::new(),
            body: br#"{"error":"access_denied","error_description":"provider-secret"}"#.to_vec(),
        });
        let transport = Replies::new(replies);
        let hub = Hub {
            refusal: None,
            bypass: None,
            transport: &transport,
            base: Url::parse(HUB).unwrap(),
        };
        let error = obtain_token_with_opener(
            &hub,
            &capabilities,
            &mut PublishEnvironment {
                bypass: None,
                credential: Credential::DeviceFlow,
                clock: &std::time::Instant::now,
                wait: &|_| {},
                notices: &mut Vec::new(),
                resolution: None,
            },
            &|_| {},
        )
        .unwrap_err();
        let expected = if poll {
            "https://tokens.example"
        } else {
            "https://device.example:8443"
        };
        for json in [false, true] {
            rendered(&error, json, expected);
        }
        assert_eq!(transport.requests.borrow().len(), if poll { 2 } else { 1 });
    }
}
