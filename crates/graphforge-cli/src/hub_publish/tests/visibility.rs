//! Real native publication with a Hub advertising negotiated visibility.
use super::*;
use clap::Parser as _;
use graphforge_hub_publish::{PUBLISH_VISIBILITY_CAPABILITY, PUBLISH_VISIBILITY_HEADER};

struct VisibilityTransport<'a> {
    hub: &'a LoopbackHub,
    major: Option<u16>,
    refusal: Option<(&'static str, &'static str)>,
    requests: RefCell<Vec<HubRequest>>,
}

impl Transport for VisibilityTransport<'_> {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, GfError> {
        self.hub.transport().get(url, range, if_range, limit)
    }

    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        self.requests.borrow_mut().push(request.clone());
        if request.method == HubMethod::Post
            && request.url.ends_with("/.gf/publish/sessions")
            && let Some((code, reason)) = self.refusal
        {
            return Ok(HubResponse {
                status: if code == "visibility_mismatch" {409} else {403},
                headers: Vec::new(),
                body: serde_json::to_vec(&json!({"code":code,"reason":reason,"message":"SECRET server prose https://upload.example/?token=SECRET"})).unwrap(),
            });
        }
        let mut response = self.hub.transport().exchange(request, limit)?;
        if request.method == HubMethod::Get
            && request.url.ends_with("/.gf/publish")
            && let Some(major) = self.major
        {
            let mut document: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            document["capabilities"]
                .as_array_mut()
                .unwrap()
                .push(json!({"capability":PUBLISH_VISIBILITY_CAPABILITY,"major":major}));
            response.body = serde_json::to_vec(&document).unwrap();
        }
        Ok(response)
    }
}

fn run(
    transport: &VisibilityTransport<'_>,
    graph: &GraphForge,
    args: &PublishArgs,
) -> Result<PublishOutcome, GfError> {
    let mut notices = Vec::new();
    let mut environment = PublishEnvironment {
        bypass: None,
        credential: Credential::Token(token(transport.hub, "curate/claims")),
        wait: &|_| panic!("explicit token needs no device flow"),
        notices: &mut notices,
        resolution: None,
    };
    publish(transport, graph, args, &mut environment)
}

#[test]
fn visibility_flags_are_mutually_exclusive_in_the_real_cli() {
    for flags in [["--public", "--private"], ["--private", "--public"]] {
        assert!(
            crate::Cli::try_parse_from([
                "gf",
                "publish",
                "alice/graph",
                "--ref",
                "main",
                flags[0],
                flags[1]
            ])
            .is_err()
        );
    }
    for flag in ["--public", "--private"] {
        crate::Cli::try_parse_from(["gf", "publish", "alice/graph", "--ref", "main", flag])
            .unwrap();
    }
}

#[test]
fn unsupported_visibility_fails_before_credentials_or_session_open() {
    let hub = LoopbackHub::start();
    for major in [None, Some(2)] {
        let transport = VisibilityTransport {
            hub: &hub,
            major,
            refusal: None,
            requests: RefCell::new(Vec::new()),
        };
        for private in [false, true] {
            let arguments = PublishArgs {
                visibility: VisibilityArgs {
                    public: !private,
                    private,
                },
                ..on_ref("curate/claims", "main")
            };
            let error = publish_with_credentials(&transport, &arguments, None, None, |_| {
                panic!("no credential or publication callback expected")
            })
            .unwrap_err();
            assert_eq!(error_code(&error), "hub.publish.unsupported_future");
        }
        assert!(
            transport
                .requests
                .borrow()
                .iter()
                .all(|r| r.method == HubMethod::Get)
        );
    }
}

#[test]
fn actual_command_checks_visibility_before_loading_a_token_or_opening_a_project() {
    let hub = LoopbackHub::start();
    let transport = VisibilityTransport {
        hub: &hub,
        major: None,
        refusal: None,
        requests: RefCell::new(Vec::new()),
    };
    let arguments = PublishArgs {
        visibility: VisibilityArgs {
            public: true,
            private: false,
        },
        ..on_ref("curate/claims", "main")
    };
    let root = tempfile::tempdir().unwrap();
    let absent = root.path().join("absent-project");
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = run_command_with_transport(
        &transport,
        &arguments,
        Some(absent.clone()),
        None,
        false,
        &mut stdout,
        &mut crate::Diagnostics::new(false, &mut stderr),
    )
    .unwrap_err();
    assert_eq!(
        error_code(&error),
        "hub.publish.unsupported_future",
        "{error}"
    );
    assert!(!absent.exists());
    assert!(
        transport
            .requests
            .borrow()
            .iter()
            .all(|r| r.method == HubMethod::Get)
    );
}

#[test]
fn visibility_is_only_a_session_header_and_separates_publication_replay() {
    let history = history();
    let hub = LoopbackHub::start();
    let transport = VisibilityTransport {
        hub: &hub,
        major: Some(1),
        refusal: None,
        requests: RefCell::new(Vec::new()),
    };
    let arguments = PublishArgs {
        visibility: VisibilityArgs {
            public: true,
            private: false,
        },
        ..on_ref("curate/claims", "main")
    };
    let public = run(&transport, &history.graph, &arguments).unwrap();
    assert!(!public.replayed);
    assert!(
        run(&transport, &history.graph, &arguments)
            .unwrap()
            .replayed
    );
    let implicit = run(&transport, &history.graph, &on_ref("curate/claims", "main")).unwrap();
    assert_ne!(
        public.receipt.operation_uuid,
        implicit.receipt.operation_uuid
    );
    let requests = transport.requests.borrow();
    let mut saw_public = false;
    let mut saw_omitted = false;
    for request in requests.iter() {
        let visibility = request.header(PUBLISH_VISIBILITY_HEADER);
        let session_open =
            request.method == HubMethod::Post && request.url.ends_with("/.gf/publish/sessions");
        if session_open {
            let document: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert!(
                document.get("visibility").is_none(),
                "visibility must not enter the signed session document"
            );
            saw_public |= visibility == Some("public");
            saw_omitted |= visibility.is_none();
        } else {
            assert!(
                visibility.is_none(),
                "visibility leaked outside session open: {request:?}"
            );
        }
    }
    assert!(saw_public && saw_omitted);
}

#[test]
fn private_plan_and_visibility_mismatch_refusals_upload_nothing() {
    let history = history();
    let hub = LoopbackHub::start();
    for (code, reason, guidance) in [
        (
            "entitlement_denied",
            "private_requires_plan",
            "Private repositories need a plan",
        ),
        (
            "visibility_mismatch",
            "repository_is_public",
            "This repository is public",
        ),
        (
            "visibility_mismatch",
            "repository_is_private",
            "This repository is private",
        ),
    ] {
        let transport = VisibilityTransport {
            hub: &hub,
            major: Some(1),
            refusal: Some((code, reason)),
            requests: RefCell::new(Vec::new()),
        };
        let arguments = PublishArgs {
            visibility: VisibilityArgs {
                public: false,
                private: true,
            },
            ..on_ref("curate/claims", "main")
        };
        let error = run(&transport, &history.graph, &arguments).unwrap_err();
        assert_eq!(error_code(&error), format!("hub.publish.{code}"));
        assert!(error.to_string().contains(guidance), "{error}");
        assert!(!error.to_string().contains("SECRET"));
        assert_eq!(
            crate::error_exit_code(&crate::CliRuntimeError::Core(error)),
            2,
            "typed visibility and entitlement refusals retain the documented exit class"
        );
        let requests = transport.requests.borrow();
        assert!(!requests.iter().any(|r| r.method == HubMethod::Put));
        assert!(!requests.iter().any(|r| r.url.ends_with("/commit")));
        let open = requests
            .iter()
            .find(|r| r.method == HubMethod::Post && r.url.ends_with("/sessions"))
            .unwrap();
        assert_eq!(open.header(PUBLISH_VISIBILITY_HEADER), Some("private"));
    }
}

#[test]
fn changing_visibility_cannot_replay_a_completed_explicit_operation() {
    let history = history();
    let hub = LoopbackHub::start();
    let transport = VisibilityTransport {
        hub: &hub,
        major: Some(1),
        refusal: None,
        requests: RefCell::new(Vec::new()),
    };
    let arguments = PublishArgs {
        visibility: VisibilityArgs {
            public: true,
            private: false,
        },
        operation_uuid: Some(Uuid::now_v7().to_string()),
        ..on_ref("curate/claims", "main")
    };
    run(&transport, &history.graph, &arguments).unwrap();
    let changed = PublishArgs {
        visibility: VisibilityArgs {
            public: false,
            private: true,
        },
        ..arguments
    };
    let error = run(&transport, &history.graph, &changed).unwrap_err();
    assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT", "{error}");
}

#[test]
fn malformed_reason_or_wrong_mismatch_status_never_becomes_a_typed_refusal() {
    for (status, reason) in [
        (409, "private_requires_plan"),
        (409, "made_up"),
        (403, "repository_is_public"),
    ] {
        let error = hub_failure(&HubResponse {
            status,
            headers: Vec::new(),
            body: serde_json::to_vec(
                &json!({"code":"visibility_mismatch","reason":reason,"message":"SECRET"}),
            )
            .unwrap(),
        });
        assert_eq!(error_code(&error), "hub.network");
        assert!(!error.to_string().contains("SECRET"));
    }
    for (status, code, reason) in [
        (500, "entitlement_denied", "private_requires_plan"),
        (400, "entitlement_denied", "storage_cap"),
        (403, "invalid_input", "invalid_visibility"),
        (409, "invalid_input", "invalid_visibility"),
    ] {
        let error = hub_failure(&HubResponse {
            status,
            headers: Vec::new(),
            body: serde_json::to_vec(&json!({
                "code":code, "reason":reason, "message":"SECRET",
            }))
            .unwrap(),
        });
        assert_eq!(error_code(&error), "hub.network");
        assert!(!error.to_string().contains("SECRET"));
    }
    // Preserve classification of legacy bodies without a negotiated reason.
    let legacy = hub_failure(&HubResponse {
        status: 500,
        headers: Vec::new(),
        body: br#"{"code":"entitlement_denied","message":"legacy"}"#.to_vec(),
    });
    assert_eq!(error_code(&legacy), "hub.publish.entitlement_denied");
}
