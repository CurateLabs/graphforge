//! Real native publication with a Hub advertising negotiated visibility.
use super::*;
use clap::Parser as _;
use graphforge_hub_publish::{PUBLISH_VISIBILITY_CAPABILITY, PUBLISH_VISIBILITY_HEADER};

pub(super) struct VisibilityTransport<'a> {
    pub(super) hub: &'a LoopbackHub,
    pub(super) major: Option<u16>,
    pub(super) refusal: Option<(&'static str, &'static str)>,
    pub(super) requests: RefCell<Vec<HubRequest>>,
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
                body: serde_json::to_vec(&json!({"code":code,"reason":reason,"message":"Server guidance https://upload.example/?token=SECRET"})).unwrap(),
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

pub(super) fn run(
    transport: &VisibilityTransport<'_>,
    graph: &GraphForge,
    args: &PublishArgs,
) -> Result<PublishOutcome, CliRuntimeError> {
    let mut notices = Vec::new();
    let mut environment = PublishEnvironment {
        terminal: None,
        bypass: None,
        credential: Credential::Token(token(transport.hub, "curate/claims")),
        clock: &std::time::Instant::now,
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
        || panic!("unsupported visibility must fail before credential loading"),
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
    for (code, reason) in [
        ("entitlement_denied", "private_requires_plan"),
        ("visibility_mismatch", "repository_is_public"),
        ("visibility_mismatch", "repository_is_private"),
    ] {
        let transport = VisibilityTransport {
            hub: &hub,
            major: Some(1),
            refusal: Some((code, reason)),
            requests: RefCell::new(Vec::new()),
        };
        let arguments = PublishArgs {
            visibility: VisibilityArgs {
                public: reason == "repository_is_private",
                private: reason != "repository_is_private",
            },
            ..on_ref("curate/claims", "main")
        };
        let error = run(&transport, &history.graph, &arguments).unwrap_err();
        assert_eq!(error_code(&error), format!("hub.publish.{code}"));
        assert!(
            error.to_string().contains(&format!("Hub reason: {reason}")),
            "{error}"
        );
        assert!(
            error.to_string().contains("Hub message: Server guidance"),
            "{error}"
        );
        assert!(!error.to_string().contains("SECRET"));
        assert_eq!(
            crate::error_exit_code(&error),
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
        assert_eq!(
            open.header(PUBLISH_VISIBILITY_HEADER),
            Some(if reason == "repository_is_private" {
                "public"
            } else {
                "private"
            })
        );
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
        let error = hub_failure(
            &HubResponse {
                status,
                headers: Vec::new(),
                body: serde_json::to_vec(
                    &json!({"code":"visibility_mismatch","reason":reason,"message":"SECRET"}),
                )
                .unwrap(),
            },
            &Url::parse(HUB).unwrap(),
        );
        assert_eq!(core_error_code(&error), "hub.network");
        assert!(!error.to_string().contains("SECRET"));
    }
    for (status, code, reason) in [
        (500, "entitlement_denied", "private_requires_plan"),
        (400, "entitlement_denied", "storage_cap"),
        (403, "invalid_input", "invalid_visibility"),
        (409, "invalid_input", "invalid_visibility"),
    ] {
        let error = hub_failure(
            &HubResponse {
                status,
                headers: Vec::new(),
                body: serde_json::to_vec(&json!({
                    "code":code, "reason":reason, "message":"SECRET",
                }))
                .unwrap(),
            },
            &Url::parse(HUB).unwrap(),
        );
        assert_eq!(core_error_code(&error), "hub.network");
        assert!(!error.to_string().contains("SECRET"));
    }
    // Preserve classification of legacy bodies without a negotiated reason.
    let legacy = hub_failure(
        &HubResponse {
            status: 500,
            headers: Vec::new(),
            body: br#"{"code":"entitlement_denied","message":"legacy"}"#.to_vec(),
        },
        &Url::parse(HUB).unwrap(),
    );
    assert_eq!(core_error_code(&legacy), "hub.publish.entitlement_denied");
}

fn push_arguments(repository: &str, flag: Option<&str>) -> PublishArgs {
    let mut argv = vec!["gf", "push", repository, "--ref", "main", "--hub", HUB];
    if let Some(flag) = flag {
        argv.push(flag);
    }
    let crate::Command::Push(args) = crate::Cli::try_parse_from(argv).unwrap().command.unwrap()
    else {
        unreachable!()
    };
    crate::porcelain::remote::push_args(
        &history().graph.research_version_retention().unwrap(),
        &args,
    )
    .unwrap()
}

fn interactive_push(
    transport: &VisibilityTransport<'_>,
    graph: &GraphForge,
    args: &mut PublishArgs,
    answer: &mut dyn std::io::BufRead,
    notices: &mut Vec<u8>,
    operations: &mut Vec<Uuid>,
) -> Result<PublishOutcome, CliRuntimeError> {
    let (identity, base) = args.destination()?;
    let resolution = resolve_publication(transport, &identity, &base, None)?;
    let mut environment = PublishEnvironment {
        terminal: Some(answer),
        bypass: None,
        credential: Credential::Token(token(transport.hub, "curate/claims")),
        wait: &|_| panic!("token must be reused"),
        clock: &Instant::now,
        notices,
        resolution: Some(resolution),
    };
    push_visibility::choose_push_visibility(transport, args, &mut environment)?;
    publish_with_operation_report(transport, graph, args, &mut environment, |id| {
        operations.push(id);
        Ok(())
    })
}

#[test]
fn push_projects_both_visibility_flags_for_short_and_https_destinations() {
    for repository in ["curate/claims", "https://hub.test/curate/claims"] {
        for flag in ["--public", "--private"] {
            let args = push_arguments(repository, Some(flag));
            assert_eq!(args.visibility.public, flag == "--public");
            assert_eq!(args.visibility.private, flag == "--private");
        }
        assert!(
            crate::Cli::try_parse_from(["gf", "push", repository, "--public", "--private"])
                .is_err()
        );
    }
}

#[test]
fn push_without_terminal_or_flag_rejects_before_the_credential_callback_for_any_target() {
    let history = history();
    for existing in [false, true] {
        let hub = LoopbackHub::start();
        if existing {
            run(
                &VisibilityTransport {
                    hub: &hub,
                    major: Some(1),
                    refusal: None,
                    requests: RefCell::new(Vec::new()),
                },
                &history.graph,
                &on_ref("curate/claims", "main"),
            )
            .unwrap();
        }
        for major in [None, Some(1)] {
            let transport = VisibilityTransport {
                hub: &hub,
                major,
                refusal: None,
                requests: RefCell::new(Vec::new()),
            };
            for repository in ["curate/claims", "https://hub.test/curate/claims"] {
                let command = crate::Cli::try_parse_from(["gf", "push", repository])
                    .unwrap()
                    .command
                    .unwrap();
                let mut graph = GraphForge::new(None).unwrap();
                let mut output = Vec::new();
                let error = crate::porcelain::remote::run_remote_with(
                    &transport,
                    &|_| panic!("must not look up any explicit token or saved session"),
                    &mut graph,
                    command,
                    false,
                    &mut output,
                )
                .unwrap_err();
                assert_eq!(error_code(&error), "hub.publish.invalid_input");
                assert_eq!(crate::error_exit_code(&error), 2);
                assert!(
                    error
                        .to_string()
                        .contains("create it with --public or --private")
                );
                assert!(output.is_empty());
                assert!(transport.requests.borrow().is_empty());
            }
        }
    }
}

#[test]
fn explicit_push_checks_capabilities_before_credential_lookup() {
    let hub = LoopbackHub::start();
    let transport = VisibilityTransport {
        hub: &hub,
        major: None,
        refusal: None,
        requests: RefCell::new(Vec::new()),
    };
    let mut history = history();
    let command =
        crate::Cli::try_parse_from(["gf", "push", "https://hub.test/curate/claims", "--public"])
            .unwrap()
            .command
            .unwrap();
    let error = crate::porcelain::remote::run_remote_with(
        &transport,
        &|_| panic!("capability preflight precedes credentials"),
        &mut history.graph,
        command,
        false,
        &mut Vec::new(),
    )
    .unwrap_err();
    assert_eq!(error_code(&error), "hub.publish.unsupported_future");
    assert!(
        transport
            .requests
            .borrow()
            .iter()
            .all(|r| r.url.ends_with("/.gf/publish"))
    );
}

#[test]
fn push_choice_is_bound_before_identity_and_context_is_read_only_once() {
    let history = history();
    let hub = LoopbackHub::private();
    let transport = VisibilityTransport {
        hub: &hub,
        major: Some(1),
        refusal: None,
        requests: RefCell::new(Vec::new()),
    };
    let mut args = push_arguments("curate/claims", None);
    let mut notices = Vec::new();
    let mut operations = Vec::new();
    let chosen = interactive_push(
        &transport,
        &history.graph,
        &mut args,
        &mut std::io::Cursor::new(b"p\n"),
        &mut notices,
        &mut operations,
    )
    .unwrap();
    assert!(args.visibility.public);
    let prompt = String::from_utf8(notices.clone()).unwrap();
    assert_eq!(prompt.matches("[p/r/cancel]").count(), 1);
    assert!(prompt.contains("anyone can read and clone it") && prompt.contains("needs a plan"));
    let requests = transport.requests.borrow();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.url.ends_with("/context"))
            .count(),
        1
    );
    let context = requests
        .iter()
        .position(|r| r.url.ends_with("/context"))
        .unwrap();
    let status = requests
        .iter()
        .position(|r| r.url.contains("/operations/"))
        .unwrap();
    assert!(context < status);
    assert_eq!(
        requests
            .iter()
            .find(|r| r.url.ends_with("/sessions"))
            .unwrap()
            .header(PUBLISH_VISIBILITY_HEADER),
        Some("public")
    );
    drop(requests);
    let explicit = run(&transport, &history.graph, &args).unwrap();
    assert!(explicit.replayed);
    assert_eq!(
        chosen.receipt.operation_uuid,
        explicit.receipt.operation_uuid
    );
    assert_eq!(operations, vec![chosen.receipt.operation_uuid]);
    // An existing published target requires no create answer and omits intent.
    let mut args = push_arguments("curate/claims", None);
    notices.clear();
    interactive_push(
        &transport,
        &history.graph,
        &mut args,
        &mut std::io::Cursor::new(b""),
        &mut notices,
        &mut operations,
    )
    .unwrap();
    assert!(notices.is_empty());
    assert!(args.visibility().unwrap().is_none());
}

#[test]
fn cancelled_push_reads_context_but_never_derives_an_operation_or_opens_a_session() {
    let history = history();
    for answer in [b"\n".as_slice(), b"cancel\n", b"x\n", b""] {
        let hub = LoopbackHub::private();
        let transport = VisibilityTransport {
            hub: &hub,
            major: Some(1),
            refusal: None,
            requests: RefCell::new(Vec::new()),
        };
        let mut args = push_arguments("curate/claims", None);
        let mut notices = Vec::new();
        let mut operations = Vec::new();
        let error = interactive_push(
            &transport,
            &history.graph,
            &mut args,
            &mut std::io::Cursor::new(answer),
            &mut notices,
            &mut operations,
        )
        .unwrap_err();
        assert_eq!(error_code(&error), "hub.publish.invalid_input");
        assert_eq!(crate::error_exit_code(&error), 2);
        assert!(error.to_string().contains("cancelled; nothing was created"));
        assert!(operations.is_empty());
        assert_eq!(
            String::from_utf8(notices)
                .unwrap()
                .matches("[p/r/cancel]")
                .count(),
            1
        );
        assert!(
            transport
                .requests
                .borrow()
                .iter()
                .all(|r| r.method == HubMethod::Get
                    && (r.url.ends_with("/.gf/publish") || r.url.ends_with("/context")))
        );
        assert_eq!(
            hub.hub
                .handle(&HubRequest::new(
                    HubMethod::Get,
                    "https://hub.test/curate/claims/.gf/refs"
                ))
                .status,
            404
        );
    }
}

#[test]
fn private_push_choice_retains_plan_refusal_before_upload() {
    let history = history();
    let hub = LoopbackHub::private();
    let transport = VisibilityTransport {
        hub: &hub,
        major: Some(1),
        refusal: Some(("entitlement_denied", "private_requires_plan")),
        requests: RefCell::new(Vec::new()),
    };
    let error = interactive_push(
        &transport,
        &history.graph,
        &mut push_arguments("curate/claims", None),
        &mut std::io::Cursor::new(b"r\n"),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert_eq!(error_code(&error), "hub.publish.entitlement_denied");
    assert!(error.to_string().contains("private_requires_plan"));
    assert!(
        transport
            .requests
            .borrow()
            .iter()
            .all(|r| r.method != HubMethod::Put && !r.url.ends_with("/commit"))
    );
}

#[test]
fn explicit_push_dispatch_records_and_replays_public_and_entitled_private_receipts() {
    // No refusal emulates an entitled account; ReferenceHub owns publication,
    // not account/visibility policy.
    for (repository, flag, visibility) in [
        ("curate/claims", "--public", "public"),
        ("https://hub.test/curate/claims", "--private", "private"),
    ] {
        let mut history = history();
        let hub = LoopbackHub::start();
        let transport = VisibilityTransport {
            hub: &hub,
            major: Some(1),
            refusal: None,
            requests: RefCell::new(Vec::new()),
        };
        let invoke = |graph: &mut GraphForge| {
            let command =
                crate::Cli::try_parse_from(["gf", "push", repository, "--hub", HUB, flag])
                    .unwrap()
                    .command
                    .unwrap();
            let mut output = Vec::new();
            crate::porcelain::remote::run_remote_with(
                &transport,
                &|_| Ok(Some(token(&hub, "curate/claims"))),
                graph,
                command,
                true,
                &mut output,
            )
            .unwrap();
            serde_json::from_slice::<serde_json::Value>(&output).unwrap()
        };
        let first = invoke(&mut history.graph);
        assert_eq!(
            first["repository"],
            json!({"owner":"curate", "repository":"claims"})
        );
        let registry = history.graph.research_version_retention().unwrap();
        assert!(
            registry
                .remote_branches
                .values()
                .any(|r| r.branch_uuid == history.branch
                    && r.repository_url == "https://hub.test/curate/claims")
        );
        let before = transport.requests.borrow().len();
        assert_eq!(invoke(&mut history.graph), first);
        assert!(
            transport.requests.borrow()[before..]
                .iter()
                .all(|r| r.method == HubMethod::Get)
        );
        let requests = transport.requests.borrow();
        assert!(requests.iter().any(|r| r.method == HubMethod::Put));
        assert_eq!(
            requests
                .iter()
                .find(|r| r.url.ends_with("/sessions"))
                .unwrap()
                .header(PUBLISH_VISIBILITY_HEADER),
            Some(visibility)
        );
    }
}

#[test]
fn interactive_push_without_required_capabilities_fails_before_authentication() {
    let hub = LoopbackHub::start();
    for major in [None, Some(1), Some(2)] {
        let transport = VisibilityTransport {
            hub: &hub,
            major,
            refusal: None,
            requests: RefCell::new(Vec::new()),
        };
        let mut args = push_arguments("https://hub.test/curate/claims", None);
        let (identity, base) = args.destination().unwrap();
        let resolution = resolve_publication(&transport, &identity, &base, None).unwrap();
        let mut input = std::io::Cursor::new(b"p\n");
        let mut notices = Vec::new();
        let mut environment = PublishEnvironment {
            terminal: Some(&mut input),
            bypass: None,
            credential: Credential::DeviceFlow,
            wait: &|_| panic!("no authentication"),
            clock: &Instant::now,
            notices: &mut notices,
            resolution: Some(resolution),
        };
        let error =
            push_visibility::choose_push_visibility(&transport, &mut args, &mut environment)
                .unwrap_err();
        assert_eq!(error_code(&error), "hub.publish.unsupported_future");
        assert_eq!(input.position(), 0);
        assert!(notices.is_empty());
        assert!(
            transport
                .requests
                .borrow()
                .iter()
                .all(|r| r.url.ends_with("/.gf/publish"))
        );
    }
}
