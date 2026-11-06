//! Parsed CLI destinations through native publication and reference Hubs.
use super::*;
use clap::Parser as _;

fn parsed(arguments: &[&str]) -> PublishArgs {
    let mut argv = vec!["gf", "publish"];
    argv.extend_from_slice(arguments);
    let crate::Command::Publish(args) = crate::Cli::try_parse_from(argv).unwrap().command.unwrap()
    else {
        panic!("expected publish");
    };
    args
}

fn command(
    transport: &dyn Transport,
    project: &Path,
    arguments: &[&str],
    load_token: impl FnOnce() -> Result<Option<PublishToken>, GfError>,
) -> Result<String, CliRuntimeError> {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    run_command_with_transport(
        transport,
        load_token,
        &parsed(arguments),
        Some(project.into()),
        None,
        false,
        &mut stdout,
        &mut crate::Diagnostics::new(false, &mut stderr),
    )?;
    assert_eq!(
        String::from_utf8(stderr).unwrap(),
        format!(
            "Publish Hub: {}\n",
            parsed(arguments)
                .destination()
                .unwrap()
                .1
                .origin()
                .ascii_serialization()
        )
    );
    Ok(String::from_utf8(stdout).unwrap())
}

#[test]
fn short_name_selects_explicit_hub_and_records_that_upstream_after_reopen() {
    let History {
        root,
        graph,
        branch,
        head,
        ..
    } = history();
    let project = root.path().join("project");
    drop(graph);
    let hub = LoopbackHub::start();
    let repository = "curate/claims";
    let secret = token(&hub, repository);
    let sensitive = secret.expose().to_owned();
    let output = command(
        &hub.transport(),
        &project,
        &[repository, "--hub", HUB],
        || Ok(Some(secret)),
    )
    .unwrap();
    assert!(!output.contains(&sensitive));
    assert!(hub.log().iter().any(|request| request.method == "POST"));
    let reopened = GraphForge::new(project.to_str()).unwrap();
    let registry = reopened.research_version_retention().unwrap();
    assert!(registry.remote_branches.values().any(|remote| {
        remote.branch_uuid == branch
            && remote.repository_url == repository_url(repository)
            && remote.head_version_uuid == head
    }));
    drop(reopened);
    // The real clone path reads the chosen Hub's published native packages.
    let destination = root.path().join("published-copy");
    let Some(crate::Command::Clone(clone)) = crate::Cli::try_parse_from([
        "gf",
        "clone",
        repository,
        destination.to_str().unwrap(),
        "--hub",
        HUB,
        "--ref",
        "main",
    ])
    .unwrap()
    .command
    else {
        panic!("expected clone")
    };
    run_clone_with(&hub.transport(), clone, true, &mut Vec::new()).unwrap();
    let cloned = GraphForge::new(destination.to_str()).unwrap();
    assert_eq!(
        cloned.research_version_retention().unwrap().heads[&branch],
        head
    );
}

struct ProductionHub {
    hub: ReferenceHub,
    requests: RefCell<Vec<HubRequest>>,
}
impl Transport for ProductionHub {
    fn get(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<HttpResponse, GfError> {
        panic!("publication uses bounded exchange");
    }
    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        assert_eq!(
            Url::parse(&request.url).unwrap().origin(),
            Url::parse(crate::hub_http::DEFAULT_HUB).unwrap().origin()
        );
        self.requests.borrow_mut().push(request.clone());
        let response = self.hub.handle(request);
        assert!(response.body.len() <= limit);
        Ok(response)
    }
}

#[test]
fn short_name_without_hub_still_publishes_to_production() {
    let History { root, graph, .. } = history();
    let project = root.path().join("project");
    drop(graph);
    let transport = ProductionHub {
        hub: ReferenceHub::with_config(ReferenceHubConfig {
            base_url: crate::hub_http::DEFAULT_HUB.into(),
            ..ReferenceHubConfig::default()
        })
        .unwrap(),
        requests: RefCell::new(Vec::new()),
    };
    let identity = RepositoryIdentity::parse("curate/claims").unwrap();
    let token = PublishToken::new(transport.hub.issue_token(&publish_scope(&identity), 900));
    command(
        &transport,
        &project,
        &["curate/claims", "--ref", "main"],
        || Ok(Some(token)),
    )
    .unwrap();
    assert_eq!(transport.hub.revisions(&identity).len(), 1);
}

#[test]
fn full_url_accepts_matching_hub_and_rejects_other_origins_before_io() {
    let History { root, graph, .. } = history();
    let project = root.path().join("project");
    drop(graph);
    let hub = LoopbackHub::start();
    command(
        &hub.transport(),
        &project,
        &[
            &repository_url("curate/claims"),
            "--hub",
            "https://HUB.test:443/",
            "--ref",
            "main",
        ],
        || Ok(Some(token(&hub, "curate/claims"))),
    )
    .unwrap();
    let absent = root.path().join("must-not-be-created");
    let before = hub.log().len();
    let error = command(
        &hub.transport(),
        &absent,
        &[
            &repository_url("curate/claims"),
            "--hub",
            "https://other.example",
            "--ref",
            "main",
            "--public",
        ],
        || panic!("mismatch must precede credentials"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("same Hub origin"), "{error}");
    assert!(!absent.exists());
    assert_eq!(hub.log().len(), before);
}

#[test]
fn unsafe_explicit_origins_fail_before_network_credentials_or_project_creation() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("absent");
    let hub = LoopbackHub::start();
    for origin in [
        "http://hub.test",
        "https://user:secret@hub.test",
        "https://hub.test/prefix",
        "https://hub.test/?token=secret",
        "https://hub.test/#fragment",
        "https://127.0.0.1",
        "not-a-url",
    ] {
        command(
            &hub.transport(),
            &project,
            &["curate/claims", "--hub", origin],
            || panic!("unsafe origin must precede credentials"),
        )
        .unwrap_err();
        assert!(!project.exists());
        assert!(hub.log().is_empty());
    }
}

#[test]
fn publish_help_and_existing_clone_login_flags_name_the_hub() {
    let help = crate::Cli::try_parse_from(["gf", "publish", "--help"])
        .err()
        .unwrap()
        .to_string();
    assert!(help.contains("--hub <HUB>"), "{help}");
    assert!(help.contains("https://graphforge.sh"), "{help}");
    let Some(crate::Command::Clone(args)) =
        crate::Cli::try_parse_from(["gf", "clone", "curate/claims", "--hub", HUB])
            .unwrap()
            .command
    else {
        panic!("expected clone")
    };
    assert_eq!(args.hub.as_deref(), Some(HUB));
    crate::Cli::try_parse_from(["gf", "auth", "login", "--hub", HUB]).unwrap();
    assert_eq!(
        crate::hub_http::parse_input_at("curate/claims", Some(HUB))
            .unwrap()
            .1
            .as_str(),
        repository_url("curate/claims")
    );
}

#[derive(Clone)]
struct NoticeSink(Rc<RefCell<Vec<u8>>>);
impl Write for NoticeSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct BeforeRequest<'a> {
    inner: &'a dyn Transport,
    notices: NoticeSink,
    absent_project: &'a Path,
    json: bool,
    mismatch: bool,
}
impl Transport for BeforeRequest<'_> {
    fn get(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<HttpResponse, GfError> {
        panic!("publication uses bounded exchange");
    }
    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        assert!(
            !self.absent_project.exists(),
            "visibility preflight must precede Project creation"
        );
        let notices = String::from_utf8(self.notices.0.borrow().clone()).unwrap();
        if self.json {
            assert!(
                notices.is_empty(),
                "JSON must keep its existing document contract"
            );
        } else {
            assert!(
                notices.starts_with(&format!("Publish Hub: {HUB}\n")),
                "{notices}"
            );
            if self.mismatch {
                assert!(
                    notices.contains("Last successful login Hub: https://other.example."),
                    "{notices}"
                );
                assert!(
                    notices.contains(&format!("gf auth login --hub {HUB}")),
                    "{notices}"
                );
                assert!(!notices.contains("no saved session"));
            }
        }
        assert_eq!(
            Url::parse(&request.url).unwrap().origin(),
            Url::parse(HUB).unwrap().origin()
        );
        assert!(request.header("authorization").is_none());
        self.inner.exchange(request, limit)
    }
}

#[test]
fn target_and_last_login_notices_precede_first_request_and_visibility_token_acquisition() {
    for json in [false, true] {
        for previous in [Some("https://other.example"), Some(HUB), None] {
            let hub = LoopbackHub::start();
            let root = tempfile::tempdir().unwrap();
            let absent = root.path().join("absent-project");
            let sink = NoticeSink(Rc::default());
            let inner = hub.transport();
            let transport = BeforeRequest {
                inner: &inner,
                notices: sink.clone(),
                absent_project: &absent,
                json,
                mismatch: previous == Some("https://other.example"),
            };
            let mut notices = sink;
            let error = run_command_with_sources(
                &transport,
                || panic!("unsupported explicit visibility must precede publish-token acquisition"),
                || Ok(previous.map(|origin| Url::parse(origin).unwrap())),
                &parsed(&["curate/claims", "--hub", HUB, "--public", "--ref", "main"]),
                Some(absent.clone()),
                None,
                json,
                &mut Vec::new(),
                &mut crate::Diagnostics::new(json, &mut notices),
            )
            .unwrap_err();
            assert_eq!(
                error_code(&error),
                "hub.publish.unsupported_future",
                "{error}"
            );
            assert!(!hub.log().is_empty());
            assert!(!absent.exists());
        }
    }
}

#[test]
fn advisory_other_hub_metadata_does_not_prevent_valid_explicit_publication() {
    let History { root, graph, .. } = history();
    let project = root.path().join("project");
    drop(graph);
    let hub = LoopbackHub::start();
    let mut notices = Vec::new();
    run_command_with_sources(
        &hub.transport(),
        || Ok(Some(token(&hub, "curate/claims"))),
        || Ok(Some(Url::parse("https://other.example").unwrap())),
        &parsed(&["curate/claims", "--hub", HUB]),
        Some(project),
        None,
        false,
        &mut Vec::new(),
        &mut crate::Diagnostics::new(false, &mut notices),
    )
    .unwrap();
    assert!(
        String::from_utf8(notices)
            .unwrap()
            .contains("Last successful login Hub:")
    );
    assert_eq!(
        hub.hub
            .revisions(&RepositoryIdentity::parse("curate/claims").unwrap())
            .len(),
        1
    );
}
