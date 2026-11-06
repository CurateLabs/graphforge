//! Parsed publish argv through the actual command path, durable reopen and HTTP.
use super::*;
use clap::Parser as _;

fn publish_command(
    transport: &dyn Transport,
    project: &Path,
    arguments: &[&str],
    load_token: impl FnOnce() -> Result<Option<PublishToken>, GfError>,
) -> Result<String, CliRuntimeError> {
    let mut argv = vec!["gf", "publish"];
    argv.extend_from_slice(arguments);
    let crate::Command::Publish(args) = crate::Cli::try_parse_from(argv).unwrap().command.unwrap()
    else {
        panic!("expected publish");
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let result = run_command_with_transport(
        transport,
        load_token,
        &args,
        Some(project.to_owned()),
        None,
        false,
        &mut stdout,
        &mut crate::Diagnostics::new(false, &mut stderr),
    );
    let diagnostics = String::from_utf8(stderr).unwrap();
    assert!(
        diagnostics.is_empty() || diagnostics == format!("Publish Hub: {HUB}\n"),
        "{diagnostics}"
    );
    result?;
    Ok(String::from_utf8(stdout).unwrap())
}

fn publish_to(hub: &LoopbackHub, project: &Path, arguments: &[&str], repository: &str) -> String {
    let secret = token(hub, repository);
    let sensitive = secret.expose().to_owned();
    let output =
        publish_command(&hub.transport(), project, arguments, || Ok(Some(secret))).unwrap();
    assert!(!output.contains(&sensitive));
    output
}

fn local(hub: &LoopbackHub, project: &Path, arguments: &[&str]) {
    porcelain::gf(hub, project, arguments).unwrap();
}

fn served_head(hub: &LoopbackHub, repository: &str, label: &str) -> String {
    let (_, lineage) = served_lineage(hub, repository);
    lineage
        .branches
        .iter()
        .find(|branch| branch.ref_name == label)
        .unwrap()
        .head_version_uuid
        .clone()
}

fn reject_before_credentials(hub: &LoopbackHub, project: &Path, arguments: &[&str], code: &str) {
    let before = hub.log().len();
    let error = publish_command(&hub.transport(), project, arguments, || {
        panic!("invalid defaults must fail before credential loading")
    })
    .unwrap_err();
    assert!(error.to_string().contains(code), "{error}");
    assert_eq!(
        hub.log().len(),
        before,
        "invalid defaults must not contact the Hub"
    );
}

#[test]
fn repeat_publish_reopens_advances_the_recorded_destination_and_ref_and_replays() {
    let History {
        root,
        mut graph,
        branch,
        ..
    } = history();
    let project = root.path().join("project");
    let head = edit(&mut graph, branch, 2);
    drop(graph);
    let hub = LoopbackHub::start();
    let repository = "curate/claims";
    publish_to(
        &hub,
        &project,
        &[&repository_url(repository), "--ref", "main"],
        repository,
    );
    assert_eq!(served_head(&hub, repository, "main"), head.to_string());

    // Only durable upstream state survives: every command closes the Project.
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    let next = edit(&mut graph, branch, 3);
    drop(graph);
    let output = publish_to(&hub, &project, &[], repository);
    assert!(
        output.contains("Published curate/claims (main) at revision sha256:"),
        "{output}"
    );
    assert_eq!(served_head(&hub, repository, "main"), next.to_string());
    let reopened = GraphForge::new(project.to_str()).unwrap();
    let committed = generation(&reopened);
    drop(reopened);
    let uploads = puts(&hub.log()).len();
    publish_to(&hub, &project, &[], repository);
    assert_eq!(puts(&hub.log()).len(), uploads);
    assert_eq!(
        generation(&GraphForge::new(project.to_str()).unwrap()),
        committed
    );
    assert_eq!(
        hub.hub
            .revisions(&RepositoryIdentity::parse(repository).unwrap())
            .len(),
        2
    );

    let destination = root.path().join("published-copy");
    clone_into(&hub, repository, &destination, Some("main"), None);
    let clone = GraphForge::new(destination.to_str()).unwrap();
    assert_eq!(
        clone.research_version_retention().unwrap().heads[&branch],
        next
    );
    assert_eq!(
        porcelain::sum(clone.execute("MATCH (n:Item) RETURN sum(n.score)").unwrap()),
        3
    );
}

#[test]
fn current_branch_and_explicit_ref_use_their_own_upstreams() {
    let History {
        root,
        graph,
        branch,
        ..
    } = history();
    let project = root.path().join("project");
    drop(graph);
    let hub = LoopbackHub::start();
    // No current selection is needed when main is the only Branch.
    publish_to(
        &hub,
        &project,
        &[&repository_url("curate/main")],
        "curate/main",
    );
    local(
        &hub,
        &project,
        &["branch", "analysis", "--author", "Researcher"],
    );
    local(&hub, &project, &["switch", "analysis"]);
    reject_before_credentials(&hub, &project, &[], "GF_RESEARCH_UPSTREAM_UNSET");
    publish_to(
        &hub,
        &project,
        &[&repository_url("curate/analysis")],
        "curate/analysis",
    );

    local(&hub, &project, &["switch", "main"]);
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    let main_head = edit(&mut graph, branch, 7);
    drop(graph);
    publish_to(&hub, &project, &[], "curate/main");
    assert_eq!(
        served_head(&hub, "curate/main", "main"),
        main_head.to_string()
    );
    local(&hub, &project, &["switch", "analysis"]);
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    let analysis = graph
        .research_version_retention()
        .unwrap()
        .current_branch
        .unwrap();
    let analysis_head = edit(&mut graph, analysis, 9);
    drop(graph);
    publish_to(&hub, &project, &[], "curate/analysis");
    assert_eq!(
        served_head(&hub, "curate/analysis", "analysis"),
        analysis_head.to_string()
    );
    // Explicit --ref selects main even while analysis is current.
    publish_to(&hub, &project, &["--ref", "main"], "curate/main");
    assert_eq!(
        served_head(&hub, "curate/main", "main"),
        main_head.to_string()
    );
}

#[test]
fn ambiguous_or_unset_defaults_refuse_instead_of_guessing() {
    let History {
        root,
        mut graph,
        branch,
        ..
    } = history();
    let project = root.path().join("project");
    create_branch(
        &mut graph,
        "other",
        BranchSource::Branch {
            branch_uuid: branch,
        },
    );
    assert!(
        graph
            .research_version_retention()
            .unwrap()
            .current_branch
            .is_none()
    );
    drop(graph);
    let hub = LoopbackHub::start();
    reject_before_credentials(
        &hub,
        &project,
        &[&repository_url("curate/claims")],
        "GF_RESEARCH_BRANCH_UNSELECTED",
    );
    reject_before_credentials(
        &hub,
        &project,
        &["--ref", "main"],
        "GF_RESEARCH_UPSTREAM_UNSET",
    );
    local(&hub, &project, &["switch", "main"]);
    for repository in ["curate/one", "curate/two"] {
        publish_to(
            &hub,
            &project,
            &[&repository_url(repository), "--ref", "main"],
            repository,
        );
    }
    reject_before_credentials(&hub, &project, &[], "GF_RESEARCH_UPSTREAM_AMBIGUOUS");
    // Explicit destination resolves the ambiguity without changing stored citations.
    publish_to(
        &hub,
        &project,
        &[&repository_url("curate/two")],
        "curate/two",
    );
}

#[test]
fn immutable_version_publish_never_infers_a_branch_or_destination() {
    let History {
        root, graph, base, ..
    } = history();
    let project = root.path().join("project");
    let hub = LoopbackHub::start();
    // A Version augments an existing publication without moving its Branch refs.
    publish_ok(&hub, &graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let (_, before) = served_lineage(&hub, "curate/claims");
    drop(graph);
    let version = base.to_string();
    publish_to(
        &hub,
        &project,
        &[&repository_url("curate/claims"), "--version-uuid", &version],
        "curate/claims",
    );
    let (_, lineage) = served_lineage(&hub, "curate/claims");
    assert_eq!(lineage.branches, before.branches);
    assert!(
        lineage
            .versions
            .iter()
            .any(|entry| entry.version_uuid == version)
    );
    let graph = GraphForge::new(project.to_str()).unwrap();
    assert!(
        graph
            .research_version_retention()
            .unwrap()
            .remote_branches
            .is_empty()
    );
    drop(graph);
    reject_before_credentials(&hub, &project, &[], "GF_RESEARCH_UPSTREAM_UNSET");
    publish_to(
        &hub,
        &project,
        &[&repository_url("curate/claims"), "--ref", "main"],
        "curate/claims",
    );
    reject_before_credentials(
        &hub,
        &project,
        &["--version-uuid", &version],
        "name the repository explicitly",
    );
    assert!(
        crate::Cli::try_parse_from([
            "gf",
            "publish",
            "curate/claims",
            "--ref",
            "main",
            "--version-uuid",
            &version
        ])
        .is_err()
    );
}

#[test]
fn missing_version_destination_never_creates_a_project() {
    let root = tempfile::tempdir().unwrap();
    let hub = LoopbackHub::start();
    let version = Uuid::now_v7().to_string();
    for (name, visibility) in [
        ("omitted", None),
        ("public", Some("--public")),
        ("private", Some("--private")),
    ] {
        let project = root.path().join(name);
        let mut arguments = vec!["--version-uuid", version.as_str()];
        if let Some(flag) = visibility {
            arguments.push(flag);
        }
        reject_before_credentials(&hub, &project, &arguments, "name the repository explicitly");
        assert!(
            !project.exists(),
            "invalid publish created {}",
            project.display()
        );
    }
}

#[test]
fn inferred_destination_still_checks_visibility_before_loading_credentials() {
    let History { root, graph, .. } = history();
    let project = root.path().join("project");
    drop(graph);
    let hub = LoopbackHub::start();
    publish_to(
        &hub,
        &project,
        &[&repository_url("curate/claims"), "--ref", "main"],
        "curate/claims",
    );
    let transport = visibility::VisibilityTransport {
        hub: &hub,
        major: None,
        refusal: None,
        requests: RefCell::new(Vec::new()),
    };
    let error = publish_command(&transport, &project, &["--public"], || {
        panic!("unsupported visibility must fail before credential loading")
    })
    .unwrap_err();
    assert_eq!(error_code(&error), "hub.publish.unsupported_future");
    assert!(
        transport
            .requests
            .borrow()
            .iter()
            .all(|request| request.method == HubMethod::Get)
    );
}

#[test]
fn publish_help_explains_optional_defaults_and_primary_login() {
    let help = crate::Cli::try_parse_from(["gf", "publish", "--help"])
        .err()
        .unwrap()
        .to_string();
    for text in [
        "[REPOSITORY]",
        "current Branch",
        "upstream",
        "gf auth login",
        "device approval",
        "Required with --version-uuid",
    ] {
        assert!(help.contains(text), "missing {text:?}: {help}");
    }
    assert!(!help.contains("<--ref <GIT_REF>|--version-uuid <VERSION_UUID>>"));
}
