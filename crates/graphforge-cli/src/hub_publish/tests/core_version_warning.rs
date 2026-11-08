//! `gf publish` reports a Core version warning from opening the Project before
//! it publishes anything, whether or not the publication then succeeds (#1936).

use super::*;
use crate::Diagnostics;

#[allow(dead_code, reason = "shared with the CLI integration tests")]
#[path = "../../../tests/support/core_version.rs"]
mod relabel;

const CORE_X: &str = "0.0.1";
const REPOSITORY: &str = "curate/claims";

/// A Project with a published-ready Branch, written by the real writer and, for
/// `Some(version)`, relabelled as written by that Core.
fn project(root: &Path, relabelled: Option<&str>) -> PathBuf {
    let path = root.join("project");
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute("CREATE (:Item {score:0})").unwrap();
    let (branch, _) = create_branch(&mut graph, "main", current());
    edit(&mut graph, branch, 1);
    drop(graph);
    if let Some(version) = relabelled {
        relabel::relabel_generation(&path, version);
    }
    path
}

struct Published {
    result: Result<(), CliRuntimeError>,
    /// What the command raised, as the CLI collects it.
    warnings: Vec<graphforge_api::Warning>,
    /// The standard-error channel the warnings were printed on.
    diagnostics_stderr: String,
    stdout: String,
}

fn publish_project(hub: &LoopbackHub, path: &Path, args: &PublishArgs, json: bool) -> Published {
    let repository = args.destination().unwrap().0.canonical_name();
    let mut diagnostics_stderr = Vec::new();
    let mut stdout = Vec::new();
    let mut diagnostics = Diagnostics::new(json, &mut diagnostics_stderr);
    let result = run_command_with_transport(
        &hub.transport(),
        || Ok(Some(token(hub, &repository))),
        args,
        Some(path.to_owned()),
        None,
        json,
        &mut stdout,
        &mut diagnostics,
    );
    let warnings = diagnostics.collected();
    drop(diagnostics);
    Published {
        result,
        warnings,
        diagnostics_stderr: String::from_utf8(diagnostics_stderr).unwrap(),
        stdout: String::from_utf8(stdout).unwrap(),
    }
}

#[test]
fn publishing_a_project_from_another_core_warns_before_publishing() {
    let root = tempfile::tempdir().unwrap();
    let path = project(root.path(), Some(CORE_X));
    let hub = LoopbackHub::start();

    let text = publish_project(&hub, &path, &on_ref(REPOSITORY, "main"), false);
    text.result.unwrap();
    assert_eq!(text.warnings.len(), 1);
    assert_eq!(
        serde_json::to_value(&text.warnings[0]).unwrap()["producing_core_version"],
        CORE_X
    );
    assert!(
        text.diagnostics_stderr
            .contains("warning GF_CORE_VERSION_MISMATCH:"),
        "{}",
        text.diagnostics_stderr
    );
    assert!(text.diagnostics_stderr.contains(CORE_X));
    assert!(
        text.diagnostics_stderr
            .contains(graphforge_api::GRAPHFORGE_VERSION)
    );

    // Recording the confirmed publication as the Branch's upstream is a write by the
    // running Core, so the Project it leaves is this Core's and no longer warns.
    let reopened = GraphForge::new(path.to_str()).unwrap();
    assert!(reopened.open_warnings().is_empty());
    drop(reopened);

    // Under --json nothing separate is printed; the warning is collected for
    // the command's one document. A second Project from the other Core, on its
    // own Hub, since the first now is this Core's.
    let other_root = root.path().join("json");
    std::fs::create_dir(&other_root).unwrap();
    let other = project(&other_root, Some(CORE_X));
    let other_hub = LoopbackHub::start();
    let json = publish_project(&other_hub, &other, &on_ref(REPOSITORY, "main"), true);
    json.result.unwrap();
    assert_eq!(json.warnings.len(), 1);
    assert_eq!(json.diagnostics_stderr, "");
}

#[test]
fn a_failed_publish_still_carries_the_open_warning_in_its_one_error_document() {
    let root = tempfile::tempdir().unwrap();
    let path = project(root.path(), Some(CORE_X));
    let hub = LoopbackHub::start();

    let failed = publish_project(&hub, &path, &on_ref(REPOSITORY, "no-such-branch"), true);
    let error = failed.result.unwrap_err();
    assert_eq!(failed.warnings.len(), 1);
    assert_eq!(failed.diagnostics_stderr, "");

    let mut rendered = Vec::new();
    crate::write_runtime_error(&error, true, &mut rendered, &failed.warnings).unwrap();
    let document: serde_json::Value = serde_json::from_slice(&rendered).unwrap();
    assert!(document["error"]["code"].is_string(), "{document}");
    assert_eq!(document["warnings"][0]["producing_core_version"], CORE_X);
}

#[test]
fn publishing_a_project_from_the_running_core_does_not_warn() {
    let root = tempfile::tempdir().unwrap();
    let path = project(root.path(), None);
    let hub = LoopbackHub::start();

    let published = publish_project(&hub, &path, &on_ref(REPOSITORY, "main"), false);
    published.result.unwrap();
    assert!(published.warnings.is_empty());
    assert_eq!(
        published.diagnostics_stderr,
        format!("Publish Hub: {HUB}\n")
    );
}

#[cfg(target_os = "linux")]
#[test]
fn publishing_from_tmpfs_records_source_qualification_without_changing_hub_receipt() {
    let root = tempfile::tempdir_in("/dev/shm").unwrap();
    let path = project(root.path(), None);
    let hub = LoopbackHub::start();
    let published = publish_project(&hub, &path, &on_ref(REPOSITORY, "main"), true);
    published.result.unwrap();
    assert_eq!(published.warnings.len(), 1);
    assert_eq!(published.warnings[0].code(), "GF_UNVERIFIED_FILESYSTEM");
    let document: serde_json::Value = serde_json::from_str(&published.stdout).unwrap();
    assert_eq!(
        document["source_unverified_durability"]["filesystem_type"],
        "tmpfs"
    );
    assert!(document.get("request_commitment").is_some());
    assert_eq!(
        GraphForge::new(path.to_str()).unwrap().open_warnings()[0].code(),
        "GF_UNVERIFIED_FILESYSTEM"
    );
}

#[cfg(target_os = "linux")]
fn copy_directory(source: &Path, target: &Path) {
    std::fs::create_dir(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_directory(&entry.path(), &destination);
        } else {
            std::fs::copy(entry.path(), destination).unwrap();
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn replay_preserves_the_original_source_qualification_after_tracking_writes() {
    for private in [false, true] {
        let qualified = tempfile::tempdir().unwrap();
        let source = project(qualified.path(), None);
        assert!(
            GraphForge::new(source.to_str())
                .unwrap()
                .committed_unverified_durability()
                .unwrap()
                .is_none()
        );
        let unproven = tempfile::tempdir_in("/dev/shm").unwrap();
        let path = unproven.path().join("copied-project");
        copy_directory(&source, &path);
        let hub = if private {
            LoopbackHub::private()
        } else {
            LoopbackHub::start()
        };
        let args = on_ref(REPOSITORY, "main");
        let first = publish_project(&hub, &path, &args, true);
        first.result.unwrap();
        let document: serde_json::Value = serde_json::from_str(&first.stdout).unwrap();
        assert!(document.get("source_unverified_durability").is_none());
        assert_eq!(
            GraphForge::new(path.to_str())
                .unwrap()
                .committed_unverified_durability()
                .unwrap()
                .unwrap()
                .filesystem_type,
            "tmpfs"
        );
        let uploads_before = puts(&hub.log()).len();
        let replay = publish_project(&hub, &path, &args, true);
        replay.result.unwrap();
        assert_eq!(puts(&hub.log()).len(), uploads_before);
        assert_eq!(replay.stdout, first.stdout);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn version_replay_preserves_an_unverified_writer_after_a_qualified_tracking_write() {
    let unproven = tempfile::tempdir_in("/dev/shm").unwrap();
    let source = project(unproven.path(), None);
    let hub = LoopbackHub::start();
    publish_project(&hub, &source, &on_ref(REPOSITORY, "main"), true)
        .result
        .unwrap();
    let graph = GraphForge::new(source.to_str()).unwrap();
    let head = graph
        .research_version_retention()
        .unwrap()
        .heads
        .values()
        .next()
        .copied()
        .unwrap();
    drop(graph);
    let version_args = PublishArgs {
        version_uuid: Some(head.to_string()),
        ..args(REPOSITORY)
    };
    let original = publish_project(&hub, &source, &version_args, true);
    original.result.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&original.stdout).unwrap()["source_unverified_durability"]
            ["filesystem_type"],
        "tmpfs"
    );
    let qualified = tempfile::tempdir().unwrap();
    let path = qualified.path().join("project");
    copy_directory(&source, &path);
    publish_project(&hub, &path, &on_ref("curate/qualified-copy", "main"), true)
        .result
        .unwrap();
    assert!(
        GraphForge::new(path.to_str())
            .unwrap()
            .committed_unverified_durability()
            .unwrap()
            .is_none()
    );
    let replay = publish_project(&hub, &path, &version_args, true);
    replay.result.unwrap();
    assert_eq!(replay.stdout, original.stdout);
}

#[cfg(target_os = "linux")]
#[test]
fn replay_reports_unavailable_when_another_publication_replaces_its_manifest() {
    let qualified = tempfile::tempdir().unwrap();
    let source = project(qualified.path(), None);
    let hub = LoopbackHub::start();
    let original_args = on_ref(REPOSITORY, "main");
    let graph = GraphForge::new(source.to_str()).unwrap();
    let original = publish_ok(&hub, &graph, &original_args).result.unwrap();
    drop(graph);
    let frozen = qualified.path().join("original-copy");
    copy_directory(&source, &frozen);
    let mut changed = GraphForge::new(source.to_str()).unwrap();
    let branch = *changed
        .research_version_retention()
        .unwrap()
        .branches
        .keys()
        .next()
        .unwrap();
    edit(&mut changed, branch, 7);
    publish_ok(&hub, &changed, &original_args).result.unwrap();
    drop(changed);
    let graph = GraphForge::new(frozen.to_str()).unwrap();
    let uploads = puts(&hub.log()).len();
    let replay = publish_ok(&hub, &graph, &original_args).result.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, original.receipt);
    assert_eq!(
        replay.source_unverified_durability,
        PublishedDurability::Unavailable
    );
    assert_eq!(puts(&hub.log()).len(), uploads);
    let mut json = Vec::new();
    write_outcome(&replay, true, &mut json).unwrap();
    let document: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(
        document.get("source_unverified_durability"),
        Some(&serde_json::Value::Null)
    );
    let mut text = Vec::new();
    write_outcome(&replay, false, &mut text).unwrap();
    assert!(
        String::from_utf8(text)
            .unwrap()
            .contains("Source durability: unavailable")
    );
}

#[cfg(target_os = "linux")]
#[test]
fn session_complete_uses_the_original_manifest_when_the_derived_stamp_differs() {
    struct StatusMiss<'a>(&'a LoopbackHub);
    impl Transport for StatusMiss<'_> {
        fn get(
            &self,
            url: &Url,
            range: Option<u64>,
            if_range: Option<&str>,
            limit: u64,
        ) -> Result<HttpResponse, GfError> {
            self.0.transport().get(url, range, if_range, limit)
        }
        fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
            let mut response = self.0.transport().exchange(request, limit)?;
            if request.method == HubMethod::Get
                && request.url.contains("/.gf/publish/operations/")
                && response.status == 200
            {
                let mut status: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
                status["receipt"] = serde_json::Value::Null;
                response.body = serde_json::to_vec(&status).unwrap();
            }
            Ok(response)
        }
    }
    let qualified = tempfile::tempdir().unwrap();
    let source = project(qualified.path(), None);
    let unproven = tempfile::tempdir_in("/dev/shm").unwrap();
    let path = unproven.path().join("project");
    copy_directory(&source, &path);
    let hub = LoopbackHub::start();
    let arguments = on_ref(REPOSITORY, "main");
    let original = publish_project(&hub, &path, &arguments, true);
    original.result.unwrap();
    let graph = GraphForge::new(path.to_str()).unwrap();
    assert!(graph.committed_unverified_durability().unwrap().is_some());
    let uploads = puts(&hub.log()).len();
    let mut notices = Vec::new();
    let outcome = publish(
        &StatusMiss(&hub),
        &graph,
        &arguments,
        &mut PublishEnvironment {
            terminal: None,
            bypass: None,
            credential: Credential::Token(token(&hub, REPOSITORY)),
            clock: &Instant::now,
            wait: &|_| panic!("completed operation must not wait"),
            notices: &mut notices,
            resolution: None,
        },
    )
    .unwrap();
    assert!(outcome.replayed);
    assert_eq!(
        outcome.source_unverified_durability,
        PublishedDurability::Qualified
    );
    assert_eq!(puts(&hub.log()).len(), uploads);
    let mut output = Vec::new();
    write_outcome(&outcome, true, &mut output).unwrap();
    assert_eq!(String::from_utf8(output).unwrap(), original.stdout);
}
