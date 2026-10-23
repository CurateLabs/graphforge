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
    result: Result<(), GfError>,
    /// What the command raised, as the CLI collects it.
    warnings: Vec<graphforge_api::CoreVersionWarning>,
    /// The standard-error channel the warnings were printed on.
    diagnostics_stderr: String,
}

fn publish_project(hub: &LoopbackHub, path: &Path, args: &PublishArgs, json: bool) -> Published {
    let mut diagnostics_stderr = Vec::new();
    let mut notices = Vec::new();
    let mut stdout = Vec::new();
    let mut diagnostics = Diagnostics::new(json, &mut diagnostics_stderr);
    let mut environment = PublishEnvironment {
        bypass: None,
        credential: Credential::Token(token(hub, REPOSITORY)),
        wait: &|_| panic!("no device flow expected"),
        notices: &mut notices,
        resolution: None,
    };
    let result = open_and_publish(
        &hub.transport(),
        path.to_str().unwrap(),
        args,
        &mut environment,
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
    assert_eq!(text.warnings[0].producing_core_version, CORE_X);
    assert!(
        text.diagnostics_stderr
            .starts_with("warning GF_CORE_VERSION_MISMATCH:"),
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
    crate::write_runtime_error(&error.into(), true, &mut rendered, &failed.warnings).unwrap();
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
    assert_eq!(published.diagnostics_stderr, "");
}
