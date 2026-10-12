//! A fresh graph workspace links every published payload it can and names the
//! ones it copies (#1992).
//!
//! Hydration hard-links immutable payloads from the content store. A payload it
//! copies instead occupies its bytes a second time on the project volume for as
//! long as the workspace exists, and a workspace that outlives its process (a
//! kill leaves it behind) keeps that space. So the set of copies is closed here:
//! each is a file a writable facade rewrites in place or locks, and anything
//! else that stops sharing the published object fails this test.

#![cfg(unix)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use graphforge_api::GraphForge;

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

const NODES: usize = 1_024;
const FAN_OUT: usize = 4;

/// The complete set of payloads a fresh workspace copies
/// (`requires_single_link_materialization` in `graphforge-storage`):
///
/// - `semantic-routes.json` is the route table, replaced in place by commits;
/// - the ordinal identity manifest, receipt and lock are rewritten and locked
///   by the identity writer, so they cannot share a content-store inode;
/// - `tombstones-v4-*` runs are not mutable. They are copied only because they
///   are not admitted at capture like the linked identity runs, so a change in
///   this set is a decision, not drift.
///
/// The identity runs themselves (`forward-v4-*`, `ordinal-v4-*`) are immutable
/// and read-only, and are linked.
fn is_intended_copy(path: &str) -> bool {
    matches!(
        path,
        "semantic-routes.json"
            | "topology/uuid-membership/manifest.json"
            | "topology/uuid-membership/topology-receipt.json"
            | "topology/uuid-membership/ordinal-v4-manifest.json"
            | "topology/uuid-membership/ordinal-v4-receipt.json"
            | "topology/uuid-membership/ordinal-v4.lock"
    ) || path.starts_with("topology/uuid-membership/tombstones-v4-")
}

fn workspace(project: &Path) -> PathBuf {
    let workspaces: Vec<_> = std::fs::read_dir(project)
        .expect("project directory")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("graphforge-graph-workspace-")
            })
        })
        .collect();
    assert_eq!(workspaces.len(), 1, "{workspaces:?}");
    workspaces.into_iter().next().unwrap()
}

/// Open `project` and report, for each published payload, whether the
/// facade's workspace holds the published object itself.
fn payloads(project: &Path) -> Vec<(String, bool, u64)> {
    let forge = GraphForge::new(project.to_str()).expect("project opens");
    let workspace = workspace(project);
    let inventory = graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("compact generation declares an inventory");
    let payloads = inventory
        .files
        .iter()
        .map(|entry| {
            let published =
                graphforge_storage::graph_object_path(project, &entry.content_sha256).unwrap();
            let materialized = workspace.join(&entry.relative_path);
            let linked = graphforge_filesystem::path_identity(&materialized)
                .unwrap_or_else(|error| panic!("{}: {error}", entry.relative_path))
                == graphforge_filesystem::path_identity(&published).unwrap();
            let links = graphforge_filesystem::path_link_count(&materialized).unwrap();
            (entry.relative_path.clone(), linked, links)
        })
        .collect();
    let evidence = forge.graph_open_evidence();
    let copied = u64::try_from(
        inventory
            .files
            .iter()
            .filter(|entry| is_intended_copy(&entry.relative_path))
            .count(),
    )
    .unwrap();
    assert_eq!(
        evidence.files_copied, copied,
        "the open's own account of what it copied is the named set"
    );
    payloads
}

fn assert_only_named_copies(project: &Path) {
    let payloads = payloads(project);
    assert!(
        payloads
            .iter()
            .any(|(path, ..)| path.contains("uuid-membership/forward-v4-")),
        "the fixture publishes identity runs: {payloads:?}"
    );
    let mut copied = BTreeSet::new();
    for (path, linked, links) in &payloads {
        if *linked {
            assert!(*links > 1, "{path} is the published object with one name");
        } else {
            assert_eq!(*links, 1, "{path} is neither linked nor a private copy");
            copied.insert(path.as_str());
        }
    }
    let unexpected: Vec<_> = copied
        .iter()
        .filter(|path| !is_intended_copy(path))
        .collect();
    assert!(
        unexpected.is_empty(),
        "payloads copied instead of linked: {unexpected:?}"
    );
    assert!(
        copied.contains("semantic-routes.json"),
        "the named copies are really copies: {copied:?}"
    );
}

#[test]
fn a_fresh_workspace_links_every_published_payload_except_the_named_copies() {
    let indexed = tempfile::tempdir().unwrap();
    let indexed_project = indexed.path().join("state");
    bulk_fixture::generate_bulk_graph(&indexed_project, NODES, FAN_OUT);
    assert_only_named_copies(&indexed_project);

    let propertied = tempfile::tempdir().unwrap();
    let propertied_project = propertied.path().join("state");
    bulk_fixture::generate_bulk_graph_with_properties(&propertied_project, NODES, FAN_OUT, true);
    assert_only_named_copies(&propertied_project);
}
