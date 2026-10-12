//! Hydration hard-links immutable payloads and copies only what a facade may
//! write (#1992). A private workspace that copies a payload which is already in
//! the content store duplicates its bytes on the project volume for as long as
//! the workspace exists, and a killed process leaves that behind.

use super::*;
use std::collections::BTreeSet;

/// Every payload of the published generation, as the path inside the workspace
/// and whether that file is the content-store object itself.
fn payloads(project: &Path) -> Vec<(String, bool, u64)> {
    let resolved = graphforge_storage::resolve_project_generation(project).unwrap();
    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    let (workspace, owner, _) = hydrate_graph_workspace(&resolved, false).unwrap();
    let payloads = inventory
        .files
        .iter()
        .map(|entry| {
            let published =
                graphforge_storage::graph_object_path(project, &entry.content_sha256).unwrap();
            let materialized = workspace.join(&entry.relative_path);
            let linked = graphforge_filesystem::path_identity(&materialized).unwrap()
                == graphforge_filesystem::path_identity(&published).unwrap();
            let links = graphforge_filesystem::path_link_count(&materialized).unwrap();
            (entry.relative_path.clone(), linked, links)
        })
        .collect();
    drop(owner);
    payloads
}

/// Payloads a writable facade rewrites in place or locks, which therefore
/// cannot share a content-store inode (`requires_single_link_materialization`
/// in `graphforge-storage`): the route table, mutable artifact pointers, and
/// the ordinal identity manifest, receipt and lock.
fn is_intended_copy(path: &str) -> bool {
    matches!(
        path,
        "topology/uuid-membership/manifest.json"
            | "topology/uuid-membership/topology-receipt.json"
            | "topology/uuid-membership/ordinal-v4-manifest.json"
            | "topology/uuid-membership/ordinal-v4-receipt.json"
            | "topology/uuid-membership/ordinal-v4.lock"
    ) || path == "semantic-routes.json"
        || path.starts_with("topology/uuid-membership/tombstones-v4-")
}

#[test]
fn a_fresh_workspace_links_every_published_payload_except_the_named_copies() {
    let project = tempfile::tempdir().unwrap();
    compact_indexed_project(project.path());

    let payloads = payloads(project.path());
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
            copied.insert(path.clone());
            assert_eq!(*links, 1, "{path} is neither linked nor a private copy");
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
}
