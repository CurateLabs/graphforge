//! A bulk-built project carries no UUID membership index, and appends are
//! answered from its Parquet (ADR 0058 decision 5, #1902; #1881 criterion
//! "The UUID membership index is no longer produced or read").
//!
//! `legacy_membership_index.rs` proves this for a project built by
//! `GraphWriter`. This builds the project through the public import session,
//! which runs on the bulk builder.

use std::sync::Arc;

use arrow::array::{FixedSizeBinaryArray, RecordBatch, StringArray};
use graphforge_api::{GraphForge, OperationId, bulk_edge_input_schema, bulk_node_input_schema};
use uuid::Uuid;

use super::support::*;

fn node_batch(ids: &[Uuid]) -> RecordBatch {
    RecordBatch::try_new(
        bulk_node_input_schema(Vec::new()).unwrap(),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(ids.iter().map(Uuid::as_bytes)).unwrap()),
            Arc::new(StringArray::from(vec!["Person"; ids.len()])),
        ],
    )
    .unwrap()
}

fn edge_batch(edge: Uuid, source: Uuid, target: Uuid) -> RecordBatch {
    let column = |value: Uuid| {
        Arc::new(FixedSizeBinaryArray::try_from_iter([value.as_bytes()].into_iter()).unwrap())
    };
    RecordBatch::try_new(
        bulk_edge_input_schema(Vec::new()).unwrap(),
        vec![
            column(edge),
            Arc::new(StringArray::from(vec!["KNOWS"])),
            column(source),
            column(target),
        ],
    )
    .unwrap()
}

fn operation(seed: u128) -> OperationId {
    OperationId(v7(0xfff_0000 + seed))
}

/// Whether a path names a membership index artifact of any generation.
fn is_membership_index(path: &str) -> bool {
    path.contains("identities-v5-")
        || path.contains("node-surrogates-v5-")
        || path.ends_with("uuid-membership/manifest.json")
        || path.ends_with("uuid-membership/topology-receipt.json")
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Inventory paths of the published generation that name an index.
fn published_index(project: &std::path::Path) -> Vec<String> {
    graphforge_storage::resolve_project_generation(project)
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .map(|entry| entry.relative_path)
        .filter(|path| is_membership_index(path))
        .collect()
}

/// The check finds the index where one exists: the checked-in project that the
/// producer of the index wrote (#1902).
#[test]
fn the_index_check_finds_a_real_legacy_index() {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    copy_dir(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/legacy-membership-index/project"),
        &project,
    );
    let found = published_index(&project);
    assert!(
        !found.is_empty(),
        "the fixture names no membership index file"
    );
}

#[test]
fn a_bulk_built_project_has_no_membership_index_and_appends_validate_against_parquet() {
    let directory = tempfile::tempdir().unwrap();
    let spec = Spec::graph(5_000, 8_000);
    let sources = Sources::write(&directory.path().join("input"), spec);
    let project = empty_project(directory.path());
    let graph = GraphForge::new(project.to_str()).unwrap();
    let mut session = register(&graph, &sources);
    let progress = validate(&graph, &mut session);
    assert!(progress.construction.unwrap().bulk_build.is_some());

    // Nothing the build encoded is an index, whether the name is the legacy
    // v5 file or one of its manifests.
    let encoded = inventory(&project);
    assert!(encoded.len() > 15);
    let named = encoded
        .iter()
        .filter(|artifact| is_membership_index(&artifact.path))
        .map(|artifact| artifact.path.clone())
        .collect::<Vec<_>>();
    assert!(named.is_empty(), "encoded: {named:?}");
    session.commit(&graph, None).unwrap();

    // Nor does the published generation's inventory, nor any file in the tree.
    let published = published_index(&project);
    assert!(published.is_empty(), "published: {published:?}");
    let on_disk = tree(&project)
        .into_keys()
        .map(|path| path.display().to_string())
        .filter(|path| is_membership_index(path))
        .collect::<Vec<_>>();
    assert!(on_disk.is_empty(), "on disk: {on_disk:?}");
    assert_eq!(counts(&graph), (5_000, 8_000));

    // Appends are answered from the published Parquet.
    let existing_node = node_uuid(3);
    let existing_edge = edge_uuid(3);
    let fresh = v7(0xabc_0001);
    fn conflict<T, E: std::fmt::Display>(result: Result<T, E>, what: &str, expected: &str) {
        let message = match result {
            Ok(_) => panic!("{what} was accepted"),
            Err(error) => error.to_string(),
        };
        assert!(message.contains(expected), "{what}: {message}");
    }
    conflict(
        graph.publish_bulk_nodes(operation(1), &[node_batch(&[existing_node])]),
        "a duplicate node",
        "identity_conflict",
    );
    conflict(
        graph.publish_bulk_edges(
            operation(2),
            &[edge_batch(existing_edge, node_uuid(0), node_uuid(1))],
        ),
        "a duplicate edge",
        "identity_conflict",
    );
    conflict(
        graph.publish_bulk_edges(
            operation(3),
            &[edge_batch(existing_node, node_uuid(0), node_uuid(1))],
        ),
        "an edge named like a node",
        "identity_conflict",
    );
    conflict(
        graph.publish_bulk_edges(
            operation(4),
            &[edge_batch(v7(0xabc_0002), node_uuid(0), fresh)],
        ),
        "an edge to a missing node",
        "missing_endpoint",
    );
    // Valid appends still work, and still publish no index.
    graph
        .publish_bulk_nodes(operation(5), &[node_batch(&[fresh])])
        .unwrap();
    graph
        .publish_bulk_edges(
            operation(6),
            &[edge_batch(v7(0xabc_0003), fresh, node_uuid(2))],
        )
        .unwrap();
    assert_eq!(counts(&graph), (5_001, 8_001));
    let published = published_index(&project);
    assert!(published.is_empty(), "after appends: {published:?}");
}
