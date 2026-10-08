//! The UUID membership index is gone (#1902).
//!
//! `tests/fixtures/legacy-membership-index/` is a real durable project written
//! by the binary that produced the index (an initial build and one append). The
//! index files must keep opening, exporting and verifying as ordinary entries;
//! nothing reads them, and every append refusal is answered from the Parquet.
//! A project created now never writes them, and its portable bundle never
//! carries them.
#![cfg(feature = "portable")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{FixedSizeBinaryArray, RecordBatch, StringArray};
use graphforge_api::{
    GraphForge, OperationId, PortableSelection, PortableV2ExportRequest, PortableV2ImportRequest,
    PortableV2Limits, PortableV2Output, PortableV2SelectionProfile, PortableVerifyRequest,
    bulk_edge_input_schema, bulk_node_input_schema, verify_portable_v2,
};
use graphforge_core::portable::PortableV2Mode;
use graphforge_ir::IrLiteral;
use uuid::Uuid;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/legacy-membership-index/project"
);

/// The fixture generator's UUIDs: `kind` 1 for nodes, 2 for edges.
fn id(kind: u128, index: u128) -> Uuid {
    Uuid::from_u128((kind << 100) | (0x7 << 76) | (0x2 << 62) | index)
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
            // Git does not preserve the producer's read-only CAS permissions.
            if from
                .parent()
                .is_some_and(|parent| parent.ends_with("graph-objects/sha256"))
            {
                let mut permissions = std::fs::metadata(&target).unwrap().permissions();
                permissions.set_readonly(true);
                std::fs::set_permissions(&target, permissions).unwrap();
            }
        }
    }
}

fn legacy_project() -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    copy_dir(Path::new(FIXTURE), &project);
    for empty in ["graph-objects/active", "graph-objects/tmp"] {
        std::fs::create_dir_all(project.join(empty)).unwrap();
    }
    (directory, project)
}

fn count(graph: &GraphForge, cypher: &str) -> i64 {
    let result = graph.execute(cypher).unwrap();
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0)
}

fn node_batch(ids: &[Uuid]) -> RecordBatch {
    let schema = bulk_node_input_schema(Vec::new()).unwrap();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(ids.iter().map(Uuid::as_bytes)).unwrap()),
            Arc::new(StringArray::from(vec!["Person"; ids.len()])),
        ],
    )
    .unwrap()
}

fn edge_batch(edge: Uuid, source: Uuid, target: Uuid) -> RecordBatch {
    let schema = bulk_edge_input_schema(Vec::new()).unwrap();
    let column = |value: Uuid| {
        Arc::new(FixedSizeBinaryArray::try_from_iter([value.as_bytes()].into_iter()).unwrap())
    };
    RecordBatch::try_new(
        schema,
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
    OperationId(id(3, seed))
}

/// Whether the bundle's tar headers name a membership index file. The bundle is
/// an uncompressed tar stream, so entry names appear verbatim.
fn bundle_names_membership_index(bundle: &Path) -> bool {
    let bytes = std::fs::read(bundle).unwrap();
    [
        &b"identities-v5-"[..],
        b"node-surrogates-v5-",
        b"uuid-membership/manifest.json",
        b"uuid-membership/topology-receipt.json",
    ]
    .iter()
    .any(|name| bytes.windows(name.len()).any(|window| window == *name))
}

fn membership_entries(project: &Path) -> Vec<String> {
    let generation = graphforge_storage::resolve_project_generation(project).unwrap();
    generation
        .graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .map(|entry| entry.relative_path)
        .filter(|path| path.starts_with("topology/uuid-membership/"))
        .collect()
}

#[test]
fn a_legacy_project_opens_appends_refuses_and_round_trips() {
    let (directory, project) = legacy_project();
    let before = membership_entries(&project);
    assert!(
        before
            .iter()
            .any(|path| path == "topology/uuid-membership/manifest.json")
            && before.iter().any(|path| path.contains("-v5-")),
        "the fixture must carry the legacy index: {before:?}"
    );

    let graph = GraphForge::new(project.to_str()).unwrap();
    assert_eq!(count(&graph, "MATCH (n) RETURN count(n)"), 14);
    assert_eq!(count(&graph, "MATCH ()-[r]->() RETURN count(r)"), 14);

    // Appends refuse against the existing graph, answered from the Parquet.
    let existing_node = id(1, 3);
    let existing_edge = id(2, 3);
    let fresh = id(1, 9_000);
    let duplicate_node = graph
        .publish_bulk_nodes(operation(1), &[node_batch(&[existing_node])])
        .unwrap_err()
        .to_string();
    assert!(
        duplicate_node.contains("identity_conflict"),
        "{duplicate_node}"
    );
    let duplicate_edge = graph
        .publish_bulk_edges(
            operation(2),
            &[edge_batch(existing_edge, id(1, 0), id(1, 1))],
        )
        .unwrap_err()
        .to_string();
    assert!(
        duplicate_edge.contains("identity_conflict"),
        "{duplicate_edge}"
    );
    let edge_named_like_a_node = graph
        .publish_bulk_edges(
            operation(3),
            &[edge_batch(existing_node, id(1, 0), id(1, 1))],
        )
        .unwrap_err()
        .to_string();
    assert!(
        edge_named_like_a_node.contains("identity_conflict"),
        "{edge_named_like_a_node}"
    );
    let missing_endpoint = graph
        .publish_bulk_edges(operation(4), &[edge_batch(id(2, 9_001), id(1, 0), fresh)])
        .unwrap_err();
    assert!(
        missing_endpoint.to_string().contains("missing_endpoint"),
        "{missing_endpoint}"
    );

    // Appends still work: a bulk append that reaches an existing node, then a
    // Cypher write, each through the writer commit that used to feed the index.
    graph
        .publish_bulk_nodes(operation(5), &[node_batch(&[fresh])])
        .unwrap();
    graph
        .publish_bulk_edges(operation(6), &[edge_batch(id(2, 9_002), fresh, id(1, 2))])
        .unwrap();
    graph
        .execute("CREATE (:Person {name: 'new'})-[:KNOWS]->(:Person {name: 'newer'})")
        .unwrap();
    assert_eq!(count(&graph, "MATCH (n) RETURN count(n)"), 17);
    assert_eq!(count(&graph, "MATCH ()-[r]->() RETURN count(r)"), 16);

    // A deletion records the spent UUID; reuse is refused by validation and commit.
    let params = HashMap::from([("id".to_owned(), IrLiteral::Uuid(*fresh.as_bytes()))]);
    graph
        .execute_with_params("MATCH (n) WHERE n.node_uuid = $id DETACH DELETE n", &params)
        .unwrap();
    assert_eq!(count(&graph, "MATCH (n) RETURN count(n)"), 16);
    let reuse = graph
        .publish_bulk_nodes(operation(7), &[node_batch(&[fresh])])
        .unwrap_err()
        .to_string();
    assert!(reuse.contains("identity_conflict"), "{reuse}");
    let reused_edge = graph
        .publish_bulk_edges(
            operation(8),
            &[edge_batch(id(2, 9_002), id(1, 0), id(1, 1))],
        )
        .unwrap_err()
        .to_string();
    assert!(reused_edge.contains("identity_conflict"), "{reused_edge}");

    // The legacy files are still ordinary graph entries: untouched, exported,
    // verified and imported.
    assert_eq!(
        membership_entries(&project)
            .into_iter()
            .filter(|path| before.contains(path))
            .count(),
        before.len(),
        "legacy membership entries carry forward unchanged"
    );
    let bundle = directory.path().join("legacy.gfpb");
    let exported = graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: bundle.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    // The check that a bundle carries no index finds one when it is there.
    assert!(
        bundle_names_membership_index(&bundle),
        "a legacy bundle must name its index files"
    );
    verify_portable_v2(
        &PortableVerifyRequest {
            input: bundle.clone(),
            mode: PortableV2Mode::Full,
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap();
    drop(graph);
    let target = directory.path().join("imported");
    GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: bundle,
            operation_id: operation(9),
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap();
    let imported = GraphForge::new(target.to_str()).unwrap();
    assert_eq!(count(&imported, "MATCH (n) RETURN count(n)"), 16);
    assert_eq!(count(&imported, "MATCH ()-[r]->() RETURN count(r)"), 15);
    assert!(!exported.package_digest.is_empty());
    // The deleted UUID stays spent after the round trip.
    assert!(
        imported
            .publish_bulk_nodes(operation(10), &[node_batch(&[fresh])])
            .is_err()
    );
}

#[test]
fn a_new_project_carries_no_membership_index_and_round_trips() {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    let graph = GraphForge::new(project.to_str()).unwrap();
    let nodes = (0..6).map(|index| id(1, index)).collect::<Vec<_>>();
    graph
        .publish_bulk_nodes(operation(1), &[node_batch(&nodes)])
        .unwrap();
    for index in 0..6_u128 {
        graph
            .publish_bulk_edges(
                operation(10 + index),
                &[edge_batch(
                    id(2, index),
                    nodes[usize::try_from(index).unwrap()],
                    nodes[usize::try_from((index + 1) % 6).unwrap()],
                )],
            )
            .unwrap();
    }
    graph
        .execute("CREATE (:Person {name: 'a'})-[:KNOWS]->(:Person {name: 'b'})")
        .unwrap();
    assert_eq!(count(&graph, "MATCH (n) RETURN count(n)"), 8);
    assert_eq!(count(&graph, "MATCH ()-[r]->() RETURN count(r)"), 7);

    // Neither the bulk publication nor the writer commit produced the index.
    let entries = membership_entries(&project);
    let index = entries
        .iter()
        .filter(|path| {
            path.ends_with("/manifest.json")
                || path.ends_with("/topology-receipt.json")
                || path.contains("-v5-")
        })
        .collect::<Vec<_>>();
    assert!(index.is_empty(), "membership index published: {index:?}");

    // Refusals are answered from the Parquet written by those commits.
    let duplicate_node = graph
        .publish_bulk_nodes(operation(40), &[node_batch(&[nodes[2]])])
        .unwrap_err()
        .to_string();
    assert!(
        duplicate_node.contains("identity_conflict"),
        "{duplicate_node}"
    );
    let duplicate_edge = graph
        .publish_bulk_edges(operation(41), &[edge_batch(id(2, 3), nodes[0], nodes[1])])
        .unwrap_err()
        .to_string();
    assert!(
        duplicate_edge.contains("identity_conflict"),
        "{duplicate_edge}"
    );
    let missing = graph
        .publish_bulk_edges(
            operation(42),
            &[edge_batch(id(2, 9_000), nodes[0], id(1, 9_000))],
        )
        .unwrap_err()
        .to_string();
    assert!(missing.contains("missing_endpoint"), "{missing}");

    let bundle = directory.path().join("new.gfpb");
    graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: bundle.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    assert!(
        !bundle_names_membership_index(&bundle),
        "the bundle must not carry the membership index"
    );
    verify_portable_v2(
        &PortableVerifyRequest {
            input: bundle.clone(),
            mode: PortableV2Mode::Full,
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap();
    drop(graph);
    let target = directory.path().join("imported");
    GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: bundle,
            operation_id: operation(50),
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap();
    let imported = GraphForge::new(target.to_str()).unwrap();
    assert_eq!(count(&imported, "MATCH (n) RETURN count(n)"), 8);
    assert_eq!(count(&imported, "MATCH ()-[r]->() RETURN count(r)"), 7);
    assert!(
        imported
            .publish_bulk_nodes(operation(51), &[node_batch(&[nodes[4]])])
            .unwrap_err()
            .to_string()
            .contains("identity_conflict")
    );
    assert!(membership_entries(&target).iter().all(|path| {
        !path.ends_with("/manifest.json")
            && !path.ends_with("/topology-receipt.json")
            && !path.contains("-v5-")
    }));
}
