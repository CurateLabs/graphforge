//! Deterministic CI fixture for file-backed graph generations (#338).
//!
//! Proves the public Rust facade can publish and reopen a multi-file graph
//! without assembling the workspace into one Arrow snapshot payload.
//!
//! Large 8M/128M evidence is not loaded in CI. Reproduce it manually with the
//! #334 / M4 entry harness after this contract lands:
//! `GF_M4_ENTRY_EVIDENCE_OUT=build/m4-entry-evidence.json make bench-m4-entry`
//! against a file-backed project built from the measured fixture.

#[path = "support/project_fixture.rs"]
mod project_fixture;

use graphforge_api::GraphForge;
use graphforge_storage::{GRAPH_FILES_FAMILY, GraphFilesOpenStrategy, resolve_project_generation};

#[test]
fn file_backed_multi_file_fixture_reopens_without_snapshot_envelope() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    {
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute("CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'})")
            .unwrap();
        graph
            .execute(
                "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
                 CREATE (a)-[:KNOWS]->(b)",
            )
            .unwrap();
    }

    let generation = resolve_project_generation(root.path()).unwrap();
    let files = generation
        .participant_snapshot("graph", GRAPH_FILES_FAMILY)
        .unwrap()
        .expect("published generation must use graph/files");
    assert_eq!(files.encoding, "json");
    assert!(
        generation
            .participant_snapshot("graph", "snapshot")
            .unwrap()
            .is_none(),
        "new publications must not write the legacy snapshot envelope"
    );
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    assert!(inventory.file_count >= 2);
    assert!(inventory.total_byte_length > 0);
    assert!(generation.graph_tree_root().is_dir());

    let reopened = GraphForge::new(Some(path)).unwrap();
    let evidence = reopened.graph_open_evidence();
    assert_eq!(
        evidence.strategy,
        GraphFilesOpenStrategy::PrivateMaterialize
    );
    assert!(evidence.files_validated >= 2);
    assert_eq!(evidence.files_copied, evidence.files_validated);
    assert_eq!(evidence.files_opened_in_place, 0);
    assert!(evidence.bytes_copied > 0);
    assert_eq!(evidence.bytes_copied, evidence.bytes_validated);

    let result = reopened
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS from, b.name AS to")
        .unwrap();
    assert_eq!(result.stats.rows_produced, 1);
}

#[test]
fn legacy_snapshot_generations_remain_readable() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(workspace.path().join("topology")).unwrap();
    std::fs::write(workspace.path().join("topology/nodes.parquet"), b"legacy").unwrap();
    project_fixture::publish_graph_workspace(root.path(), workspace.path());

    let generation = resolve_project_generation(root.path()).unwrap();
    assert!(
        generation
            .participant_snapshot("graph", "snapshot")
            .unwrap()
            .is_some()
    );
    assert!(
        generation
            .participant_snapshot("graph", GRAPH_FILES_FAMILY)
            .unwrap()
            .is_none()
    );

    let opened = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
    assert_eq!(
        opened.graph_open_evidence().strategy,
        GraphFilesOpenStrategy::LegacySnapshotHydrate
    );
}

#[test]
fn unsupported_graph_files_inventory_version_is_structured() {
    let error = graphforge_storage::decode_inventory(
        br#"{"format":"graphforge-graph-files","format_version":99,"file_count":0,"files":[],"total_byte_length":0}
"#,
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
}
