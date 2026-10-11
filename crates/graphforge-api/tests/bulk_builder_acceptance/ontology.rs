//! An initial import runs on a project that adopted one ontology document, in
//! strict and advisory mode alike, through the public routes (#1969, a slice of
//! #1881; ADR 0023, ADR 0058).
//!
//! Such a project is in the single-document regime: it carries no composition
//! context and no storage bindings, so an import builds under the document's
//! declarations, stores declared types under their ontology identities, names
//! routes by label, and publishes the participants the project already had.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, ListArray, StringArray, UInt32Array};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    AdoptOntologyRequest, CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, GraphForge,
    OntologyMode, OperationId, WriteContext,
};
use uuid::Uuid;

use super::support::*;

const ONTOLOGY: &str = "ontology_id: bulk\nversion: \"1\"\nentity_types:\n  - name: Host\n    abstract: false\n  - name: Switch\n    abstract: false\nrelation_types:\n  - name: CONNECTS\n    src: Host\n    dst: Host\nproperties: []\n";

/// An empty durable project that adopted `ONTOLOGY` in `mode`.
fn adopted(directory: &Path, name: &str, mode: OntologyMode) -> (std::path::PathBuf, GraphForge) {
    let project = directory.join(name);
    fs::create_dir(&project).unwrap();
    let ontology = directory.join(format!("{name}.yaml"));
    fs::write(&ontology, ONTOLOGY).unwrap();
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: WriteContext {
                operation_uuid: OperationId(v7(7)),
                actor_uuid: None,
            },
            path: ontology,
            mode,
        })
        .unwrap();
    (project, graph)
}

/// Import `sources` into `graph` through the public session and commit.
fn import(graph: &GraphForge, sources: &Sources) -> Uuid {
    let mut session = register(graph, sources);
    validate(graph, &mut session);
    session.commit(graph, None).unwrap()
}

fn count(graph: &GraphForge, query: &str) -> i64 {
    graph.execute(query).unwrap().batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count is an integer")
        .value(0)
}

/// The typed answers a reopened project must give.
fn typed_counts(graph: &GraphForge) -> [i64; 4] {
    [
        count(graph, "MATCH (n:Host) RETURN count(n) AS n"),
        count(graph, "MATCH (n:Switch) RETURN count(n) AS n"),
        count(
            graph,
            "MATCH (:Host)-[r:CONNECTS]->(:Host) RETURN count(r) AS n",
        ),
        count(graph, "MATCH ()-[r:CONNECTS]->() RETURN count(r) AS n"),
    ]
}

/// The `(capability, family)` of every participant of the current generation.
fn participants(project: &Path) -> BTreeSet<(String, String)> {
    graphforge_storage::resolve_project_generation(project)
        .unwrap()
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .map(|snapshot| (snapshot.capability_id, snapshot.record_family_id))
        .collect()
}

/// The storage bindings the current generation publishes, if any.
fn published_bindings(project: &Path) -> Option<graphforge_storage::SemanticStorageBindings> {
    graphforge_storage::semantic_storage_bindings(
        &graphforge_storage::resolve_project_generation(project).unwrap(),
    )
    .unwrap()
}

/// The route each table of the published graph is stored under: the first
/// path component beneath `topology/edges`, `properties` and `edge_properties`.
fn routes(project: &Path) -> BTreeSet<String> {
    graphforge_storage::resolve_project_generation(project)
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter_map(|entry| {
            let mut parts = entry.relative_path.split('/');
            let mut prefix = parts.next()?.to_owned();
            if prefix == "topology" {
                if parts.next()? != "edges" {
                    return None;
                }
                prefix = "topology/edges".to_owned();
            } else if prefix != "properties" && prefix != "edge_properties" {
                return None;
            }
            let route = parts.next()?;
            Some(format!("{prefix}/{}", route.trim_end_matches(".parquet")))
        })
        .collect()
}

/// The bytes of one published file of the current generation.
fn published_file(project: &Path, relative: &str) -> Vec<u8> {
    let inventory = graphforge_storage::resolve_project_generation(project)
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap();
    let entry = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == relative)
        .unwrap_or_else(|| panic!("{relative} is not published"));
    fs::read(graphforge_storage::graph_object_path(project, &entry.content_sha256).unwrap())
        .unwrap()
}

/// Every node's type identities as stored, sorted: the integers a reader
/// resolves a label from.
fn stored_node_types(project: &Path) -> Vec<u32> {
    let generation = graphforge_storage::resolve_project_generation(project).unwrap();
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let workspace = scratch.path().join("workspace");
    graphforge_storage::materialize_graph_objects(project, &inventory, &workspace).unwrap();
    let mut types = Vec::new();
    for entry in inventory.files.iter().filter(|entry| {
        (entry.relative_path.starts_with("topology/nodes/")
            || entry.relative_path == "topology/nodes.parquet")
            && entry.relative_path.ends_with(".parquet")
    }) {
        let file = fs::File::open(workspace.join(&entry.relative_path)).unwrap();
        for batch in parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap()
        {
            let batch = batch.unwrap();
            let lists = batch
                .column_by_name("type_ids")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..lists.len() {
                let values = lists.value(row);
                types.extend(
                    values
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap()
                        .values(),
                );
            }
        }
    }
    types.sort_unstable();
    types
}

fn uuids(ids: impl Iterator<Item = Uuid>) -> ArrayRef {
    let ids = ids.collect::<Vec<_>>();
    Arc::new(
        FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_bytes().as_slice())).unwrap(),
    )
}

fn node_chunk(label: &str, range: std::ops::Range<usize>) -> RecordBatch {
    RecordBatch::try_new(
        CONSTRUCTION_NODE_SCHEMA.clone(),
        vec![
            uuids(range.clone().map(node_uuid)),
            Arc::new(StringArray::from(range.map(|_| label).collect::<Vec<_>>())),
        ],
    )
    .unwrap()
}

fn edge_chunk(rel_type: &str, nodes: usize, range: std::ops::Range<usize>) -> RecordBatch {
    RecordBatch::try_new(
        CONSTRUCTION_EDGE_SCHEMA.clone(),
        vec![
            uuids(range.clone().map(edge_uuid)),
            Arc::new(StringArray::from(
                range.clone().map(|_| rel_type).collect::<Vec<_>>(),
            )),
            uuids(range.clone().map(|row| node_uuid(row % nodes))),
            uuids(range.map(|row| node_uuid((row + 1) % nodes))),
        ],
    )
    .unwrap()
}

#[test]
fn a_strict_ontology_refuses_an_import_of_undeclared_types_at_validate() {
    let directory = tempfile::tempdir().unwrap();
    let (project, graph) = adopted(directory.path(), "strict", OntologyMode::Strict);
    let before = current_generation(&project);

    // Nodes labelled `Person`, which the ontology does not declare.
    let people = Sources::write(&directory.path().join("people"), Spec::graph(10, 10));
    // Declared nodes joined by a relationship the ontology does not declare.
    let hosts = Sources::write_typed(&directory.path().join("hosts"), 4, 4, "Host", "UNDECLARED");
    for (sources, expected) in [
        (people, "unknown strict ontology entity type"),
        (hosts, "unknown strict ontology relationship type"),
    ] {
        let mut session = register(&graph, &sources);
        let refusal = session.validate(&graph).unwrap_err().to_string();
        assert!(refusal.contains(expected), "{refusal}");
        assert_eq!(session.status().1.rows_accepted, 0);
        assert!(
            session
                .commit(&graph, None)
                .unwrap_err()
                .to_string()
                .contains("validated")
        );
        assert_eq!(current_generation(&project), before);
        assert_eq!(counts(&graph), (0, 0));
    }
}

#[test]
fn a_strict_import_commits_and_reopens_with_exact_typed_counts_and_ids() {
    let directory = tempfile::tempdir().unwrap();
    let (project, graph) = adopted(directory.path(), "strict", OntologyMode::Strict);
    let sources = Sources::write_typed(&directory.path().join("input"), 6, 5, "Host", "CONNECTS");

    import(&graph, &sources);
    assert_eq!(counts(&graph), (6, 5));
    assert_eq!(typed_counts(&graph), [6, 0, 5, 5]);
    drop(graph);

    let reopened = GraphForge::new(project.to_str()).unwrap();
    assert_eq!(counts(&reopened), (6, 5));
    assert_eq!(typed_counts(&reopened), [6, 0, 5, 5]);
    // `Host` is the document's first entity: stored under its ontology
    // identity 0, not under a tagged runtime identity.
    assert_eq!(stored_node_types(&project), vec![0; 6]);
    // The project stays in the single-document regime.
    assert!(published_bindings(&project).is_none());
}

#[test]
fn a_strict_import_publishes_what_the_writer_path_publishes() {
    let directory = tempfile::tempdir().unwrap();
    let (imported, graph) = adopted(directory.path(), "imported", OntologyMode::Strict);
    import(
        &graph,
        &Sources::write_typed(&directory.path().join("input"), 6, 5, "Host", "CONNECTS"),
    );
    drop(graph);

    let (written, graph) = adopted(directory.path(), "written", OntologyMode::Strict);
    let nodes = (0..6)
        .map(|_| graph.add_node("Host", &Default::default()).unwrap())
        .collect::<Vec<_>>();
    for edge in 0..5 {
        graph
            .add_edge(
                &nodes[edge % 6],
                "CONNECTS",
                &nodes[(edge + 1) % 6],
                &Default::default(),
            )
            .unwrap();
    }
    drop(graph);

    let imported_graph = GraphForge::new(imported.to_str()).unwrap();
    let written_graph = GraphForge::new(written.to_str()).unwrap();
    assert_eq!(counts(&imported_graph), counts(&written_graph));
    assert_eq!(typed_counts(&imported_graph), typed_counts(&written_graph));
    assert_eq!(stored_node_types(&imported), stored_node_types(&written));
    assert_eq!(participants(&imported), participants(&written));
    assert_eq!(routes(&imported), routes(&written));
    for file in [
        "semantic-routes.json",
        "topology/runtime_entity_label_encoding.json",
    ] {
        assert_eq!(
            published_file(&imported, file),
            published_file(&written, file)
        );
    }
    assert!(published_bindings(&imported).is_none() && published_bindings(&written).is_none());
}

#[test]
fn the_chunk_api_builds_under_a_strict_ontology_and_refuses_undeclared_types() {
    let directory = tempfile::tempdir().unwrap();
    let (project, graph) = adopted(directory.path(), "strict", OntologyMode::Strict);
    let before = current_generation(&project);

    // An undeclared label is refused when the build seals, and nothing publishes.
    let mut session = graph.begin_graph_construction(Default::default()).unwrap();
    session
        .append_nodes("nodes", &node_chunk("Person", 0..4))
        .unwrap();
    session
        .append_edges("edges", &edge_chunk("CONNECTS", 4, 0..3))
        .unwrap();
    let refusal = session.seal_and_publish().unwrap_err().to_string();
    assert!(
        refusal.contains("unknown strict ontology entity type"),
        "{refusal}"
    );
    drop(session);
    assert_eq!(current_generation(&project), before);

    let mut session = graph.begin_graph_construction(Default::default()).unwrap();
    session
        .append_nodes("nodes", &node_chunk("Host", 0..4))
        .unwrap();
    session
        .append_edges("edges", &edge_chunk("UNDECLARED", 4, 0..3))
        .unwrap();
    let refusal = session.seal_and_publish().unwrap_err().to_string();
    assert!(
        refusal.contains("unknown strict ontology relationship type"),
        "{refusal}"
    );
    drop(session);
    assert_eq!(current_generation(&project), before);

    let mut session = graph.begin_graph_construction(Default::default()).unwrap();
    session
        .append_nodes("nodes", &node_chunk("Host", 0..4))
        .unwrap();
    session
        .append_nodes("more-nodes", &node_chunk("Switch", 4..6))
        .unwrap();
    session
        .append_edges("edges", &edge_chunk("CONNECTS", 4, 0..3))
        .unwrap();
    session.seal_and_publish().unwrap();
    assert_eq!(counts(&graph), (6, 3));
    drop(session);
    drop(graph);

    let reopened = GraphForge::new(project.to_str()).unwrap();
    assert_eq!(counts(&reopened), (6, 3));
    assert_eq!(typed_counts(&reopened), [4, 2, 3, 3]);
    assert_eq!(stored_node_types(&project), vec![0, 0, 0, 0, 1, 1]);
    assert!(published_bindings(&project).is_none());
}

#[test]
fn an_advisory_import_stores_declared_types_by_ontology_identity_and_admits_others() {
    let directory = tempfile::tempdir().unwrap();
    let (project, graph) = adopted(directory.path(), "advisory", OntologyMode::Advisory);
    // `Person` is undeclared: advisory mode admits it as a runtime type.
    let people = Sources::write(&directory.path().join("people"), Spec::graph(10, 10));
    let mut session = register(&graph, &people);
    validate(&graph, &mut session);
    session.commit(&graph, None).unwrap();
    drop(graph);

    let reopened = GraphForge::new(project.to_str()).unwrap();
    assert_eq!(counts(&reopened), (10, 10));
    assert_eq!(
        count(&reopened, "MATCH (n:Person) RETURN count(n) AS n"),
        10
    );

    let (project, graph) = adopted(
        directory.path(),
        "advisory-declared",
        OntologyMode::Advisory,
    );
    import(
        &graph,
        &Sources::write_typed(&directory.path().join("input"), 6, 5, "Host", "CONNECTS"),
    );
    drop(graph);
    let reopened = GraphForge::new(project.to_str()).unwrap();
    assert_eq!(typed_counts(&reopened), [6, 0, 5, 5]);
    assert_eq!(stored_node_types(&project), vec![0; 6]);
    assert!(published_bindings(&project).is_none());
}
